"""
CPU usage forecasting
"""

import torch
import numpy as np
from typing import Optional
import logging

from ..models.patchtst import PatchTST

logger = logging.getLogger(__name__)


def _model_device(model) -> torch.device:
    try:
        return next(model.parameters()).device
    except StopIteration:
        return torch.device("cpu")


class CPUPredictor:
    """
    CPU usage predictor using PatchTST

    Outputs quantile forecasts for multiple horizons
    """

    def __init__(self, model: PatchTST, horizons: list[int] = None, 
                 cpu_mu: float = 0.0, cpu_sigma: float = 1.0):
        """
        Args:
            model: Trained PatchTST model
            horizons: Forecast horizons in minutes
            cpu_mu: Mean from training normalization
            cpu_sigma: Std dev from training normalization
        """
        self.model = model
        self.horizons = horizons or (model.forecast_horizons if model else [5, 10, 15])
        self.cpu_mu = cpu_mu
        self.cpu_sigma = cpu_sigma if cpu_sigma > 0 else 1.0

        logger.info(f"Initialized CPUPredictor with horizons={self.horizons}, mu={cpu_mu:.4f}, sigma={cpu_sigma:.4f}")

    def predict(self, features: np.ndarray, return_dict: bool = True):
        if features.ndim == 2:
            features = np.expand_dims(features, axis=0)

        features_tensor = torch.as_tensor(
            features,
            dtype=torch.float32,
            device=_model_device(self.model),
        )

        self.model.eval()
        with torch.no_grad():
            cpu_pred, _mem_pred, _throttle, _oom = self.model(features_tensor)
            # cpu_pred: [batch, num_horizons]
            predictions = cpu_pred.cpu().numpy()

        # De-normalize
        predictions = predictions * self.cpu_sigma + self.cpu_mu
        predictions = np.maximum(predictions, 0.0)  # CPU can't be negative

        if not return_dict:
            return predictions

        result = {}
        for i, h in enumerate(self.horizons):
            p50 = float(predictions[0, i])
            result[h] = {0.5: p50, 0.7: p50 * 1.1, 0.9: p50 * 1.2}

        logger.debug(f"CPU predictions: {result}")
        return result

    def predict_all(self, features: np.ndarray) -> tuple[dict[int, dict[float, float]], float]:
        """
        Run a single forward pass and return both the CPU forecast dict
        and the throttle probability derived from the model's throttle logit.

        Args:
            features: Input array of shape (T, F) or (1, T, F)

        Returns:
            cpu_forecast: dict[horizon -> {quantile -> value}] (in actual CPU cores)
            throttle_prob: float in [0, 1] from sigmoid(throttle_logit)
        """
        if features.ndim == 2:
            features = np.expand_dims(features, axis=0)
        
        # DEBUG: Check input
        if np.isnan(features).any():
            logger.warning("NaN in input features!")
        if np.isinf(features).any():
            logger.warning("Inf in input features!")

        features_tensor = torch.as_tensor(
            features,
            dtype=torch.float32,
            device=_model_device(self.model),
        )

        self.model.eval()
        with torch.no_grad():
            cpu_pred, _mem_pred, throttle_logit, _oom_logit = self.model(features_tensor)
            # DEBUG: Check raw outputs
            if torch.isnan(cpu_pred).any():
                logger.warning("NaN in cpu_pred!")
            if torch.isnan(throttle_logit).any():
                logger.warning("NaN in throttle_logit!")
            predictions = cpu_pred.cpu().numpy()
            throttle_prob = float(torch.sigmoid(throttle_logit).cpu().numpy().flat[0])

        # DE-NORMALIZE: Apply inverse z-score transformation
        predictions = predictions * self.cpu_sigma + self.cpu_mu
        predictions = np.maximum(predictions, 0.0)  # CPU can't be negative

        cpu_forecast: dict[int, dict[float, float]] = {}
        for i, h in enumerate(self.horizons):
            p50 = float(predictions[0, i])
            cpu_forecast[h] = {
                0.5: p50,
                0.7: p50 * 1.1,   # p70: 10% above median
                0.9: p50 * 1.2,   # p90: 20% above median (conservative SLA buffer)
            }

        logger.debug(f"CPU predict_all: forecast={cpu_forecast}, throttle_prob={throttle_prob:.4f}")
        return cpu_forecast, throttle_prob
