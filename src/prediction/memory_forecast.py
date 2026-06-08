"""
Memory usage forecasting
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


class MemoryPredictor:
    """
    Memory usage predictor using PatchTST

    Outputs quantile forecasts for multiple horizons
    """

    def __init__(self, model: PatchTST, horizons: list[int] = None,
                 mem_mu: float = 0.0, mem_sigma: float = 1.0):
        """
        Args:
            model: Trained PatchTST model
            horizons: Forecast horizons in minutes
            mem_mu: Mean from training normalization
            mem_sigma: Std dev from training normalization
        """
        self.model = model
        self.horizons = horizons or (model.forecast_horizons if model else [5, 10, 15])
        self.mem_mu = mem_mu
        self.mem_sigma = mem_sigma if mem_sigma > 0 else 1.0

        logger.info(f"Initialized MemoryPredictor with horizons={self.horizons}, mu={mem_mu:.0f}, sigma={mem_sigma:.0f}")

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
            _cpu_pred, mem_pred, _throttle, _oom = self.model(features_tensor)
            predictions = mem_pred.cpu().numpy()

        # De-normalize
        predictions = predictions * self.mem_sigma + self.mem_mu
        predictions = np.maximum(predictions, 0.0)  # Memory can't be negative

        if not return_dict:
            return predictions

        result = {}
        for i, h in enumerate(self.horizons):
            p50 = float(predictions[0, i])
            result[h] = {0.5: p50, 0.7: p50 * 1.05, 0.9: p50 * 1.1}

        logger.debug(f"Memory predictions: {result}")
        return result

    def predict_all(self, features: np.ndarray) -> tuple[dict[int, dict[float, float]], float]:
        """
        Run a single forward pass and return both the memory forecast dict
        and the OOM probability derived from the model's oom logit.

        Args:
            features: Input array of shape (T, F) or (1, T, F)

        Returns:
            memory_forecast: dict[horizon -> {quantile -> value}] (in bytes)
            oom_prob: float in [0, 1] from sigmoid(oom_logit)
        """
        if features.ndim == 2:
            features = np.expand_dims(features, axis=0)

        # DIAGNOSTIC
        logger.warning(f"[MEMORY] model is None: {self.model is None}")
        logger.warning(f"[MEMORY] input sum: {features.sum():.6f}")

        if self.model is None:
            logger.error("[MEMORY] MODEL IS NONE - returning zeros!")
            memory_forecast = {}
            for h in self.horizons:
                memory_forecast[h] = {0.5: 0.0, 0.7: 0.0, 0.9: 0.0}
            return memory_forecast, 0.0

        features_tensor = torch.as_tensor(
            features,
            dtype=torch.float32,
            device=_model_device(self.model),
        )

        self.model.eval()
        with torch.no_grad():
            _cpu_pred, mem_pred, _throttle_logit, oom_logit = self.model(features_tensor)
            predictions = mem_pred.cpu().numpy()
            oom_prob = float(torch.sigmoid(oom_logit).cpu().numpy().flat[0])

        # LOG THE RAW MODEL OUTPUT
        logger.warning(f"[MEMORY] RAW predictions: {predictions}")

        # DE-NORMALIZE: Apply inverse z-score transformation
        predictions = predictions * self.mem_sigma + self.mem_mu
        predictions = np.maximum(predictions, 0.0)  # Memory can't be negative

        memory_forecast: dict[int, dict[float, float]] = {}
        for i, h in enumerate(self.horizons):
            p50 = float(predictions[0, i])
            memory_forecast[h] = {
                0.5: p50,
                0.7: p50 * 1.05,  # memory grows more predictably, tighter band
                0.9: p50 * 1.1,   # p90: 10% above median
            }

        logger.debug(f"Memory predict_all: forecast={memory_forecast}, oom_prob={oom_prob:.4f}")
        return memory_forecast, oom_prob
