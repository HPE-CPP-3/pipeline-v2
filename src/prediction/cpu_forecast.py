"""
CPU usage forecasting
"""

import torch
import numpy as np
from typing import Optional
import logging

from ..models.patchtst import PatchTST

logger = logging.getLogger(__name__)


class CPUPredictor:
    """
    CPU usage predictor using PatchTST

    Outputs quantile forecasts for multiple horizons
    """

    def __init__(self, model: PatchTST, horizons: list[int] = None):
        """
        Args:
            model: Trained PatchTST model
            horizons: Forecast horizons in minutes
        """
        self.model = model
        self.horizons = horizons or model.forecast_horizons

        logger.info(f"Initialized CPUPredictor with horizons={self.horizons}")

    def predict(
        self,
        features: np.ndarray,
        return_dict: bool = True,
    ) -> dict[int, dict[float, float]]:
        """
        Make CPU usage predictions

        Args:
            features: Input features (seq_len, input_dim) or (batch_size, seq_len, input_dim)
            return_dict: If True, return nested dict

        Returns:
            Dict of horizon -> quantile -> value
        """
        # Ensure 3D input
        if features.ndim == 2:
            features = np.expand_dims(features, axis=0)

        # Convert to tensor
        features_tensor = torch.FloatTensor(features)

        # Make predictions
        self.model.eval()
        with torch.no_grad():
            predictions = self.model.predict(features_tensor, return_numpy=True)

        if not return_dict:
            return predictions

        # Convert to nested dict
        result = {}
        for (horizon, quantile), values in predictions.items():
            if horizon not in result:
                result[horizon] = {}
            # Take first batch item
            result[horizon][quantile] = float(values[0].mean())  # Average over horizon

        logger.debug(f"CPU predictions: {result}")
        return result
