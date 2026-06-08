"""
Quantile loss (Pinball loss) for PatchTST
"""

import torch
import torch.nn as nn
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def pinball_loss(
    y_true: torch.Tensor,
    y_pred: torch.Tensor,
    quantile: float,
) -> torch.Tensor:
    """
    Calculate pinball loss (quantile loss)

    Pinball loss is used for quantile regression:
    L_τ(y, ŷ) = τ * max(y - ŷ, 0) + (1 - τ) * max(ŷ - y, 0)

    Args:
        y_true: Ground truth values
        y_pred: Predicted quantile values
        quantile: Quantile level (τ) between 0 and 1

    Returns:
        Pinball loss (scalar)
    """
    # Calculate errors
    errors = y_true - y_pred

    # Pinball loss
    loss = torch.max((quantile - 1) * errors, quantile * errors)

    return loss.mean()


class QuantileLoss(nn.Module):
    """
    Multi-quantile loss for PatchTST

    Combines pinball losses for multiple quantiles with optional weighting
    """

    def __init__(
        self,
        quantiles: list[float] = None,
        quantile_weights: Optional[dict[float, float]] = None,
    ):
        """
        Args:
            quantiles: List of quantiles to optimize
            quantile_weights: Optional weights for each quantile loss
                             Higher weights = more emphasis on that quantile
        """
        super().__init__()

        self.quantiles = quantiles or [0.5, 0.7, 0.9]
        self.quantile_weights = quantile_weights or {}

        # Default weights (can be adjusted for conservative forecasts)
        default_weights = {
            0.5: 1.0,  # Median
            0.7: 1.2,  # Upper quantile (slightly more weight)
            0.9: 1.5,  # High quantile (most weight for conservative forecasts)
        }

        # Merge with provided weights
        for q in self.quantiles:
            if q not in self.quantile_weights:
                self.quantile_weights[q] = default_weights.get(q, 1.0)

        logger.info(
            f"Initialized QuantileLoss with quantiles={self.quantiles}, "
            f"weights={self.quantile_weights}"
        )

    def forward(
        self,
        predictions: dict[tuple[int, float], torch.Tensor],
        y_true: torch.Tensor,
    ) -> torch.Tensor:
        """
        Calculate total quantile loss

        Args:
            predictions: Dict of (horizon, quantile) -> predictions
            y_true: Ground truth values (batch_size, max_horizon)

        Returns:
            Weighted sum of pinball losses
        """
        total_loss = 0.0
        loss_breakdown = {}

        for (horizon, quantile), y_pred in predictions.items():
            # Get ground truth for this horizon
            if y_true.shape[1] >= horizon:
                y_true_horizon = y_true[:, :horizon]
            else:
                # Pad if necessary
                pad_len = horizon - y_true.shape[1]
                y_true_horizon = torch.nn.functional.pad(y_true, (0, pad_len))

            # Calculate pinball loss
            loss = pinball_loss(y_true_horizon, y_pred, quantile)

            # Apply weight
            weight = self.quantile_weights.get(quantile, 1.0)
            weighted_loss = loss * weight

            total_loss += weighted_loss
            loss_breakdown[(horizon, quantile)] = loss.item()

        return total_loss

    def calculate_metrics(
        self,
        predictions: dict[tuple[int, float], torch.Tensor],
        y_true: torch.Tensor,
    ) -> dict[str, float]:
        """
        Calculate evaluation metrics

        Args:
            predictions: Dict of (horizon, quantile) -> predictions
            y_true: Ground truth values

        Returns:
            Dict of metric name -> value
        """
        metrics = {}

        # Calculate MAE for each quantile
        for (horizon, quantile), y_pred in predictions.items():
            if y_true.shape[1] >= horizon:
                y_true_horizon = y_true[:, :horizon]
            else:
                pad_len = horizon - y_true.shape[1]
                y_true_horizon = torch.nn.functional.pad(y_true, (0, pad_len))

            mae = torch.abs(y_true_horizon - y_pred).mean().item()
            metrics[f"mae_h{horizon}_q{quantile}"] = mae

            # Pinball loss
            pinball = pinball_loss(y_true_horizon, y_pred, quantile).item()
            metrics[f"pinball_h{horizon}_q{quantile}"] = pinball

        # Coverage metrics (for quantile calibration)
        for horizon in set(h for h, _ in predictions.keys()):
            q_low = None
            q_high = None

            for (h, q), y_pred in predictions.items():
                if h == horizon:
                    if q < 0.5:
                        q_low = y_pred
                    elif q > 0.5:
                        q_high = y_pred

            if q_low is not None and q_high is not None:
                # Calculate coverage
                coverage = (
                    ((y_true[:, :horizon] >= q_low) & (y_true[:, :horizon] <= q_high))
                    .float()
                    .mean()
                    .item()
                )
                metrics[f"coverage_h{horizon}"] = coverage

        return metrics
