"""
EMA-based drift detection
"""

import math
from typing import Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class EMADriftDetector:
    """
    Exponential Moving Average based drift detection

    No fixed window - continuous adaptation

    Features:
    - Per-feature EMA and EMA variance
    - Z-score based drift detection
    - Configurable thresholds
    - Cooldown period
    """

    def __init__(self, config: dict):
        """
        Args:
            config: Configuration dict with:
                - ema_alpha: EMA smoothing factor (0.0-1.0)
                - z_score_threshold: Z-score threshold for drift
                - feature_ratio_threshold: Fraction of features for trigger
                - min_features_monitored: Minimum features to monitor
                - cooldown_minutes: Cooldown after drift trigger
                - exclude_features: Features to exclude from detection
        """
        self.alpha = config.get("ema_alpha", 0.1)
        self.z_threshold = config.get("z_score_threshold", 3.0)
        self.feature_ratio_threshold = config.get("feature_ratio_threshold", 0.2)
        self.min_features = config.get("min_features_monitored", 5)
        self.cooldown_minutes = config.get("cooldown_minutes", 30)
        self.exclude_features = set(config.get("exclude_features", []))

        # EMA state
        self.ema = {}  # feature_key -> EMA value
        self.ema_var = {}  # feature_key -> EMA variance

        # Drift tracking
        self.last_drift_trigger: Optional[datetime] = None
        self.drift_count = 0

        logger.info(
            f"Initialized EMADriftDetector: alpha={self.alpha}, "
            f"z_threshold={self.z_threshold}, feature_ratio={self.feature_ratio_threshold}"
        )

    def update(self, feature_key: str, value: float) -> dict:
        """
        Update EMA and detect drift for single feature

        Args:
            feature_key: Feature identifier
            value: Current feature value

        Returns:
            Dict with drift metrics
        """
        # Check if excluded
        if feature_key in self.exclude_features:
            return {"drift_detected": False, "excluded": True}

        # Initialize if first observation
        if feature_key not in self.ema:
            self.ema[feature_key] = value
            self.ema_var[feature_key] = 0.0

            return {
                "drift_detected": False,
                "initialized": True,
                "z_score": 0.0,
            }

        # Update EMA
        old_ema = self.ema[feature_key]
        self.ema[feature_key] = self.alpha * value + (1 - self.alpha) * old_ema

        # Update EMA variance (Welford's online algorithm)
        deviation = value - old_ema
        self.ema_var[feature_key] = (
            self.alpha * (deviation**2) + (1 - self.alpha) * self.ema_var[feature_key]
        )

        # Calculate z-score
        std = (
            math.sqrt(self.ema_var[feature_key])
            if self.ema_var[feature_key] > 0
            else 1e-6
        )
        z_score = abs(value - old_ema) / std

        drift_detected = z_score > self.z_threshold

        return {
            "drift_detected": drift_detected,
            "z_score": z_score,
            "ema": self.ema[feature_key],
            "std": std,
            "value": value,
        }

    def update_batch(self, features: dict[str, float]) -> dict[str, dict]:
        """
        Update EMA for multiple features

        Args:
            features: Dict of feature_key -> value

        Returns:
            Dict of feature_key -> drift metrics
        """
        results = {}

        for feature_key, value in features.items():
            results[feature_key] = self.update(feature_key, value)

        return results

    def should_trigger_retrain(self, drift_status: dict[str, dict]) -> bool:
        """
        Determine if drift warrants retraining

        Args:
            drift_status: Dict of feature_key -> drift metrics

        Returns:
            True if sufficient features are drifting
        """
        # Check cooldown
        if self.last_drift_trigger:
            elapsed = (datetime.now() - self.last_drift_trigger).total_seconds() / 60
            if elapsed < self.cooldown_minutes:
                logger.debug(
                    f"Drift retrain in cooldown ({elapsed:.1f}min < {self.cooldown_minutes}min)"
                )
                return False

        # Count drifting features
        drifted_count = 0
        total_features = 0

        for feature_key, status in drift_status.items():
            if status.get("excluded", False):
                continue

            if status.get("initialized", False):
                continue

            total_features += 1

            if status.get("drift_detected", False):
                drifted_count += 1

        # Check minimum features
        if total_features < self.min_features:
            logger.debug(
                f"Insufficient features for drift detection: {total_features} < {self.min_features}"
            )
            return False

        # Check ratio
        drift_ratio = drifted_count / total_features

        if drift_ratio >= self.feature_ratio_threshold:
            self.last_drift_trigger = datetime.now()
            self.drift_count += 1

            logger.info(
                f"Drift trigger #{self.drift_count}: "
                f"{drifted_count}/{total_features} features drifting ({drift_ratio:.1%})"
            )
            return True

        return False

    def get_drift_status(self, feature_key: str) -> dict:
        """
        Get current drift status for a feature

        Args:
            feature_key: Feature identifier

        Returns:
            Dict with EMA, std, and other metrics
        """
        if feature_key not in self.ema:
            return {"available": False}

        return {
            "available": True,
            "ema": self.ema[feature_key],
            "std": math.sqrt(self.ema_var.get(feature_key, 0)),
            "z_threshold": self.z_threshold,
        }

    def get_all_drift_status(self) -> dict[str, dict]:
        """
        Get drift status for all features

        Returns:
            Dict of feature_key -> status
        """
        return {key: self.get_drift_status(key) for key in self.ema.keys()}

    def get_drift_ratio(self, drift_status: dict[str, dict]) -> float:
        """
        Calculate current drift ratio

        Args:
            drift_status: Dict of feature_key -> drift metrics

        Returns:
            Fraction of features drifting
        """
        drifted = 0
        total = 0

        for status in drift_status.values():
            if status.get("excluded") or status.get("initialized"):
                continue

            total += 1
            if status.get("drift_detected"):
                drifted += 1

        if total == 0:
            return 0.0

        return drifted / total

    def reset(self, feature_key: Optional[str] = None):
        """
        Reset EMA state

        Args:
            feature_key: Optional specific feature to reset (None for all)
        """
        if feature_key:
            if feature_key in self.ema:
                del self.ema[feature_key]
            if feature_key in self.ema_var:
                del self.ema_var[feature_key]
            logger.debug(f"Reset drift detection for {feature_key}")
        else:
            self.ema = {}
            self.ema_var = {}
            self.last_drift_trigger = None
            self.drift_count = 0
            logger.info("Reset all drift detection state")
