"""
CPU throttle risk calculation (derived from forecasts)
"""

from typing import Optional
import logging

logger = logging.getLogger(__name__)


class ThrottleRiskCalculator:
    """
    Calculate CPU throttle risk from forecasts

    Risk factors:
    - CPU usage vs limit ratio
    - Throttling ratio
    - Recent spikes
    """

    def __init__(self, config: dict):
        """
        Args:
            config: Configuration with thresholds:
                - critical_ratio: p90 forecast / limit for critical risk
                - high_ratio: p90 forecast / limit for high risk
                - current_throttle_ratio_high: Current throttling considered risky
        """
        self.critical_ratio = config.get("critical_ratio", 0.95)
        self.high_ratio = config.get("high_ratio", 0.85)
        self.current_throttle_ratio_high = config.get(
            "current_throttle_ratio_high", 0.1
        )

        logger.info("Initialized ThrottleRiskCalculator")

    def calculate_risk(
        self,
        cpu_forecast: dict[int, dict[float, float]],
        cpu_limit: float,
        current_throttle_ratio: Optional[float] = None,
        recent_cpu_spike: Optional[float] = None,
    ) -> dict:
        """
        Calculate throttle risk

        Args:
            cpu_forecast: Dict of horizon -> quantile -> value
            cpu_limit: CPU limit in cores
            current_throttle_ratio: Current throttling ratio (optional)

        Returns:
            Dict with:
                - will_throttle: bool
                - confidence: float (0-1)
                - probability: float (0-1)
                - time_to_throttle: str or None
                - risk_level: str (LOW, MEDIUM, HIGH, CRITICAL)
        """
        if cpu_limit <= 0:
            return {
                "will_throttle": False,
                "confidence": 0.0,
                "probability": 0.0,
                "time_to_throttle": None,
                "risk_level": "UNKNOWN",
                "reason": "No CPU limit set",
            }

        # Get p90 forecast for 15min horizon (most relevant)
        p90_15min = cpu_forecast.get(15, {}).get(0.9, 0.0)
        p90_10min = cpu_forecast.get(10, {}).get(0.9, 0.0)
        p90_5min = cpu_forecast.get(5, {}).get(0.9, 0.0)

        # Calculate ratios
        ratio_15min = p90_15min / cpu_limit
        ratio_10min = p90_10min / cpu_limit
        ratio_5min = p90_5min / cpu_limit

        # Determine risk level
        risk_level = "LOW"
        will_throttle = False
        probability = 0.0
        confidence = 0.5
        time_to_throttle = None

        # Critical: p90 forecast >= 95% of limit
        if ratio_15min >= self.critical_ratio:
            risk_level = "CRITICAL"
            will_throttle = True
            probability = 0.95
            confidence = 0.9
            time_to_throttle = "5-15min"

        # High: p90 forecast >= 85% of limit
        elif ratio_15min >= self.high_ratio:
            risk_level = "HIGH"
            will_throttle = True
            probability = 0.8
            confidence = 0.8
            time_to_throttle = "10-15min"

        # Medium: p90 forecast >= 70% of limit OR high current throttling
        elif ratio_15min >= 0.7 or (
            current_throttle_ratio
            and current_throttle_ratio > self.current_throttle_ratio_high
        ):
            risk_level = "MEDIUM"
            will_throttle = False
            probability = 0.5
            confidence = 0.7
            time_to_throttle = "15+min"

        # Spike-aware escalation for near-saturation workloads
        if recent_cpu_spike and recent_cpu_spike > 10.0 and ratio_15min >= 0.8:
            if risk_level == "MEDIUM":
                risk_level = "HIGH"
                will_throttle = True
                probability = max(probability, 0.8)
                confidence = max(confidence, 0.75)
                time_to_throttle = "5-15min"

        # Calculate confidence based on forecast spread
        if risk_level != "LOW":
            p50_15min = cpu_forecast.get(15, {}).get(0.5, 0.0)
            spread = (p90_15min - p50_15min) / (p50_15min + 1e-6)

            # Lower spread = higher confidence
            if spread < 0.1:
                confidence = min(confidence + 0.1, 1.0)
            elif spread > 0.3:
                confidence = max(confidence - 0.1, 0.3)

        # Determine reason
        reason = f"p90 forecast {p90_15min:.2f} cores / {cpu_limit:.2f} limit ({ratio_15min:.1%})"
        if (
            current_throttle_ratio
            and current_throttle_ratio > self.current_throttle_ratio_high
        ):
            reason += f", current throttle ratio {current_throttle_ratio:.1%}"

        return {
            "will_throttle": will_throttle,
            "confidence": confidence,
            "probability": probability,
            "time_to_throttle": time_to_throttle,
            "risk_level": risk_level,
            "reason": reason,
            "metrics": {
                "p90_5min": p90_5min,
                "p90_10min": p90_10min,
                "p90_15min": p90_15min,
                "ratio_15min": ratio_15min,
                "current_throttle_ratio": current_throttle_ratio,
                "recent_cpu_spike": recent_cpu_spike,
            },
        }
