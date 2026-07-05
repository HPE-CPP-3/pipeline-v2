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
        self.horizon = config.get("horizon", 5)

        logger.info(f"Initialized ThrottleRiskCalculator with target horizon={self.horizon}m")

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

        # Get p90 forecast for target and other horizons
        p90_target = cpu_forecast.get(self.horizon, {}).get(0.9, 0.0)
        p90_5min = cpu_forecast.get(5, {}).get(0.9, 0.0)
        p90_10min = cpu_forecast.get(10, {}).get(0.9, 0.0)
        p90_15min = cpu_forecast.get(15, {}).get(0.9, 0.0)

        # Calculate ratios
        ratio_target = p90_target / cpu_limit
        ratio_15min = p90_15min / cpu_limit

        # Determine risk level
        risk_level = "LOW"
        will_throttle = False
        probability = 0.0
        confidence = 0.5
        time_to_throttle = None

        # Critical: p90 forecast >= 95% of limit
        if ratio_target >= self.critical_ratio:
            risk_level = "CRITICAL"
            will_throttle = True
            probability = 0.95
            confidence = 0.9
            time_to_throttle = f"<{self.horizon}min"

        # High: p90 forecast >= 85% of limit
        elif ratio_target >= self.high_ratio:
            risk_level = "HIGH"
            will_throttle = True
            probability = 0.8
            confidence = 0.8
            time_to_throttle = f"5-{self.horizon}min" if self.horizon > 5 else f"<{self.horizon}min"

        # Medium: p90 forecast >= 70% of limit OR high current throttling
        elif ratio_target >= 0.7 or (
            current_throttle_ratio
            and current_throttle_ratio > self.current_throttle_ratio_high
        ):
            risk_level = "MEDIUM"
            will_throttle = False
            probability = 0.5
            confidence = 0.7
            time_to_throttle = f"{self.horizon}+min"

        # Spike-aware escalation for near-saturation workloads
        if recent_cpu_spike and recent_cpu_spike > 10.0 and ratio_target >= 0.8:
            if risk_level == "MEDIUM":
                risk_level = "HIGH"
                will_throttle = True
                probability = max(probability, 0.8)
                confidence = max(confidence, 0.75)
                time_to_throttle = f"5-{self.horizon}min" if self.horizon > 5 else f"<{self.horizon}min"

        # Calculate confidence based on forecast spread
        if risk_level != "LOW":
            p50_target = cpu_forecast.get(self.horizon, {}).get(0.5, 0.0)
            spread = (p90_target - p50_target) / (p50_target + 1e-6)

            # Lower spread = higher confidence
            if spread < 0.1:
                confidence = min(confidence + 0.1, 1.0)
            elif spread > 0.3:
                confidence = max(confidence - 0.1, 0.3)

        # Determine reason
        reason = f"p90 forecast {p90_target:.2f} cores / {cpu_limit:.2f} limit ({ratio_target:.1%})"
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
                "ratio_target": ratio_target,
                "current_throttle_ratio": current_throttle_ratio,
                "recent_cpu_spike": recent_cpu_spike,
            },
        }

    def calculate_risk_from_model_prob(
        self,
        throttle_prob: float,
        cpu_forecast: dict[int, dict[float, float]],
        cpu_limit: float,
        current_throttle_ratio: Optional[float] = None,
        recent_cpu_spike: Optional[float] = None,
    ) -> dict:
        """
        Option A two-layer calculation: uses the model's learned throttle probability
        as an early-warning signal, then enriches it with rule-based context
        from the actual CPU limit.

        The model probability gates whether we escalate the rule-based level,
        and is surfaced directly so operators can see both signals.

        Args:
            throttle_prob: Sigmoid output of the model's throttle_logit (0-1).
                           High values mean the model learned a risk pattern from
                           the historical sequence, even before thresholds are breached.
            cpu_forecast: Dict of horizon -> quantile -> value (de-normalized, in cores)
            cpu_limit: CPU limit in cores (from K8s resource limits, pre-normalization)
            current_throttle_ratio: Current throttling ratio (optional)
            recent_cpu_spike: Recent CPU spike magnitude (optional)

        Returns:
            Dict with all fields from calculate_risk(), plus:
                - model_throttle_prob: float — raw model probability
                - source: str — "model+rules" to distinguish from pure rule-based
        """
        # Start with the rule-based analysis
        result = self.calculate_risk(
            cpu_forecast=cpu_forecast,
            cpu_limit=cpu_limit,
            current_throttle_ratio=current_throttle_ratio,
            recent_cpu_spike=recent_cpu_spike,
        )

        # Blend model probability into the result.
        # The model acts as an early-warning signal: if it's highly confident
        # but the forecast hasn't breached thresholds yet, we escalate.
        if cpu_limit > 0:
            rule_prob = result["probability"]

            # Weighted blend: model gets 40% weight, rules get 60%
            # Rules are more reliable when limits are known; model catches temporal patterns
            if throttle_prob > 0.1:
                blended_prob = 0.4 * throttle_prob + 0.6 * rule_prob
            else:
                blended_prob = rule_prob  # model hasn't learned this pattern yet, trust rules
            result["probability"] = round(blended_prob, 4)

            # Model-driven escalation: if the model is very confident (>0.85)
            # but rules only say LOW/MEDIUM, bump up one level as a heads-up.
            if throttle_prob >= 0.85 and result["risk_level"] == "LOW":
                result["risk_level"] = "MEDIUM"
                result["will_throttle"] = False
                if result["time_to_throttle"] is None:
                    result["time_to_throttle"] = "15+min"
                result["reason"] += f" [model early-warning: prob={throttle_prob:.2f}]"

            elif throttle_prob >= 0.85 and result["risk_level"] == "MEDIUM":
                result["risk_level"] = "HIGH"
                result["will_throttle"] = True
                if result["time_to_throttle"] in (None, "15+min"):
                    result["time_to_throttle"] = "10-15min"
                result["reason"] += f" [model escalation: prob={throttle_prob:.2f}]"

        result["model_throttle_prob"] = round(throttle_prob, 4)
        result["source"] = "model+rules"
        return result