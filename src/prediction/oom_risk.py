"""
Memory OOM risk calculation (derived from forecasts)
"""

from typing import Optional
import logging

logger = logging.getLogger(__name__)


class OOMRiskCalculator:
    """
    Calculate memory OOM risk from forecasts

    Risk factors:
    - Memory usage vs limit ratio
    - Working set growth
    - OOM fail count
    """

    def __init__(self, config: dict):
        """
        Args:
            config: Configuration with thresholds:
                - critical_ratio: p90 forecast / limit for critical risk
                - high_ratio: p90 forecast / limit for high risk
                - failcnt_threshold: Memory fail count threshold
                - growth_rate_high: Working set growth rate considered high
        """
        self.critical_ratio = config.get("critical_ratio", 0.95)
        self.high_ratio = config.get("high_ratio", 0.85)
        self.failcnt_threshold = config.get("failcnt_threshold", 1)
        self.growth_rate_high = config.get("growth_rate_high", 0.1)

        logger.info("Initialized OOMRiskCalculator")

    def calculate_risk(
        self,
        memory_forecast: dict[int, dict[float, float]],
        memory_limit: float,
        current_failcnt: int = 0,
        working_set_growth_rate: Optional[float] = None,
        failcnt_spike: Optional[float] = None,
    ) -> dict:
        """
        Calculate OOM risk

        Args:
            memory_forecast: Dict of horizon -> quantile -> value
            memory_limit: Memory limit in bytes
            current_failcnt: Current memory fail count
            working_set_growth_rate: Working set growth rate per minute (optional)

        Returns:
            Dict with:
                - oom_risk: str (LOW, MEDIUM, HIGH, CRITICAL)
                - probability: float (0-1)
                - estimated_time: str or None
                - reason: str
        """
        if memory_limit <= 0:
            return {
                "oom_risk": "UNKNOWN",
                "probability": 0.0,
                "estimated_time": None,
                "reason": "No memory limit set",
            }

        # Get p90 forecast for 15min horizon
        p90_15min = memory_forecast.get(15, {}).get(0.9, 0.0)
        p90_10min = memory_forecast.get(10, {}).get(0.9, 0.0)
        p90_5min = memory_forecast.get(5, {}).get(0.9, 0.0)

        # Calculate ratios
        ratio_15min = p90_15min / memory_limit
        ratio_10min = p90_10min / memory_limit
        ratio_5min = p90_5min / memory_limit

        # Determine risk level
        oom_risk = "LOW"
        probability = 0.0
        estimated_time = None

        # Critical: p90 forecast >= 95% of limit OR recent OOM
        if (
            ratio_15min >= self.critical_ratio
            or current_failcnt >= self.failcnt_threshold
            or (failcnt_spike is not None and failcnt_spike > 0)
        ):
            oom_risk = "CRITICAL"
            probability = 0.95
            estimated_time = "5-15min"

        # High: p90 forecast >= 85% of limit OR high growth rate
        elif ratio_15min >= self.high_ratio or (
            working_set_growth_rate and working_set_growth_rate > self.growth_rate_high
        ):
            oom_risk = "HIGH"
            probability = 0.8
            estimated_time = "10-15min"

        # Medium: p90 forecast >= 70% of limit
        elif ratio_15min >= 0.7:
            oom_risk = "MEDIUM"
            probability = 0.5
            estimated_time = "15+min"

        # Calculate estimated time more precisely
        if oom_risk in ["CRITICAL", "HIGH"]:
            # Linear extrapolation
            current_usage = memory_forecast.get(5, {}).get(0.5, p90_5min)
            growth_per_min = (p90_15min - current_usage) / 10

            if growth_per_min > 0:
                remaining = memory_limit - current_usage
                minutes_to_oom = remaining / growth_per_min

                if minutes_to_oom < 5:
                    estimated_time = "<5min"
                elif minutes_to_oom < 15:
                    estimated_time = f"{int(minutes_to_oom)}min"
                else:
                    estimated_time = f"{int(minutes_to_oom)}min"

        # Determine reason
        reason = (
            f"p90 forecast {p90_15min:.0f} / {memory_limit:.0f} ({ratio_15min:.1%})"
        )
        if current_failcnt > 0:
            reason += f", failcnt={current_failcnt}"
        if working_set_growth_rate and working_set_growth_rate > self.growth_rate_high:
            reason += f", growth rate {working_set_growth_rate:.1%}/min"
        if failcnt_spike and failcnt_spike > 0:
            reason += f", failcnt spike={failcnt_spike:.0f}"

        return {
            "oom_risk": oom_risk,
            "probability": probability,
            "estimated_time": estimated_time,
            "reason": reason,
            "metrics": {
                "p90_5min": p90_5min,
                "p90_10min": p90_10min,
                "p90_15min": p90_15min,
                "ratio_15min": ratio_15min,
                "current_failcnt": current_failcnt,
                "working_set_growth_rate": working_set_growth_rate,
                "failcnt_spike": failcnt_spike,
            },
        }

    def calculate_risk_from_model_prob(
        self,
        oom_prob: float,
        memory_forecast: dict[int, dict[float, float]],
        memory_limit: float,
        current_failcnt: int = 0,
        working_set_growth_rate: Optional[float] = None,
        failcnt_spike: Optional[float] = None,
    ) -> dict:
        """
        Option A two-layer calculation: uses the model's learned OOM probability
        as an early-warning signal, then enriches it with rule-based context
        from the actual memory limit.

        Args:
            oom_prob: Sigmoid output of the model's oom_logit (0-1).
                      High values mean the model detected a risk pattern from
                      the historical sequence.
            memory_forecast: Dict of horizon -> quantile -> value (de-normalized, in bytes)
            memory_limit: Memory limit in bytes (from K8s resource limits, pre-normalization)
            current_failcnt: Current memory fail count
            working_set_growth_rate: Working set growth rate per minute (optional)
            failcnt_spike: Recent spike in fail count (optional)

        Returns:
            Dict with all fields from calculate_risk(), plus:
                - model_oom_prob: float — raw model probability
                - source: str — "model+rules" to distinguish from pure rule-based
        """
        # Start with the rule-based analysis
        result = self.calculate_risk(
            memory_forecast=memory_forecast,
            memory_limit=memory_limit,
            current_failcnt=current_failcnt,
            working_set_growth_rate=working_set_growth_rate,
            failcnt_spike=failcnt_spike,
        )

        # Blend model probability into the result.
        if memory_limit > 0:
            rule_prob = result["probability"]

            # Weighted blend: model gets 40%, rules get 60%
            if oom_prob > 0.1:
                blended_prob = 0.4 * oom_prob + 0.6 * rule_prob
            else:
                blended_prob = rule_prob
            result["probability"] = round(blended_prob, 4)

            # Model-driven escalation: if model is very confident but rules
            # haven't caught up yet (temporal patterns before threshold breach).
            if oom_prob >= 0.85 and result["oom_risk"] == "LOW":
                result["oom_risk"] = "MEDIUM"
                if result["estimated_time"] is None:
                    result["estimated_time"] = "15+min"
                result["reason"] += f" [model early-warning: prob={oom_prob:.2f}]"

            elif oom_prob >= 0.85 and result["oom_risk"] == "MEDIUM":
                result["oom_risk"] = "HIGH"
                if result["estimated_time"] in (None, "15+min"):
                    result["estimated_time"] = "10-15min"
                result["reason"] += f" [model escalation: prob={oom_prob:.2f}]"

        result["model_oom_prob"] = round(oom_prob, 4)
        result["source"] = "model+rules"
        return result