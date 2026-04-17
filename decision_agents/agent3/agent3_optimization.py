"""
Agent 3: Resource Optimization
================================
Standalone agent. Feed it dummy data from dummy_forecast.json and run.

Architecture:
  dummy_forecast.json  (or Redis stream: stream:prediction:complete)
        │
  OptimizationConfig   ← thresholds
        │
  Four-Branch Rule Engine
  ┌─────┬───────────┬─────────┬──────┐
scale_up  scale_down  retrain  hold
        │
  ReplicaCalculator   ← formula
        │
  ScalingDecision (printed to terminal + saved to action_log.json)

Run:
  python agent3_optimization.py
  python agent3_optimization.py --forecast dummy_forecast.json
  python agent3_optimization.py --forecast dummy_forecast.json --current-replicas 4
  python agent3_optimization.py --forecast high_load.json --current-replicas 2
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

# ─────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("agent3.optimization")


# ─────────────────────────────────────────────
# Enums and Data Models
# ─────────────────────────────────────────────

class ScalingAction(str, Enum):
    SCALE_UP   = "scale_up"
    SCALE_DOWN = "scale_down"
    HOLD       = "hold"
    RETRAIN    = "retrain"


class RiskLevel(str, Enum):
    LOW      = "LOW"
    MEDIUM   = "MEDIUM"
    HIGH     = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class ScalingDecision:
    recommended_action:    str
    current_replicas:      int
    recommended_replicas:  int
    confidence:            float
    cpu_forecast_p50_5m:   float
    cpu_forecast_p90_5m:   float
    cpu_forecast_p90_15m:  float
    memory_forecast_p50_5m:  float
    memory_forecast_p90_5m:  float
    memory_forecast_p90_15m: float
    throttle_risk_level:   str
    oom_risk_level:        str
    throttle_prob:         float
    oom_prob:              float
    reason:                str
    namespace:             str
    pod:                   str
    container:             str
    timestamp:             str


# ─────────────────────────────────────────────
# Optimization Configuration
# ─────────────────────────────────────────────

class OptimizationConfig:
    # Branch 1 — scale_up trigger (either risk level is HIGH or CRITICAL)
    RISK_LEVELS_HIGH       = {RiskLevel.HIGH.value, RiskLevel.CRITICAL.value}

    # Branch 2 — scale_down: both risks are LOW and confidence is sufficient
    SCALE_DOWN_MIN_CONF    = 0.65

    # Branch 3 — retrain: model confidence is too low to trust
    RETRAIN_CONF_THRESHOLD = 0.30

    # Replica calculation: target workload utilization
    TARGET_UTILIZATION     = 0.70

    # Hard safety caps
    MAX_SCALE_UP_FACTOR    = 3.0   # won't recommend more than 3x current replicas
    MIN_REPLICAS           = 1


# ─────────────────────────────────────────────
# Replica Calculator
# ─────────────────────────────────────────────

class ReplicaCalculator:
    """
    Computes recommended replica count based on forecast pressure
    and current replica count.
    """

    def __init__(self, config: OptimizationConfig = OptimizationConfig()):
        self.cfg = config

    def compute_scale_up(self, current: int, cpu_p90: float, mem_p90: float) -> int:
        """
        Scale up: drive utilization toward TARGET_UTILIZATION.
        pressure / target_utilization gives us the ideal scale factor.
        """
        pressure = max(cpu_p90, mem_p90)
        if pressure > 0:
            scale_factor = pressure / self.cfg.TARGET_UTILIZATION
            raw = math.ceil(current * scale_factor)
        else:
            raw = current + 1

        # Always scale up at least 1 replica
        raw = max(current + 1, raw)

        # Hard cap: don't jump more than MAX_SCALE_UP_FACTOR times
        capped = min(raw, math.floor(current * self.cfg.MAX_SCALE_UP_FACTOR))
        result = max(current + 1, capped)

        logger.info(
            f"  [REPLICA CALC] scale_up: pressure={pressure:.2f}, "
            f"factor={pressure / self.cfg.TARGET_UTILIZATION:.2f}x, "
            f"{current} -> {result}"
        )
        return result

    def compute_scale_down(self, current: int, cpu_p90: float, mem_p90: float) -> int:
        """
        Scale down: reduce toward TARGET_UTILIZATION.
        """
        pressure = max(cpu_p90, mem_p90)
        if pressure > 0:
            scale_factor = pressure / self.cfg.TARGET_UTILIZATION
            raw = math.floor(current * scale_factor)
        else:
            raw = current - 1

        result = max(self.cfg.MIN_REPLICAS, raw)

        logger.info(
            f"  [REPLICA CALC] scale_down: pressure={pressure:.2f}, "
            f"factor={pressure / self.cfg.TARGET_UTILIZATION:.2f}x, "
            f"{current} -> {result}"
        )
        return result


# ─────────────────────────────────────────────
# Four-Branch Rule Engine
# ─────────────────────────────────────────────

class OptimizationEngine:
    """
    Core decision logic: reads pre-computed risk signals from Agent 2
    and applies the four-branch conditional rule.

    Branch 1 — SCALE_UP:   throttle_risk OR oom_risk is HIGH/CRITICAL
    Branch 2 — SCALE_DOWN: both risks are LOW + confidence is sufficient
    Branch 3 — RETRAIN:    confidence below retrain threshold (model drift)
    Branch 4 — HOLD:       none of the above
    """

    def __init__(self, config: OptimizationConfig = OptimizationConfig()):
        self.cfg        = config
        self.calculator = ReplicaCalculator(config)

    def decide(self, forecast: dict) -> tuple[ScalingAction, int, str]:
        """
        Returns (action, recommended_replicas, reason).
        """
        confidence          = float(forecast.get("confidence", 1.0))
        current_replicas    = int(forecast.get("current_replicas", 1))
        throttle_risk_level = forecast.get("throttle_risk_level", "LOW")
        oom_risk_level      = forecast.get("oom_risk_level", "LOW")
        throttle_prob       = float(forecast.get("throttle_prob", 0.0))
        oom_prob            = float(forecast.get("oom_prob", 0.0))

        # Parse the nested forecast dicts (Agent 2 stringifies them)
        cpu_forecast = _parse_forecast(forecast.get("cpu_forecast_json", "{}"))
        mem_forecast = _parse_forecast(forecast.get("memory_forecast_json", "{}"))
        cpu_p90_15m  = _get_quantile(cpu_forecast, horizon="15", quantile="0.9")
        mem_p90_15m  = _get_quantile(mem_forecast, horizon="15", quantile="0.9")

        logger.info(
            f"  confidence={confidence:.2f} | "
            f"throttle_risk={throttle_risk_level} (prob={throttle_prob:.2f}) | "
            f"oom_risk={oom_risk_level} (prob={oom_prob:.2f})"
        )
        logger.info(
            f"  cpu_p90_15m={cpu_p90_15m:.2f} | mem_p90_15m={mem_p90_15m:.2f} | "
            f"current_replicas={current_replicas}"
        )

        # ── Branch 3 first: retrain if model is unreliable ───────────────
        if confidence < self.cfg.RETRAIN_CONF_THRESHOLD:
            reason = (
                f"Model confidence critically low ({confidence:.2f} < "
                f"{self.cfg.RETRAIN_CONF_THRESHOLD}). "
                f"Triggering retraining signal. No scaling action taken."
            )
            logger.info(f"  -> Branch 3: RETRAIN -- {reason}")
            return ScalingAction.RETRAIN, current_replicas, reason

        # ── Branch 1: scale_up if either risk is HIGH or CRITICAL ────────
        if (throttle_risk_level in self.cfg.RISK_LEVELS_HIGH or
                oom_risk_level in self.cfg.RISK_LEVELS_HIGH):

            recommended = self.calculator.compute_scale_up(
                current_replicas, cpu_p90_15m, mem_p90_15m
            )
            parts = []
            if throttle_risk_level in self.cfg.RISK_LEVELS_HIGH:
                parts.append(
                    f"CPU throttle risk is {throttle_risk_level} "
                    f"(p90={cpu_p90_15m:.0%} of limit)"
                )
            if oom_risk_level in self.cfg.RISK_LEVELS_HIGH:
                parts.append(
                    f"Memory OOM risk is {oom_risk_level} "
                    f"(p90={mem_p90_15m:.0%} of limit)"
                )
            reason = ". ".join(parts) + (
                f". Scaling from {current_replicas} -> {recommended} replicas "
                f"to maintain <={self.cfg.TARGET_UTILIZATION:.0%} utilization."
            )
            logger.info(f"  -> Branch 1: SCALE_UP -- {reason}")
            return ScalingAction.SCALE_UP, recommended, reason

        # ── Branch 2: scale_down if both risks are LOW + confident ───────
        if (throttle_risk_level == RiskLevel.LOW.value and
                oom_risk_level == RiskLevel.LOW.value and
                confidence >= self.cfg.SCALE_DOWN_MIN_CONF):

            recommended = self.calculator.compute_scale_down(
                current_replicas, cpu_p90_15m, mem_p90_15m
            )
            if recommended < current_replicas:
                reason = (
                    f"Both CPU ({throttle_risk_level}) and memory ({oom_risk_level}) "
                    f"risks are LOW with confidence={confidence:.2f}. "
                    f"Scaling down from {current_replicas} -> {recommended} replicas "
                    f"to recover unused capacity."
                )
                logger.info(f"  -> Branch 2: SCALE_DOWN -- {reason}")
                return ScalingAction.SCALE_DOWN, recommended, reason
            else:
                # Scale-down formula returned same or higher — hold instead
                reason = (
                    f"Risks are LOW but current replica count ({current_replicas}) "
                    f"is already at minimum or optimal capacity. Holding."
                )
                logger.info(f"  -> Branch 4 (via Branch 2): HOLD -- {reason}")
                return ScalingAction.HOLD, current_replicas, reason

        # ── Branch 4: hold (moderate risk or borderline confidence) ──────
        reason = (
            f"Resource pressure is moderate (throttle={throttle_risk_level}, "
            f"oom={oom_risk_level}) and confidence={confidence:.2f}. "
            f"No scaling action required. Maintaining {current_replicas} replicas."
        )
        logger.info(f"  -> Branch 4: HOLD -- {reason}")
        return ScalingAction.HOLD, current_replicas, reason


# ─────────────────────────────────────────────
# Main Optimization Agent
# ─────────────────────────────────────────────

class ResourceOptimizationAgent:

    def __init__(self):
        self.config = OptimizationConfig()
        self.engine = OptimizationEngine(self.config)

    def run(self, forecast: dict) -> ScalingDecision:
        logger.info("=" * 60)
        logger.info("Agent 3: Resource Optimization -- Starting")
        logger.info("=" * 60)
        logger.info(
            f"Input: pod={forecast.get('pod')} | "
            f"namespace={forecast.get('namespace')} | "
            f"current_replicas={forecast.get('current_replicas')}"
        )
        logger.info("")

        # ── Step 1: Four-Branch Decision ─────────────────────────────────
        logger.info("Step 1: Applying Four-Branch Optimization Rule...")
        action, recommended_replicas, reason = self.engine.decide(forecast)

        # ── Step 2: Extract forecast values for downstream (Agent 4) ─────
        cpu_forecast = _parse_forecast(forecast.get("cpu_forecast_json", "{}"))
        mem_forecast = _parse_forecast(forecast.get("memory_forecast_json", "{}"))

        decision = ScalingDecision(
            recommended_action     = action.value,
            current_replicas       = int(forecast.get("current_replicas", 1)),
            recommended_replicas   = recommended_replicas,
            confidence             = float(forecast.get("confidence", 0.0)),
            cpu_forecast_p50_5m    = _get_quantile(cpu_forecast, "5",  "0.5"),
            cpu_forecast_p90_5m    = _get_quantile(cpu_forecast, "5",  "0.9"),
            cpu_forecast_p90_15m   = _get_quantile(cpu_forecast, "15", "0.9"),
            memory_forecast_p50_5m = _get_quantile(mem_forecast, "5",  "0.5"),
            memory_forecast_p90_5m = _get_quantile(mem_forecast, "5",  "0.9"),
            memory_forecast_p90_15m= _get_quantile(mem_forecast, "15", "0.9"),
            throttle_risk_level    = forecast.get("throttle_risk_level", "LOW"),
            oom_risk_level         = forecast.get("oom_risk_level", "LOW"),
            throttle_prob          = float(forecast.get("throttle_prob", 0.0)),
            oom_prob               = float(forecast.get("oom_prob", 0.0)),
            reason                 = reason,
            namespace              = forecast.get("namespace", ""),
            pod                    = forecast.get("pod", ""),
            container              = forecast.get("container", ""),
            timestamp              = datetime.now(timezone.utc).isoformat(),
        )

        return decision

    def to_agent4_payload(self, decision: ScalingDecision) -> dict:
        """
        Convert ScalingDecision → the exact JSON format Agent 4 expects.
        This is the payload written to Redis / passed directly to Agent 4.
        """
        return {
            "namespace":              decision.namespace,
            "pod":                    decision.pod,
            "container":              decision.container,
            "timestamp":              decision.timestamp,
            "cpu_forecast_p50_5m":    round(decision.cpu_forecast_p50_5m,  4),
            "cpu_forecast_p90_5m":    round(decision.cpu_forecast_p90_5m,  4),
            "cpu_forecast_p90_15m":   round(decision.cpu_forecast_p90_15m, 4),
            "memory_forecast_p50_5m": round(decision.memory_forecast_p50_5m,  4),
            "memory_forecast_p90_5m": round(decision.memory_forecast_p90_5m,  4),
            "memory_forecast_p90_15m":round(decision.memory_forecast_p90_15m, 4),
            "confidence":             round(decision.confidence, 4),
            "current_replicas":       decision.current_replicas,
            "recommended_replicas":   decision.recommended_replicas,
            "recommended_action":     decision.recommended_action,
            "reason":                 decision.reason,
        }


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _parse_forecast(raw) -> dict:
    """Parse cpu_forecast_json / memory_forecast_json — handles both str and dict."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(f"Could not parse forecast JSON: {raw[:80]}")
    return {}


def _get_quantile(forecast: dict, horizon: str, quantile: str) -> float:
    """Safe lookup: forecast[horizon][quantile] with default 0.0."""
    return float(forecast.get(horizon, {}).get(quantile, 0.0))


# ─────────────────────────────────────────────
# Pretty Print
# ─────────────────────────────────────────────

def print_decision(decision: ScalingDecision, agent4_payload: dict):
    action_icons = {
        ScalingAction.SCALE_UP.value:   "[^] SCALE UP",
        ScalingAction.SCALE_DOWN.value: "[v] SCALE DOWN",
        ScalingAction.HOLD.value:       "[=] HOLD",
        ScalingAction.RETRAIN.value:    "[~] RETRAIN",
    }

    print("\n" + "=" * 60)
    print("  OPTIMIZATION DECISION")
    print("=" * 60)
    print(f"  Action:            {action_icons.get(decision.recommended_action, decision.recommended_action)}")
    print(f"  Current replicas:  {decision.current_replicas}")
    print(f"  Recommended:       {decision.recommended_replicas}")
    print(f"  Confidence:        {decision.confidence:.2f}")
    print("-" * 60)
    print(f"  CPU  p90 (5m):    {decision.cpu_forecast_p90_5m:.2f}")
    print(f"  CPU  p90 (15m):   {decision.cpu_forecast_p90_15m:.2f}  "
          f"[Throttle risk: {decision.throttle_risk_level} | prob={decision.throttle_prob:.2f}]")
    print(f"  Mem  p90 (5m):    {decision.memory_forecast_p90_5m:.2f}")
    print(f"  Mem  p90 (15m):   {decision.memory_forecast_p90_15m:.2f}  "
          f"[OOM risk:      {decision.oom_risk_level} | prob={decision.oom_prob:.2f}]")
    print("-" * 60)
    print(f"  Reason: {decision.reason}")
    print("=" * 60)
    print(f"  Timestamp: {decision.timestamp}")
    print("=" * 60)
    print()
    print("  -> Agent 4 Payload:")
    print(json.dumps(agent4_payload, indent=4))
    print("=" * 60 + "\n")


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

async def run_redis_mode(redis_host: str = "localhost", redis_port: int = 6380):
    import sys
    import os
    # Add repo root to path to import src
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from src.storage.redis_store import RedisStore

    logger.info(f"Connecting to Redis at {redis_host}:{redis_port}...")
    redis_store = RedisStore(host=redis_host, port=redis_port)
    agent = ResourceOptimizationAgent()

    logger.info("Agent 3: Listening on stream:prediction:complete...")
    last_id = "$"  # Read only new messages
    
    try:
        while True:
            messages = await redis_store.read_stream_messages(
                stream_name="stream:prediction:complete",
                last_id=last_id,
                block_ms=5000,
                count=10,
            )
            for msg_id, msg in messages:
                last_id = msg_id

                # ── Unpack Agent 2's nested forecast_json blob ─────────────────
                # Agent 2 writes: { "namespace": ..., "forecast_json": "<json>" }
                # forecast_json inner keys: cpu_forecast, memory_forecast,
                #   throttle_risk (dict), oom_risk (dict), throttle_prob,
                #   oom_prob, confidence
                forecast = {
                    "namespace":        msg.get("namespace", ""),
                    "pod":              msg.get("pod", ""),
                    "container":        msg.get("container", ""),
                    "current_replicas": 1,   # default; future: query K8s API
                }

                raw = msg.get("forecast_json", "{}")
                try:
                    inner = json.loads(raw)
                except json.JSONDecodeError:
                    logger.error(
                        f"Invalid forecast_json in msg {msg_id}: {raw[:120]}"
                    )
                    continue

                # Map Agent 2 output fields → Agent 3 expected field names
                forecast["cpu_forecast_json"]    = json.dumps(inner.get("cpu_forecast", {}))
                forecast["memory_forecast_json"] = json.dumps(inner.get("memory_forecast", {}))
                forecast["throttle_prob"]        = float(inner.get("throttle_prob", 0.0))
                forecast["oom_prob"]             = float(inner.get("oom_prob", 0.0))
                forecast["confidence"]           = float(inner.get("confidence", 1.0))

                # Agent 2 stores risk level under different nested keys
                throttle_risk = inner.get("throttle_risk", {})
                oom_risk      = inner.get("oom_risk", {})
                forecast["throttle_risk_level"] = throttle_risk.get("risk_level", "LOW")
                forecast["oom_risk_level"]      = oom_risk.get("oom_risk", "LOW")

                logger.info(f"--- Received Prediction Event {msg_id} ---")
                logger.info(
                    f"  Unpacked: throttle={forecast['throttle_risk_level']} "
                    f"oom={forecast['oom_risk_level']} "
                    f"conf={forecast['confidence']:.2f}"
                )

                # Run optimization
                decision       = agent.run(forecast)
                agent4_payload = agent.to_agent4_payload(decision)

                # Print to terminal
                print_decision(decision, agent4_payload)

                # Publish to next stream
                # Redis Streams require all field values to be strings
                out_id = await redis_store.write_stream_message(
                    stream_name="stream:optimization:complete",
                    payload={k: str(v) for k, v in agent4_payload.items()}
                )
                logger.info(f"Published decision to stream:optimization:complete (ID {out_id})\n")
                
    except KeyboardInterrupt:
        logger.info("Stopping Agent 3 Redis loop.")
    finally:
        await redis_store.close()


def main():
    import asyncio
    
    parser = argparse.ArgumentParser(
        description="Agent 3: Resource Optimization"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["file", "redis"],
        default="file",
        help="Run mode: 'file' for standalone testing, 'redis' for live stream listening",
    )
    parser.add_argument(
        "--forecast",
        type=str,
        default="dummy_forecast.json",
        help="(File mode) Path to Agent 2's forecast JSON file (default: dummy_forecast.json)",
    )
    parser.add_argument(
        "--current-replicas",
        type=int,
        default=None,
        help="(File mode) Override current replica count",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="action_log.json",
        help="(File mode) Path to write output (default: action_log.json)",
    )
    parser.add_argument(
        "--redis-host",
        type=str,
        default="localhost",
        help="Redis host",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6380,  # Based on mock_ingestion.py default
        help="Redis port",
    )
    args = parser.parse_args()

    if args.mode == "redis":
        asyncio.run(run_redis_mode(redis_host=args.redis_host, redis_port=args.redis_port))
    else:
        # Load forecast
        forecast_path = Path(args.forecast)
        if not forecast_path.exists():
            print(f"Error: Forecast file not found: {forecast_path}")
            exit(1)

        with open(forecast_path) as f:
            forecast = json.load(f)

        if args.current_replicas is not None:
            forecast["current_replicas"] = args.current_replicas
            logger.info(f"Overriding current_replicas -> {args.current_replicas}")

        agent          = ResourceOptimizationAgent()
        decision       = agent.run(forecast)
        agent4_payload = agent.to_agent4_payload(decision)

        print_decision(decision, agent4_payload)

        output_path = Path(args.output)
        with open(output_path, "w") as f:
            json.dump(asdict(decision), f, indent=2)
        logger.info(f"Decision saved to: {output_path}")

        agent4_path = Path(args.output).parent / "agent4_ready_payload.json"
        with open(agent4_path, "w") as f:
            json.dump(agent4_payload, f, indent=2)
        logger.info(f"Agent 4 payload saved to: {agent4_path}")


if __name__ == "__main__":
    main()
