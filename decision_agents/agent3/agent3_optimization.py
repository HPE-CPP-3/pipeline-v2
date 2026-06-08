"""
Agent 3: Resource Optimization
================================
Standalone agent. Feed it dummy data from dummy_forecast.json and run.

Target container this agent is built for:
  namespace:  test-workload
  deployment: stress-test-app
  pod:        stress-test-app-975d97b75-q7t2w   (pod name changes on restart,
                                                  we query by deployment instead)
    container:  stress-container

Architecture:
  dummy_forecast.json  (or Redis stream: stream:prediction:complete)
        │
  K8sResourceFetcher   ← queries test-workload/stress-test-app for:
        │                  cpu_request, cpu_limit
        │                  memory_request, memory_limit
        │                  current live replica count
        │                  falls back gracefully if cluster unreachable
        │                  results cached 30s to avoid API hammering
        │
  detect_qos_class()   ← derives Guaranteed / Burstable / BestEffort
        │
  OptimizationConfig   ← QoS-aware thresholds
        │
  Four-Branch Rule Engine
  ┌─────┬───────────┬─────────┬──────┐
scale_up  scale_down  retrain  hold
        │
  ReplicaCalculator    ← formula (uses QoS-aware target utilization)
        │
  ScalingDecision (printed to terminal + saved to action_log.json)
                  + agent4_ready_payload.json

Run:
  # With live cluster query (default)
  python agent3_optimization.py
  python agent3_optimization.py --forecast dummy_forecast.json

  # Override replica count for testing
  python agent3_optimization.py --forecast dummy_forecast.json --current-replicas 4

  # Skip cluster query entirely (local testing without a cluster)
  python agent3_optimization.py --forecast dummy_forecast.json --no-k8s

  # Redis mode (production)
  python agent3_optimization.py --mode redis
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import socket
import time
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


def _add_file_logger(log_path: str, encoding: str = "utf-8") -> None:
    """Mirror console logs into a file using the same format."""
    root     = logging.getLogger()
    abs_path = os.path.abspath(log_path)
    if any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == abs_path
        for h in root.handlers
    ):
        return
    fh = logging.FileHandler(abs_path, encoding=encoding)
    fh.setLevel(logging.INFO)
    fh.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )
    root.addHandler(fh)


# ─────────────────────────────────────────────
# Target workload constants
# ─────────────────────────────────────────────
# Agent 1 monitors exactly this container.
# All K8s queries in this agent are scoped to these values.
# If you ever change the monitored workload, update only here.

TARGET_NAMESPACE  = "test-workload"
TARGET_DEPLOYMENT = "stress-test-app"
TARGET_CONTAINER  = "stress-container"   # container name inside the pod spec


# ─────────────────────────────────────────────
# Redis single-instance lock (unchanged)
# ─────────────────────────────────────────────

class _RedisSingleInstanceLock:
    """
    Prevents two Agent 3 instances from consuming the same Redis stream
    simultaneously and issuing conflicting scaling decisions.
    Uses SET NX (only set if not exists) with a TTL that refreshes in the loop.
    """

    def __init__(
        self,
        redis_host: str,
        redis_port: int,
        key: str,
        ttl_seconds: int = 30,
    ):
        self.redis_host  = redis_host
        self.redis_port  = redis_port
        self.key         = key
        self.ttl_seconds = ttl_seconds
        # Unique token per host+pid so we know which process holds the lock
        self.token       = f"{socket.gethostname()}:{os.getpid()}"
        self._redis      = None

    def acquire_or_exit(self) -> None:
        import redis
        self._redis = redis.Redis(
            host=self.redis_host, port=self.redis_port, decode_responses=True
        )
        ok = self._redis.set(self.key, self.token, nx=True, ex=self.ttl_seconds)
        if not ok:
            current = self._redis.get(self.key)
            raise SystemExit(
                f"Another Agent 3 instance is already running "
                f"(lock={self.key}, holder={current}). "
                f"Stop the other instance or delete the Redis key to proceed."
            )

    def refresh(self) -> None:
        """Call inside the main loop to keep the lock alive."""
        if not self._redis:
            return
        try:
            if self._redis.get(self.key) == self.token:
                self._redis.expire(self.key, self.ttl_seconds)
        except Exception:
            pass

    def release(self) -> None:
        """Call in the finally block to clean up on exit."""
        if not self._redis:
            return
        try:
            if self._redis.get(self.key) == self.token:
                self._redis.delete(self.key)
        except Exception:
            pass


# ─────────────────────────────────────────────
# Kubernetes Resource Fetcher
# ─────────────────────────────────────────────

class K8sResourceFetcher:
    """
    Queries the Kubernetes API for the stress-test-app container's
    real cpu/memory requests and limits, and the live replica count.

    Two concerns handled here:

      LATENCY        Results are cached with a TTL (default 30s).
                     Repeated calls within the TTL window skip the API
                     call entirely and return the cached value.
                     A single K8s API call typically adds 20-50ms latency,
                     which is fine for this pipeline but we cache anyway
                     because the same pod may be processed multiple times
                     in a burst.

      GRACEFUL DEGR  Every method is wrapped in try/except.
                     If the cluster is unreachable, the package is missing,
                     or the pod no longer exists, we return zero-value
                     fallbacks so Agent 3 never crashes.
                     Agent 3 then uses whatever values Agent 2 provided.

    Auto-detects environment:
      Inside a pod  -> ServiceAccount token auto-mounted at
                       /var/run/secrets/kubernetes.io/serviceaccount
      Outside (dev) -> ~/.kube/config  (same file kubectl uses)
    """

    def __init__(self, cache_ttl_seconds: int = 30):
        """
        cache_ttl_seconds: how long to reuse a cached result.
                           30s means we re-query the cluster at most
                           once every 30 seconds per deployment.
        """
        self.cache_ttl_seconds = cache_ttl_seconds

        # In-memory cache: "namespace/deployment/container" -> (timestamp, result_dict)
        # We key by deployment name, not pod name, because pod names change on restart.
        self._cache: dict[str, tuple[float, dict]] = {}

        # Kubernetes API clients — set to None if initialisation fails
        self.core_v1 = None   # CoreV1Api  — read pods
        self.apps_v1 = None   # AppsV1Api  — read deployments

        self._load_config()

    def _load_config(self) -> None:
        """
        Try in-cluster config first, then local kubeconfig.
        If both fail, leave core_v1/apps_v1 as None so all queries
        silently return fallback zeros.
        """
        try:
            from kubernetes import client, config

            try:
                # Running inside a Kubernetes pod
                config.load_incluster_config()
                logger.info("[K8s] In-cluster config loaded (ServiceAccount token)")
            except Exception:
                # Running locally on a developer machine
                config.load_kube_config()
                logger.info("[K8s] Local kubeconfig loaded (~/.kube/config)")

            self.core_v1 = client.CoreV1Api()
            self.apps_v1 = client.AppsV1Api()

        except ImportError:
            logger.warning(
                "[K8s] 'kubernetes' package not installed. "
                "Run:  pip install kubernetes  to enable live cluster queries. "
                "Continuing with forecast-provided values."
            )
        except Exception as e:
            logger.warning(
                f"[K8s] Could not connect to cluster: {e}. "
                f"Continuing with forecast-provided values."
            )

    # ── Public interface ──────────────────────────────────────────────

    def get_resources_for_stress_test_app(self) -> dict:
        """
        Fetches cpu_request, cpu_limit, memory_request, memory_limit
        for the stress-test-app container in the test-workload namespace.

        Strategy:
          1. List all pods with label app=stress-test-app in test-workload
          2. Pick the first Running pod (all pods share the same resource spec)
          3. Find the stress-test-app container inside that pod
          4. Return its requests and limits

        We use a label selector rather than a hardcoded pod name because
        pod names change on every restart (e.g. stress-test-app-975d97b75-q7t2w).
        The deployment label app=stress-test-app is stable.

        Returns zeros for any value not set (BestEffort QoS pods have no spec).
        Results cached for cache_ttl_seconds.
        """
        if self.core_v1 is None:
            return self._empty_resources("K8s client not available")

        # Cache key is stable across pod restarts
        cache_key = f"{TARGET_NAMESPACE}/{TARGET_DEPLOYMENT}/{TARGET_CONTAINER}"

        # Return cached result if still fresh — avoids 20-50ms API call per event
        cached = self._cache.get(cache_key)
        if cached is not None:
            cached_at, cached_value = cached
            age = time.monotonic() - cached_at
            if age < self.cache_ttl_seconds:
                logger.info(
                    f"[K8s] Cache hit for {cache_key} "
                    f"(age={age:.1f}s, ttl={self.cache_ttl_seconds}s) — skipping API call"
                )
                return cached_value

        # Cache miss or stale — hit the live API
        try:
            label_selector = f"app={TARGET_DEPLOYMENT}"

            # Prefer the Deployment's selector matchLabels (most reliable),
            # since not every cluster uses the legacy `app=<deployment>` label.
            if self.apps_v1 is not None:
                try:
                    deployment = self.apps_v1.read_namespaced_deployment(
                        name=TARGET_DEPLOYMENT,
                        namespace=TARGET_NAMESPACE,
                    )
                    match_labels = (
                        (deployment.spec.selector.match_labels or {})
                        if deployment and deployment.spec and deployment.spec.selector
                        else {}
                    )
                    if match_labels:
                        label_selector = ",".join(
                            f"{k}={match_labels[k]}" for k in sorted(match_labels)
                        )
                except Exception:
                    # If the Deployment lookup fails, fall back to app=<deployment>
                    pass

            # List all pods belonging to our deployment via label selector.
            # Standard Kubernetes convention: the deployment controller sets
            # app=<deployment-name> on every pod it creates.
            pod_list = self.core_v1.list_namespaced_pod(
                namespace      = TARGET_NAMESPACE,
                label_selector = label_selector,
            )

            if not pod_list.items:
                logger.warning(
                    f"[K8s] No pods found for selector '{label_selector}' "
                    f"in namespace {TARGET_NAMESPACE}. "
                    f"If your deployment uses different labels, update the "
                    f"label_selector in get_resources_for_stress_test_app()."
                )
                return self._empty_resources("no pods found for label selector")

            # Pick a Running pod — all pods in the ReplicaSet share the same
            # container spec so it does not matter which one we pick.
            target_pod = None
            for pod in pod_list.items:
                phase = (pod.status.phase or "Unknown") if pod.status else "Unknown"
                if phase == "Running":
                    target_pod = pod
                    break

            # Fallback: use first pod even if not Running yet
            if target_pod is None:
                target_pod = pod_list.items[0]
                phase = (target_pod.status.phase or "Unknown") if target_pod.status else "Unknown"
                logger.warning(
                    f"[K8s] No Running pod found — using pod in phase '{phase}'"
                )

            logger.info(
                f"[K8s] Reading resource spec from pod: "
                f"{target_pod.metadata.name} (namespace={TARGET_NAMESPACE})"
            )

            # Search for our target container inside the pod spec
            for container in target_pod.spec.containers:
                if container.name != TARGET_CONTAINER:
                    continue

                # resources is None for BestEffort pods (nothing configured)
                res      = container.resources
                requests = (res.requests or {}) if res else {}
                limits   = (res.limits   or {}) if res else {}

                result = {
                    "cpu_request":    self._parse_cpu(requests.get("cpu")),
                    "cpu_limit":      self._parse_cpu(limits.get("cpu")),
                    "memory_request": self._parse_memory(requests.get("memory")),
                    "memory_limit":   self._parse_memory(limits.get("memory")),
                }

                logger.info(
                    f"[K8s] {TARGET_CONTAINER} → "
                    f"cpu_request={result['cpu_request']:.3f} cores | "
                    f"cpu_limit={result['cpu_limit']:.3f} cores | "
                    f"mem_request={result['memory_request']:.0f} B | "
                    f"mem_limit={result['memory_limit']:.0f} B"
                )

                # Store in cache with current monotonic timestamp
                self._cache[cache_key] = (time.monotonic(), result)
                return result

            # If we get here, the container name didn't match anything in the pod spec
            found = [c.name for c in target_pod.spec.containers]
            logger.warning(
                f"[K8s] Container '{TARGET_CONTAINER}' not found in pod "
                f"{target_pod.metadata.name}. "
                f"Containers present: {found}. "
                f"If the container name differs, update TARGET_CONTAINER constant."
            )

        except Exception as e:
            logger.warning(
                f"[K8s] get_resources_for_stress_test_app failed: {e} "
                f"— using forecast-provided values"
            )

        return self._empty_resources("query failed or container not found")

    def get_live_replica_count(self) -> int:
        """
        Reads the ready replica count directly from the stress-test-app
        Deployment object.

        This fixes the hardcoded 'current_replicas: 1' default that existed
        in the original Redis mode — we now return the real live count.

        Returns 1 as a safe fallback if anything fails.
        """
        if self.apps_v1 is None:
            return 1

        try:
            deployment = self.apps_v1.read_namespaced_deployment(
                name      = TARGET_DEPLOYMENT,
                namespace = TARGET_NAMESPACE,
            )

            # ready_replicas is None when no pods are ready yet (e.g. fresh deploy)
            ready = deployment.status.ready_replicas
            if ready is None:
                # Fall back to desired (spec) replicas — better than returning 1
                ready = deployment.spec.replicas or 1
                logger.warning(
                    f"[K8s] Deployment '{TARGET_DEPLOYMENT}' has no ready replicas yet — "
                    f"using desired spec replicas ({ready}) as fallback"
                )
            else:
                logger.info(
                    f"[K8s] Deployment '{TARGET_DEPLOYMENT}' — "
                    f"ready_replicas={ready}"
                )

            return int(ready)

        except Exception as e:
            logger.warning(
                f"[K8s] get_live_replica_count failed: {e} — defaulting to 1"
            )
            return 1

    # ── Kubernetes unit parsers ───────────────────────────────────────

    @staticmethod
    def _parse_cpu(value: str | None) -> float:
        """
        Converts Kubernetes CPU strings to float (cores).
          '250m'  -> 0.25
          '500m'  -> 0.5
          '1'     -> 1.0
          '2000m' -> 2.0
          None    -> 0.0
        """
        if not value:
            return 0.0
        value = str(value).strip()
        if value.endswith("m"):
            return float(value[:-1]) / 1000.0
        return float(value)

    @staticmethod
    def _parse_memory(value: str | None) -> float:
        """
        Converts Kubernetes memory strings to float (bytes).
          '256Mi' -> 268435456.0
          '512Mi' -> 536870912.0
          '1Gi'   -> 1073741824.0
          None    -> 0.0
        Note: longer suffixes are checked first to avoid 'Ki' matching 'K'.
        """
        if not value:
            return 0.0
        value = str(value).strip()
        units = {
            "Ki": 1024,
            "Mi": 1024 ** 2,
            "Gi": 1024 ** 3,
            "Ti": 1024 ** 4,
            "K":  1000,
            "M":  1000 ** 2,
            "G":  1000 ** 3,
        }
        for suffix, multiplier in units.items():
            if value.endswith(suffix):
                return float(value[: -len(suffix)]) * multiplier
        return float(value)  # plain integer bytes with no suffix

    @staticmethod
    def _empty_resources(reason: str = "") -> dict:
        """Returns zero-value fallback dict. Never raises."""
        if reason:
            logger.debug(f"[K8s] Returning empty resources: {reason}")
        return {
            "cpu_request":    0.0,
            "cpu_limit":      0.0,
            "memory_request": 0.0,
            "memory_limit":   0.0,
        }


# ─────────────────────────────────────────────
# QoS Detection
# ─────────────────────────────────────────────

class QoSClass(str, Enum):
    """
    Kubernetes Quality of Service classes.
    Kubernetes assigns these automatically based on how requests and limits
    are configured in the pod spec.

    Guaranteed  -> cpu_request == cpu_limit AND mem_request == mem_limit
                   Pod is never evicted under memory pressure.
                   Treat as critical: scale up earlier, scale down cautiously.

    Burstable   -> at least one request < limit (or only limits set).
                   Standard behaviour — the common case.

    BestEffort  -> no requests or limits set at all.
                   First to be evicted under pressure.
                   Tolerate higher utilization, scale down aggressively.
    """
    GUARANTEED  = "Guaranteed"
    BURSTABLE   = "Burstable"
    BEST_EFFORT = "BestEffort"


def detect_qos_class(
    cpu_request: float,
    cpu_limit: float,
    mem_request: float,
    mem_limit: float,
) -> QoSClass:
    """
    Applies the official Kubernetes QoS classification logic.

    Concrete examples for stress-test-app:

      requests.cpu=250m  limits.cpu=500m
      requests.memory=256Mi limits.memory=512Mi
        -> 0.25 != 0.50 -> Burstable  (most common real-world case)

      requests.cpu=500m  limits.cpu=500m
      requests.memory=512Mi limits.memory=512Mi
        -> equal for both -> Guaranteed

      (nothing set in pod spec)
        -> all zeros -> BestEffort
    """
    all_zero = (
        cpu_request == 0 and cpu_limit == 0
        and mem_request == 0 and mem_limit == 0
    )
    if all_zero:
        return QoSClass.BEST_EFFORT

    # Both resources must have request == limit within float rounding tolerance
    cpu_guaranteed = cpu_request > 0 and abs(cpu_request - cpu_limit) < 1e-6
    mem_guaranteed = mem_request > 0 and abs(mem_request - mem_limit) < 1e-6

    if cpu_guaranteed and mem_guaranteed:
        return QoSClass.GUARANTEED

    return QoSClass.BURSTABLE


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
    recommended_action:      str
    current_replicas:        int
    recommended_replicas:    int
    confidence:              float
    cpu_forecast_p50_5m:     float
    cpu_forecast_p90_5m:     float
    cpu_forecast_p90_15m:    float
    memory_forecast_p50_5m:  float
    memory_forecast_p90_5m:  float
    memory_forecast_p90_15m: float
    throttle_risk_level:     str
    oom_risk_level:          str
    throttle_prob:           float
    oom_prob:                float
    cpu_limit:               float
    memory_limit:            float
    cpu_request:             float   # NEW: from live K8s query
    memory_request:          float   # NEW: from live K8s query
    qos_class:               str     # NEW: Guaranteed | Burstable | BestEffort
    reason:                  str
    namespace:               str
    pod:                     str
    container:               str
    timestamp:               str


# ─────────────────────────────────────────────
# Optimization Configuration (QoS-aware)
# ─────────────────────────────────────────────

class OptimizationConfig:
    # Branch 1 — scale_up trigger
    RISK_LEVELS_HIGH = {RiskLevel.HIGH.value, RiskLevel.CRITICAL.value}

    # Branch 3 — retrain threshold (same regardless of QoS class)
    RETRAIN_CONF_THRESHOLD = 0.30

    # Hard cap — never jump more than 3x current replicas in one step
    MAX_SCALE_UP_FACTOR = 3.0

    # QoS-aware policy table.
    #
    # Guaranteed: critical pod, scale up earlier (60% target) so it never
    #             gets stressed. Scale down only with very high confidence.
    #             Never drop below 2 replicas.
    #
    # Burstable:  normal pod. Original hardcoded values preserved exactly
    #             so existing behaviour is unchanged for the common case.
    #
    # BestEffort: throwaway pod. Tolerate 85% pressure before scaling up.
    #             Scale down aggressively with lower confidence bar.
    QOS_POLICY = {
        QoSClass.GUARANTEED: {
            "target_utilization":   0.60,
            "scale_down_min_conf":  0.85,
            "min_replicas":         2,
        },
        QoSClass.BURSTABLE: {
            "target_utilization":   0.70,   # original behaviour
            "scale_down_min_conf":  0.65,   # original behaviour
            "min_replicas":         1,
        },
        QoSClass.BEST_EFFORT: {
            "target_utilization":   0.85,
            "scale_down_min_conf":  0.50,
            "min_replicas":         1,
        },
    }

    @classmethod
    def for_qos(cls, qos: QoSClass) -> dict:
        return cls.QOS_POLICY[qos]


# ─────────────────────────────────────────────
# Replica Calculator
# ─────────────────────────────────────────────

class ReplicaCalculator:
    """
    Computes recommended replica count from forecast pressure.
    target_utilization and min_replicas are passed per-call so
    QoS class can influence the result without subclassing.
    """

    def __init__(self, config: OptimizationConfig = OptimizationConfig()):
        self.cfg = config

    def compute_scale_up(
        self,
        current: int,
        cpu_p90: float,
        mem_p90: float,
        target_utilization: float,
        min_replicas: int,
    ) -> int:
        """
        pressure / target_utilization = ideal scale factor.
        Example: pressure=0.90, target=0.70 -> factor=1.28 -> ceil(4 * 1.28) = 6
        """
        pressure = max(cpu_p90, mem_p90)
        if pressure > 0:
            scale_factor = pressure / target_utilization
            raw = math.ceil(current * scale_factor)
        else:
            raw = current + 1

        # Always add at least 1 replica over current
        raw = max(current + 1, raw)

        # Hard cap: never jump more than MAX_SCALE_UP_FACTOR times current
        capped = min(raw, math.floor(current * self.cfg.MAX_SCALE_UP_FACTOR))
        result = max(current + 1, capped)
        result = max(result, min_replicas)

        logger.info(
            f"  [REPLICA CALC] scale_up: pressure={pressure:.2f} | "
            f"target_util={target_utilization:.0%} | "
            f"factor={pressure / target_utilization:.2f}x | "
            f"{current} -> {result}"
        )
        return result

    def compute_scale_down(
        self,
        current: int,
        cpu_p90: float,
        mem_p90: float,
        target_utilization: float,
        min_replicas: int,
    ) -> int:
        """
        Reduce toward target_utilization.
        Example: pressure=0.20, target=0.70 -> factor=0.28 -> floor(4 * 0.28) = 1
        """
        pressure = max(cpu_p90, mem_p90)
        if pressure > 0:
            scale_factor = pressure / target_utilization
            raw = math.floor(current * scale_factor)
        else:
            raw = current - 1

        result = max(min_replicas, raw)

        logger.info(
            f"  [REPLICA CALC] scale_down: pressure={pressure:.2f} | "
            f"target_util={target_utilization:.0%} | "
            f"factor={pressure / target_utilization:.2f}x | "
            f"{current} -> {result}"
        )
        return result


# ─────────────────────────────────────────────
# Four-Branch Rule Engine (QoS-aware)
# ─────────────────────────────────────────────

class OptimizationEngine:
    """
    Core decision logic: reads risk signals from Agent 2 and applies
    the four-branch conditional rule.

    Branch 1 — SCALE_UP:   throttle_risk OR oom_risk is HIGH/CRITICAL
    Branch 2 — SCALE_DOWN: both risks LOW + confidence sufficient
    Branch 3 — RETRAIN:    confidence below retrain threshold
    Branch 4 — HOLD:       none of the above

    Branch 3 is evaluated FIRST because a low-confidence model output
    should not trigger scaling in any direction.

    All thresholds (target utilization, min replicas, scale-down confidence)
    are read from the QoS policy table rather than being hardcoded.
    The Burstable row preserves the original hardcoded values exactly.
    """

    def __init__(self, config: OptimizationConfig = OptimizationConfig()):
        self.cfg        = config
        self.calculator = ReplicaCalculator(config)

    def decide(self, forecast: dict) -> tuple[ScalingAction, int, str]:
        """Returns (action, recommended_replicas, reason)."""

        confidence          = float(forecast.get("confidence",          1.0))
        current_replicas    = int(forecast.get("current_replicas",      1))
        throttle_risk_level = forecast.get("throttle_risk_level",       "LOW")
        oom_risk_level      = forecast.get("oom_risk_level",            "LOW")
        throttle_prob       = float(forecast.get("throttle_prob",       0.0))
        oom_prob            = float(forecast.get("oom_prob",            0.0))

        # ── Resolve QoS policy ────────────────────────────────────────────
        # qos_class is written into forecast by ResourceOptimizationAgent.run()
        # after the K8s query and detect_qos_class() call.
        qos_str   = forecast.get("qos_class", QoSClass.BURSTABLE.value)
        qos_class = (
            QoSClass(qos_str)
            if qos_str in QoSClass._value2member_map_
            else QoSClass.BURSTABLE   # safe fallback if value is unrecognised
        )
        policy              = self.cfg.for_qos(qos_class)
        target_utilization  = policy["target_utilization"]
        scale_down_min_conf = policy["scale_down_min_conf"]
        min_replicas        = policy["min_replicas"]

        # ── Parse Agent 2 forecast values ────────────────────────────────
        cpu_forecast = _parse_forecast(forecast.get("cpu_forecast_json", "{}"))
        mem_forecast = _parse_forecast(forecast.get("memory_forecast_json", "{}"))
        cpu_p90_15m  = _get_quantile(cpu_forecast, horizon="15", quantile="0.9")
        mem_p90_15m  = _get_quantile(mem_forecast, horizon="15", quantile="0.9")

        # Convert to utilization ratios when limits are available.
        # Agent 4 governance thresholds are ratio-based (0.0-1.0+).
        cpu_limit    = float(forecast.get("cpu_limit",    0.0) or 0.0)
        mem_limit    = float(forecast.get("memory_limit", 0.0) or 0.0)
        cpu_pressure = (cpu_p90_15m / cpu_limit) if cpu_limit > 0 else cpu_p90_15m
        mem_pressure = (mem_p90_15m / mem_limit) if mem_limit > 0 else mem_p90_15m

        # ── Log full decision context ─────────────────────────────────────
        logger.info(
            f"  QoS={qos_class.value} | "
            f"target_util={target_utilization:.0%} | "
            f"scale_down_min_conf={scale_down_min_conf:.2f} | "
            f"min_replicas={min_replicas}"
        )
        logger.info(
            f"  confidence={confidence:.2f} | "
            f"throttle_risk={throttle_risk_level} (prob={throttle_prob:.2f}) | "
            f"oom_risk={oom_risk_level} (prob={oom_prob:.2f})"
        )
        logger.info(
            f"  cpu_p90_15m={cpu_p90_15m:.4f} (pressure={cpu_pressure:.2f}) | "
            f"mem_p90_15m={mem_p90_15m:.0f}B (pressure={mem_pressure:.2f}) | "
            f"current_replicas={current_replicas}"
        )

        # ── Branch 3: retrain if model confidence too low ─────────────────
        if confidence < self.cfg.RETRAIN_CONF_THRESHOLD:
            reason = (
                f"Model confidence critically low ({confidence:.2f} < "
                f"{self.cfg.RETRAIN_CONF_THRESHOLD}). "
                f"Triggering retraining signal. No scaling action taken."
            )
            logger.info(f"  -> Branch 3: RETRAIN -- {reason}")
            return ScalingAction.RETRAIN, current_replicas, reason

        # ── Branch 1: scale_up if either risk is HIGH or CRITICAL ─────────
        if (throttle_risk_level in self.cfg.RISK_LEVELS_HIGH or
                oom_risk_level in self.cfg.RISK_LEVELS_HIGH):

            recommended = self.calculator.compute_scale_up(
                current_replicas,
                cpu_pressure,
                mem_pressure,
                target_utilization,
                min_replicas,
            )
            parts = []
            if throttle_risk_level in self.cfg.RISK_LEVELS_HIGH:
                parts.append(
                    f"CPU throttle risk is {throttle_risk_level} "
                    f"(p90={cpu_pressure:.0%} of limit)"
                )
            if oom_risk_level in self.cfg.RISK_LEVELS_HIGH:
                parts.append(
                    f"Memory OOM risk is {oom_risk_level} "
                    f"(p90={mem_pressure:.0%} of limit)"
                )
            reason = ". ".join(parts) + (
                f". QoS={qos_class.value}. "
                f"Scaling from {current_replicas} -> {recommended} replicas "
                f"to maintain <={target_utilization:.0%} utilization."
            )
            logger.info(f"  -> Branch 1: SCALE_UP -- {reason}")
            return ScalingAction.SCALE_UP, recommended, reason

        # ── Branch 2: scale_down if both risks LOW and confidence sufficient
        if (throttle_risk_level == RiskLevel.LOW.value and
                oom_risk_level == RiskLevel.LOW.value and
                confidence >= scale_down_min_conf):

            recommended = self.calculator.compute_scale_down(
                current_replicas,
                cpu_pressure,
                mem_pressure,
                target_utilization,
                min_replicas,
            )
            if recommended < current_replicas:
                reason = (
                    f"Both CPU ({throttle_risk_level}) and memory ({oom_risk_level}) "
                    f"risks are LOW with confidence={confidence:.2f}. "
                    f"QoS={qos_class.value}. "
                    f"Scaling down from {current_replicas} -> {recommended} replicas "
                    f"to recover unused capacity."
                )
                logger.info(f"  -> Branch 2: SCALE_DOWN -- {reason}")
                return ScalingAction.SCALE_DOWN, recommended, reason
            else:
                # Formula returned same or higher — already at optimal floor
                reason = (
                    f"Risks are LOW but current replica count ({current_replicas}) "
                    f"is already at minimum or optimal capacity "
                    f"(QoS={qos_class.value}, min_replicas={min_replicas}). Holding."
                )
                logger.info(f"  -> Branch 4 (via Branch 2): HOLD -- {reason}")
                return ScalingAction.HOLD, current_replicas, reason

        # ── Branch 4: hold ────────────────────────────────────────────────
        reason = (
            f"Resource pressure is moderate "
            f"(throttle={throttle_risk_level}, oom={oom_risk_level}) "
            f"and confidence={confidence:.2f}. "
            f"QoS={qos_class.value}. "
            f"No scaling action required. Maintaining {current_replicas} replicas."
        )
        logger.info(f"  -> Branch 4: HOLD -- {reason}")
        return ScalingAction.HOLD, current_replicas, reason


# ─────────────────────────────────────────────
# Main Optimization Agent
# ─────────────────────────────────────────────

class ResourceOptimizationAgent:

    def __init__(self, enable_k8s_query: bool = True):
        """
        enable_k8s_query: pass False via --no-k8s flag for testing
                          without a real cluster connection.
        """
        self.config = OptimizationConfig()
        self.engine = OptimizationEngine(self.config)

        # Try to initialise the K8s fetcher.
        # On failure self.k8s stays None and run() uses Agent 2 values as fallback.
        self.k8s: K8sResourceFetcher | None = None
        if enable_k8s_query:
            try:
                self.k8s = K8sResourceFetcher(cache_ttl_seconds=30)
            except Exception as e:
                logger.warning(
                    f"[K8s] Fetcher could not be initialised: {e}. "
                    f"Running without live cluster queries."
                )

    def run(self, forecast: dict) -> ScalingDecision:
        logger.info("=" * 60)
        logger.info("Agent 3: Resource Optimization -- Starting")
        logger.info("=" * 60)
        logger.info(
            f"Target:  {TARGET_NAMESPACE}/{TARGET_DEPLOYMENT}/{TARGET_CONTAINER}"
        )
        logger.info(
            f"Input:   pod={forecast.get('pod')} | "
            f"current_replicas={forecast.get('current_replicas')}"
        )
        logger.info("")

        # ── Step 0: Query live cluster ────────────────────────────────────
        #
        # Fetches real cpu/memory requests+limits and live replica count
        # directly from the Kubernetes API for the stress-test-app container.
        #
        # Why we do this:
        #   - Agent 2 sends limits (from Prometheus) but never sends requests.
        #     Without requests we cannot compute QoS class accurately.
        #   - The hardcoded current_replicas=1 default in Redis mode is wrong.
        #     We fix it by reading the actual Deployment ready_replicas.
        #
        # Graceful degradation:
        #   - If cluster is unreachable, K8s returns zeros.
        #   - We only overwrite Agent 2's limit values if K8s returned something real.
        #   - Agent 3 never crashes regardless of cluster state.

        if self.k8s is not None:
            logger.info("Step 0: Querying live Kubernetes cluster...")

            k8s_res       = self.k8s.get_resources_for_stress_test_app()
            live_replicas = self.k8s.get_live_replica_count()

            # Live replica count is always trusted over the Redis message default
            forecast["current_replicas"] = live_replicas

            # Requests come only from K8s (Agent 2 doesn't send them)
            forecast["cpu_request"]    = k8s_res["cpu_request"]
            forecast["memory_request"] = k8s_res["memory_request"]

            # For limits: only overwrite if K8s returned a real value.
            # Keeps Agent 2's limit if K8s query partially failed.
            if k8s_res["cpu_limit"] > 0:
                forecast["cpu_limit"] = k8s_res["cpu_limit"]
            if k8s_res["memory_limit"] > 0:
                forecast["memory_limit"] = k8s_res["memory_limit"]

            logger.info(
                f"  [K8s] cpu:    request={k8s_res['cpu_request']:.3f} cores | "
                f"limit={forecast.get('cpu_limit', 0)}"
            )
            logger.info(
                f"  [K8s] memory: request={k8s_res['memory_request']:.0f} B | "
                f"limit={forecast.get('memory_limit', 0)} B"
            )
            logger.info(f"  [K8s] live replicas = {live_replicas}")

        else:
            logger.info(
                "Step 0: K8s query skipped (--no-k8s flag set) — "
                "using forecast-provided values. "
                "QoS will default to Burstable if requests are unknown."
            )

        # ── Step 0b: Detect QoS class from requests + limits ─────────────
        #
        # With real requests from K8s we can now correctly classify this pod.
        # The result is stored in forecast["qos_class"] and read by the engine.

        cpu_request = float(forecast.get("cpu_request",    0.0) or 0.0)
        cpu_limit   = float(forecast.get("cpu_limit",      0.0) or 0.0)
        mem_request = float(forecast.get("memory_request", 0.0) or 0.0)
        mem_limit   = float(forecast.get("memory_limit",   0.0) or 0.0)

        qos_class = detect_qos_class(cpu_request, cpu_limit, mem_request, mem_limit)
        forecast["qos_class"] = qos_class.value

        logger.info(
            f"  [QoS] cpu_req={cpu_request:.3f} cpu_lim={cpu_limit:.3f} | "
            f"mem_req={mem_request:.0f}B mem_lim={mem_limit:.0f}B "
            f"-> QoS = {qos_class.value}"
        )
        logger.info("")

        # ── Step 1: Four-Branch Decision (QoS-aware) ─────────────────────
        logger.info("Step 1: Applying Four-Branch Optimization Rule...")
        action, recommended_replicas, reason = self.engine.decide(forecast)

        # ── Step 2: Build ScalingDecision dataclass for Agent 4 ──────────
        cpu_forecast = _parse_forecast(forecast.get("cpu_forecast_json", "{}"))
        mem_forecast = _parse_forecast(forecast.get("memory_forecast_json", "{}"))

        cpu_p50_5m  = _get_quantile(cpu_forecast, "5",  "0.5")
        cpu_p90_5m  = _get_quantile(cpu_forecast, "5",  "0.9")
        cpu_p90_15m = _get_quantile(cpu_forecast, "15", "0.9")
        mem_p50_5m  = _get_quantile(mem_forecast, "5",  "0.5")
        mem_p90_5m  = _get_quantile(mem_forecast, "5",  "0.9")
        mem_p90_15m = _get_quantile(mem_forecast, "15", "0.9")

        # Ratio form (0.0-1.0+) when limits known — Agent 4 thresholds are ratios
        cpu_p50_5m_r  = (cpu_p50_5m  / cpu_limit) if cpu_limit > 0 else cpu_p50_5m
        cpu_p90_5m_r  = (cpu_p90_5m  / cpu_limit) if cpu_limit > 0 else cpu_p90_5m
        cpu_p90_15m_r = (cpu_p90_15m / cpu_limit) if cpu_limit > 0 else cpu_p90_15m
        mem_p50_5m_r  = (mem_p50_5m  / mem_limit) if mem_limit > 0 else mem_p50_5m
        mem_p90_5m_r  = (mem_p90_5m  / mem_limit) if mem_limit > 0 else mem_p90_5m
        mem_p90_15m_r = (mem_p90_15m / mem_limit) if mem_limit > 0 else mem_p90_15m

        decision = ScalingDecision(
            recommended_action      = action.value,
            current_replicas        = int(forecast.get("current_replicas", 1)),
            recommended_replicas    = recommended_replicas,
            confidence              = float(forecast.get("confidence", 0.0)),
            cpu_forecast_p50_5m     = cpu_p50_5m_r,
            cpu_forecast_p90_5m     = cpu_p90_5m_r,
            cpu_forecast_p90_15m    = cpu_p90_15m_r,
            memory_forecast_p50_5m  = mem_p50_5m_r,
            memory_forecast_p90_5m  = mem_p90_5m_r,
            memory_forecast_p90_15m = mem_p90_15m_r,
            throttle_risk_level     = forecast.get("throttle_risk_level", "LOW"),
            oom_risk_level          = forecast.get("oom_risk_level",      "LOW"),
            throttle_prob           = float(forecast.get("throttle_prob", 0.0)),
            oom_prob                = float(forecast.get("oom_prob",      0.0)),
            cpu_limit               = cpu_limit,
            memory_limit            = mem_limit,
            cpu_request             = cpu_request,
            memory_request          = mem_request,
            qos_class               = qos_class.value,
            reason                  = reason,
            namespace               = forecast.get("namespace",  TARGET_NAMESPACE),
            pod                     = forecast.get("pod",        TARGET_DEPLOYMENT),
            container               = forecast.get("container",  TARGET_CONTAINER),
            timestamp               = datetime.now(timezone.utc).isoformat(),
        )

        return decision

    def to_agent4_payload(self, decision: ScalingDecision) -> dict:
        """
        Converts ScalingDecision -> the JSON payload Agent 4 expects.
        New fields (qos_class, cpu_request, memory_request) give Agent 4's
        LLM prompt the context it needs to make QoS-aware governance decisions.
        """
        return {
            "namespace":               decision.namespace,
            "pod":                     decision.pod,
            "container":               decision.container,
            "timestamp":               decision.timestamp,
            "cpu_forecast_p50_5m":     round(decision.cpu_forecast_p50_5m,  4),
            "cpu_forecast_p90_5m":     round(decision.cpu_forecast_p90_5m,  4),
            "cpu_forecast_p90_15m":    round(decision.cpu_forecast_p90_15m, 4),
            "memory_forecast_p50_5m":  round(decision.memory_forecast_p50_5m,  4),
            "memory_forecast_p90_5m":  round(decision.memory_forecast_p90_5m,  4),
            "memory_forecast_p90_15m": round(decision.memory_forecast_p90_15m, 4),
            "confidence":              round(decision.confidence, 4),
            "current_replicas":        decision.current_replicas,
            "recommended_replicas":    decision.recommended_replicas,
            "recommended_action":      decision.recommended_action,
            "cpu_limit":               round(float(decision.cpu_limit    or 0.0), 6),
            "memory_limit":            int(decision.memory_limit          or 0),
            "cpu_request":             round(float(decision.cpu_request  or 0.0), 6),
            "memory_request":          int(decision.memory_request        or 0),
            "qos_class":               decision.qos_class,
            "reason":                  decision.reason,
        }


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _parse_forecast(raw) -> dict:
    """Parse cpu_forecast_json / memory_forecast_json — handles str and dict."""
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
    print(f"  Target:            {TARGET_NAMESPACE}/{TARGET_DEPLOYMENT}/{TARGET_CONTAINER}")
    print(f"  Action:            {action_icons.get(decision.recommended_action, decision.recommended_action)}")
    print(f"  Current replicas:  {decision.current_replicas}")
    print(f"  Recommended:       {decision.recommended_replicas}")
    print(f"  Confidence:        {decision.confidence:.2f}")
    print(f"  QoS Class:         {decision.qos_class}")
    print(f"  CPU  req / limit:  {decision.cpu_request:.3f} / {decision.cpu_limit:.3f} cores")
    print(f"  Mem  req / limit:  {decision.memory_request:.0f} / {decision.memory_limit:.0f} bytes")
    print("-" * 60)
    print(f"  CPU  p90 (5m):    {decision.cpu_forecast_p90_5m:.4f}")
    print(
        f"  CPU  p90 (15m):   {decision.cpu_forecast_p90_15m:.4f}  "
        f"[Throttle risk: {decision.throttle_risk_level} | prob={decision.throttle_prob:.2f}]"
    )
    print(f"  Mem  p90 (5m):    {decision.memory_forecast_p90_5m:.4f}")
    print(
        f"  Mem  p90 (15m):   {decision.memory_forecast_p90_15m:.4f}  "
        f"[OOM risk:      {decision.oom_risk_level} | prob={decision.oom_prob:.2f}]"
    )
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
# Redis mode
# ─────────────────────────────────────────────

async def run_redis_mode(
    redis_host: str = "localhost",
    redis_port: int = 6380,
    enable_k8s_query: bool = True,
):
    import sys
    sys.path.append(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    )
    from src.storage.redis_store import RedisStore

    logger.info(f"Connecting to Redis at {redis_host}:{redis_port}...")
    redis_store = RedisStore(host=redis_host, port=redis_port)
    agent       = ResourceOptimizationAgent(enable_k8s_query=enable_k8s_query)

    logger.info("Agent 3: Listening on stream:prediction:complete...")
    last_id = "$"

    lock = _RedisSingleInstanceLock(
        redis_host  = redis_host,
        redis_port  = redis_port,
        key         = os.environ.get("PIPELINE_AGENT3_LOCK_KEY", "lock:agent3:optimization"),
        ttl_seconds = int(os.environ.get("PIPELINE_AGENT3_LOCK_TTL", "30")),
    )
    lock.acquire_or_exit()
    logger.info("Agent 3 lock acquired.")

    try:
        while True:
            lock.refresh()
            messages = await redis_store.read_stream_messages(
                stream_name = "stream:prediction:complete",
                last_id     = last_id,
                block_ms    = 5000,
                count       = 10,
            )
            for msg_id, msg in messages:
                last_id = msg_id

                # Build base forecast from the Redis message.
                # namespace/pod/container fall back to TARGET_* constants if
                # Agent 2 did not include them (defensive default).
                # current_replicas starts at 1 but is overwritten by the
                # K8s live query inside agent.run().
                forecast = {
                    "namespace":        msg.get("namespace",  TARGET_NAMESPACE),
                    "pod":              msg.get("pod",        TARGET_DEPLOYMENT),
                    "container":        msg.get("container",  TARGET_CONTAINER),
                    "current_replicas": 1,
                }

                raw = msg.get("forecast_json", "{}")
                try:
                    inner = json.loads(raw)
                except json.JSONDecodeError:
                    logger.error(
                        f"Invalid forecast_json in msg {msg_id}: {raw[:120]}"
                    )
                    continue

                forecast["cpu_forecast_json"]    = json.dumps(inner.get("cpu_forecast",    {}))
                forecast["memory_forecast_json"] = json.dumps(inner.get("memory_forecast", {}))
                forecast["throttle_prob"]        = float(inner.get("throttle_prob", 0.0))
                forecast["oom_prob"]             = float(inner.get("oom_prob",      0.0))
                forecast["confidence"]           = float(inner.get("confidence",    1.0))
                # Limits from Agent 2 — may be overwritten by K8s query in run()
                forecast["cpu_limit"]            = float(inner.get("cpu_limit",    0.0) or 0.0)
                forecast["memory_limit"]         = float(inner.get("memory_limit", 0.0) or 0.0)

                throttle_risk = inner.get("throttle_risk", {})
                oom_risk      = inner.get("oom_risk",      {})
                forecast["throttle_risk_level"] = throttle_risk.get("risk_level", "LOW")
                forecast["oom_risk_level"]      = oom_risk.get("oom_risk",        "LOW")

                logger.info(f"--- Received Prediction Event {msg_id} ---")
                logger.info(
                    f"  throttle={forecast['throttle_risk_level']} | "
                    f"oom={forecast['oom_risk_level']} | "
                    f"conf={forecast['confidence']:.2f}"
                )

                decision       = agent.run(forecast)
                agent4_payload = agent.to_agent4_payload(decision)

                # Persist the agent outputs into the log file as JSON.
                # In Redis mode we don't write action_log.json (file mode only),
                # so this provides a durable, greppable record of decisions.
                try:
                    logger.info(
                        "[OUTPUT] scaling_decision=%s",
                        json.dumps(
                            asdict(decision),
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    )
                    logger.info(
                        "[OUTPUT] agent4_payload=%s",
                        json.dumps(
                            agent4_payload,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    )
                except Exception as e:
                    logger.warning(f"[OUTPUT] Failed to serialize outputs: {e}")

                print_decision(decision, agent4_payload)

                out_id = await redis_store.write_stream_message(
                    stream_name = "stream:optimization:complete",
                    payload     = {k: str(v) for k, v in agent4_payload.items()}
                )
                logger.info(
                    f"Published to stream:optimization:complete (ID {out_id})\n"
                )

    except KeyboardInterrupt:
        logger.info("Stopping Agent 3 Redis loop.")
    finally:
        lock.release()
        await redis_store.close()


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

def main():
    import asyncio

    parser = argparse.ArgumentParser(description="Agent 3: Resource Optimization")

    parser.add_argument(
        "--log-file",
        type=str,
        default=os.environ.get(
            "PIPELINE_AGENT3_LOG_FILE",
            str(Path(__file__).resolve().parent / "agent3_log.txt"),
        ),
        help="Path to write Agent 3 logs (default: agent3_log.txt)",
    )
    parser.add_argument(
        "--log-encoding",
        type=str,
        default=os.environ.get("PIPELINE_AGENT3_LOG_ENCODING", "utf-8"),
        help="Log file encoding (default: utf-8)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["file", "redis"],
        default="file",
        help="Run mode: 'file' for standalone testing, 'redis' for live stream",
    )
    parser.add_argument(
        "--forecast",
        type=str,
        default="dummy_forecast.json",
        help="(File mode) Path to Agent 2 forecast JSON file",
    )
    parser.add_argument(
        "--current-replicas",
        type=int,
        default=None,
        help="(File mode) Override current replica count for testing",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(Path(__file__).resolve().parent / "action_log.json"),
        help="(File mode) Path to write output (default: action_log.json)",
    )
    parser.add_argument(
        "--redis-host",
        type=str,
        default="localhost",
        help="Redis host (default: localhost)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6380,
        help="Redis port (default: 6380)",
    )
    parser.add_argument(
        "--no-k8s",
        action="store_true",
        default=False,
        help=(
            "Disable live Kubernetes queries — use forecast file values only. "
            "Useful for local testing without a cluster. "
            "QoS will default to Burstable when cpu_request is unknown."
        ),
    )

    args = parser.parse_args()
    _add_file_logger(args.log_file, encoding=args.log_encoding)

    if args.mode == "redis":
        asyncio.run(
            run_redis_mode(
                redis_host       = args.redis_host,
                redis_port       = args.redis_port,
                enable_k8s_query = not args.no_k8s,
            )
        )
    else:
        forecast_path = Path(args.forecast)
        if not forecast_path.exists():
            print(f"Error: Forecast file not found: {forecast_path}")
            exit(1)

        with open(forecast_path) as f:
            forecast = json.load(f)

        if args.current_replicas is not None:
            forecast["current_replicas"] = args.current_replicas
            logger.info(f"Overriding current_replicas -> {args.current_replicas}")

        agent          = ResourceOptimizationAgent(enable_k8s_query=not args.no_k8s)
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