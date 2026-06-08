"""
Agent 5: K8s Executor and Monitoring
====================================
Standalone agent. Consumes approved governance decisions and applies them to K8s.
Monitors feature drift using EMADriftDetector and triggers retraining.

Run:
    python decision_agents/agent5/agent5_executor.py --mode redis
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import asyncio
import socket
import sys
import yaml
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# Add repo root to path to import src
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.storage.redis_store import RedisStore
from src.models.drift_detector import EMADriftDetector

# ─────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("agent5.executor")


def _add_file_logger(log_path: str, encoding: str = "utf-8") -> None:
    """Mirror console logs into a file using the same format."""
    root = logging.getLogger()
    abs_path = os.path.abspath(log_path)
    if any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == abs_path for h in root.handlers):
        return

    file_handler = logging.FileHandler(abs_path, encoding=encoding)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
    )
    root.addHandler(file_handler)


class _RedisSingleInstanceLock:
    """Simple best-effort Redis lock so only one Agent 5 executor runs."""

    def __init__(
        self,
        redis_host: str,
        redis_port: int,
        key: str,
        ttl_seconds: int = 60,
    ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.key = key
        self.ttl_seconds = ttl_seconds
        self.token = f"{socket.gethostname()}:{os.getpid()}"
        self._redis = None

    def acquire_or_exit(self) -> None:
        import redis

        self._redis = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
        ok = self._redis.set(self.key, self.token, nx=True, ex=self.ttl_seconds)
        if not ok:
            current = self._redis.get(self.key)
            raise SystemExit(
                f"Another Agent 5 instance appears to be running (lock={self.key}, holder={current}). "
                f"Stop the other instance or delete the key to proceed."
            )

    def refresh(self) -> None:
        if not self._redis:
            return
        try:
            val = self._redis.get(self.key)
            if val == self.token:
                self._redis.expire(self.key, self.ttl_seconds)
        except Exception:
            pass

    def release(self) -> None:
        if not self._redis:
            return
        try:
            val = self._redis.get(self.key)
            if val == self.token:
                self._redis.delete(self.key)
        except Exception:
            pass


class K8sExecutor:
    """Interacts with Kubernetes API to scale deployments."""

    def __init__(self, enable_k8s: bool = True):
        self.core_v1 = None
        self.apps_v1 = None
        self.enable_k8s = enable_k8s

        if not enable_k8s:
            logger.info("[K8s] Kubernetes integration disabled by flag.")
            return

        try:
            from kubernetes import client, config

            try:
                config.load_incluster_config()
                logger.info("[K8s] In-cluster config loaded.")
            except Exception:
                try:
                    config.load_kube_config()
                    logger.info("[K8s] Local kubeconfig loaded.")
                except Exception as e:
                    logger.warning(f"[K8s] Could not load kubeconfig: {e}. Running in dry-run mode.")
                    return

            self.core_v1 = client.CoreV1Api()
            self.apps_v1 = client.AppsV1Api()
        except ImportError:
            logger.warning("[K8s] 'kubernetes' package not installed. Scaling will run in dry-run mode.")

    def resolve_deployment_name(self, pod_name: str, namespace: str) -> str:
        """Resolve deployment name from pod owner references or fallback to string stripping."""
        if not self.apps_v1 or not self.core_v1:
            # Fallback suffix stripping
            parts = pod_name.split("-")
            if len(parts) > 2:
                return "-".join(parts[:-2])
            return pod_name

        try:
            pod = self.core_v1.read_namespaced_pod(name=pod_name, namespace=namespace)
            if pod.metadata.owner_references:
                for owner in pod.metadata.owner_references:
                    if owner.kind == "ReplicaSet":
                        rs_name = owner.name
                        rs = self.apps_v1.read_namespaced_replica_set(name=rs_name, namespace=namespace)
                        if rs.metadata.owner_references:
                            for rs_owner in rs.metadata.owner_references:
                                if rs_owner.kind == "Deployment":
                                    logger.info(f"[K8s] Resolved pod {pod_name} to Deployment {rs_owner.name}")
                                    return rs_owner.name
        except Exception as e:
            logger.warning(f"[K8s] Failed to resolve deployment via owner refs: {e}. Using fallback suffix stripping.")

        parts = pod_name.split("-")
        if len(parts) > 2:
            return "-".join(parts[:-2])
        return pod_name

    def scale_deployment(self, deployment_name: str, namespace: str, replicas: int) -> bool:
        """Scale a Kubernetes deployment to the desired replicas count."""
        if not self.apps_v1:
            logger.warning(f"[K8s] K8s API unavailable. Dry-run: Scale {namespace}/{deployment_name} to {replicas} replicas.")
            return False

        try:
            body = {"spec": {"replicas": replicas}}
            self.apps_v1.patch_namespaced_deployment_scale(
                name=deployment_name,
                namespace=namespace,
                body=body
            )
            logger.info(f"[K8s] Successfully scaled deployment {namespace}/{deployment_name} to {replicas} replicas.")
            return True
        except Exception as e:
            logger.error(f"[K8s] Failed to scale deployment {namespace}/{deployment_name} to {replicas}: {e}")
            return False


async def run_governance_listener(redis_store: RedisStore, executor: K8sExecutor):
    """Listens to stream:governance:complete and applies scaling or retrain commands."""
    last_id = "$"
    logger.info("Agent 5: Listening on stream:governance:complete...")

    while True:
        try:
            messages = await redis_store.read_stream_messages(
                stream_name="stream:governance:complete",
                last_id=last_id,
                block_ms=5000,
                count=10,
            )
            for msg_id, raw_payload in messages:
                last_id = msg_id
                logger.info(f"--- Received Governance Decision {msg_id} ---")

                outcome = raw_payload.get("outcome", "")
                action = raw_payload.get("recommended_action", "")
                namespace = raw_payload.get("namespace", "test-workload")
                pod_name = raw_payload.get("pod", "stress-test-app")
                container = raw_payload.get("container", "")

                try:
                    approved_replicas = int(float(raw_payload.get("approved_replicas", "1")))
                except ValueError:
                    approved_replicas = 1

                # Cleaner approval detection: accept any outcome containing "APPROVED" or "ESCALATED_TO_LLM"
                # (case-insensitive, to handle both plain strings and enum representations)
                outcome_upper = outcome.upper()
                is_approved = (
                    "APPROVED" in outcome_upper
                    or "ESCALATED_TO_LLM" in outcome_upper
                )
                logger.info(
                    f"Decision details: outcome={outcome} (approved={is_approved}) | "
                    f"action={action} | replicas={approved_replicas} | target={namespace}/{pod_name}"
                )

                if is_approved:
                    if action in ("scale_up", "scale_down"):
                        deployment_name = executor.resolve_deployment_name(pod_name, namespace)
                        executor.scale_deployment(deployment_name, namespace, approved_replicas)
                    elif action == "retrain":
                        logger.info(f"[Executor] Retrain action approved by governance. Publishing request to stream:retrain:request.")
                        await redis_store.write_stream_message(
                            stream_name="stream:retrain:request",
                            payload={
                                "namespace": namespace,
                                "pod": pod_name,
                                "container": container,
                                "reason": f"Approved governance action 'retrain' from decision {msg_id}"
                            }
                        )
                else:
                    logger.info(f"Governance decision rejected or hold. No scaling action executed.")

        except Exception as e:
            logger.exception("Error in governance listener loop", exc_info=e)
            await asyncio.sleep(5)


async def run_drift_listener(redis_store: RedisStore, drift_detector: EMADriftDetector):
    """Listens to stream:ingestion:complete, monitors feature drift, and signals retraining.

    Uses `raw_features_json` (original-scale metrics) for drift detection.
    Falls back to `features_json` (normalized) if raw field is missing for backward compatibility.
    """
    last_id = "$"
    logger.info("Agent 5: Listening on stream:ingestion:complete for drift detection...")

    while True:
        try:
            messages = await redis_store.read_stream_messages(
                stream_name="stream:ingestion:complete",
                last_id=last_id,
                block_ms=5000,
                count=10,
            )
            for msg_id, raw_payload in messages:
                last_id = msg_id

                namespace = raw_payload.get("namespace", "test-workload")
                pod_name = raw_payload.get("pod", "stress-test-app")
                container = raw_payload.get("container", "")

                # Prefer raw features (original scale) for drift detection
                raw_features_json = raw_payload.get("raw_features_json", "{}")
                if raw_features_json and raw_features_json != "{}":
                    features_json = raw_features_json
                else:
                    # Fallback to normalized features (backward compatibility)
                    features_json = raw_payload.get("features_json", "{}")
                    if features_json != "{}":
                        logger.debug(f"No raw_features_json, using normalized features for {namespace}/{pod_name}")

                if not features_json or features_json == "{}":
                    logger.debug(f"No features found in message {msg_id}, skipping drift update")
                    continue

                try:
                    features = json.loads(features_json)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in features field for {msg_id}, skipping")
                    continue

                if not features:
                    continue

                # Cast numeric values
                numeric_features = {}
                for k, v in features.items():
                    try:
                        numeric_features[k] = float(v)
                    except (ValueError, TypeError):
                        pass

                if not numeric_features:
                    continue

                # Update drift detector
                drift_status = drift_detector.update_batch(numeric_features)
                drift_ratio = drift_detector.get_drift_ratio(drift_status)

                logger.info(f"[Drift] Updated features for {namespace}/{pod_name}. Current drift ratio: {drift_ratio:.1%}")

                if drift_detector.should_trigger_retrain(drift_status):
                    logger.warning(
                        f"[Drift] Feature drift detected for {namespace}/{pod_name} (ratio={drift_ratio:.1%}). "
                        f"Publishing retraining request."
                    )
                    await redis_store.write_stream_message(
                        stream_name="stream:retrain:request",
                        payload={
                            "namespace": namespace,
                            "pod": pod_name,
                            "container": container,
                            "reason": f"Feature drift ratio ({drift_ratio:.1%}) exceeded threshold ({drift_detector.feature_ratio_threshold:.1%})"
                        }
                    )

        except Exception as e:
            logger.exception("Error in drift listener loop", exc_info=e)
            await asyncio.sleep(5)


async def main():
    parser = argparse.ArgumentParser(description="Agent 5: K8s Executor & Monitoring")
    parser.add_argument(
        "--log-file",
        type=str,
        default=os.environ.get(
            "PIPELINE_AGENT5_LOG_FILE",
            str(Path(__file__).resolve().parent / "agent5_log.txt"),
        ),
        help="Path to write Agent 5 logs",
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
        default=6380,
        help="Redis port",
    )
    parser.add_argument(
        "--no-k8s",
        action="store_true",
        default=False,
        help="Disable Kubernetes API scaling (run in dry-run mode)",
    )

    args = parser.parse_args()
    _add_file_logger(args.log_file)

    # Acquire Redis lock
    lock = _RedisSingleInstanceLock(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        key=os.environ.get("PIPELINE_AGENT5_LOCK_KEY", "lock:agent5:executor"),
        ttl_seconds=30,
    )
    lock.acquire_or_exit()
    logger.info("Agent 5 lock acquired.")

    # Load drift configurations from configs/training.yaml if available
    drift_config = {}
    config_path = Path(__file__).resolve().parents[2] / "configs" / "training.yaml"
    if config_path.exists():
        try:
            with open(config_path) as f:
                cfg = yaml.safe_load(f)
                drift_config = cfg.get("drift", {})
                logger.info(f"Loaded drift configuration from {config_path}")
        except Exception as e:
            logger.warning(f"Failed to load drift config from {config_path}: {e}")
    else:
        logger.warning(f"Drift config file not found at {config_path}. Using default drift detector parameters.")

    redis_store = RedisStore(host=args.redis_host, port=args.redis_port)
    executor = K8sExecutor(enable_k8s=not args.no_k8s)
    drift_detector = EMADriftDetector(drift_config)

    # Lock refresh loop
    async def refresh_lock_loop():
        while True:
            await asyncio.sleep(10)
            lock.refresh()

    try:
        # Run loops concurrently
        await asyncio.gather(
            run_governance_listener(redis_store, executor),
            run_drift_listener(redis_store, drift_detector),
            refresh_lock_loop(),
        )
    except KeyboardInterrupt:
        logger.info("Stopping Agent 5.")
    finally:
        lock.release()
        await redis_store.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass