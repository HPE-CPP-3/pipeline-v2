"""Redis-first ingestion bridge.

This provides a Prometheus-free Stage 1 for the pipeline.

It reads raw metrics from Redis (either a stream or existing feature keys)
then emits a canonical `stream:ingestion:complete` event that Stage 2
(`WorkloadPredictionAgent`) already consumes.

Input stream (default): `stream:metrics:latest`
Output stream (default): `stream:ingestion:complete`

Expected downstream fields (output payload):
- namespace, pod, container
- features_json: JSON dict of numeric feature_name -> value
- raw_limits_json: JSON dict with optional keys:
    cpu_limit (cores), memory_limit (bytes), throttle_ratio, memory_failcnt
- timestamp (ISO)

The bridge is tolerant: if the input stream message already contains
`features_json` and/or `raw_limits_json`, it will pass those through.
Otherwise it will build them from the message fields.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..storage.redis_store import RedisStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RedisIngestionBridgeConfig:
    input_mode: str = "stream"  # stream | keys

    # Stream mode
    input_stream: str = "stream:metrics:latest"
    start_id: str = "$"  # "$" = new only, "0" = from beginning

    # Key mode
    poll_seconds: int = 60

    # Output
    output_stream: str = "stream:ingestion:complete"

    # Optional filters
    filter_namespace: str | None = None
    filter_pod: str | None = None
    filter_container: str | None = None

    # Defaults if limits not present in source
    default_cpu_limit: float = 0.0
    default_memory_limit: float = 0.0


class RedisIngestionBridge:
    def __init__(self, redis_store: RedisStore, config: RedisIngestionBridgeConfig):
        self.redis_store = redis_store
        self.cfg = config

    async def run(self) -> None:
        if self.cfg.input_mode == "keys":
            await self._run_keys_mode()
        else:
            await self._run_stream_mode()

    # ------------------------------------------------------------------
    # Stream mode: read raw metrics events from an input stream
    # ------------------------------------------------------------------

    async def _run_stream_mode(self) -> None:
        last_id = self.cfg.start_id
        logger.info(
            "RedisIngestionBridge stream mode: input=%s start_id=%s output=%s",
            self.cfg.input_stream,
            last_id,
            self.cfg.output_stream,
        )

        while True:
            messages = await self.redis_store.read_stream_messages(
                stream_name=self.cfg.input_stream,
                last_id=last_id,
                block_ms=5000,
                count=50,
            )
            for msg_id, msg in messages:
                last_id = msg_id
                try:
                    await self._handle_input_message(msg)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("redis_ingestion_bridge_message_failed", exc_info=exc)

    async def _handle_input_message(self, msg: dict[str, str]) -> None:
        namespace = msg.get("namespace", "")
        pod = msg.get("pod", "")
        container = msg.get("container", "")

        if self._filtered_out(namespace, pod, container):
            return

        features_json = msg.get("features_json")
        raw_limits_json = msg.get("raw_limits_json")

        # Build features_json from message fields if not provided.
        if not features_json:
            features = self._extract_numeric_features_from_msg(msg)
            features_json = json.dumps(features)

        # Build raw_limits_json from message fields if not provided.
        if not raw_limits_json:
            raw_limits = {
                "cpu_limit": self._safe_float(msg.get("cpu_limit"), self.cfg.default_cpu_limit),
                "memory_limit": self._safe_float(
                    msg.get("memory_limit"), self.cfg.default_memory_limit
                ),
                "throttle_ratio": self._safe_float(msg.get("throttle_ratio"), 0.0),
                "memory_failcnt": int(self._safe_float(msg.get("memory_failcnt"), 0.0)),
            }
            raw_limits_json = json.dumps(raw_limits)

        timestamp = msg.get("timestamp") or datetime.now(timezone.utc).isoformat()

        await self.redis_store.write_stream_message(
            stream_name=self.cfg.output_stream,
            payload={
                "namespace": namespace,
                "pod": pod,
                "container": container,
                "features_json": features_json,
                "raw_limits_json": raw_limits_json,
                "timestamp": timestamp,
            },
        )

    def _extract_numeric_features_from_msg(self, msg: dict[str, str]) -> dict[str, float]:
        reserved = {
            "namespace",
            "pod",
            "container",
            "timestamp",
            "features_json",
            "raw_limits_json",
            "cpu_limit",
            "memory_limit",
            "throttle_ratio",
            "memory_failcnt",
        }
        out: dict[str, float] = {}
        for key, value in msg.items():
            if key in reserved:
                continue
            f = self._try_float(value)
            if f is None:
                continue
            out[key] = float(f)
        return out

    # ------------------------------------------------------------------
    # Keys mode: periodically read the latest feature keys and emit an event
    # ------------------------------------------------------------------

    async def _run_keys_mode(self) -> None:
        if not self.cfg.filter_namespace or not self.cfg.filter_pod:
            raise ValueError(
                "keys mode requires --namespace and --pod (so we know which keys to read)"
            )

        namespace = self.cfg.filter_namespace
        pod = self.cfg.filter_pod
        container = self.cfg.filter_container or ""

        logger.info(
            "RedisIngestionBridge keys mode: reading features:%s:%s:* every %ss output=%s",
            namespace,
            pod,
            self.cfg.poll_seconds,
            self.cfg.output_stream,
        )

        while True:
            try:
                features_map = await self.redis_store.get_features(pod=pod, namespace=namespace)
                # Convert {feature -> [(ts,val)]} into {feature -> latest_val}
                features: dict[str, float] = {}
                for feature_name, samples in features_map.items():
                    if not samples:
                        continue
                    _ts, val = samples[-1]
                    try:
                        features[feature_name] = float(val)
                    except (TypeError, ValueError):
                        continue

                raw_limits = {
                    "cpu_limit": float(self.cfg.default_cpu_limit),
                    "memory_limit": float(self.cfg.default_memory_limit),
                    "throttle_ratio": 0.0,
                    "memory_failcnt": 0,
                }

                await self.redis_store.write_stream_message(
                    stream_name=self.cfg.output_stream,
                    payload={
                        "namespace": namespace,
                        "pod": pod,
                        "container": container,
                        "features_json": json.dumps(features),
                        "raw_limits_json": json.dumps(raw_limits),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("redis_ingestion_bridge_keys_tick_failed", exc_info=exc)

            await asyncio.sleep(max(1, int(self.cfg.poll_seconds)))

    # ------------------------------------------------------------------
    # Utils
    # ------------------------------------------------------------------

    def _filtered_out(self, namespace: str, pod: str, container: str) -> bool:
        if self.cfg.filter_namespace and namespace != self.cfg.filter_namespace:
            return True
        if self.cfg.filter_pod and pod != self.cfg.filter_pod:
            return True
        if self.cfg.filter_container and container != (self.cfg.filter_container or ""):
            return True
        return False

    def _try_float(self, v: Any) -> float | None:
        try:
            if v is None:
                return None
            return float(v)
        except (TypeError, ValueError):
            return None

    def _safe_float(self, v: Any, default: float) -> float:
        f = self._try_float(v)
        return float(default if f is None else f)
