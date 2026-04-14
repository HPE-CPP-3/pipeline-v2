"""Plug-and-play log ingestion agent.

Runs on a strict 60-second ticker and publishes normalized features to:
- Redis (latest state + stream trigger)
- InfluxDB (historical telemetry)
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
import logging
from typing import Any

import pandas as pd

from ..ingestion.prometheus_client import PrometheusClient
from ..storage.csv_store import CSVStore
from ..storage.influxdb_store import InfluxDBStore
from ..storage.redis_store import RedisStore
from .schemas import IngestionResult, PodScope, TargetConfig

logger = logging.getLogger(__name__)


class LogIngestionAgent:
    """Prometheus-backed ingestion service with plug-and-play target config."""

    def __init__(
        self,
        prometheus_client: PrometheusClient,
        redis_store: RedisStore,
        influxdb_store: InfluxDBStore,
        csv_store: CSVStore | None = None,
    ):
        self.prometheus = prometheus_client
        self.redis_store = redis_store
        self.influxdb_store = influxdb_store
        self.csv_store = csv_store

    async def collect(
        self, target_config: TargetConfig, window_minutes: int = 60
    ) -> IngestionResult:
        """Collect container/node/k8s features for a target using dynamic PromQL mapping."""
        namespace = target_config.namespace
        pod = target_config.pod_name
        container = target_config.container_name

        scope = PodScope(pod=pod, namespace=namespace, container=container)

        # Container-level
        cpu_usage = await self.prometheus.get_container_cpu_usage_seconds_total(
            namespace=namespace,
            pod_name=pod,
            container_name=container,
            window_minutes=window_minutes,
        )
        cpu_throttled = await self.prometheus.get_container_cpu_throttled_seconds_total(
            namespace=namespace,
            pod_name=pod,
            container_name=container,
            window_minutes=window_minutes,
        )
        memory_ws = await self.prometheus.get_container_memory_working_set_bytes(
            namespace=namespace,
            pod_name=pod,
            container_name=container,
            window_minutes=window_minutes,
        )
        memory_fail = await self.prometheus.get_container_memory_failures_total(
            namespace=namespace,
            pod_name=pod,
            container_name=container,
            window_minutes=window_minutes,
        )

        df = self._merge_series(
            {
                "container_cpu_usage_seconds_total": cpu_usage,
                "container_cpu_cfs_throttled_seconds_total": cpu_throttled,
                "container_memory_working_set_bytes": memory_ws,
                "container_memory_failures_total": memory_fail,
            }
        )

        # Node-level
        node_name = self._lookup_node_for_pod(pod=pod, namespace=namespace)
        if node_name:
            node_features = await self.prometheus.get_node_level_feature_matrix(
                node=node_name,
                window_minutes=window_minutes,
            )
            node_df = self._merge_series(node_features)
            df = pd.concat([df, node_df], axis=1)

        if df.empty:
            df.index = pd.DatetimeIndex([datetime.now(timezone.utc)])

        # K8s control-plane snapshot
        control = await self.prometheus.get_k8s_control_plane_snapshot(
            namespace=namespace,
            pod_name=pod,
            container_name=container,
        )
        for col, val in control.items():
            df[col] = val

        # Derived signals
        df = self._add_derived_signals(df)
        df = df.sort_index().ffill().fillna(0)

        metadata = {
            "source": "prometheus",
            "node": node_name,
            "target_config": asdict(target_config),
            "collected_at": datetime.now(timezone.utc).isoformat(),
        }
        return IngestionResult(scope=scope, raw_metrics=df, metadata=metadata)

    async def collect_and_publish(
        self, target_config: TargetConfig, window_minutes: int = 60
    ) -> IngestionResult:
        """Collect, normalize [0,1], then publish to Redis + InfluxDB + stream."""
        result = await self.collect(
            target_config=target_config, window_minutes=window_minutes
        )
        df = self._normalize_rolling_minmax(result.raw_metrics.copy())

        if df.empty:
            return result

        latest = df.iloc[-1]
        payload = {
            k: float(v)
            for k, v in latest.to_dict().items()
            if isinstance(v, (int, float))
        }

        await self.redis_store.store_features(
            pod=target_config.pod_name,
            namespace=target_config.namespace,
            features=payload,
            timestamp=datetime.now(timezone.utc),
        )

        await self.influxdb_store.write_metrics_dataframe(
            namespace=target_config.namespace,
            pod=target_config.pod_name,
            df=df,
            container=target_config.container_name,
            node=result.metadata.get("node"),
        )

        if self.csv_store is not None:
            await self.csv_store.write_metrics_dataframe(
                namespace=target_config.namespace,
                pod=target_config.pod_name,
                df=df,
                container=target_config.container_name,
                node=result.metadata.get("node"),
            )

        await self.redis_store.write_stream_message(
            stream_name="stream:ingestion:complete",
            payload={
                "namespace": target_config.namespace,
                "pod": target_config.pod_name,
                "container": target_config.container_name or "",
                "features_json": json.dumps(payload),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

        result.raw_metrics = df
        return result

    async def run_loop(
        self, target_config: TargetConfig, window_minutes: int = 60
    ) -> None:
        """Run ingestion every 60 seconds (strict ticker)."""
        import asyncio

        while True:
            started = datetime.now(timezone.utc)
            try:
                await self.collect_and_publish(
                    target_config=target_config, window_minutes=window_minutes
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("ingestion_tick_failed", exc_info=exc)

            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            await asyncio.sleep(max(0.0, 60.0 - elapsed))

    def _lookup_node_for_pod(self, pod: str, namespace: str) -> str | None:
        result = self.prometheus.query(
            f'kube_pod_info{{pod="{pod}", namespace="{namespace}"}}'
        )
        if not result:
            return None
        return result[0].get("metric", {}).get("node")

    def _merge_series(
        self, series_map: dict[str, list[tuple[datetime, float]]]
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for name, values in series_map.items():
            if not values:
                continue
            frame = pd.DataFrame(values, columns=["timestamp", name]).set_index(
                "timestamp"
            )
            frames.append(frame)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, axis=1)

    def _add_derived_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        if (
            "kube_pod_container_resource_limits_cpu" in df.columns
            and "container_cpu_usage_seconds_total" in df.columns
        ):
            df["derived_usage_vs_limit"] = df["container_cpu_usage_seconds_total"] / df[
                "kube_pod_container_resource_limits_cpu"
            ].replace(0, 1.0)
        else:
            df["derived_usage_vs_limit"] = 0.0

        if "container_cpu_usage_seconds_total" in df.columns:
            total = df["container_cpu_usage_seconds_total"].replace(0, 1.0)
            throttled = df.get("container_cpu_cfs_throttled_seconds_total", 0.0)
            df["derived_pressure_throttled_ratio"] = throttled / total
            df["derived_cpu_volatility_5m"] = (
                df["container_cpu_usage_seconds_total"].rolling(5).std().fillna(0)
            )
            df["derived_cpu_volatility_10m"] = (
                df["container_cpu_usage_seconds_total"].rolling(10).std().fillna(0)
            )
        else:
            df["derived_pressure_throttled_ratio"] = 0.0
            df["derived_cpu_volatility_5m"] = 0.0
            df["derived_cpu_volatility_10m"] = 0.0

        return df

    def _normalize_rolling_minmax(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_cols = df.select_dtypes(include=["number"]).columns
        for col in numeric_cols:
            roll_min = df[col].rolling(window=60, min_periods=1).min()
            roll_max = df[col].rolling(window=60, min_periods=1).max()
            denom = (roll_max - roll_min).replace(0, 1.0)
            df[col] = (df[col] - roll_min) / denom
            df[col] = df[col].clip(0.0, 1.0)
        return df
