"""
Feature builder - constructs feature vectors from Prometheus metrics and events
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional
import logging
import pandas as pd

from .prometheus_client import PrometheusClient

logger = logging.getLogger(__name__)


class FeatureBuilder:
    """
    Builds feature vectors for prediction

    Combines:
    - Container metrics (CPU, memory, throttling)
    - Node metrics
    - K8s lifecycle metrics
    - Event-based features (time_since_last_*)
    - Derived features (pressure, temporal, efficiency)
    """

    def __init__(
        self,
        prometheus_client: PrometheusClient,
        event_store,
        feature_store,
        config: dict,
    ):
        self.prometheus = prometheus_client
        self.event_store = event_store
        self.feature_store = feature_store
        self.config = config

        # Load config
        self.windows = config.get("windows", {"short": 5, "medium": 10, "long": 30})
        self.pressure_thresholds = config.get("pressure", {})
        self.dynamic_threshold_config = config.get("dynamic_threshold", {})

        logger.info("Initialized FeatureBuilder")

    async def build_features(
        self,
        pod: str,
        namespace: str,
        container: Optional[str] = None,
        window_minutes: int = 90,
    ) -> Optional[pd.DataFrame]:
        """
        Build complete feature vector for a pod

        Args:
            pod: Pod name
            namespace: Namespace
            container: Optional container name
            window_minutes: Historical window (60-120 min)

        Returns:
            DataFrame with time-indexed features
        """
        end = datetime.now()
        start = end - timedelta(minutes=window_minutes)

        # Gather all features in parallel
        tasks = [
            self._get_container_features(pod, namespace, container, window_minutes),
            self._get_node_features(pod, namespace, window_minutes),
            self._get_k8s_features(pod, namespace, container, window_minutes),
            self._get_event_features(pod, namespace, window_minutes),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle errors
        features = {}
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(f"Feature collection task {i} failed: {result}")
            else:
                features.update(result)

        if not features:
            return None

        # Create DataFrame
        df = pd.DataFrame(features)
        df = df.sort_index()

        # Add derived features
        df = await self._add_derived_features(df, pod, namespace, container)

        logger.debug(
            f"Built features for {pod}: {len(df)} samples, {len(df.columns)} features"
        )
        return df

    async def _get_container_features(
        self,
        pod: str,
        namespace: str,
        container: Optional[str],
        window_minutes: int,
    ) -> dict:
        """Get container-level metrics"""
        features = {}

        # CPU usage rate
        cpu_data = await self.prometheus.get_container_cpu_usage(
            pod, namespace, container, window_minutes
        )
        if cpu_data:
            df_cpu = pd.DataFrame(cpu_data, columns=["timestamp", "cpu_usage_rate"])
            df_cpu = df_cpu.set_index("timestamp")
            features["cpu_usage_rate"] = df_cpu["cpu_usage_rate"]

        # CPU throttle ratio
        throttle_data = await self.prometheus.get_cpu_throttle_ratio(
            pod, namespace, container, window_minutes
        )
        if throttle_data:
            df_throttle = pd.DataFrame(
                throttle_data, columns=["timestamp", "cpu_throttle_ratio"]
            )
            df_throttle = df_throttle.set_index("timestamp")
            features["cpu_throttle_ratio"] = df_throttle["cpu_throttle_ratio"]

        # Memory usage
        mem_data = await self.prometheus.get_container_memory_usage(
            pod, namespace, container, window_minutes
        )
        if mem_data:
            df_mem = pd.DataFrame(mem_data, columns=["timestamp", "memory_usage_bytes"])
            df_mem = df_mem.set_index("timestamp")
            features["memory_usage_bytes"] = df_mem["memory_usage_bytes"]

        return features

    async def _get_node_features(
        self,
        pod: str,
        namespace: str,
        window_minutes: int,
    ) -> dict:
        """Get node-level metrics (requires pod-to-node mapping)"""
        # Get pod's node
        node_query = f'kube_pod_info{{pod="{pod}", namespace="{namespace}"}}'
        node_result = self.prometheus.query(node_query)

        if not node_result:
            return {}

        node = node_result[0].get("metric", {}).get("node", "")
        if not node:
            return {}

        # Get node metrics
        node_metrics = await self.prometheus.get_node_metrics(node, window_minutes)

        features = {}
        for metric_name, values in node_metrics.items():
            if values:
                df = pd.DataFrame(values, columns=["timestamp", metric_name])
                df = df.set_index("timestamp")
                features[metric_name] = df[metric_name]

        return features

    async def _get_k8s_features(
        self,
        pod: str,
        namespace: str,
        container: Optional[str],
        window_minutes: int,
    ) -> dict:
        """Get K8s lifecycle and resource features"""
        features = {}

        # Get resource limits
        limits = await self.prometheus.get_resource_limits(pod, namespace, container)

        # Add as constant series (same value for all timestamps)
        if limits["cpu_cores"] > 0:
            features["cpu_limit_cores"] = pd.Series(
                limits["cpu_cores"],
                index=pd.date_range(
                    end=datetime.now(),
                    periods=window_minutes,
                    freq="1min",
                ),
            )

        if limits["memory_bytes"] > 0:
            features["memory_limit_bytes"] = pd.Series(
                limits["memory_bytes"],
                index=pd.date_range(
                    end=datetime.now(),
                    periods=window_minutes,
                    freq="1min",
                ),
            )

        return features

    async def _get_event_features(
        self,
        pod: str,
        namespace: str,
        window_minutes: int,
    ) -> dict:
        """Get event-based features (time_since_last_*)"""
        features = {}

        # Get time since last events
        event_types = ["restart", "scaling", "eviction", "oom", "scheduling"]

        index = pd.date_range(
            end=datetime.now(),
            periods=window_minutes,
            freq="1min",
        )

        for event_type in event_types:
            time_since = await self.event_store.get_time_since_last_event(
                pod, event_type
            )

            # Create constant series
            features[f"time_since_last_{event_type}"] = pd.Series(
                time_since,
                index=index,
            )

        return features

    async def _add_derived_features(
        self,
        df: pd.DataFrame,
        pod: str,
        namespace: str,
        container: Optional[str],
    ) -> pd.DataFrame:
        """Add derived features (pressure, temporal, efficiency)"""

        # Pressure signals
        if "cpu_usage_rate" in df.columns and "cpu_limit_cores" in df.columns:
            df["cpu_pressure_ratio"] = df["cpu_usage_rate"] / df["cpu_limit_cores"]

        if "memory_usage_bytes" in df.columns and "memory_limit_bytes" in df.columns:
            df["memory_pressure_ratio"] = (
                df["memory_usage_bytes"] / df["memory_limit_bytes"]
            )

        # Temporal features (rolling statistics)
        for col in ["cpu_usage_rate", "memory_usage_bytes"]:
            if col not in df.columns:
                continue

            # Rolling mean
            df[f"{col}_mean_5m"] = df[col].rolling(window=5).mean()
            df[f"{col}_mean_10m"] = df[col].rolling(window=10).mean()
            df[f"{col}_mean_30m"] = df[col].rolling(window=30).mean()

            # Rolling std
            df[f"{col}_std_5m"] = df[col].rolling(window=5).std()
            df[f"{col}_std_10m"] = df[col].rolling(window=10).std()

            # Lag features
            df[f"{col}_lag_1m"] = df[col].shift(1)
            df[f"{col}_lag_5m"] = df[col].shift(5)

            # Spike indicator
            df[f"{col}_spike"] = (df[col] - df[f"{col}_lag_1m"]).abs() / (
                df[f"{col}_lag_1m"] + 1e-6
            )

        # Efficiency metrics
        if "cpu_usage_rate" in df.columns:
            if "cpu_limit_cores" in df.columns:
                df["cpu_usage_vs_limit"] = df["cpu_usage_rate"] / df["cpu_limit_cores"]

        if "memory_usage_bytes" in df.columns:
            if "memory_limit_bytes" in df.columns:
                df["memory_usage_vs_limit"] = (
                    df["memory_usage_bytes"] / df["memory_limit_bytes"]
                )

        # Fill NaN values (pandas>=3 removed fillna(method=...))
        df = df.ffill().fillna(0)

        return df
