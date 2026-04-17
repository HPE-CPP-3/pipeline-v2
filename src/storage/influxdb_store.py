"""
InfluxDB storage for long-term telemetry, prediction audit trails,
decision logging, and feedback loop data.

Uses InfluxDB 2.x async client with structured measurements:
- metrics: raw/normalized container & node telemetry
- predictions: PatchTST forecast outputs + confidence scores
- decisions: optimization decisions with policy version
- feedback: predicted vs actual comparison for drift detection
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import numpy as np
import pandas as pd
from influxdb_client import Point, WritePrecision
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

logger = logging.getLogger(__name__)


class InfluxDBStore:
    """
    Async InfluxDB 2.x storage layer.

    Measurements:
        metrics      – raw/normalized container & node telemetry
        predictions  – PatchTST forecast outputs + confidence
        decisions    – optimization agent decisions
        feedback     – predicted vs actual (for NRMSE / bias)
    """

    # Default retention in days per measurement
    DEFAULT_RETENTION = {
        "metrics": 30,
        "predictions": 90,
        "decisions": 365,
        "feedback": 180,
    }

    def __init__(
        self,
        url: str = "http://localhost:8086",
        token: str = "dev-token-change-me",
        org: str = "pipeline-v2",
        bucket: str = "metrics",
    ):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self._client: Optional[InfluxDBClientAsync] = None

        logger.info(f"Initialized InfluxDBStore targeting {url}, org={org}, bucket={bucket}")

    async def _get_client(self) -> InfluxDBClientAsync:
        """Lazy-initialize the async client."""
        if self._client is None:
            self._client = InfluxDBClientAsync(
                url=self.url,
                token=self.token,
                org=self.org,
            )
        return self._client

    # ------------------------------------------------------------------
    # WRITE methods
    # ------------------------------------------------------------------

    async def write_metrics(
        self,
        namespace: str,
        pod: str,
        metrics: dict[str, float],
        timestamp: Optional[datetime] = None,
        container: Optional[str] = None,
        node: Optional[str] = None,
    ) -> None:
        """
        Write a single metrics snapshot to InfluxDB.

        Args:
            namespace: Kubernetes namespace
            pod: Pod name
            metrics: Dict of metric_name -> float value
            timestamp: Metric timestamp (defaults to now UTC)
            container: Optional container name tag
            node: Optional node name tag
        """
        ts = timestamp or datetime.now(timezone.utc)
        client = await self._get_client()
        write_api = client.write_api()

        point = (
            Point("metrics")
            .tag("namespace", namespace)
            .tag("pod", pod)
            .time(ts, WritePrecision.S)
        )

        if container:
            point = point.tag("container", container)
        if node:
            point = point.tag("node", node)

        for metric_name, value in metrics.items():
            try:
                point = point.field(metric_name, float(value))
            except (ValueError, TypeError):
                # Skip non-numeric fields (e.g. node name strings)
                continue

        await write_api.write(bucket=self.bucket, record=point)
        logger.debug(
            f"Wrote {len(metrics)} metric fields for {namespace}/{pod} at {ts.isoformat()}"
        )

    async def write_metrics_dataframe(
        self,
        namespace: str,
        pod: str,
        df: pd.DataFrame,
        container: Optional[str] = None,
        node: Optional[str] = None,
    ) -> None:
        """
        Write an entire DataFrame of metrics (one point per row).

        The DataFrame index must be a DatetimeIndex.
        Each numeric column becomes a field on the 'metrics' measurement.
        """
        if df.empty:
            logger.warning("write_metrics_dataframe called with empty DataFrame")
            return

        client = await self._get_client()
        write_api = client.write_api()

        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        points: list[Point] = []

        for ts, row in df.iterrows():
            point = (
                Point("metrics")
                .tag("namespace", namespace)
                .tag("pod", pod)
                .time(ts, WritePrecision.S)
            )
            if container:
                point = point.tag("container", container)
            if node:
                point = point.tag("node", node)

            for col in numeric_cols:
                val = row[col]
                if pd.notna(val):
                    point = point.field(col, float(val))

            points.append(point)

        await write_api.write(bucket=self.bucket, record=points)
        logger.info(
            f"Wrote {len(points)} metric points for {namespace}/{pod} "
            f"({len(numeric_cols)} fields each)"
        )

    async def write_prediction(
        self,
        pod: str,
        namespace: str,
        predictions: dict[str, Any],
        confidence: float,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Write a prediction result to InfluxDB for audit trail.

        Args:
            pod: Pod name
            namespace: Namespace
            predictions: Dict with forecast data (will be JSON-serialized for complex values)
            confidence: Prediction confidence score [0, 1]
            timestamp: Prediction timestamp
        """
        ts = timestamp or datetime.now(timezone.utc)
        client = await self._get_client()
        write_api = client.write_api()

        point = (
            Point("predictions")
            .tag("namespace", namespace)
            .tag("pod", pod)
            .field("confidence", float(confidence))
            .time(ts, WritePrecision.S)
        )

        # Flatten simple numeric predictions as fields, serialize complex ones
        for key, value in predictions.items():
            if isinstance(value, (int, float)):
                point = point.field(key, float(value))
            elif isinstance(value, dict):
                # Flatten nested dicts: e.g. cpu_forecast -> cpu_forecast_json
                point = point.field(f"{key}_json", json.dumps(value, default=str))
            else:
                point = point.field(key, str(value))

        await write_api.write(bucket=self.bucket, record=point)
        logger.debug(f"Wrote prediction for {namespace}/{pod} (confidence={confidence:.3f})")

    async def write_decision(
        self,
        pod: str,
        namespace: str,
        action_type: str,
        target_resource: str,
        proposed_change: dict,
        urgency: str,
        confidence: float,
        rationale: str,
        policy_version: str,
        agent: str = "optimization",
        approved_by: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Write an optimization decision to InfluxDB for audit.

        Args:
            pod: Pod name
            namespace: Namespace
            action_type: SCALE_UP / SCALE_DOWN / NO_ACTION
            target_resource: CPU_LIMIT / MEMORY_LIMIT / REPLICAS / etc.
            proposed_change: Dict with current/proposed/unit
            urgency: LOW / MEDIUM / HIGH / CRITICAL
            confidence: Decision confidence
            rationale: Human-readable reason
            policy_version: Hash of policy config at decision time
            agent: Which agent authored the decision
            approved_by: Who approved (rule_engine / llm / etc.)
            timestamp: Decision timestamp
        """
        ts = timestamp or datetime.now(timezone.utc)
        client = await self._get_client()
        write_api = client.write_api()

        point = (
            Point("decisions")
            .tag("namespace", namespace)
            .tag("pod", pod)
            .tag("agent", agent)
            .tag("action_type", action_type)
            .tag("target_resource", target_resource)
            .tag("urgency", urgency)
            .field("confidence", float(confidence))
            .field("rationale", rationale)
            .field("policy_version", policy_version)
            .field("proposed_change_json", json.dumps(proposed_change, default=str))
            .time(ts, WritePrecision.S)
        )

        if approved_by:
            point = point.tag("approved_by", approved_by)

        await write_api.write(bucket=self.bucket, record=point)
        logger.info(
            f"Wrote decision for {namespace}/{pod}: {action_type} {target_resource} "
            f"(urgency={urgency}, agent={agent})"
        )

    async def write_feedback(
        self,
        pod: str,
        namespace: str,
        predicted: float,
        actual: float,
        metric_name: str = "cpu_p90",
        timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Write prediction-vs-actual feedback for drift detection.

        Args:
            pod: Pod name
            namespace: Namespace
            predicted: Predicted value
            actual: Actual measured value
            metric_name: Which metric (cpu_p90, memory_p90, etc.)
            timestamp: Feedback timestamp
        """
        ts = timestamp or datetime.now(timezone.utc)
        client = await self._get_client()
        write_api = client.write_api()

        error = actual - predicted
        absolute_error = abs(error)

        point = (
            Point("feedback")
            .tag("namespace", namespace)
            .tag("pod", pod)
            .tag("metric_name", metric_name)
            .field("predicted", float(predicted))
            .field("actual", float(actual))
            .field("error", float(error))
            .field("absolute_error", float(absolute_error))
            .time(ts, WritePrecision.S)
        )

        await write_api.write(bucket=self.bucket, record=point)
        logger.debug(
            f"Wrote feedback for {namespace}/{pod}/{metric_name}: "
            f"predicted={predicted:.4f}, actual={actual:.4f}, error={error:.4f}"
        )

    # ------------------------------------------------------------------
    # READ / QUERY methods
    # ------------------------------------------------------------------

    async def get_historical_window(
        self,
        pod: str,
        namespace: str,
        window_minutes: int = 90,
    ) -> pd.DataFrame:
        """
        Get historical metrics for a pod within a time window.

        Args:
            pod: Pod name
            namespace: Namespace
            window_minutes: How far back to query

        Returns:
            DataFrame indexed by timestamp with metric columns
        """
        client = await self._get_client()
        query_api = client.query_api()

        flux_query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: -{window_minutes}m)
            |> filter(fn: (r) => r._measurement == "metrics")
            |> filter(fn: (r) => r.namespace == "{namespace}")
            |> filter(fn: (r) => r.pod == "{pod}")
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        """

        try:
            tables = await query_api.query(flux_query)
            records = []
            for table in tables:
                for record in table.records:
                    row = record.values.copy()
                    records.append(row)

            if not records:
                logger.debug(f"No historical data for {namespace}/{pod} in last {window_minutes}m")
                return pd.DataFrame()

            df = pd.DataFrame(records)

            # Clean up InfluxDB metadata columns
            drop_cols = [
                c for c in df.columns
                if c.startswith("_") or c in ("result", "table")
            ]
            df = df.drop(columns=drop_cols, errors="ignore")

            # Set time as index if present
            if "_time" not in drop_cols and "time" in df.columns:
                df = df.set_index("time")
            elif "_time" in df.columns:
                df = df.set_index("_time")

            # Drop tag columns that are redundant (already filtered)
            df = df.drop(columns=["namespace", "pod", "container", "node"], errors="ignore")

            return df.sort_index()

        except Exception as e:
            logger.error(f"InfluxDB query failed for get_historical_window: {e}")
            return pd.DataFrame()

    async def get_prediction_history(
        self,
        pod: str,
        namespace: str,
        hours: int = 24,
    ) -> pd.DataFrame:
        """
        Get prediction history for a pod.

        Returns:
            DataFrame with prediction records (confidence, forecasts)
        """
        client = await self._get_client()
        query_api = client.query_api()

        flux_query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: -{hours}h)
            |> filter(fn: (r) => r._measurement == "predictions")
            |> filter(fn: (r) => r.namespace == "{namespace}")
            |> filter(fn: (r) => r.pod == "{pod}")
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        """

        try:
            tables = await query_api.query(flux_query)
            records = []
            for table in tables:
                for record in table.records:
                    records.append(record.values.copy())

            if not records:
                return pd.DataFrame()

            df = pd.DataFrame(records)
            drop_cols = [c for c in df.columns if c.startswith("_") or c in ("result", "table")]
            df = df.drop(columns=drop_cols, errors="ignore")

            if "_time" in df.columns:
                df = df.set_index("_time")

            return df.sort_index()

        except Exception as e:
            logger.error(f"InfluxDB query failed for get_prediction_history: {e}")
            return pd.DataFrame()

    async def get_decisions_without_feedback(
        self,
        min_age_minutes: int = 5,
    ) -> pd.DataFrame:
        """
        Reconciler query: find approved decisions that are old enough
        to have stabilized but haven't received feedback yet.

        This enables the Monitoring Agent to act as a durable reconciler
        instead of relying on ephemeral asyncio.sleep tasks.

        Returns:
            DataFrame of decisions needing feedback
        """
        client = await self._get_client()
        query_api = client.query_api()

        # Get decisions from the last 2 hours that are at least min_age_minutes old
        flux_query = f"""
        decisions = from(bucket: "{self.bucket}")
            |> range(start: -2h, stop: -{min_age_minutes}m)
            |> filter(fn: (r) => r._measurement == "decisions")
            |> filter(fn: (r) => r.action_type != "NO_ACTION")
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])

        decisions
        """

        try:
            tables = await query_api.query(flux_query)
            records = []
            for table in tables:
                for record in table.records:
                    records.append(record.values.copy())

            if not records:
                return pd.DataFrame()

            df = pd.DataFrame(records)
            drop_cols = [c for c in df.columns if c.startswith("_") or c in ("result", "table")]
            df = df.drop(columns=drop_cols, errors="ignore")

            return df

        except Exception as e:
            logger.error(f"InfluxDB query failed for get_decisions_without_feedback: {e}")
            return pd.DataFrame()

    async def calculate_nrmse(
        self,
        pod: str,
        namespace: str,
        hours: int = 24,
    ) -> float:
        """
        Calculate Normalized Root Mean Square Error for recent predictions.

        NRMSE = RMSE / range(actual)

        Args:
            pod: Pod name
            namespace: Namespace
            hours: Lookback window

        Returns:
            NRMSE value (0.0 if no feedback data)
        """
        client = await self._get_client()
        query_api = client.query_api()

        flux_query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: -{hours}h)
            |> filter(fn: (r) => r._measurement == "feedback")
            |> filter(fn: (r) => r.namespace == "{namespace}")
            |> filter(fn: (r) => r.pod == "{pod}")
            |> filter(fn: (r) => r._field == "error" or r._field == "actual")
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        """

        try:
            tables = await query_api.query(flux_query)
            records = []
            for table in tables:
                for record in table.records:
                    records.append(record.values.copy())

            if not records:
                return 0.0

            df = pd.DataFrame(records)

            if "error" not in df.columns or "actual" not in df.columns:
                return 0.0

            errors = df["error"].astype(float)
            actuals = df["actual"].astype(float)

            mse = float(np.mean(errors ** 2))
            rmse = float(np.sqrt(mse))

            actual_range = float(actuals.max() - actuals.min())
            if actual_range < 1e-6:
                return 0.0

            return rmse / actual_range

        except Exception as e:
            logger.error(f"InfluxDB query failed for calculate_nrmse: {e}")
            return 0.0

    async def calculate_bias(
        self,
        pod: str,
        namespace: str,
        hours: int = 24,
    ) -> float:
        """
        Calculate prediction bias (systematic over/under-prediction).

        Positive bias = over-prediction, Negative = under-prediction.

        Args:
            pod: Pod name
            namespace: Namespace
            hours: Lookback window

        Returns:
            Mean bias value (0.0 if no data)
        """
        client = await self._get_client()
        query_api = client.query_api()

        flux_query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: -{hours}h)
            |> filter(fn: (r) => r._measurement == "feedback")
            |> filter(fn: (r) => r.namespace == "{namespace}")
            |> filter(fn: (r) => r.pod == "{pod}")
            |> filter(fn: (r) => r._field == "predicted" or r._field == "actual")
            |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        """

        try:
            tables = await query_api.query(flux_query)
            records = []
            for table in tables:
                for record in table.records:
                    records.append(record.values.copy())

            if not records:
                return 0.0

            df = pd.DataFrame(records)

            if "predicted" not in df.columns or "actual" not in df.columns:
                return 0.0

            predicted = df["predicted"].astype(float)
            actual = df["actual"].astype(float)

            return float(np.mean(predicted - actual))

        except Exception as e:
            logger.error(f"InfluxDB query failed for calculate_bias: {e}")
            return 0.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def health_check(self) -> bool:
        """Check if InfluxDB is reachable."""
        try:
            client = await self._get_client()
            ready = await client.ping()
            return ready
        except Exception as e:
            logger.error(f"InfluxDB health check failed: {e}")
            return False

    async def close(self) -> None:
        """Close the async client."""
        if self._client is not None:
            await self._client.close()
            self._client = None
            logger.info("InfluxDB client closed")
