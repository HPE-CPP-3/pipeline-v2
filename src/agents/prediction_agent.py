"""Independent workload prediction agent.

Consumes Stage-1 completion events from Redis Stream, fetches
24h seasonal context from InfluxDB, runs PatchTST inference,
computes confidence, then writes forecast back to Redis.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging

import numpy as np
import pandas as pd

from ..prediction import CPUPredictor, MemoryPredictor
from ..storage.csv_store import CSVStore
from ..storage.influxdb_store import InfluxDBStore
from ..storage.redis_store import RedisStore

logger = logging.getLogger(__name__)


class WorkloadPredictionAgent:
    """Event-driven predictor that runs independently from ingestion."""

    def __init__(
        self,
        redis_store: RedisStore,
        influxdb_store: InfluxDBStore,
        csv_store: CSVStore | None,
        cpu_predictor: CPUPredictor,
        memory_predictor: MemoryPredictor,
    ):
        self.redis_store = redis_store
        self.influxdb_store = influxdb_store
        self.csv_store = csv_store
        self.cpu_predictor = cpu_predictor
        self.memory_predictor = memory_predictor

    async def run_loop(self) -> None:
        """Wake up only when Stage 1 emits ingestion completion."""
        last_id = "$"
        while True:
            messages = await self.redis_store.read_stream_messages(
                stream_name="stream:ingestion:complete",
                last_id=last_id,
                block_ms=5000,
                count=10,
            )
            for msg_id, msg in messages:
                last_id = msg_id
                await self._handle_ingestion_complete(msg)

    async def _handle_ingestion_complete(self, msg: dict[str, str]) -> None:
        namespace = msg.get("namespace", "")
        pod = msg.get("pod", "")
        container = msg.get("container", "") or None

        features_json = msg.get("features_json", "{}")
        latest_features = json.loads(features_json)

        # Redis latest feature vector
        latest_df = pd.DataFrame([latest_features])

        # 24-hour seasonal context from InfluxDB
        historical = await self.influxdb_store.get_historical_window(
            pod=pod,
            namespace=namespace,
            window_minutes=24 * 60,
        )

        # If empty history, still run with current snapshot
        if historical.empty:
            historical = latest_df.copy()

        model_input = self._build_model_input(historical, latest_df)

        cpu_forecast = self.cpu_predictor.predict(model_input)
        memory_forecast = self.memory_predictor.predict(model_input)

        confidence = self._compute_confidence(latest_df, historical)

        forecast_payload = {
            "namespace": namespace,
            "pod": pod,
            "container": container or "",
            "cpu_forecast": self._stringify_forecast(cpu_forecast),
            "memory_forecast": self._stringify_forecast(memory_forecast),
            "confidence": confidence,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        await self.redis_store.write_stream_message(
            stream_name="stream:prediction:complete",
            payload={
                "namespace": namespace,
                "pod": pod,
                "container": container or "",
                "forecast_json": json.dumps(forecast_payload),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

        await self.redis_store.store_features(
            pod=pod,
            namespace=namespace,
            features={
                "prediction_confidence": confidence,
            },
            timestamp=datetime.now(timezone.utc),
        )

        await self.influxdb_store.write_prediction(
            pod=pod,
            namespace=namespace,
            predictions={
                "cpu_forecast": self._stringify_forecast(cpu_forecast),
                "memory_forecast": self._stringify_forecast(memory_forecast),
            },
            confidence=confidence,
        )

        if self.csv_store is not None:
            await self.csv_store.write_prediction(
                namespace=namespace,
                pod=pod,
                container=container,
                cpu_forecast=self._stringify_forecast(cpu_forecast),
                memory_forecast=self._stringify_forecast(memory_forecast),
                confidence=confidence,
            )

    def _build_model_input(
        self, historical: pd.DataFrame, latest_df: pd.DataFrame
    ) -> np.ndarray:
        numeric_hist = historical.select_dtypes(include=[np.number])
        if numeric_hist.empty:
            return np.zeros((1, 90, 1), dtype=float)

        # Keep a single target channel for current model API
        if "container_cpu_usage_seconds_total" in numeric_hist.columns:
            series = numeric_hist["container_cpu_usage_seconds_total"].astype(float)
        else:
            series = numeric_hist.iloc[:, 0].astype(float)

        # Last 90 steps expected
        arr = series.tail(90).to_numpy(dtype=float)
        if arr.shape[0] < 90:
            pad = np.zeros(90 - arr.shape[0], dtype=float)
            arr = np.concatenate([pad, arr])

        return arr.reshape(1, 90, 1)

    def _compute_confidence(
        self, latest_df: pd.DataFrame, historical: pd.DataFrame
    ) -> float:
        """Simple deviation-based confidence: higher deviation => lower confidence."""
        h = historical.select_dtypes(include=[np.number])
        l = latest_df.select_dtypes(include=[np.number])
        if h.empty or l.empty:
            return 0.5

        cols = [c for c in l.columns if c in h.columns]
        if not cols:
            return 0.5

        zscores: list[float] = []
        for c in cols:
            mu = float(h[c].mean())
            sigma = float(h[c].std())
            x = float(l[c].iloc[-1])
            if sigma <= 1e-9:
                z = 0.0
            else:
                z = abs(x - mu) / sigma
            zscores.append(z)

        avg_z = float(np.mean(zscores)) if zscores else 0.0
        confidence = 1.0 / (1.0 + avg_z)
        return max(0.0, min(1.0, confidence))

    def _stringify_forecast(
        self,
        forecast: dict[int, dict[float, float]],
    ) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        for h, qmap in forecast.items():
            out[str(h)] = {str(q): float(v) for q, v in qmap.items()}
        return out
