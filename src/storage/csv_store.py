"""CSV storage for live append-only telemetry and predictions."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import logging

import pandas as pd

logger = logging.getLogger(__name__)


class CSVStore:
    """Live CSV writer for ingestion and prediction outputs."""

    def __init__(self, base_path: str = "data/csv"):
        self.base_path = Path(base_path)
        self.metrics_dir = self.base_path / "metrics"
        self.predictions_dir = self.base_path / "predictions"
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        self.predictions_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized CSVStore at {self.base_path}")

    async def write_metrics_dataframe(
        self,
        namespace: str,
        pod: str,
        df: pd.DataFrame,
        container: str | None = None,
        node: str | None = None,
    ) -> Path:
        """Append a metrics DataFrame to a pod-specific CSV file."""
        if df.empty:
            return self.metrics_dir / f"{namespace}__{pod}.csv"

        out = df.copy()
        out["namespace"] = namespace
        out["pod"] = pod
        out["container"] = container or ""
        out["node"] = node or ""

        filepath = self.metrics_dir / f"{namespace}__{pod}.csv"
        header = not filepath.exists()
        out.to_csv(
            filepath, mode="a", header=header, index=True, index_label="timestamp"
        )
        return filepath

    async def write_prediction(
        self,
        namespace: str,
        pod: str,
        container: str | None,
        cpu_forecast: dict,
        memory_forecast: dict,
        confidence: float,
        timestamp: datetime | None = None,
    ) -> Path:
        """Append a prediction record to CSV."""
        ts = timestamp or datetime.now(timezone.utc)
        row = {
            "timestamp": ts.isoformat(),
            "namespace": namespace,
            "pod": pod,
            "container": container or "",
            "confidence": float(confidence),
            "cpu_forecast_json": json.dumps(cpu_forecast, default=str),
            "memory_forecast_json": json.dumps(memory_forecast, default=str),
        }
        filepath = self.predictions_dir / f"{namespace}__{pod}.csv"
        header = not filepath.exists()
        pd.DataFrame([row]).to_csv(filepath, mode="a", header=header, index=False)
        return filepath
