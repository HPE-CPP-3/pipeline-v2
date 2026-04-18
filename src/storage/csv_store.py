"""CSV storage for live append-only telemetry and predictions.

IMPORTANT: write_metrics_dataframe writes TWO files per pod:
  - {namespace}__{pod}.csv          -- normalized features (as before, for fast Redis replay)
  - {namespace}__{pod}__raw.csv     -- raw (pre-normalization) metrics for fine-tuning

The fine-tuner reads the __raw.csv so that engineer_features() and
generate_risk_labels() operate on original-scale data, exactly like train.py.
"""

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

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    async def write_metrics_dataframe(
        self,
        namespace: str,
        pod: str,
        df: pd.DataFrame,
        container: str | None = None,
        node: str | None = None,
        raw_df: pd.DataFrame | None = None,
    ) -> Path:
        """
        Append a metrics DataFrame to pod-specific CSV files.

        Args:
            namespace: K8s namespace
            pod: Pod name
            df: Normalized feature DataFrame (index = timestamp)
            container: Optional container name
            node: Optional node name
            raw_df: Optional un-normalized DataFrame for fine-tuning.
                    If None, df is written as-is to the raw file too
                    (caller should pass the pre-normalization frame).
        """
        if df.empty:
            return self.metrics_dir / f"{namespace}__{pod}.csv"

        # --- normalized file (for downstream Redis / InfluxDB consumers) ---
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

        # --- raw file (for incremental fine-tuner) ---
        raw_frame = raw_df if raw_df is not None else df
        if not raw_frame.empty:
            raw_out = raw_frame.copy()
            raw_out["namespace"] = namespace
            raw_out["pod"] = pod
            raw_out["container"] = container or ""
            raw_out["node"] = node or ""

            raw_filepath = self.metrics_dir / f"{namespace}__{pod}__raw.csv"
            raw_header = not raw_filepath.exists()
            raw_out.to_csv(
                raw_filepath,
                mode="a",
                header=raw_header,
                index=True,
                index_label="timestamp",
            )

        return filepath

    # ------------------------------------------------------------------
    # Predictions
    # ------------------------------------------------------------------

    async def write_prediction(
        self,
        namespace: str,
        pod: str,
        container: str | None,
        cpu_forecast: dict,
        memory_forecast: dict,
        confidence: float,
        timestamp: datetime | None = None,
        # --- Option A risk fields ---
        throttle_prob: float = 0.0,
        oom_prob: float = 0.0,
        throttle_risk_level: str = "",
        throttle_time_to_event: str = "",
        throttle_reason: str = "",
        oom_risk_level: str = "",
        oom_estimated_time: str = "",
        oom_reason: str = "",
    ) -> Path:
        """Append a prediction record to CSV.

        The base forecast columns are preserved for backward compatibility.
        The risk columns (throttle_*, oom_*) are new and populated only when
        the two-layer Option A calculators are wired in prediction_agent.py.
        They default to empty strings / 0.0 so existing callers don't break.
        """
        ts = timestamp or datetime.now(timezone.utc)
        row = {
            "timestamp": ts.isoformat(),
            "namespace": namespace,
            "pod": pod,
            "container": container or "",
            "confidence": float(confidence),
            "cpu_forecast_json": json.dumps(cpu_forecast, default=str),
            "memory_forecast_json": json.dumps(memory_forecast, default=str),
            # Model risk probabilities (raw sigmoid outputs)
            "throttle_prob": round(float(throttle_prob), 4),
            "oom_prob": round(float(oom_prob), 4),
            # Rule-enriched throttle risk
            "throttle_risk_level": throttle_risk_level,
            "throttle_time_to_event": throttle_time_to_event,
            "throttle_reason": throttle_reason,
            # Rule-enriched OOM risk
            "oom_risk_level": oom_risk_level,
            "oom_estimated_time": oom_estimated_time,
            "oom_reason": oom_reason,
        }
        filepath = self.predictions_dir / f"{namespace}__{pod}.csv"
        header = not filepath.exists()
        pd.DataFrame([row]).to_csv(filepath, mode="a", header=header, index=False)
        return filepath