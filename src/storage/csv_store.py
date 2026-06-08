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
import os

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
        workload = self._get_workload_name(pod)
        if df.empty:
            return self.metrics_dir / f"{namespace}__{workload}.csv"

        # --- normalized file (for downstream Redis / InfluxDB consumers) ---
        out = df.copy()
        out["namespace"] = namespace
        out["pod"] = pod
        out["container"] = container or ""
        out["node"] = node or ""

        filepath = self.metrics_dir / f"{namespace}__{workload}.csv"
        header = not filepath.exists()
        out = self._align_df_columns(out, filepath)

        last_ts = self._get_last_timestamp(filepath)
        if last_ts is not None:
            out_index_dt = pd.to_datetime(out.index, format="mixed", utc=True)
            out = out[out_index_dt > last_ts]

        if not out.empty:
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

            raw_filepath = self.metrics_dir / f"{namespace}__{workload}__raw.csv"
            raw_header = not raw_filepath.exists()
            raw_out = self._align_df_columns(raw_out, raw_filepath)

            last_raw_ts = self._get_last_timestamp(raw_filepath)
            if last_raw_ts is not None:
                raw_out_index_dt = pd.to_datetime(raw_out.index, format="mixed", utc=True)
                raw_out = raw_out[raw_out_index_dt > last_raw_ts]

            if not raw_out.empty:
                raw_out.to_csv(
                    raw_filepath,
                    mode="a",
                    header=raw_header,
                    index=True,
                    index_label="timestamp",
                )

        return filepath

    def _get_last_timestamp(self, file_path: Path) -> datetime | None:
        """Efficiently read the last timestamp from a CSV file."""
        if not file_path.exists():
            return None
        try:
            with open(file_path, "rb") as f:
                try:
                    f.seek(-2, os.SEEK_END)
                    while f.read(1) != b"\n":
                        f.seek(-2, os.SEEK_CUR)
                except OSError:
                    f.seek(0)
                last_line = f.readline().decode().strip()
                if last_line:
                    parts = last_line.split(",")
                    if parts and parts[0] != "timestamp":
                        return pd.to_datetime(parts[0], format="mixed", utc=True)
        except Exception as e:
            logger.warning(f"Failed to read last timestamp from {file_path}: {e}")
        return None

    def _get_workload_name(self, pod_name: str) -> str:
        """Resolve the workload/deployment name by removing pod-specific hashes/suffixes."""
        parts = pod_name.split("-")
        if len(parts) > 2:
            return "-".join(parts[:-2])
        return pod_name

    def _align_df_columns(self, df_to_write: pd.DataFrame, file_path: Path) -> pd.DataFrame:
        """Align DataFrame columns to match either the existing CSV file or a standard schema."""
        if file_path.exists():
            try:
                with open(file_path, "r") as f:
                    header_line = f.readline().strip()
                if header_line:
                    cols = header_line.split(",")
                    if cols[0] == "timestamp":
                        cols = cols[1:]
                    # Reindex to match the file columns exactly
                    return df_to_write.reindex(columns=cols, fill_value=0.0)
            except Exception as e:
                logger.warning(f"Failed to read header from {file_path}: {e}")

        # Standard column order
        standard_cols = [
            "container_cpu_usage_seconds_total",
            "container_cpu_cfs_throttled_seconds_total",
            "container_memory_working_set_bytes",
            "container_memory_failures_total",
            "node_load1",
            "node_load5",
            "node_load15",
            "node_memory_MemAvailable_bytes",
            "node_disk_read_bytes_total",
            "node_network_transmit_bytes_total",
            "kube_pod_container_resource_requests_cpu",
            "kube_pod_container_resource_requests_memory",
            "kube_pod_container_resource_limits_cpu",
            "kube_pod_container_resource_limits_memory",
            "kube_pod_status_phase",
            "kube_pod_container_status_restarts_total",
            "derived_usage_vs_limit",
            "derived_pressure_throttled_ratio",
            "derived_cpu_volatility_5m",
            "derived_cpu_volatility_10m",
            "namespace",
            "pod",
            "container",
            "node"
        ]
        extra_cols = [c for c in df_to_write.columns if c not in standard_cols]
        target_cols = [c for c in standard_cols if c in df_to_write.columns] + extra_cols
        if not target_cols:
            return df_to_write
        return df_to_write.reindex(columns=target_cols, fill_value=0.0)

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