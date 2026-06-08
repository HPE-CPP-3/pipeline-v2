"""Independent workload prediction agent.

Consumes Stage-1 completion events from Redis Stream, fetches
24h seasonal context from InfluxDB, runs PatchTST inference,
computes confidence, then writes forecast back to Redis.

After every `finetune_every_n_steps` ingestion events, triggers
incremental fine-tuning on recent CSV data and hot-reloads weights.

Option A wiring (two-layer risk):
  1. cpu_predictor.predict_all()  → cpu_forecast + throttle_prob (from model logit)
  2. memory_predictor.predict_all() → memory_forecast + oom_prob (from model logit)
  3. ThrottleRiskCalculator.calculate_risk_from_model_prob() → enriched throttle risk
  4. OOMRiskCalculator.calculate_risk_from_model_prob()      → enriched OOM risk
  Raw limits are read from the stream message (published by ingestion_agent
  before rolling-minmax normalisation destroys them).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
import time
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from ..prediction import CPUPredictor, MemoryPredictor
from ..prediction.throttle_risk import ThrottleRiskCalculator
from ..prediction.oom_risk import OOMRiskCalculator
from ..storage.csv_store import CSVStore
from ..storage.influxdb_store import InfluxDBStore
from ..storage.redis_store import RedisStore
from ..training.incremental_trainer import IncrementalTrainer, IncrementalConfig

logger = logging.getLogger(__name__)

# Default risk calculator config — mirrors configs/prediction.yaml
_DEFAULT_THROTTLE_CFG = {
    "critical_ratio": 0.95,
    "high_ratio": 0.85,
    "current_throttle_ratio_high": 0.1,
}
_DEFAULT_OOM_CFG = {
    "critical_ratio": 0.95,
    "high_ratio": 0.85,
    "failcnt_threshold": 1,
    "growth_rate_high": 0.1,
}


class WorkloadPredictionAgent:
    """Event-driven predictor with periodic incremental fine-tuning."""

    def __init__(
        self,
        redis_store: RedisStore,
        influxdb_store: InfluxDBStore,
        csv_store: CSVStore | None,
        cpu_predictor: CPUPredictor,
        memory_predictor: MemoryPredictor,
        # --- fine-tune knobs ---
        finetune_every_n_steps: int = 60,       # trigger after 60 ingestion events (~1h)
        finetune_rows: int = 500,               # rows of recent CSV to fine-tune on
        checkpoint_path: str = "models/patchtst_multi.pt",
        csv_metrics_path: str = "data/csv/metrics",
        # --- guard rails ---
        global_cooldown_seconds: int = 1800,    # 30 min between retrains
        max_retrains_per_day: int = 8,
        # --- risk calculator config ---
        throttle_risk_config: dict | None = None,
        oom_risk_config: dict | None = None,
    ):
        self.redis_store = redis_store
        self.influxdb_store = influxdb_store
        self.csv_store = csv_store
        self.cpu_predictor = cpu_predictor
        self.memory_predictor = memory_predictor

        # Fine-tune config
        self.finetune_every_n_steps = finetune_every_n_steps
        self.csv_metrics_path = Path(csv_metrics_path)
        self._step_counter: int = 0

        # Guard rail state
        self._global_cooldown_seconds = global_cooldown_seconds
        self._max_retrains_per_day = max_retrains_per_day
        self._last_finetune_ts: float = 0.0
        self._retrains_today: int = 0
        self._retrains_day: int = -1  # day-of-year when counter was last reset

        # Trainer (lazy init so we don't load train.py until needed)
        self._trainer: Optional[IncrementalTrainer] = None
        self._incremental_cfg = IncrementalConfig(
            checkpoint_path=checkpoint_path,
            finetune_rows=finetune_rows,
        )

        # Risk calculators (Option A: model prob + rule-based enrichment)
        self._throttle_calc = ThrottleRiskCalculator(
            throttle_risk_config or _DEFAULT_THROTTLE_CFG
        )
        self._oom_calc = OOMRiskCalculator(
            oom_risk_config or _DEFAULT_OOM_CFG
        )

        self._train_mod = None
        self._feature_cols: list[str] = []
        self._mu: dict = {}
        self._sigma: dict = {}
        self._ckpt_mtime: float = 0.0

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Per-event handler
    # ------------------------------------------------------------------

    async def _handle_ingestion_complete(self, msg: dict[str, str]) -> None:
        namespace = msg.get("namespace", "")
        pod = msg.get("pod", "")
        container = msg.get("container", "") or None

        features_json = msg.get("features_json", "{}")
        latest_features = json.loads(features_json)

        # DEBUG: Check for NaN in features
        import math
        nan_keys = [k for k, v in latest_features.items() if isinstance(v, float) and math.isnan(v)]
        if nan_keys:
            logger.warning(f"NaN values in features: {nan_keys}")

        # Raw limits published by ingestion_agent before normalisation
        raw_limits: dict = json.loads(msg.get("raw_limits_json", "{}"))
        cpu_limit: float = raw_limits.get("cpu_limit", 0.0)
        memory_limit: float = raw_limits.get("memory_limit", 0.0)
        current_throttle_ratio: float = raw_limits.get("throttle_ratio", 0.0)
        current_failcnt: int = int(raw_limits.get("memory_failcnt", 0))

        # DEBUG: Log raw limits
        logger.info(f"Raw limits: cpu={cpu_limit}, mem={memory_limit}, throttle={current_throttle_ratio}, failcnt={current_failcnt}")

        # Redis latest feature vector
        latest_df = pd.DataFrame([latest_features])

        # 24-hour seasonal context from InfluxDB
        historical = self._load_recent_csv_for_inference(namespace, pod, n_rows=90)
        if historical is None or historical.empty:
            historical = latest_df.copy()
        model_input = self._build_model_input(historical, latest_df)

        # ------------------------------------------------------------------
        # Option A two-layer inference
        # ------------------------------------------------------------------
        # Layer 1: model forward pass — get forecasts AND learned risk logits
        cpu_forecast, throttle_prob = self._safe_predict_all_cpu(model_input)
        memory_forecast, oom_prob = self._safe_predict_all_memory(model_input)

        # Layer 2: rule-based calculators enriched with model probabilities
        throttle_risk: dict = {}
        oom_risk: dict = {}

        if cpu_limit > 0:
            throttle_risk = self._throttle_calc.calculate_risk_from_model_prob(
                throttle_prob=throttle_prob,
                cpu_forecast=cpu_forecast,
                cpu_limit=cpu_limit,
                current_throttle_ratio=current_throttle_ratio or None,
            )
            logger.info(
                f"[{namespace}/{pod}] throttle_risk={throttle_risk['risk_level']} "
                f"prob={throttle_risk['probability']:.3f} "
                f"model_prob={throttle_prob:.3f} "
                f"reason={throttle_risk['reason']}"
            )
        else:
            logger.debug(
                f"[{namespace}/{pod}] cpu_limit unknown, skipping ThrottleRiskCalculator"
            )

        if memory_limit > 0:
            oom_risk = self._oom_calc.calculate_risk_from_model_prob(
                oom_prob=oom_prob,
                memory_forecast=memory_forecast,
                memory_limit=memory_limit,
                current_failcnt=current_failcnt,
            )
            logger.info(
                f"[{namespace}/{pod}] oom_risk={oom_risk['oom_risk']} "
                f"prob={oom_risk['probability']:.3f} "
                f"model_prob={oom_prob:.3f} "
                f"reason={oom_risk['reason']}"
            )
        else:
            logger.debug(
                f"[{namespace}/{pod}] memory_limit unknown, skipping OOMRiskCalculator"
            )

        confidence = self._compute_confidence(latest_df, historical)

        # Build full forecast payload (forecasts + risk assessments)
        forecast_payload = {
            "namespace": namespace,
            "pod": pod,
            "container": container or "",
            # Embed raw limits for downstream decision agents (ratios/guard-rails)
            "cpu_limit": cpu_limit,
            "memory_limit": memory_limit,
            "cpu_forecast": self._stringify_forecast(cpu_forecast),
            "memory_forecast": self._stringify_forecast(memory_forecast),
            "throttle_prob": round(throttle_prob, 4),
            "oom_prob": round(oom_prob, 4),
            "throttle_risk": throttle_risk,
            "oom_risk": oom_risk,
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
                "throttle_prob": round(throttle_prob, 4),
                "oom_prob": round(oom_prob, 4),
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
                throttle_prob=throttle_prob,
                oom_prob=oom_prob,
                throttle_risk_level=throttle_risk.get("risk_level", ""),
                throttle_time_to_event=throttle_risk.get("time_to_throttle", ""),
                throttle_reason=throttle_risk.get("reason", ""),
                oom_risk_level=oom_risk.get("oom_risk", ""),
                oom_estimated_time=oom_risk.get("estimated_time", ""),
                oom_reason=oom_risk.get("reason", ""),
            )

        # ---------------------------------------------------------------
        # Increment step counter and maybe trigger fine-tuning
        # ---------------------------------------------------------------
        self._step_counter += 1
        if self._step_counter % self.finetune_every_n_steps == 0:
            logger.info(
                f"Step {self._step_counter}: triggering incremental fine-tune "
                f"(every {self.finetune_every_n_steps} steps)"
            )
            # Run in executor so we don't block the event loop during training
            await asyncio.get_event_loop().run_in_executor(
                None,
                self._run_finetune,
                namespace,
                pod,
            )

    def _load_recent_csv_for_inference(
        self, namespace: str, pod: str, n_rows: int = 90
    ) -> Optional[pd.DataFrame]:
        """Load raw (pre-normalization) CSV for building model input."""
        csv_file = self.csv_metrics_path / f"{namespace}__{pod}__raw.csv"
        if not csv_file.exists():
            logger.debug(f"Raw CSV not found for inference: {csv_file}")
            return None
        try:
            # Some historical CSVs were written with different schemas; skip malformed lines.
            df = pd.read_csv(csv_file, on_bad_lines="skip")
            if "timestamp" not in df.columns:
                if "Unnamed: 0" in df.columns:
                    df = df.rename(columns={"Unnamed: 0": "timestamp"})
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df = df.sort_values("timestamp").reset_index(drop=True)
            return df.tail(n_rows).reset_index(drop=True)
        except Exception as e:
            logger.error(f"Failed to load raw CSV for inference {csv_file}: {e}")
            return None

    # ------------------------------------------------------------------
    # Safe predict_all wrappers (fall back to predict() if model is None)
    # ------------------------------------------------------------------

    def _safe_predict_all_cpu(
        self, model_input: np.ndarray
    ) -> tuple[dict[int, dict[float, float]], float]:
        """Call cpu_predictor.predict_all(); fall back gracefully if model is missing."""
        if self.cpu_predictor.model is None:
            forecast = self.cpu_predictor.predict(model_input)
            return forecast, 0.0
        try:
            return self.cpu_predictor.predict_all(model_input)
        except Exception as exc:
            logger.warning(f"predict_all (CPU) failed, falling back: {exc}")
            return self.cpu_predictor.predict(model_input), 0.0

    def _safe_predict_all_memory(
        self, model_input: np.ndarray
    ) -> tuple[dict[int, dict[float, float]], float]:
        """Call memory_predictor.predict_all(); fall back gracefully if model is missing."""
        if self.memory_predictor.model is None:
            forecast = self.memory_predictor.predict(model_input)
            return forecast, 0.0
        try:
            return self.memory_predictor.predict_all(model_input)
        except Exception as exc:
            logger.warning(f"predict_all (memory) failed, falling back: {exc}")
            return self.memory_predictor.predict(model_input), 0.0

    # ------------------------------------------------------------------
    # Fine-tune orchestration (runs in thread pool)
    # ------------------------------------------------------------------

    def _run_finetune(self, namespace: str, pod: str) -> None:
        """
        Called in a thread executor. Loads recent CSV, fine-tunes, hot-reloads.
        Guard rails: cooldown + daily cap enforced here.
        """
        # --- Guard: daily cap ---
        today = datetime.now().timetuple().tm_yday
        if today != self._retrains_day:
            self._retrains_today = 0
            self._retrains_day = today

        if self._retrains_today >= self._max_retrains_per_day:
            logger.info(
                f"fine-tune skipped: daily cap reached ({self._retrains_today}/{self._max_retrains_per_day})"
            )
            return

        # --- Guard: global cooldown ---
        since_last = time.time() - self._last_finetune_ts
        if since_last < self._global_cooldown_seconds:
            logger.info(
                f"fine-tune skipped: cooldown ({since_last:.0f}s < {self._global_cooldown_seconds}s)"
            )
            return

        # --- Load recent CSV rows ---
        recent_df = self._load_recent_csv(namespace, pod)
        if recent_df is None or len(recent_df) < self._incremental_cfg.min_rows:
            logger.warning("fine-tune skipped: not enough CSV rows.")
            return

        # --- Run fine-tune ---
        trainer = self._get_trainer()
        result = trainer.fine_tune(recent_df)

        if result is not None:
            # Hot-reload into running predictors
            trainer.load_model_into_predictors(self.cpu_predictor, self.memory_predictor)
            self._last_finetune_ts = time.time()
            self._retrains_today += 1
            logger.info(
                f"Fine-tune #day={self._retrains_today} complete. "
                f"Weights hot-reloaded into predictors."
            )

    def _load_recent_csv(self, namespace: str, pod: str) -> Optional[pd.DataFrame]:
        """Load the pod's RAW (pre-normalization) metrics CSV for fine-tuning."""
        # Prefer the raw file written by ingestion_agent; fall back to normalized
        csv_file = self.csv_metrics_path / f"{namespace}__{pod}__raw.csv"
        if not csv_file.exists():
            csv_file = self.csv_metrics_path / f"{namespace}__{pod}.csv"
        if not csv_file.exists():
            logger.warning(f"CSV not found: {csv_file}")
            return None
        try:
            # Some historical CSVs were written with different schemas; skip malformed lines.
            df = pd.read_csv(csv_file, on_bad_lines="skip")
            # Rename index column back to timestamp if needed
            if "timestamp" not in df.columns and df.index.name == "timestamp":
                df = df.reset_index()
            elif "timestamp" not in df.columns and "Unnamed: 0" in df.columns:
                df = df.rename(columns={"Unnamed: 0": "timestamp"})
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df = df.sort_values("timestamp").reset_index(drop=True)
            n = self._incremental_cfg.finetune_rows
            return df.tail(n).reset_index(drop=True)
        except Exception as e:
            logger.error(f"Failed to load CSV {csv_file}: {e}")
            return None

    def _get_trainer(self) -> IncrementalTrainer:
        if self._trainer is None:
            self._trainer = IncrementalTrainer(self._incremental_cfg)
        return self._trainer

    # ------------------------------------------------------------------
    # Inference helpers
    # ------------------------------------------------------------------

    # In src/agents/prediction_agent.py, replace _build_model_input with:

    # In src/agents/prediction_agent.py, replace the entire _build_model_input method:

    def _ensure_inference_cache(self) -> bool:
        """Load train_mod + checkpoint into cache. Reload only if checkpoint changed on disk."""
        import torch
        import importlib.util
        import sys
        from pathlib import Path

        checkpoint_path = Path(self._incremental_cfg.checkpoint_path)
        if not checkpoint_path.exists():
            logger.warning(f"Checkpoint not found: {checkpoint_path}")
            return False

        current_mtime = checkpoint_path.stat().st_mtime
        if current_mtime == self._ckpt_mtime and self._train_mod is not None:
            return True  # cache is fresh, nothing to do

        # --- (re)load train.py ---
        if self._train_mod is None:
            candidates = [
                Path("train.py"),
                Path(__file__).resolve().parents[3] / "train.py",
            ]
            for p in candidates:
                if p.exists():
                    spec = importlib.util.spec_from_file_location("train_module", str(p))
                    mod = importlib.util.module_from_spec(spec)
                    sys.modules["train_module"] = mod
                    spec.loader.exec_module(mod)
                    self._train_mod = mod
                    logger.info(f"Loaded train.py from {p}")
                    break

        if self._train_mod is None:
            logger.error("train.py not found")
            return False

        # --- (re)load checkpoint metadata ---
        ckpt = torch.load(checkpoint_path, map_location="cpu")
        self._feature_cols = ckpt.get("feature_cols", [])
        self._mu = ckpt.get("mu", {})
        self._sigma = ckpt.get("sigma", {})
        self._ckpt_mtime = current_mtime
        logger.info(f"Cached checkpoint metadata: {len(self._feature_cols)} features")
        return True


    def _build_model_input(
        self, historical: pd.DataFrame, latest_df: pd.DataFrame
    ) -> np.ndarray:
        """Build input tensor matching training feature dimensions and normalization."""
        if not self._ensure_inference_cache():
            return np.zeros((1, 60, 26), dtype=np.float32)

        feature_cols = self._feature_cols
        n_features = len(feature_cols)

        if not feature_cols:
            logger.warning("No feature_cols in cache")
            return np.zeros((1, 60, 26), dtype=np.float32)

        # --- feature engineering ---
        df_to_engineer = historical.copy()
        if "timestamp" not in df_to_engineer.columns:
            if df_to_engineer.index.name == "timestamp":
                df_to_engineer = df_to_engineer.reset_index()
            elif isinstance(df_to_engineer.index, pd.DatetimeIndex):
                df_to_engineer = df_to_engineer.reset_index().rename(
                    columns={"index": "timestamp"}
                )
            else:
                df_to_engineer["timestamp"] = pd.to_datetime(df_to_engineer.index)

        try:
            engineered = self._train_mod.engineer_features(df_to_engineer)
        except Exception as e:
            logger.warning(f"Feature engineering failed: {e}, using raw data")
            engineered = df_to_engineer

        # --- align columns ---
        available_cols = [c for c in feature_cols if c in engineered.columns]
        if not available_cols:
            logger.warning("No matching feature columns after engineering")
            return np.zeros((1, 60, n_features), dtype=np.float32)

        if len(available_cols) < 10:
            missing = set(feature_cols) - set(engineered.columns)
            logger.warning(f"Missing {len(missing)} features: {list(missing)[:10]}")

        # --- extract last 60 rows into full-width array ---
        src = engineered[available_cols].tail(60).to_numpy(dtype=np.float32)
        if src.shape[0] < 60:
            src = np.concatenate(
                [np.zeros((60 - src.shape[0], len(available_cols)), dtype=np.float32), src],
                axis=0,
            )

        full_arr = np.zeros((60, n_features), dtype=np.float32)
        for col, col_src in zip(available_cols, src.T):
            full_arr[:, feature_cols.index(col)] = col_src

        # --- z-score normalize using cached training stats ---
        mu_arr = np.array(
            [float(self._mu.get(c, 0.0)) for c in feature_cols], dtype=np.float32
        )
        sigma_arr = np.array(
            [float(self._sigma.get(c, 1.0)) for c in feature_cols], dtype=np.float32
        )
        sigma_arr = np.where(sigma_arr > 0, sigma_arr, 1.0)
        full_arr = (full_arr - mu_arr) / sigma_arr

        full_arr = np.clip(full_arr, -5.0, 5.0)
        full_arr = np.nan_to_num(full_arr, nan=0.0, posinf=5.0, neginf=-5.0)

        return full_arr.reshape(1, 60, n_features)

    def _compute_confidence(
        self, latest_df: pd.DataFrame, historical: pd.DataFrame
    ) -> float:
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
            z = 0.0 if sigma <= 1e-9 else abs(x - mu) / sigma
            zscores.append(z)

        avg_z = float(np.mean(zscores)) if zscores else 0.0
        return max(0.0, min(1.0, 1.0 / (1.0 + avg_z)))

    def _stringify_forecast(
        self,
        forecast: dict[int, dict[float, float]],
    ) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        for h, qmap in forecast.items():
            out[str(h)] = {str(q): float(v) for q, v in qmap.items()}
        return out
