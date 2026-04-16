"""
Incremental fine-tuner for PatchTST multi-output model.

Loads a saved checkpoint, pulls the last N rows from CSV,
runs engineer_features + generate_risk_labels (same pipeline as train.py),
fine-tunes for a few epochs with a low LR, then saves back to checkpoint.

Normalization stats (mu, sigma) are NEVER re-fitted — always loaded from checkpoint.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Re-use train.py helpers (import them directly so logic stays in sync)
# ---------------------------------------------------------------------------

def _import_train_helpers():
    """Lazy import to avoid circular deps and keep train.py as the single source."""
    import importlib.util, sys
    # Try to import from the train.py at project root
    candidates = [
        Path(__file__).resolve().parents[3] / "train.py",
        Path("train.py"),
    ]
    for p in candidates:
        if p.exists():
            spec = importlib.util.spec_from_file_location("train_module", str(p))
            mod = importlib.util.module_from_spec(spec)
            sys.modules["train_module"] = mod
            spec.loader.exec_module(mod)
            return mod
    raise FileNotFoundError("train.py not found. Expected at project root.")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class IncrementalConfig:
    checkpoint_path: str = "models/patchtst_multi.pt"

    # How many recent rows to use for fine-tuning window
    finetune_rows: int = 500          # ~500 minutes of history

    # Minimum rows required to attempt fine-tuning
    min_rows: int = 120               # at least 2x context_len

    # Training hyper-params for fine-tune
    epochs: int = 5
    lr: float = 1e-4
    weight_decay: float = 1e-4
    grad_clip: float = 1.0
    batch_size: int = 32

    # Loss weights (mirror train.py defaults)
    cpu_weight: float = 1.0
    throttle_weight: float = 2.0
    mem_weight: float = 1.0
    oom_weight: float = 2.0
    alpha: float = 0.9                # asymmetric loss alpha

    # Risk label thresholds (keep in sync with train.py defaults)
    throttle_risk_threshold: float = 0.05
    oom_risk_threshold: float = 0.70
    risk_window: int = 5

    device: str = "cuda" if torch.cuda.is_available() else "cpu"
    num_workers: int = 0              # 0 = main process (safer for online fine-tune)


# ---------------------------------------------------------------------------
# Trainer
# ---------------------------------------------------------------------------

class IncrementalTrainer:
    """
    Fine-tunes the saved PatchTST checkpoint on recent CSV data.

    Usage:
        trainer = IncrementalTrainer(cfg)
        new_state = trainer.fine_tune(recent_df)   # returns updated checkpoint dict
    """

    def __init__(self, cfg: IncrementalConfig):
        self.cfg = cfg
        self._train_mod = None        # lazy-loaded train.py module
        self._checkpoint: Optional[dict] = None
        self._model: Optional[nn.Module] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fine_tune(self, recent_df: pd.DataFrame) -> Optional[dict]:
        """
        Fine-tune on recent_df and save updated checkpoint.

        Args:
            recent_df: Raw metrics DataFrame (columns as written by CSVStore).
                       Should contain at least cfg.min_rows rows.

        Returns:
            Updated checkpoint dict, or None if skipped.
        """
        if len(recent_df) < self.cfg.min_rows:
            logger.warning(
                f"fine_tune: only {len(recent_df)} rows, need {self.cfg.min_rows}. Skipping."
            )
            return None

        # 1. Load helpers from train.py
        tm = self._get_train_module()

        # 2. Load checkpoint
        ckpt = self._load_checkpoint()
        if ckpt is None:
            logger.error("fine_tune: no checkpoint found. Run full training first.")
            return None

        # 3. Prepare data using SAME feature pipeline as train.py
        df = self._prepare_data(recent_df, tm, ckpt)
        if df is None:
            return None

        feature_cols: list[str] = ckpt["feature_cols"]
        cpu_idx: int = ckpt["cpu_idx"]
        mem_idx: int = ckpt["mem_idx"]
        horizons: list[int] = ckpt["horizons"]
        context_len: int = ckpt["context_len"]

        # 4. Build dataset / loader
        dataset = tm.ContainerMetricsDataset(df, feature_cols, horizons, context_len)
        if len(dataset) < self.cfg.batch_size:
            logger.warning(
                f"fine_tune: dataset has only {len(dataset)} samples after windowing. Skipping."
            )
            return None

        loader = DataLoader(
            dataset,
            batch_size=self.cfg.batch_size,
            shuffle=True,
            num_workers=self.cfg.num_workers,
            drop_last=True,
        )

        # 5. Rebuild model from checkpoint
        model = self._build_model(ckpt, len(feature_cols), cpu_idx, mem_idx, tm)
        model.train()

        # 6. Loss + optimizer
        criterion = self._build_criterion(tm)
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=self.cfg.lr,
            weight_decay=self.cfg.weight_decay,
        )

        # 7. Fine-tune loop
        logger.info(
            f"Fine-tuning for {self.cfg.epochs} epochs, "
            f"{len(loader)} batches/epoch, lr={self.cfg.lr}"
        )
        t0 = time.time()
        for epoch in range(1, self.cfg.epochs + 1):
            epoch_losses = []
            for x, y_cpu, y_mem, y_throttle, y_oom in loader:
                x = x.to(self.cfg.device)
                y_cpu = y_cpu.to(self.cfg.device)
                y_mem = y_mem.to(self.cfg.device)
                y_throttle = y_throttle.to(self.cfg.device)
                y_oom = y_oom.to(self.cfg.device)

                optimizer.zero_grad()
                cpu_pred, mem_pred, throttle_logit, oom_logit = model(x)
                loss, breakdown = criterion(
                    cpu_pred, mem_pred, throttle_logit, oom_logit,
                    y_cpu, y_mem, y_throttle, y_oom,
                )
                loss.backward()
                nn.utils.clip_grad_norm_(model.parameters(), self.cfg.grad_clip)
                optimizer.step()
                epoch_losses.append(loss.item())

            logger.info(
                f"  epoch {epoch}/{self.cfg.epochs} | loss={np.mean(epoch_losses):.4f}"
            )

        elapsed = time.time() - t0
        logger.info(f"Fine-tune complete in {elapsed:.1f}s")

        # 8. Save updated checkpoint (preserve all metadata, only replace model_state)
        updated_ckpt = dict(ckpt)
        updated_ckpt["model_state"] = model.state_dict()
        updated_ckpt["last_finetune_rows"] = len(recent_df)
        updated_ckpt["last_finetune_time"] = time.time()

        Path(self.cfg.checkpoint_path).parent.mkdir(parents=True, exist_ok=True)
        torch.save(updated_ckpt, self.cfg.checkpoint_path)
        logger.info(f"Checkpoint updated at {self.cfg.checkpoint_path}")

        # Cache updated model in memory for hot-reload
        self._model = model
        self._checkpoint = updated_ckpt

        return updated_ckpt

    def load_model_into_predictors(
        self,
        cpu_predictor,
        memory_predictor,
    ) -> bool:
        """
        Hot-reload updated weights into running CPUPredictor / MemoryPredictor.

        Args:
            cpu_predictor: CPUPredictor instance from src/prediction/cpu_forecast.py
            memory_predictor: MemoryPredictor instance from src/prediction/memory_forecast.py

        Returns:
            True if reload succeeded.
        """
        if self._model is None:
            logger.warning("load_model_into_predictors: no in-memory model, loading from disk.")
            ckpt = self._load_checkpoint()
            if ckpt is None:
                return False
            tm = self._get_train_module()
            feature_cols = ckpt["feature_cols"]
            cpu_idx = ckpt["cpu_idx"]
            mem_idx = ckpt["mem_idx"]
            self._model = self._build_model(ckpt, len(feature_cols), cpu_idx, mem_idx, tm)

        # Both CPUPredictor and MemoryPredictor wrap the same PatchTST model object
        # We replace their .model attribute in-place
        cpu_predictor.model = self._model
        memory_predictor.model = self._model
        logger.info("Hot-reloaded fine-tuned weights into predictors.")
        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_train_module(self):
        if self._train_mod is None:
            self._train_mod = _import_train_helpers()
        return self._train_mod

    def _load_checkpoint(self) -> Optional[dict]:
        p = Path(self.cfg.checkpoint_path)
        if not p.exists():
            logger.error(f"Checkpoint not found: {p}")
            return None
        ckpt = torch.load(p, map_location=self.cfg.device)
        logger.debug(f"Loaded checkpoint from {p}")
        return ckpt

    def _prepare_data(self, raw_df: pd.DataFrame, tm, ckpt: dict) -> Optional[pd.DataFrame]:
        """
        Run engineer_features + generate_risk_labels + normalize
        using checkpoint's mu/sigma (NO re-fitting).
        """
        try:
            df = tm.engineer_features(raw_df.copy())
        except Exception as e:
            logger.error(f"engineer_features failed: {e}")
            return None

        # Build a minimal Config-like object for generate_risk_labels
        class _RiskCfg:
            throttle_risk_threshold = self.cfg.throttle_risk_threshold
            oom_risk_threshold = self.cfg.oom_risk_threshold
            risk_window = self.cfg.risk_window

        try:
            df = tm.generate_risk_labels(df, _RiskCfg())
        except Exception as e:
            logger.error(f"generate_risk_labels failed: {e}")
            return None

        feature_cols: list[str] = ckpt["feature_cols"]

        # Check all required columns exist
        missing = [c for c in feature_cols if c not in df.columns]
        if missing:
            logger.error(f"Missing feature columns after engineering: {missing}")
            return None

        # Normalize using checkpoint stats (never refit)
        mu = pd.Series(ckpt["mu"])
        sigma = pd.Series(ckpt["sigma"]).replace(0, 1)

        df[feature_cols] = (df[feature_cols] - mu) / sigma
        df[feature_cols] = df[feature_cols].clip(-5, 5)

        return df

    def _build_model(self, ckpt: dict, num_channels: int, cpu_idx: int, mem_idx: int, tm) -> nn.Module:
        """Reconstruct PatchTSTMultiOutput from checkpoint metadata."""

        class _Cfg:
            horizons = ckpt["horizons"]
            context_len = ckpt["context_len"]
            patch_len = ckpt["patch_len"]
            stride = ckpt["stride"]
            d_model = ckpt["d_model"]
            n_heads = ckpt["n_heads"]
            n_layers = ckpt["n_layers"]
            dropout = ckpt["dropout"]

        model = tm.PatchTSTMultiOutput(_Cfg(), num_channels, cpu_idx, mem_idx)
        model.load_state_dict(ckpt["model_state"])
        model = model.to(self.cfg.device)
        return model

    def _build_criterion(self, tm) -> nn.Module:
        """Build MultiTaskLoss using IncrementalConfig weights."""

        class _LossCfg:
            cpu_weight = self.cfg.cpu_weight
            mem_weight = self.cfg.mem_weight
            throttle_weight = self.cfg.throttle_weight
            oom_weight = self.cfg.oom_weight
            alpha = self.cfg.alpha

        return tm.MultiTaskLoss(_LossCfg())