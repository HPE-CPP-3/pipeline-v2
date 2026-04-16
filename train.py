#!/usr/bin/env python3
"""
PatchTST Multi-Output Training for Container Metrics
=====================================================
Predicts:
- Future CPU usage (regression)
- CPU throttling risk (binary classification)
- Future memory usage (regression)
- OOM risk (binary classification)

Usage:
    python train.py --csv data/csv/metrics/
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Config:
    # Data paths
    csv_path: str = "data/csv/metrics"
    checkpoint_path: str = "data/models/patchtst_multi.pt"
    
    # Sequence parameters (1-minute intervals)
    context_len: int = 60          # 60 minutes of history
    horizons: list[int] = field(default_factory=lambda: [5, 10, 15])  # Predict 5,10,15 min ahead
    
    # PatchTST architecture (PRESERVED)
    patch_len: int = 12
    stride: int = 6
    d_model: int = 128
    n_heads: int = 8
    n_layers: int = 4
    dropout: float = 0.1
    
    # Training
    batch_size: int = 64
    epochs: int = 100
    lr: float = 1e-3
    weight_decay: float = 1e-4
    grad_clip: float = 1.0
    train_ratio: float = 0.7
    val_ratio: float = 0.15
    
    # Risk thresholds (for RAW data - memory in bytes, CPU in cores)
    throttle_risk_threshold: float = 0.05    # 5% throttling ratio
    oom_risk_threshold: float = 0.70         # 70% of memory limit
    risk_window: int = 5                    # Look ahead 5 steps for risk
    
    # Loss weights
    cpu_weight: float = 1.0
    throttle_weight: float = 2.0
    mem_weight: float = 1.0
    oom_weight: float = 2.0
    alpha: float = 0.9                      # Asymmetric loss parameter
    
    seed: int = 42
    device: str = "cuda" if torch.cuda.is_available() else "cpu"
    num_workers: int = 4


# ---------------------------------------------------------------------------
# Feature Engineering
# ---------------------------------------------------------------------------

def load_csv_files(csv_path: str) -> pd.DataFrame:
    """Load CSV files from path, directory, or glob pattern."""
    path = Path(csv_path)
    files = []
    
    if path.is_dir():
        all_files = sorted(glob.glob(str(path / "*.csv")))
        files = [f for f in all_files if "_raw.csv" in f]
    elif "*" in csv_path:
        files = sorted(glob.glob(csv_path))
    elif path.is_file():
        files = [str(path)]
    else:
        raise FileNotFoundError(f"No CSV files found: {csv_path}")
    
    dfs = []
    for f in files:
        df = pd.read_csv(f, parse_dates=["timestamp"])
        dfs.append(df)
        logger.info(f"Loaded {Path(f).name}: {len(df)} rows")
    
    return pd.concat(dfs, ignore_index=True)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add engineered features for better predictions."""
    df = df.sort_values("timestamp").copy()
    
    # CPU features
    cpu = df["container_cpu_usage_seconds_total"]
    df["cpu_roll_mean_5"] = cpu.rolling(5, min_periods=1).mean()
    df["cpu_roll_std_5"] = cpu.rolling(5, min_periods=1).std().fillna(0)
    df["cpu_roll_mean_10"] = cpu.rolling(10, min_periods=1).mean()
    df["cpu_roll_std_10"] = cpu.rolling(10, min_periods=1).std().fillna(0)
    df["cpu_diff"] = cpu.diff().fillna(0)
    
    # Memory features (raw bytes)
    mem = df["container_memory_working_set_bytes"]
    mem_limit = df["kube_pod_container_resource_limits_memory"].replace(0, np.nan)
    df["mem_usage_ratio"] = (mem / mem_limit).fillna(0)
    df["mem_roll_mean_5"] = mem.rolling(5, min_periods=1).mean()
    df["mem_roll_std_5"] = mem.rolling(5, min_periods=1).std().fillna(0)
    df["mem_growth_rate"] = mem.pct_change(5).fillna(0).clip(-1, 1)
    
    # Throttling features
    df["throttle_roll_max_5"] = df["derived_pressure_throttled_ratio"].rolling(5, min_periods=1).max()
    
    # Time features
    ts = pd.to_datetime(df["timestamp"])
    hour = ts.dt.hour + ts.dt.minute / 60
    df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    df["dayofweek"] = ts.dt.dayofweek / 7.0
    
    # Fill NaNs
    df = df.fillna(0)
    
    return df


def generate_risk_labels(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Generate binary risk labels based on future windows using raw values."""
    df = df.copy()
    window = cfg.risk_window
    
    # Throttle risk: look ahead 'window' steps
    throttle_future_max = (
        df["derived_pressure_throttled_ratio"]
        .rolling(window=window, min_periods=1)
        .max()
        .shift(-window + 1)
    )
    df["throttle_risk"] = (throttle_future_max > cfg.throttle_risk_threshold).astype(float)
    
    # OOM risk: calculate memory usage ratio from RAW values, then look ahead
    mem = df["container_memory_working_set_bytes"]
    mem_limit = df["kube_pod_container_resource_limits_memory"].replace(0, np.nan)
    mem_usage_ratio = (mem / mem_limit).fillna(0)
    
    oom_future_max = (
        mem_usage_ratio
        .rolling(window=window, min_periods=1)
        .max()
        .shift(-window + 1)
    )
    df["oom_risk"] = (oom_future_max > cfg.oom_risk_threshold).astype(float)
    
    # Fill NaN at the end
    df = df.fillna(0)
    
    # Log statistics
    throttle_pos = df["throttle_risk"].mean()
    oom_pos = df["oom_risk"].mean()
    logger.info(f"Throttle risk positive rate: {throttle_pos:.3f} (threshold={cfg.throttle_risk_threshold})")
    logger.info(f"OOM risk positive rate: {oom_pos:.3f} (threshold={cfg.oom_risk_threshold})")
    
    return df


def get_feature_columns(df: pd.DataFrame) -> list:
    """Get list of feature columns (exclude non-features and targets)."""
    exclude = {
        "timestamp", "namespace", "pod", "container", "node",
        "throttle_risk", "oom_risk"
    }
    
    feature_cols = [c for c in df.columns if c not in exclude]
    
    # Ensure targets are in features
    assert "container_cpu_usage_seconds_total" in feature_cols
    assert "container_memory_working_set_bytes" in feature_cols
    
    return feature_cols


def normalize_data(train_df, val_df, test_df, feature_cols):
    """Normalize using training statistics."""
    mu = train_df[feature_cols].mean()
    sigma = train_df[feature_cols].std().replace(0, 1)
    
    for df in [train_df, val_df, test_df]:
        df[feature_cols] = (df[feature_cols] - mu) / sigma
        df[feature_cols] = df[feature_cols].clip(-5, 5)
    
    return mu, sigma


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class ContainerMetricsDataset(Dataset):
    """Multi-output dataset for container metrics."""
    
    def __init__(
        self,
        df: pd.DataFrame,
        feature_cols: list,
        horizons: list,
        context_len: int,
    ):
        self.feature_cols = feature_cols
        self.horizons = horizons
        self.context_len = context_len
        
        # Get indices for target columns
        self.cpu_idx = feature_cols.index("container_cpu_usage_seconds_total")
        self.mem_idx = feature_cols.index("container_memory_working_set_bytes")
        
        # Convert to numpy for speed
        self.features = df[feature_cols].to_numpy(dtype=np.float32)
        self.throttle_risk = df["throttle_risk"].to_numpy(dtype=np.float32)
        self.oom_risk = df["oom_risk"].to_numpy(dtype=np.float32)
        
        max_horizon = max(horizons)
        self.valid_start = context_len
        self.valid_end = len(df) - max_horizon
    
    def __len__(self):
        return max(0, self.valid_end - self.valid_start)
    
    def __getitem__(self, idx):
        t = self.valid_start + idx
        
        # Input features [context_len, num_features]
        x = self.features[t - self.context_len:t]
        
        # CPU targets at each horizon
        y_cpu = np.array([self.features[t + h - 1, self.cpu_idx] for h in self.horizons], dtype=np.float32)
        
        # Memory targets at each horizon
        y_mem = np.array([self.features[t + h - 1, self.mem_idx] for h in self.horizons], dtype=np.float32)
        
        # Risk labels
        y_throttle = self.throttle_risk[t:t+1]
        y_oom = self.oom_risk[t:t+1]
        
        return (
            torch.from_numpy(x),
            torch.from_numpy(y_cpu),
            torch.from_numpy(y_mem),
            torch.from_numpy(y_throttle),
            torch.from_numpy(y_oom),
        )


# ---------------------------------------------------------------------------
# Model - PRESERVED PATCHTST ARCHITECTURE
# ---------------------------------------------------------------------------

class SLAAsymmetricLoss(nn.Module):
    """Asymmetric loss for regression (penalizes under-prediction)."""
    def __init__(self, alpha: float = 0.9):
        super().__init__()
        self.alpha = alpha
    
    def forward(self, y_hat, y):
        err = y - y_hat
        return torch.where(err > 0, self.alpha * err.abs(), (1 - self.alpha) * err.abs()).mean()


class MultiTaskLoss(nn.Module):
    """Combined loss for regression + classification tasks."""
    def __init__(self, cfg: Config):
        super().__init__()
        self.cpu_weight = cfg.cpu_weight
        self.mem_weight = cfg.mem_weight
        self.throttle_weight = cfg.throttle_weight
        self.oom_weight = cfg.oom_weight
        self.reg_loss = SLAAsymmetricLoss(alpha=cfg.alpha)
        self.cls_loss = nn.BCEWithLogitsLoss()
    
    def forward(self, cpu_pred, mem_pred, throttle_logit, oom_logit, y_cpu, y_mem, y_throttle, y_oom):
        loss_cpu = self.reg_loss(cpu_pred, y_cpu)
        loss_mem = self.reg_loss(mem_pred, y_mem)
        loss_throttle = self.cls_loss(throttle_logit, y_throttle)
        loss_oom = self.cls_loss(oom_logit, y_oom)
        
        total = (self.cpu_weight * loss_cpu + 
                self.mem_weight * loss_mem + 
                self.throttle_weight * loss_throttle + 
                self.oom_weight * loss_oom)
        
        return total, {
            "cpu": loss_cpu.item(),
            "mem": loss_mem.item(),
            "throttle": loss_throttle.item(),
            "oom": loss_oom.item()
        }


class PatchTSTBlock(nn.Module):
    """Channel-independent PatchTST encoder - PRESERVED ARCHITECTURE."""
    
    def __init__(self, num_channels, context_len, patch_len, stride, d_model, n_heads, n_layers, dropout):
        super().__init__()
        self.num_channels = num_channels
        self.patch_len = patch_len
        self.stride = stride
        
        self.n_patches = 1 + (context_len - patch_len) // stride
        if self.n_patches <= 0:
            raise ValueError(f"Invalid patch settings: context_len={context_len}, patch_len={patch_len}")
        
        self.patch_embed = nn.Linear(patch_len, d_model)
        self.pos_emb = nn.Parameter(torch.zeros(1, 1, self.n_patches, d_model))
        
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=n_heads, dim_feedforward=4*d_model,
            dropout=dropout, batch_first=True, activation="gelu"
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, n_layers)
        self.dropout = nn.Dropout(dropout)
        
        nn.init.trunc_normal_(self.pos_emb, std=0.02)
    
    def forward(self, x):
        B, L, C = x.shape
        x = x.permute(0, 2, 1)  # [B, C, L]
        
        patches = x.unfold(-1, self.patch_len, self.stride)  # [B, C, N, P]
        z = self.patch_embed(patches)  # [B, C, N, D]
        z = z + self.pos_emb[:, :, :z.size(2), :]
        z = self.dropout(z)
        
        z = z.reshape(B * C, self.n_patches, -1)
        z = self.encoder(z)
        z = z.mean(dim=1)
        z = z.view(B, C, -1)
        return z


class PatchTSTMultiOutput(nn.Module):
    """Multi-output PatchTST for container metrics forecasting - PRESERVED ARCHITECTURE."""
    
    def __init__(self, cfg: Config, num_channels: int, cpu_idx: int, mem_idx: int):
        super().__init__()
        self.cpu_idx = cpu_idx
        self.mem_idx = mem_idx
        self.num_horizons = len(cfg.horizons)
        
        # Shared backbone
        self.backbone = PatchTSTBlock(
            num_channels=num_channels,
            context_len=cfg.context_len,
            patch_len=cfg.patch_len,
            stride=cfg.stride,
            d_model=cfg.d_model,
            n_heads=cfg.n_heads,
            n_layers=cfg.n_layers,
            dropout=cfg.dropout
        )
        
        # Regression heads
        self.cpu_head = nn.Sequential(
            nn.Linear(cfg.d_model, cfg.d_model // 2),
            nn.GELU(),
            nn.Linear(cfg.d_model // 2, self.num_horizons)
        )
        
        self.mem_head = nn.Sequential(
            nn.Linear(cfg.d_model, cfg.d_model // 2),
            nn.GELU(),
            nn.Linear(cfg.d_model // 2, self.num_horizons)
        )
        
        # Classification heads (using global context)
        self.throttle_head = nn.Sequential(
            nn.Linear(cfg.d_model, cfg.d_model // 2),
            nn.GELU(),
            nn.Dropout(cfg.dropout),
            nn.Linear(cfg.d_model // 2, 1)
        )
        
        self.oom_head = nn.Sequential(
            nn.Linear(cfg.d_model, cfg.d_model // 2),
            nn.GELU(),
            nn.Dropout(cfg.dropout),
            nn.Linear(cfg.d_model // 2, 1)
        )
    
    def forward(self, x):
        # x: [B, L, C]
        z = self.backbone(x)  # [B, C, D]
        
        # Regression predictions
        cpu_pred = self.cpu_head(z[:, self.cpu_idx, :])
        mem_pred = self.mem_head(z[:, self.mem_idx, :])
        
        # Classification predictions (global context)
        global_ctx = z.mean(dim=1)
        throttle_logit = self.throttle_head(global_ctx)
        oom_logit = self.oom_head(global_ctx)
        
        return cpu_pred, mem_pred, throttle_logit, oom_logit


# ---------------------------------------------------------------------------
# Evaluation Metrics
# ---------------------------------------------------------------------------

def convert_to_serializable(obj):
    """Convert numpy types to Python native types for JSON serialization."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_to_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    else:
        return obj


@torch.no_grad()
def evaluate(model, loader, criterion, device, horizons):
    """Evaluate model on validation/test set."""
    model.eval()
    
    total_loss = 0
    loss_components = {"cpu": 0, "mem": 0, "throttle": 0, "oom": 0}
    
    all_throttle_preds = []
    all_throttle_true = []
    all_oom_preds = []
    all_oom_true = []
    all_cpu_preds = []
    all_cpu_true = []
    all_mem_preds = []
    all_mem_true = []
    
    for x, y_cpu, y_mem, y_throttle, y_oom in loader:
        x = x.to(device)
        y_cpu = y_cpu.to(device)
        y_mem = y_mem.to(device)
        y_throttle = y_throttle.to(device)
        y_oom = y_oom.to(device)
        
        cpu_pred, mem_pred, throttle_logit, oom_logit = model(x)
        loss, breakdown = criterion(cpu_pred, mem_pred, throttle_logit, oom_logit,
                                    y_cpu, y_mem, y_throttle, y_oom)
        
        total_loss += loss.item()
        for k in loss_components:
            loss_components[k] += breakdown[k]
        
        # Store predictions
        all_cpu_preds.append(cpu_pred.cpu().numpy())
        all_cpu_true.append(y_cpu.cpu().numpy())
        all_mem_preds.append(mem_pred.cpu().numpy())
        all_mem_true.append(y_mem.cpu().numpy())
        
        throttle_pred = (torch.sigmoid(throttle_logit) > 0.5).float()
        all_throttle_preds.append(throttle_pred.cpu().numpy())
        all_throttle_true.append(y_throttle.cpu().numpy())
        
        oom_pred = (torch.sigmoid(oom_logit) > 0.5).float()
        all_oom_preds.append(oom_pred.cpu().numpy())
        all_oom_true.append(y_oom.cpu().numpy())
    
    n_batches = len(loader)
    metrics = {
        "loss": total_loss / n_batches,
        "loss_cpu": loss_components["cpu"] / n_batches,
        "loss_mem": loss_components["mem"] / n_batches,
        "loss_throttle": loss_components["throttle"] / n_batches,
        "loss_oom": loss_components["oom"] / n_batches,
    }
    
    # Regression metrics
    cpu_pred_all = np.concatenate(all_cpu_preds, axis=0)
    cpu_true_all = np.concatenate(all_cpu_true, axis=0)
    mem_pred_all = np.concatenate(all_mem_preds, axis=0)
    mem_true_all = np.concatenate(all_mem_true, axis=0)
    
    metrics["cpu_mae"] = float(np.mean(np.abs(cpu_pred_all - cpu_true_all)))
    metrics["cpu_rmse"] = float(np.sqrt(np.mean((cpu_pred_all - cpu_true_all) ** 2)))
    metrics["mem_mae"] = float(np.mean(np.abs(mem_pred_all - mem_true_all)))
    metrics["mem_rmse"] = float(np.sqrt(np.mean((mem_pred_all - mem_true_all) ** 2)))
    
    # Per-horizon CPU metrics
    for i, h in enumerate(horizons):
        metrics[f"cpu_horizon_{h}_mae"] = float(np.mean(np.abs(cpu_pred_all[:, i] - cpu_true_all[:, i])))
        metrics[f"cpu_horizon_{h}_rmse"] = float(np.sqrt(np.mean((cpu_pred_all[:, i] - cpu_true_all[:, i]) ** 2)))
    
    # Per-horizon Memory metrics
    for i, h in enumerate(horizons):
        metrics[f"mem_horizon_{h}_mae"] = float(np.mean(np.abs(mem_pred_all[:, i] - mem_true_all[:, i])))
        metrics[f"mem_horizon_{h}_rmse"] = float(np.sqrt(np.mean((mem_pred_all[:, i] - mem_true_all[:, i]) ** 2)))
    
    # Classification metrics
    throttle_pred_all = np.concatenate(all_throttle_preds, axis=0).flatten()
    throttle_true_all = np.concatenate(all_throttle_true, axis=0).flatten()
    oom_pred_all = np.concatenate(all_oom_preds, axis=0).flatten()
    oom_true_all = np.concatenate(all_oom_true, axis=0).flatten()
    
    metrics["throttle_accuracy"] = float(accuracy_score(throttle_true_all, throttle_pred_all))
    metrics["oom_accuracy"] = float(accuracy_score(oom_true_all, oom_pred_all))
    
    if throttle_true_all.sum() > 0:
        metrics["throttle_precision"] = float(precision_score(throttle_true_all, throttle_pred_all, zero_division=0))
        metrics["throttle_recall"] = float(recall_score(throttle_true_all, throttle_pred_all, zero_division=0))
        metrics["throttle_f1"] = float(f1_score(throttle_true_all, throttle_pred_all, zero_division=0))
    else:
        metrics["throttle_precision"] = 0.0
        metrics["throttle_recall"] = 0.0
        metrics["throttle_f1"] = 0.0
    
    if oom_true_all.sum() > 0:
        metrics["oom_precision"] = float(precision_score(oom_true_all, oom_pred_all, zero_division=0))
        metrics["oom_recall"] = float(recall_score(oom_true_all, oom_pred_all, zero_division=0))
        metrics["oom_f1"] = float(f1_score(oom_true_all, oom_pred_all, zero_division=0))
    else:
        metrics["oom_precision"] = 0.0
        metrics["oom_recall"] = 0.0
        metrics["oom_f1"] = 0.0
    
    return metrics


# ---------------------------------------------------------------------------
# Training Loop
# ---------------------------------------------------------------------------

def seed_everything(seed: int):
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def train(cfg: Config):
    """Main training function."""
    seed_everything(cfg.seed)
    logger.info(f"Device: {cfg.device}")
    
    # Load and preprocess data
    logger.info("Loading CSV files...")
    df_raw = load_csv_files(cfg.csv_path)
    logger.info(f"Loaded {len(df_raw)} rows")
    
    # Engineer features
    logger.info("Engineering features...")
    df = engineer_features(df_raw)
    
    # Generate risk labels
    logger.info("Generating risk labels...")
    df = generate_risk_labels(df, cfg)
    
    # Get feature columns
    feature_cols = get_feature_columns(df)
    logger.info(f"Using {len(feature_cols)} features")
    
    # Get target indices
    cpu_idx = feature_cols.index("container_cpu_usage_seconds_total")
    mem_idx = feature_cols.index("container_memory_working_set_bytes")
    
    # Split chronologically
    n = len(df)
    train_end = int(n * cfg.train_ratio)
    val_end = int(n * (cfg.train_ratio + cfg.val_ratio))
    
    train_df = df.iloc[:train_end].copy()
    val_df = df.iloc[train_end:val_end].copy()
    test_df = df.iloc[val_end:].copy()
    
    logger.info(f"Train: {len(train_df)}, Val: {len(val_df)}, Test: {len(test_df)}")
    
    # Normalize
    mu, sigma = normalize_data(train_df, val_df, test_df, feature_cols)
    
    # Create datasets
    train_ds = ContainerMetricsDataset(train_df, feature_cols, cfg.horizons, cfg.context_len)
    val_ds = ContainerMetricsDataset(val_df, feature_cols, cfg.horizons, cfg.context_len)
    test_ds = ContainerMetricsDataset(test_df, feature_cols, cfg.horizons, cfg.context_len)
    
    train_loader = DataLoader(train_ds, batch_size=cfg.batch_size, shuffle=True, 
                              num_workers=cfg.num_workers, drop_last=True)
    val_loader = DataLoader(val_ds, batch_size=cfg.batch_size, shuffle=False, num_workers=cfg.num_workers)
    test_loader = DataLoader(test_ds, batch_size=cfg.batch_size, shuffle=False, num_workers=cfg.num_workers)
    
    logger.info(f"Train batches: {len(train_loader)}, Val batches: {len(val_loader)}")
    
    # Create model
    model = PatchTSTMultiOutput(cfg, len(feature_cols), cpu_idx, mem_idx).to(cfg.device)
    n_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    logger.info(f"Model parameters: {n_params:,}")
    
    # Loss, optimizer, scheduler
    criterion = MultiTaskLoss(cfg)
    optimizer = torch.optim.AdamW(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode="min", factor=0.5, patience=5)
    
    # Training loop
    best_val_loss = float("inf")
    best_state = None
    patience_counter = 0
    
    for epoch in range(1, cfg.epochs + 1):
        model.train()
        train_losses = []
        
        for x, y_cpu, y_mem, y_throttle, y_oom in train_loader:
            x = x.to(cfg.device)
            y_cpu = y_cpu.to(cfg.device)
            y_mem = y_mem.to(cfg.device)
            y_throttle = y_throttle.to(cfg.device)
            y_oom = y_oom.to(cfg.device)
            
            optimizer.zero_grad()
            cpu_pred, mem_pred, throttle_logit, oom_logit = model(x)
            loss, _ = criterion(cpu_pred, mem_pred, throttle_logit, oom_logit,
                                        y_cpu, y_mem, y_throttle, y_oom)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), cfg.grad_clip)
            optimizer.step()
            
            train_losses.append(loss.item())
        
        # Validation
        val_metrics = evaluate(model, val_loader, criterion, cfg.device, cfg.horizons)
        scheduler.step(val_metrics["loss"])
        
        # Logging
        if epoch % 5 == 0 or epoch == 1:
            logger.info(
                f"Epoch {epoch:3d}/{cfg.epochs} | "
                f"Train Loss: {np.mean(train_losses):.4f} | "
                f"Val Loss: {val_metrics['loss']:.4f} | "
                f"CPU RMSE: {val_metrics['cpu_rmse']:.4f} | "
                f"Mem RMSE: {val_metrics['mem_rmse']:.4f} | "
                f"Throttle Acc: {val_metrics['throttle_accuracy']:.3f} | "
                f"OOM Acc: {val_metrics['oom_accuracy']:.3f}"
            )
        
        # Checkpoint
        if val_metrics["loss"] < best_val_loss:
            best_val_loss = val_metrics["loss"]
            patience_counter = 0
            best_state = {
                "model_state": model.state_dict(),
                "feature_cols": feature_cols,
                "cpu_idx": cpu_idx,
                "mem_idx": mem_idx,
                "horizons": cfg.horizons,
                "context_len": cfg.context_len,
                "patch_len": cfg.patch_len,
                "stride": cfg.stride,
                "d_model": cfg.d_model,
                "n_heads": cfg.n_heads,
                "n_layers": cfg.n_layers,
                "dropout": cfg.dropout,
                "mu": mu.to_dict(),
                "sigma": sigma.to_dict(),
                "config": cfg.__dict__,
                "val_metrics": convert_to_serializable(val_metrics)
            }
            Path(cfg.checkpoint_path).parent.mkdir(parents=True, exist_ok=True)
            torch.save(best_state, cfg.checkpoint_path)
        else:
            patience_counter += 1
            if patience_counter >= 10:
                logger.info(f"Early stopping at epoch {epoch}")
                break
    
    # Test evaluation
    if best_state:
        model.load_state_dict(best_state["model_state"])
        test_metrics = evaluate(model, test_loader, criterion, cfg.device, cfg.horizons)
        
        logger.info("\n" + "="*70)
        logger.info("TEST RESULTS - ALL METRICS")
        logger.info("="*70)
        logger.info(f"Total Loss: {test_metrics['loss']:.4f}")
        logger.info(f"  - CPU Loss: {test_metrics['loss_cpu']:.4f}")
        logger.info(f"  - Memory Loss: {test_metrics['loss_mem']:.4f}")
        logger.info(f"  - Throttle Loss: {test_metrics['loss_throttle']:.4f}")
        logger.info(f"  - OOM Loss: {test_metrics['loss_oom']:.4f}")
        
        logger.info(f"\nCPU Forecasting:")
        logger.info(f"  MAE: {test_metrics['cpu_mae']:.4f}")
        logger.info(f"  RMSE: {test_metrics['cpu_rmse']:.4f}")
        for h in cfg.horizons:
            logger.info(f"  Horizon {h}min - MAE: {test_metrics[f'cpu_horizon_{h}_mae']:.4f}, RMSE: {test_metrics[f'cpu_horizon_{h}_rmse']:.4f}")
        
        logger.info(f"\nMemory Forecasting:")
        logger.info(f"  MAE: {test_metrics['mem_mae']:.4f}")
        logger.info(f"  RMSE: {test_metrics['mem_rmse']:.4f}")
        for h in cfg.horizons:
            logger.info(f"  Horizon {h}min - MAE: {test_metrics[f'mem_horizon_{h}_mae']:.4f}, RMSE: {test_metrics[f'mem_horizon_{h}_rmse']:.4f}")
        
        logger.info(f"\nThrottle Risk Classification:")
        logger.info(f"  Accuracy: {test_metrics['throttle_accuracy']:.3f}")
        logger.info(f"  Precision: {test_metrics['throttle_precision']:.3f}")
        logger.info(f"  Recall: {test_metrics['throttle_recall']:.3f}")
        logger.info(f"  F1 Score: {test_metrics['throttle_f1']:.3f}")
        
        logger.info(f"\nOOM Risk Classification:")
        logger.info(f"  Accuracy: {test_metrics['oom_accuracy']:.3f}")
        logger.info(f"  Precision: {test_metrics['oom_precision']:.3f}")
        logger.info(f"  Recall: {test_metrics['oom_recall']:.3f}")
        logger.info(f"  F1 Score: {test_metrics['oom_f1']:.3f}")
        logger.info("="*70)
        
        # Save test metrics (with proper JSON serialization)
        metrics_path = Path(cfg.checkpoint_path).parent / "test_metrics.json"
        with open(metrics_path, "w") as f:
            json.dump(convert_to_serializable(test_metrics), f, indent=2)
        
        logger.info(f"\nCheckpoint saved to {cfg.checkpoint_path}")
        logger.info(f"Test metrics saved to {metrics_path}")
    
    return best_state


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Train PatchTST Multi-Output Model")
    parser.add_argument("--csv", default="data/csv/metrics", help="Path to CSV file or directory")
    parser.add_argument("--checkpoint", default="models/patchtst_multi.pt", help="Checkpoint path")
    parser.add_argument("--epochs", type=int, default=100, help="Number of epochs")
    parser.add_argument("--batch-size", type=int, default=64, help="Batch size")
    parser.add_argument("--context-len", type=int, default=60, help="Context length (minutes)")
    parser.add_argument("--device", default=None, help="Device to use")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    cfg = Config(
        csv_path=args.csv,
        checkpoint_path=args.checkpoint,
        epochs=args.epochs,
        batch_size=args.batch_size,
        context_len=args.context_len,
        device=args.device or ("cuda" if torch.cuda.is_available() else "cpu")
    )
    train(cfg)