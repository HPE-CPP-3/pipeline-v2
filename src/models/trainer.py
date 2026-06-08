"""
PatchTST trainer with quantile loss
"""

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from typing import Optional
import logging
from datetime import datetime

from .patchtst import PatchTST
from .quantile_loss import QuantileLoss

logger = logging.getLogger(__name__)


class PatchTSTTrainer:
    """
    Trainer for PatchTST model

    Supports:
    - Full training (base model)
    - Incremental training (fine-tuning)
    - Early stopping
    - Gradient clipping
    """

    def __init__(
        self,
        model: PatchTST,
        quantiles: list[float] = None,
        quantile_weights: Optional[dict[float, float]] = None,
        lr: float = 0.001,
        weight_decay: float = 0.01,
        gradient_clip: float = 1.0,
    ):
        """
        Args:
            model: PatchTST model to train
            quantiles: Quantiles for loss
            quantile_weights: Weights for each quantile loss
            lr: Learning rate
            weight_decay: L2 regularization
            gradient_clip: Gradient clipping value
        """
        self.model = model
        self.lr = lr
        self.weight_decay = weight_decay
        self.gradient_clip = gradient_clip

        # Loss function
        self.criterion = QuantileLoss(quantiles, quantile_weights)

        # Optimizer
        self.optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=lr,
            weight_decay=weight_decay,
        )

        # Learning rate scheduler
        self.scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            self.optimizer,
            mode="min",
            factor=0.5,
            patience=5,
            min_lr=1e-6,
        )

        # Training history
        self.history = {
            "train_loss": [],
            "val_loss": [],
            "metrics": [],
        }

        logger.info(
            f"Initialized PatchTSTTrainer: lr={lr}, "
            f"weight_decay={weight_decay}, gradient_clip={gradient_clip}"
        )

    def _prepare_data(
        self,
        X: torch.Tensor,
        y: torch.Tensor,
        validation_split: float = 0.2,
        batch_size: int = 64,
    ) -> tuple[DataLoader, DataLoader]:
        """
        Prepare train and validation data loaders

        Args:
            X: Input features (batch_size, seq_len, input_dim)
            y: Ground truth (batch_size, max_horizon)
            validation_split: Fraction for validation
            batch_size: Batch size

        Returns:
            train_loader, val_loader
        """
        n_samples = X.shape[0]
        n_val = int(n_samples * validation_split)
        n_train = n_samples - n_val

        # Split data
        X_train, X_val = X[:n_train], X[n_train:]
        y_train, y_val = y[:n_train], y[n_train:]

        # Create datasets
        train_dataset = TensorDataset(X_train, y_train)
        val_dataset = TensorDataset(X_val, y_val)

        # Create loaders
        train_loader = DataLoader(
            train_dataset,
            batch_size=batch_size,
            shuffle=True,
            drop_last=True,
        )

        val_loader = DataLoader(
            val_dataset,
            batch_size=batch_size,
            shuffle=False,
            drop_last=False,
        )

        logger.info(f"Data prepared: {n_train} train, {n_val} validation samples")
        return train_loader, val_loader

    def train_epoch(self, train_loader: DataLoader) -> float:
        """
        Train for one epoch

        Args:
            train_loader: Training data loader

        Returns:
            Average training loss
        """
        self.model.train()
        total_loss = 0.0
        n_batches = 0

        for X_batch, y_batch in train_loader:
            # Forward pass
            predictions = self.model(X_batch)

            # Calculate loss
            loss = self.criterion(predictions, y_batch)

            # Backward pass
            self.optimizer.zero_grad()
            loss.backward()

            # Gradient clipping
            if self.gradient_clip > 0:
                torch.nn.utils.clip_grad_norm_(
                    self.model.parameters(),
                    self.gradient_clip,
                )

            # Update weights
            self.optimizer.step()

            total_loss += loss.item()
            n_batches += 1

        return total_loss / n_batches

    def validate(self, val_loader: DataLoader) -> tuple[float, dict]:
        """
        Validate model

        Args:
            val_loader: Validation data loader

        Returns:
            (average loss, metrics dict)
        """
        self.model.eval()
        total_loss = 0.0
        n_batches = 0
        all_predictions = {}
        all_y_true = []

        with torch.no_grad():
            for X_batch, y_batch in val_loader:
                # Forward pass
                predictions = self.model(X_batch)

                # Calculate loss
                loss = self.criterion(predictions, y_batch)

                total_loss += loss.item()
                n_batches += 1

                # Collect predictions for metrics
                for k, v in predictions.items():
                    if k not in all_predictions:
                        all_predictions[k] = []
                    all_predictions[k].append(v)
                all_y_true.append(y_batch)

        # Concatenate predictions
        all_predictions = {k: torch.cat(v, dim=0) for k, v in all_predictions.items()}
        all_y_true = torch.cat(all_y_true, dim=0)

        # Calculate metrics
        metrics = self.criterion.calculate_metrics(all_predictions, all_y_true)

        return total_loss / n_batches, metrics

    def fit(
        self,
        X: torch.Tensor,
        y: torch.Tensor,
        epochs: int = 100,
        batch_size: int = 64,
        validation_split: float = 0.2,
        early_stopping_patience: int = 10,
        early_stopping_min_delta: float = 0.001,
    ) -> dict:
        """
        Train the model

        Args:
            X: Input features
            y: Ground truth
            epochs: Number of epochs
            batch_size: Batch size
            validation_split: Validation fraction
            early_stopping_patience: Patience for early stopping
            early_stopping_min_delta: Minimum improvement

        Returns:
            Training history
        """
        # Prepare data
        train_loader, val_loader = self._prepare_data(
            X, y, validation_split, batch_size
        )

        # Training loop
        best_val_loss = float("inf")
        patience_counter = 0
        start_time = datetime.now()

        logger.info(
            f"Starting training: {epochs} epochs, {len(train_loader)} batches/epoch"
        )

        for epoch in range(epochs):
            # Train
            train_loss = self.train_epoch(train_loader)

            # Validate
            val_loss, metrics = self.validate(val_loader)

            # Update learning rate
            self.scheduler.step(val_loss)

            # Log progress
            logger.info(
                f"Epoch {epoch + 1}/{epochs}: "
                f"train_loss={train_loss:.4f}, val_loss={val_loss:.4f}"
            )

            # Store history
            self.history["train_loss"].append(train_loss)
            self.history["val_loss"].append(val_loss)
            self.history["metrics"].append(metrics)

            # Early stopping
            if val_loss < best_val_loss - early_stopping_min_delta:
                best_val_loss = val_loss
                patience_counter = 0

                # Save best model state
                self.best_state = {
                    k: v.cpu().clone() for k, v in self.model.state_dict().items()
                }
            else:
                patience_counter += 1

                if patience_counter >= early_stopping_patience:
                    logger.info(f"Early stopping at epoch {epoch + 1}")
                    break

        # Restore best model
        if hasattr(self, "best_state"):
            self.model.load_state_dict(self.best_state)

        training_time = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Training completed in {training_time:.1f}s, best val_loss={best_val_loss:.4f}"
        )

        return {
            "epochs_trained": epoch + 1,
            "best_val_loss": best_val_loss,
            "training_time_seconds": training_time,
            "history": self.history,
        }

    def fit_incremental(
        self,
        X: torch.Tensor,
        y: torch.Tensor,
        epochs: int = 10,
        batch_size: int = 64,
        validation_split: float = 0.15,
    ) -> dict:
        """
        Incremental training (fine-tuning)

        Uses lower learning rate and fewer epochs

        Args:
            X: Input features
            y: Ground truth
            epochs: Number of epochs
            batch_size: Batch size
            validation_split: Validation fraction

        Returns:
            Training history
        """
        # Reduce learning rate for fine-tuning
        old_lr = self.optimizer.param_groups[0]["lr"]
        for param_group in self.optimizer.param_groups:
            param_group["lr"] = old_lr * 0.1

        logger.info(f"Starting incremental training: lr={old_lr * 0.1:.6f}")

        # Train
        result = self.fit(
            X,
            y,
            epochs=epochs,
            batch_size=batch_size,
            validation_split=validation_split,
            early_stopping_patience=5,  # Shorter patience
        )

        # Restore learning rate
        for param_group in self.optimizer.param_groups:
            param_group["lr"] = old_lr

        return result
