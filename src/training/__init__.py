"""Training module - incremental fine-tuning for online learning."""

from .incremental_trainer import IncrementalTrainer, IncrementalConfig

__all__ = ["IncrementalTrainer", "IncrementalConfig"]