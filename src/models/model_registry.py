"""
Model registry for versioned model storage
"""

import json
import torch
from pathlib import Path
from datetime import datetime
from typing import Optional, Any
import logging

from .patchtst import PatchTST

logger = logging.getLogger(__name__)


class ModelRegistry:
    """
    Registry for versioned model storage

    Versioning scheme: {timestamp}-{type}
    Examples:
    - 20260413-120000-base
    - 20260413-180000-incremental

    Stores:
    - Model weights (.pt file)
    - Metadata (JSON)
    - Config (YAML)
    """

    def __init__(self, storage_path: str = "/data/models", keep_last_n: int = 10):
        """
        Args:
            storage_path: Base path for model storage
            keep_last_n: Number of models to keep (cleanup older)
        """
        self.storage_path = Path(storage_path)
        self.keep_last_n = keep_last_n

        # Create directories
        self.storage_path.mkdir(parents=True, exist_ok=True)
        (self.storage_path / "base").mkdir(exist_ok=True)
        (self.storage_path / "incremental").mkdir(exist_ok=True)

        # Active model
        self.active_model: Optional[PatchTST] = None
        self.active_model_version: Optional[str] = None

        logger.info(f"Initialized ModelRegistry at {self.storage_path}")

    def _generate_version(self, model_type: str = "incremental") -> str:
        """
        Generate version string

        Args:
            model_type: "base" or "incremental"

        Returns:
            Version string (e.g., "20260413-120000-incremental")
        """
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        return f"{timestamp}-{model_type}"

    def save(
        self,
        model: PatchTST,
        model_type: str = "incremental",
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Save model to registry

        Args:
            model: PatchTST model
            model_type: "base" or "incremental"
            metadata: Optional metadata dict

        Returns:
            Version string
        """
        # Generate version
        version = self._generate_version(model_type)

        # Create model directory
        model_dir = self.storage_path / model_type / version
        model_dir.mkdir(parents=True, exist_ok=True)

        # Save weights
        weights_path = model_dir / "model.pt"
        torch.save(model.state_dict(), weights_path)

        # Save metadata
        metadata = metadata or {}
        metadata.update(
            {
                "version": version,
                "model_type": model_type,
                "saved_at": datetime.now().isoformat(),
                "config": {
                    "patch_size": model.patch_size,
                    "patch_stride": model.patch_stride,
                    "n_layers": model.n_layers,
                    "d_model": model.d_model,
                    "n_heads": model.n_heads,
                    "d_ff": model.d_ff,
                    "forecast_horizons": model.forecast_horizons,
                    "quantiles": model.quantiles,
                },
            }
        )

        metadata_path = model_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)

        logger.info(f"Saved model {version} to {model_dir}")

        # Cleanup old models
        self._cleanup_old_models(model_type)

        return version

    def load(
        self,
        version: str,
        model_type: str = "incremental",
    ) -> PatchTST:
        """
        Load model from registry

        Args:
            version: Version string
            model_type: "base" or "incremental"

        Returns:
            Loaded PatchTST model
        """
        model_dir = self.storage_path / model_type / version

        if not model_dir.exists():
            raise FileNotFoundError(f"Model {version} not found")

        # Load metadata
        metadata_path = model_dir / "metadata.json"
        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        # Reconstruct model from config
        config = metadata.get("config", {})
        model = PatchTST(
            patch_size=config.get("patch_size", 16),
            patch_stride=config.get("patch_stride", 8),
            n_layers=config.get("n_layers", 3),
            d_model=config.get("d_model", 128),
            n_heads=config.get("n_heads", 8),
            d_ff=config.get("d_ff", 256),
            forecast_horizons=config.get("forecast_horizons", [5, 10, 15]),
            quantiles=config.get("quantiles", [0.5, 0.7, 0.9]),
        )

        # Load weights
        weights_path = model_dir / "model.pt"
        model.load_state_dict(torch.load(weights_path, weights_only=True))

        logger.info(f"Loaded model {version} from {model_dir}")

        return model

    def load_latest(
        self, model_type: str = "incremental"
    ) -> Optional[tuple[str, PatchTST]]:
        """
        Load latest model of given type

        Args:
            model_type: "base" or "incremental"

        Returns:
            (version, model) or None if no models found
        """
        model_type_dir = self.storage_path / model_type

        if not model_type_dir.exists():
            return None

        # Find latest version (sorted by name, which is timestamp-based)
        versions = sorted([d.name for d in model_type_dir.iterdir() if d.is_dir()])

        if not versions:
            return None

        latest_version = versions[-1]
        model = self.load(latest_version, model_type)

        return latest_version, model

    def set_active_model(self, version: str, model_type: str = "incremental"):
        """
        Set active model for inference

        Args:
            version: Version string
            model_type: "base" or "incremental"
        """
        model = self.load(version, model_type)

        self.active_model = model
        self.active_model_version = version

        logger.info(f"Set active model: {version}")

    def get_active_model(self) -> Optional[PatchTST]:
        """
        Get active model for inference

        Returns:
            Active PatchTST model or None
        """
        return self.active_model

    def get_active_version(self) -> Optional[str]:
        """
        Get active model version

        Returns:
            Version string or None
        """
        return self.active_model_version

    def list_models(self, model_type: Optional[str] = None) -> list[dict[str, str]]:
        """
        List all models in registry

        Args:
            model_type: Optional filter ("base" or "incremental")

        Returns:
            List of model metadata dicts
        """
        models = []

        types_to_check = [model_type] if model_type else ["base", "incremental"]

        for mt in types_to_check:
            type_dir = self.storage_path / mt

            if not type_dir.exists():
                continue

            for version_dir in type_dir.iterdir():
                if not version_dir.is_dir():
                    continue

                metadata_path = version_dir / "metadata.json"

                if metadata_path.exists():
                    with open(metadata_path, "r") as f:
                        metadata = json.load(f)

                    metadata["path"] = str(version_dir)
                    models.append(metadata)

        # Sort by saved_at
        models.sort(key=lambda m: m.get("saved_at", ""), reverse=True)

        return models

    def _cleanup_old_models(self, model_type: str):
        """
        Remove old models, keeping only last N

        Args:
            model_type: "base" or "incremental"
        """
        type_dir = self.storage_path / model_type

        if not type_dir.exists():
            return

        # Get all versions sorted
        versions = sorted([d.name for d in type_dir.iterdir() if d.is_dir()])

        # Remove old ones
        if len(versions) > self.keep_last_n:
            to_remove = versions[: -self.keep_last_n]

            for version in to_remove:
                version_dir = type_dir / version

                # Remove directory
                import shutil

                shutil.rmtree(version_dir)

                logger.info(f"Removed old model: {version}")

    async def get_last_base_training(self) -> Optional[datetime]:
        """
        Get timestamp of last base model training

        Returns:
            datetime or None
        """
        base_dir = self.storage_path / "base"

        if not base_dir.exists():
            return None

        versions = sorted([d.name for d in base_dir.iterdir() if d.is_dir()])

        if not versions:
            return None

        # Get metadata of latest
        latest_version = versions[-1]
        metadata_path = base_dir / latest_version / "metadata.json"

        if metadata_path.exists():
            with open(metadata_path, "r") as f:
                metadata = json.load(f)

            saved_at = metadata.get("saved_at")
            if saved_at:
                return datetime.fromisoformat(saved_at)

        return None
