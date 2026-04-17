import glob
import importlib.util
from pathlib import Path

import torch


def _load_train_module():
    candidates = [
        Path(__file__).resolve().parents[3] / "train.py",
        Path("train.py"),
    ]
    for p in candidates:
        if p.exists():
            spec = importlib.util.spec_from_file_location("train_module", str(p))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            return mod
    raise FileNotFoundError("train.py not found at project root.")


class ModelRegistry:
    def __init__(self, storage_path: str = "data/models"):
        self.storage_path = Path(storage_path)
        self._device = "cuda" if torch.cuda.is_available() else "cpu"

    def load_latest(self, *args, **kwargs):
        candidates = sorted(
            self.storage_path.glob("**/*.pt"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            return None

        ckpt_path = candidates[0]
        ckpt = torch.load(ckpt_path, map_location=self._device, weights_only=False)

        # Must be a checkpoint dict saved by train.py
        if not isinstance(ckpt, dict) or "model_state" not in ckpt:
            return None

        tm = _load_train_module()

        class _Cfg:
            horizons    = ckpt["horizons"]
            context_len = ckpt["context_len"]
            patch_len   = ckpt["patch_len"]
            stride      = ckpt["stride"]
            d_model     = ckpt["d_model"]
            n_heads     = ckpt["n_heads"]
            n_layers    = ckpt["n_layers"]
            dropout     = ckpt["dropout"]

        num_channels = len(ckpt["feature_cols"])
        model = tm.PatchTSTMultiOutput(_Cfg(), num_channels, ckpt["cpu_idx"], ckpt["mem_idx"])
        model.load_state_dict(ckpt["model_state"])
        model = model.to(self._device)
        model.eval()

        return ckpt_path.stem, model