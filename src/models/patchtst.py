# PatchTST is now loaded from checkpoints via ModelRegistry.
# This alias exists so `from ..models.patchtst import PatchTST` keeps working,
# but the class is never instantiated directly in the inference path.

class PatchTST:
    """Placeholder — real model is PatchTSTMultiOutput, loaded by ModelRegistry."""
    pass