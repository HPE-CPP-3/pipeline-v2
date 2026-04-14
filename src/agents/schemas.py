"""Schemas shared across the two-stage decoupled pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd


@dataclass(frozen=True)
class PodScope:
    pod: str
    namespace: str
    container: Optional[str] = None


@dataclass(frozen=True)
class TargetConfig:
    namespace: str
    pod_name: str
    container_name: Optional[str] = None


@dataclass
class IngestionResult:
    scope: PodScope
    raw_metrics: pd.DataFrame
    metadata: dict[str, Any]
