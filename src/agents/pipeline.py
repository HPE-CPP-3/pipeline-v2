"""Minimal orchestration wrappers.

This module keeps orchestration intentionally simple:
- Stage 1 ingestion runs on a strict 60s ticker.
- Stage 2 prediction is event-driven from Redis Streams.

No direct agent-to-agent calls are used.
"""

from __future__ import annotations

import asyncio

from .ingestion_agent import LogIngestionAgent
from .prediction_agent import WorkloadPredictionAgent
from .schemas import TargetConfig


class TwoStagePipeline:
    """Runs ingestion and prediction independently in parallel loops."""

    def __init__(
        self,
        ingestion_agent: LogIngestionAgent,
        prediction_agent: WorkloadPredictionAgent,
    ):
        self.ingestion_agent = ingestion_agent
        self.prediction_agent = prediction_agent

    async def run(self, target_config: TargetConfig, window_minutes: int = 60) -> None:
        await asyncio.gather(
            self.ingestion_agent.run_loop(
                target_config=target_config,
                window_minutes=window_minutes,
            ),
            self.prediction_agent.run_loop(),
        )
