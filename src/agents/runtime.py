"""Runtime CLI for the decoupled 2-stage pipeline."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

from ..ingestion.prometheus_client import PrometheusClient
from ..models import ModelRegistry, PatchTST
from ..prediction import CPUPredictor, MemoryPredictor
from ..storage.csv_store import CSVStore
from ..storage.influxdb_store import InfluxDBStore
from ..storage.redis_store import RedisStore
from .ingestion_agent import LogIngestionAgent
from .pipeline import TwoStagePipeline
from .prediction_agent import WorkloadPredictionAgent
from .schemas import TargetConfig


logger = logging.getLogger(__name__)


def _build_redis_store() -> RedisStore:
    return RedisStore(
        host=os.getenv("PIPELINE_REDIS_HOST", "localhost"),
        port=int(os.getenv("PIPELINE_REDIS_PORT", "6379")),
        password=os.getenv("PIPELINE_REDIS_PASSWORD") or None,
    )


def _build_influx_store() -> InfluxDBStore:
    return InfluxDBStore(
        url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
        token=os.getenv("INFLUXDB_TOKEN", "dev-token-change-me"),
        org=os.getenv("INFLUXDB_ORG", "pipeline-v2"),
        bucket=os.getenv("INFLUXDB_BUCKET", "metrics"),
    )


def _build_csv_store() -> CSVStore:
    return CSVStore(base_path=os.getenv("PIPELINE_CSV_PATH", "data/csv"))


def _build_predictors(model_path: str) -> tuple[CPUPredictor, MemoryPredictor]:
    registry = ModelRegistry(storage_path=model_path)
    latest = registry.load_latest("incremental") or registry.load_latest("base")
    if latest:
        _, model = latest
    else:
        model = PatchTST(input_dim=1)
    return CPUPredictor(model), MemoryPredictor(model)


async def run_pipeline(
    target_config: TargetConfig, prometheus_url: str, model_path: str
) -> None:
    redis_store = _build_redis_store()
    influx_store = _build_influx_store()
    csv_store = _build_csv_store()
    prom = PrometheusClient(url=prometheus_url)

    cpu_predictor, memory_predictor = _build_predictors(model_path=model_path)

    ingestion_agent = LogIngestionAgent(
        prometheus_client=prom,
        redis_store=redis_store,
        influxdb_store=influx_store,
        csv_store=csv_store,
    )
    prediction_agent = WorkloadPredictionAgent(
        redis_store=redis_store,
        influxdb_store=influx_store,
        csv_store=csv_store,
        cpu_predictor=cpu_predictor,
        memory_predictor=memory_predictor,
    )

    pipeline = TwoStagePipeline(
        ingestion_agent=ingestion_agent,
        prediction_agent=prediction_agent,
    )

    try:
        await pipeline.run(target_config=target_config, window_minutes=60)
    finally:
        await redis_store.close()
        await influx_store.close()


async def run_ingestion_only(target_config: TargetConfig, prometheus_url: str) -> None:
    redis_store = _build_redis_store()
    influx_store = _build_influx_store()
    csv_store = _build_csv_store()
    prom = PrometheusClient(url=prometheus_url)
    ingestion_agent = LogIngestionAgent(
        prometheus_client=prom,
        redis_store=redis_store,
        influxdb_store=influx_store,
        csv_store=csv_store,
    )
    try:
        await ingestion_agent.run_loop(target_config=target_config, window_minutes=60)
    finally:
        await redis_store.close()
        await influx_store.close()


async def run_prediction_only(model_path: str) -> None:
    redis_store = _build_redis_store()
    influx_store = _build_influx_store()
    csv_store = _build_csv_store()
    cpu_predictor, memory_predictor = _build_predictors(model_path=model_path)
    prediction_agent = WorkloadPredictionAgent(
        redis_store=redis_store,
        influxdb_store=influx_store,
        csv_store=csv_store,
        cpu_predictor=cpu_predictor,
        memory_predictor=memory_predictor,
    )
    try:
        await prediction_agent.run_loop()
    finally:
        await redis_store.close()
        await influx_store.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Run decoupled ingestion + prediction agents"
    )
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--pod", required=True)
    parser.add_argument("--container", default=None)
    parser.add_argument(
        "--prometheus-url",
        default=os.getenv("PIPELINE_PROMETHEUS_URL", "http://localhost:9090"),
    )
    parser.add_argument(
        "--model-path", default=os.getenv("PIPELINE_MODEL_PATH", "data/models")
    )
    parser.add_argument(
        "--agent",
        choices=["both", "ingestion", "prediction"],
        default="both",
        help="Run both agents, only ingestion, or only prediction",
    )
    args = parser.parse_args()

    target_config = TargetConfig(
        namespace=args.namespace,
        pod_name=args.pod,
        container_name=args.container,
    )

    if args.agent == "ingestion":
        asyncio.run(
            run_ingestion_only(
                target_config=target_config,
                prometheus_url=args.prometheus_url,
            )
        )
    elif args.agent == "prediction":
        asyncio.run(run_prediction_only(model_path=args.model_path))
    else:
        asyncio.run(
            run_pipeline(
                target_config=target_config,
                prometheus_url=args.prometheus_url,
                model_path=args.model_path,
            )
        )


if __name__ == "__main__":
    main()
