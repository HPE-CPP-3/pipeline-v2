"""Runtime CLI for the decoupled 2-stage pipeline."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import torch
import sys

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
        port=int(os.getenv("PIPELINE_REDIS_PORT", "6380")),
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
    import glob
    import importlib.util
    from pathlib import Path

    # Load train.py helpers to get PatchTSTMultiOutput
    candidates_train = [
        Path(__file__).resolve().parents[3] / "train.py",
        Path("train.py"),
    ]
    train_mod = None
    for p in candidates_train:
        if p.exists():
            spec = importlib.util.spec_from_file_location("train_module", str(p))
            train_mod = importlib.util.module_from_spec(spec)
            sys.modules["train_module"] = train_mod
            spec.loader.exec_module(train_mod)
            break

    # Find checkpoint
    candidates_ckpt = sorted(
        glob.glob(str(Path(model_path) / "**" / "patchtst_multi.pt"), recursive=True)
    )
    ckpt_path = candidates_ckpt[0] if candidates_ckpt else str(Path(model_path) / "patchtst_multi.pt")

    device = "cuda" if torch.cuda.is_available() else "cpu"

    if train_mod and Path(ckpt_path).exists():
        ckpt = torch.load(ckpt_path, map_location=device)

        class _Cfg:
            horizons = ckpt["horizons"]
            context_len = ckpt["context_len"]
            patch_len = ckpt["patch_len"]
            stride = ckpt["stride"]
            d_model = ckpt["d_model"]
            n_heads = ckpt["n_heads"]
            n_layers = ckpt["n_layers"]
            dropout = ckpt["dropout"]

        num_channels = len(ckpt["feature_cols"])
        cpu_idx = ckpt["cpu_idx"]
        mem_idx = ckpt["mem_idx"]
        feature_cols = ckpt["feature_cols"]

        model = train_mod.PatchTSTMultiOutput(_Cfg(), num_channels, cpu_idx, mem_idx)
        model.load_state_dict(ckpt["model_state"])
        model = model.to(device)
        model.eval()
        logger.info(f"Loaded PatchTSTMultiOutput from checkpoint: {ckpt_path}")
        horizons = ckpt["horizons"]
        
        # Extract normalization stats for CPU and memory
        mu_dict = ckpt.get("mu", {})
        sigma_dict = ckpt.get("sigma", {})
        
        cpu_col = feature_cols[cpu_idx]
        mem_col = feature_cols[mem_idx]
        
        cpu_mu = float(mu_dict.get(cpu_col, 0.0))
        cpu_sigma = float(sigma_dict.get(cpu_col, 1.0))
        mem_mu = float(mu_dict.get(mem_col, 0.0))
        mem_sigma = float(sigma_dict.get(mem_col, 1.0))
        
        logger.info(f"CPU normalization: mu={cpu_mu:.4f}, sigma={cpu_sigma:.4f}")
        logger.info(f"Memory normalization: mu={mem_mu:.0f}, sigma={mem_sigma:.0f}")
        
    else:
        logger.warning("No checkpoint found. Predictors will use a dummy model.")
        model = None
        horizons = [5, 10, 15]
        cpu_mu, cpu_sigma = 0.0, 1.0
        mem_mu, mem_sigma = 0.0, 1.0

    return (CPUPredictor(model, horizons=horizons, cpu_mu=cpu_mu, cpu_sigma=cpu_sigma),
            MemoryPredictor(model, horizons=horizons, mem_mu=mem_mu, mem_sigma=mem_sigma))

def _build_prediction_agent(
    redis_store: RedisStore,
    influx_store: InfluxDBStore,
    csv_store: CSVStore,
    cpu_predictor: CPUPredictor,
    memory_predictor: MemoryPredictor,
    model_path: str,
    finetune_every_n_steps: int,
    finetune_rows: int,
    global_cooldown_seconds: int,
    max_retrains_per_day: int,
    csv_metrics_path: str,
) -> WorkloadPredictionAgent:
    """Construct WorkloadPredictionAgent with all fine-tune params."""
    # Resolve the checkpoint path — look for patchtst_multi.pt first (train.py output),
    # fall back to whatever ModelRegistry would use.
    import glob
    from pathlib import Path

    candidates = sorted(glob.glob(str(Path(model_path) / "**" / "patchtst_multi.pt"), recursive=True))
    checkpoint_path = candidates[0] if candidates else str(Path(model_path) / "patchtst_multi.pt")

    return WorkloadPredictionAgent(
        redis_store=redis_store,
        influxdb_store=influx_store,
        csv_store=csv_store,
        cpu_predictor=cpu_predictor,
        memory_predictor=memory_predictor,
        finetune_every_n_steps=finetune_every_n_steps,
        finetune_rows=finetune_rows,
        checkpoint_path=checkpoint_path,
        csv_metrics_path=csv_metrics_path,
        global_cooldown_seconds=global_cooldown_seconds,
        max_retrains_per_day=max_retrains_per_day,
    )


async def run_pipeline(
    target_config: TargetConfig,
    prometheus_url: str,
    model_path: str,
    finetune_every_n_steps: int,
    finetune_rows: int,
    global_cooldown_seconds: int,
    max_retrains_per_day: int,
) -> None:
    redis_store = _build_redis_store()
    influx_store = _build_influx_store()
    csv_store = _build_csv_store()
    prom = PrometheusClient(url=prometheus_url)
    csv_metrics_path = os.path.join(os.getenv("PIPELINE_CSV_PATH", "data/csv"), "metrics")

    cpu_predictor, memory_predictor = _build_predictors(model_path=model_path)

    ingestion_agent = LogIngestionAgent(
        prometheus_client=prom,
        redis_store=redis_store,
        influxdb_store=influx_store,
        csv_store=csv_store,
    )
    prediction_agent = _build_prediction_agent(
        redis_store=redis_store,
        influx_store=influx_store,
        csv_store=csv_store,
        cpu_predictor=cpu_predictor,
        memory_predictor=memory_predictor,
        model_path=model_path,
        finetune_every_n_steps=finetune_every_n_steps,
        finetune_rows=finetune_rows,
        global_cooldown_seconds=global_cooldown_seconds,
        max_retrains_per_day=max_retrains_per_day,
        csv_metrics_path=csv_metrics_path,
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


async def run_prediction_only(
    model_path: str,
    finetune_every_n_steps: int,
    finetune_rows: int,
    global_cooldown_seconds: int,
    max_retrains_per_day: int,
) -> None:
    redis_store = _build_redis_store()
    influx_store = _build_influx_store()
    csv_store = _build_csv_store()
    csv_metrics_path = os.path.join(os.getenv("PIPELINE_CSV_PATH", "data/csv"), "metrics")
    cpu_predictor, memory_predictor = _build_predictors(model_path=model_path)

    prediction_agent = _build_prediction_agent(
        redis_store=redis_store,
        influx_store=influx_store,
        csv_store=csv_store,
        cpu_predictor=cpu_predictor,
        memory_predictor=memory_predictor,
        model_path=model_path,
        finetune_every_n_steps=finetune_every_n_steps,
        finetune_rows=finetune_rows,
        global_cooldown_seconds=global_cooldown_seconds,
        max_retrains_per_day=max_retrains_per_day,
        csv_metrics_path=csv_metrics_path,
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

    # Fine-tune knobs
    parser.add_argument(
        "--finetune-every",
        type=int,
        default=int(os.getenv("PIPELINE_FINETUNE_EVERY_N_STEPS", "60")),
        help="Trigger fine-tune after every N ingestion events (default: 60 ~ 1h)",
    )
    parser.add_argument(
        "--finetune-rows",
        type=int,
        default=int(os.getenv("PIPELINE_FINETUNE_ROWS", "500")),
        help="Number of recent CSV rows to fine-tune on (default: 500)",
    )
    parser.add_argument(
        "--finetune-cooldown",
        type=int,
        default=int(os.getenv("PIPELINE_FINETUNE_COOLDOWN_SECONDS", "1800")),
        help="Minimum seconds between fine-tune runs (default: 1800 = 30min)",
    )
    parser.add_argument(
        "--finetune-max-per-day",
        type=int,
        default=int(os.getenv("PIPELINE_FINETUNE_MAX_PER_DAY", "8")),
        help="Maximum fine-tune runs per day (default: 8)",
    )

    args = parser.parse_args()

    target_config = TargetConfig(
        namespace=args.namespace,
        pod_name=args.pod,
        container_name=args.container,
    )

    ft_kwargs = dict(
        finetune_every_n_steps=args.finetune_every,
        finetune_rows=args.finetune_rows,
        global_cooldown_seconds=args.finetune_cooldown,
        max_retrains_per_day=args.finetune_max_per_day,
    )

    if args.agent == "ingestion":
        asyncio.run(
            run_ingestion_only(
                target_config=target_config,
                prometheus_url=args.prometheus_url,
            )
        )
    elif args.agent == "prediction":
        asyncio.run(
            run_prediction_only(
                model_path=args.model_path,
                **ft_kwargs,
            )
        )
    else:
        asyncio.run(
            run_pipeline(
                target_config=target_config,
                prometheus_url=args.prometheus_url,
                model_path=args.model_path,
                **ft_kwargs,
            )
        )


if __name__ == "__main__":
    main()