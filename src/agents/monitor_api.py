"""Lightweight web interface for ingestion/prediction observability."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from ..models import ModelRegistry
from ..storage.influxdb_store import InfluxDBStore
from ..storage.redis_store import RedisStore


app = FastAPI(title="Pipeline V2 Monitor API", version="0.1.0")


def _redis() -> RedisStore:
    import os

    return RedisStore(
        host=os.getenv("PIPELINE_REDIS_HOST", "localhost"),
        port=int(os.getenv("PIPELINE_REDIS_PORT", "6379")),
        password=os.getenv("PIPELINE_REDIS_PASSWORD") or None,
    )


def _influx() -> InfluxDBStore:
    import os

    return InfluxDBStore(
        url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
        token=os.getenv("INFLUXDB_TOKEN", "dev-token-change-me"),
        org=os.getenv("INFLUXDB_ORG", "pipeline-v2"),
        bucket=os.getenv("INFLUXDB_BUCKET", "metrics"),
    )


@app.get("/health")
async def health() -> dict:
    redis = _redis()
    influx = _influx()
    try:
        pong = await redis.client.ping()
        ok = await influx.health_check()
        return {"redis": bool(pong), "influxdb": bool(ok), "ok": bool(pong and ok)}
    finally:
        await redis.close()
        await influx.close()


@app.get("/status")
async def status(namespace: str, pod: str) -> dict:
    redis = _redis()
    influx = _influx()
    try:
        feat_count = 0
        async for _ in redis.client.scan_iter(match=f"features:{namespace}:{pod}:*"):
            feat_count += 1

        ingestion_len = await redis.client.xlen("stream:ingestion:complete")
        prediction_len = await redis.client.xlen("stream:prediction:complete")

        metrics_df = await influx.get_historical_window(
            pod=pod,
            namespace=namespace,
            window_minutes=120,
        )
        prediction_df = await influx.get_prediction_history(
            pod=pod,
            namespace=namespace,
            hours=24,
        )

        return {
            "namespace": namespace,
            "pod": pod,
            "redis_feature_keys": feat_count,
            "stream_ingestion_complete": ingestion_len,
            "stream_prediction_complete": prediction_len,
            "influx_metrics_rows_2h": int(len(metrics_df)),
            "influx_prediction_rows_24h": int(len(prediction_df)),
        }
    finally:
        await redis.close()
        await influx.close()


@app.get("/model/performance")
async def model_performance(namespace: str, pod: str) -> dict:
    influx = _influx()
    import os

    registry = ModelRegistry(
        storage_path=os.getenv("PIPELINE_MODEL_PATH", "data/models")
    )
    models = registry.list_models()
    latest = models[0] if models else None
    try:
        nrmse = await influx.calculate_nrmse(pod=pod, namespace=namespace, hours=24)
        bias = await influx.calculate_bias(pod=pod, namespace=namespace, hours=24)
        return {
            "model_available": latest is not None,
            "latest_model": latest,
            "nrmse_24h": nrmse,
            "bias_24h": bias,
            "note": "If no trained model exists, runtime uses a fresh PatchTST init.",
        }
    finally:
        await influx.close()


@app.get("/csv/status")
async def csv_status(namespace: str, pod: str) -> dict:
    import os

    base = Path(os.getenv("PIPELINE_CSV_PATH", "data/csv"))
    metrics_file = base / "metrics" / f"{namespace}__{pod}.csv"
    pred_file = base / "predictions" / f"{namespace}__{pod}.csv"
    return {
        "metrics_csv": str(metrics_file),
        "metrics_exists": metrics_file.exists(),
        "metrics_size_bytes": metrics_file.stat().st_size
        if metrics_file.exists()
        else 0,
        "predictions_csv": str(pred_file),
        "predictions_exists": pred_file.exists(),
        "predictions_size_bytes": pred_file.stat().st_size if pred_file.exists() else 0,
    }


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return """
    <html>
      <head><title>Pipeline V2 Monitor</title></head>
      <body style="font-family: sans-serif; margin: 2rem;">
        <h1>Pipeline V2 Monitor</h1>
        <p>Use these endpoints:</p>
        <ul>
          <li><code>/health</code></li>
          <li><code>/status?namespace=&lt;ns&gt;&pod=&lt;pod&gt;</code></li>
          <li><code>/model/performance?namespace=&lt;ns&gt;&pod=&lt;pod&gt;</code></li>
          <li><code>/csv/status?namespace=&lt;ns&gt;&pod=&lt;pod&gt;</code></li>
        </ul>
      </body>
    </html>
    """
