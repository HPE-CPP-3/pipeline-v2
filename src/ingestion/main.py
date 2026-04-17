"""
Ingestion main entry point
"""

import asyncio
import yaml
from pathlib import Path
import logging
import signal
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _resolve_config_path() -> Path:
    """Resolve ingestion config path for VM and container runs."""
    env_path = os.getenv("PIPELINE_INGESTION_CONFIG")
    if env_path:
        return Path(env_path)

    candidates = [
        Path("configs/ingestion.yaml"),
        Path(__file__).resolve().parents[2] / "configs" / "ingestion.yaml",
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate

    return candidates[0]


async def main():
    """Main ingestion loop"""
    logger.info("Starting Pipeline V2 Ingestion")

    # Load configuration
    config_path = _resolve_config_path()
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
    else:
        logger.warning("Config not found, using defaults")
        config = {}

    # Initialize components
    from ..storage.redis_store import RedisStore, EventStore
    from ..ingestion.prometheus_client import PrometheusClient
    from ..ingestion.feature_builder import FeatureBuilder
    from ..storage.parquet_store import ParquetStore

    # Redis
    redis_config = config.get("redis", {})
    redis_host = os.getenv("PIPELINE_REDIS_HOST", redis_config.get("host", "localhost"))
    redis_port = int(
        os.getenv("PIPELINE_REDIS_PORT", str(redis_config.get("port", 6379)))
    )
    redis_password = os.getenv("PIPELINE_REDIS_PASSWORD", redis_config.get("password"))
    redis_store = RedisStore(
        host=redis_host,
        port=redis_port,
        password=redis_password,
    )

    event_store = EventStore(redis_store.client)

    # Prometheus
    prom_config = config.get("prometheus", {})
    prom_url = os.getenv(
        "PIPELINE_PROMETHEUS_URL", prom_config.get("url", "http://localhost:9090")
    )
    prometheus_client = PrometheusClient(
        url=prom_url,
        timeout_seconds=prom_config.get("timeout_seconds", 30),
    )

    # Parquet storage
    parquet_config = config.get("parquet", {})
    parquet_path = os.getenv(
        "PIPELINE_PARQUET_PATH", parquet_config.get("base_path", "data/parquet")
    )
    parquet_store = ParquetStore(
        base_path=parquet_path,
    )

    # Feature builder
    feature_builder = FeatureBuilder(
        prometheus_client,
        event_store,
        redis_store,
        config.get("features", {}),
    )

    logger.info("External event watcher is disabled in VM mode")

    # Main ingestion loop
    ingestion_config = config.get("ingestion", {})
    refresh_interval = ingestion_config.get("refresh_interval_seconds", 60)

    logger.info(f"Starting ingestion loop (interval={refresh_interval}s)")

    # Handle shutdown signals
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    try:
        while not shutdown_event.is_set():
            start_time = asyncio.get_event_loop().time()

            # Get active pods
            active_pods = await prometheus_client.get_active_pods()

            logger.info(f"Processing {len(active_pods)} active pods")

            # Build features for each pod
            for pod_info in active_pods:
                try:
                    features = await feature_builder.build_features(
                        pod=pod_info["pod"],
                        namespace=pod_info["namespace"],
                        container=pod_info.get("container"),
                        window_minutes=90,
                    )

                    if features is not None:
                        # Store in Redis
                        # TODO: Store features

                        # Write to Parquet
                        await parquet_store.write_metrics(
                            namespace=pod_info["namespace"],
                            pod=pod_info["pod"],
                            df=features,
                        )

                except Exception as e:
                    logger.error(f"Error processing pod {pod_info['pod']}: {e}")

            # Sleep until next interval
            elapsed = asyncio.get_event_loop().time() - start_time
            sleep_time = max(0, refresh_interval - elapsed)

            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    finally:
        # Cleanup
        logger.info("Shutting down ingestion")
        await redis_store.close()


def run():
    """Sync wrapper for CLI entrypoints."""
    asyncio.run(main())


if __name__ == "__main__":
    run()
