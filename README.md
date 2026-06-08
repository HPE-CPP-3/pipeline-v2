# Pipeline V2 - Decoupled Ingestion + Workload Forecasting

This repo now runs two independent agents connected only through shared state:

1. `LogIngestionAgent` (Stage 1)
2. `WorkloadPredictionAgent` (Stage 2)

They communicate through:

- Redis (latest state + durable stream trigger)
- InfluxDB (24h+ historical context)
- CSV files (live append for metrics and predictions)

## Stage 1: Plug-and-Play Ingestion

Input discovery uses a simple `target_config`:

- `namespace`
- `pod_name`
- `container_name` (optional)

No container URL is required. The agent generates PromQL dynamically using these labels and queries centralized Prometheus.

Execution frequency:

- Strict 60-second ticker (`run_loop`)

Collected feature matrix:

- Container-level:
  - `container_cpu_usage_seconds_total`
  - `container_cpu_cfs_throttled_seconds_total`
  - `container_memory_working_set_bytes`
  - `container_memory_failures_total`
- Node-level:
  - `node_load1`, `node_load5`, `node_load15`
  - `node_memory_MemAvailable_bytes`
  - `node_disk_read_bytes_total`
  - `node_network_transmit_bytes_total`
- K8s control-plane:
  - `kube_pod_container_resource_requests`
  - `kube_pod_container_resource_limits`
  - `kube_pod_status_phase`
  - `kube_pod_container_status_restarts_total`
- Derived signals:
  - efficiency: `usage / limit`
  - pressure: `throttled_time / total_time`
  - volatility: rolling stddev of CPU over 5m and 10m

Stage 1 output:

- Writes normalized features to Redis
- Writes time-series to InfluxDB
- Writes live metrics rows to CSV (`data/csv/metrics/...`)
- Publishes a durable trigger to `stream:ingestion:complete`

## Stage 2: Workload Prediction

This agent is event-driven and wakes up only when Stage 1 writes to Redis stream.

Workflow:

- Reads latest normalized features from Redis stream payload
- Fetches last 24 hours from InfluxDB for seasonal context
- Runs PatchTST inference for CPU and memory
- Computes confidence based on deviation from historical distribution
- Writes forecast to:
  - Redis stream: `stream:prediction:complete`
  - InfluxDB `predictions` measurement
  - CSV (`data/csv/predictions/...`)

## Web Interface / Observability

Run monitor API:

```bash
uvicorn src.agents.monitor_api:app --host 0.0.0.0 --port 8010
```

Then open:

- `http://localhost:8010/` (simple web page)
- `http://localhost:8010/health`
- `http://localhost:8010/status?namespace=<ns>&pod=<pod>`
- `http://localhost:8010/model/performance?namespace=<ns>&pod=<pod>`
- `http://localhost:8010/csv/status?namespace=<ns>&pod=<pod>`

Model clarity:

- If no saved model exists in `data/models`, runtime uses a fresh PatchTST init.
- `model/performance` reports:
  - latest model metadata (if present)
  - `NRMSE` (24h)
  - `bias` (24h)

## Running

Install:

```bash
pip install -e .
```

Run both agents together:

```bash
pipeline-agentic \
  --namespace <namespace> \
  --pod <pod-name> \
  --container <container-name> \
  --prometheus-url http://localhost:9090
```

Important local default for your dev setup:

- Redis container is mapped to host port `6380`
- Set `PIPELINE_REDIS_PORT=6380` (or pass as env before run)

Run ingestion only:

```bash
pipeline-agentic --agent ingestion --namespace <namespace> --pod <pod-name>
```

Run prediction only:

```bash
pipeline-agentic \
  --agent prediction \
  --namespace dummy \
  --pod dummy
```

## Test Environment

- Cluster setup: `docs/CLUSTER_SETUP.md`
- Full quickstart: `docs/QUICKSTART.md`
