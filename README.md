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
# Pipeline V3 — Decoupled Ingestion → Forecasting → Decision Agents

This repository is an agentic pipeline that ingests Kubernetes workload metrics, builds features, forecasts CPU/memory using PatchTST, and then runs downstream decision-making agents.

The codebase is named “pipeline-v3” (workspace), while the Python package metadata/entrypoint still uses the older name (`pipeline-v2`, `pipeline-agentic`).

## Architecture (Agents + Shared State)

Agents communicate only through shared state (no direct calls):

- **Redis**: latest feature state + durable triggers via Redis Streams
- **InfluxDB**: time-series history (metrics + predictions)
- **CSV**: append-only outputs for metrics/predictions and model fine-tuning

### Agent 1 — Ingestion (Stage 1)

Primary mode:

- `LogIngestionAgent` queries Prometheus using `namespace`, `pod`, and optional `container` labels.
- Runs on a strict 60s loop.

Outputs:

- Writes normalized features/state to Redis
- Writes time-series to InfluxDB
- Appends rows under `data/csv/metrics/`
- Publishes trigger events to Redis stream: `stream:ingestion:complete`

Optional mode (Prometheus-free):

- `RedisIngestionBridge` reads raw metrics from `stream:metrics:latest` (or keys) and emits `stream:ingestion:complete`.

### Agent 2 — Prediction (Stage 2)

`WorkloadPredictionAgent` is event-driven:

- Wakes up on `stream:ingestion:complete`
- Pulls history from InfluxDB for context
- Runs PatchTST inference for CPU + memory
- Optionally fine-tunes periodically using recent CSV rows

Outputs:

- Publishes to `stream:prediction:complete`
- Writes predictions to InfluxDB
- Appends rows under `data/csv/predictions/`

### Agent 3 — Optimization (Decision Agent)

- Consumes `stream:prediction:complete`
- Produces optimization actions/events on `stream:optimization:complete`
- Logs to `decision_agents/agent3/agent3_log.txt`
- Enforces a single-instance Redis lock: `lock:agent3:optimization`

### Agent 4 — Governance (Decision Agent)

- Consumes `stream:optimization:complete`
- Produces governance actions/events on `stream:governance:complete`
- Logs to `decision_agents/agent4/agent4_log.txt`
- Enforces a single-instance Redis lock: `lock:agent4:governance`
- Can optionally use `GROQ_API_KEY` (if provided)

## Install

From the repo root:

```bash
python3 -m venv .venv
source .venv/bin/activate

pip install -U pip
pip install -e .
```

Notes:

- If you don’t want editable install, you can still run via module mode with `PYTHONPATH="$PWD"` (see below).

## Run

### Option A (recommended): Start all 4 agents

This is the most reliable “one command” runner:

```bash
bash scripts/run_all_agents.sh \
  --source prometheus \
  --namespace <ns> \
  --pod <pod> \
  --container <container> \
  --prometheus-url http://localhost:9090
```

If your Prometheus is exposed via a Kubernetes NodePort (common in local test clusters), pass that URL instead (example):

```bash
bash scripts/run_all_agents.sh --source prometheus --namespace <ns> --pod <pod> --prometheus-url http://localhost:30000
```

Prometheus-free mode (Agent 1 reads raw metrics from Redis, then continues 2→3→4):

```bash
bash scripts/run_all_agents.sh --source redis
```

### Option B: Run Agents 1 + 2 only

If `pipeline-agentic` is installed:

```bash
pipeline-agentic \
  --agent both \
  --namespace <ns> \
  --pod <pod> \
  --container <container> \
  --prometheus-url http://localhost:9090
```

If you hit import issues (or you didn’t install editable), use module mode:

```bash
PYTHONPATH="$PWD" python -m src.agents.runtime \
  --agent both \
  --namespace <ns> \
  --pod <pod> \
  --container <container> \
  --prometheus-url http://localhost:9090
```

### Option C: Run decision agents (3 + 4) manually

Run from the repo root so log paths resolve correctly:

```bash
python decision_agents/agent3/agent3_optimization.py \
  --mode redis \
  --redis-host localhost \
  --redis-port 6380 \
  --log-file decision_agents/agent3/agent3_log.txt

python decision_agents/agent4/agent4_governance.py \
  --mode redis \
  --redis-host localhost \
  --redis-port 6380 \
  --log-file decision_agents/agent4/agent4_log.txt
```

If you need to restart Agent 3/4 and they complain about locks:

```bash
redis-cli -p 6380 DEL lock:agent3:optimization lock:agent4:governance
```

## Observe outputs

### 1) CSV outputs (most visible)

- Metrics: `data/csv/metrics/*.csv`
- Predictions: `data/csv/predictions/*.csv`

These are append-only and are the easiest way to confirm the pipeline is producing data.

### 2) Decision agent logs

```bash
tail -f decision_agents/agent3/agent3_log.txt
tail -f decision_agents/agent4/agent4_log.txt
```

### 3) InfluxDB (long-term storage)

If InfluxDB is enabled/configured, the pipeline writes to these measurements in the configured bucket (default bucket: `metrics`):

- `metrics` (telemetry)
- `predictions` (forecasts + confidence)
- `decisions` (optimization/governance actions)
- `feedback` (predicted vs actual; used for performance metrics)

### 4) Redis stream flow (debugging)

Pipeline event streams:

- `stream:ingestion:complete` (Agent 1 → Agent 2)
- `stream:prediction:complete` (Agent 2 → Agent 3)
- `stream:optimization:complete` (Agent 3 → Agent 4)
- `stream:governance:complete` (Agent 4 output)

### 5) Web monitor API

Run:

```bash
uvicorn src.agents.monitor_api:app --host 0.0.0.0 --port 8010
```

Then open:
Open:

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
pip install -r requirements.txt
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
## Configuration (defaults)

- Redis: `PIPELINE_REDIS_HOST` (default `localhost`), `PIPELINE_REDIS_PORT` (default `6380`)
- InfluxDB: `INFLUXDB_URL` (default `http://localhost:8086`), `INFLUXDB_ORG` (default `pipeline-v2`), `INFLUXDB_BUCKET` (default `metrics`)
- CSV base path: `PIPELINE_CSV_PATH` (default `data/csv`)
- Prometheus URL: `PIPELINE_PROMETHEUS_URL` (default `http://localhost:9090`)

## Docs

- Cluster setup: `docs/CLUSTER_SETUP.md`
- Quickstart: `docs/QUICKSTART.md`
- Implementation summary: `docs/IMPLEMENTATION_SUMMARY.md`
- Project report: `docs/PROJECT_REPORT.md` (PDF: `docs/project_report.pdf`)
