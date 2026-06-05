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

Open:

- `http://localhost:8010/` (simple web page)
- `http://localhost:8010/health`
- `http://localhost:8010/status?namespace=<ns>&pod=<pod>`
- `http://localhost:8010/model/performance?namespace=<ns>&pod=<pod>`
- `http://localhost:8010/csv/status?namespace=<ns>&pod=<pod>`

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
