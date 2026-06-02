# Pipeline V3 - Decoupled Ingestion, Workload Forecasting, Optimization + Governance

This repo now runs four independent agents connected only through shared state:

1. `LogIngestionAgent` (Stage 1)
2. `WorkloadPredictionAgent` (Stage 2)
3. `ResourceOptimizationAgent` (Stage 3)
4. `GovernanceAgent` (Stage 4)

They communicate through:

- Redis (latest state + durable stream triggers)
- InfluxDB (24h+ historical context)
- CSV files and JSON logs (live append for metrics, predictions, and actions)

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

## Stage 3: Resource Optimization

This agent is event-driven and wakes up only when Stage 2 writes to the Redis prediction stream.

Workflow:

- Listens on Redis stream `stream:prediction:complete`
- Applies four-branch optimization logic (`scale_up`, `scale_down`, `retrain`, `hold`) based on prediction data and rules
- Writes optimization decisions to:
  - Redis stream: `stream:optimization:complete`
  - Action log: `action_log.json`

## Stage 4: Governance and LLM Reasoning

This agent is event-driven and wakes up only when Stage 3 writes to the Redis optimization stream.

Workflow:

- Listens on Redis stream `stream:optimization:complete`
- Applies rule-based governance checks to validate safety and compliance of proposed scaling actions
- Escapes/escalates flagged decisions to LLM reasoning (using Groq LLM API if configured, otherwise falls back to mock responses)
- Writes final governance decisions to:
  - Redis stream: `stream:governance:complete`
  - Decision log: `decision_log.json`

## Web Interface / Observability & Logs

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

Other observability details:

- **Logs**: Each agent outputs detailed logs to the console.
- **Decision Logs**: Stage 3 and Stage 4 save decisions to `action_log.json` and `decision_log.json` respectively.
- **Redis Streams**: Monitor stream entries using Redis CLI (e.g., `XREAD STREAMS stream:prediction:complete 0` or check the other streams).

## Running

Install dependencies:

```bash
pip install -r requirements.txt
pip install -e .
```

### Running Stage 1 & 2 (Data Pipeline)

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

### Running Stage 3 & 4 (Decision Agents)

Once Stage 1 & 2 are running and publishing to Redis streams, start the decision agents:

#### Run Agents 3 and 4 Together

Use the provided scripts to launch both decision agents simultaneously:

**PowerShell (Windows):**
```powershell
# Run both agents
powershell -ExecutionPolicy Bypass -File scripts\run_pipeline.ps1

# Run only Agent 3
powershell -ExecutionPolicy Bypass -File scripts\run_pipeline.ps1 -Agent 3

# Run only Agent 4
powershell -ExecutionPolicy Bypass -File scripts\run_pipeline.ps1 -Agent 4
```

**Bash (Linux/Mac):**
```bash
# Run both agents
bash scripts/run_pipeline.sh

# Run only Agent 3
bash scripts/run_pipeline.sh --agent 3

# Run only Agent 4
bash scripts/run_pipeline.sh --agent 4
```

#### Individual Agent Commands

Alternatively, run each agent separately:

##### Agent 3: Resource Optimization

Run in Redis streaming mode to listen for prediction events:

```bash
python decision_agents/agent3/agent3_optimization.py --mode redis
```

Optional parameters:
- `--redis-host localhost` (default: localhost)
- `--redis-port 6380` (default: 6380)

##### Agent 4: Governance and LLM Reasoning

Run in Redis streaming mode to listen for optimization decisions:

```bash
python decision_agents/agent4/agent4_governance.py --mode redis
```

Optional parameters:
- `--redis-host localhost` (default: localhost)
- `--redis-port 6380` (default: 6380)
- `--groq-key YOUR_GROQ_API_KEY` (for real LLM reasoning, otherwise uses mock responses)

## Testing with Dummy Data

For development/testing without running the full pipeline:

### Test Agent 3:
```bash
python decision_agents/agent3/agent3_optimization.py --forecast decision_agents/agent3/dummy_forecast.json
```

### Test Agent 4:
```bash
python decision_agents/agent4/agent4_governance.py --payload decision_agents/agent4/dummy_payload.json
```

### Test with Redis Streams

When running agents 3 and 4 with the pipeline scripts, you can inject test data directly into Redis streams:

```bash
redis-cli -p 6380 XADD stream:prediction:complete '*' \
  namespace test-ns pod test-pod container test-c \
  forecast_json '{"cpu_forecast":{"5":{"0.5":0.6,"0.9":0.82},"15":{"0.5":0.65,"0.9":0.91}},"memory_forecast":{"5":{"0.5":0.55,"0.9":0.76},"15":{"0.5":0.60,"0.9":0.88}},"throttle_prob":0.87,"oom_prob":0.76,"confidence":0.73,"throttle_risk":{"risk_level":"HIGH"},"oom_risk":{"oom_risk":"HIGH"}}'
```

## Configuration

Key configuration files:
- `configs/features.yaml`: Feature extraction settings
- `configs/model.yaml`: Model hyperparameters
- `configs/training.yaml`: Training parameters

## Troubleshooting Stage 3 and 4

- **No events received**: Ensure agents 1 and 2 are running and Redis is accessible.
- **Redis connection errors**: Check Redis port and host settings.
- **LLM API errors**: For Agent 4, provide `GROQ_API_KEY` or it will use mock responses.
- **Model not found**: Ensure `data/models/patchtst_multi.pt` exists from training.

## Architecture Flow

```
Prometheus → Agent 1 → Redis → Agent 2 → Redis → Agent 3 → Redis → Agent 4 → Final Decision
     ↑           ↑           ↑           ↑           ↑           ↑           ↑
   Metrics    Ingestion   Prediction  Optimization Governance   Action
```

Each agent runs independently and communicates asynchronously through Redis streams, allowing for decoupled scaling and fault tolerance.

## Test Environment

- Cluster setup: `docs/CLUSTER_SETUP.md`
- Full quickstart: `docs/QUICKSTART.md`
