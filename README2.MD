# Running Agents 3 and 4 After Agents 1 and 2

This guide explains how to run the decision-making agents (Agent 3: Resource Optimization and Agent 4: Governance) after the data pipeline agents (Agent 1: Ingestion and Agent 2: Prediction) have been executed.

## Prerequisites

1. **Agents 1 and 2 must be running first (unless you use `run_all_agents.sh`)**: Ensure that the ingestion and prediction pipeline is active and publishing data to Redis streams.
2. **Redis must be running**: The agents communicate via Redis streams. Default configuration expects Redis on `localhost:6380`.
3. **Python environment**: Install dependencies with either:
  - `pip install -r scripts/requirements.txt` and `pip install -e .`, or
  - `pip install -e .` (installs dependencies from `pyproject.toml`)
4. **(Optional) Kubernetes integration for Agent 3**: If you want Agent 3 to query live Kubernetes for requests/limits and replica count, install:
  - `pip install kubernetes`
  Agent 3 will use in-cluster config when running inside Kubernetes, otherwise it will use your local kubeconfig (`~/.kube/config`).

## Agent Overview

- **Agent 1 (Ingestion)**: Collects metrics from Prometheus and publishes to Redis stream `stream:ingestion:complete`
- **Agent 2 (Prediction)**: Listens to `stream:ingestion:complete`, runs PatchTST forecasting, publishes to `stream:prediction:complete`
- **Agent 3 (Optimization)**: Listens to `stream:prediction:complete`, applies scaling rules, publishes to `stream:optimization:complete`
- **Agent 4 (Governance)**: Listens to `stream:optimization:complete`, applies governance checks with LLM reasoning, publishes to `stream:governance:complete`

## Running Agents 1 and 2

First, start the data pipeline agents. You can run them together or separately:

### Run both agents together:
```bash
pipeline-agentic \
  --agent both \
  --namespace <namespace> \
  --pod <pod-name> \
  --container <container-name> \
  --prometheus-url <prometheus-url>
```

Notes:
- In the dev cluster setup used in this repo, Prometheus is often exposed via NodePort (e.g. `http://localhost:30000`).

### Run ingestion only:
```bash
pipeline-agentic --agent ingestion --namespace <namespace> --pod <pod-name>
```

### Run prediction only:
```bash
pipeline-agentic \
  --agent prediction \
  --namespace dummy \
  --pod dummy
```

Set environment variables as needed:
- `PIPELINE_REDIS_PORT=6380` (for local dev setup)
- `INFLUXDB_URL`, `INFLUXDB_TOKEN`, etc. for InfluxDB storage

## Run All 4 Agents With One Script (Mac/Linux)

If you want a single command to start the full pipeline (Agents 1→4), use:

```bash
bash scripts/run_all_agents.sh \
  --source prometheus \
  --namespace <namespace> \
  --pod <pod-name> \
  --container <container-name> \
  --prometheus-url <prometheus-url>
```

Or, for Prometheus-free local testing where Agent 1 reads raw metrics from Redis and Agent 2 predicts from the ingestion stream:

```bash
bash scripts/run_all_agents.sh --source redis
```

### Individual Agent Commands

Alternatively, run each agent separately:

#### Agent 3: Resource Optimization

Run in Redis streaming mode to listen for prediction events:

```bash
python decision_agents/agent3/agent3_optimization.py \
  --mode redis \
  --log-file decision_agents/agent3/agent3_log.txt
```

Optional parameters:
- `--redis-host localhost` (default: localhost)
- `--redis-port 6380` (default: 6380)

Agent 3 will:
- Listen on stream `stream:prediction:complete`
- Apply four-branch optimization logic (scale_up, scale_down, retrain, hold)
- Publish decisions to `stream:optimization:complete`
- If `kubernetes` is installed and a cluster is reachable, it will also query live Kubernetes for:
  - CPU/memory requests and limits for the target container
  - Current live replica count from the Deployment

#### Agent 4: Governance and LLM Reasoning

Run in Redis streaming mode to listen for optimization decisions:

```bash
python decision_agents/agent4/agent4_governance.py \
  --mode redis \
  --log-file decision_agents/agent4/agent4_log.txt
```

Optional parameters:
- `--redis-host localhost` (default: localhost)
- `--redis-port 6380` (default: 6380)
- `--groq-key YOUR_GROQ_API_KEY` (for real LLM reasoning, otherwise uses mock responses)

Agent 4 will:
- Listen on stream `stream:optimization:complete`
- Apply rule-based governance checks
- Escalate flagged decisions to LLM reasoning
- Publish final governance decisions to `stream:governance:complete`

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

This will trigger both agents to process the test data and output decisions.

## Monitoring and Observability

- **Web Interface**: Run `uvicorn src.agents.monitor_api:app --host 0.0.0.0 --port 8010` and visit `http://localhost:8010/`
- **Logs**: Each agent outputs detailed logs to console
- **Decision Logs**:
  - Agent 3: `decision_agents/agent3/action_log.json` (+ `decision_agents/agent3/agent4_ready_payload.json`)
  - Agent 4: `decision_agents/agent4/decision_log.json`
- **Redis Streams**: Monitor streams with Redis CLI: `XREAD STREAMS stream:prediction:complete 0`

## Configuration

Key configuration files:
- `configs/features.yaml`: Feature extraction settings
- `configs/model.yaml`: Model hyperparameters
- `configs/training.yaml`: Training parameters

## Troubleshooting

- **No events received**: Ensure agents 1 and 2 are running and Redis is accessible
- **Redis connection errors**: Check Redis port and host settings
- **Agent 3 Kubernetes warnings**:
  - If you see `kubernetes package not installed`, run `pip install kubernetes`.
  - If you see `Could not connect to cluster`, verify your kubeconfig (`~/.kube/config`) or in-cluster ServiceAccount.
  - If you see pod lookup warnings, Agent 3 derives its pod selector from the Deployment's `spec.selector.matchLabels` (fallback to `app=<deployment>`). If your deployment name/namespace/container changed, update the `TARGET_*` constants in `decision_agents/agent3/agent3_optimization.py`.
- **LLM API errors**: For Agent 4, provide `GROQ_API_KEY` or it will use mock responses
- **Model not found**: Ensure `data/models/patchtst_multi.pt` exists from training

## Architecture Flow

```
Prometheus → Agent 1 → Redis → Agent 2 → Redis → Agent 3 → Redis → Agent 4 → Final Decision
     ↑           ↑           ↑           ↑           ↑           ↑           ↑
   Metrics    Ingestion   Prediction  Optimization Governance   Action
```

Each agent runs independently and communicates asynchronously through Redis streams, allowing for decoupled scaling and fault tolerance.
