# Pipeline V2 — Agentic Kubernetes Autoscaling

An end-to-end agentic pipeline that ingests live Kubernetes workload metrics, forecasts CPU/memory demand using PatchTST, and autonomously scales deployments through a four-stage decision chain.

## Architecture

Five agents communicate exclusively through shared state — no direct agent-to-agent calls:

```
Prometheus
    │
Agent 1 (Ingestion) ──► stream:ingestion:complete
                                │
                         Agent 2 (Prediction) ──► stream:prediction:complete
                                                          │
                                                   Agent 3 (Optimization) ──► stream:optimization:complete
                                                                                        │
                                                                                 Agent 4 (Governance) ──► stream:governance:complete
                                                                                                                  │
                                                                                                           Agent 5 (Executor)
                                                                                                                  │
                                                                                                          Kubernetes API
```

**Shared state:**
- **Redis** (`localhost:6380`) — latest feature state, durable stream triggers, single-instance locks, anti-flapping cooldowns
- **InfluxDB** (`localhost:8086`) — long-term time-series (metrics + predictions)
- **CSV** (`data/csv/`) — append-only outputs for metrics, predictions, and model fine-tuning

---

## Agent Reference

### Agent 1 — Ingestion (`src/agents/ingestion_agent.py`)

- Queries Prometheus every **60 seconds** using `namespace` + `pod` + optional `container` labels
- Collects: CPU usage/throttle, memory working set/failcnt, node load/memory/disk/network, K8s requests/limits
- Computes derived signals: efficiency (`usage/limit`), pressure (`throttled_time/total_time`), volatility (rolling stddev)
- Normalizes via rolling min-max
- Embeds `cpu_usage_cores` (raw Prometheus-measured CPU) into `raw_limits_json` for downstream idle detection
- Publishes trigger to `stream:ingestion:complete`

**Output streams/stores:** Redis feature state · InfluxDB `metrics` · CSV `data/csv/metrics/` · `stream:ingestion:complete`

---

### Agent 2 — Prediction (`src/agents/prediction_agent.py`)

- Wakes up on every `stream:ingestion:complete` event
- Loads last 90 CSV rows as model context
- Runs **PatchTST** inference: CPU + memory forecasts at horizons 5, 10, 15 minutes (p50/p70/p90 quantiles)
- Applies a **forecast sanity clamp** (idle detection): if actual live CPU < p90_forecast / 3, the forecast is dampened to `actual_cpu × 1.2` to prevent model context-lag from trapping the pipeline in scale-up during idle workloads
- Computes `ThrottleRiskCalculator` (CPU) and `OOMRiskCalculator` (memory): two-layer model + rule hybrid
- Periodically triggers incremental fine-tuning on recent CSV data (every 60 steps, guarded by 30-min global cooldown)

**Risk horizons:** configured to **5-minute** target via `configs/prediction.yaml`

**Output streams/stores:** `stream:prediction:complete` · InfluxDB `predictions` · CSV `data/csv/predictions/`

---

### Agent 3 — Optimization (`decision_agents/agent3/agent3_optimization.py`)

Implements a **four-branch decision engine**:

| Branch | Condition | Action |
|--------|-----------|--------|
| Branch 3 | `confidence < 0.30` | RETRAIN |
| Branch 1 | `throttle_risk` or `oom_risk` is HIGH or CRITICAL | SCALE_UP |
| Branch 2 | both risks LOW **and** `confidence >= scale_down_min_conf` | SCALE_DOWN |
| Branch 4 | none of the above | HOLD |

**QoS-aware thresholds** (auto-detected from live K8s resource spec):

| QoS | target_util | scale_down_min_conf | min_replicas |
|-----|-------------|---------------------|--------------|
| Guaranteed | 60% | 0.85 | 2 |
| **Burstable** (default) | **70%** | **0.65** | **1** |
| BestEffort | 85% | 0.50 | 1 |

**Scale-down cooldown:** configurable via `SCALE_DOWN_COOLDOWN_SEC` env var (default: `300`).  
When scaling down with short cooldown for testing, set `SCALE_DOWN_COOLDOWN_SEC=60`.

**K8s queries:** live `ready_replicas` from Deployment + container resource spec (30s TTL cache). Falls back gracefully to forecast values if cluster is unreachable.

**Output stream:** `stream:optimization:complete` · log: `decision_agents/agent3/agent3_log.txt`

---

### Agent 4 — Governance (`decision_agents/agent4/agent4_governance.py`)

The final gate before any action reaches Kubernetes. Implements two circuit breakers + LLM escalation:

**Circuit breakers (run before LLM):**
1. **OOM Panic Fast-Track** — immediately approves scale-up if `memory_p90_5m > 95%`
2. **Absolute Max Cap** — caps any recommendation above `MAX_REPLICAS_ABSOLUTE = 20`. If already at max, returns `APPROVED` (hold). If below max, caps and returns `APPROVED_WITH_CAP`

**Rule engine flags** (LLM escalation triggers):
- `LOW_CONFIDENCE` — model confidence below threshold
- `AGGRESSIVE_SCALE` — replica jump ratio exceeds threshold
- `HIGH_CPU_RISK` — suppressed for `scale_up` (high CPU is the reason to scale up, not a red flag)
- `HIGH_MEMORY_RISK` — suppressed for `scale_up` (same rationale)
- `SCALE_DOWN_RISK` — scale-down with low confidence

**LLM backends** (select via `--llm-provider`):
- `local` — local GGUF model via llama-cpp-python (default in production)
- `gemini` — Google Gemini API (`GEMINI_API_KEY`)
- `hosted` — OpenAI-compatible endpoint (`HOSTED_LLM_URL`, `HOSTED_LLM_MODEL`)

**Anti-flapping (Redis cooldown key):** After any approved scaling action, writes `cooldown:<ns>:<deployment>` with 300s TTL. Scale-down requests during this window are rejected with `ANTI_FLAPPING_COOLDOWN`.

**Output stream:** `stream:governance:complete` · log: `decision_agents/agent4/agent4_log.txt`

---

### Agent 5 — Executor (`decision_agents/agent5/agent5_executor.py`)

- Listens on `stream:governance:complete`
- Executes approved scale-up/scale-down via Kubernetes `patch_namespaced_deployment_scale`
- Resolves pod names to Deployment names via `OwnerReferences` traversal (with heuristic fallback)
- Runs a parallel **EMA drift detector** on `stream:ingestion:complete` — publishes retraining requests to `stream:retrain:request` when >20% of features drift beyond 3σ

**Output:** Kubernetes deployment scaling · `stream:retrain:request` · log: `decision_agents/agent5/agent5_log.txt`

---

## Quick Start

### 1. Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

### 2. Start local state stores

```bash
bash scripts/setup-local-dev.sh   # starts Redis on :6380 and InfluxDB on :8086
```

### 3. Start or connect a Kubernetes cluster with Prometheus

```bash
bash scripts/setup-test-cluster.sh   # brings up a local test cluster
```

Prometheus will be available at `http://localhost:30000`.

Deploy the stress workload:

```bash
kubectl apply -f scripts/stress-test-deployment.yaml
kubectl get pods -n test-workload
```

### 4. Run all 5 agents (recommended)

```bash
bash scripts/run_all_agents.sh \
  --source prometheus \
  --namespace test-workload \
  --pod <stress-test-app-pod> \
  --llm-provider local
```

With a short scale-down cooldown for testing:

```bash
SCALE_DOWN_COOLDOWN_SEC=60 bash scripts/run_all_agents.sh \
  --source prometheus \
  --namespace test-workload \
  --pod <stress-test-app-pod> \
  --llm-provider local
```

### 5. Run agents individually

**Agents 1 + 2 (Ingestion + Prediction):**
```bash
python -m src.agents.runtime \
  --agent both \
  --namespace test-workload \
  --pod <pod> \
  --prometheus-url http://localhost:30000 \
  --model-path data/models
```

**Agent 3 (Optimization):**
```bash
python decision_agents/agent3/agent3_optimization.py \
  --mode redis \
  --redis-host localhost \
  --redis-port 6380
```

**Agent 4 (Governance):**
```bash
python decision_agents/agent4/agent4_governance.py \
  --mode redis \
  --redis-host localhost \
  --redis-port 6380 \
  --llm-provider local
```

**Agent 5 (Executor):**
```bash
python decision_agents/agent5/agent5_executor.py \
  --redis-host localhost \
  --redis-port 6380
```

---

## Troubleshooting

### Clear locks after an unclean restart

```bash
redis-cli -p 6380 DEL lock:agent3:optimization lock:agent4:governance lock:agent5:executor
```

### Force-clear anti-flapping cooldown to allow immediate scale-down

```bash
redis-cli -p 6380 DEL cooldown:test-workload:stress-test-app
```

### Pipeline stuck in SCALE_UP during idle workload

This means the model's context window still has recent high-CPU history. The **forecast sanity clamp** in Agent 2 normally resolves this automatically (detects actual CPU < forecast/3 and dampens the forecast). If it persists, check that `cpu_usage_cores` is non-zero in the `raw_limits_json` field in `stream:ingestion:complete`.

### Agent 4 rejected a valid scale-up

The LLM rejected the decision. Check `agent4_log.txt` for the `LLM Reasoning` block. Since `HIGH_CPU_RISK` is now suppressed for `scale_up` actions, this should only happen if there is also a `LOW_CONFIDENCE` or `AGGRESSIVE_SCALE` flag. If the LLM is still hallucinating backwards physics, consider switching to `--llm-provider gemini`.

### Lock held by crashed process

```bash
redis-cli -p 6380 GET lock:agent3:optimization   # see who holds it
redis-cli -p 6380 DEL lock:agent3:optimization   # force-release
```

---

## Observe Outputs

### Decision agent logs
```bash
tail -f decision_agents/agent3/agent3_log.txt
tail -f decision_agents/agent4/agent4_log.txt
tail -f decision_agents/agent5/agent5_log.txt
```

### Redis stream inspection
```bash
redis-cli -p 6380 XREVRANGE stream:prediction:complete + - COUNT 1
redis-cli -p 6380 XREVRANGE stream:optimization:complete + - COUNT 1
redis-cli -p 6380 XREVRANGE stream:governance:complete + - COUNT 1
```

### CSV outputs
```
data/csv/metrics/     — raw + normalized ingestion rows
data/csv/predictions/ — PatchTST forecast rows
```

### Dashboard (K8s Sentinel)
```bash
uvicorn dashboard.main:app --host 0.0.0.0 --port 8000 --reload
# Open http://localhost:8000
```

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PIPELINE_REDIS_HOST` | `localhost` | Redis host |
| `PIPELINE_REDIS_PORT` | `6380` | Redis port |
| `INFLUXDB_URL` | `http://localhost:8086` | InfluxDB URL |
| `INFLUXDB_ORG` | `pipeline-v2` | InfluxDB org |
| `INFLUXDB_BUCKET` | `metrics` | InfluxDB bucket |
| `PIPELINE_PROMETHEUS_URL` | `http://localhost:30000` | Prometheus URL |
| `SCALE_DOWN_COOLDOWN_SEC` | `300` | Agent 3 scale-down cooldown (seconds) |
| `GEMINI_API_KEY` | — | Gemini API key (for `--llm-provider gemini`) |
| `HOSTED_LLM_URL` | — | Hosted OpenAI-compatible endpoint |
| `HOSTED_LLM_MODEL` | `gemma4:12b` | Hosted LLM model name |

Risk calculation thresholds: `configs/prediction.yaml`

---

## Docs

- Cluster setup: `docs/CLUSTER_SETUP.md`
- Quickstart: `docs/QUICKSTART.md`
- Implementation summary: `docs/IMPLEMENTATION_SUMMARY.md`
