# Implementation Summary

## Current Architecture — Five Agents, Shared State

The pipeline is composed of five fully decoupled agents that communicate exclusively through shared state (Redis Streams, InfluxDB, CSV files). No agent calls another directly.

```
Agent 1 (Ingestion)
  └─► stream:ingestion:complete
        └─► Agent 2 (Prediction)
              └─► stream:prediction:complete
                    └─► Agent 3 (Optimization)
                          └─► stream:optimization:complete
                                └─► Agent 4 (Governance)
                                      └─► stream:governance:complete
                                            └─► Agent 5 (Executor) ──► Kubernetes
```

---

## Agent 1 — Ingestion (`src/agents/ingestion_agent.py`)

**Mode:** Strict 60-second Prometheus polling loop  
**Input:** `namespace` + `pod` + optional `container` labels  

Collects and normalizes:
- Container: CPU usage/throttle, memory working set/failcnt
- Node: load (1m/5m/15m), memory available, disk, network
- K8s: requests/limits, pod phase, restart count
- Derived: `derived_usage_vs_limit`, `derived_pressure_throttled_ratio`, `derived_cpu_volatility_{5,10}m`

**Key behavior:**  
Raw limits are embedded in `raw_limits_json` **before** rolling min-max normalization, including the new `cpu_usage_cores` field (actual measured Prometheus CPU in cores). This allows the prediction agent to perform idle detection against live measurements.

Publishes `stream:ingestion:complete` with `features_json` and `raw_limits_json`.

---

## Agent 2 — Prediction (`src/agents/prediction_agent.py`)

**Mode:** Event-driven — wakes on `stream:ingestion:complete`  
**Model:** PatchTST (multivariate, multi-horizon, multi-quantile)

### Two-Layer Inference (Option A)
1. PatchTST forward pass → CPU forecast (5/10/15m, p50/p70/p90) + throttle logit
2. PatchTST forward pass → Memory forecast (5/10/15m, p50/p70/p90) + OOM logit
3. `ThrottleRiskCalculator` enriches with rule-based ratios (horizon: **5 min**, configurable in `configs/prediction.yaml`)
4. `OOMRiskCalculator` enriches similarly

### Forecast Sanity Clamp (Idle Detection)
When `actual_cpu_cores < p90_5m_forecast / 3`, the model context still has stale high-CPU history. The clamp replaces all forecast quantiles with `actual_cpu × 1.2` (20% headroom), ensuring the pipeline can scale down promptly instead of waiting for the 60-step context window to flush.

Log signature: `FORECAST CLAMP: actual_cpu=X.XXX < p90/3=Y.YYY — dampening...`

### Incremental Fine-Tuning
- Triggers every 60 ingestion steps (≈1h)
- 30-min global cooldown between retrains, max 8/day
- Hot-reloads checkpoint weights without restarting
- Can also be triggered via `stream:retrain:request` (from Agent 5 drift detector)

**Output:** `stream:prediction:complete` · InfluxDB `predictions` · CSV `data/csv/predictions/`

---

## Agent 3 — Optimization (`decision_agents/agent3/agent3_optimization.py`)

### Four-Branch Decision Engine (unchanged core logic)

```
if confidence < 0.30           → RETRAIN
if throttle_risk or oom_risk is HIGH/CRITICAL → SCALE_UP
if both risks LOW and conf >= min_conf        → SCALE_DOWN
otherwise                                     → HOLD
```

### QoS-Aware Thresholds

Auto-detected from live Kubernetes resource spec (30s cache):

| QoS | target_util | scale_down_min_conf | min_replicas |
|-----|-------------|---------------------|--------------|
| Guaranteed | 60% | 0.85 | 2 |
| **Burstable** (default) | **70%** | **0.65** | **1** |
| BestEffort | 85% | 0.50 | 1 |

Falls back gracefully to Burstable if K8s is unreachable.

### Scale-Down Cooldown
Configurable via `SCALE_DOWN_COOLDOWN_SEC` (default: `300`s).  
Prevents back-to-back scale-down actions after a recent scale-up.

**Output:** `stream:optimization:complete` · `decision_agents/agent3/agent3_log.txt`

---

## Agent 4 — Governance (`decision_agents/agent4/agent4_governance.py`)

### Execution Flow

```
Payload in
  │
  ├─► Circuit Breaker 1: OOM Panic Fast-Track (memory_p90_5m > 95%)
  │     └─► Immediate APPROVED (bypass LLM)
  │
  ├─► Circuit Breaker 2: Absolute Max Cap (> 20 replicas)
  │     └─► Cap to 20 and APPROVED_WITH_CAP
  │
  ├─► Rule Engine → raises flags
  │     - LOW_CONFIDENCE
  │     - AGGRESSIVE_SCALE
  │     - HIGH_CPU_RISK  (suppressed for scale_up — high CPU confirms scale-up)
  │     - HIGH_MEMORY_RISK  (suppressed for scale_up)
  │     - SCALE_DOWN_RISK
  │
  ├─► No flags → auto-APPROVED
  └─► Flags → LLM Reasoning → APPROVED / APPROVED_WITH_CAP / REJECTED
```

### LLM Escalation
The LLM prompt explicitly grounds the model in Kubernetes autoscaling physics:  
*"Scaling up when CPU is high DISTRIBUTES load — never reject a scale_up because CPU is high."*  
The LLM is only asked to check: aggressive jumps, low confidence, flapping patterns.

### Anti-Flapping
After any approved scaling action, writes Redis key `cooldown:<ns>:<deployment>` with 300s TTL. Scale-down requests during the cooldown window are rejected with `ANTI_FLAPPING_COOLDOWN`.

**LLM backends:** `local` (llama-cpp GGUF) · `gemini` · `hosted` (OpenAI-compatible)

**Output:** `stream:governance:complete` · `decision_agents/agent4/agent4_log.txt`

---

## Agent 5 — Executor (`decision_agents/agent5/agent5_executor.py`)

Two async loops running in parallel:

1. **Governance listener** — reads `stream:governance:complete`, resolves pod → Deployment via `OwnerReferences`, calls `patch_namespaced_deployment_scale`
2. **Drift listener** — reads `stream:ingestion:complete`, runs EMA drift detector (z=3σ), publishes `stream:retrain:request` if >20% of features drift

**Output:** Kubernetes scale operations · `stream:retrain:request` · `decision_agents/agent5/agent5_log.txt`

---

## Shared State Communication

| Store | What's Stored | Who Writes | Who Reads |
|-------|--------------|------------|-----------|
| Redis Streams | Durable event triggers between agents | Agents 1→2→3→4→5 | Next agent in chain |
| Redis Keys | Feature state, lock keys, cooldown keys | Agents 1, 3, 4 | Agents 2, 3, 4 |
| InfluxDB | Long-term metrics + prediction history | Agents 1, 2 | Dashboard, Agent 2 context |
| CSV | Append-only metrics + predictions (for fine-tuning) | Agents 1, 2 | Agent 2 fine-tuner |

---

## Configuration Files

| File | What It Controls |
|------|-----------------|
| `configs/prediction.yaml` | Forecast horizons, risk thresholds (throttle/OOM ratios), inference interval |
| `configs/training.yaml` | Drift detection (alpha, z_threshold, feature_ratio) |
| `scripts/stress-test-deployment.yaml` | Test workload: Low (1 CPU/120s) → High (4 CPU/60s) → Low (1 CPU/60s) → Idle (600s) |

---

## Operational Modes

| Mode | Command |
|------|---------|
| All 5 agents | `bash scripts/run_all_agents.sh --source prometheus --namespace <ns> --pod <pod> --llm-provider local` |
| Short cooldown for testing | `SCALE_DOWN_COOLDOWN_SEC=60 bash scripts/run_all_agents.sh ...` |
| Agents 1+2 only | `python -m src.agents.runtime --agent both --namespace <ns> --pod <pod> --prometheus-url http://localhost:30000` |
| Agent 4 file mode | `python decision_agents/agent4/agent4_governance.py --payload dummy_payload.json --llm-provider local` |
