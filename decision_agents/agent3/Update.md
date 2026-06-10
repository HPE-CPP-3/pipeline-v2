# Agent 3 — Resource Optimization

## Overview

Agent 3 sits between Agent 2 (forecasting) and Agent 4 (governance). It reads predicted CPU/memory risk signals from `stream:prediction:complete` and decides whether to scale up, scale down, hold, or trigger retraining.

---

## Four-Branch Decision Engine

Evaluated strictly in this order:

| Branch | Condition | Action |
|--------|-----------|--------|
| Branch 3 | `confidence < 0.30` | RETRAIN |
| Branch 1 | `throttle_risk` or `oom_risk` is HIGH or CRITICAL | SCALE_UP |
| Branch 2 | both risks LOW **and** `confidence >= scale_down_min_conf` **and** cooldown elapsed | SCALE_DOWN |
| Branch 4 | none of the above | HOLD |

---

## QoS-Aware Thresholds

Agent 3 queries the live Kubernetes cluster (30s TTL cache) to read actual resource requests/limits and derive the pod's QoS class:

| QoS | target_util | scale_down_min_conf | min_replicas |
|-----|-------------|---------------------|--------------|
| Guaranteed | 60% | 0.85 | 2 |
| **Burstable** (default) | **70%** | **0.65** | **1** |
| BestEffort | 85% | 0.50 | 1 |

If the cluster is unreachable, falls back to **Burstable** (identical to original behaviour).

---

## Scale-Down Cooldown

An in-process cooldown prevents scale-down immediately after a scale-up:

```python
self.scale_down_cooldown_sec = float(os.environ.get("SCALE_DOWN_COOLDOWN_SEC", "300.0"))
```

Override with the environment variable:
```bash
SCALE_DOWN_COOLDOWN_SEC=60 bash scripts/run_all_agents.sh ...  # faster testing
```

---

## K8s Resource Fetcher

- Reads live `ready_replicas` from the Deployment object
- Reads `cpu_request`, `cpu_limit`, `memory_request`, `memory_limit` from a running pod's container spec
- Uses a **30-second TTL cache** to avoid hammering the API on every event
- Gracefully degrades: any exception returns zeros and falls back to forecast-provided values

Auto-detects environment:
```python
try:
    config.load_incluster_config()   # inside a pod — uses ServiceAccount token
except:
    config.load_kube_config()        # local dev — uses ~/.kube/config
```

---

## Replica Calculator

**Scale-up:** `ceil(current_replicas × (cpu_pressure / target_utilization))`  
**Scale-down:** `ceil(current_replicas × (cpu_pressure / target_utilization))`

Where `cpu_pressure = cpu_p90_5m / cpu_limit`.

---

## CLI Usage

```bash
# Production (Redis mode, with K8s cluster)
python decision_agents/agent3/agent3_optimization.py \
  --mode redis \
  --redis-host localhost \
  --redis-port 6380

# Without K8s cluster (pure forecast values, QoS defaults to Burstable)
python decision_agents/agent3/agent3_optimization.py \
  --mode redis \
  --no-k8s

# File mode (test with a JSON forecast)
python decision_agents/agent3/agent3_optimization.py \
  --forecast dummy_forecast.json
```

---

## RBAC Requirements (in-cluster)

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: agent3-reader
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list"]
- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["get", "list"]
```

---

## Output

- Publishes to `stream:optimization:complete`
- Logs to `decision_agents/agent3/agent3_log.txt`
- Single-instance lock: `lock:agent3:optimization` (30s TTL, auto-refreshed)

---

## What Did NOT Change from Original

| Component | Status |
|-----------|--------|
| `_RedisSingleInstanceLock` | Identical |
| `ScalingAction` and `RiskLevel` enums | Identical |
| `ReplicaCalculator` math | Identical |
| Four-branch structure and order | Identical |
| Redis stream names | Identical |
| All original CLI arguments | Identical |