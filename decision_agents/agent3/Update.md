# Agent 3 — Resource Optimization
## What Was There Before · What Was Added · Why It Matters

---

## 1. Overview

Agent 3 sits between Agent 2 (forecasting) and Agent 4 (governance). It reads predicted CPU and memory risk signals, then decides whether to scale up, scale down, hold, or retrain.

The original implementation worked but treated every pod identically regardless of criticality. The updated version adds **live Kubernetes querying** and **QoS-aware decision thresholds** — while leaving the core four-branch logic completely unchanged.

| | Before | After |
|---|---|---|
| Data source | Agent 2 forecast only | Agent 2 forecast + live K8s query |
| `current_replicas` | Hardcoded to `1` | Read from live Deployment |
| `cpu_request` / `memory_request` | Never available | Fetched from K8s |
| QoS class | Unknown — all pods treated the same | Detected: Guaranteed / Burstable / BestEffort |
| Scaling thresholds | Same for every pod (`TARGET=0.70`, `MIN_CONF=0.65`, `MIN_REP=1`) | Differ by QoS class (Burstable row is identical to original) |

---

## 2. What Was There Before

### 2.1 The Four-Branch Engine

The core logic evaluated four branches in order. **This is completely unchanged.**

```
Branch 3 — RETRAIN:     if confidence < 0.30
Branch 1 — SCALE_UP:    if throttle_risk or oom_risk is HIGH or CRITICAL
Branch 2 — SCALE_DOWN:  if both risks are LOW and confidence >= 0.65
Branch 4 — HOLD:        none of the above
```

### 2.2 Hardcoded Thresholds (the problem)

Every pod used the same three values regardless of how critical it was:

```python
# Original OptimizationConfig
TARGET_UTILIZATION     = 0.70   # scale up when pressure exceeds 70%
SCALE_DOWN_MIN_CONF    = 0.65   # scale down needs 65% confidence
MIN_REPLICAS           = 1      # always allowed to drop to 1 replica
```

A critical payments pod and a throwaway batch job received identical scaling decisions. A `Guaranteed` pod (where `cpu_request == cpu_limit`) should never be scaled down as aggressively as a `BestEffort` pod with no resource spec at all.

### 2.3 The `current_replicas` Default

In Redis mode, the current replica count was hardcoded. The original code even acknowledged this gap with a comment:

```python
# Original Redis mode
forecast = {
    "namespace":        msg.get("namespace", ""),
    "pod":              msg.get("pod", ""),
    "current_replicas": 1,   # default; future: query K8s API  ← the comment
}
```

### 2.4 No `cpu_request` or `memory_request`

Agent 2 only sends **limits** from its Prometheus scrape. Requests were never available. Without requests it is impossible to compute the official Kubernetes QoS class — because the QoS formula requires comparing request vs limit for both CPU and memory.

---

## 3. What Was Added

### 3.1 Three Target Constants

Added near the top of the file, after the logging setup. These are the single source of truth for which workload Agent 3 monitors. If the target ever changes, only these three lines need updating.

```python
TARGET_NAMESPACE  = "test-workload"
TARGET_DEPLOYMENT = "stress-test-app"
TARGET_CONTAINER  = "stress-test-app"   # container name inside the pod spec
```

> The deployment name is used instead of the pod name (`stress-test-app-975d97b75-q7t2w`) because pod names change on every restart. The deployment name is stable.

---

### 3.2 `K8sResourceFetcher` (new class)

An entirely new class that connects to the live Kubernetes cluster and fetches real resource values. It makes two queries per event:

| Method | What It Does |
|---|---|
| `get_resources_for_stress_test_app()` | Lists pods with `label app=stress-test-app`, picks a Running pod, reads the `stress-test-app` container resource spec. Returns `cpu_request`, `cpu_limit`, `memory_request`, `memory_limit`. |
| `get_live_replica_count()` | Reads the Deployment object directly and returns `ready_replicas`. Fixes the hardcoded default of `1`. |

**LATENCY — 30 second cache:**

```python
# Before hitting the API, check the cache first
cached_at, cached_value = self._cache[cache_key]
age = time.monotonic() - cached_at
if age < self.cache_ttl_seconds:   # default 30s
    return cached_value            # skip the 20-50ms API call entirely
```

**GRACEFUL DEGRADATION — every method is wrapped in try/except:**

```python
except Exception as e:
    logger.warning(f"[K8s] Failed: {e} — using forecast-provided values")
    return self._empty_resources()  # returns zeros, Agent 3 never crashes
```

**Auto-detects environment:**

```python
try:
    config.load_incluster_config()  # inside a pod — ServiceAccount token
except Exception:
    config.load_kube_config()       # local dev — ~/.kube/config
```

**Why label selector instead of pod name:**

```python
# Pod name changes every restart:
#   stress-test-app-975d97b75-q7t2w  ->  stress-test-app-975d97b75-xp9kl
#
# Label selector is stable:
pod_list = self.core_v1.list_namespaced_pod(
    namespace      = TARGET_NAMESPACE,
    label_selector = f"app={TARGET_DEPLOYMENT}",  # app=stress-test-app
)
```

> If your deployment uses a different label, the pod list will return empty and the fetcher falls back to zeros. Check with: `kubectl get pods -n test-workload --show-labels`

---

### 3.3 QoS Detection

A new enum and a pure function that applies the official Kubernetes QoS classification rules:

```python
class QoSClass(str, Enum):
    GUARANTEED  = "Guaranteed"   # cpu_request == cpu_limit AND mem_request == mem_limit
    BURSTABLE   = "Burstable"    # at least one request < limit  (most common)
    BEST_EFFORT = "BestEffort"   # nothing set at all

def detect_qos_class(cpu_request, cpu_limit, mem_request, mem_limit) -> QoSClass:
    if all values are 0:                        return BestEffort
    if request == limit for BOTH cpu and memory: return Guaranteed
    return Burstable
```

**Concrete example for stress-test-app:**

```python
# If the pod spec has:
#   requests.cpu: 250m   limits.cpu: 500m
#   requests.memory: 256Mi   limits.memory: 512Mi
#
# Then: 0.25 != 0.50  ->  Burstable  (most common real-world case)
```

---

### 3.4 QoS-Aware Thresholds in `OptimizationConfig`

The three flat hardcoded values are replaced by a lookup table. The **Burstable row preserves the original values exactly** — so if the K8s query fails and QoS defaults to Burstable, Agent 3 behaves identically to before.

```python
QOS_POLICY = {
    QoSClass.GUARANTEED: {
        "target_utilization":   0.60,   # scale up earlier — protect critical pods
        "scale_down_min_conf":  0.85,   # very cautious before scaling down
        "min_replicas":         2,      # never drop below 2
    },
    QoSClass.BURSTABLE: {
        "target_utilization":   0.70,   # ← original hardcoded value, unchanged
        "scale_down_min_conf":  0.65,   # ← original hardcoded value, unchanged
        "min_replicas":         1,      # ← original hardcoded value, unchanged
    },
    QoSClass.BEST_EFFORT: {
        "target_utilization":   0.85,   # tolerate more pressure
        "scale_down_min_conf":  0.50,   # scale down aggressively
        "min_replicas":         1,
    },
}
```

| QoS Class | target_utilization | scale_down_min_conf | min_replicas |
|---|---|---|---|
| Guaranteed | 0.60 — scale up earlier | 0.85 — very cautious | 2 — never drop below 2 |
| **Burstable (default)** | **0.70 — original value** | **0.65 — original value** | **1 — original value** |
| BestEffort | 0.85 — tolerate more | 0.50 — scale down easily | 1 |

---

### 3.5 New Step 0 in `ResourceOptimizationAgent.run()`

The only change to the main `run()` method. Two new steps inserted before the existing Step 1. Step 1 itself is completely untouched.

```python
# BEFORE — run() started directly at Step 1
logger.info("Step 1: Applying Four-Branch Optimization Rule...")

# AFTER — Step 0 inserted before Step 1
if self.k8s is not None:
    logger.info("Step 0: Querying live Kubernetes cluster...")

    k8s_res       = self.k8s.get_resources_for_stress_test_app()
    live_replicas = self.k8s.get_live_replica_count()

    # Always trust live replica count
    forecast["current_replicas"] = live_replicas        # fixes hardcoded 1

    # Requests only come from K8s — Agent 2 never sends them
    forecast["cpu_request"]    = k8s_res["cpu_request"]    # brand new field
    forecast["memory_request"] = k8s_res["memory_request"] # brand new field

    # Only overwrite limits if K8s returned something real
    # (keeps Agent 2 limits as fallback if K8s partially failed)
    if k8s_res["cpu_limit"] > 0:
        forecast["cpu_limit"] = k8s_res["cpu_limit"]
    if k8s_res["memory_limit"] > 0:
        forecast["memory_limit"] = k8s_res["memory_limit"]

# Step 0b: detect QoS and store in forecast for the engine to read
qos_class = detect_qos_class(cpu_request, cpu_limit, mem_request, mem_limit)
forecast["qos_class"] = qos_class.value

# Step 1: Four-Branch Decision — completely unchanged
logger.info("Step 1: Applying Four-Branch Optimization Rule...")
```

---

### 3.6 Three New Fields Passed to Agent 4

`ScalingDecision` dataclass and `to_agent4_payload()` now include three new fields. Agent 4 will not break without reading them — but once Agent 4 is updated, it will have full QoS context for its governance decisions and LLM prompt.

```python
# Added to ScalingDecision dataclass
cpu_request:    float   # from live K8s query
memory_request: float   # from live K8s query
qos_class:      str     # "Guaranteed" | "Burstable" | "BestEffort"

# Added to to_agent4_payload()
"cpu_request":    round(float(decision.cpu_request  or 0.0), 6),
"memory_request": int(decision.memory_request        or 0),
"qos_class":      decision.qos_class,
```

---

### 3.7 The `--no-k8s` Flag

A new CLI argument that disables all Kubernetes queries. Useful for local testing without a cluster connection. When set, Agent 3 uses the forecast file values and QoS defaults to Burstable.

```bash
# With cluster (production or local dev with kubectl configured)
python agent3_optimization.py --forecast dummy_forecast.json

# Without cluster (pure local testing)
python agent3_optimization.py --forecast dummy_forecast.json --no-k8s

# Redis mode with cluster
python agent3_optimization.py --mode redis

# Redis mode without cluster
python agent3_optimization.py --mode redis --no-k8s
```

---

## 4. What Did NOT Change

| Component | Status |
|---|---|
| `_RedisSingleInstanceLock` | Identical |
| `ScalingAction` and `RiskLevel` enums | Identical |
| `ReplicaCalculator` math (scale_up / scale_down formulas) | Identical — QoS-aware values passed in, not hardcoded inside |
| Four-branch structure in `OptimizationEngine` | Identical — same four branches, same order |
| `_parse_forecast()` and `_get_quantile()` helpers | Identical |
| Redis stream names | Identical — `stream:prediction:complete` / `stream:optimization:complete` |
| All original CLI arguments | Identical |
| `action_log.json` and `agent4_ready_payload.json` output | Identical — three new keys added to `agent4_ready_payload.json` |

---

## 5. Impact Summary

| Scenario | Before | After |
|---|---|---|
| Guaranteed pod, HIGH risk | SCALE_UP using 0.70 target | SCALE_UP using 0.60 target — more replicas, earlier action |
| Guaranteed pod, scale down | Scale down at `conf=0.65`, `min=1` | Needs `conf=0.85`, `min=2` — much more conservative |
| **Burstable pod (most common)** | **Original behaviour** | **Identical to original — no change** |
| BestEffort pod, LOW risk | HOLD at `min=1`, needs `conf=0.65` | Scale down sooner, needs only `conf=0.50` |
| K8s cluster unreachable | N/A — cluster never queried | Falls back to forecast values, QoS defaults to Burstable — identical to original |

---

## 6. RBAC Requirements

If Agent 3 runs as a pod inside the cluster, its ServiceAccount needs read access:

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

Without this, the `ApiException` fallback fires and Agent 3 continues with forecast-provided values.

---

> **Target workload:** `test-workload / stress-test-app / stress-test-app`  
> **Install dependency:** `pip install kubernetes`