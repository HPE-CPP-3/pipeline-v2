# Quickstart

## Prerequisites

- Kubernetes cluster with Prometheus (NodePort on `:30000`)
- Redis on `localhost:6380` and InfluxDB on `localhost:8086`
- Python 3.10+ with the repo installed (`pip install -e .`)

---

## 1. Start local state stores

```bash
bash scripts/setup-local-dev.sh
```

Starts Redis on `:6380` and InfluxDB on `:8086`.

---

## 2. Start a test Kubernetes cluster

```bash
bash scripts/setup-test-cluster.sh
```

Prometheus will be available at `http://localhost:30000`.

---

## 3. Deploy the stress test workload

```bash
kubectl apply -f scripts/stress-test-deployment.yaml
kubectl get pods -n test-workload
```

The workload cycles through:
- **Low load** — `stress --cpu 1` for 120s
- **High load** — `stress --cpu 4` for 60s
- **Low load** — `stress --cpu 1` for 60s
- **Idle** — `sleep 600`

Resources: `requests cpu=0.5 / memory=256Mi`, `limits cpu=2.0 / memory=512Mi` (QoS: Burstable)

---

## 4. Run all agents (recommended)

```bash
bash scripts/run_all_agents.sh \
  --source prometheus \
  --namespace test-workload \
  --pod <stress-test-app-xxxxxx-xxxxx> \
  --llm-provider local
```

**With a shorter scale-down cooldown for faster testing:**

```bash
SCALE_DOWN_COOLDOWN_SEC=60 bash scripts/run_all_agents.sh \
  --source prometheus \
  --namespace test-workload \
  --pod <stress-test-app-xxxxxx-xxxxx> \
  --llm-provider local
```

The script launches all five agents and prints the stream flow:

```
stream:ingestion:complete    <-- Agent 1 writes here
stream:prediction:complete   <-- Agent 2 writes here
stream:optimization:complete <-- Agent 3 writes here
stream:governance:complete   <-- Agent 4 writes here
stream:retrain:request       <-- Agent 5 writes retraining requests here
```

---

## 5. Watch it work

**Follow Agent 3 decisions in real time:**
```bash
tail -f decision_agents/agent3/agent3_log.txt
```

**Follow Agent 4 governance in real time:**
```bash
tail -f decision_agents/agent4/agent4_log.txt
```

**Watch replica count change:**
```bash
watch -n5 kubectl get deployment stress-test-app -n test-workload
```

**Inspect latest prediction:**
```bash
redis-cli -p 6380 XREVRANGE stream:prediction:complete + - COUNT 1
```

---

## 6. Trigger a manual scale-down (for testing)

If you want to force-test the scale-down path immediately (without waiting for idle):

```bash
# Clear any active cooldown key
redis-cli -p 6380 DEL cooldown:test-workload:stress-test-app

# Trigger a low-CPU prediction event
python scratch/trigger_scale_down.py
```

---

## 7. Open the dashboard

```bash
uvicorn dashboard.main:app --host 0.0.0.0 --port 8000 --reload
```

Open `http://localhost:8000` to see the K8s Sentinel dashboard.

---

## Alternative: Prometheus-free mode (Redis bridge)

If you don't have Prometheus, you can inject raw metric events directly:

```bash
bash scripts/run_all_agents.sh --source redis
```

Then publish sample events to `stream:metrics:latest`:

```python
import json, redis
from datetime import datetime, timezone

r = redis.Redis(host='localhost', port=6380, decode_responses=True)
r.xadd('stream:metrics:latest', {
    'namespace': 'test-workload',
    'pod': 'stress-test-app',
    'container': 'stress-container',
    'features_json': json.dumps({
        'container_cpu_usage_seconds_total': 1.5,
        'container_memory_working_set_bytes': 200_000_000,
    }),
    'raw_limits_json': json.dumps({
        'cpu_limit': 2.0,
        'memory_limit': 536_870_912,
        'throttle_ratio': 0.05,
        'memory_failcnt': 0,
        'cpu_usage_cores': 1.5,    # important for idle-detection clamp
    }),
    'timestamp': datetime.now(timezone.utc).isoformat(),
})
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Agent won't start — "lock held by another instance" | `redis-cli -p 6380 DEL lock:agent3:optimization lock:agent4:governance lock:agent5:executor` |
| Pipeline stuck in SCALE_UP during idle workload | Forecast sanity clamp should activate. If not, verify `cpu_usage_cores` is present in the ingestion stream. |
| Scale-down rejected with `ANTI_FLAPPING_COOLDOWN` | `redis-cli -p 6380 DEL cooldown:test-workload:stress-test-app` or wait for 300s TTL |
| Agent 4 LLM rejecting valid scale-ups | Check logs for `HIGH_CPU_RISK` on scale_up — should now be suppressed. Restart agents to pick up latest code. |
| LLM inference taking >15s | Switch from `local` to `gemini` provider: `--llm-provider gemini` (requires `GEMINI_API_KEY`) |

---

## Docs Index

- `docs/CLUSTER_SETUP.md` — Prometheus + kube-prometheus-stack setup
- `docs/IMPLEMENTATION_SUMMARY.md` — detailed agent architecture
- `docs/PROJECT_REPORT.md` — project report
- `configs/prediction.yaml` — risk thresholds and forecast horizons
