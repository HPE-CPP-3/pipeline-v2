# Quickstart: Two Independent Agents

This quickstart runs Stage 1 (ingestion) and Stage 2 (prediction) as independent services.

## 1) Start local state stores

```bash
cd pipeline-v3
./scripts/setup-local-dev.sh
```

This brings up:

- Redis (`localhost:6380`)
- InfluxDB (`localhost:8086`)

## 2) Start or connect a Kubernetes cluster with Prometheus

If you don't have Prometheus handy, you can skip this and run the Prometheus-free
Redis ingestion bridge (see the section "Alternative: Prometheus-free Stage 1").

If you want a local test cluster:

```bash
./scripts/setup-test-cluster.sh
```

Prometheus will be available at `http://localhost:30000`.

## 3) Pick a target pod

```bash
kubectl get pods -n test-workload
```

Use one `stress-test-app-*` pod as your plug-and-play target.

## 4) Run Stage 1 (Ingestion) only

```bash
pipeline-agentic \
  --agent ingestion \
  --namespace test-workload \
  --pod <stress-test-app-pod> \
  --container stress-container \
  --prometheus-url http://localhost:30000
```

Behavior:

- Runs every 60 seconds
- Collects + normalizes + writes to Redis/InfluxDB/CSV
- Emits `stream:ingestion:complete`

## Alternative: Prometheus-free Stage 1 (read from Redis)

Stage 1 can also be driven directly from Redis (no Prometheus).

1) Start the bridge (reads `stream:metrics:latest`, writes `stream:ingestion:complete`):

```bash
pipeline-agentic \
  --agent redis-ingestion \
  --redis-input-mode stream \
  --redis-input-stream stream:metrics:latest \
  --redis-output-stream stream:ingestion:complete \
  --redis-start-id '$' \
  --cpu-limit 1.0 \
  --memory-limit 536870912
```

2) Publish a sample upstream event into `stream:metrics:latest`.
The bridge will pass through `features_json` and `raw_limits_json` if they are present:

```bash
python - <<'PY'
import json
from datetime import datetime, timezone
import redis

r = redis.Redis(host='localhost', port=6380, decode_responses=True)
msg = {
  'namespace': 'test-workload',
  'pod': 'stress-test-app',
  'container': 'stress-test-app',
  'features_json': json.dumps({
    'container_cpu_usage_seconds_total': 1.0,
    'container_memory_working_set_bytes': 120_000_000,
  }),
  'raw_limits_json': json.dumps({
    'cpu_limit': 1.0,
    'memory_limit': 536_870_912,
    'throttle_ratio': 0.02,
    'memory_failcnt': 0,
  }),
  'timestamp': datetime.now(timezone.utc).isoformat(),
}
print('xadd', r.xadd('stream:metrics:latest', msg))
PY
```

## 5) Run Stage 2 (Prediction) only

In a second terminal:

```bash
pipeline-agentic \
  --agent prediction \
  --model-path data/models
```

Behavior:

- Sleeps until it receives `stream:ingestion:complete`
- Reads latest features and 24h InfluxDB context
- Runs PatchTST CPU+memory inference
- Writes `stream:prediction:complete` and live CSV prediction rows

## 6) Verify outputs

Redis stream check:

```bash
docker exec -it pipeline-redis redis-cli XRANGE stream:prediction:complete - + COUNT 5
```

InfluxDB check (Data Explorer):

- Measurement: `metrics`
- Measurement: `predictions`

CSV live check:

```bash
ls -lh data/csv/metrics
ls -lh data/csv/predictions
```

## Optional: run both in one process

```bash
pipeline-agentic \
  --agent both \
  --namespace test-workload \
  --pod <stress-test-app-pod> \
  --container stress-container \
  --prometheus-url http://localhost:30000
```
