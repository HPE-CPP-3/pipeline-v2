# Quickstart: Two Independent Agents

This quickstart runs Stage 1 (ingestion) and Stage 2 (prediction) as independent services.

## 1) Start local state stores

```bash
cd /home/vulcan/Abhay/Projects/HPE/pipeline-v2
./scripts/setup-local-dev.sh
```

This brings up:

- Redis (`localhost:6380`)
- InfluxDB (`localhost:8086`)

## 2) Start or connect a Kubernetes cluster with Prometheus

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

## 5) Run Stage 2 (Prediction) only

In a second terminal:

```bash
pipeline-agentic \
  --agent prediction \
  --namespace dummy \
  --pod dummy \
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
