# Implementation Summary

## Refactor Outcome

The pipeline has been simplified to two independent agents connected only by shared state.

- Stage 1 Ingestion: `src/agents/ingestion_agent.py`
- Stage 2 Prediction: `src/agents/prediction_agent.py`
- Minimal orchestrator: `src/agents/pipeline.py`
- Runtime CLI: `src/agents/runtime.py`

## Stage 1: Plug-and-Play Ingestion

Implemented with `TargetConfig` input:

- `namespace`
- `pod_name`
- `container_name` (optional)

Behavior:

- Dynamically maps `target_config` to PromQL labels
- Runs on a strict 60-second ticker
- Collects feature matrix across container, node, and k8s control plane
- Computes derived efficiency/pressure/volatility signals
- Normalizes numeric features to `[0,1]` via rolling min-max
- Writes latest normalized state to Redis
- Writes time-series to InfluxDB
- Emits durable trigger to `stream:ingestion:complete`

## Stage 2: Independent Workload Prediction

Implemented as stream-triggered consumer:

- Wakes up only on `stream:ingestion:complete`
- Reads latest normalized vector from Redis payload
- Pulls last 24h seasonal context from InfluxDB
- Runs PatchTST CPU + memory inference
- Computes confidence from deviation vs historical distribution
- Writes forecast to:
  - Redis stream `stream:prediction:complete`
  - InfluxDB `predictions` measurement

## Shared State Communication

No direct agent-to-agent calls are used.

- Redis: current state + durable stream signals
- InfluxDB: historical context and prediction audit

## Operational Modes

`pipeline-agentic` supports:

- `--agent both` (default): run ingestion + prediction together
- `--agent ingestion`: run only Stage 1
- `--agent prediction`: run only Stage 2
