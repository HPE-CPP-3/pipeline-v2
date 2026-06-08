#!/usr/bin/env bash
# ============================================================
# run_all_agents.sh  —  Start Agents 1 + 2 + 3 + 4
# ============================================================
# Usage (from anywhere):
#   bash scripts/run_all_agents.sh --source prometheus --namespace <ns> --pod <pod> [--container <c>]
#   bash scripts/run_all_agents.sh --source redis
#
# What it starts:
#   - Agents 1+2 (pipeline-agentic)
#   - Agent 3 (decision_agents/agent3) in Redis stream mode
#   - Agent 4 (decision_agents/agent4) in Redis stream mode
#
# Notes:
#   - This script does NOT modify Agent 1/2 code; it only orchestrates processes.
#   - Agent 3/4 have single-instance Redis locks; if you already have them running,
#     they will exit with a lock error.
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

SOURCE="prometheus" # prometheus | redis

REDIS_HOST="${REDIS_HOST:-${PIPELINE_REDIS_HOST:-localhost}}"
REDIS_PORT="${REDIS_PORT:-${PIPELINE_REDIS_PORT:-6380}}"
PROMETHEUS_URL="${PROMETHEUS_URL:-${PIPELINE_PROMETHEUS_URL:-http://localhost:9090}}"
MODEL_PATH="${MODEL_PATH:-${PIPELINE_MODEL_PATH:-data/models}}"

NAMESPACE="${NAMESPACE:-${PIPELINE_NAMESPACE:-}}"
POD="${POD:-${PIPELINE_POD:-}}"
CONTAINER="${CONTAINER:-${PIPELINE_CONTAINER:-}}"

GEMINI_KEY="${GEMINI_API_KEY:-}"

usage() {
  cat <<EOF
Usage:
  bash scripts/run_all_agents.sh --source prometheus --namespace <ns> --pod <pod> [--container <c>]
  bash scripts/run_all_agents.sh --source redis

Options:
  --source        prometheus | redis   (default: prometheus)
  --namespace     Kubernetes namespace (required for --source prometheus)
  --pod           Pod name             (required for --source prometheus)
  --container     Container name       (optional)
  --redis-host    Redis host           (default: $REDIS_HOST)
  --redis-port    Redis port           (default: $REDIS_PORT)
  --prometheus-url Prometheus base URL (default: $PROMETHEUS_URL)
  --model-path    Model directory      (default: $MODEL_PATH)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) SOURCE="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --pod) POD="$2"; shift 2 ;;
    --container) CONTAINER="$2"; shift 2 ;;
    --redis-host) REDIS_HOST="$2"; shift 2 ;;
    --redis-port) REDIS_PORT="$2"; shift 2 ;;
    --prometheus-url) PROMETHEUS_URL="$2"; shift 2 ;;
    --model-path) MODEL_PATH="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
done

cd "$ROOT"

# Ensure local imports work even if the package isn't installed editable.
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"

# Prefer venv executables if present
PYTHON_BIN="${PYTHON_BIN:-${VENV_PYTHON:-}}"
if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x "$ROOT/.venv/bin/python" ]]; then
    PYTHON_BIN="$ROOT/.venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  else
    echo "ERROR: No Python found. Activate .venv or install python3." >&2
    exit 1
  fi
fi

PIPELINE_AGENTIC_BIN="${PIPELINE_AGENTIC_BIN:-$ROOT/.venv/bin/pipeline-agentic}"
if [[ ! -x "$PIPELINE_AGENTIC_BIN" ]]; then
  PIPELINE_AGENTIC_BIN=""
fi

export PIPELINE_REDIS_HOST="$REDIS_HOST"
export PIPELINE_REDIS_PORT="$REDIS_PORT"

PIDS=()

start_bg() {
  local name="$1"; shift
  echo "[$name] $*"
  "$@" &
  local pid=$!
  PIDS+=("$pid")
  echo "[$name] PID: $pid"
}

cleanup() {
  echo ""
  echo "Stopping agents..."
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait || true
}
trap cleanup INT TERM EXIT

echo ""
echo "======================================================"
echo "  Pipeline Runner — Redis at ${REDIS_HOST}:${REDIS_PORT}"
echo "======================================================"
echo "  Source: ${SOURCE}"
if [[ "$SOURCE" == "prometheus" ]]; then
  echo "  Target: ${NAMESPACE}/${POD}${CONTAINER:+ (container=${CONTAINER})}"
  echo "  Prometheus: ${PROMETHEUS_URL}"
else
  echo "  Model path: ${MODEL_PATH}"
fi

echo ""

case "$SOURCE" in
  prometheus)
    if [[ -z "$NAMESPACE" || -z "$POD" ]]; then
      echo "Error: --namespace and --pod are required for --source prometheus"
      echo ""
      usage
      exit 2
    fi

    # Agent 1 + Agent 2 in one process
    agent12_cmd=()
    if [[ -n "${PIPELINE_AGENTIC_BIN}" ]]; then
      agent12_cmd+=("$PIPELINE_AGENTIC_BIN")
    else
      agent12_cmd+=("$PYTHON_BIN" -m src.agents.runtime)
    fi
    agent12_cmd+=(
      --agent both
      --namespace "$NAMESPACE"
      --pod "$POD"
      --prometheus-url "$PROMETHEUS_URL"
      --model-path "$MODEL_PATH"
    )
    if [[ -n "$CONTAINER" ]]; then
      agent12_cmd+=(--container "$CONTAINER")
    fi
    start_bg "Agent1+2" "${agent12_cmd[@]}"
    ;;

  redis)
    # Agent 1: Prometheus-free ingestion bridge (reads stream:metrics:latest by default)
    if [[ -n "${PIPELINE_AGENTIC_BIN}" ]]; then
      start_bg "Agent1" "$PIPELINE_AGENTIC_BIN" \
        --agent redis-ingestion
    else
      start_bg "Agent1" "$PYTHON_BIN" -m src.agents.runtime \
        --agent redis-ingestion
    fi

    # Agent 2: prediction agent
    if [[ -n "${PIPELINE_AGENTIC_BIN}" ]]; then
      start_bg "Agent2" "$PIPELINE_AGENTIC_BIN" \
        --agent prediction \
        --model-path "$MODEL_PATH"
    else
      start_bg "Agent2" "$PYTHON_BIN" -m src.agents.runtime \
        --agent prediction \
        --model-path "$MODEL_PATH"
    fi
    ;;

  *)
    echo "Error: unknown --source '$SOURCE' (expected prometheus|redis)"
    exit 2
    ;;
esac

# Agent 3
start_bg "Agent3" "$PYTHON_BIN" "$ROOT/decision_agents/agent3/agent3_optimization.py" \
  --mode redis \
  --redis-host "$REDIS_HOST" \
  --redis-port "$REDIS_PORT" \
  --log-file "$ROOT/decision_agents/agent3/agent3_log.txt"

# Agent 4
if [[ -n "$GEMINI_KEY" ]]; then
  start_bg "Agent4" "$PYTHON_BIN" "$ROOT/decision_agents/agent4/agent4_governance.py" \
    --mode redis \
    --redis-host "$REDIS_HOST" \
    --redis-port "$REDIS_PORT" \
    --log-file "$ROOT/decision_agents/agent4/agent4_log.txt" \
    --gemini-key "$GEMINI_KEY"
else
  start_bg "Agent4" "$PYTHON_BIN" "$ROOT/decision_agents/agent4/agent4_governance.py" \
    --mode redis \
    --redis-host "$REDIS_HOST" \
    --redis-port "$REDIS_PORT" \
    --log-file "$ROOT/decision_agents/agent4/agent4_log.txt"
fi

echo ""
echo "All agents launched. Press Ctrl+C to stop."
echo ""
echo "Stream flow:"
echo "  stream:ingestion:complete    <-- Agent 1 writes here"
echo "  stream:prediction:complete   <-- Agent 2 writes here"
echo "  stream:optimization:complete <-- Agent 3 writes here"
echo "  stream:governance:complete   <-- Agent 4 writes here"
echo ""

wait
