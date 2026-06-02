#!/usr/bin/env bash
# ============================================================
# run_pipeline.sh  —  Start Agent 3 + Agent 4 (Redis mode)
# ============================================================
# Usage (from repo root):
#   bash scripts/run_pipeline.sh            # start Agent 3 AND Agent 4
#   bash scripts/run_pipeline.sh --agent 3  # only Agent 3
#   bash scripts/run_pipeline.sh --agent 4  # only Agent 4
#
# Prerequisites:
#   1. Redis running:  docker-compose -f scripts/docker-compose.dev.yaml up -d redis
#   2. Venv active:    source .venv/bin/activate
#   3. Deps installed: pip install redis groq
# ============================================================

set -euo pipefail

REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6380}"
GROQ_KEY="${GROQ_API_KEY:-}"
AGENT="${1:-all}"           # all | 3 | 4

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

echo ""
echo "======================================================"
echo "  Pipeline Runner — Redis at ${REDIS_HOST}:${REDIS_PORT}"
echo "======================================================"
echo ""

start_agent3() {
    echo "[Agent 3] Starting Resource Optimization (redis mode)..."
    python "$ROOT/decision_agents/agent3/agent3_optimization.py" \
        --mode redis \
        --redis-host "$REDIS_HOST" \
        --redis-port "$REDIS_PORT" &
    AGENT3_PID=$!
    echo "[Agent 3] PID: $AGENT3_PID"
}

start_agent4() {
    echo "[Agent 4] Starting Governance (redis mode)..."
    GROQ_ARG=""
    if [ -n "$GROQ_KEY" ]; then
        GROQ_ARG="--groq-key $GROQ_KEY"
    fi
    python "$ROOT/decision_agents/agent4/agent4_governance.py" \
        --mode redis \
        --redis-host "$REDIS_HOST" \
        --redis-port "$REDIS_PORT" \
        $GROQ_ARG &
    AGENT4_PID=$!
    echo "[Agent 4] PID: $AGENT4_PID"
}

case "$AGENT" in
    3)
        start_agent3
        wait $AGENT3_PID
        ;;
    4)
        start_agent4
        wait $AGENT4_PID
        ;;
    *)
        start_agent3
        sleep 1
        start_agent4
        echo ""
        echo "Both agents running. Press Ctrl+C to stop."
        echo ""
        echo "Stream flow:"
        echo "  stream:prediction:complete   <-- Agent 2 writes here"
        echo "  stream:optimization:complete <-- Agent 3 writes here"
        echo "  stream:governance:complete   <-- Agent 4 writes here"
        echo ""
        echo "To inject a test message (verify Agent 3+4 respond):"
        cat <<'EOF'
  redis-cli -p 6380 XADD stream:prediction:complete '*' \
    namespace test-ns pod test-pod container test-c \
    forecast_json '{"cpu_forecast":{"5":{"0.5":0.6,"0.9":0.82},"15":{"0.5":0.65,"0.9":0.91}},"memory_forecast":{"5":{"0.5":0.55,"0.9":0.76},"15":{"0.5":0.60,"0.9":0.88}},"throttle_prob":0.87,"oom_prob":0.76,"confidence":0.73,"throttle_risk":{"risk_level":"HIGH"},"oom_risk":{"oom_risk":"HIGH"}}'
EOF
        # Wait for both
        wait
        ;;
esac
