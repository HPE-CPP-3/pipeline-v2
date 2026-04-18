#!/usr/bin/env bash
# ============================================================
# run_pipeline.sh  —  DEPRECATED (use run_all_agents.sh)
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$SCRIPT_DIR")"

echo "ERROR: scripts/run_pipeline.sh is deprecated." >&2
echo "Use ONE script to start agents: bash scripts/run_all_agents.sh" >&2
echo "  - Kubernetes/Prometheus mode:" >&2
echo "      bash scripts/run_all_agents.sh --source prometheus --namespace <ns> --pod <pod> [--container <c>]" >&2
echo "  - Local Redis mode:" >&2
echo "      bash scripts/run_all_agents.sh --source redis" >&2
exit 2
