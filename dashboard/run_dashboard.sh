#!/usr/bin/env bash
# ============================================================
# run_dashboard.sh  —  Start K8s Sentinel Dashboard Server
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$ROOT_DIR"

# Ensure local imports work
export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"

# Check for venv python or uvicorn
UVICORN_BIN="$ROOT_DIR/.venv/bin/uvicorn"
if [[ ! -x "$UVICORN_BIN" ]]; then
  echo "Error: uvicorn not found in .venv/bin. Please run scripts/setup-local-dev.sh first." >&2
  exit 1
fi

echo "======================================================"
  echo "  K8s Sentinel Dashboard Server"
  echo "  URL: http://localhost:8000"
  echo "======================================================"

exec "$UVICORN_BIN" dashboard.main:app --host 0.0.0.0 --port 8000 --reload
