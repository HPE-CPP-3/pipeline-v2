# ============================================================
# run_pipeline.ps1  --  DEPRECATED (use run_all_agents.sh)
# ============================================================
# This repo uses ONE supported launcher for starting agents:
#   bash scripts/run_all_agents.sh --source prometheus --namespace <ns> --pod <pod> [--container <c>]
#   bash scripts/run_all_agents.sh --source redis

Write-Error "scripts/run_pipeline.ps1 is deprecated. Use scripts/run_all_agents.sh instead."
exit 2
