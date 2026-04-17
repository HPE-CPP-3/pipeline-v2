# ============================================================
# run_pipeline.ps1  --  Start Agent 3 + Agent 4 (Redis mode)
# ============================================================
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\run_pipeline.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\run_pipeline.ps1 -Agent 3
#   powershell -ExecutionPolicy Bypass -File scripts\run_pipeline.ps1 -Agent 4
# ============================================================

param (
    [string]$Agent = "all",
    [int]$RedisPort = 6380,
    [string]$RedisHost = "localhost",
    [string]$GroqKey = $env:GROQ_API_KEY
)

$Root = Split-Path -Parent $PSScriptRoot

Write-Host ""
Write-Host "======================================================" -ForegroundColor Yellow
Write-Host "  Pipeline Runner -- Redis at ${RedisHost}:${RedisPort}" -ForegroundColor Yellow
Write-Host "======================================================" -ForegroundColor Yellow
Write-Host ""

function Start-Agent3 {
    Write-Host "[Agent 3] Starting Resource Optimization (redis mode)..." -ForegroundColor Cyan
    $cmd = "cd '$Root'; python decision_agents\agent3\agent3_optimization.py --mode redis --redis-host $RedisHost --redis-port $RedisPort; Read-Host 'Press Enter to close'"
    Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $cmd
}

function Start-Agent4 {
    Write-Host "[Agent 4] Starting Governance (redis mode)..." -ForegroundColor Magenta
    $groqArg = ""
    if ($GroqKey) { $groqArg = "--groq-key $GroqKey" }
    $cmd = "cd '$Root'; python decision_agents\agent4\agent4_governance.py --mode redis --redis-host $RedisHost --redis-port $RedisPort $groqArg; Read-Host 'Press Enter to close'"
    Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $cmd
}

switch ($Agent) {
    "3"   { Start-Agent3 }
    "4"   { Start-Agent4 }
    default {
        Start-Agent3
        Start-Sleep -Seconds 1
        Start-Agent4
    }
}

Write-Host ""
Write-Host "Agents launched in separate windows." -ForegroundColor Green
Write-Host ""
Write-Host "Stream flow:" -ForegroundColor White
Write-Host "  stream:prediction:complete    written by Agent 2" -ForegroundColor Gray
Write-Host "  stream:optimization:complete  written by Agent 3" -ForegroundColor Gray
Write-Host "  stream:governance:complete    written by Agent 4" -ForegroundColor Gray
Write-Host ""
