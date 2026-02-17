$ErrorActionPreference = "Stop"

$ROOT = Split-Path -Parent $PSScriptRoot
Set-Location $ROOT

if (-not (Test-Path .\.venv)) {
  throw "Missing .venv. Run scripts/setup_windows.ps1 first."
}

. .\.venv\Scripts\Activate.ps1

$HostAddr = if ($env:GT_HOST) { $env:GT_HOST } else { "0.0.0.0" }
$Port = if ($env:GT_PORT) { $env:GT_PORT } else { "8000" }
$Config = if ($env:GT_CONFIG) { $env:GT_CONFIG } else { "configs/config.yaml" }

Write-Host "============================================================"
Write-Host "Gamma Trader local runner"
Write-Host "API:       http://$HostAddr`:$Port"
Write-Host "Dashboard: open web/index.html"
Write-Host "Config:    $Config"
Write-Host "============================================================"

Start-Process -NoNewWindow -FilePath "uvicorn" -ArgumentList "gamma_trader_api.app:app --host $HostAddr --port $Port"
Start-Process -NoNewWindow -FilePath "gt-watch" -ArgumentList "--config $Config --bootstrap"

Write-Host "Started API + watcher. Close this window to stop them (or stop the processes manually)."
