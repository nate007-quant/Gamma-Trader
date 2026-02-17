$ErrorActionPreference = "Stop"

$ROOT = Split-Path -Parent $PSScriptRoot
Set-Location $ROOT

py -m venv .venv
. .\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -e .\python
pip install -e .\api

if (-not (Test-Path .\configs\config.yaml)) {
  Copy-Item .\configs\config.example.yaml .\configs\config.yaml
  Write-Host "Created configs/config.yaml"
} else {
  Write-Host "configs/config.yaml already exists"
}

Write-Host "Setup complete."
