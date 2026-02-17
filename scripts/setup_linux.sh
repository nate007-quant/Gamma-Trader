#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -e ./python
pip install -e ./api

if [[ ! -f configs/config.yaml ]]; then
  cp configs/config.example.yaml configs/config.yaml
  echo "Created configs/config.yaml"
else
  echo "configs/config.yaml already exists"
fi

echo "Setup complete."
