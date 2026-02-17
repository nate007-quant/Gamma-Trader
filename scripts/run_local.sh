#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -d ".venv" ]]; then
  echo "Missing .venv. Run: python3 -m venv .venv && source .venv/bin/activate && pip install -e ./python && pip install -e ./api"
  exit 1
fi

source .venv/bin/activate

HOST="${GT_HOST:-0.0.0.0}"
PORT="${GT_PORT:-8000}"
CONFIG="${GT_CONFIG:-configs/config.yaml}"

echo "============================================================"
echo "Gamma Trader local runner"
echo "API:    http://${HOST}:${PORT}"
echo "Dashboard: open web/index.html"
echo "Config: ${CONFIG}"
echo "============================================================"

# Start API
uvicorn gamma_trader_api.app:app --host "$HOST" --port "$PORT" &
API_PID=$!

# Start watcher
# Requires an already-trained model at data/model.joblib.
# If not present, you should run:
#   gt-build-dataset --config $CONFIG && gt-train --config $CONFIG && gt-export-dashboard --config $CONFIG

gt-watch --config "$CONFIG" --bootstrap &
WATCH_PID=$!

cleanup() {
  echo "\nStopping..."
  kill "$WATCH_PID" 2>/dev/null || true
  kill "$API_PID" 2>/dev/null || true
}
trap cleanup EXIT

wait "$API_PID" "$WATCH_PID"
