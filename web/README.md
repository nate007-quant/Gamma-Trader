# Gamma Trader Dashboard (local)

## Start API
```bash
cd api
python -m venv .venv && source .venv/bin/activate
pip install -e .
uvicorn gamma_trader_api.app:app --host 0.0.0.0 --port 8000
```

## Start dashboard
Just open `web/index.html` in your browser.

The dashboard calls:
- `http://localhost:8000/plan/latest`
- `http://localhost:8000/series/today`

## Produce data for the dashboard
Run from repo root after you have snapshots + model:
```bash
cd python
python -m venv .venv && source .venv/bin/activate
pip install -e .

gt-build-dataset --config ../configs/config.yaml

gt-train --config ../configs/config.yaml

gt-export-dashboard --config ../configs/config.yaml
```
