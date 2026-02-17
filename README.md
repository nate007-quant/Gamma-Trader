# Gamma Trader

A cross-platform (Windows + Linux) pipeline that ingests SPX options snapshot JSON files, computes gamma-related levels/features, trains a model, and generates a morning trading plan.

## Inputs
- Snapshot folder: `/mnt/SPX` (configurable)
- Snapshot file format (from the existing PowerShell script):
  `TICKER-SPOT-YYYY-MM-DD-OBS_DATE-OBS_TIME.json`

## MVP workflow
1. Build dataset from snapshots → `data/dataset.parquet`
2. Train a baseline model (logistic regression) to predict **next 15m direction**
3. Generate a daily plan markdown → `data/plan.md`
4. Watch the snapshot folder for new files (intraday updates)

## Setup + run (Linux)
```bash
./scripts/setup_linux.sh

# build initial model + dashboard artifacts (run once, or whenever retraining)
source .venv/bin/activate

gt-build-dataset --config configs/config.yaml
gt-train --config configs/config.yaml
gt-export-dashboard --config configs/config.yaml

# run API + watcher
./scripts/run_local.sh
```

## Setup + run (Windows)
```powershell
.\scripts\setup_windows.ps1

# build initial model + dashboard artifacts (run once, or whenever retraining)
. .\.venv\Scripts\Activate.ps1

gt-build-dataset --config configs\config.yaml
gt-train --config configs\config.yaml
gt-export-dashboard --config configs\config.yaml

# run API + watcher
.\scripts\run_local.ps1
```

## Notes
- The current model is a baseline. Next iterations will add:
  - walk-forward retraining
  - more targets (to-close, level-touch)
  - calibration & confidence gating
  - reporting with explicit trade setups and invalidation levels
