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

## Quickstart (Linux)
```bash
cd python
python -m venv .venv && source .venv/bin/activate
pip install -e .

cp ../configs/config.example.yaml ../configs/config.yaml
# edit configs/config.yaml if needed

gt-build-dataset --config ../configs/config.yaml
gt-train --config ../configs/config.yaml
gt-make-plan --config ../configs/config.yaml

gt-watch --config ../configs/config.yaml
```

## Quickstart (Windows)
Same commands, using PowerShell:
```powershell
cd python
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .

copy ..\configs\config.example.yaml ..\configs\config.yaml
# edit config

gt-build-dataset --config ..\configs\config.yaml
gt-train --config ..\configs\config.yaml
gt-make-plan --config ..\configs\config.yaml

gt-watch --config ..\configs\config.yaml
```

## Notes
- The current model is a baseline. Next iterations will add:
  - walk-forward retraining
  - more targets (to-close, level-touch)
  - calibration & confidence gating
  - reporting with explicit trade setups and invalidation levels
