from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"

app = FastAPI(title="Gamma Trader API", version="0.1.0")


def _read_json(path: Path) -> Any:
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"missing: {path.name}")
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/plan/latest")
def plan_latest():
    return _read_json(DATA_DIR / "latest_plan.json")


@app.get("/series/today")
def series_today(limit: int = 400):
    p = DATA_DIR / "timeseries.parquet"
    if not p.exists():
        raise HTTPException(status_code=404, detail="missing: timeseries.parquet")

    df = pd.read_parquet(p)
    if df.empty:
        return []

    day = df["date"].max()
    g = df[df["date"] == day].sort_values("ts")

    if limit and len(g) > limit:
        g = g.iloc[-limit:]

    cols = [c for c in ["ts", "spot", "call_wall", "put_wall", "magnet", "flip", "p_up"] if c in g.columns]
    out = g[cols].copy()

    # JSON safe
    out["ts"] = out["ts"].astype(str)
    return out.to_dict(orient="records")
