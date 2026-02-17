from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import pandas as pd
import yaml


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--data", default="data/dataset.parquet")
    ap.add_argument("--model", default="data/model.joblib")
    ap.add_argument("--out-plan", default="data/latest_plan.json")
    ap.add_argument("--out-series", default="data/timeseries.parquet")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    symbol = cfg.get("symbol", "SPX")

    df = pd.read_parquet(args.data).sort_values(["date", "ts"]).reset_index(drop=True)
    day = df["date"].max()
    g = df[df["date"] == day].copy()

    pack = joblib.load(args.model)
    model = pack["model"]
    feats = pack["features"]

    p = model.predict_proba(g[feats])[:, 1]
    g["p_up"] = p

    last = g.sort_values("ts").iloc[-1]
    p_last = float(last["p_up"])
    bias = "UP" if p_last >= 0.55 else "DOWN" if p_last <= 0.45 else "NEUTRAL"

    plan = {
        "symbol": symbol,
        "date": day,
        "target": f"next {cfg.get('label',{}).get('horizon_minutes', cfg.get('interval_minutes',15))}m direction",
        "latest": {
            "ts": str(last["ts"]),
            "spot": float(last["spot"]),
            "call_wall": None if pd.isna(last.get("call_wall")) else float(last["call_wall"]),
            "put_wall": None if pd.isna(last.get("put_wall")) else float(last["put_wall"]),
            "magnet": None if pd.isna(last.get("magnet")) else float(last["magnet"]),
            "flip": None if pd.isna(last.get("flip")) else float(last["flip"]),
            "pressure": None if pd.isna(last.get("pressure")) else float(last["pressure"]),
            "p_up": p_last,
            "bias": bias,
        },
    }

    Path(args.out_plan).write_text(json.dumps(plan, indent=2), encoding="utf-8")

    Path(args.out_series).parent.mkdir(parents=True, exist_ok=True)
    g.to_parquet(args.out_series, index=False)

    print(f"wrote {args.out_plan} and {args.out_series}")


if __name__ == "__main__":
    main()
