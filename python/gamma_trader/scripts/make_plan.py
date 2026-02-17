from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import pandas as pd
import yaml


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--data", default="data/dataset.parquet")
    ap.add_argument("--model", default="data/model.joblib")
    ap.add_argument("--date", default="")
    ap.add_argument("--out", default="data/plan.md")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    pack = joblib.load(args.model)
    model = pack["model"]
    feats = pack["features"]

    df = pd.read_parquet(args.data)
    if args.date:
        day = args.date
    else:
        day = max(df["date"].unique())

    g = df[df["date"] == day].sort_values("ts")
    if g.empty:
        raise SystemExit(f"no rows for date={day}")

    last = g.iloc[-1]
    X = g[feats]
    p = model.predict_proba(X)[:, 1]

    p_last = float(p[-1])
    bias = "UP" if p_last >= 0.55 else "DOWN" if p_last <= 0.45 else "NEUTRAL"

    lines = []
    lines.append(f"# Gamma Trader Plan — {cfg.get('symbol','SPX')} — {day}")
    lines.append("")
    lines.append("## Latest gamma state (last snapshot)")
    lines.append(f"- Spot: {last['spot']:.2f}")
    lines.append(f"- Call wall: {last['call_wall']}")
    lines.append(f"- Put wall: {last['put_wall']}")
    lines.append(f"- Magnet: {last['magnet']}")
    lines.append(f"- Flip: {last['flip']}")
    lines.append(f"- Pressure: {last['pressure']}")
    lines.append("")
    lines.append("## Model")
    lines.append(f"- Target: next {cfg.get('label',{}).get('horizon_minutes', cfg.get('interval_minutes',15))}m direction")
    lines.append(f"- P(up) last snapshot: {p_last:.3f}")
    lines.append(f"- Bias: **{bias}**")
    lines.append("")
    lines.append("## Playbook (simple)")
    lines.append("- If price is between put wall and call wall: expect mean reversion / pinning more than trend unless P(up) is extreme.")
    lines.append("- Near magnet: watch for stalling; fading extensions often has better R/R.")
    lines.append("- If price breaks beyond wall with rising abs GEX: trend days become more likely.")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote -> {out}")


if __name__ == "__main__":
    main()
