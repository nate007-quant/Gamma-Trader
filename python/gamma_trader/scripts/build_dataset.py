from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import yaml

from gamma_trader.ingest.snapshot import iter_snapshot_files, load_snapshot_json
from gamma_trader.features.levels import compute_levels_from_columnar_json
from gamma_trader.labels.targets import add_direction_label


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--out", default="data/dataset.parquet")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    snap_dir = Path(cfg["snapshot_dir"])
    glob = cfg.get("snapshot_glob", "*.json")

    rows = []
    for path, meta in iter_snapshot_files(snap_dir, glob=glob):
        js = load_snapshot_json(path)
        lvl = compute_levels_from_columnar_json(
            js,
            band_pct=float(cfg.get("band_pct", 0.05)),
            contract_multiplier=int(cfg.get("contract_multiplier", 100)),
        )
        rows.append(
            {
                "ts": meta.observed_dt,
                "date": meta.observed_dt.date().isoformat(),
                "expiration": meta.expiration.isoformat(),
                "spot": lvl.spot,
                "call_wall": lvl.call_wall,
                "put_wall": lvl.put_wall,
                "magnet": lvl.magnet,
                "flip": lvl.flip,
                "pressure": lvl.pressure,
                "call_wall_abs_gex": lvl.call_wall_abs_gex,
                "put_wall_abs_gex": lvl.put_wall_abs_gex,
                "magnet_abs_gex": lvl.magnet_abs_gex,
                "vega_net": lvl.vega_net,
                "vega_abs": lvl.vega_abs,
                "atm_iv_mid": lvl.atm_iv_mid,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("No snapshots found")

    df = df.sort_values(["date", "ts"]).reset_index(drop=True)

    # label within each day
    horizon_min = int(cfg.get("label", {}).get("horizon_minutes", cfg.get("interval_minutes", 15)))
    interval = int(cfg.get("interval_minutes", 15))
    horizon_bars = max(1, horizon_min // interval)

    parts = []
    for d, g in df.groupby("date", sort=False):
        parts.append(add_direction_label(g, horizon_bars=horizon_bars, price_col="spot"))
    out = pd.concat(parts, ignore_index=True)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    print(f"wrote {len(out):,} rows -> {out_path}")


if __name__ == "__main__":
    main()
