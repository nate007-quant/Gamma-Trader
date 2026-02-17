from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import joblib
import pandas as pd
import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from gamma_trader.features.levels import compute_levels_from_columnar_json
from gamma_trader.ingest.snapshot import load_snapshot_json, parse_snapshot_filename


def _safe_float(x):
    try:
        if x is None:
            return None
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def _write_plan_and_series(cfg: dict, *, df_today: pd.DataFrame, model_pack: dict, out_plan: Path, out_series: Path):
    symbol = cfg.get("symbol", "SPX")
    feats = model_pack["features"]
    model = model_pack["model"]

    g = df_today.sort_values("ts").copy()
    g["p_up"] = model.predict_proba(g[feats])[:, 1]

    last = g.iloc[-1]
    p_last = float(last["p_up"])
    bias = "UP" if p_last >= 0.55 else "DOWN" if p_last <= 0.45 else "NEUTRAL"

    plan = {
        "symbol": symbol,
        "date": str(last["date"]),
        "target": f"next {cfg.get('label',{}).get('horizon_minutes', cfg.get('interval_minutes',15))}m direction",
        "latest": {
            "ts": str(last["ts"]),
            "spot": float(last["spot"]),
            "call_wall": _safe_float(last.get("call_wall")),
            "put_wall": _safe_float(last.get("put_wall")),
            "magnet": _safe_float(last.get("magnet")),
            "flip": _safe_float(last.get("flip")),
            "pressure": _safe_float(last.get("pressure")),
            "p_up": p_last,
            "bias": bias,
        },
    }

    out_plan.parent.mkdir(parents=True, exist_ok=True)
    out_plan.write_text(json.dumps(plan, indent=2), encoding="utf-8")

    out_series.parent.mkdir(parents=True, exist_ok=True)
    g.to_parquet(out_series, index=False)


class Handler(FileSystemEventHandler):
    def __init__(self, on_new_file):
        super().__init__()
        self.on_new_file = on_new_file

    def on_created(self, event):
        if event.is_directory:
            return
        self.on_new_file(Path(event.src_path))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--model", default="data/model.joblib")
    ap.add_argument("--out-plan", default="data/latest_plan.json")
    ap.add_argument("--out-series", default="data/timeseries.parquet")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    from gamma_trader.ingest.config import resolve_snapshot_dir

    snap_dir = resolve_snapshot_dir(cfg).expanduser()

    model_pack = joblib.load(args.model)

    out_plan = Path(args.out_plan)
    out_series = Path(args.out_series)

    # in-memory state for today's rows
    today_rows: list[dict] = []
    today_key: str | None = None

    def on_new(p: Path):
        nonlocal today_key, today_rows

        if p.suffix.lower() != ".json":
            return

        meta = parse_snapshot_filename(p.name)
        if meta is None:
            return

        day = meta.observed_dt.date().isoformat()
        if today_key is None:
            today_key = day
        elif day != today_key:
            # new day -> reset
            today_key = day
            today_rows = []

        # file may still be writing; retry briefly
        js = None
        for _ in range(5):
            try:
                js = load_snapshot_json(p)
                break
            except Exception:
                time.sleep(0.2)
        if js is None:
            print(f"skip (unreadable): {p.name}")
            return

        lvl = compute_levels_from_columnar_json(
            js,
            band_pct=float(cfg.get("band_pct", 0.05)),
            contract_multiplier=int(cfg.get("contract_multiplier", 100)),
        )

        row = {
            "ts": meta.observed_dt,
            "date": day,
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

        today_rows.append(row)
        df_today = pd.DataFrame(today_rows).drop_duplicates(subset=["ts"]).sort_values("ts")

        _write_plan_and_series(cfg, df_today=df_today, model_pack=model_pack, out_plan=out_plan, out_series=out_series)
        print(f"updated plan/series from: {p.name}")

    obs = Observer()
    obs.schedule(Handler(on_new), str(snap_dir), recursive=False)
    obs.start()
    print(f"watching {snap_dir} ...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        obs.stop()
        obs.join()


if __name__ == "__main__":
    main()
