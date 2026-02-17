from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import yaml
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import joblib


FEATURES = [
    "spot",
    "call_wall",
    "put_wall",
    "magnet",
    "flip",
    "pressure",
    "call_wall_abs_gex",
    "put_wall_abs_gex",
    "magnet_abs_gex",
    "vega_net",
    "vega_abs",
    "atm_iv_mid",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--data", default="data/dataset.parquet")
    ap.add_argument("--model-out", default="data/model.joblib")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())

    df = pd.read_parquet(args.data)
    df = df.dropna(subset=["y_dir"]).copy()

    # time split by last N days
    days = sorted(df["date"].unique())
    test_days = int(cfg.get("training", {}).get("test_days", 10))
    cut = max(1, len(days) - test_days)
    train_set = df[df["date"].isin(days[:cut])]
    test_set = df[df["date"].isin(days[cut:])]

    X_train = train_set[FEATURES]
    y_train = train_set["y_dir"].astype(int)
    X_test = test_set[FEATURES]
    y_test = test_set["y_dir"].astype(int)

    pre = ColumnTransformer(
        [("num", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), FEATURES)],
        remainder="drop",
    )

    clf = LogisticRegression(max_iter=2000)
    pipe = Pipeline([("pre", pre), ("clf", clf)])
    pipe.fit(X_train, y_train)

    p = pipe.predict_proba(X_test)[:, 1]
    y_hat = (p >= 0.5).astype(int)

    acc = float(accuracy_score(y_test, y_hat)) if len(y_test) else float("nan")
    auc = float(roc_auc_score(y_test, p)) if len(set(y_test)) > 1 else float("nan")

    Path(args.model_out).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": pipe, "features": FEATURES}, args.model_out)

    print(f"train days: {len(days[:cut])} | test days: {len(days[cut:])}")
    print(f"test rows: {len(test_set):,} | acc={acc:.4f} auc={auc:.4f}")
    print(f"saved -> {args.model_out}")


if __name__ == "__main__":
    main()
