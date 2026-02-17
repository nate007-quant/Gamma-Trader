from __future__ import annotations

import pandas as pd


def add_direction_label(df: pd.DataFrame, *, horizon_bars: int = 1, price_col: str = "spot") -> pd.DataFrame:
    """Adds y_dir in {0,1} for next horizon_bars forward return.

    Expects df sorted by time within a day.
    """
    out = df.copy()
    fwd = out[price_col].shift(-horizon_bars)
    ret = (fwd / out[price_col]) - 1.0
    out["y_ret"] = ret
    out["y_dir"] = (ret > 0).astype("int")
    return out
