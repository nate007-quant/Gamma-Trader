from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Any

import numpy as np


@dataclass
class LevelFeatures:
    spot: float
    call_wall: float | None
    put_wall: float | None
    magnet: float | None
    flip: float | None
    pressure: float | None

    call_wall_abs_gex: float
    put_wall_abs_gex: float
    magnet_abs_gex: float

    vega_net: float
    vega_abs: float

    atm_iv_mid: float | None
    iv_upper: float | None
    iv_lower: float | None
    iv_move: float | None


def _to_arr(d: dict[str, Any], key: str):
    v = d.get(key)
    return None if v is None else np.asarray(v)


def _normalize_side(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip().lower()
    if s in {"c", "call", "calls"}:
        return "call"
    if s in {"p", "put", "puts"}:
        return "put"
    return s


def compute_levels_from_columnar_json(
    js: dict[str, Any],
    *,
    band_pct: float = 0.05,
    contract_multiplier: int = 100,
) -> LevelFeatures:
    # Required
    sym = _to_arr(js, "optionSymbol")
    if sym is None:
        raise ValueError("JSON missing optionSymbol")

    strike = _to_arr(js, "strike")
    side_raw = _to_arr(js, "side")
    oi = _to_arr(js, "openInterest")
    vol = _to_arr(js, "volume")
    u = _to_arr(js, "underlyingPrice")
    gamma = _to_arr(js, "gamma")
    iv = _to_arr(js, "iv")
    vega = _to_arr(js, "vega")

    n = len(sym)
    if strike is None or len(strike) != n:
        raise ValueError("JSON missing/unaligned strike")

    side = np.array([_normalize_side(x) for x in (side_raw if side_raw is not None else [None] * n)])

    spot = float(u[0]) if u is not None and len(u) else float("nan")
    if not np.isfinite(spot) or spot <= 0:
        # fallback: approximate from median strike
        spot = float(np.nanmedian(strike))

    lo = spot * (1.0 - band_pct)
    hi = spot * (1.0 + band_pct)

    in_band = (strike >= lo) & (strike <= hi)
    strike_b = strike[in_band]
    side_b = side[in_band]

    oi_b = (oi[in_band] if oi is not None else np.zeros_like(strike_b, dtype=float)).astype(float)
    vol_b = (vol[in_band] if vol is not None else np.zeros_like(strike_b, dtype=float)).astype(float)
    gamma_b = (gamma[in_band] if gamma is not None else np.zeros_like(strike_b, dtype=float)).astype(float)
    vega_b = (vega[in_band] if vega is not None else np.zeros_like(strike_b, dtype=float)).astype(float)
    iv_b = (iv[in_band] if iv is not None else np.full_like(strike_b, np.nan, dtype=float)).astype(float)

    call_mask = side_b == "call"
    put_mask = side_b == "put"

    call_vol = float(np.nansum(vol_b[call_mask]))
    put_vol = float(np.nansum(vol_b[put_mask]))
    den = call_vol + put_vol
    pressure = float((call_vol - put_vol) / den) if den > 0 else None

    # Vega net/abs (sign puts negative like PS1)
    signed = np.where(put_mask, -1.0, 1.0)
    vega_contrib = vega_b * oi_b * float(contract_multiplier)
    vega_net = float(np.nansum(vega_contrib * signed))
    vega_abs = float(np.nansum(np.abs(vega_contrib)))

    # By-strike NetGEX
    spot2 = spot * spot
    sign_g = np.where(put_mask, -1.0, 1.0)
    gex = gamma_b * oi_b * float(contract_multiplier) * spot2 * sign_g

    # group by strike
    uniq, inv = np.unique(strike_b, return_inverse=True)
    net_by = np.zeros(len(uniq), dtype=float)
    abs_by = np.zeros(len(uniq), dtype=float)
    for i, gi in enumerate(gex):
        j = inv[i]
        net_by[j] += float(gi)
    abs_by = np.abs(net_by)

    call_wall = None
    put_wall = None
    magnet = None
    flip = None

    call_wall_abs = 0.0
    put_wall_abs = 0.0
    magnet_abs = 0.0

    if len(uniq):
        j_pos = int(np.argmax(net_by))
        j_neg = int(np.argmin(net_by))
        call_wall = float(uniq[j_pos])
        put_wall = float(uniq[j_neg])
        call_wall_abs = float(abs_by[j_pos])
        put_wall_abs = float(abs_by[j_neg])

        mag_lo = spot * 0.99
        mag_hi = spot * 1.01
        in_mag = (uniq >= mag_lo) & (uniq <= mag_hi)
        if np.any(in_mag):
            jj = np.where(in_mag)[0]
            j_mag = int(jj[np.argmax(abs_by[jj])])
            magnet = float(uniq[j_mag])
            magnet_abs = float(abs_by[j_mag])

        # flip = first sign change of cumulative net_by across sorted strikes
        cum = 0.0
        prev_cum = None
        prev_strike = None
        for s, ng in zip(uniq, net_by):
            cum += float(ng)
            if prev_cum is not None:
                if (prev_cum < 0 <= cum) or (prev_cum > 0 >= cum):
                    flip = float(prev_strike if abs(prev_cum) <= abs(cum) else s)
                    break
            prev_cum = cum
            prev_strike = float(s)

    # ATM IV mid (rough): nearest strike to spot; average call+put IVs if present
    atm_iv_mid = None
    try:
        # use all IVs (not band filtered) if available
        if iv is not None and side_raw is not None:
            iv_all = np.asarray(iv, dtype=float)
            strike_all = np.asarray(strike, dtype=float)
            side_all = np.array([_normalize_side(x) for x in side_raw])
            valid = np.isfinite(iv_all) & (iv_all > 0) & np.isfinite(strike_all)
            strike_v = strike_all[valid]
            iv_v = iv_all[valid]
            side_v = side_all[valid]
            if len(strike_v):
                atm = strike_v[np.argmin(np.abs(strike_v - spot))]
                at_mask = strike_v == atm
                c = np.nanmean(iv_v[at_mask & (side_v == "call")])
                p = np.nanmean(iv_v[at_mask & (side_v == "put")])
                vals = [x for x in [c, p] if np.isfinite(x) and x > 0]
                if vals:
                    # normalize % inputs
                    m = float(np.mean(vals))
                    if m > 5.0:
                        m = m / 100.0
                    atm_iv_mid = m
    except Exception:
        atm_iv_mid = None

    # IV band to expiration: we don't know obs time-to-exp here; caller can fill later.
    iv_upper = None
    iv_lower = None
    iv_move = None

    return LevelFeatures(
        spot=float(spot),
        call_wall=call_wall,
        put_wall=put_wall,
        magnet=magnet,
        flip=flip,
        pressure=pressure,
        call_wall_abs_gex=call_wall_abs,
        put_wall_abs_gex=put_wall_abs,
        magnet_abs_gex=magnet_abs,
        vega_net=vega_net,
        vega_abs=vega_abs,
        atm_iv_mid=atm_iv_mid,
        iv_upper=iv_upper,
        iv_lower=iv_lower,
        iv_move=iv_move,
    )
