from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import AlphaData, confidence_from_score, latest_meta, latest_stock_snapshot, load_alpha_data, safe_float


def _price_matrix(data: AlphaData, lookback: int = 360) -> pd.DataFrame:
    series: dict[str, pd.Series] = {}
    for key, frame in data.technicals.items():
        snapshot = latest_stock_snapshot(data, key)
        if snapshot is None:
            continue
        trimmed = frame.tail(lookback)
        series[key] = pd.Series(trimmed["_close"].to_numpy(), index=trimmed["date"], name=key)
    if not series:
        return pd.DataFrame()
    return pd.concat(series.values(), axis=1).sort_index().ffill().dropna(axis=1, thresh=250)


def _regression_residual(y: pd.Series, x: pd.DataFrame) -> pd.Series | None:
    sample = pd.concat([y.rename("y"), x], axis=1).dropna()
    if len(sample) < 120:
        return None
    yv = sample["y"].to_numpy(dtype=float)
    xv = sample.drop(columns=["y"]).to_numpy(dtype=float)
    xv = np.column_stack([np.ones(len(xv)), xv])
    try:
        beta, *_ = np.linalg.lstsq(xv, yv, rcond=None)
    except np.linalg.LinAlgError:
        return None
    return pd.Series(yv - xv @ beta, index=sample.index)


def generate_residual_stat_arb_signals(
    data_root: Path | None = None,
    symbols: list[str] | None = None,
    data: AlphaData | None = None,
) -> pd.DataFrame:
    """Rank short-horizon residual mean-reversion signals.

    Positive scores favor a long candidate after sector/market residual underperformance.
    Negative scores favor a short candidate after residual outperformance.
    """

    data = data or load_alpha_data(data_root, symbols=symbols)
    prices = _price_matrix(data)
    if prices.empty or prices.shape[1] < 5:
        return pd.DataFrame()

    returns = prices.pct_change().replace([np.inf, -np.inf], np.nan).dropna(how="all")
    market = returns.mean(axis=1).rename("market")
    meta = {key: latest_meta(data, key) for key in returns.columns}
    sector_map = {key: str(meta[key].get("sector") or "") for key in returns.columns}

    rows: list[dict[str, Any]] = []
    for key in returns.columns:
        sector = sector_map.get(key, "")
        peer_keys = [candidate for candidate in returns.columns if sector_map.get(candidate, "") == sector and candidate != key]
        if len(peer_keys) >= 3:
            sector_return = returns[peer_keys].median(axis=1).rename("sector")
        else:
            sector_return = market.rename("sector")
        residual = _regression_residual(returns[key], pd.concat([market, sector_return], axis=1))
        if residual is None or len(residual) < 120:
            continue
        residual_5d = residual.tail(5).sum()
        residual_vol = residual.tail(63).std(ddof=0)
        if residual_vol in (None, 0) or pd.isna(residual_vol):
            continue
        residual_z = float(residual_5d / (residual_vol * np.sqrt(5)))
        score = float(np.clip(-residual_z, -4, 4))
        snapshot = latest_stock_snapshot(data, key)
        if snapshot is None:
            continue
        direction = "underperformed peers" if residual_z < 0 else "outperformed peers"
        rows.append(
            {
                **snapshot,
                "residual_z": residual_z,
                "residual_score": score,
                "residual_confidence": confidence_from_score(score),
                "residual_reason": f"{direction} on market/sector residuals; mean-reversion score {score:.2f}",
                "residual_observations": int(len(residual)),
                "residual_volatility": safe_float(residual_vol),
            }
        )
    return pd.DataFrame(rows)
