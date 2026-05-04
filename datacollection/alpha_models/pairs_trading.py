from __future__ import annotations

from itertools import combinations
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import AlphaData, confidence_from_score, latest_meta, latest_stock_snapshot, load_alpha_data, safe_float


def _aligned_log_prices(data: AlphaData, keys: list[str], lookback: int = 252) -> pd.DataFrame:
    series: list[pd.Series] = []
    for key in keys:
        frame = data.technicals.get(key)
        if frame is None:
            continue
        trimmed = frame.tail(lookback)
        values = pd.to_numeric(trimmed["_close"], errors="coerce")
        values = np.log(values.where(values > 0))
        series.append(pd.Series(values.to_numpy(), index=trimmed["date"], name=key))
    if not series:
        return pd.DataFrame()
    return pd.concat(series, axis=1).sort_index().ffill().dropna(axis=1, thresh=180)


def _half_life_proxy(spread: pd.Series) -> float | None:
    spread = spread.dropna()
    if len(spread) < 80:
        return None
    lagged = spread.shift(1).dropna()
    delta = spread.diff().dropna()
    sample = pd.concat([lagged.rename("lagged"), delta.rename("delta")], axis=1).dropna()
    if len(sample) < 60:
        return None
    x = sample["lagged"] - sample["lagged"].mean()
    y = sample["delta"]
    denom = float((x * x).sum())
    if denom == 0:
        return None
    beta = float((x * y).sum() / denom)
    if beta >= 0:
        return None
    return float(np.clip(-np.log(2) / beta, 1, 120))


def generate_pair_trade_signals(
    data_root: Path | None = None,
    symbols: list[str] | None = None,
    data: AlphaData | None = None,
    max_names_per_sector: int = 35,
    min_abs_z: float = 1.25,
) -> pd.DataFrame:
    """Find same-sector relative-value pair trades from public price histories."""

    data = data or load_alpha_data(data_root, symbols=symbols)
    snapshots = {
        key: snapshot
        for key in data.technicals
        if (snapshot := latest_stock_snapshot(data, key)) is not None
    }
    if len(snapshots) < 2:
        return pd.DataFrame()

    snapshot_frame = pd.DataFrame(snapshots.values())
    rows: list[dict[str, Any]] = []
    for sector, members in snapshot_frame.groupby("sector"):
        keys = (
            members.sort_values("dollar_volume", ascending=False)
            .head(max_names_per_sector)["symbol_key"]
            .astype(str)
            .tolist()
        )
        prices = _aligned_log_prices(data, keys)
        if prices.shape[1] < 2:
            continue
        returns = prices.diff().dropna()
        for left, right in combinations(prices.columns, 2):
            sample = pd.concat([prices[left].rename("left"), prices[right].rename("right")], axis=1).dropna()
            if len(sample) < 180:
                continue
            corr = returns[left].corr(returns[right]) if left in returns and right in returns else None
            if corr is None or pd.isna(corr) or corr < 0.55:
                continue
            right_var = float(sample["right"].var(ddof=0))
            if right_var == 0:
                continue
            hedge_ratio = float(sample["left"].cov(sample["right"]) / right_var)
            spread = sample["left"] - hedge_ratio * sample["right"]
            spread_std = float(spread.tail(126).std(ddof=0))
            if spread_std == 0 or pd.isna(spread_std):
                continue
            spread_z = float((spread.iloc[-1] - spread.tail(126).mean()) / spread_std)
            if abs(spread_z) < min_abs_z:
                continue
            half_life = _half_life_proxy(spread)
            if half_life is None:
                half_life = 60.0
            if half_life > 80:
                continue
            if spread_z > 0:
                long_key, short_key = right, left
                reason = f"{left} rich versus {right}; spread z-score {spread_z:.2f}"
            else:
                long_key, short_key = left, right
                reason = f"{left} cheap versus {right}; spread z-score {spread_z:.2f}"
            expected_mean_reversion = min(abs(spread_z) / max(half_life, 1), 1.0)
            quality_score = float(
                np.clip(abs(spread_z) * 0.75 + corr * 0.75 + (30 / max(half_life, 1)) * 0.25, 0, 4)
            )
            rows.append(
                {
                    "long_symbol": latest_meta(data, long_key).get("symbol") or long_key,
                    "long_name": latest_meta(data, long_key).get("name", ""),
                    "short_symbol": latest_meta(data, short_key).get("symbol") or short_key,
                    "short_name": latest_meta(data, short_key).get("name", ""),
                    "sector": sector,
                    "spread_z": spread_z,
                    "hedge_ratio": hedge_ratio,
                    "correlation": corr,
                    "half_life_days": half_life,
                    "expected_mean_reversion": expected_mean_reversion,
                    "pair_score": quality_score,
                    "confidence": confidence_from_score(quality_score, high=2.2, medium=1.3),
                    "reason": reason,
                    "last_date": max(
                        str(snapshots[long_key]["last_date"]),
                        str(snapshots[short_key]["last_date"]),
                    ),
                    "long_last_close": safe_float(snapshots[long_key]["last_close"]),
                    "short_last_close": safe_float(snapshots[short_key]["last_close"]),
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return frame.sort_values(["pair_score", "correlation"], ascending=False).reset_index(drop=True)
