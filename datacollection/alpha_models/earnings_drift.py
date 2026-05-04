from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .common import (
    AlphaData,
    confidence_from_score,
    latest_stock_snapshot,
    load_alpha_data,
    pct_change,
    robust_zscore,
    safe_float,
    sector_neutralize,
)


DRIFT_METRICS = ("revenue", "net_income", "eps_diluted", "operating_cash_flow", "gross_profit")


def _metric_growth(frame: pd.DataFrame | None, metric: str) -> tuple[float | None, pd.Timestamp | None]:
    if frame is None or frame.empty:
        return None, None
    rows = frame[(frame["metric"] == metric) & frame["form"].astype(str).isin(["10-Q", "10-K"])].dropna(
        subset=["end", "filed", "value"]
    )
    if rows.empty:
        return None, None
    rows = rows.sort_values(["end", "filed"]).drop_duplicates(["end"], keep="last")
    if len(rows) < 5:
        return None, pd.Timestamp(rows.iloc[-1]["filed"])
    latest = rows.iloc[-1]
    previous_comparable = rows.iloc[-5] if len(rows) >= 5 else rows.iloc[-2]
    previous_value = safe_float(previous_comparable["value"])
    latest_value = safe_float(latest["value"])
    if previous_value in (None, 0) or latest_value is None:
        return None, latest["filed"]
    return (latest_value - previous_value) / abs(previous_value), pd.Timestamp(latest["filed"])


def _post_filing_drift(price_frame: pd.DataFrame, filed: pd.Timestamp | None) -> float | None:
    if filed is None or pd.isna(filed):
        return None
    after = price_frame[price_frame["date"] >= filed]
    if after.empty:
        return None
    first = safe_float(after.iloc[0]["_close"])
    last = safe_float(price_frame.iloc[-1]["_close"])
    return pct_change(first, last)


def generate_earnings_drift_signals(
    data_root: Path | None = None,
    symbols: list[str] | None = None,
    data: AlphaData | None = None,
) -> pd.DataFrame:
    """Generate a filing/earnings-drift proxy from SEC filings and local prices.

    The local dataset does not include historical earnings-surprise consensus data, so this model is
    explicitly a public filing and reported-growth drift proxy.
    """

    data = data or load_alpha_data(data_root, symbols=symbols)
    rows: list[dict[str, Any]] = []
    for key, price_frame in data.technicals.items():
        snapshot = latest_stock_snapshot(data, key)
        if snapshot is None:
            continue
        fundamentals = data.fundamentals.get(key)
        metric_values: dict[str, float | None] = {}
        filing_dates: list[pd.Timestamp] = []
        for metric in DRIFT_METRICS:
            growth, filed = _metric_growth(fundamentals, metric)
            metric_values[f"{metric}_yoy_growth"] = growth
            if filed is not None and not pd.isna(filed):
                filing_dates.append(pd.Timestamp(filed))
        latest_filed = max(filing_dates) if filing_dates else None
        rows.append(
            {
                **snapshot,
                **metric_values,
                "latest_filing_date": latest_filed.date().isoformat() if latest_filed is not None else None,
                "post_filing_drift": _post_filing_drift(price_frame, latest_filed),
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    growth_cols = [f"{metric}_yoy_growth" for metric in DRIFT_METRICS]
    frame["reported_growth_raw"] = sum(robust_zscore(frame[column]) for column in growth_cols) / len(growth_cols)
    frame["filing_drift_raw_score"] = (
        0.48 * frame["reported_growth_raw"]
        + 0.24 * robust_zscore(frame["post_filing_drift"])
        + 0.16 * robust_zscore(frame["return_1m"])
        + 0.12 * robust_zscore(frame["trend_200"])
    )
    frame["filing_drift_score"] = sector_neutralize(frame, "filing_drift_raw_score")
    frame["filing_drift_confidence"] = frame["filing_drift_score"].map(confidence_from_score)

    def reason(row: pd.Series) -> str:
        parts: list[str] = []
        if safe_float(row.get("reported_growth_raw")) and row["reported_growth_raw"] > 0.5:
            parts.append("positive reported growth proxy")
        if safe_float(row.get("post_filing_drift")) and row["post_filing_drift"] > 0:
            parts.append("positive post-filing drift")
        if safe_float(row.get("filing_drift_score")) and row["filing_drift_score"] < -0.5:
            parts.append("weak filing/growth drift proxy")
        if row.get("latest_filing_date"):
            parts.append(f"latest filing {row['latest_filing_date']}")
        return ", ".join(parts[:4]) or "ranked by SEC filing drift proxy"

    frame["filing_drift_reason"] = frame.apply(reason, axis=1)
    return frame
