from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .common import (
    AlphaData,
    confidence_from_score,
    enrichment_summary,
    latest_stock_snapshot,
    load_alpha_data,
    robust_zscore,
    safe_float,
    sector_neutralize,
)


def _latest_metric(frame: pd.DataFrame | None, metric: str) -> float | None:
    if frame is None or frame.empty:
        return None
    rows = frame[frame["metric"] == metric].dropna(subset=["end", "value"])
    if rows.empty:
        return None
    rows = rows.sort_values(["end", "filed"]).drop_duplicates(["end"], keep="last")
    return safe_float(rows.iloc[-1]["value"])


def _fundamental_features(data: AlphaData, key: str) -> dict[str, float | None]:
    frame = data.fundamentals.get(key)
    assets = _latest_metric(frame, "assets")
    liabilities = _latest_metric(frame, "liabilities")
    gross_profit = _latest_metric(frame, "gross_profit")
    net_income = _latest_metric(frame, "net_income")
    operating_cash_flow = _latest_metric(frame, "operating_cash_flow")
    equity = _latest_metric(frame, "stockholders_equity")
    return {
        "gross_profitability": gross_profit / assets if assets not in (None, 0) and gross_profit is not None else None,
        "net_margin_proxy": net_income / assets if assets not in (None, 0) and net_income is not None else None,
        "cash_quality": operating_cash_flow / assets if assets not in (None, 0) and operating_cash_flow is not None else None,
        "leverage_proxy": liabilities / assets if assets not in (None, 0) and liabilities is not None else None,
        "roe_proxy": net_income / equity if equity not in (None, 0) and net_income is not None else None,
    }


def generate_multifactor_signals(
    data_root: Path | None = None,
    symbols: list[str] | None = None,
    data: AlphaData | None = None,
) -> pd.DataFrame:
    """Score stocks with public multi-factor signals.

    Positive scores favor long/buy candidates; negative scores favor sell/short candidates.
    """

    data = data or load_alpha_data(data_root, symbols=symbols)
    rows: list[dict[str, Any]] = []
    for key in sorted(data.technicals):
        snapshot = latest_stock_snapshot(data, key)
        if snapshot is None:
            continue
        enrich = enrichment_summary(data.enrichment.get(key, {}))
        target_upside = None
        if enrich.get("price_target_mean") not in (None, 0):
            target_upside = safe_float(enrich["price_target_mean"]) / snapshot["last_close"] - 1
        rows.append(
            {
                **snapshot,
                **_fundamental_features(data, key),
                **enrich,
                "target_upside": target_upside,
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    frame["analyst_factor"] = -pd.to_numeric(frame["analyst_rating_score"], errors="coerce")
    frame["quality_raw"] = (
        0.35 * robust_zscore(frame["gross_profitability"])
        + 0.25 * robust_zscore(frame["net_margin_proxy"])
        + 0.25 * robust_zscore(frame["cash_quality"])
        + 0.15 * robust_zscore(frame["roe_proxy"])
        - 0.25 * robust_zscore(frame["leverage_proxy"])
    )
    frame["momentum_raw"] = (
        0.50 * robust_zscore(frame["momentum_12_1"])
        + 0.25 * robust_zscore(frame["return_6m"])
        + 0.15 * robust_zscore(frame["return_3m"])
        + 0.10 * robust_zscore(frame["return_1m"])
    )
    frame["trend_raw"] = 0.60 * robust_zscore(frame["trend_200"]) + 0.40 * robust_zscore(frame["trend_50"])
    frame["sentiment_raw"] = (
        0.45 * robust_zscore(frame["target_upside"])
        + 0.35 * robust_zscore(frame["analyst_factor"])
        + 0.20 * robust_zscore(frame["institutions_percent_held"])
    )
    frame["risk_raw"] = -robust_zscore(frame["volatility_21d"])
    frame["multifactor_raw_score"] = (
        0.30 * frame["momentum_raw"]
        + 0.24 * frame["quality_raw"]
        + 0.18 * frame["trend_raw"]
        + 0.18 * frame["sentiment_raw"]
        + 0.10 * frame["risk_raw"]
    )
    frame["multifactor_score"] = sector_neutralize(frame, "multifactor_raw_score")
    frame["multifactor_confidence"] = frame["multifactor_score"].map(confidence_from_score)

    def reason(row: pd.Series) -> str:
        parts: list[str] = []
        if safe_float(row.get("momentum_raw")) and row["momentum_raw"] > 0.5:
            parts.append("strong momentum")
        if safe_float(row.get("quality_raw")) and row["quality_raw"] > 0.5:
            parts.append("quality/profitability support")
        if safe_float(row.get("trend_200")) and row["trend_200"] > 0:
            parts.append("above 200D trend")
        if safe_float(row.get("sentiment_raw")) and row["sentiment_raw"] > 0.4:
            parts.append("analyst/enrichment support")
        if not parts and safe_float(row.get("multifactor_score")) and row["multifactor_score"] < 0:
            parts.append("weak factor blend")
        return ", ".join(parts[:4]) or "ranked by public multi-factor blend"

    frame["multifactor_reason"] = frame.apply(reason, axis=1)
    return frame
