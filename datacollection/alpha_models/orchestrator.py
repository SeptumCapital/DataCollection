from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .common import AlphaData, clean_payload, load_alpha_data, write_alpha_json
from .earnings_drift import generate_earnings_drift_signals
from .ml_alpha_combiner import generate_ml_alpha_combiner_signals
from .multifactor import generate_multifactor_signals
from .pairs_trading import generate_pair_trade_signals
from .residual_stat_arb import generate_residual_stat_arb_signals


OFFLINE_ALPHA_FILENAME = "offline_alpha_recommendations.json"


def offline_alpha_path(data_root: Path) -> Path:
    return data_root / "recommendations" / OFFLINE_ALPHA_FILENAME


def _limited_rows(frame: pd.DataFrame, count: int, ascending: bool = False) -> pd.DataFrame:
    if frame.empty:
        return frame
    sorted_frame = frame.sort_values("alpha_score", ascending=ascending, na_position="last")
    return sorted_frame.head(count).reset_index(drop=True)


def _stock_rows(frame: pd.DataFrame, signal: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rank, row in enumerate(frame.to_dict("records"), start=1):
        rows.append(
            {
                "rank": rank,
                "signal": signal,
                "confidence": row.get("alpha_confidence"),
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "sector": row.get("sector"),
                "industry": row.get("industry"),
                "last_date": row.get("last_date"),
                "last_close": row.get("last_close"),
                "alpha_score": row.get("alpha_score"),
                "ml_expected_21d": row.get("ml_expected_21d"),
                "multifactor_score": row.get("multifactor_score"),
                "residual_score": row.get("residual_score"),
                "filing_drift_score": row.get("filing_drift_score"),
                "momentum_12_1": row.get("momentum_12_1"),
                "trend_200": row.get("trend_200"),
                "volatility_21d": row.get("volatility_21d"),
                "reason": row.get("alpha_reason"),
            }
        )
    return rows


def _pair_rows(frame: pd.DataFrame, count: int) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for rank, row in enumerate(frame.head(count).to_dict("records"), start=1):
        rows.append(
            {
                "rank": rank,
                "long_symbol": row.get("long_symbol"),
                "long_name": row.get("long_name"),
                "short_symbol": row.get("short_symbol"),
                "short_name": row.get("short_name"),
                "sector": row.get("sector"),
                "spread_z": row.get("spread_z"),
                "hedge_ratio": row.get("hedge_ratio"),
                "expected_mean_reversion": row.get("expected_mean_reversion"),
                "correlation": row.get("correlation"),
                "half_life_days": row.get("half_life_days"),
                "confidence": row.get("confidence"),
                "reason": row.get("reason"),
                "last_date": row.get("last_date"),
                "long_last_close": row.get("long_last_close"),
                "short_last_close": row.get("short_last_close"),
            }
        )
    return rows


def _diagnostics(**frames: pd.DataFrame) -> dict[str, Any]:
    return {
        name: {
            "rows": int(len(frame)),
            "columns": list(frame.columns)[:40],
        }
        for name, frame in frames.items()
    }


def build_offline_alpha_recommendations(
    data_root: Path | None = None,
    symbols: list[str] | None = None,
    output_path: Path | None = None,
    count: int = 5,
) -> dict[str, Any]:
    """Run all offline alpha models and write the app-readable artifact."""

    data: AlphaData = load_alpha_data(data_root, symbols=symbols)
    multifactor = generate_multifactor_signals(data=data)
    residual = generate_residual_stat_arb_signals(data=data)
    filing_drift = generate_earnings_drift_signals(data=data)
    combined, model_info = generate_ml_alpha_combiner_signals(
        data=data,
        multifactor=multifactor,
        residual=residual,
        filing_drift=filing_drift,
    )
    pairs = generate_pair_trade_signals(data=data)

    buy = _stock_rows(_limited_rows(combined, count, ascending=False), "BUY")
    sell = _stock_rows(_limited_rows(combined, count, ascending=True), "SELL")
    pair_rows = _pair_rows(pairs, count)
    as_of_values = [row.get("last_date") for row in [*buy, *sell, *pair_rows] if row.get("last_date")]
    status = "ready" if len(buy) == count and len(sell) == count and len(pair_rows) == count else "partial"

    payload = {
        "status": status,
        "as_of": max(as_of_values) if as_of_values else None,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "universe": "S&P 500",
        "disclaimer": "Offline alpha research signals from public local data. Not financial advice.",
        "model": {
            "name": "Offline alpha ensemble: multi-factor, residual stat arb, filing drift, pairs, ML combiner",
            **model_info,
            "stock_count": int(len(combined)),
            "pair_count": int(len(pairs)),
        },
        "methodology": [
            "Runs after market close from local S&P 500 prices, technicals, SEC fundamentals, and Yahoo enrichment.",
            "Stock buy/sell rankings blend multi-factor scores, residual stat-arb mean reversion, SEC filing drift proxy, and a supervised next-21D return model.",
            "Pair trades are same-sector relative-value spreads ranked by spread z-score, return correlation, hedge ratio, and mean-reversion half-life proxy.",
            "The app reads this artifact only; it does not train or recompute alpha recommendations during normal use.",
        ],
        "buy": buy,
        "sell": sell,
        "pairs": pair_rows,
        "diagnostics": _diagnostics(
            multifactor=multifactor,
            residual_stat_arb=residual,
            filing_drift=filing_drift,
            ml_alpha_combiner=combined,
            pairs_trading=pairs,
        ),
    }
    payload = clean_payload(payload)
    output_path = output_path or offline_alpha_path(data.data_root)
    write_alpha_json(output_path, payload)
    return payload
