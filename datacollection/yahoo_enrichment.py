from __future__ import annotations

import time
import json
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from .config import data_root
from .storage import ensure_dir, write_dataframe, write_json
from .universe import provider_symbol


TABLES = (
    "institutional_holders",
    "mutualfund_holders",
    "major_holders",
    "insider_transactions",
    "insider_purchases",
    "insider_roster_holders",
    "recommendations_summary",
    "upgrades_downgrades",
    "earnings_estimate",
    "revenue_estimate",
    "eps_trend",
    "eps_revisions",
    "growth_estimates",
)


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp,)):
        return None if pd.isna(value) else value.isoformat()
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            pass
    return value


def frame_to_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    normalized = frame.reset_index()
    return [
        {str(key): _jsonable(value) for key, value in row.items()}
        for row in normalized.to_dict("records")
    ]


def insider_action(row: dict[str, Any]) -> str:
    text = f"{row.get('Transaction') or ''} {row.get('Text') or ''}".strip().lower()
    if "sale at price" in text or text.startswith("sale "):
        return "Sell"
    if "purchase at price" in text or text.startswith("purchase "):
        return "Buy"
    if "stock award" in text or "grant" in text:
        return "Award/Grant"
    if "stock gift" in text or "gift" in text:
        return "Gift"
    if "derivative" in text or "exercise" in text or "conversion" in text:
        return "Exercise/Conversion"
    return "Other"


def _transaction_date(row: dict[str, Any]) -> pd.Timestamp | None:
    raw = row.get("Start Date") or row.get("Transaction Start Date")
    date = pd.to_datetime(raw, errors="coerce")
    return None if pd.isna(date) else date


def explicit_insider_summary(rows: list[dict[str, Any]], collected_at: Any = None, days: int = 183) -> dict[str, Any]:
    reference = pd.to_datetime(collected_at, errors="coerce")
    if pd.isna(reference):
        dates = [_transaction_date(row) for row in rows]
        dates = [value for value in dates if value is not None]
        reference = max(dates) if dates else pd.Timestamp.now("UTC")
    reference = pd.Timestamp(reference).tz_localize(None)
    cutoff = reference - pd.Timedelta(days=days)

    def is_recent(row: dict[str, Any]) -> bool:
        date = _transaction_date(row)
        return bool(date is not None and pd.Timestamp(date).tz_localize(None) >= cutoff)

    explicit_buys = [row for row in rows if insider_action(row) == "Buy" and is_recent(row)]
    explicit_sells = [row for row in rows if insider_action(row) == "Sell" and is_recent(row)]

    def total(column: str, selected: list[dict[str, Any]]) -> float:
        values = []
        for row in selected:
            value = row.get(column)
            try:
                if value is not None and not pd.isna(value):
                    values.append(float(value))
            except (TypeError, ValueError):
                pass
        return sum(values)

    return {
        "explicit_insider_buy_flag": bool(explicit_buys),
        "explicit_insider_buy_count": len(explicit_buys),
        "explicit_insider_sell_count": len(explicit_sells),
        "explicit_insider_buy_shares": total("Shares", explicit_buys),
        "explicit_insider_sell_shares": total("Shares", explicit_sells),
        "explicit_insider_buy_value": total("Value", explicit_buys),
        "explicit_insider_sell_value": total("Value", explicit_sells),
    }


def extract_yahoo_enrichment(symbol: str) -> dict[str, Any]:
    provider_ticker = provider_symbol(symbol, "yahoo")
    ticker = yf.Ticker(provider_ticker)
    payload: dict[str, Any] = {
        "symbol": symbol,
        "provider_symbol": provider_ticker,
        "provider": "yfinance/yahoo",
        "tables": {},
        "errors": {},
    }

    for table in TABLES:
        try:
            value = getattr(ticker, table)
            if isinstance(value, pd.DataFrame):
                payload["tables"][table] = frame_to_records(value)
            else:
                payload["tables"][table] = _jsonable(value)
        except Exception as exc:  # noqa: BLE001 - partial data is common for these modules.
            payload["errors"][table] = str(exc)

    try:
        payload["analyst_price_targets"] = {
            str(key): _jsonable(value)
            for key, value in ticker.analyst_price_targets.items()
        }
    except Exception as exc:  # noqa: BLE001
        payload["errors"]["analyst_price_targets"] = str(exc)
        payload["analyst_price_targets"] = {}

    payload["collected_at"] = pd.Timestamp.now("UTC").isoformat()
    return payload


def summarize_enrichment(payload: dict[str, Any]) -> dict[str, Any]:
    tables = payload.get("tables", {})
    major_rows = tables.get("major_holders") or []
    major = {
        row.get("Breakdown") or row.get("index"): row.get("Value")
        for row in major_rows
        if isinstance(row, dict)
    }

    explicit = explicit_insider_summary(tables.get("insider_transactions") or [], payload.get("collected_at"))

    rec_rows = tables.get("recommendations_summary") or []
    current_rec = next((row for row in rec_rows if row.get("period") == "0m"), rec_rows[0] if rec_rows else {})
    rec_score = None
    rec_total = 0
    if current_rec:
        weights = {"strongBuy": 1, "buy": 2, "hold": 3, "sell": 4, "strongSell": 5}
        numerator = 0.0
        for key, weight in weights.items():
            count = current_rec.get(key) or 0
            numerator += float(count) * weight
            rec_total += int(count)
        if rec_total:
            rec_score = numerator / rec_total

    price_targets = payload.get("analyst_price_targets") or {}
    earnings = (tables.get("earnings_estimate") or [{}])[0]
    revenue = (tables.get("revenue_estimate") or [{}])[0]

    return {
        "symbol": payload.get("symbol"),
        "provider_symbol": payload.get("provider_symbol"),
        "collected_at": payload.get("collected_at"),
        "institutions_percent_held": major.get("institutionsPercentHeld"),
        "insiders_percent_held": major.get("insidersPercentHeld"),
        "institutional_holders_count": len(tables.get("institutional_holders") or []),
        "insider_buy_flag": explicit["explicit_insider_buy_flag"],
        "insider_purchase_shares_6m": explicit["explicit_insider_buy_shares"],
        "insider_purchase_transactions_6m": explicit["explicit_insider_buy_count"],
        "insider_sale_shares_6m": explicit["explicit_insider_sell_shares"],
        "insider_sale_transactions_6m": explicit["explicit_insider_sell_count"],
        **explicit,
        "analyst_rating_score": rec_score,
        "analyst_rating_count": rec_total or None,
        "price_target_mean": price_targets.get("mean"),
        "price_target_high": price_targets.get("high"),
        "price_target_low": price_targets.get("low"),
        "eps_estimate_current_q": earnings.get("avg"),
        "eps_estimate_current_q_analysts": earnings.get("numberOfAnalysts"),
        "revenue_estimate_current_q": revenue.get("avg"),
        "revenue_estimate_current_q_analysts": revenue.get("numberOfAnalysts"),
    }


def save_yahoo_enrichment(symbol: str, output_dir: Path | None = None) -> tuple[Path, dict[str, Any]]:
    if output_dir is None:
        output_dir = data_root() / "enrichment" / "yahoo_quote_summary"
    ensure_dir(output_dir)
    payload = extract_yahoo_enrichment(symbol)
    path = write_json(output_dir / f"{provider_symbol(symbol, 'yahoo')}.json", payload)
    return path, summarize_enrichment(payload)


def save_yahoo_enrichment_batch(
    symbols: list[str],
    output_dir: Path | None = None,
    sleep_seconds: float = 0.25,
    overwrite: bool = False,
) -> list[dict[str, Any]]:
    if output_dir is None:
        output_dir = data_root() / "enrichment" / "yahoo_quote_summary"
    ensure_dir(output_dir)

    summaries: list[dict[str, Any]] = []
    for symbol in symbols:
        path = output_dir / f"{provider_symbol(symbol, 'yahoo')}.json"
        if path.exists() and not overwrite:
            try:
                summaries.append(summarize_enrichment(json.loads(path.read_text(encoding="utf-8"))))
            except Exception:
                pass
            continue
        _, summary = save_yahoo_enrichment(symbol, output_dir=output_dir)
        summaries.append(summary)
        time.sleep(sleep_seconds)

    write_dataframe(data_root() / "enrichment" / "yahoo_quote_summary_summary.csv", pd.DataFrame(summaries))
    return summaries
