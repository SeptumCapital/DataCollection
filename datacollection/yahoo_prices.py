from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from .config import data_root
from .http_client import HttpClient, ProviderError
from .storage import ensure_dir, write_dataframe
from .universe import provider_symbol


YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _to_epoch(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())


def fetch_yahoo_daily_prices(symbol: str, start: date, end: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    provider_ticker = provider_symbol(symbol, "yahoo")
    client = HttpClient(headers={"User-Agent": "Mozilla/5.0 SenQuantDataCollection/0.1"})
    payload = client.get_json(
        YAHOO_CHART_URL.format(symbol=provider_ticker),
        period1=_to_epoch(start),
        period2=_to_epoch(end + timedelta(days=1)),
        interval="1d",
        events="div,splits",
        includeAdjustedClose="true",
    )

    chart = payload.get("chart", {})
    if chart.get("error"):
        raise ProviderError(f"Yahoo chart error for {symbol}: {chart['error']}")
    result = (chart.get("result") or [None])[0]
    if not result:
        raise ProviderError(f"Yahoo returned no chart result for {symbol}")

    timestamps = result.get("timestamp") or []
    quote = (result.get("indicators", {}).get("quote") or [{}])[0]
    adjclose = (result.get("indicators", {}).get("adjclose") or [{}])[0].get("adjclose", [])
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(timestamps, unit="s", utc=True).date.astype(str),
            "open": quote.get("open", []),
            "high": quote.get("high", []),
            "low": quote.get("low", []),
            "close": quote.get("close", []),
            "volume": quote.get("volume", []),
            "adj_close": adjclose,
        }
    )
    prices.insert(0, "symbol", symbol)
    prices.insert(1, "provider_symbol", provider_ticker)
    prices = prices.dropna(subset=["date"]).sort_values("date")

    events: list[dict[str, object]] = []
    for event_type, values in (result.get("events") or {}).items():
        for epoch, event in values.items():
            row = dict(event)
            row["symbol"] = symbol
            row["provider_symbol"] = provider_ticker
            row["event_type"] = event_type
            row["date"] = datetime.fromtimestamp(int(epoch), tz=timezone.utc).date().isoformat()
            events.append(row)
    actions = pd.DataFrame(events)
    return prices, actions


def save_yahoo_daily_prices(
    symbols: list[str],
    start: date,
    end: date,
    output_dir: Path | None = None,
) -> list[Path]:
    if output_dir is None:
        output_dir = data_root() / "prices" / "yahoo_daily"
    ensure_dir(output_dir)
    action_dir = ensure_dir(output_dir / "corporate_actions")

    written: list[Path] = []
    for symbol in symbols:
        prices, actions = fetch_yahoo_daily_prices(symbol, start=start, end=end)
        written.append(write_dataframe(output_dir / f"{symbol.replace('.', '-')}.csv", prices))
        if not actions.empty:
            write_dataframe(action_dir / f"{symbol.replace('.', '-')}.csv", actions)
    return written
