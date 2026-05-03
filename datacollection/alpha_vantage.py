from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .config import alpha_vantage_api_key, data_root
from .http_client import HttpClient, ProviderError
from .storage import ensure_dir, write_json


ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"

FUNDAMENTAL_FUNCTIONS = ("OVERVIEW", "INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW", "EARNINGS")
TECHNICAL_FUNCTIONS = ("SMA", "EMA", "RSI", "MACD", "BBANDS", "ADX", "OBV")


def _require_api_key(api_key: str | None = None) -> str:
    key = api_key or alpha_vantage_api_key()
    if not key:
        raise ProviderError("ALPHA_VANTAGE_API_KEY is not set. Copy .env.example to .env and add a key.")
    return key


def _client(sleep_seconds: float) -> HttpClient:
    return HttpClient(headers={"User-Agent": "SenQuantDataCollection/0.1"}, min_interval=sleep_seconds)


def _query_with_client(
    client: HttpClient,
    function: str,
    symbol: str | None = None,
    api_key: str | None = None,
    **params: Any,
) -> dict[str, Any]:
    request_params: dict[str, Any] = {"function": function, "apikey": _require_api_key(api_key)}
    if symbol:
        request_params["symbol"] = symbol
    request_params.update({key: value for key, value in params.items() if value is not None})
    return client.get_json(ALPHA_VANTAGE_URL, **request_params)


def query_alpha_vantage(
    function: str,
    symbol: str | None = None,
    api_key: str | None = None,
    sleep_seconds: float = 12.5,
    **params: Any,
) -> dict[str, Any]:
    return _query_with_client(
        _client(sleep_seconds),
        function,
        symbol=symbol,
        api_key=api_key,
        **params,
    )


def save_fundamentals(
    symbol: str,
    functions: tuple[str, ...] = FUNDAMENTAL_FUNCTIONS,
    output_dir: Path | None = None,
    sleep_seconds: float = 12.5,
) -> list[Path]:
    if output_dir is None:
        output_dir = data_root() / "alpha_vantage" / "fundamentals"
    ensure_dir(output_dir)

    written: list[Path] = []
    client = _client(sleep_seconds)
    for function in functions:
        payload = _query_with_client(client, function, symbol=symbol)
        written.append(write_json(output_dir / function.lower() / f"{symbol.replace('.', '-')}.json", payload))
    return written


def technical_params(function: str) -> dict[str, Any]:
    base = {"interval": "daily"}
    if function in {"SMA", "EMA", "RSI", "BBANDS", "ADX"}:
        base["time_period"] = 14 if function in {"RSI", "ADX"} else 20
    if function in {"SMA", "EMA", "RSI", "MACD", "BBANDS"}:
        base["series_type"] = "close"
    return base


def save_technical_indicators(
    symbol: str,
    functions: tuple[str, ...] = TECHNICAL_FUNCTIONS,
    output_dir: Path | None = None,
    sleep_seconds: float = 12.5,
) -> list[Path]:
    if output_dir is None:
        output_dir = data_root() / "alpha_vantage" / "technicals"
    ensure_dir(output_dir)

    written: list[Path] = []
    client = _client(sleep_seconds)
    for function in functions:
        payload = _query_with_client(
            client,
            function,
            symbol=symbol,
            **technical_params(function),
        )
        written.append(write_json(output_dir / function.lower() / f"{symbol.replace('.', '-')}.json", payload))
    return written


def save_news_sentiment(
    symbol: str,
    output_dir: Path | None = None,
    time_from: str | None = None,
    time_to: str | None = None,
    limit: int = 1000,
    sort: str = "EARLIEST",
    sleep_seconds: float = 12.5,
) -> Path:
    if output_dir is None:
        output_dir = data_root() / "alpha_vantage" / "sentiment"
    ensure_dir(output_dir)
    payload = query_alpha_vantage(
        "NEWS_SENTIMENT",
        sleep_seconds=sleep_seconds,
        tickers=symbol,
        time_from=time_from,
        time_to=time_to,
        limit=limit,
        sort=sort,
    )
    return write_json(output_dir / f"{symbol.replace('.', '-')}.json", payload)


def save_daily_adjusted_csv(
    symbol: str,
    output_dir: Path | None = None,
    outputsize: str = "full",
    sleep_seconds: float = 12.5,
) -> Path:
    if output_dir is None:
        output_dir = data_root() / "alpha_vantage" / "prices_daily_adjusted"
    ensure_dir(output_dir)
    key = _require_api_key(None)
    text = _client(sleep_seconds).get_text(
        ALPHA_VANTAGE_URL,
        function="TIME_SERIES_DAILY_ADJUSTED",
        symbol=symbol,
        outputsize=outputsize,
        datatype="csv",
        apikey=key,
    )
    if text.lstrip().startswith("{"):
        raise ProviderError(text[:500])
    path = output_dir / f"{symbol.replace('.', '-')}.csv"
    path.write_text(text, encoding="utf-8")
    return path


def alpha_json_to_feed_frame(path: Path) -> pd.DataFrame:
    payload = pd.read_json(path, typ="series").to_dict()
    feed = payload.get("feed", [])
    return pd.json_normalize(feed)
