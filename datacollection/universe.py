from __future__ import annotations

from io import StringIO
from pathlib import Path

import pandas as pd

from .config import data_root, sec_user_agent
from .http_client import HttpClient
from .storage import write_dataframe


SP500_WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SEC_TICKER_EXCHANGE_URL = "https://www.sec.gov/files/company_tickers_exchange.json"


def provider_symbol(symbol: str, provider: str) -> str:
    """Normalize S&P symbols for common provider conventions."""
    if provider in {"yahoo", "stooq"}:
        return symbol.replace(".", "-")
    if provider == "sec":
        return symbol.replace(".", "-").upper()
    return symbol.upper()


def _clean_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = (
        frame.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.strip("_")
    )
    return frame


def fetch_sp500_from_wikipedia(url: str = SP500_WIKIPEDIA_URL) -> pd.DataFrame:
    client = HttpClient(headers={"User-Agent": "Mozilla/5.0 SenQuantDataCollection/0.1"})
    html = client.get_text(url)
    tables = pd.read_html(StringIO(html))
    table = next(
        (
            _clean_columns(candidate)
            for candidate in tables
            if {"symbol", "security"}.issubset(set(_clean_columns(candidate).columns))
        ),
        None,
    )
    if table is None:
        raise RuntimeError("Could not find the S&P 500 constituents table on Wikipedia.")

    rename = {
        "security": "name",
        "gics_sector": "sector",
        "gics_sub_industry": "industry",
        "headquarters_location": "headquarters",
        "date_added": "date_added",
        "cik": "cik",
        "founded": "founded",
    }
    table = table.rename(columns=rename)
    keep = [
        column
        for column in (
            "symbol",
            "name",
            "sector",
            "industry",
            "headquarters",
            "date_added",
            "cik",
            "founded",
        )
        if column in table.columns
    ]
    table = table[keep].copy()
    table["symbol"] = table["symbol"].astype(str).str.strip().str.upper()
    if "cik" in table.columns:
        table["cik"] = table["cik"].astype(str).str.replace(r"\.0$", "", regex=True)
        table["cik"] = table["cik"].str.zfill(10)
    return table.sort_values("symbol").reset_index(drop=True)


def fetch_sec_ticker_map() -> pd.DataFrame:
    client = HttpClient(headers={"User-Agent": sec_user_agent(), "Accept-Encoding": "gzip, deflate"})
    payload = client.get_json(SEC_TICKER_EXCHANGE_URL)
    frame = pd.DataFrame(payload["data"], columns=payload["fields"])
    frame.columns = [column.lower() for column in frame.columns]
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    frame["sec_symbol_key"] = frame["ticker"].str.replace("-", ".", regex=False)
    frame["cik"] = frame["cik"].astype(str).str.zfill(10)
    return frame.rename(columns={"ticker": "sec_ticker", "name": "sec_name", "exchange": "sec_exchange"})


def build_sp500_universe(include_sec_exchange: bool = True) -> pd.DataFrame:
    sp500 = fetch_sp500_from_wikipedia()
    sp500["yahoo_symbol"] = sp500["symbol"].map(lambda value: provider_symbol(value, "yahoo"))
    sp500["alpha_vantage_symbol"] = sp500["symbol"].str.replace(".", "-", regex=False)
    sp500["sec_symbol_key"] = sp500["symbol"]

    if include_sec_exchange:
        sec_map = fetch_sec_ticker_map()
        sp500 = sp500.merge(
            sec_map[["sec_symbol_key", "sec_ticker", "sec_exchange"]],
            on="sec_symbol_key",
            how="left",
        )
    return sp500.drop(columns=["sec_symbol_key"]).reset_index(drop=True)


def save_sp500_universe(path: Path | None = None, include_sec_exchange: bool = True) -> Path:
    if path is None:
        path = data_root() / "universe" / "sp500_constituents.csv"
    return write_dataframe(path, build_sp500_universe(include_sec_exchange=include_sec_exchange))
