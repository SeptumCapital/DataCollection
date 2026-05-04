from __future__ import annotations

import json
import math
import mimetypes
import os
import re
import argparse
import time
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np

try:
    import yfinance as yf
except ImportError:  # pragma: no cover - app still runs without live news.
    yf = None


APP_ROOT = Path(__file__).resolve().parent
STATIC_ROOT = APP_ROOT / "static"
DATA_ROOT = Path(os.environ.get("SENQUANT_DATA_ROOT", APP_ROOT.parent / "data")).expanduser()
UNIVERSE_PATH = DATA_ROOT / "universe" / "sp500_constituents.csv"
REFRESH_MARKER_PATH = DATA_ROOT / ".senquant_refresh_complete"
TECHNICALS_DIR = DATA_ROOT / "technicals" / "from_yahoo_daily"
PRICES_DIR = DATA_ROOT / "prices" / "yahoo_daily"
FUNDAMENTALS_DIR = DATA_ROOT / "fundamentals" / "sec_companyfacts" / "long"
ENRICHMENT_DIR = DATA_ROOT / "enrichment" / "yahoo_quote_summary"
NEWS_DIR = DATA_ROOT / "news" / "yahoo"
MARKET_NEWS_PATH = NEWS_DIR / "market.json"
SOCIAL_DIR = DATA_ROOT / "social" / "twitter"
RECOMMENDATIONS_PATH = DATA_ROOT / "recommendations" / "local_quant_recommendations.json"
ADVANCED_RECOMMENDATIONS_PATH = DATA_ROOT / "recommendations" / "advanced_quant_recommendations.json"

SECTOR_NEWS_SYMBOLS = {
    "Communication Services": "XLC",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Financials": "XLF",
    "Health Care": "XLV",
    "Industrials": "XLI",
    "Information Technology": "XLK",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Utilities": "XLU",
}

DEFAULT_METRICS = ("adj_close", "volume", "return_21d", "rsi_14", "sma_50", "sma_200")
FUNDAMENTAL_METRICS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "eps_diluted",
    "assets",
    "liabilities",
    "stockholders_equity",
    "operating_cash_flow",
    "capex",
)


def safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def safe_int(value: object) -> int | None:
    result = safe_float(value)
    return int(result) if result is not None else None


def jsonable(value: object) -> object:
    if isinstance(value, np.generic):
        return jsonable(value.item())
    if isinstance(value, (pd.Timestamp, date)):
        return value.isoformat()
    if pd.isna(value):
        return None
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    return value


def safe_symbol(symbol: str) -> str:
    return symbol.upper().replace(".", "-")


def pct_change(first: float | None, last: float | None) -> float | None:
    if first in (None, 0) or last is None:
        return None
    return (last / first) - 1


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def table_rows(payload: dict[str, object], table: str) -> list[dict[str, object]]:
    tables = payload.get("tables", {})
    if not isinstance(tables, dict):
        return []
    rows = tables.get(table, [])
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def first_row(rows: list[dict[str, object]], key: str, value: str) -> dict[str, object]:
    for row in rows or []:
        if str(row.get(key, "")).lower() == value.lower():
            return row
    return {}


def insider_action(row: dict[str, object]) -> str:
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


def transaction_date(row: dict[str, object]) -> pd.Timestamp | None:
    raw = row.get("Start Date") or row.get("Transaction Start Date")
    parsed = pd.to_datetime(raw, errors="coerce")
    return None if pd.isna(parsed) else parsed


def explicit_insider_summary(
    rows: list[dict[str, object]],
    collected_at: object = None,
    days: int = 183,
) -> dict[str, object]:
    rows = rows or []
    reference = pd.to_datetime(collected_at, errors="coerce")
    if pd.isna(reference):
        dates = [transaction_date(row) for row in rows]
        dates = [value for value in dates if value is not None]
        reference = max(dates) if dates else pd.Timestamp.now("UTC")
    reference = pd.Timestamp(reference).tz_localize(None)
    cutoff = reference - pd.Timedelta(days=days)

    def is_recent(row: dict[str, object]) -> bool:
        parsed = transaction_date(row)
        return bool(parsed is not None and pd.Timestamp(parsed).tz_localize(None) >= cutoff)

    buys = [row for row in rows if insider_action(row) == "Buy" and is_recent(row)]
    sells = [row for row in rows if insider_action(row) == "Sell" and is_recent(row)]

    def total(column: str, selected: list[dict[str, object]]) -> float:
        values: list[float] = []
        for row in selected:
            value = safe_float(row.get(column))
            if value is not None:
                values.append(value)
        return sum(values)

    return {
        "explicit_insider_buy_flag": bool(buys),
        "explicit_insider_buy_count": len(buys),
        "explicit_insider_sell_count": len(sells),
        "explicit_insider_buy_shares": total("Shares", buys),
        "explicit_insider_sell_shares": total("Shares", sells),
        "explicit_insider_buy_value": total("Value", buys),
        "explicit_insider_sell_value": total("Value", sells),
    }


def recent_explicit_insider_rows(
    rows: list[dict[str, object]],
    collected_at: object = None,
    days: int = 183,
) -> list[dict[str, object]]:
    rows = rows or []
    reference = pd.to_datetime(collected_at, errors="coerce")
    if pd.isna(reference):
        dates = [transaction_date(row) for row in rows]
        dates = [value for value in dates if value is not None]
        reference = max(dates) if dates else pd.Timestamp.now("UTC")
    reference = pd.Timestamp(reference).tz_localize(None)
    cutoff = reference - pd.Timedelta(days=days)

    enriched: list[dict[str, object]] = []
    for row in rows:
        action = insider_action(row)
        parsed = transaction_date(row)
        if action not in {"Buy", "Sell"} or parsed is None:
            continue
        parsed = pd.Timestamp(parsed).tz_localize(None)
        if parsed < cutoff:
            continue
        enriched.append({**row, "action": action, "_parsed_date": parsed})

    enriched.sort(key=lambda row: (0 if row["action"] == "Buy" else 1, -row["_parsed_date"].timestamp()))
    return [{key: value for key, value in row.items() if key != "_parsed_date"} for row in enriched]


def summarize_enrichment_payload(payload: dict[str, object]) -> dict[str, object]:
    major = {
        row.get("Breakdown") or row.get("index"): row.get("Value")
        for row in table_rows(payload, "major_holders")
        if isinstance(row, dict)
    }
    explicit = explicit_insider_summary(table_rows(payload, "insider_transactions"), payload.get("collected_at"))
    rec_rows = table_rows(payload, "recommendations_summary")
    current_rec = first_row(rec_rows, "period", "0m") or (rec_rows[0] if rec_rows else {})

    rec_score = None
    rec_total = 0
    if current_rec:
        weights = {"strongBuy": 1, "buy": 2, "hold": 3, "sell": 4, "strongSell": 5}
        numerator = 0.0
        for column, weight in weights.items():
            count = safe_int(current_rec.get(column)) or 0
            numerator += count * weight
            rec_total += count
        if rec_total:
            rec_score = numerator / rec_total

    targets = payload.get("analyst_price_targets", {})
    targets = targets if isinstance(targets, dict) else {}
    earnings = (table_rows(payload, "earnings_estimate") or [{}])[0]
    revenue = (table_rows(payload, "revenue_estimate") or [{}])[0]

    return {
        "has_enrichment": True,
        "institutions_percent_held": safe_float(major.get("institutionsPercentHeld")),
        "insiders_percent_held": safe_float(major.get("insidersPercentHeld")),
        "institutional_holders_count": len(table_rows(payload, "institutional_holders")),
        "insider_buy_flag": explicit["explicit_insider_buy_flag"],
        "insider_purchase_shares_6m": explicit["explicit_insider_buy_shares"],
        "insider_purchase_transactions_6m": explicit["explicit_insider_buy_count"],
        "insider_sale_shares_6m": explicit["explicit_insider_sell_shares"],
        "insider_sale_transactions_6m": explicit["explicit_insider_sell_count"],
        **explicit,
        "analyst_rating_score": rec_score,
        "analyst_rating_count": rec_total or None,
        "price_target_mean": safe_float(targets.get("mean")),
        "price_target_high": safe_float(targets.get("high")),
        "price_target_low": safe_float(targets.get("low")),
        "eps_estimate_current_q": safe_float(earnings.get("avg")),
        "eps_estimate_current_q_analysts": safe_int(earnings.get("numberOfAnalysts")),
        "revenue_estimate_current_q": safe_float(revenue.get("avg")),
        "revenue_estimate_current_q_analysts": safe_int(revenue.get("numberOfAnalysts")),
    }


def normalize_news_item(item: dict[str, object]) -> dict[str, object]:
    content = item.get("content") if isinstance(item.get("content"), dict) else {}
    title = content.get("title") or item.get("title")
    summary = content.get("summary") or item.get("summary")
    provider = content.get("provider") if isinstance(content.get("provider"), dict) else {}
    canonical = content.get("canonicalUrl") if isinstance(content.get("canonicalUrl"), dict) else {}
    thumbnail = content.get("thumbnail") if isinstance(content.get("thumbnail"), dict) else {}
    resolutions = thumbnail.get("resolutions") if isinstance(thumbnail.get("resolutions"), list) else []
    publish_time = content.get("pubDate") or item.get("providerPublishTime")
    if isinstance(publish_time, (int, float)):
        publish_time = pd.to_datetime(publish_time, unit="s", utc=True).isoformat()
    return {
        "title": title,
        "summary": summary,
        "publisher": provider.get("displayName") or item.get("publisher"),
        "url": canonical.get("url") or item.get("link"),
        "published_at": publish_time,
        "thumbnail": (resolutions[0].get("url") if resolutions and isinstance(resolutions[0], dict) else None),
    }


def fetch_or_load_news(symbol: str, max_age_seconds: int = 3600) -> dict[str, object]:
    key = safe_symbol(symbol)
    path = NEWS_DIR / f"{key}.json"
    if path.exists() and time.time() - path.stat().st_mtime < max_age_seconds:
        return load_json(path)

    if yf is None:
        return {"symbol": symbol.upper(), "provider": "yfinance/yahoo", "items": [], "error": "yfinance is not installed"}

    try:
        items = yf.Ticker(key).news or []
        payload = {
            "symbol": symbol.upper(),
            "provider": "yfinance/yahoo",
            "collected_at": pd.Timestamp.now("UTC").isoformat(),
            "items": [normalize_news_item(item) for item in items[:20] if isinstance(item, dict)],
        }
        write_json(path, payload)
        return payload
    except Exception as exc:  # noqa: BLE001
        if path.exists():
            payload = load_json(path)
            payload["stale_error"] = str(exc)
            return payload
        return {"symbol": symbol.upper(), "provider": "yfinance/yahoo", "items": [], "error": str(exc)}


def fetch_or_load_market_news(max_age_seconds: int = 900) -> dict[str, object]:
    if MARKET_NEWS_PATH.exists() and time.time() - MARKET_NEWS_PATH.stat().st_mtime < max_age_seconds:
        return load_json(MARKET_NEWS_PATH)

    if yf is None:
        return {"provider": "yfinance/yahoo", "items": [], "error": "yfinance is not installed"}

    seen: set[str] = set()
    items: list[dict[str, object]] = []
    for symbol in ("^GSPC", "^DJI", "^IXIC", "^RUT"):
        try:
            for item in yf.Ticker(symbol).news or []:
                if not isinstance(item, dict):
                    continue
                normalized = normalize_news_item(item)
                key = str(normalized.get("url") or normalized.get("title") or "")
                if not key or key in seen:
                    continue
                seen.add(key)
                items.append(normalized)
        except Exception:
            continue

    items.sort(key=lambda item: str(item.get("published_at") or ""), reverse=True)
    payload = {
        "provider": "yfinance/yahoo",
        "collected_at": pd.Timestamp.now("UTC").isoformat(),
        "items": items[:30],
    }
    write_json(MARKET_NEWS_PATH, payload)
    return payload


def fetch_sector_news(sector: str, max_age_seconds: int = 1800) -> dict[str, object]:
    symbol = SECTOR_NEWS_SYMBOLS.get(sector)
    if not symbol:
        return {"sector": sector, "provider": "not_configured", "items": []}
    path = NEWS_DIR / "sectors" / f"{safe_symbol(sector)}.json"
    if path.exists() and time.time() - path.stat().st_mtime < max_age_seconds:
        return load_json(path)
    if yf is None:
        return {"sector": sector, "symbol": symbol, "provider": "yfinance/yahoo", "items": [], "error": "yfinance is not installed"}
    try:
        payload = {
            "sector": sector,
            "symbol": symbol,
            "provider": "yfinance/yahoo",
            "collected_at": pd.Timestamp.now("UTC").isoformat(),
            "items": [
                normalize_news_item(item)
                for item in (yf.Ticker(symbol).news or [])[:20]
                if isinstance(item, dict)
            ],
        }
        write_json(path, payload)
        return payload
    except Exception as exc:  # noqa: BLE001
        if path.exists():
            payload = load_json(path)
            payload["stale_error"] = str(exc)
            return payload
        return {"sector": sector, "symbol": symbol, "provider": "yfinance/yahoo", "items": [], "error": str(exc)}


def load_social_posts(symbol: str) -> dict[str, object]:
    key = safe_symbol(symbol)
    json_path = SOCIAL_DIR / f"{key}.json"
    csv_path = SOCIAL_DIR / f"{key}.csv"
    if json_path.exists():
        payload = load_json(json_path)
        posts = payload.get("posts", payload.get("items", []))
        return {"symbol": symbol.upper(), "provider": payload.get("provider", "local"), "posts": posts}
    if csv_path.exists():
        frame = pd.read_csv(csv_path)
        return {"symbol": symbol.upper(), "provider": "local_csv", "posts": frame.to_dict("records")}
    return {
        "symbol": symbol.upper(),
        "provider": "not_configured",
        "posts": [],
        "message": "Add data/social/twitter/SYMBOL.json or SYMBOL.csv with popular X posts, or connect an X API collector.",
    }


@dataclass
class DataStore:
    universe: pd.DataFrame
    stocks: pd.DataFrame
    sectors: list[str]
    exchanges: list[str]
    available_symbols: set[str]
    generated_at: str

    @classmethod
    def empty(cls) -> "DataStore":
        columns = [
            "symbol",
            "name",
            "sector",
            "industry",
            "exchange",
            "has_prices",
            "has_technicals",
            "has_fundamentals",
            "has_enrichment",
            "insider_buy_flag",
            "at_52w_high",
            "at_52w_low",
            "above_sma_200",
            "below_sma_200",
        ]
        return cls(
            universe=pd.DataFrame(columns=["symbol", "sector", "sec_exchange"]),
            stocks=pd.DataFrame(columns=columns),
            sectors=[],
            exchanges=[],
            available_symbols=set(),
            generated_at=pd.Timestamp.now("UTC").isoformat(),
        )

    @classmethod
    def load(cls) -> "DataStore":
        if not UNIVERSE_PATH.exists():
            return cls.empty()

        universe = pd.read_csv(UNIVERSE_PATH, dtype={"cik": str}).fillna("")
        universe["symbol_key"] = universe["symbol"].map(safe_symbol)

        available_symbols = {path.stem.upper() for path in TECHNICALS_DIR.glob("*.csv")}
        rows: list[dict[str, object]] = []
        for record in universe.to_dict("records"):
            symbol = record["symbol"]
            key = safe_symbol(symbol)
            tech_file = TECHNICALS_DIR / f"{key}.csv"
            price_file = PRICES_DIR / f"{key}.csv"
            source_file = tech_file if tech_file.exists() else price_file

            row = {
                "symbol": symbol,
                "name": record.get("name", ""),
                "sector": record.get("sector", ""),
                "industry": record.get("industry", ""),
                "exchange": record.get("sec_exchange", ""),
                "headquarters": record.get("headquarters", ""),
                "date_added": record.get("date_added", ""),
                "has_prices": price_file.exists(),
                "has_technicals": tech_file.exists(),
                "has_fundamentals": (FUNDAMENTALS_DIR / f"{key}.csv").exists(),
                "has_enrichment": (ENRICHMENT_DIR / f"{key}.json").exists(),
                "institutions_percent_held": None,
                "insiders_percent_held": None,
                "institutional_holders_count": None,
                "insider_buy_flag": False,
                "insider_purchase_shares_6m": None,
                "insider_sale_shares_6m": None,
                "analyst_rating_score": None,
                "analyst_rating_count": None,
                "price_target_mean": None,
                "eps_estimate_current_q": None,
                "revenue_estimate_current_q": None,
                "above_sma_200": False,
                "below_sma_200": False,
                "at_52w_high": False,
                "at_52w_low": False,
                "high_52w": None,
                "low_52w": None,
                "distance_from_52w_high": None,
                "distance_from_52w_low": None,
            }
            enrichment_file = ENRICHMENT_DIR / f"{key}.json"
            if enrichment_file.exists():
                try:
                    row.update(summarize_enrichment_payload(load_json(enrichment_file)))
                except Exception as exc:  # noqa: BLE001
                    row["enrichment_error"] = str(exc)

            if source_file.exists():
                try:
                    series = pd.read_csv(
                        source_file,
                        usecols=lambda column: column
                        in {
                            "date",
                            "adj_close",
                            "close",
                            "volume",
                            "return_1d",
                            "return_21d",
                            "return_5d",
                            "rsi_14",
                            "sma_50",
                            "sma_200",
                            "volatility_21d",
                        },
                    )
                    series["date"] = pd.to_datetime(series["date"], errors="coerce")
                    series = series.dropna(subset=["date"]).sort_values("date")
                    if not series.empty:
                        close_col = "adj_close" if "adj_close" in series else "close"
                        last = series.iloc[-1]
                        last_close = safe_float(last.get(close_col))
                        row.update(
                            {
                                "last_date": last["date"].date().isoformat(),
                                "last_close": last_close,
                                "volume": safe_int(last.get("volume")),
                                "return_1d": safe_float(last.get("return_1d")),
                                "return_5d": safe_float(last.get("return_5d")),
                                "return_21d": safe_float(last.get("return_21d")),
                                "rsi_14": safe_float(last.get("rsi_14")),
                                "sma_50": safe_float(last.get("sma_50")),
                                "sma_200": safe_float(last.get("sma_200")),
                                "volatility_21d": safe_float(last.get("volatility_21d")),
                            }
                        )
                        close = pd.to_numeric(series[close_col], errors="coerce")
                        trailing_year = series[series["date"] >= (series["date"].max() - pd.Timedelta(days=365))]
                        trailing_close = pd.to_numeric(trailing_year[close_col], errors="coerce").dropna()
                        high_52w = safe_float(trailing_close.max()) if not trailing_close.empty else None
                        low_52w = safe_float(trailing_close.min()) if not trailing_close.empty else None
                        sma_200 = safe_float(last.get("sma_200"))
                        distance_from_high = pct_change(high_52w, last_close)
                        distance_from_low = pct_change(low_52w, last_close)
                        row.update(
                            {
                                "above_sma_200": bool(
                                    last_close is not None and sma_200 is not None and last_close >= sma_200
                                ),
                                "below_sma_200": bool(
                                    last_close is not None and sma_200 is not None and last_close < sma_200
                                ),
                                "high_52w": high_52w,
                                "low_52w": low_52w,
                                "distance_from_52w_high": distance_from_high,
                                "distance_from_52w_low": distance_from_low,
                                "at_52w_high": bool(distance_from_high is not None and distance_from_high >= -0.02),
                                "at_52w_low": bool(distance_from_low is not None and distance_from_low <= 0.02),
                            }
                        )
                        row["return_1y"] = pct_change(
                            safe_float(close[series["date"] >= (series["date"].max() - pd.Timedelta(days=365))].dropna().iloc[0])
                            if not close[series["date"] >= (series["date"].max() - pd.Timedelta(days=365))].dropna().empty
                            else None,
                            last_close,
                        )
                except Exception as exc:  # noqa: BLE001 - bad files should not prevent the app from opening.
                    row["load_error"] = str(exc)
            rows.append(row)

        stocks = pd.DataFrame(rows)
        sectors = sorted(value for value in universe["sector"].dropna().unique().tolist() if value)
        exchanges = sorted(value for value in universe.get("sec_exchange", pd.Series(dtype=str)).dropna().unique().tolist() if value)
        return cls(
            universe=universe,
            stocks=stocks,
            sectors=sectors,
            exchanges=exchanges,
            available_symbols=available_symbols,
            generated_at=pd.Timestamp.now("UTC").isoformat(),
        )

    def filter_stocks(self, query: dict[str, list[str]]) -> dict[str, object]:
        frame = self.stocks.copy()
        search = query.get("q", [""])[0].strip().lower()
        sector = query.get("sector", [""])[0]
        exchange = query.get("exchange", [""])[0]
        has_data = query.get("hasData", [""])[0]
        sort = query.get("sort", ["symbol"])[0]
        direction = query.get("direction", ["asc"])[0]
        limit = int(query.get("limit", ["100"])[0] or 100)

        if search:
            mask = (
                frame["symbol"].astype(str).str.lower().str.contains(search, regex=False)
                | frame["name"].astype(str).str.lower().str.contains(search, regex=False)
                | frame["industry"].astype(str).str.lower().str.contains(search, regex=False)
            )
            frame = frame[mask]
        if sector:
            frame = frame[frame["sector"] == sector]
        if exchange:
            frame = frame[frame["exchange"] == exchange]
        if has_data == "prices":
            frame = frame[frame["has_prices"]]
        if has_data == "fundamentals":
            frame = frame[frame["has_fundamentals"]]
        if has_data == "enrichment":
            frame = frame[frame["has_enrichment"]]
        if query.get("insiderBuy", [""])[0] == "true":
            frame = frame[frame["insider_buy_flag"].fillna(False).astype(bool)]
        condition = query.get("condition", [""])[0]
        if condition == "52w_high":
            frame = frame[frame["at_52w_high"].fillna(False).astype(bool)]
        elif condition == "52w_low":
            frame = frame[frame["at_52w_low"].fillna(False).astype(bool)]
        elif condition == "above_200d":
            frame = frame[frame["above_sma_200"].fillna(False).astype(bool)]
        elif condition == "below_200d":
            frame = frame[frame["below_sma_200"].fillna(False).astype(bool)]

        for column, op, value in (
            ("last_close", "min", query.get("priceMin", [""])[0]),
            ("last_close", "max", query.get("priceMax", [""])[0]),
            ("return_21d", "min", query.get("return21Min", [""])[0]),
            ("return_21d", "max", query.get("return21Max", [""])[0]),
            ("rsi_14", "min", query.get("rsiMin", [""])[0]),
            ("rsi_14", "max", query.get("rsiMax", [""])[0]),
            ("institutions_percent_held", "min", query.get("instMin", [""])[0]),
            ("analyst_rating_score", "max", query.get("ratingMax", [""])[0]),
        ):
            numeric = safe_float(value)
            if numeric is None or column not in frame.columns:
                continue
            if op == "min":
                frame = frame[pd.to_numeric(frame[column], errors="coerce") >= numeric]
            else:
                frame = frame[pd.to_numeric(frame[column], errors="coerce") <= numeric]

        if sort in frame.columns:
            frame = frame.sort_values(sort, ascending=direction != "desc", na_position="last")

        return {
            "total": int(len(frame)),
            "rows": [
                {key: jsonable(value) for key, value in record.items() if key != "symbol_key"}
                for record in frame.head(limit).to_dict("records")
            ],
        }

    def momentum_recommendations(self, limit: int = 10) -> dict[str, object]:
        global MOMENTUM_CACHE
        if (
            MOMENTUM_CACHE["store_loaded_at"] == STORE_LOADED_AT
            and MOMENTUM_CACHE["limit"] == limit
            and MOMENTUM_CACHE["payload"] is not None
        ):
            return MOMENTUM_CACHE["payload"]

        rows: list[dict[str, object]] = []
        universe_meta = {
            safe_symbol(row.get("symbol", "")): row
            for row in self.universe.to_dict("records")
            if row.get("symbol")
        }

        for path in sorted(TECHNICALS_DIR.glob("*.csv")):
            try:
                frame = pd.read_csv(path, usecols=lambda column: column in {"date", "adj_close", "close", "sma_200"})
                frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
                frame = frame.dropna(subset=["date"]).sort_values("date")
                if len(frame) < 252:
                    continue
                close_col = "adj_close" if "adj_close" in frame.columns else "close"
                close = pd.to_numeric(frame[close_col], errors="coerce")
                valid = frame.assign(_close=close).dropna(subset=["_close"])
                if len(valid) < 252:
                    continue
                latest = valid.iloc[-1]
                latest_close = safe_float(latest["_close"])
                sma_200 = safe_float(latest.get("sma_200"))
                if latest_close is None or sma_200 in (None, 0):
                    continue

                def trailing_return(days: int) -> float | None:
                    if len(valid) <= days:
                        return None
                    start_value = safe_float(valid.iloc[-days - 1]["_close"])
                    return pct_change(start_value, latest_close)

                return_12m = trailing_return(252)
                if return_12m is None or return_12m < -0.95 or return_12m > 5:
                    continue
                key = path.stem.upper()
                meta = universe_meta.get(key, {})
                rows.append(
                    {
                        "symbol": meta.get("symbol") or key,
                        "name": meta.get("name", ""),
                        "sector": meta.get("sector", ""),
                        "industry": meta.get("industry", ""),
                        "last_date": latest["date"].date().isoformat(),
                        "last_close": latest_close,
                        "return_1m": trailing_return(21),
                        "return_3m": trailing_return(63),
                        "return_12m": return_12m,
                        "sma_200": sma_200,
                        "distance_from_sma_200": pct_change(sma_200, latest_close),
                    }
                )
            except Exception:
                continue

        rows.sort(key=lambda row: row["return_12m"], reverse=True)
        ranked = []
        for index, row in enumerate(rows[:limit], start=1):
            ranked.append({"rank": index, **{key: jsonable(value) for key, value in row.items()}})
        payload = {
            "model": "12-month cross-sectional momentum",
            "universe": "S&P 500",
            "as_of": ranked[0]["last_date"] if ranked else None,
            "rows": ranked,
        }
        MOMENTUM_CACHE = {"store_loaded_at": STORE_LOADED_AT, "limit": limit, "payload": payload}
        return payload

    def group_momentum_leaders(self, limit: int = 3) -> dict[str, object]:
        global GROUP_MOMENTUM_CACHE
        if (
            GROUP_MOMENTUM_CACHE["store_loaded_at"] == STORE_LOADED_AT
            and GROUP_MOMENTUM_CACHE["payload"] is not None
        ):
            payload = GROUP_MOMENTUM_CACHE["payload"]
            return {
                **payload,
                "periods": {
                    period: {
                        "sectors": values["sectors"][:limit],
                        "industries": values["industries"][:limit],
                    }
                    for period, values in payload["periods"].items()
                },
            }

        periods = {
            "1W": 5,
            "1M": 21,
            "3M": 63,
            "1Y": 252,
        }
        stock_rows: list[dict[str, object]] = []
        universe_meta = {
            safe_symbol(row.get("symbol", "")): row
            for row in self.universe.to_dict("records")
            if row.get("symbol")
        }

        for path in sorted(TECHNICALS_DIR.glob("*.csv")):
            try:
                frame = pd.read_csv(path, usecols=lambda column: column in {"date", "adj_close", "close"})
                frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
                frame = frame.dropna(subset=["date"]).sort_values("date")
                close_col = "adj_close" if "adj_close" in frame.columns else "close"
                close = pd.to_numeric(frame[close_col], errors="coerce")
                valid = frame.assign(_close=close).dropna(subset=["_close"])
                if len(valid) <= max(periods.values()):
                    continue
                latest_close = safe_float(valid.iloc[-1]["_close"])
                if latest_close is None:
                    continue
                key = path.stem.upper()
                meta = universe_meta.get(key, {})
                row = {
                    "symbol": meta.get("symbol") or key,
                    "sector": meta.get("sector", ""),
                    "industry": meta.get("industry", ""),
                    "last_date": valid.iloc[-1]["date"].date().isoformat(),
                }
                for label, days in periods.items():
                    start_value = safe_float(valid.iloc[-days - 1]["_close"])
                    value = pct_change(start_value, latest_close)
                    row[label] = value if value is not None and -0.95 <= value <= 5 else None
                stock_rows.append(row)
            except Exception:
                continue

        def leaders(group_key: str, period: str) -> list[dict[str, object]]:
            frame = pd.DataFrame(stock_rows)
            if frame.empty or group_key not in frame.columns:
                return []
            frame = frame.dropna(subset=[period])
            if frame.empty:
                return []
            grouped = (
                frame.groupby(group_key, dropna=True)
                .agg(
                    momentum=(period, "median"),
                    stock_count=(period, "count"),
                    leaders=("symbol", lambda values: ", ".join(list(values)[:3])),
                )
                .reset_index()
            )
            grouped = grouped[grouped[group_key].astype(str).str.len() > 0]
            grouped = grouped[grouped["stock_count"] >= 3]
            grouped = grouped.sort_values("momentum", ascending=False).head(10)
            return [
                {
                    "name": row[group_key],
                    "momentum": jsonable(row["momentum"]),
                    "stock_count": int(row["stock_count"]),
                    "sample_symbols": row["leaders"],
                }
                for row in grouped.to_dict("records")
            ]

        payload = {
            "model": "median constituent return by group",
            "as_of": max((row["last_date"] for row in stock_rows), default=None),
            "periods": {
                period: {
                    "sectors": leaders("sector", period),
                    "industries": leaders("industry", period),
                }
                for period in periods
            },
        }
        GROUP_MOMENTUM_CACHE = {"store_loaded_at": STORE_LOADED_AT, "payload": payload}
        return {
            **payload,
            "periods": {
                period: {
                    "sectors": values["sectors"][:limit],
                    "industries": values["industries"][:limit],
                }
                for period, values in payload["periods"].items()
            },
        }

    def sector_detail(self, sector: str) -> dict[str, object]:
        members = self.stocks[self.stocks["sector"] == sector].copy()
        if members.empty:
            return {"sector": sector, "error": f"No stocks found for sector: {sector}"}

        periods = {"1W": 5, "1M": 21, "3M": 63, "1Y": 252}
        return_rows: list[dict[str, object]] = []
        for row in members.to_dict("records"):
            key = safe_symbol(str(row.get("symbol", "")))
            path = TECHNICALS_DIR / f"{key}.csv"
            if not path.exists():
                continue
            try:
                frame = pd.read_csv(path, usecols=lambda column: column in {"date", "adj_close", "close", "sma_200", "rsi_14"})
                frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
                frame = frame.dropna(subset=["date"]).sort_values("date")
                close_col = "adj_close" if "adj_close" in frame.columns else "close"
                close = pd.to_numeric(frame[close_col], errors="coerce")
                valid = frame.assign(_close=close).dropna(subset=["_close"])
                if len(valid) <= 5:
                    continue
                latest = valid.iloc[-1]
                latest_close = safe_float(latest["_close"])
                if latest_close is None:
                    continue

                item = {
                    "symbol": row.get("symbol"),
                    "name": row.get("name"),
                    "industry": row.get("industry"),
                    "last_date": latest["date"].date().isoformat(),
                    "last_close": latest_close,
                    "sma_200": safe_float(latest.get("sma_200")),
                    "rsi_14": safe_float(latest.get("rsi_14")),
                }
                item["distance_from_sma_200"] = pct_change(item["sma_200"], latest_close)
                for label, days in periods.items():
                    if len(valid) <= days:
                        item[label] = None
                        continue
                    start_value = safe_float(valid.iloc[-days - 1]["_close"])
                    value = pct_change(start_value, latest_close)
                    item[label] = value if value is not None and -0.95 <= value <= 5 else None
                return_rows.append(item)
            except Exception:
                continue

        frame = pd.DataFrame(return_rows)

        def median_value(column: str) -> float | None:
            if frame.empty or column not in frame.columns:
                return None
            values = pd.to_numeric(frame[column], errors="coerce").dropna()
            return safe_float(values.median()) if not values.empty else None

        performance = {
            "return_1w": median_value("1W"),
            "return_1m": median_value("1M"),
            "return_3m": median_value("3M"),
            "return_1y": median_value("1Y"),
            "median_rsi_14": median_value("rsi_14"),
            "median_distance_from_sma_200": median_value("distance_from_sma_200"),
            "above_sma_200_pct": safe_float(
                (pd.to_numeric(frame["distance_from_sma_200"], errors="coerce") > 0).mean()
            )
            if not frame.empty and "distance_from_sma_200" in frame.columns
            else None,
        }
        leaders = frame.sort_values("1M", ascending=False, na_position="last").head(5).to_dict("records") if not frame.empty else []
        laggards = frame.sort_values("1M", ascending=True, na_position="last").head(5).to_dict("records") if not frame.empty else []
        member_rows = [
            {key: jsonable(value) for key, value in record.items() if key != "symbol_key"}
            for record in members.sort_values("symbol").to_dict("records")
        ]
        return {
            "sector": sector,
            "stock_count": int(len(members)),
            "as_of": max((row.get("last_date") for row in return_rows), default=None),
            "performance": {key: jsonable(value) for key, value in performance.items()},
            "leaders_1m": [{key: jsonable(value) for key, value in row.items()} for row in leaders],
            "laggards_1m": [{key: jsonable(value) for key, value in row.items()} for row in laggards],
            "members": member_rows,
        }

    def enrichment(self, symbol: str) -> dict[str, object]:
        key = safe_symbol(symbol)
        path = ENRICHMENT_DIR / f"{key}.json"
        if not path.exists():
            return {"symbol": symbol.upper(), "has_enrichment": False}
        payload = load_json(path)
        summary = summarize_enrichment_payload(payload)
        transactions = recent_explicit_insider_rows(
            table_rows(payload, "insider_transactions"),
            payload.get("collected_at"),
        )
        institutions = table_rows(payload, "institutional_holders")[:20]
        funds = table_rows(payload, "mutualfund_holders")[:10]
        return {
            "symbol": symbol.upper(),
            "has_enrichment": True,
            "summary": {key: jsonable(value) for key, value in summary.items()},
            "major_holders": table_rows(payload, "major_holders"),
            "institutional_holders": institutions,
            "mutualfund_holders": funds,
            "insider_purchases": table_rows(payload, "insider_purchases"),
            "insider_transactions": transactions[:80],
            "insider_buy_transactions": [row for row in transactions if row["action"] == "Buy"],
            "insider_sell_transactions": [row for row in transactions if row["action"] == "Sell"],
            "recommendations_summary": table_rows(payload, "recommendations_summary"),
            "earnings_estimate": table_rows(payload, "earnings_estimate"),
            "revenue_estimate": table_rows(payload, "revenue_estimate"),
            "eps_trend": table_rows(payload, "eps_trend"),
            "eps_revisions": table_rows(payload, "eps_revisions"),
            "growth_estimates": table_rows(payload, "growth_estimates"),
            "analyst_price_targets": payload.get("analyst_price_targets", {}),
            "collected_at": payload.get("collected_at"),
        }

    def stock_detail(self, symbol: str, query: dict[str, list[str]]) -> dict[str, object]:
        key = safe_symbol(symbol)
        interval = query.get("interval", ["daily"])[0]
        range_name = query.get("range", ["1y"])[0]
        metrics = [metric for metric in query.get("metrics", [",".join(DEFAULT_METRICS)])[0].split(",") if metric]
        path = TECHNICALS_DIR / f"{key}.csv"
        if not path.exists():
            path = PRICES_DIR / f"{key}.csv"
        if not path.exists():
            raise FileNotFoundError(f"No price or technical data found for {symbol}")

        frame = pd.read_csv(path)
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame = frame.dropna(subset=["date"]).sort_values("date")
        frame = apply_range(frame, range_name)
        frame = resample_frame(frame, interval)
        columns = ["date", *[metric for metric in metrics if metric in frame.columns]]
        frame = frame[columns].copy()
        frame["date"] = frame["date"].dt.date.astype(str)

        meta = self.stocks[self.stocks["symbol"].map(safe_symbol) == key]
        return {
            "symbol": symbol.upper(),
            "meta": {} if meta.empty else {k: jsonable(v) for k, v in meta.iloc[0].to_dict().items()},
            "interval": interval,
            "range": range_name,
            "metrics": [metric for metric in metrics if metric in frame.columns],
            "series": [{k: jsonable(v) for k, v in row.items()} for row in frame.to_dict("records")],
        }

    def fundamentals(self, symbol: str, query: dict[str, list[str]]) -> dict[str, object]:
        key = safe_symbol(symbol)
        path = FUNDAMENTALS_DIR / f"{key}.csv"
        if not path.exists():
            return {"symbol": symbol.upper(), "metrics": [], "series": []}

        metric = query.get("metric", ["revenue"])[0]
        form = query.get("form", ["10-K"])[0]
        frame = pd.read_csv(path)
        frame = frame[frame["metric"].isin(FUNDAMENTAL_METRICS)]
        if metric:
            frame = frame[frame["metric"] == metric]
        if form:
            frame = frame[frame["form"] == form]
        frame["end"] = pd.to_datetime(frame["end"], errors="coerce")
        frame["filed"] = pd.to_datetime(frame["filed"], errors="coerce")
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        frame = frame.dropna(subset=["end", "value"])
        frame = frame.sort_values(["metric", "end", "filed"]).drop_duplicates(["metric", "end"], keep="last")
        frame = frame.sort_values("end").tail(80)
        frame["end"] = frame["end"].dt.date.astype(str)
        frame["filed"] = frame["filed"].dt.date.astype(str)
        return {
            "symbol": symbol.upper(),
            "available_metrics": sorted(pd.read_csv(path, usecols=["metric"])["metric"].dropna().unique().tolist()),
            "metric": metric,
            "series": [
                {key: jsonable(value) for key, value in row.items()}
                for row in frame[["end", "metric", "value", "unit", "fy", "fp", "form", "filed"]].to_dict("records")
            ],
        }


def apply_range(frame: pd.DataFrame, range_name: str) -> pd.DataFrame:
    if frame.empty or range_name == "max":
        return frame
    days = {"1m": 31, "3m": 93, "6m": 186, "1y": 366, "3y": 366 * 3, "5y": 366 * 5}.get(range_name)
    if days is None:
        return frame
    cutoff = frame["date"].max() - pd.Timedelta(days=days)
    return frame[frame["date"] >= cutoff]


def resample_frame(frame: pd.DataFrame, interval: str) -> pd.DataFrame:
    if interval == "daily" or frame.empty:
        return frame
    rule = {"weekly": "W-FRI", "monthly": "ME", "yearly": "YE"}.get(interval, "D")
    numeric_cols = [col for col in frame.columns if col not in {"symbol", "provider_symbol", "date"}]
    numeric = frame.set_index("date")[numeric_cols].apply(pd.to_numeric, errors="coerce")
    agg = {col: "last" for col in numeric_cols}
    if "volume" in agg:
        agg["volume"] = "sum"
    return numeric.resample(rule).agg(agg).dropna(how="all").reset_index()


STORE = DataStore.load()
STORE_LOADED_AT = time.time()
STORE_RELOAD_CHECKED_AT = 0.0
MOMENTUM_CACHE: dict[str, object] = {"store_loaded_at": 0.0, "limit": 0, "payload": None}
GROUP_MOMENTUM_CACHE: dict[str, object] = {"store_loaded_at": 0.0, "payload": None}
RECOMMENDATION_CACHE: dict[str, object] = {"store_loaded_at": 0.0, "payload": None}
ADVANCED_RECOMMENDATION_CACHE: dict[str, object] = {"store_loaded_at": 0.0, "payload": None}
RECOMMENDATION_BUILD_LOCK = threading.Lock()
ADVANCED_RECOMMENDATION_BUILD_LOCK = threading.Lock()
ADVANCED_RECOMMENDATION_DELAY_STATE: dict[str, bool] = {"scheduled": False}
CHAT_SESSION_MEMORY: dict[str, dict[str, object]] = {}
EXTERNAL_LLM_LAST_ERROR: dict[str, str | None] = {"message": None}

CHAT_ROW_COLUMNS = [
    "rank",
    "signal",
    "confidence",
    "symbol",
    "name",
    "sector",
    "industry",
    "last_close",
    "quant_score",
    "ml_expected_21d",
    "momentum_12_1",
    "return_1m",
    "return_3m",
    "return_21d",
    "return_1y",
    "rsi_14",
    "distance_from_sma_200",
    "institutions_percent_held",
    "analyst_rating_score",
    "price_target_mean",
    "target_upside",
    "reason",
]


def chat_value(value: object) -> object:
    return jsonable(value)


def chat_rows(frame: pd.DataFrame, limit: int = 8) -> list[dict[str, object]]:
    if frame.empty:
        return []
    frame = frame.copy()
    if "distance_from_sma_200" not in frame.columns and {"last_close", "sma_200"}.issubset(frame.columns):
        close = pd.to_numeric(frame["last_close"], errors="coerce")
        sma = pd.to_numeric(frame["sma_200"], errors="coerce")
        frame["distance_from_sma_200"] = (close / sma) - 1
    columns = [column for column in CHAT_ROW_COLUMNS if column in frame.columns]
    return [
        {key: chat_value(value) for key, value in record.items()}
        for record in frame[columns].head(limit).to_dict("records")
    ]


def chat_actions_from_rows(rows: list[dict[str, object]]) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    for row in rows[:5]:
        symbol = str(row.get("symbol") or "")
        if symbol:
            actions.append({"type": "stock", "value": symbol, "label": f"Open {symbol}"})
    return actions


def chat_find_symbols(store: DataStore, question: str, context: dict[str, object]) -> list[str]:
    symbol_map = {safe_symbol(str(row.get("symbol", ""))): str(row.get("symbol", "")) for row in store.stocks.to_dict("records")}
    stopwords = {
        "a",
        "an",
        "and",
        "are",
        "best",
        "by",
        "for",
        "from",
        "in",
        "is",
        "me",
        "of",
        "on",
        "or",
        "show",
        "stock",
        "stocks",
        "the",
        "to",
        "top",
        "what",
        "with",
        "buy",
        "sell",
        "hold",
        "recommend",
        "recommendation",
        "recommendations",
    }
    found: list[str] = []
    for token in re.findall(r"\b[A-Za-z][A-Za-z.-]{0,9}\b", question):
        if token.lower() in stopwords:
            continue
        if token.islower() and len(token) <= 2:
            continue
        symbol = symbol_map.get(safe_symbol(token))
        if symbol and symbol not in found:
            found.append(symbol)
    lowered = question.lower()
    for row in store.stocks[["symbol", "name"]].dropna().to_dict("records"):
        name = str(row.get("name") or "").lower()
        symbol = str(row.get("symbol") or "")
        if len(name) >= 4 and name in lowered and symbol not in found:
            found.append(symbol)
    selected = str(context.get("selected") or "")
    if not found and selected and re.search(r"\b(this|current|selected)\s+(stock|ticker|company)\b", lowered):
        found.append(selected)
    return found[:5]


def chat_find_sector(store: DataStore, question: str, context: dict[str, object]) -> str | None:
    lowered = question.lower()
    aliases = {
        "tech": "Information Technology",
        "technology": "Information Technology",
        "healthcare": "Health Care",
        "health care": "Health Care",
        "financial": "Financials",
        "finance": "Financials",
        "communication": "Communication Services",
        "consumer discretionary": "Consumer Discretionary",
        "consumer staples": "Consumer Staples",
        "real estate": "Real Estate",
    }
    for alias, sector in aliases.items():
        if alias in lowered and sector in store.sectors:
            return sector
    for sector in sorted(store.sectors, key=len, reverse=True):
        if sector.lower() in lowered:
            return sector
    current = str(context.get("sector") or "")
    if current and re.search(r"\b(this|current|selected)\s+sector\b", lowered):
        return current
    return None


def chat_period(question: str) -> str:
    lowered = question.lower()
    if "1 week" in lowered or "one week" in lowered or "1w" in lowered or "week" in lowered:
        return "1W"
    if "3 month" in lowered or "three month" in lowered or "3m" in lowered or "quarter" in lowered:
        return "3M"
    if "1 year" in lowered or "one year" in lowered or "12 month" in lowered or "1y" in lowered or "year" in lowered:
        return "1Y"
    return "1M"


def chat_stock_answer(store: DataStore, symbols: list[str]) -> dict[str, object]:
    frame = store.stocks[store.stocks["symbol"].isin(symbols)].copy()
    order = {symbol: index for index, symbol in enumerate(symbols)}
    frame["_order"] = frame["symbol"].map(order)
    frame = frame.sort_values("_order")
    rows = chat_rows(frame, limit=len(symbols))
    if len(rows) == 1:
        row = rows[0]
        answer = (
            f"{row.get('symbol')} is {row.get('name')} in {row.get('sector')} / {row.get('industry')}. "
            "Key fields are shown below: latest price, 21D return, 1Y return, RSI, 200D SMA distance, "
            "institutional ownership, analyst rating, and target price."
        )
    else:
        answer = "Here is a side-by-side comparison for the requested tickers."
    return {"answer": answer, "rows": rows, "actions": chat_actions_from_rows(rows)}


def chat_sector_answer(store: DataStore, sector: str, question: str) -> dict[str, object]:
    detail = store.sector_detail(sector)
    members = pd.DataFrame(detail.get("members") or [])
    lowered = question.lower()
    if members.empty:
        return {"answer": f"I could not find member stocks for {sector}.", "rows": [], "actions": []}
    sort_column = "return_21d" if "21" in lowered or "month" in lowered or "1m" in lowered else "return_1y"
    if "rsi" in lowered:
        sort_column = "rsi_14"
    if sort_column in members.columns:
        members = members.sort_values(sort_column, ascending=False, na_position="last")
    rows = chat_rows(members, limit=10)
    performance = detail.get("performance", {})
    answer = (
        f"{sector} has {detail.get('stock_count')} S&P 500 members. "
        f"Median 1M return is {format_chat_percent(performance.get('return_1m'))}, "
        f"median 1Y return is {format_chat_percent(performance.get('return_1y'))}, and "
        f"{format_chat_percent(performance.get('above_sma_200_pct'))} of members are above the 200D SMA."
    )
    actions = [{"type": "sector", "value": sector, "label": f"Open {sector} sector"}, *chat_actions_from_rows(rows)]
    return {"answer": answer, "rows": rows, "actions": actions}


def format_chat_percent(value: object) -> str:
    number = safe_float(value)
    return "-" if number is None else f"{number * 100:.1f}%"


def chat_requested_limit(question: str, default: int = 10, maximum: int = 25) -> int:
    lowered = question.lower()
    word_numbers = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
    }
    for pattern in (r"\btop\s+(\d{1,2})\b", r"\bshow\s+(\d{1,2})\b", r"\b(\d{1,2})\s+(?:stocks|tickers|names)\b"):
        match = re.search(pattern, lowered)
        if match:
            return max(1, min(maximum, int(match.group(1))))
    for word, value in word_numbers.items():
        if re.search(rf"\btop\s+{word}\b", lowered) or re.search(rf"\b{word}\s+(?:stocks|tickers|names)\b", lowered):
            return max(1, min(maximum, value))
    return default


def chat_recommendation_answer(store: DataStore, question: str) -> dict[str, object]:
    lowered = question.lower()
    limit = chat_requested_limit(question, default=10)
    side = "sell" if re.search(r"\b(sell|sells|short|shorts|avoid|weakest|worst)\b", lowered) else "buy"
    payload = recommendation_payload(store, limit=limit, allow_compute=False)
    rows = [
        {key: chat_value(value) for key, value in row.items()}
        for row in payload.get(side, [])
    ]
    if rows:
        label = "buy" if side == "buy" else "sell"
        return {
            "answer": (
                f"Top {len(rows)} {label} recommendations from the local quant model. "
                "These are research signals only, not financial advice."
            ),
            "rows": rows,
            "actions": chat_actions_from_rows(rows),
        }

    message = str(payload.get("message") or "")
    if payload.get("status") == "building":
        return {
            "answer": message or "The recommendation model is still building. Try again shortly.",
            "rows": [],
            "actions": [],
        }
    return {"answer": f"No {side} recommendations are available in the local data yet.", "rows": [], "actions": []}


def chat_ranked_answer(store: DataStore, question: str) -> dict[str, object]:
    lowered = question.lower()
    limit = chat_requested_limit(question, default=10)
    frame = store.stocks.copy()
    if "distance_from_sma_200" not in frame.columns and {"last_close", "sma_200"}.issubset(frame.columns):
        close = pd.to_numeric(frame["last_close"], errors="coerce")
        sma = pd.to_numeric(frame["sma_200"], errors="coerce")
        frame["distance_from_sma_200"] = (close / sma) - 1
    answer = "Here are the matching stocks from the local data."

    if "insider" in lowered and ("buy" in lowered or "purchase" in lowered):
        frame = frame[frame["insider_buy_flag"].fillna(False).astype(bool)]
        frame = frame.sort_values("return_21d", ascending=False, na_position="last")
        answer = "Stocks with an insider buy flag, ranked by 21D return."
    elif "52" in lowered and "high" in lowered:
        frame = frame[frame["at_52w_high"].fillna(False).astype(bool)]
        frame = frame.sort_values("return_21d", ascending=False, na_position="last")
        answer = "Stocks currently flagged near a 52-week high."
    elif "52" in lowered and "low" in lowered:
        frame = frame[frame["at_52w_low"].fillna(False).astype(bool)]
        frame = frame.sort_values("return_21d", ascending=True, na_position="last")
        answer = "Stocks currently flagged near a 52-week low."
    elif "below" in lowered and "200" in lowered:
        frame = frame[frame["below_sma_200"].fillna(False).astype(bool)]
        frame = frame.sort_values("distance_from_sma_200", ascending=True, na_position="last")
        answer = "Stocks below their 200D SMA, with the weakest distance first."
    elif "above" in lowered and "200" in lowered:
        frame = frame[frame["above_sma_200"].fillna(False).astype(bool)]
        frame = frame.sort_values("distance_from_sma_200", ascending=False, na_position="last")
        answer = "Stocks above their 200D SMA, with the strongest distance first."
    elif "oversold" in lowered or ("lowest" in lowered and "rsi" in lowered):
        frame = frame.sort_values("rsi_14", ascending=True, na_position="last")
        answer = "Lowest RSI stocks in the local data."
    elif "overbought" in lowered or ("highest" in lowered and "rsi" in lowered):
        frame = frame.sort_values("rsi_14", ascending=False, na_position="last")
        answer = "Highest RSI stocks in the local data."
    elif "rating" in lowered or "analyst" in lowered:
        frame = frame.sort_values("analyst_rating_score", ascending=True, na_position="last")
        answer = "Best analyst rating scores in the local data. Lower scores indicate stronger ratings."
    elif "momentum" in lowered or "top" in lowered or "best" in lowered or "strongest" in lowered:
        payload = store.momentum_recommendations(limit=limit)
        rows = [
            {key: chat_value(value) for key, value in row.items()}
            for row in payload.get("rows", [])
        ]
        return {
            "answer": f"Top {len(rows)} 12-month cross-sectional momentum recommendations from the local model.",
            "rows": rows,
            "actions": chat_actions_from_rows(rows),
        }

    rows = chat_rows(frame, limit=limit)
    return {"answer": answer, "rows": rows, "actions": chat_actions_from_rows(rows)}


def chat_group_momentum_answer(store: DataStore, question: str) -> dict[str, object]:
    period = chat_period(question)
    payload = store.group_momentum_leaders(limit=5)
    period_payload = payload.get("periods", {}).get(period, {})
    lowered = question.lower()
    key = "industries" if "industry" in lowered else "sectors"
    rows = period_payload.get(key, [])
    answer = f"Top {key[:-1]} momentum groups for {period}, using median constituent returns."
    actions = [
        {"type": "sector", "value": row["name"], "label": f"Open {row['name']}"}
        for row in rows
        if key == "sectors"
    ]
    return {"answer": answer, "group_rows": rows, "actions": actions}


def chat_ranked_query(question: str) -> bool:
    lowered = question.lower()
    return bool(
        re.search(
            r"\b(insider|52|below|above|200|oversold|overbought|rating|analyst|momentum|top|best|strongest|weakest|rsi)\b",
            lowered,
        )
    )


def chat_help_answer(store: DataStore) -> dict[str, object]:
    return {
        "answer": (
            "Ask about a ticker, compare tickers, screen for technical conditions, or inspect sector momentum. "
            "Examples: 'compare AAPL MSFT', 'top momentum stocks', 'technology sector leaders', "
            "'stocks above 200 SMA', 'lowest RSI stocks', or 'insider buys'."
        ),
        "suggestions": [
            "Top momentum stocks",
            "Compare AAPL MSFT",
            "Information Technology sector leaders",
            "Stocks above 200 SMA",
            "Lowest RSI stocks",
            "Stocks with insider buys",
        ],
        "actions": [{"type": "sector", "value": sector, "label": sector} for sector in store.sectors[:5]],
    }


def ollama_base_url() -> str | None:
    raw = (
        os.environ.get("SENQUANT_OLLAMA_BASE_URL")
        or os.environ.get("SENQUANT_OLLAMA_HOSTPORT")
        or os.environ.get("OLLAMA_BASE_URL")
        or ""
    ).strip()
    if not raw:
        return None
    if not re.match(r"^https?://", raw):
        raw = f"http://{raw}"
    return raw.rstrip("/")


def ollama_chat_enabled() -> bool:
    disabled = os.environ.get("SENQUANT_ENABLE_OLLAMA_CHAT", "").lower() in {"0", "false", "no", "off"}
    return bool(ollama_base_url()) and not disabled


def ollama_chat_status() -> dict[str, object]:
    return {
        "enabled": ollama_chat_enabled(),
        "base_url_configured": bool(ollama_base_url()),
        "model": ollama_model_name(),
        "timeout_seconds": ollama_timeout_seconds(),
        "external_fallback": external_llm_status(),
    }


def ollama_model_name() -> str:
    return os.environ.get("SENQUANT_OLLAMA_MODEL") or os.environ.get("OLLAMA_MODEL") or "llama3.2:1b"


def ollama_timeout_seconds() -> float:
    return safe_float(os.environ.get("SENQUANT_OLLAMA_TIMEOUT_SECONDS")) or 45.0


def ollama_max_tokens() -> int:
    return safe_int(os.environ.get("SENQUANT_OLLAMA_MAX_TOKENS")) or 160


def external_llm_base_url() -> str | None:
    raw = (
        os.environ.get("SENQUANT_EXTERNAL_LLM_BASE_URL")
        or os.environ.get("RUNPOD_OPENAI_BASE_URL")
        or os.environ.get("RUNPOD_BASE_URL")
        or ""
    ).strip()
    return raw.rstrip("/") or None


def external_llm_api_key() -> str | None:
    return (
        os.environ.get("SENQUANT_EXTERNAL_LLM_API_KEY")
        or os.environ.get("RUNPOD_API_KEY")
        or ""
    ).strip() or None


def external_llm_model_name() -> str:
    return (
        os.environ.get("SENQUANT_EXTERNAL_LLM_MODEL")
        or os.environ.get("RUNPOD_MODEL")
        or "qwen"
    )


def external_llm_enabled() -> bool:
    disabled = os.environ.get("SENQUANT_ENABLE_EXTERNAL_LLM_CHAT", "").lower() in {"0", "false", "no", "off"}
    return bool(external_llm_base_url() and external_llm_api_key()) and not disabled


def external_llm_api_style() -> str:
    configured = os.environ.get("SENQUANT_EXTERNAL_LLM_API_STYLE", "").strip().lower()
    if configured in {"openai", "runpod"}:
        return configured
    base_url = external_llm_base_url() or ""
    if "runpod.ai/v2" in base_url and "/openai/" not in base_url:
        return "runpod"
    return "openai"


def external_llm_timeout_seconds() -> float:
    return safe_float(os.environ.get("SENQUANT_EXTERNAL_LLM_TIMEOUT_SECONDS")) or 90.0


def external_llm_max_tokens() -> int:
    return safe_int(os.environ.get("SENQUANT_EXTERNAL_LLM_MAX_TOKENS")) or 260


def external_llm_status() -> dict[str, object]:
    return {
        "enabled": external_llm_enabled(),
        "base_url_configured": bool(external_llm_base_url()),
        "api_key_configured": bool(external_llm_api_key()),
        "model": external_llm_model_name(),
        "api_style": external_llm_api_style(),
        "timeout_seconds": external_llm_timeout_seconds(),
    }


def recommendation_context_needed(question: str, local_response: dict[str, object]) -> bool:
    lowered = question.lower()
    if re.search(r"\b(recommend|recommendation|recommendations|buy|buys|sell|sells|short|shorts|avoid|advanced|model|signal|signals|quant)\b", lowered):
        return True
    rows = local_response.get("rows")
    if isinstance(rows, list):
        return any(isinstance(row, dict) and row.get("signal") for row in rows)
    return False


def compact_recommendation_rows(rows: object, limit: int = 5) -> list[dict[str, object]]:
    if not isinstance(rows, list):
        return []
    columns = [
        "rank",
        "signal",
        "confidence",
        "symbol",
        "name",
        "sector",
        "industry",
        "last_close",
        "quant_score",
        "advanced_score",
        "ml_expected_21d",
        "probability_up",
        "momentum_12_1",
        "return_1m",
        "return_3m",
        "distance_from_sma_200",
        "target_upside",
        "reason",
    ]
    compacted: list[dict[str, object]] = []
    for row in rows[:limit]:
        if isinstance(row, dict):
            compacted.append({key: row.get(key) for key in columns if key in row})
    return compacted


def recommendation_payload_context(store: DataStore) -> dict[str, object]:
    context: dict[str, object] = {}
    basic = recommendation_payload(store, limit=5, allow_compute=False)
    context["basic"] = {
        "status": basic.get("status"),
        "as_of": basic.get("as_of"),
        "model": basic.get("model", {}).get("name") if isinstance(basic.get("model"), dict) else basic.get("model"),
        "methodology": list(basic.get("methodology") or [])[:3],
        "buy": compact_recommendation_rows(basic.get("buy")),
        "sell": compact_recommendation_rows(basic.get("sell")),
    }
    advanced = advanced_recommendation_payload(store, limit=5, allow_compute=False)
    context["advanced"] = {
        "status": advanced.get("status"),
        "as_of": advanced.get("as_of"),
        "model": advanced.get("model", {}).get("name") if isinstance(advanced.get("model"), dict) else advanced.get("model"),
        "methodology": list(advanced.get("methodology") or [])[:4],
        "buy": compact_recommendation_rows(advanced.get("buy")),
        "sell": compact_recommendation_rows(advanced.get("sell")),
    }
    return context


def compact_chat_payload(question: str, local_response: dict[str, object]) -> dict[str, object]:
    payload = {
        "question": question[:1000],
        "local_answer": local_response.get("answer"),
        "stock_rows": list(local_response.get("rows") or [])[:10],
        "group_rows": list(local_response.get("group_rows") or [])[:10],
        "recommendation_context": local_response.get("recommendation_context"),
        "answer_style": (
            "Write for an average investor. Use two to four short sentences plus up to three concise bullets when helpful. "
            "When recommendation_context is present, explain the basic and advanced recommendation signals, any overlap or "
            "difference between them, and the main drivers. Do not output JSON, code, markdown tables, raw field names, "
            "or internal instructions. Mention that recommendations are research signals, not financial advice."
        ),
    }
    if payload["recommendation_context"] is None:
        payload.pop("recommendation_context")
    return payload


def local_response_has_data(local_response: dict[str, object]) -> bool:
    return bool(local_response.get("rows") or local_response.get("group_rows"))


def answer_says_local_data_missing(answer: object) -> bool:
    lowered = str(answer or "").lower()
    missing_phrases = (
        "i do not have that in the loaded senquant data",
        "could not find",
        "no ",
        "not available",
        "does not contain",
        "try again shortly",
    )
    return any(phrase in lowered for phrase in missing_phrases)


def should_try_external_llm(local_response: dict[str, object], llm_answer: str | None = None) -> bool:
    if not external_llm_enabled():
        return False
    if llm_answer and answer_says_local_data_missing(llm_answer):
        return True
    if not local_response_has_data(local_response) and answer_says_local_data_missing(local_response.get("answer")):
        return True
    return False


def ollama_answer_usable(content: str) -> bool:
    if not content:
        return False
    stripped = content.strip()
    if stripped.startswith(("{", "[")) or stripped.endswith(("}", "]")):
        return False
    lowered = stripped.lower()
    blocked_phrases = (
        "json",
        "local_answer",
        "stock_rows",
        "group_rows",
        "answer_style",
        "provided data",
        "as an ai",
        "i don't have access to the internet",
    )
    return not any(phrase in lowered for phrase in blocked_phrases)


def external_answer_usable(content: str) -> bool:
    if not content:
        return False
    stripped = content.strip()
    if not stripped:
        return False
    if stripped.startswith(("{", "[")) and stripped.endswith(("}", "]")):
        return False
    return True


def clean_external_answer(content: str) -> str:
    cleaned = str(content or "").strip()
    cleaned = re.sub(r"(?is)<think>.*?</think>", "", cleaned).strip()
    cleaned = re.sub(r"(?is)<thinking>.*?</thinking>", "", cleaned).strip()
    cleaned = re.sub(r"(?is)^think:\s*.*?(?:\n\s*\n|$)", "", cleaned).strip()
    if "Assistant:" in cleaned:
        cleaned = cleaned.split("Assistant:", 1)[-1].strip()
    for prefix in ("assistant:", "ASSISTANT:"):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :].strip()
    if "\nFinal answer:" in cleaned:
        cleaned = cleaned.split("\nFinal answer:", 1)[-1].strip()
    if "\nAnswer:" in cleaned:
        cleaned = cleaned.split("\nAnswer:", 1)[-1].strip()
    replacements = {
        "local_answer": "local answer",
        "stock_rows": "stock rows",
        "group_rows": "group rows",
        "recommendation_context": "recommendation context",
        "answer_style": "answer style",
    }
    for raw, label in replacements.items():
        cleaned = re.sub(rf"\b{raw}\b", label, cleaned, flags=re.IGNORECASE)
    return cleaned


def call_ollama_chat(question: str, local_response: dict[str, object]) -> str | None:
    base_url = ollama_base_url()
    if not base_url:
        return None

    model = ollama_model_name()
    timeout = ollama_timeout_seconds()
    system_prompt = (
        "You are SenQuant's market data assistant. You do not have internet access, browser access, or tools. "
        "Answer only from the data in the user's JSON payload. Do not invent prices, returns, sectors, ratings, "
        "recommendations, or dates. Never mention JSON, payloads, instructions, tools, or data availability mechanics. "
        "If the local data does not answer the question, say: 'I do not have that in the loaded SenQuant data.' "
        "Use plain, user-friendly language. Start with the direct answer. Use at most three short bullets. "
        "Do not output markdown tables. Do not provide personalized financial advice."
    )
    user_payload = compact_chat_payload(question, local_response)
    body = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": json.dumps(user_payload, separators=(",", ":"), default=str),
            },
        ],
        "options": {"temperature": 0.1, "num_ctx": 4096, "num_predict": ollama_max_tokens()},
        "keep_alive": os.environ.get("SENQUANT_OLLAMA_KEEP_ALIVE", "30m"),
    }
    request = Request(
        f"{base_url}/api/chat",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - URL is operator-configured.
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        print(f"Ollama chat unavailable: {exc}", flush=True)
        return None

    message = payload.get("message") if isinstance(payload, dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    content = str(content or "").strip()
    if not ollama_answer_usable(content):
        return None
    return content or None


def external_llm_system_prompt() -> str:
    return (
        "You are SenQuant's fallback assistant, powered by an external Qwen model. "
        "Use loaded SenQuant rows when they are provided. If the question asks for current market prices, latest news, "
        "today's macro data, or real-time facts not present in the rows, say that live internet data is not available. "
        "For general finance, investing, quantitative methods, definitions, and app navigation, answer plainly. "
        "Do not give personalized financial advice. Keep the response concise and user-friendly."
    )


def external_llm_prompt(question: str, local_response: dict[str, object]) -> str:
    return (
        f"System: {external_llm_system_prompt()}\n\n"
        "User data:\n"
        f"{json.dumps(compact_chat_payload(question, local_response), separators=(',', ':'), default=str)}\n\n"
        "Assistant:"
    )


def normalize_runpod_base_url(base_url: str) -> str:
    for suffix in ("/runsync", "/run"):
        if base_url.endswith(suffix):
            return base_url[: -len(suffix)]
    return base_url


def extract_external_text(payload: object) -> str | None:
    if isinstance(payload, str):
        return payload.strip() or None
    if isinstance(payload, list):
        parts = [extract_external_text(item) for item in payload]
        return "\n".join(part for part in parts if part).strip() or None
    if not isinstance(payload, dict):
        return None

    status = str(payload.get("status") or "").upper()
    if status and status not in {"COMPLETED", "SUCCEEDED", "SUCCESS"}:
        EXTERNAL_LLM_LAST_ERROR["message"] = f"RunPod returned status {status}."
        return None

    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0] if isinstance(choices[0], dict) else {}
        message = first.get("message") if isinstance(first, dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if content is None and isinstance(first, dict):
            content = first.get("text")
        if content is None and isinstance(first, dict) and isinstance(first.get("tokens"), list):
            return "".join(str(token) for token in first["tokens"]).strip() or None
        return extract_external_text(content)

    output = payload.get("output")
    if output is not None:
        return extract_external_text(output)

    tokens = payload.get("tokens")
    if isinstance(tokens, list):
        return "".join(str(token) for token in tokens).strip() or None

    for key in ("text", "response", "generated_text", "content", "answer", "completion"):
        if key in payload:
            return extract_external_text(payload.get(key))
    return None


def describe_external_payload_shape(payload: object) -> str:
    if isinstance(payload, dict):
        keys = ",".join(str(key) for key in list(payload.keys())[:8])
        output = payload.get("output")
        if isinstance(output, list) and output:
            first = output[0]
            if isinstance(first, dict):
                return f"top-level keys: {keys}; output[0] keys: {','.join(str(key) for key in list(first.keys())[:8])}"
        if isinstance(output, dict):
            return f"top-level keys: {keys}; output keys: {','.join(str(key) for key in list(output.keys())[:8])}"
        return f"top-level keys: {keys}"
    return f"payload type: {type(payload).__name__}"


def call_external_openai_chat(question: str, local_response: dict[str, object], base_url: str, api_key: str) -> str | None:
    body = {
        "model": external_llm_model_name(),
        "messages": [
            {"role": "system", "content": external_llm_system_prompt()},
            {
                "role": "user",
                "content": json.dumps(compact_chat_payload(question, local_response), separators=(",", ":"), default=str),
            },
        ],
        "temperature": 0.2,
        "max_tokens": external_llm_max_tokens(),
        "stream": False,
    }
    request = Request(
        f"{base_url}/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    with urlopen(request, timeout=external_llm_timeout_seconds()) as response:  # noqa: S310 - operator-configured URL.
        payload = json.loads(response.read().decode("utf-8"))
    return extract_external_text(payload)


def call_external_runpod_chat(question: str, local_response: dict[str, object], base_url: str, api_key: str) -> str | None:
    wait_ms = int(min(300_000, max(1_000, external_llm_timeout_seconds() * 1000)))
    prompt = external_llm_prompt(question, local_response)
    max_tokens = external_llm_max_tokens()
    body = {
        "input": {
            "prompt": prompt,
            "max_tokens": max_tokens,
            "temperature": 0.2,
            "top_k": -1,
            "top_p": 1,
            "sampling_params": {
                "max_tokens": max_tokens,
                "temperature": 0.2,
                "seed": -1,
                "top_k": -1,
                "top_p": 1,
            },
        }
    }
    request = Request(
        f"{normalize_runpod_base_url(base_url)}/runsync?wait={wait_ms}",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    with urlopen(request, timeout=external_llm_timeout_seconds()) as response:  # noqa: S310 - operator-configured URL.
        payload = json.loads(response.read().decode("utf-8"))
    text = extract_external_text(payload)
    if not text and not EXTERNAL_LLM_LAST_ERROR["message"]:
        EXTERNAL_LLM_LAST_ERROR["message"] = f"RunPod returned no text output ({describe_external_payload_shape(payload)})."
    return text


def call_external_llm_chat(question: str, local_response: dict[str, object]) -> str | None:
    base_url = external_llm_base_url()
    api_key = external_llm_api_key()
    EXTERNAL_LLM_LAST_ERROR["message"] = None
    if not base_url or not api_key:
        EXTERNAL_LLM_LAST_ERROR["message"] = "External LLM base URL or API key is missing."
        return None

    try:
        if external_llm_api_style() == "runpod":
            content = call_external_runpod_chat(question, local_response, base_url, api_key)
        else:
            content = call_external_openai_chat(question, local_response, base_url, api_key)
    except Exception as exc:  # noqa: BLE001
        EXTERNAL_LLM_LAST_ERROR["message"] = str(exc)
        print(f"External LLM unavailable: {exc}", flush=True)
        return None

    content = clean_external_answer(str(content or "").strip())
    if not content:
        if not EXTERNAL_LLM_LAST_ERROR["message"]:
            EXTERNAL_LLM_LAST_ERROR["message"] = "RunPod returned no text output."
        return None
    if not external_answer_usable(content):
        EXTERNAL_LLM_LAST_ERROR["message"] = "RunPod returned a response that looked like internal data rather than an answer."
        return None
    return content or None


def external_llm_response(
    question: str,
    local_response: dict[str, object],
    notice: str | None = None,
) -> dict[str, object] | None:
    external_answer = call_external_llm_chat(question, local_response)
    if not external_answer:
        return None
    model = external_llm_model_name()
    default_notice = (
        f"The local SenQuant/Ollama assistant did not have a good answer, "
        f"so this response used an external call to {model}."
    )
    return {
        **local_response,
        "answer": external_answer,
        "assistant_provider": "external",
        "assistant_model": model,
        "assistant_notice": notice or default_notice,
    }


def enrich_chat_response_with_ollama(question: str, local_response: dict[str, object]) -> dict[str, object]:
    if not ollama_chat_enabled():
        if should_try_external_llm(local_response):
            response = external_llm_response(question, local_response)
            if response:
                return response
        return {**local_response, "assistant_provider": "local"}

    llm_answer = call_ollama_chat(question, local_response)
    if should_try_external_llm(local_response, llm_answer):
        response = external_llm_response(question, local_response)
        if response:
            return response
    if not llm_answer:
        if should_try_external_llm(local_response):
            response = external_llm_response(question, local_response)
            if response:
                return response
        return {**local_response, "assistant_provider": "local", "llm_fallback": True}
    return {**local_response, "answer": llm_answer, "assistant_provider": "ollama", "assistant_model": ollama_model_name()}


def chat_session_id(payload: dict[str, object]) -> str | None:
    raw = str(payload.get("session_id") or payload.get("sessionId") or "").strip()
    if not raw:
        return None
    cleaned = re.sub(r"[^A-Za-z0-9_.:-]", "", raw)
    return cleaned[:96] or None


def remember_chat_question(session_id: str | None, question: str, local_response: dict[str, object]) -> None:
    if not session_id or not question:
        return
    CHAT_SESSION_MEMORY[session_id] = {
        "question": question,
        "local_response": local_response,
        "stored_at": time.time(),
    }
    while len(CHAT_SESSION_MEMORY) > 200:
        oldest = next(iter(CHAT_SESSION_MEMORY))
        CHAT_SESSION_MEMORY.pop(oldest, None)


def external_retry_requested(question: str) -> bool:
    lowered = question.lower().strip()
    return bool(
        re.search(
            r"\b(use|try|ask|call|route|send)\s+(the\s+)?(external|runpod|qwen|qwen3|32b)\b",
            lowered,
        )
        or re.fullmatch(r"(external|use external|try external|ask qwen|use qwen|runpod)", lowered)
    )


def with_recommendation_context(store: DataStore, question: str, local_response: dict[str, object]) -> dict[str, object]:
    if not recommendation_context_needed(question, local_response):
        return local_response
    try:
        return {**local_response, "recommendation_context": recommendation_payload_context(store)}
    except Exception as exc:  # noqa: BLE001
        print(f"Recommendation context unavailable for chat: {exc}", flush=True)
        return local_response


def chat_local_response(store: DataStore, question: str, context: dict[str, object]) -> dict[str, object]:
    if not question:
        return with_recommendation_context(store, question, chat_help_answer(store))

    lowered = question.lower()
    symbols = chat_find_symbols(store, question, context)
    if re.search(r"\b(buy|buys|sell|sells|recommend|recommendation|recommendations|short|shorts|avoid)\b", lowered) and (
        not symbols or re.search(r"\b(top|list|which|what|show)\b", lowered)
    ):
        return with_recommendation_context(store, question, chat_recommendation_answer(store, question))

    if symbols:
        return with_recommendation_context(store, question, chat_stock_answer(store, symbols))

    sector = chat_find_sector(store, question, context)
    if sector:
        return with_recommendation_context(store, question, chat_sector_answer(store, sector, question))

    if ("sector" in lowered or "industry" in lowered) and ("momentum" in lowered or "strongest" in lowered or "top" in lowered):
        return with_recommendation_context(store, question, chat_group_momentum_answer(store, question))

    if re.search(r"\b(help|what can|examples|how do)\b", lowered):
        return with_recommendation_context(store, question, chat_help_answer(store))

    if chat_ranked_query(question):
        return with_recommendation_context(store, question, chat_ranked_answer(store, question))

    return with_recommendation_context(
        store,
        question,
        {"answer": "I do not have that in the loaded SenQuant data.", "rows": [], "actions": []},
    )


def chat_response(store: DataStore, payload: dict[str, object]) -> dict[str, object]:
    question = str(payload.get("question") or "").strip()
    context = payload.get("context", {})
    context = context if isinstance(context, dict) else {}
    session_id = chat_session_id(payload)

    if external_retry_requested(question):
        previous = CHAT_SESSION_MEMORY.get(session_id or "")
        if not previous:
            return {
                "answer": "Ask a question first, then type 'use external' to retry that question with the external model.",
                "rows": [],
                "actions": [],
                "assistant_provider": "local",
            }
        if not external_llm_enabled():
            return {
                "answer": "External fallback is not configured yet. Add the RunPod environment variables in Render first.",
                "rows": [],
                "actions": [],
                "assistant_provider": "local",
            }
        previous_question = str(previous.get("question") or "")
        previous_local_response = previous.get("local_response")
        previous_local_response = previous_local_response if isinstance(previous_local_response, dict) else {}
        model = external_llm_model_name()
        response = external_llm_response(
            previous_question,
            previous_local_response,
            notice=f"You asked to retry the previous question with the external model, so this response used {model}.",
        )
        if response:
            return response
        detail = EXTERNAL_LLM_LAST_ERROR["message"]
        suffix = f" Detail: {detail}" if detail else ""
        return {
            **previous_local_response,
            "answer": f"I tried the external model, but it did not return a usable answer.{suffix}",
            "assistant_provider": "local",
            "llm_fallback": True,
        }

    local_response = chat_local_response(store, question, context)
    remember_chat_question(session_id, question, local_response)
    return enrich_chat_response_with_ollama(question, local_response)


RECOMMENDATION_FEATURES = [
    "momentum_12_1",
    "return_6m",
    "return_3m",
    "return_1m",
    "trend_50",
    "trend_200",
    "rsi_scaled",
    "volatility_21d",
]


def numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def finite_float(value: object) -> float | None:
    result = safe_float(value)
    return result if result is not None and np.isfinite(result) else None


def standard_score(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    median = values.median()
    spread = values.std(ddof=0)
    if pd.isna(spread) or spread == 0:
        return pd.Series(0.0, index=series.index)
    return ((values - median) / spread).clip(-3, 3).fillna(0)


def technical_feature_row(frame: pd.DataFrame, index: int) -> dict[str, float] | None:
    close_col = "adj_close" if "adj_close" in frame.columns else "close"
    close = numeric_series(frame, close_col)
    if index < 252 or index >= len(frame):
        return None

    def close_at(offset: int) -> float | None:
        position = index - offset
        if position < 0 or position >= len(close):
            return None
        return finite_float(close.iloc[position])

    current = close_at(0)
    one_month_ago = close_at(21)
    three_months_ago = close_at(63)
    six_months_ago = close_at(126)
    twelve_months_ago = close_at(252)
    if current is None or one_month_ago is None or twelve_months_ago is None:
        return None

    sma_50 = finite_float(frame.iloc[index].get("sma_50"))
    sma_200 = finite_float(frame.iloc[index].get("sma_200"))
    rsi = finite_float(frame.iloc[index].get("rsi_14"))
    daily_returns = close.pct_change().iloc[max(0, index - 21) : index + 1]
    volatility = finite_float(daily_returns.std(ddof=0))
    row = {
        "momentum_12_1": pct_change(twelve_months_ago, one_month_ago),
        "return_6m": pct_change(six_months_ago, current),
        "return_3m": pct_change(three_months_ago, current),
        "return_1m": pct_change(one_month_ago, current),
        "trend_50": pct_change(sma_50, current),
        "trend_200": pct_change(sma_200, current),
        "rsi_scaled": ((rsi - 50) / 50) if rsi is not None else None,
        "volatility_21d": volatility,
    }
    if any(row.get(feature) is None for feature in RECOMMENDATION_FEATURES):
        return None
    if any(abs(float(row[feature])) > 10 for feature in RECOMMENDATION_FEATURES):
        return None
    return {key: float(value) for key, value in row.items()}


def load_recommendation_frame(path: Path) -> pd.DataFrame | None:
    try:
        frame = pd.read_csv(
            path,
            usecols=lambda column: column in {"date", "adj_close", "close", "sma_50", "sma_200", "rsi_14"},
        )
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame = frame.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        return frame if len(frame) > 280 else None
    except Exception:
        return None


def train_ridge_forecaster(samples: list[dict[str, float]]) -> dict[str, object]:
    if len(samples) < 300:
        return {"trained": False, "sample_count": len(samples), "coefficients": {}}
    x = np.array([[sample[feature] for feature in RECOMMENDATION_FEATURES] for sample in samples], dtype=float)
    y = np.array([sample["target_21d"] for sample in samples], dtype=float)
    y = np.clip(y, -0.5, 0.5)
    x_mean = x.mean(axis=0)
    x_std = x.std(axis=0)
    x_std[x_std == 0] = 1
    y_mean = float(y.mean())
    xs = (x - x_mean) / x_std
    alpha = 8.0
    identity = np.eye(xs.shape[1])
    beta = np.linalg.solve(xs.T @ xs + alpha * identity, xs.T @ (y - y_mean))
    return {
        "trained": True,
        "sample_count": len(samples),
        "x_mean": x_mean,
        "x_std": x_std,
        "y_mean": y_mean,
        "beta": beta,
        "coefficients": {feature: jsonable(float(beta[index])) for index, feature in enumerate(RECOMMENDATION_FEATURES)},
    }


def ridge_predict(model: dict[str, object], features: dict[str, float]) -> float | None:
    if not model.get("trained"):
        return None
    x = np.array([features[feature] for feature in RECOMMENDATION_FEATURES], dtype=float)
    x_mean = model["x_mean"]
    x_std = model["x_std"]
    beta = model["beta"]
    prediction = float(((x - x_mean) / x_std) @ beta + model["y_mean"])
    return float(np.clip(prediction, -0.5, 0.5))


def recommendation_reason(row: dict[str, object], signal: str) -> str:
    reasons: list[str] = []
    ml_expected = safe_float(row.get("ml_expected_21d"))
    momentum = safe_float(row.get("momentum_12_1"))
    trend_200 = safe_float(row.get("distance_from_sma_200"))
    rsi = safe_float(row.get("rsi_14"))
    rating = safe_float(row.get("analyst_rating_score"))
    volatility = safe_float(row.get("volatility_21d"))
    if signal == "BUY":
        if ml_expected is not None and ml_expected > 0:
            reasons.append("positive ML 21D forecast")
        if momentum is not None and momentum > 0:
            reasons.append("positive 12-1 momentum")
        if trend_200 is not None and trend_200 > 0:
            reasons.append("above 200D SMA")
        if rating is not None and rating <= 2.3:
            reasons.append("favorable analyst score")
        if row.get("insider_buy_flag"):
            reasons.append("insider buy flag")
    else:
        if ml_expected is not None and ml_expected < 0:
            reasons.append("negative ML 21D forecast")
        if momentum is not None and momentum < 0:
            reasons.append("weak 12-1 momentum")
        if trend_200 is not None and trend_200 < 0:
            reasons.append("below 200D SMA")
        if rsi is not None and rsi < 35:
            reasons.append("weak RSI")
        if volatility is not None and volatility > 0.035:
            reasons.append("high short-term volatility")
    return ", ".join(reasons[:4]) or "ranked by ensemble score"


def recommendation_cache_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    marker_mtime = REFRESH_MARKER_PATH.stat().st_mtime if REFRESH_MARKER_PATH.exists() else 0
    return path.stat().st_mtime >= marker_mtime


def limited_recommendation_payload(payload: dict[str, object], limit: int) -> dict[str, object]:
    return {
        **payload,
        "buy": payload.get("buy", [])[:limit],
        "sell": payload.get("sell", [])[:limit],
    }


def building_recommendation_payload(kind: str) -> dict[str, object]:
    label = "advanced recommendations" if kind == "advanced" else "recommendations"
    return {
        "status": "building",
        "message": f"Building {label} in the background. This page will refresh automatically.",
        "universe": "S&P 500",
        "disclaimer": "Beta model output for research only. Not financial advice.",
        "methodology": [],
        "model": {"name": "Building model", "training_samples": 0},
        "buy": [],
        "sell": [],
    }


def start_recommendation_build(kind: str) -> None:
    lock = ADVANCED_RECOMMENDATION_BUILD_LOCK if kind == "advanced" else RECOMMENDATION_BUILD_LOCK
    if lock.locked():
        return

    def worker() -> None:
        with lock:
            try:
                store = current_store()
                if kind == "advanced":
                    advanced_recommendation_payload(store, limit=15, allow_compute=True, force_rebuild=True)
                else:
                    recommendation_payload(store, limit=15, allow_compute=True, force_rebuild=True)
            except Exception as exc:  # noqa: BLE001
                print(f"Background {kind} recommendation build failed: {exc}", flush=True)

    thread = threading.Thread(target=worker, name=f"{kind}-recommendation-build", daemon=True)
    thread.start()


def start_advanced_recommendation_build_later(delay_seconds: int = 90) -> None:
    if ADVANCED_RECOMMENDATION_BUILD_LOCK.locked() or ADVANCED_RECOMMENDATION_DELAY_STATE["scheduled"]:
        return
    ADVANCED_RECOMMENDATION_DELAY_STATE["scheduled"] = True

    def delayed_worker() -> None:
        try:
            time.sleep(delay_seconds)
            if not recommendation_cache_fresh(ADVANCED_RECOMMENDATIONS_PATH):
                start_recommendation_build("advanced")
        finally:
            ADVANCED_RECOMMENDATION_DELAY_STATE["scheduled"] = False

    thread = threading.Thread(target=delayed_worker, name="advanced-recommendation-build-delay", daemon=True)
    thread.start()


def recommendation_payload(
    store: DataStore,
    limit: int = 15,
    allow_compute: bool = True,
    force_rebuild: bool = False,
) -> dict[str, object]:
    global RECOMMENDATION_CACHE
    if (
        not force_rebuild
        and RECOMMENDATION_CACHE["store_loaded_at"] == STORE_LOADED_AT
        and RECOMMENDATION_CACHE["payload"] is not None
    ):
        payload = RECOMMENDATION_CACHE["payload"]
        return limited_recommendation_payload(payload, limit)

    if not force_rebuild and RECOMMENDATIONS_PATH.exists():
        payload = load_json(RECOMMENDATIONS_PATH)
        RECOMMENDATION_CACHE = {"store_loaded_at": STORE_LOADED_AT, "payload": payload}
        if not recommendation_cache_fresh(RECOMMENDATIONS_PATH):
            payload = {**payload, "status": "stale_rebuilding", "message": "Showing cached recommendations while rebuilding from newer data."}
            start_recommendation_build("basic")
        return limited_recommendation_payload(payload, limit)

    if not allow_compute:
        start_recommendation_build("basic")
        return building_recommendation_payload("basic")

    universe_meta = {
        safe_symbol(row.get("symbol", "")): row
        for row in store.stocks.to_dict("records")
        if row.get("symbol")
    }
    training_samples: list[dict[str, float]] = []
    latest_rows: list[dict[str, object]] = []
    as_of_dates: list[str] = []

    for path in sorted(TECHNICALS_DIR.glob("*.csv")):
        frame = load_recommendation_frame(path)
        if frame is None:
            continue
        close_col = "adj_close" if "adj_close" in frame.columns else "close"
        close = numeric_series(frame, close_col)
        for index in range(252, len(frame) - 21, 21):
            features = technical_feature_row(frame, index)
            if not features:
                continue
            current = finite_float(close.iloc[index])
            future = finite_float(close.iloc[index + 21])
            target = pct_change(current, future)
            if target is None or target < -0.75 or target > 1.5:
                continue
            training_samples.append({**features, "target_21d": float(target)})

        latest_features = technical_feature_row(frame, len(frame) - 1)
        if not latest_features:
            continue
        key = path.stem.upper()
        meta = universe_meta.get(key, {})
        current = finite_float(close.iloc[-1])
        sma_200 = finite_float(frame.iloc[-1].get("sma_200"))
        latest_rows.append(
            {
                **latest_features,
                "symbol": meta.get("symbol") or key,
                "name": meta.get("name", ""),
                "sector": meta.get("sector", ""),
                "industry": meta.get("industry", ""),
                "last_date": frame.iloc[-1]["date"].date().isoformat(),
                "last_close": current,
                "rsi_14": finite_float(frame.iloc[-1].get("rsi_14")),
                "sma_200": sma_200,
                "distance_from_sma_200": pct_change(sma_200, current),
                "insider_buy_flag": bool(meta.get("insider_buy_flag")),
                "institutions_percent_held": safe_float(meta.get("institutions_percent_held")),
                "analyst_rating_score": safe_float(meta.get("analyst_rating_score")),
                "price_target_mean": safe_float(meta.get("price_target_mean")),
            }
        )
        as_of_dates.append(frame.iloc[-1]["date"].date().isoformat())

    model = train_ridge_forecaster(training_samples)
    frame = pd.DataFrame(latest_rows)
    if frame.empty:
        return {"error": "No recommendation data available", "buy": [], "sell": []}

    frame["ml_expected_21d"] = [ridge_predict(model, row) for row in frame[RECOMMENDATION_FEATURES].to_dict("records")]
    frame["target_upside"] = (
        pd.to_numeric(frame["price_target_mean"], errors="coerce") / pd.to_numeric(frame["last_close"], errors="coerce")
    ) - 1
    frame["analyst_factor"] = -pd.to_numeric(frame["analyst_rating_score"], errors="coerce")
    frame["insider_factor"] = frame["insider_buy_flag"].fillna(False).astype(float)
    frame["factor_score"] = (
        0.22 * standard_score(frame["momentum_12_1"])
        + 0.18 * standard_score(frame["return_6m"])
        + 0.14 * standard_score(frame["return_3m"])
        + 0.14 * standard_score(frame["trend_200"])
        + 0.08 * standard_score(frame["trend_50"])
        - 0.10 * standard_score(frame["volatility_21d"])
        + 0.06 * standard_score(frame["target_upside"])
        + 0.04 * standard_score(frame["analyst_factor"])
        + 0.04 * standard_score(frame["institutions_percent_held"])
        + 0.02 * standard_score(frame["insider_factor"])
    )
    ml_component = standard_score(frame["ml_expected_21d"]) if model.get("trained") else 0
    frame["quant_score"] = 0.55 * ml_component + 0.45 * frame["factor_score"]
    frame = frame.sort_values("quant_score", ascending=False, na_position="last")

    def output_rows(selected: pd.DataFrame, signal: str) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for rank, row in enumerate(selected.to_dict("records"), start=1):
            score = safe_float(row.get("quant_score")) or 0
            confidence = "High" if abs(score) >= 1.25 else "Medium" if abs(score) >= 0.65 else "Low"
            payload_row = {
                "rank": rank,
                "signal": signal,
                "confidence": confidence,
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "sector": row.get("sector"),
                "industry": row.get("industry"),
                "last_date": row.get("last_date"),
                "last_close": row.get("last_close"),
                "ml_expected_21d": row.get("ml_expected_21d"),
                "quant_score": row.get("quant_score"),
                "momentum_12_1": row.get("momentum_12_1"),
                "return_3m": row.get("return_3m"),
                "return_1m": row.get("return_1m"),
                "rsi_14": row.get("rsi_14"),
                "distance_from_sma_200": row.get("distance_from_sma_200"),
                "analyst_rating_score": row.get("analyst_rating_score"),
                "target_upside": row.get("target_upside"),
                "reason": recommendation_reason(row, signal),
            }
            rows.append({key: jsonable(value) for key, value in payload_row.items()})
        return rows

    buy = output_rows(frame.head(25), "BUY")
    sell = output_rows(frame.tail(25).sort_values("quant_score", ascending=True), "SELL")
    payload = {
        "status": "ready",
        "as_of": max(as_of_dates) if as_of_dates else None,
        "universe": "S&P 500",
        "disclaimer": "Beta model output for research only. Not financial advice.",
        "methodology": [
            "Historical supervised ridge regression trained on each stock's rolling technical features to forecast next 21 trading day return.",
            "Cross-sectional quant factor ensemble using 12-1 momentum, 6M/3M/1M returns, 50D/200D trend, volatility penalty, analyst rating, target upside, institutional ownership, and insider buy flag.",
            "Final score blends the ML forecast with the factor ensemble; highest scores form the buy list and lowest scores form the sell list.",
        ],
        "model": {
            "name": "Local ridge regression plus quant factor ensemble",
            "target": "next 21 trading day return",
            "training_samples": model.get("sample_count", 0),
            "trained": bool(model.get("trained")),
            "features": RECOMMENDATION_FEATURES,
            "coefficients": model.get("coefficients", {}),
        },
        "buy": buy,
        "sell": sell,
    }
    RECOMMENDATION_CACHE = {"store_loaded_at": STORE_LOADED_AT, "payload": payload}
    write_json(RECOMMENDATIONS_PATH, payload)
    return {**payload, "buy": buy[:limit], "sell": sell[:limit]}


def collect_recommendation_training_data(store: DataStore) -> tuple[list[dict[str, float]], list[dict[str, object]], list[str]]:
    universe_meta = {
        safe_symbol(row.get("symbol", "")): row
        for row in store.stocks.to_dict("records")
        if row.get("symbol")
    }
    training_samples: list[dict[str, float]] = []
    latest_rows: list[dict[str, object]] = []
    as_of_dates: list[str] = []

    for path in sorted(TECHNICALS_DIR.glob("*.csv")):
        frame = load_recommendation_frame(path)
        if frame is None:
            continue
        close_col = "adj_close" if "adj_close" in frame.columns else "close"
        close = numeric_series(frame, close_col)
        key = path.stem.upper()
        meta = universe_meta.get(key, {})
        for index in range(252, len(frame) - 21, 21):
            features = technical_feature_row(frame, index)
            if not features:
                continue
            current = finite_float(close.iloc[index])
            future = finite_float(close.iloc[index + 21])
            target = pct_change(current, future)
            if target is None or target < -0.75 or target > 1.5:
                continue
            training_samples.append(
                {
                    **features,
                    "target_21d": float(target),
                    "sector_code": float(abs(hash(str(meta.get("sector", "")))) % 97) / 97,
                    "sample_age": float((len(frame) - index) / max(1, len(frame))),
                }
            )

        latest_features = technical_feature_row(frame, len(frame) - 1)
        if not latest_features:
            continue
        current = finite_float(close.iloc[-1])
        sma_200 = finite_float(frame.iloc[-1].get("sma_200"))
        latest_rows.append(
            {
                **latest_features,
                "symbol": meta.get("symbol") or key,
                "name": meta.get("name", ""),
                "sector": meta.get("sector", ""),
                "industry": meta.get("industry", ""),
                "last_date": frame.iloc[-1]["date"].date().isoformat(),
                "last_close": current,
                "rsi_14": finite_float(frame.iloc[-1].get("rsi_14")),
                "sma_200": sma_200,
                "distance_from_sma_200": pct_change(sma_200, current),
                "insider_buy_flag": bool(meta.get("insider_buy_flag")),
                "institutions_percent_held": safe_float(meta.get("institutions_percent_held")),
                "analyst_rating_score": safe_float(meta.get("analyst_rating_score")),
                "price_target_mean": safe_float(meta.get("price_target_mean")),
            }
        )
        as_of_dates.append(frame.iloc[-1]["date"].date().isoformat())

    return training_samples, latest_rows, as_of_dates


def advanced_recommendation_payload(
    store: DataStore,
    limit: int = 15,
    allow_compute: bool = True,
    force_rebuild: bool = False,
) -> dict[str, object]:
    global ADVANCED_RECOMMENDATION_CACHE
    if (
        not force_rebuild
        and ADVANCED_RECOMMENDATION_CACHE["store_loaded_at"] == STORE_LOADED_AT
        and ADVANCED_RECOMMENDATION_CACHE["payload"] is not None
    ):
        payload = ADVANCED_RECOMMENDATION_CACHE["payload"]
        return limited_recommendation_payload(payload, limit)

    if not force_rebuild and ADVANCED_RECOMMENDATIONS_PATH.exists():
        payload = load_json(ADVANCED_RECOMMENDATIONS_PATH)
        ADVANCED_RECOMMENDATION_CACHE = {"store_loaded_at": STORE_LOADED_AT, "payload": payload}
        if not recommendation_cache_fresh(ADVANCED_RECOMMENDATIONS_PATH):
            payload = {**payload, "status": "stale_rebuilding", "message": "Showing cached advanced recommendations while rebuilding from newer data."}
            start_recommendation_build("advanced")
        return limited_recommendation_payload(payload, limit)

    if not allow_compute:
        start_recommendation_build("advanced")
        return building_recommendation_payload("advanced")

    try:
        from sklearn.decomposition import PCA
        from sklearn.ensemble import ExtraTreesRegressor, HistGradientBoostingRegressor, RandomForestRegressor
        from sklearn.linear_model import ElasticNetCV, HuberRegressor
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import RobustScaler
    except ImportError as exc:
        base = recommendation_payload(store, limit=25, allow_compute=allow_compute)
        payload = {
            **base,
            "status": "fallback",
            "advanced_unavailable": True,
            "error": f"Advanced ML libraries are not installed in this environment: {exc}",
            "model": {
                **base.get("model", {}),
                "name": "Fallback local ridge regression plus quant factor ensemble",
            },
            "methodology": [
                "Advanced model unavailable locally because scikit-learn/scipy are missing.",
                "Render will install scikit-learn/scipy from requirements.txt and use the full advanced ensemble.",
                *base.get("methodology", []),
            ],
        }
        return limited_recommendation_payload(payload, limit)

    training_samples, latest_rows, as_of_dates = collect_recommendation_training_data(store)
    frame = pd.DataFrame(latest_rows)
    if len(training_samples) < 500 or frame.empty:
        return {"error": "Not enough data for advanced recommendations", "buy": [], "sell": []}

    advanced_features = [
        *RECOMMENDATION_FEATURES,
        "target_upside",
        "analyst_factor",
        "institutions_percent_held",
        "insider_factor",
    ]
    train = pd.DataFrame(training_samples)
    train["target_upside"] = 0.0
    train["analyst_factor"] = 0.0
    train["institutions_percent_held"] = 0.0
    train["insider_factor"] = 0.0
    frame["target_upside"] = (
        pd.to_numeric(frame["price_target_mean"], errors="coerce") / pd.to_numeric(frame["last_close"], errors="coerce")
    ) - 1
    frame["analyst_factor"] = -pd.to_numeric(frame["analyst_rating_score"], errors="coerce")
    frame["insider_factor"] = frame["insider_buy_flag"].fillna(False).astype(float)

    train_x = train[advanced_features].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    train_y = np.clip(pd.to_numeric(train["target_21d"], errors="coerce").fillna(0.0), -0.5, 0.5)
    latest_x = frame[advanced_features].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # Keep the heaviest ensemble bounded so the first Render request finishes predictably.
    if len(train_x) > 80_000:
        train_x = train_x.tail(80_000)
        train_y = train_y.tail(80_000)

    models = {
        "elastic_net": make_pipeline(RobustScaler(), ElasticNetCV(l1_ratio=[0.15, 0.5, 0.85], cv=4, max_iter=5000)),
        "huber": make_pipeline(RobustScaler(), HuberRegressor(epsilon=1.35, alpha=0.0005, max_iter=700)),
        "hist_gradient_boosting": HistGradientBoostingRegressor(max_iter=220, learning_rate=0.045, l2_regularization=0.08, random_state=42),
        "extra_trees": ExtraTreesRegressor(n_estimators=160, max_depth=8, min_samples_leaf=25, random_state=42, n_jobs=-1),
        "random_forest": RandomForestRegressor(n_estimators=120, max_depth=8, min_samples_leaf=30, random_state=42, n_jobs=-1),
    }

    predictions: dict[str, np.ndarray] = {}
    for name, model in models.items():
        model.fit(train_x, train_y)
        predictions[name] = np.clip(model.predict(latest_x), -0.5, 0.5)

    pca = PCA(n_components=min(4, len(advanced_features)), random_state=42)
    scaled_latest = RobustScaler().fit_transform(latest_x)
    principal = pca.fit_transform(scaled_latest)
    frame["pca_quality"] = standard_score(pd.Series(principal[:, 0], index=frame.index))
    frame["ml_ensemble_21d"] = np.mean(np.column_stack(list(predictions.values())), axis=1)
    frame["ml_dispersion"] = np.std(np.column_stack(list(predictions.values())), axis=1)
    frame["sector_neutral_score"] = (
        standard_score(frame["ml_ensemble_21d"])
        + 0.30 * standard_score(frame["momentum_12_1"])
        + 0.22 * standard_score(frame["trend_200"])
        - 0.18 * standard_score(frame["volatility_21d"])
        + 0.10 * standard_score(frame["target_upside"])
        + 0.08 * standard_score(frame["analyst_factor"])
    )
    frame["sector_neutral_score"] = frame["sector_neutral_score"] - frame.groupby("sector")["sector_neutral_score"].transform("median")
    frame["advanced_score"] = (
        0.52 * standard_score(frame["ml_ensemble_21d"])
        + 0.28 * standard_score(frame["sector_neutral_score"])
        + 0.12 * standard_score(frame["pca_quality"])
        - 0.08 * standard_score(frame["ml_dispersion"])
    )
    frame = frame.sort_values("advanced_score", ascending=False, na_position="last")

    def advanced_reason(row: dict[str, object], signal: str) -> str:
        parts: list[str] = []
        ensemble = safe_float(row.get("ml_ensemble_21d"))
        dispersion = safe_float(row.get("ml_dispersion"))
        sector_score = safe_float(row.get("sector_neutral_score"))
        trend_200 = safe_float(row.get("distance_from_sma_200"))
        if signal == "BUY":
            if ensemble is not None and ensemble > 0:
                parts.append("positive multi-model forecast")
            if sector_score is not None and sector_score > 0:
                parts.append("sector-neutral strength")
            if trend_200 is not None and trend_200 > 0:
                parts.append("above 200D SMA")
        else:
            if ensemble is not None and ensemble < 0.01:
                parts.append("weak multi-model forecast")
            if sector_score is not None and sector_score < 0:
                parts.append("sector-neutral weakness")
            if trend_200 is not None and trend_200 < 0:
                parts.append("below 200D SMA")
        if dispersion is not None and dispersion < 0.02:
            parts.append("models agree")
        return ", ".join(parts[:4]) or recommendation_reason(row, signal)

    def rows(selected: pd.DataFrame, signal: str) -> list[dict[str, object]]:
        output: list[dict[str, object]] = []
        for rank, row in enumerate(selected.to_dict("records"), start=1):
            score = safe_float(row.get("advanced_score")) or 0
            dispersion = safe_float(row.get("ml_dispersion")) or 0
            confidence = "High" if abs(score) >= 1.15 and dispersion < 0.025 else "Medium" if abs(score) >= 0.55 else "Low"
            item = {
                "rank": rank,
                "signal": signal,
                "confidence": confidence,
                "symbol": row.get("symbol"),
                "name": row.get("name"),
                "sector": row.get("sector"),
                "industry": row.get("industry"),
                "last_date": row.get("last_date"),
                "last_close": row.get("last_close"),
                "ml_expected_21d": row.get("ml_ensemble_21d"),
                "advanced_score": row.get("advanced_score"),
                "model_agreement": 1 - min(1, dispersion / 0.05),
                "sector_neutral_score": row.get("sector_neutral_score"),
                "momentum_12_1": row.get("momentum_12_1"),
                "return_3m": row.get("return_3m"),
                "return_1m": row.get("return_1m"),
                "rsi_14": row.get("rsi_14"),
                "distance_from_sma_200": row.get("distance_from_sma_200"),
                "target_upside": row.get("target_upside"),
                "reason": advanced_reason(row, signal),
            }
            output.append({key: jsonable(value) for key, value in item.items()})
        return output

    buy = rows(frame.head(30), "BUY")
    sell = rows(frame.tail(30).sort_values("advanced_score", ascending=True), "SELL")
    payload = {
        "status": "ready",
        "as_of": max(as_of_dates) if as_of_dates else None,
        "universe": "S&P 500",
        "disclaimer": "Advanced beta model output for research only. Not financial advice.",
        "methodology": [
            "Machine learning ensemble: ElasticNetCV, Huber regression, histogram gradient boosting, extra trees, and random forest.",
            "Statistical layer: robust scaling, PCA factor extraction, sector-neutral residual scoring, model-agreement penalty, and volatility penalty.",
            "Final score blends multi-model expected 21D return, sector-neutral strength, PCA quality factor, and forecast agreement.",
        ],
        "model": {
            "name": "Advanced statistical and machine learning ensemble",
            "target": "next 21 trading day return",
            "training_samples": int(len(train_x)),
            "raw_training_samples": int(len(training_samples)),
            "models": list(models.keys()),
            "features": advanced_features,
            "pca_components": int(pca.n_components_),
        },
        "buy": buy,
        "sell": sell,
    }
    ADVANCED_RECOMMENDATION_CACHE = {"store_loaded_at": STORE_LOADED_AT, "payload": payload}
    write_json(ADVANCED_RECOMMENDATIONS_PATH, payload)
    return limited_recommendation_payload(payload, limit)


def current_store() -> DataStore:
    global STORE, STORE_LOADED_AT, STORE_RELOAD_CHECKED_AT
    now = time.time()
    if now - STORE_RELOAD_CHECKED_AT < 60:
        return STORE
    STORE_RELOAD_CHECKED_AT = now

    marker_mtime = REFRESH_MARKER_PATH.stat().st_mtime if REFRESH_MARKER_PATH.exists() else 0
    if (STORE.universe.empty and UNIVERSE_PATH.exists()) or marker_mtime > STORE_LOADED_AT:
        STORE = DataStore.load()
        STORE_LOADED_AT = now
    return STORE


def refresh_enabled() -> bool:
    return os.environ.get("SENQUANT_ENABLE_DAILY_REFRESH", "").lower() in {"1", "true", "yes", "on"}


def next_refresh_delay_seconds() -> float:
    timezone_name = os.environ.get("SENQUANT_REFRESH_TIMEZONE", "America/New_York")
    hour = int(os.environ.get("SENQUANT_REFRESH_HOUR", "17"))
    minute = int(os.environ.get("SENQUANT_REFRESH_MINUTE", "30"))
    tz = ZoneInfo(timezone_name)
    now = datetime.now(tz)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    marker_day = None
    if REFRESH_MARKER_PATH.exists():
        marker_day = datetime.fromtimestamp(REFRESH_MARKER_PATH.stat().st_mtime, tz).date()
    if now.weekday() < 5 and now >= target and marker_day != now.date():
        return 60

    if now >= target:
        target += timedelta(days=1)
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return max(60, (target - now).total_seconds())


def daily_refresh_loop() -> None:
    from datacollection.daily_refresh import refresh_daily_market_data

    while True:
        delay = next_refresh_delay_seconds()
        print(f"Next daily market refresh in {delay / 3600:.2f} hours", flush=True)
        time.sleep(delay)
        try:
            result = refresh_daily_market_data()
            print(f"Daily market refresh complete: {result}", flush=True)
            start_recommendation_build("basic")
            start_advanced_recommendation_build_later(delay_seconds=120)
        except Exception as exc:  # noqa: BLE001
            print(f"Daily market refresh failed: {exc}", flush=True)


def start_daily_refresh_thread() -> None:
    if not refresh_enabled():
        return
    thread = threading.Thread(target=daily_refresh_loop, name="daily-market-refresh", daemon=True)
    thread.start()


def start_recommendation_prewarm_threads() -> None:
    if not recommendation_cache_fresh(RECOMMENDATIONS_PATH):
        start_recommendation_build("basic")
    if not recommendation_cache_fresh(ADVANCED_RECOMMENDATIONS_PATH):
        start_advanced_recommendation_build_later(delay_seconds=120)


class AppHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            store = current_store()
            if parsed.path == "/":
                self.serve_file(STATIC_ROOT / "index.html")
            elif parsed.path.startswith("/static/"):
                self.serve_file(STATIC_ROOT / parsed.path.removeprefix("/static/"))
            elif parsed.path == "/health":
                self.send_json(
                    {
                        "ok": True,
                        "data_root": str(DATA_ROOT),
                        "has_universe": UNIVERSE_PATH.exists(),
                        "constituents": int(len(store.universe)),
                        "daily_refresh_enabled": refresh_enabled(),
                        "ollama_chat": ollama_chat_status(),
                        "refresh_marker": REFRESH_MARKER_PATH.read_text(encoding="utf-8").strip()
                        if REFRESH_MARKER_PATH.exists()
                        else None,
                    }
                )
            elif parsed.path == "/api/summary":
                self.send_json(
                    {
                        "constituents": int(len(store.universe)),
                        "with_prices": int(store.stocks["has_prices"].sum()),
                        "with_technicals": int(store.stocks["has_technicals"].sum()),
                        "with_fundamentals": int(store.stocks["has_fundamentals"].sum()),
                        "with_enrichment": int(store.stocks["has_enrichment"].sum()),
                        "sectors": store.sectors,
                        "exchanges": store.exchanges,
                        "generated_at": store.generated_at,
                    }
                )
            elif parsed.path == "/api/stocks":
                self.send_json(store.filter_stocks(parse_qs(parsed.query)))
            elif parsed.path == "/api/market-news":
                self.send_json(fetch_or_load_market_news())
            elif parsed.path == "/api/momentum":
                limit = safe_int(parse_qs(parsed.query).get("limit", ["10"])[0]) or 10
                self.send_json(store.momentum_recommendations(limit=limit))
            elif parsed.path == "/api/group-momentum":
                limit = safe_int(parse_qs(parsed.query).get("limit", ["3"])[0]) or 3
                self.send_json(store.group_momentum_leaders(limit=limit))
            elif parsed.path == "/api/chat/status":
                self.send_json(ollama_chat_status())
            elif parsed.path == "/api/recommendations":
                limit = safe_int(parse_qs(parsed.query).get("limit", ["15"])[0]) or 15
                self.send_json(recommendation_payload(store, limit=limit, allow_compute=False))
            elif parsed.path == "/api/recommendations/advanced":
                limit = safe_int(parse_qs(parsed.query).get("limit", ["15"])[0]) or 15
                self.send_json(advanced_recommendation_payload(store, limit=limit, allow_compute=False))
            elif parsed.path.startswith("/api/sector/") and parsed.path.endswith("/news"):
                sector = unquote(parsed.path.removeprefix("/api/sector/").removesuffix("/news"))
                self.send_json(fetch_sector_news(sector))
            elif parsed.path.startswith("/api/sector/"):
                sector = unquote(parsed.path.removeprefix("/api/sector/"))
                self.send_json(store.sector_detail(sector))
            elif parsed.path.startswith("/api/stock/") and parsed.path.endswith("/fundamentals"):
                symbol = unquote(parsed.path.split("/")[3])
                self.send_json(store.fundamentals(symbol, parse_qs(parsed.query)))
            elif parsed.path.startswith("/api/stock/") and parsed.path.endswith("/enrichment"):
                symbol = unquote(parsed.path.split("/")[3])
                self.send_json(store.enrichment(symbol))
            elif parsed.path.startswith("/api/stock/") and parsed.path.endswith("/news"):
                symbol = unquote(parsed.path.split("/")[3])
                self.send_json(fetch_or_load_news(symbol))
            elif parsed.path.startswith("/api/stock/") and parsed.path.endswith("/social"):
                symbol = unquote(parsed.path.split("/")[3])
                self.send_json(load_social_posts(symbol))
            elif parsed.path.startswith("/api/stock/"):
                symbol = unquote(parsed.path.split("/")[3])
                self.send_json(store.stock_detail(symbol, parse_qs(parsed.query)))
            else:
                self.send_error(HTTPStatus.NOT_FOUND)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=500)

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            if parsed.path != "/api/chat":
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            length = safe_int(self.headers.get("Content-Length")) or 0
            raw = self.rfile.read(min(length, 64_000)) if length else b"{}"
            payload = json.loads(raw.decode("utf-8") or "{}")
            if not isinstance(payload, dict):
                raise ValueError("Expected a JSON object")
            self.send_json(chat_response(current_store(), payload))
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=500)

    def serve_file(self, path: Path) -> None:
        resolved = path.resolve()
        if not str(resolved).startswith(str(STATIC_ROOT.resolve())) or not resolved.exists():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        content_type = mimetypes.guess_type(resolved.name)[0] or "application/octet-stream"
        payload = resolved.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def send_json(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        if not re.search(r"/api/stock/.+", self.path):
            super().log_message(format, *args)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SenQuant local data browser.")
    parser.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    start_daily_refresh_thread()
    start_recommendation_prewarm_threads()
    print(f"SenQuant Data Browser running at http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
