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
from zoneinfo import ZoneInfo

import pandas as pd

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
        except Exception as exc:  # noqa: BLE001
            print(f"Daily market refresh failed: {exc}", flush=True)


def start_daily_refresh_thread() -> None:
    if not refresh_enabled():
        return
    thread = threading.Thread(target=daily_refresh_loop, name="daily-market-refresh", daemon=True)
    thread.start()


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
    print(f"SenQuant Data Browser running at http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
