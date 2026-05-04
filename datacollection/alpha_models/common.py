from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from datacollection.config import data_root as configured_data_root
from datacollection.storage import write_json


MIN_HISTORY_DAYS = 280
MIN_DOLLAR_VOLUME = 10_000_000


@dataclass(frozen=True)
class AlphaData:
    data_root: Path
    universe: pd.DataFrame
    technicals: dict[str, pd.DataFrame]
    fundamentals: dict[str, pd.DataFrame]
    enrichment: dict[str, dict[str, Any]]


def alpha_data_root(data_root: Path | None = None) -> Path:
    return (data_root or configured_data_root()).expanduser()


def symbol_key(symbol: object) -> str:
    return str(symbol or "").upper().replace(".", "-")


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


def jsonable(value: object) -> object:
    if isinstance(value, np.generic):
        return jsonable(value.item())
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return value.isoformat()
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if pd.isna(value):
        return None
    return value


def pct_change(first: float | None, last: float | None) -> float | None:
    if first in (None, 0) or last is None:
        return None
    return (last / first) - 1


def robust_zscore(values: pd.Series, clip: float = 3.0) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    median = numeric.median()
    mad = (numeric - median).abs().median()
    if pd.isna(mad) or mad == 0:
        std = numeric.std(ddof=0)
        if pd.isna(std) or std == 0:
            return pd.Series(0.0, index=values.index)
        score = (numeric - median) / std
    else:
        score = 0.6745 * (numeric - median) / mad
    return score.clip(-clip, clip).fillna(0.0)


def sector_neutralize(frame: pd.DataFrame, score_col: str, sector_col: str = "sector") -> pd.Series:
    if frame.empty or score_col not in frame.columns or sector_col not in frame.columns:
        return pd.Series(dtype=float)
    scores = pd.to_numeric(frame[score_col], errors="coerce").fillna(0.0)
    medians = scores.groupby(frame[sector_col].fillna("")).transform("median")
    return (scores - medians).fillna(0.0)


def confidence_from_score(score: object, high: float = 1.2, medium: float = 0.55) -> str:
    value = abs(safe_float(score) or 0.0)
    if value >= high:
        return "High"
    if value >= medium:
        return "Medium"
    return "Low"


def load_universe(root: Path, symbols: list[str] | None = None) -> pd.DataFrame:
    path = root / "universe" / "sp500_constituents.csv"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "name", "sector", "industry"])
    universe = pd.read_csv(path, dtype={"cik": str}).fillna("")
    universe["symbol_key"] = universe["symbol"].map(symbol_key)
    if symbols:
        selected = {symbol_key(symbol) for symbol in symbols}
        universe = universe[universe["symbol_key"].isin(selected)]
    return universe.reset_index(drop=True)


def load_technical_file(path: Path) -> pd.DataFrame | None:
    try:
        frame = pd.read_csv(path)
    except Exception:
        return None
    if "date" not in frame.columns:
        return None
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    close_col = "adj_close" if "adj_close" in frame.columns else "close"
    if close_col not in frame.columns:
        return None
    frame["_close"] = pd.to_numeric(frame[close_col], errors="coerce")
    frame["_dollar_volume"] = frame["_close"] * pd.to_numeric(frame.get("volume"), errors="coerce")
    frame = frame.dropna(subset=["_close"])
    if len(frame) < MIN_HISTORY_DAYS:
        return None
    return frame


def load_fundamental_file(path: Path) -> pd.DataFrame | None:
    try:
        frame = pd.read_csv(path)
    except Exception:
        return None
    required = {"metric", "value", "end", "filed", "form"}
    if not required.issubset(frame.columns):
        return None
    frame["end"] = pd.to_datetime(frame["end"], errors="coerce")
    frame["filed"] = pd.to_datetime(frame["filed"], errors="coerce")
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    return frame.dropna(subset=["metric", "value"]).sort_values(["metric", "end", "filed"])


def load_enrichment_file(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_alpha_data(data_root: Path | None = None, symbols: list[str] | None = None) -> AlphaData:
    root = alpha_data_root(data_root)
    universe = load_universe(root, symbols=symbols)
    keys = set(universe["symbol_key"].astype(str)) if not universe.empty else None

    technicals: dict[str, pd.DataFrame] = {}
    for path in sorted((root / "technicals" / "from_yahoo_daily").glob("*.csv")):
        key = path.stem.upper()
        if keys is not None and key not in keys:
            continue
        frame = load_technical_file(path)
        if frame is not None:
            technicals[key] = frame

    fundamentals: dict[str, pd.DataFrame] = {}
    for path in sorted((root / "fundamentals" / "sec_companyfacts" / "long").glob("*.csv")):
        key = path.stem.upper()
        if keys is not None and key not in keys:
            continue
        frame = load_fundamental_file(path)
        if frame is not None:
            fundamentals[key] = frame

    enrichment: dict[str, dict[str, Any]] = {}
    for path in sorted((root / "enrichment" / "yahoo_quote_summary").glob("*.json")):
        key = path.stem.upper()
        if keys is not None and key not in keys:
            continue
        enrichment[key] = load_enrichment_file(path)

    return AlphaData(
        data_root=root,
        universe=universe,
        technicals=technicals,
        fundamentals=fundamentals,
        enrichment=enrichment,
    )


def latest_meta(data: AlphaData, key: str) -> dict[str, Any]:
    if data.universe.empty:
        return {"symbol": key, "name": "", "sector": "", "industry": ""}
    rows = data.universe[data.universe["symbol_key"] == key]
    if rows.empty:
        return {"symbol": key, "name": "", "sector": "", "industry": ""}
    row = rows.iloc[0].to_dict()
    return {
        "symbol": row.get("symbol") or key,
        "name": row.get("name", ""),
        "sector": row.get("sector", ""),
        "industry": row.get("industry", ""),
    }


def trailing_return(frame: pd.DataFrame, days: int, end_index: int | None = None) -> float | None:
    if frame.empty:
        return None
    end_index = len(frame) - 1 if end_index is None else end_index
    start_index = end_index - days
    if start_index < 0 or end_index >= len(frame):
        return None
    return pct_change(safe_float(frame.iloc[start_index]["_close"]), safe_float(frame.iloc[end_index]["_close"]))


def latest_liquidity(frame: pd.DataFrame) -> float:
    dollar_volume = pd.to_numeric(frame.get("_dollar_volume"), errors="coerce").tail(21).median()
    return float(dollar_volume) if pd.notna(dollar_volume) else 0.0


def latest_stock_snapshot(data: AlphaData, key: str) -> dict[str, Any] | None:
    frame = data.technicals.get(key)
    if frame is None or frame.empty:
        return None
    liquidity = latest_liquidity(frame)
    if liquidity < MIN_DOLLAR_VOLUME:
        return None
    latest = frame.iloc[-1]
    current = safe_float(latest.get("_close"))
    if current is None:
        return None
    sma_50 = safe_float(latest.get("sma_50"))
    sma_200 = safe_float(latest.get("sma_200"))
    meta = latest_meta(data, key)
    return {
        **meta,
        "symbol_key": key,
        "last_date": latest["date"].date().isoformat(),
        "last_close": current,
        "dollar_volume": liquidity,
        "return_1m": trailing_return(frame, 21),
        "return_3m": trailing_return(frame, 63),
        "return_6m": trailing_return(frame, 126),
        "momentum_12_1": pct_change(
            safe_float(frame.iloc[-253]["_close"]) if len(frame) > 253 else None,
            safe_float(frame.iloc[-22]["_close"]) if len(frame) > 22 else None,
        ),
        "trend_50": pct_change(sma_50, current),
        "trend_200": pct_change(sma_200, current),
        "rsi_14": safe_float(latest.get("rsi_14")),
        "volatility_21d": safe_float(latest.get("volatility_21d")),
    }


def first_numeric(payload: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = safe_float(payload.get(key))
        if value is not None:
            return value
    return None


def enrichment_summary(payload: dict[str, Any]) -> dict[str, float | bool | None]:
    targets = payload.get("analyst_price_targets")
    targets = targets if isinstance(targets, dict) else {}
    rec_rows = payload.get("tables", {}).get("recommendations_summary", []) if isinstance(payload.get("tables"), dict) else []
    rec = rec_rows[0] if rec_rows and isinstance(rec_rows[0], dict) else {}
    weights = {"strongBuy": 1, "buy": 2, "hold": 3, "sell": 4, "strongSell": 5}
    rec_total = 0
    rec_score = 0.0
    for column, weight in weights.items():
        count = int(safe_float(rec.get(column)) or 0)
        rec_total += count
        rec_score += count * weight
    return {
        "analyst_rating_score": (rec_score / rec_total) if rec_total else None,
        "analyst_rating_count": rec_total or None,
        "price_target_mean": safe_float(targets.get("mean")),
        "institutions_percent_held": first_numeric(payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}, ("institutionsPercentHeld", "institutions_percent_held")),
    }


def write_alpha_json(path: Path, payload: dict[str, Any]) -> Path:
    cleaned = clean_payload(payload)
    return write_json(path, cleaned)


def clean_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): clean_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [clean_payload(item) for item in value]
    return jsonable(value)
