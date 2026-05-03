from __future__ import annotations

import os
import time
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

import pandas as pd
from tqdm import tqdm

from .config import data_root
from .storage import ensure_dir, write_dataframe
from .technicals import compute_from_price_dir
from .universe import save_sp500_universe
from .yahoo_prices import fetch_yahoo_daily_prices


REFRESH_MARKER = ".senquant_refresh_complete"


def refresh_marker_path() -> Path:
    return data_root() / REFRESH_MARKER


def universe_path() -> Path:
    return data_root() / "universe" / "sp500_constituents.csv"


def load_or_create_universe() -> pd.DataFrame:
    path = universe_path()
    if path.exists():
        return pd.read_csv(path, dtype={"cik": str})
    save_sp500_universe(path)
    return pd.read_csv(path, dtype={"cik": str})


def record_errors(errors: list[dict[str, str]], name: str) -> None:
    if errors:
        write_dataframe(data_root() / "errors" / f"{name}.csv", pd.DataFrame(errors))


@contextmanager
def refresh_lock(max_age_seconds: int = 6 * 60 * 60) -> Iterator[None]:
    root = data_root()
    lock = root / ".senquant_daily_refresh.lock"
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        age = time.time() - lock.stat().st_mtime
        if age <= max_age_seconds:
            raise RuntimeError(f"Daily refresh already appears to be running: {lock}")
        lock.unlink(missing_ok=True)
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(f"{os.getpid()}\n")
        yield
    finally:
        lock.unlink(missing_ok=True)


def symbol_key(symbol: str) -> str:
    return symbol.replace(".", "-")


def latest_price_date(path: Path) -> date | None:
    if not path.exists():
        return None
    try:
        frame = pd.read_csv(path, usecols=["date"])
    except Exception:  # noqa: BLE001
        return None
    if frame.empty:
        return None
    parsed = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if parsed.empty:
        return None
    return parsed.max().date()


def merge_price_file(path: Path, updates: pd.DataFrame) -> None:
    if path.exists():
        current = pd.read_csv(path)
        combined = pd.concat([current, updates], ignore_index=True)
    else:
        combined = updates
    combined = combined.drop_duplicates(subset=["date"], keep="last").sort_values("date")
    write_dataframe(path, combined)


def merge_actions_file(path: Path, updates: pd.DataFrame) -> None:
    if updates.empty:
        return
    if path.exists():
        current = pd.read_csv(path)
        combined = pd.concat([current, updates], ignore_index=True)
    else:
        combined = updates
    keys = [column for column in ("date", "event_type", "amount", "splitRatio") if column in combined.columns]
    combined = combined.drop_duplicates(subset=keys or None, keep="last").sort_values("date")
    write_dataframe(path, combined)


def refresh_daily_market_data(
    *,
    symbols: list[str] | None = None,
    lookback_days: int = 10,
    update_universe: bool = True,
    end: date | None = None,
) -> dict[str, object]:
    end = end or date.today()
    root = data_root()
    price_dir = ensure_dir(root / "prices" / "yahoo_daily")
    action_dir = ensure_dir(price_dir / "corporate_actions")
    errors: list[dict[str, str]] = []

    with refresh_lock():
        if update_universe:
            try:
                save_sp500_universe(universe_path())
            except Exception as exc:  # noqa: BLE001
                errors.append({"symbol": "*", "stage": "universe", "error": str(exc)})

        universe = load_or_create_universe()
        if symbols:
            selected = {symbol.upper() for symbol in symbols}
            universe = universe[universe["symbol"].astype(str).str.upper().isin(selected)]

        updated = 0
        for row in tqdm(universe.to_dict("records"), desc="Daily Yahoo price refresh"):
            symbol = str(row["symbol"])
            key = symbol_key(symbol)
            path = price_dir / f"{key}.csv"
            last = latest_price_date(path)
            start = (last - timedelta(days=lookback_days)) if last else date(1990, 1, 1)
            try:
                prices, actions = fetch_yahoo_daily_prices(symbol, start=start, end=end)
                if prices.empty:
                    continue
                merge_price_file(path, prices)
                merge_actions_file(action_dir / f"{key}.csv", actions)
                updated += 1
            except Exception as exc:  # noqa: BLE001
                errors.append({"symbol": symbol, "stage": "prices", "error": str(exc)})

        technical_files = compute_from_price_dir(
            price_dir=price_dir,
            output_dir=root / "technicals" / "from_yahoo_daily",
        )
        record_errors(errors, "daily_refresh")
        marker = refresh_marker_path()
        marker.write_text(pd.Timestamp.now("UTC").isoformat(), encoding="utf-8")
        return {
            "updated_price_files": updated,
            "technical_files": len(technical_files),
            "errors": len(errors),
            "marker": str(marker),
        }
