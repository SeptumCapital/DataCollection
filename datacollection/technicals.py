from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import data_root
from .storage import ensure_dir, write_dataframe


def compute_technical_features(prices: pd.DataFrame) -> pd.DataFrame:
    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values("date").reset_index(drop=True)

    close = pd.to_numeric(frame["adj_close"].fillna(frame["close"]), errors="coerce")
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce")

    frame["return_1d"] = close.pct_change()
    frame["return_5d"] = close.pct_change(5)
    frame["return_21d"] = close.pct_change(21)
    frame["sma_20"] = close.rolling(20).mean()
    frame["sma_50"] = close.rolling(50).mean()
    frame["sma_200"] = close.rolling(200).mean()
    frame["ema_12"] = close.ewm(span=12, adjust=False).mean()
    frame["ema_26"] = close.ewm(span=26, adjust=False).mean()
    frame["macd"] = frame["ema_12"] - frame["ema_26"]
    frame["macd_signal"] = frame["macd"].ewm(span=9, adjust=False).mean()
    frame["macd_hist"] = frame["macd"] - frame["macd_signal"]

    delta = close.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss
    frame["rsi_14"] = 100 - (100 / (1 + rs))

    rolling_std = close.rolling(20).std()
    frame["bb_mid_20"] = frame["sma_20"]
    frame["bb_upper_20"] = frame["sma_20"] + (2 * rolling_std)
    frame["bb_lower_20"] = frame["sma_20"] - (2 * rolling_std)
    frame["bb_width_20"] = (frame["bb_upper_20"] - frame["bb_lower_20"]) / frame["bb_mid_20"]

    prev_close = close.shift(1)
    true_range = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    frame["atr_14"] = true_range.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()

    direction = close.diff().fillna(0).apply(lambda value: 1 if value > 0 else (-1 if value < 0 else 0))
    frame["obv"] = (direction * volume.fillna(0)).cumsum()
    frame["dollar_volume"] = close * volume
    frame["volatility_21d"] = frame["return_1d"].rolling(21).std()
    frame["volatility_63d"] = frame["return_1d"].rolling(63).std()
    frame["date"] = frame["date"].dt.date.astype(str)
    return frame


def compute_from_price_file(price_file: Path, output_dir: Path | None = None) -> Path:
    if output_dir is None:
        output_dir = data_root() / "technicals" / "from_yahoo_daily"
    ensure_dir(output_dir)
    prices = pd.read_csv(price_file)
    features = compute_technical_features(prices)
    return write_dataframe(output_dir / price_file.name, features)


def compute_from_price_dir(price_dir: Path | None = None, output_dir: Path | None = None) -> list[Path]:
    if price_dir is None:
        price_dir = data_root() / "prices" / "yahoo_daily"
    files = sorted(path for path in price_dir.glob("*.csv") if path.is_file())
    return [compute_from_price_file(path, output_dir=output_dir) for path in files]
