from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from datacollection.alpha_models.common import AlphaData
from datacollection.alpha_models.earnings_drift import generate_earnings_drift_signals
from datacollection.alpha_models.ml_alpha_combiner import generate_ml_alpha_combiner_signals
from datacollection.alpha_models.multifactor import generate_multifactor_signals
from datacollection.alpha_models.orchestrator import build_offline_alpha_recommendations
from datacollection.alpha_models.pairs_trading import generate_pair_trade_signals
from datacollection.alpha_models.residual_stat_arb import generate_residual_stat_arb_signals


def technical_frame(symbol: str, days: int = 340, drift: float = 0.001, spread: np.ndarray | None = None) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=days)
    base = 100 * np.exp(np.linspace(0, drift * days, days))
    cycle = 1 + 0.015 * np.sin(np.linspace(0, 14, days))
    close = base * cycle
    if spread is not None:
        close = close * np.exp(spread)
    frame = pd.DataFrame(
        {
            "symbol": symbol,
            "provider_symbol": symbol,
            "date": dates,
            "open": close,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "adj_close": close,
            "volume": 1_000_000,
        }
    )
    frame["_close"] = frame["adj_close"]
    frame["_dollar_volume"] = frame["_close"] * frame["volume"]
    frame["return_1d"] = frame["_close"].pct_change()
    frame["return_21d"] = frame["_close"].pct_change(21)
    frame["sma_50"] = frame["_close"].rolling(50).mean()
    frame["sma_200"] = frame["_close"].rolling(200).mean()
    frame["rsi_14"] = 50 + 10 * np.sin(np.linspace(0, 10, days))
    frame["volatility_21d"] = frame["return_1d"].rolling(21).std()
    return frame


def fundamentals(symbol: str) -> pd.DataFrame:
    rows = []
    dates = pd.date_range("2023-03-31", periods=6, freq="QE")
    for index, end in enumerate(dates):
        filed = end + pd.Timedelta(days=30)
        scale = 1 + index * 0.08
        for metric, value in {
            "assets": 1_000_000_000 * scale,
            "liabilities": 350_000_000 * scale,
            "gross_profit": 230_000_000 * scale,
            "net_income": 120_000_000 * scale,
            "operating_cash_flow": 150_000_000 * scale,
            "stockholders_equity": 650_000_000 * scale,
            "revenue": 700_000_000 * scale,
            "eps_diluted": 2.0 * scale,
        }.items():
            rows.append(
                {
                    "symbol": symbol,
                    "metric": metric,
                    "value": value,
                    "end": end,
                    "filed": filed,
                    "form": "10-Q",
                }
            )
    return pd.DataFrame(rows)


def alpha_data(symbol_count: int = 10) -> AlphaData:
    universe_rows = []
    technicals = {}
    fundamentals_map = {}
    for index in range(symbol_count):
        symbol = f"T{index}"
        sector = f"Sector {index // 2}"
        spread = None
        if index % 2 == 0:
            spread = 0.005 * np.sin(np.linspace(0, 20, 340))
            spread[-20:] += np.linspace(0, 0.025, 20)
        universe_rows.append(
            {
                "symbol": symbol,
                "symbol_key": symbol,
                "name": f"Test {index}",
                "sector": sector,
                "industry": "Synthetic",
            }
        )
        technicals[symbol] = technical_frame(symbol, drift=0.0005 + index * 0.0001, spread=spread)
        fundamentals_map[symbol] = fundamentals(symbol)
    return AlphaData(
        data_root=Path("."),
        universe=pd.DataFrame(universe_rows),
        technicals=technicals,
        fundamentals=fundamentals_map,
        enrichment={},
    )


class AlphaModelTests(unittest.TestCase):
    def test_independent_stock_models_return_scores(self) -> None:
        data = alpha_data(10)
        multifactor = generate_multifactor_signals(data=data)
        residual = generate_residual_stat_arb_signals(data=data)
        filing = generate_earnings_drift_signals(data=data)
        combined, model = generate_ml_alpha_combiner_signals(
            data=data,
            multifactor=multifactor,
            residual=residual,
            filing_drift=filing,
        )
        self.assertGreaterEqual(len(multifactor), 5)
        self.assertGreaterEqual(len(residual), 5)
        self.assertGreaterEqual(len(filing), 5)
        self.assertGreaterEqual(len(combined), 5)
        self.assertIn("alpha_score", combined.columns)
        self.assertIn("training_samples", model)

    def test_pairs_model_returns_same_sector_pairs(self) -> None:
        pairs = generate_pair_trade_signals(data=alpha_data(10), min_abs_z=0.5)
        self.assertFalse(pairs.empty)
        self.assertTrue((pairs["sector"].astype(str).str.startswith("Sector")).all())
        self.assertIn("hedge_ratio", pairs.columns)

    def test_orchestrator_writes_expected_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "universe").mkdir(parents=True)
            (root / "technicals" / "from_yahoo_daily").mkdir(parents=True)
            (root / "fundamentals" / "sec_companyfacts" / "long").mkdir(parents=True)
            data = alpha_data(10)
            data.universe.drop(columns=[]).to_csv(root / "universe" / "sp500_constituents.csv", index=False)
            for symbol, frame in data.technicals.items():
                frame.drop(columns=["_close", "_dollar_volume"]).to_csv(
                    root / "technicals" / "from_yahoo_daily" / f"{symbol}.csv",
                    index=False,
                )
            for symbol, frame in data.fundamentals.items():
                frame.to_csv(root / "fundamentals" / "sec_companyfacts" / "long" / f"{symbol}.csv", index=False)
            payload = build_offline_alpha_recommendations(data_root=root, count=5)
            self.assertEqual(len(payload["buy"]), 5)
            self.assertEqual(len(payload["sell"]), 5)
            self.assertIn("pairs", payload)
            self.assertTrue((root / "recommendations" / "offline_alpha_recommendations.json").exists())


if __name__ == "__main__":
    unittest.main()
