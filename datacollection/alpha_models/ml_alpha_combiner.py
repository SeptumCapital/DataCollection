from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import AlphaData, confidence_from_score, latest_stock_snapshot, load_alpha_data, pct_change, robust_zscore, safe_float


ML_FEATURES = [
    "momentum_12_1",
    "return_6m",
    "return_3m",
    "return_1m",
    "trend_50",
    "trend_200",
    "rsi_scaled",
    "volatility_21d",
]


def _feature_row(frame: pd.DataFrame, index: int) -> dict[str, float] | None:
    if index < 253 or index >= len(frame):
        return None
    current = safe_float(frame.iloc[index]["_close"])
    if current is None:
        return None

    def close_at(offset: int) -> float | None:
        position = index - offset
        if position < 0:
            return None
        return safe_float(frame.iloc[position]["_close"])

    sma_50 = safe_float(frame.iloc[index].get("sma_50"))
    sma_200 = safe_float(frame.iloc[index].get("sma_200"))
    rsi = safe_float(frame.iloc[index].get("rsi_14"))
    daily_returns = pd.to_numeric(frame["_close"], errors="coerce").pct_change().iloc[max(0, index - 21) : index + 1]
    row = {
        "momentum_12_1": pct_change(close_at(252), close_at(21)),
        "return_6m": pct_change(close_at(126), current),
        "return_3m": pct_change(close_at(63), current),
        "return_1m": pct_change(close_at(21), current),
        "trend_50": pct_change(sma_50, current),
        "trend_200": pct_change(sma_200, current),
        "rsi_scaled": ((rsi - 50) / 50) if rsi is not None else None,
        "volatility_21d": safe_float(daily_returns.std(ddof=0)),
    }
    if any(row[feature] is None for feature in ML_FEATURES):
        return None
    return {feature: float(row[feature]) for feature in ML_FEATURES}


def _train_samples(data: AlphaData) -> tuple[pd.DataFrame, pd.Series]:
    samples: list[dict[str, float]] = []
    targets: list[float] = []
    for frame in data.technicals.values():
        if len(frame) < 320:
            continue
        for index in range(253, len(frame) - 21, 21):
            features = _feature_row(frame, index)
            if features is None:
                continue
            current = safe_float(frame.iloc[index]["_close"])
            future = safe_float(frame.iloc[index + 21]["_close"])
            target = pct_change(current, future)
            if target is None or target < -0.75 or target > 1.5:
                continue
            samples.append(features)
            targets.append(float(np.clip(target, -0.5, 0.5)))
    return pd.DataFrame(samples), pd.Series(targets, dtype=float)


def _fit_predict(train_x: pd.DataFrame, train_y: pd.Series, latest_x: pd.DataFrame) -> tuple[np.ndarray, dict[str, Any]]:
    if len(train_x) < 300 or latest_x.empty:
        return np.zeros(len(latest_x)), {"trained": False, "training_samples": int(len(train_x)), "models": []}
    if len(train_x) > 80_000:
        train_x = train_x.tail(80_000)
        train_y = train_y.tail(80_000)
    train_x = train_x.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    latest_x = latest_x.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    predictions: list[np.ndarray] = []
    models: list[str] = []

    try:
        from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
        from sklearn.linear_model import HuberRegressor
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import RobustScaler

        estimators = {
            "huber": make_pipeline(RobustScaler(), HuberRegressor(epsilon=1.35, alpha=0.0005, max_iter=500)),
            "hist_gradient_boosting": HistGradientBoostingRegressor(max_iter=160, learning_rate=0.05, l2_regularization=0.08, random_state=42),
            "random_forest": RandomForestRegressor(n_estimators=80, max_depth=8, min_samples_leaf=30, random_state=42, n_jobs=-1),
        }
        for name, model in estimators.items():
            model.fit(train_x, train_y)
            predictions.append(np.clip(model.predict(latest_x), -0.5, 0.5))
            models.append(name)
    except Exception:
        x = train_x.to_numpy(dtype=float)
        y = train_y.to_numpy(dtype=float)
        x_mean = x.mean(axis=0)
        x_std = x.std(axis=0)
        x_std[x_std == 0] = 1
        xs = (x - x_mean) / x_std
        beta = np.linalg.solve(xs.T @ xs + 8.0 * np.eye(xs.shape[1]), xs.T @ (y - y.mean()))
        latest = latest_x.to_numpy(dtype=float)
        predictions.append(np.clip(((latest - x_mean) / x_std) @ beta + y.mean(), -0.5, 0.5))
        models.append("ridge_fallback")

    return np.mean(np.column_stack(predictions), axis=1), {
        "trained": True,
        "training_samples": int(len(train_x)),
        "models": models,
    }


def _component_map(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame.empty or "symbol_key" not in frame.columns:
        return pd.DataFrame(columns=["symbol_key", *columns])
    existing = ["symbol_key", *[column for column in columns if column in frame.columns]]
    return frame[existing].drop_duplicates("symbol_key")


def generate_ml_alpha_combiner_signals(
    data_root: Path | None = None,
    symbols: list[str] | None = None,
    data: AlphaData | None = None,
    multifactor: pd.DataFrame | None = None,
    residual: pd.DataFrame | None = None,
    filing_drift: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Train an offline next-21-trading-day model and blend it with alpha components."""

    data = data or load_alpha_data(data_root, symbols=symbols)
    rows: list[dict[str, Any]] = []
    for key, frame in data.technicals.items():
        snapshot = latest_stock_snapshot(data, key)
        if snapshot is None:
            continue
        features = _feature_row(frame, len(frame) - 1)
        if features is None:
            continue
        rows.append({**snapshot, **features})
    latest = pd.DataFrame(rows)
    if latest.empty:
        return latest, {"trained": False, "training_samples": 0, "models": []}

    train_x, train_y = _train_samples(data)
    predictions, model_info = _fit_predict(train_x[ML_FEATURES] if not train_x.empty else train_x, train_y, latest[ML_FEATURES])
    latest["ml_expected_21d"] = predictions
    latest["ml_score"] = robust_zscore(latest["ml_expected_21d"])

    component_frames = [
        _component_map(multifactor if multifactor is not None else pd.DataFrame(), ["multifactor_score", "multifactor_reason"]),
        _component_map(residual if residual is not None else pd.DataFrame(), ["residual_score", "residual_reason"]),
        _component_map(filing_drift if filing_drift is not None else pd.DataFrame(), ["filing_drift_score", "filing_drift_reason"]),
    ]
    for component in component_frames:
        if not component.empty:
            latest = latest.merge(component, on="symbol_key", how="left")

    for column in ("multifactor_score", "residual_score", "filing_drift_score"):
        if column not in latest.columns:
            latest[column] = 0.0
        latest[column] = pd.to_numeric(latest[column], errors="coerce").fillna(0.0)

    latest["alpha_score"] = (
        0.36 * latest["ml_score"]
        + 0.32 * robust_zscore(latest["multifactor_score"])
        + 0.18 * robust_zscore(latest["residual_score"])
        + 0.14 * robust_zscore(latest["filing_drift_score"])
    )
    latest["alpha_confidence"] = latest["alpha_score"].map(confidence_from_score)

    def reason(row: pd.Series) -> str:
        pieces: list[str] = []
        bearish = (safe_float(row.get("alpha_score")) or 0.0) < 0
        if bearish:
            if safe_float(row.get("ml_expected_21d")) and row["ml_expected_21d"] < 0:
                pieces.append("negative ML 21D forecast")
            if safe_float(row.get("multifactor_score")) and row["multifactor_score"] < 0:
                pieces.append("weak multi-factor blend")
            if safe_float(row.get("residual_score")) and row["residual_score"] < 0:
                pieces.append(str(row.get("residual_reason") or "residual mean-reversion short signal"))
            if safe_float(row.get("filing_drift_score")) and row["filing_drift_score"] < 0:
                pieces.append("weak SEC filing/growth drift proxy")
            if not pieces:
                pieces.append("negative blended alpha score")
        else:
            if safe_float(row.get("ml_expected_21d")) and row["ml_expected_21d"] > 0:
                pieces.append("positive ML 21D forecast")
            if safe_float(row.get("multifactor_score")) and row["multifactor_score"] > 0:
                pieces.append(str(row.get("multifactor_reason") or "multi-factor support"))
            if safe_float(row.get("residual_score")) and row["residual_score"] > 0:
                pieces.append(str(row.get("residual_reason") or "residual mean-reversion support"))
            if safe_float(row.get("filing_drift_score")) and row["filing_drift_score"] > 0:
                pieces.append(str(row.get("filing_drift_reason") or "filing drift support"))
        return ", ".join(pieces[:4]) or "ranked by offline alpha combiner"

    latest["alpha_reason"] = latest.apply(reason, axis=1)
    return latest.sort_values("alpha_score", ascending=False).reset_index(drop=True), {
        **model_info,
        "target": "next 21 trading day return",
        "features": ML_FEATURES,
    }
