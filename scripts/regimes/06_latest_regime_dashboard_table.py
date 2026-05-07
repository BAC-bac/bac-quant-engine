"""
06_latest_regime_dashboard_table.py
===================================

BAC Quant Engine - Regime Engine
Stage 06: Latest regime dashboard table.

Purpose:
- Read latest regime forecast output
- Create a clean dashboard-ready table
- Rank current market states by risk/opportunity categories
- Save CSV and Parquet outputs
"""

from pathlib import Path
from datetime import datetime
import logging
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

FORECAST_PATH = Path(
    "E:/Quant_Lab/data/analysis/regime_forecasts/FTMO/regime_forecast_latest.parquet"
)

OUTPUT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_dashboard/FTMO")
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


log_path = LOG_DIR / f"latest_regime_dashboard_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


TIMEFRAME_ORDER = {
    "M1": 1,
    "M2": 2,
    "M3": 3,
    "M5": 5,
    "M10": 10,
    "M15": 15,
    "M30": 30,
    "H1": 60,
    "H2": 120,
    "H3": 180,
    "H4": 240,
    "H8": 480,
    "H12": 720,
    "D1": 1440,
    "W1": 10080,
    "MN1": 43200,
}


RISK_SIGNALS = {
    "volatility_expansion_risk",
    "range_breakout_risk",
    "mixed_or_uncertain",
}

OPPORTUNITY_SIGNALS = {
    "bullish_resolution_bias",
    "bearish_resolution_bias",
    "regime_persistence_trend",
}

RANGE_SIGNALS = {
    "regime_persistence_range",
    "range_continuation_bias",
}


def classify_dashboard_bucket(row: pd.Series) -> str:
    signal = row["forecast_signal"]
    current_regime = row["current_regime"]

    if signal in {"volatility_expansion_risk", "range_breakout_risk"}:
        return "watchlist_risk_event"

    if signal in {"bullish_resolution_bias", "bearish_resolution_bias"}:
        return "directional_resolution_watch"

    if signal == "regime_persistence_trend":
        return "persistent_trend"

    if signal in {"regime_persistence_range", "range_continuation_bias"}:
        return "persistent_range"

    if "volatile" in str(current_regime):
        return "volatile_environment"

    return "mixed_or_uncertain"


def classify_bias(row: pd.Series) -> str:
    bull = row.get("bullish_probability_pct", 0)
    bear = row.get("bearish_probability_pct", 0)
    current = str(row.get("current_regime", ""))

    if bull > bear * 1.5 and bull >= 20:
        return "bullish"

    if bear > bull * 1.5 and bear >= 20:
        return "bearish"

    if "bull" in current:
        return "bullish_current"

    if "bear" in current:
        return "bearish_current"

    return "neutral"


def calculate_priority_score(row: pd.Series) -> float:
    """
    Higher score = more interesting for review.
    This is not a trading signal. It is a dashboard prioritisation score.
    """
    score = 0.0

    score += float(row.get("current_confidence", 0)) * 20
    score += float(row.get("volatility_expansion_probability_pct", 0)) * 0.35
    score += float(row.get("breakout_probability_pct", 0)) * 0.25
    score += float(row.get("persistence_probability_pct", 0)) * 0.10

    signal = row.get("forecast_signal")

    if signal == "volatility_expansion_risk":
        score += 20

    elif signal == "range_breakout_risk":
        score += 18

    elif signal in {"bullish_resolution_bias", "bearish_resolution_bias"}:
        score += 15

    elif signal == "regime_persistence_trend":
        score += 10

    elif signal == "mixed_or_uncertain":
        score += 3

    return round(score, 2)


def build_dashboard_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["latest_time"] = pd.to_datetime(df["latest_time"], utc=True)
    df["timeframe_rank"] = df["timeframe"].map(TIMEFRAME_ORDER).fillna(999999)

    df["dashboard_bucket"] = df.apply(classify_dashboard_bucket, axis=1)
    df["directional_bias"] = df.apply(classify_bias, axis=1)
    df["priority_score"] = df.apply(calculate_priority_score, axis=1)

    keep_cols = [
        "symbol",
        "timeframe",
        "latest_time",
        "current_regime",
        "most_likely_next_regime",
        "forecast_signal",
        "dashboard_bucket",
        "directional_bias",
        "current_confidence",
        "persistence_probability_pct",
        "breakout_probability_pct",
        "volatility_expansion_probability_pct",
        "bullish_probability_pct",
        "bearish_probability_pct",
        "range_probability_pct",
        "transition_probability_pct",
        "current_regime_run_length",
        "trend_state",
        "volatility_state",
        "momentum_state",
        "trend_strength_state",
        "forecast_source",
        "priority_score",
        "timeframe_rank",
    ]

    existing_cols = [col for col in keep_cols if col in df.columns]

    dashboard = df[existing_cols].copy()

    dashboard = dashboard.sort_values(
        by=["priority_score", "timeframe_rank", "symbol"],
        ascending=[False, True, True],
    ).reset_index(drop=True)

    return dashboard


def save_outputs(df: pd.DataFrame, name: str, timestamp: str) -> None:
    csv_path = OUTPUT_ROOT / f"{name}_{timestamp}.csv"
    parquet_path = OUTPUT_ROOT / f"{name}_{timestamp}.parquet"

    latest_csv = OUTPUT_ROOT / f"{name}_latest.csv"
    latest_parquet = OUTPUT_ROOT / f"{name}_latest.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    df.to_csv(latest_csv, index=False)
    df.to_parquet(latest_parquet, index=False)

    logger.info(f"Saved {name}: {latest_csv}")


def main() -> None:
    logger.info("Starting latest regime dashboard table build")
    logger.info(f"Forecast path: {FORECAST_PATH}")
    logger.info(f"Output root: {OUTPUT_ROOT}")

    if not FORECAST_PATH.exists():
        raise FileNotFoundError(f"Missing forecast file: {FORECAST_PATH}")

    forecast_df = pd.read_parquet(FORECAST_PATH)

    logger.info(f"Forecast rows loaded: {len(forecast_df)}")

    dashboard = build_dashboard_table(forecast_df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    save_outputs(dashboard, "latest_regime_dashboard", timestamp)

    watchlist = dashboard[
        dashboard["dashboard_bucket"].isin(
            [
                "watchlist_risk_event",
                "directional_resolution_watch",
                "volatile_environment",
            ]
        )
    ].copy()

    save_outputs(watchlist, "latest_regime_watchlist", timestamp)

    trend_table = dashboard[
        dashboard["dashboard_bucket"] == "persistent_trend"
    ].copy()

    save_outputs(trend_table, "latest_persistent_trends", timestamp)

    range_table = dashboard[
        dashboard["dashboard_bucket"] == "persistent_range"
    ].copy()

    save_outputs(range_table, "latest_persistent_ranges", timestamp)

    logger.info("Dashboard build completed successfully")
    logger.info(f"Dashboard rows: {len(dashboard)}")
    logger.info(f"Watchlist rows: {len(watchlist)}")
    logger.info(f"Persistent trend rows: {len(trend_table)}")
    logger.info(f"Persistent range rows: {len(range_table)}")

    logger.info("Dashboard bucket counts:")
    logger.info(dashboard["dashboard_bucket"].value_counts().to_string())

    logger.info("Top 30 dashboard priorities:")
    display_cols = [
        "symbol",
        "timeframe",
        "current_regime",
        "forecast_signal",
        "dashboard_bucket",
        "directional_bias",
        "priority_score",
        "persistence_probability_pct",
        "breakout_probability_pct",
        "volatility_expansion_probability_pct",
    ]

    logger.info(
        dashboard[display_cols]
        .head(30)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()