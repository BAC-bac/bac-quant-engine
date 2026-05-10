"""
11_build_regime_signal_router.py
================================

BAC Quant Engine - Regime Engine
Stage 11: Regime Signal Router.

Purpose:
- Read recent regime dashboard output
- Convert market regimes into strategy-routing decisions
- Recommend suitable strategy family, risk mode and execution priority
- Save a clean routing table for future strategy/backtest/EA integration

This script does NOT place trades.
It creates decision-support outputs.
"""

from pathlib import Path
from datetime import datetime
import argparse
import logging
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RECENT_DIR = Path("E:/Quant_Lab/data/analysis/regime_recent/FTMO")
OUTPUT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_signal_router/FTMO")
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


TIMEFRAME_GROUPS = {
    "small": ["M1", "M2", "M3", "M4", "M5", "M6", "M10", "M12", "M15"],
    "medium": ["M20", "M30", "H1", "H2", "H3", "H4"],
    "large": ["H6", "H8", "H12", "D1", "W1", "MN1"],
    "full": None,
}


TIMEFRAME_ORDER = {
    "M1": 1,
    "M2": 2,
    "M3": 3,
    "M4": 4,
    "M5": 5,
    "M6": 6,
    "M10": 10,
    "M12": 12,
    "M15": 15,
    "M20": 20,
    "M30": 30,
    "H1": 60,
    "H2": 120,
    "H3": 180,
    "H4": 240,
    "H6": 360,
    "H8": 480,
    "H12": 720,
    "D1": 1440,
    "W1": 10080,
    "MN1": 43200,
}


log_path = LOG_DIR / f"regime_signal_router_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def get_mode_suffix(mode: str) -> str:
    return "" if mode == "full" else f"_{mode}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build BACQE regime signal routing table."
    )

    parser.add_argument(
        "--mode",
        choices=["full", "small", "medium", "large"],
        default="small",
        help="Timeframe group to route.",
    )

    parser.add_argument(
        "--source",
        choices=["recent", "dashboard"],
        default="recent",
        help="Use recent operational dashboard or full dashboard output.",
    )

    parser.add_argument(
        "--min-priority",
        type=float,
        default=0.0,
        help="Optional minimum priority score filter.",
    )

    return parser.parse_args()


def get_input_path(mode: str, source: str) -> Path:
    suffix = get_mode_suffix(mode)

    if source == "recent":
        return RECENT_DIR / f"recent_regime_dashboard{suffix}_latest.parquet"

    full_dashboard_dir = Path("E:/Quant_Lab/data/analysis/regime_dashboard/FTMO")
    return full_dashboard_dir / f"latest_regime_dashboard{suffix}_latest.parquet"


def normalise_probability(value) -> float:
    if pd.isna(value):
        return 0.0

    return float(value)


def choose_strategy_family(row: pd.Series) -> str:
    regime = str(row.get("current_regime", ""))
    forecast_signal = str(row.get("forecast_signal", ""))
    bucket = str(row.get("dashboard_bucket", ""))
    bias = str(row.get("directional_bias", ""))

    persistence = normalise_probability(row.get("persistence_probability_pct", 0))
    breakout = normalise_probability(row.get("breakout_probability_pct", 0))
    vol_expansion = normalise_probability(row.get("volatility_expansion_probability_pct", 0))

    if forecast_signal == "volatility_expansion_risk":
        return "defensive_volatility_filter"

    if forecast_signal == "range_breakout_risk":
        return "breakout_watch"

    if bucket == "persistent_trend" or forecast_signal == "regime_persistence_trend":
        if "bull" in regime or bias in {"bullish", "bullish_current"}:
            return "trend_following_long_bias"

        if "bear" in regime or bias in {"bearish", "bearish_current"}:
            return "trend_following_short_bias"

        return "trend_following_neutral"

    if bucket == "persistent_range" or forecast_signal in {
        "regime_persistence_range",
        "range_continuation_bias",
    }:
        return "mean_reversion_range"

    if forecast_signal in {"bullish_resolution_bias", "bearish_resolution_bias"}:
        return "directional_resolution_watch"

    if "volatile" in regime or vol_expansion >= 20:
        return "reduced_risk_observation"

    if breakout >= 15:
        return "breakout_watch"

    if persistence >= 85:
        return "persistence_monitor"

    return "no_trade_observation"


def choose_risk_mode(row: pd.Series) -> str:
    forecast_signal = str(row.get("forecast_signal", ""))
    regime = str(row.get("current_regime", ""))

    confidence = normalise_probability(row.get("current_confidence", 0))
    vol_expansion = normalise_probability(row.get("volatility_expansion_probability_pct", 0))
    breakout = normalise_probability(row.get("breakout_probability_pct", 0))

    if forecast_signal in {"volatility_expansion_risk", "mixed_or_uncertain"}:
        return "defensive"

    if "high_vol" in regime or "volatile" in regime:
        return "reduced"

    if vol_expansion >= 20:
        return "reduced"

    if breakout >= 20:
        return "cautious"

    if confidence >= 0.75 and forecast_signal in {
        "regime_persistence_trend",
        "regime_persistence_range",
    }:
        return "normal"

    return "cautious"


def choose_position_sizing_profile(row: pd.Series) -> str:
    risk_mode = row["risk_mode"]
    strategy = row["recommended_strategy_family"]

    if risk_mode == "defensive":
        return "flat_or_minimum_size"

    if risk_mode == "reduced":
        return "reduced_size"

    if risk_mode == "cautious":
        return "small_size"

    if strategy in {
        "trend_following_long_bias",
        "trend_following_short_bias",
        "mean_reversion_range",
    }:
        return "standard_size"

    return "small_size"


def choose_execution_permission(row: pd.Series) -> str:
    strategy = row["recommended_strategy_family"]
    risk_mode = row["risk_mode"]

    if strategy in {
        "defensive_volatility_filter",
        "reduced_risk_observation",
        "no_trade_observation",
    }:
        return "observe_only"

    if strategy == "breakout_watch":
        return "watch_only"

    if risk_mode == "defensive":
        return "observe_only"

    if strategy in {
        "breakout_watch",
        "directional_resolution_watch",
        "persistence_monitor",
    }:
        return "watch_only"

    return "strategy_allowed"


def choose_primary_direction(row: pd.Series) -> str:
    strategy = row["recommended_strategy_family"]
    bias = str(row.get("directional_bias", ""))

    if strategy == "trend_following_long_bias":
        return "long"

    if strategy == "trend_following_short_bias":
        return "short"

    if strategy == "directional_resolution_watch":
        if bias in {"bullish", "bullish_current"}:
            return "long_watch"

        if bias in {"bearish", "bearish_current"}:
            return "short_watch"

    if strategy == "mean_reversion_range":
        return "two_sided"

    return "neutral"


def calculate_router_score(row: pd.Series) -> float:
    priority = normalise_probability(row.get("priority_score", 0))
    confidence = normalise_probability(row.get("current_confidence", 0))
    persistence = normalise_probability(row.get("persistence_probability_pct", 0))
    breakout = normalise_probability(row.get("breakout_probability_pct", 0))
    vol_expansion = normalise_probability(row.get("volatility_expansion_probability_pct", 0))

    strategy = row["recommended_strategy_family"]
    permission = row["execution_permission"]

    score = 0.0

    score += priority * 0.45
    score += confidence * 20
    score += persistence * 0.10
    score += breakout * 0.10

    if strategy in {
        "trend_following_long_bias",
        "trend_following_short_bias",
        "mean_reversion_range",
    }:
        score += 15

    elif strategy in {
        "breakout_watch",
        "directional_resolution_watch",
    }:
        score += 8

    if permission == "observe_only":
        score -= 20

    if vol_expansion >= 20:
        score -= 8

    return round(max(score, 0), 2)


def build_router(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["timeframe_rank"] = df["timeframe"].map(TIMEFRAME_ORDER).fillna(999999)

    df["recommended_strategy_family"] = df.apply(choose_strategy_family, axis=1)
    df["risk_mode"] = df.apply(choose_risk_mode, axis=1)
    df["position_sizing_profile"] = df.apply(choose_position_sizing_profile, axis=1)
    df["execution_permission"] = df.apply(choose_execution_permission, axis=1)
    df["primary_direction"] = df.apply(choose_primary_direction, axis=1)
    df["router_score"] = df.apply(calculate_router_score, axis=1)

    keep_cols = [
        "symbol",
        "timeframe",
        "latest_time",
        "current_regime",
        "most_likely_next_regime",
        "forecast_signal",
        "dashboard_bucket",
        "directional_bias",
        "recommended_strategy_family",
        "primary_direction",
        "risk_mode",
        "position_sizing_profile",
        "execution_permission",
        "router_score",
        "priority_score",
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
        "recent_rows_processed",
        "recent_features_rows",
        "recent_classified_rows",
        "timeframe_rank",
    ]

    existing_cols = [col for col in keep_cols if col in df.columns]

    router = df[existing_cols].copy()

    router = router.sort_values(
        by=["router_score", "timeframe_rank", "symbol"],
        ascending=[False, True, True],
    ).reset_index(drop=True)

    return router


def save_outputs(df: pd.DataFrame, name: str, timestamp: str, mode: str) -> None:
    suffix = get_mode_suffix(mode)

    csv_path = OUTPUT_ROOT / f"{name}{suffix}_{timestamp}.csv"
    parquet_path = OUTPUT_ROOT / f"{name}{suffix}_{timestamp}.parquet"

    latest_csv = OUTPUT_ROOT / f"{name}{suffix}_latest.csv"
    latest_parquet = OUTPUT_ROOT / f"{name}{suffix}_latest.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    df.to_csv(latest_csv, index=False)
    df.to_parquet(latest_parquet, index=False)

    logger.info(f"Saved {name}: {latest_csv}")


def main(mode: str, source: str, min_priority: float) -> None:
    logger.info("=" * 80)
    logger.info("Starting BACQE Regime Signal Router")
    logger.info(f"Mode: {mode}")
    logger.info(f"Source: {source}")
    logger.info(f"Minimum priority filter: {min_priority}")
    logger.info("=" * 80)

    input_path = get_input_path(mode=mode, source=source)

    logger.info(f"Input path: {input_path}")
    logger.info(f"Output root: {OUTPUT_ROOT}")

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input dashboard file: {input_path}")

    dashboard_df = pd.read_parquet(input_path)

    if dashboard_df.empty:
        logger.warning("Input dashboard is empty")
        return

    if min_priority > 0 and "priority_score" in dashboard_df.columns:
        dashboard_df = dashboard_df[
            dashboard_df["priority_score"] >= min_priority
        ].copy()

    if dashboard_df.empty:
        logger.warning("No rows left after priority filter")
        return

    router_df = build_router(dashboard_df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    save_outputs(router_df, "regime_signal_router", timestamp, mode)

    allowed_df = router_df[
        router_df["execution_permission"] == "strategy_allowed"
    ].copy()

    watch_df = router_df[
        router_df["execution_permission"] == "watch_only"
    ].copy()

    observe_df = router_df[
        router_df["execution_permission"] == "observe_only"
    ].copy()

    save_outputs(allowed_df, "regime_signal_router_allowed", timestamp, mode)
    save_outputs(watch_df, "regime_signal_router_watch", timestamp, mode)
    save_outputs(observe_df, "regime_signal_router_observe", timestamp, mode)

    logger.info("=" * 80)
    logger.info("Regime Signal Router completed")
    logger.info(f"Input rows: {len(dashboard_df)}")
    logger.info(f"Router rows: {len(router_df)}")
    logger.info(f"Allowed rows: {len(allowed_df)}")
    logger.info(f"Watch rows: {len(watch_df)}")
    logger.info(f"Observe rows: {len(observe_df)}")
    logger.info("=" * 80)

    logger.info("Recommended strategy family counts:")
    logger.info(router_df["recommended_strategy_family"].value_counts().to_string())

    logger.info("Risk mode counts:")
    logger.info(router_df["risk_mode"].value_counts().to_string())

    logger.info("Execution permission counts:")
    logger.info(router_df["execution_permission"].value_counts().to_string())

    logger.info("Top 30 routed opportunities:")
    display_cols = [
        "symbol",
        "timeframe",
        "current_regime",
        "forecast_signal",
        "recommended_strategy_family",
        "primary_direction",
        "risk_mode",
        "position_sizing_profile",
        "execution_permission",
        "router_score",
    ]

    logger.info(
        router_df[display_cols]
        .head(30)
        .to_string(index=False)
    )


if __name__ == "__main__":
    args = parse_args()
    main(
        mode=args.mode,
        source=args.source,
        min_priority=args.min_priority,
    )