"""
12_strategy_mapping_by_regime.py
================================

BAC Quant Engine - Regime Engine
Stage 12: Strategy mapping by regime.

Purpose:
- Read regime signal router output
- Map regime strategy families into named candidate strategies
- Create strategy selection tables for future backtesting/execution layers
- Save outputs for analysis and integration

This script does NOT place trades.
"""

from pathlib import Path
from datetime import datetime
import argparse
import logging
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

ROUTER_ROOT = Path("E:/Quant_Lab/data/analysis/regime_signal_router/FTMO")
OUTPUT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_strategy_mapping/FTMO")
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


log_path = LOG_DIR / f"strategy_mapping_by_regime_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


STRATEGY_CATALOGUE = {
    "mean_reversion_range": [
        "bollinger_band_mean_reversion",
        "rsi_range_reversion",
    ],
    "trend_following_long_bias": [
        "ema_trend_following_long",
        "adx_trend_continuation_long",
    ],
    "trend_following_short_bias": [
        "ema_trend_following_short",
        "adx_trend_continuation_short",
    ],
    "breakout_watch": [
        "atr_volatility_breakout",
        "bollinger_band_squeeze_breakout",
    ],
    "directional_resolution_watch": [
        "directional_breakout_confirmation",
    ],
    "persistence_monitor": [
        "regime_persistence_monitor",
    ],
    "reduced_risk_observation": [
        "reduced_risk_observation",
    ],
    "defensive_volatility_filter": [
        "no_trade_defensive_filter",
    ],
    "no_trade_observation": [
        "no_trade_observation",
    ],
}


STRATEGY_METADATA = {
    "bollinger_band_mean_reversion": {
        "strategy_type": "mean_reversion",
        "default_direction": "two_sided",
        "preferred_risk_mode": "normal",
        "requires_confirmation": True,
        "notes": "Use in persistent range/quiet range regimes only.",
    },
    "rsi_range_reversion": {
        "strategy_type": "mean_reversion",
        "default_direction": "two_sided",
        "preferred_risk_mode": "normal",
        "requires_confirmation": True,
        "notes": "Use RSI extremes inside stable range regimes.",
    },
    "ema_trend_following_long": {
        "strategy_type": "trend_following",
        "default_direction": "long",
        "preferred_risk_mode": "normal",
        "requires_confirmation": True,
        "notes": "Use only when trend persistence is high and volatility is controlled.",
    },
    "adx_trend_continuation_long": {
        "strategy_type": "trend_following",
        "default_direction": "long",
        "preferred_risk_mode": "normal",
        "requires_confirmation": True,
        "notes": "Use when ADX/trend strength confirms persistent bullish regime.",
    },
    "ema_trend_following_short": {
        "strategy_type": "trend_following",
        "default_direction": "short",
        "preferred_risk_mode": "normal",
        "requires_confirmation": True,
        "notes": "Use only when bearish trend persistence is high and volatility is controlled.",
    },
    "adx_trend_continuation_short": {
        "strategy_type": "trend_following",
        "default_direction": "short",
        "preferred_risk_mode": "normal",
        "requires_confirmation": True,
        "notes": "Use when ADX/trend strength confirms persistent bearish regime.",
    },
    "atr_volatility_breakout": {
        "strategy_type": "breakout",
        "default_direction": "conditional",
        "preferred_risk_mode": "cautious",
        "requires_confirmation": True,
        "notes": "Watch for volatility expansion and range break confirmation.",
    },
    "bollinger_band_squeeze_breakout": {
        "strategy_type": "breakout",
        "default_direction": "conditional",
        "preferred_risk_mode": "cautious",
        "requires_confirmation": True,
        "notes": "Use only after squeeze/expansion confirmation.",
    },
    "directional_breakout_confirmation": {
        "strategy_type": "breakout",
        "default_direction": "conditional",
        "preferred_risk_mode": "cautious",
        "requires_confirmation": True,
        "notes": "Directional bias exists but needs execution confirmation.",
    },
    "regime_persistence_monitor": {
        "strategy_type": "monitor",
        "default_direction": "neutral",
        "preferred_risk_mode": "cautious",
        "requires_confirmation": True,
        "notes": "Monitor persistent regime; do not trade without strategy confirmation.",
    },
    "reduced_risk_observation": {
        "strategy_type": "risk_filter",
        "default_direction": "neutral",
        "preferred_risk_mode": "reduced",
        "requires_confirmation": False,
        "notes": "Observation only due to risk/volatility conditions.",
    },
    "no_trade_defensive_filter": {
        "strategy_type": "risk_filter",
        "default_direction": "neutral",
        "preferred_risk_mode": "defensive",
        "requires_confirmation": False,
        "notes": "No-trade defensive state.",
    },
    "no_trade_observation": {
        "strategy_type": "risk_filter",
        "default_direction": "neutral",
        "preferred_risk_mode": "defensive",
        "requires_confirmation": False,
        "notes": "No actionable regime setup.",
    },
}


def get_mode_suffix(mode: str) -> str:
    return "" if mode == "full" else f"_{mode}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Map BACQE regime router output to candidate strategies."
    )

    parser.add_argument(
        "--mode",
        choices=["full", "small", "medium", "large"],
        default="small",
        help="Timeframe group to process.",
    )

    parser.add_argument(
        "--permission",
        choices=["all", "allowed", "watch", "observe"],
        default="all",
        help="Which router permission table to use.",
    )

    return parser.parse_args()


def get_router_path(mode: str, permission: str) -> Path:
    suffix = get_mode_suffix(mode)

    if permission == "allowed":
        return ROUTER_ROOT / f"regime_signal_router_allowed{suffix}_latest.parquet"

    if permission == "watch":
        return ROUTER_ROOT / f"regime_signal_router_watch{suffix}_latest.parquet"

    if permission == "observe":
        return ROUTER_ROOT / f"regime_signal_router_observe{suffix}_latest.parquet"

    return ROUTER_ROOT / f"regime_signal_router{suffix}_latest.parquet"


def calculate_strategy_score(row: pd.Series, strategy_name: str) -> float:
    router_score = float(row.get("router_score", 0) or 0)
    confidence = float(row.get("current_confidence", 0) or 0)
    persistence = float(row.get("persistence_probability_pct", 0) or 0)
    breakout = float(row.get("breakout_probability_pct", 0) or 0)
    vol_expansion = float(row.get("volatility_expansion_probability_pct", 0) or 0)

    metadata = STRATEGY_METADATA.get(strategy_name, {})
    strategy_type = metadata.get("strategy_type", "unknown")

    score = router_score

    if strategy_type == "trend_following":
        score += persistence * 0.15
        score += confidence * 10

    elif strategy_type == "mean_reversion":
        score += persistence * 0.10
        score += confidence * 8
        score -= vol_expansion * 0.20

    elif strategy_type == "breakout":
        score += breakout * 0.20
        score += vol_expansion * 0.10

    elif strategy_type == "risk_filter":
        score = max(score - 25, 0)

    return round(max(score, 0), 2)


def expand_strategy_candidates(router_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, row in router_df.iterrows():
        family = row.get("recommended_strategy_family")
        strategies = STRATEGY_CATALOGUE.get(family, ["unmapped_strategy_family"])

        for strategy_name in strategies:
            metadata = STRATEGY_METADATA.get(
                strategy_name,
                {
                    "strategy_type": "unknown",
                    "default_direction": "neutral",
                    "preferred_risk_mode": "cautious",
                    "requires_confirmation": True,
                    "notes": "Strategy family not mapped yet.",
                },
            )

            rows.append(
                {
                    "symbol": row.get("symbol"),
                    "timeframe": row.get("timeframe"),
                    "latest_time": row.get("latest_time"),
                    "current_regime": row.get("current_regime"),
                    "forecast_signal": row.get("forecast_signal"),
                    "dashboard_bucket": row.get("dashboard_bucket"),
                    "directional_bias": row.get("directional_bias"),
                    "recommended_strategy_family": family,
                    "candidate_strategy": strategy_name,
                    "strategy_type": metadata["strategy_type"],
                    "strategy_direction": metadata["default_direction"],
                    "preferred_risk_mode": metadata["preferred_risk_mode"],
                    "requires_confirmation": metadata["requires_confirmation"],
                    "strategy_notes": metadata["notes"],
                    "execution_permission": row.get("execution_permission"),
                    "risk_mode": row.get("risk_mode"),
                    "position_sizing_profile": row.get("position_sizing_profile"),
                    "primary_direction": row.get("primary_direction"),
                    "router_score": row.get("router_score"),
                    "strategy_score": calculate_strategy_score(row, strategy_name),
                    "priority_score": row.get("priority_score"),
                    "current_confidence": row.get("current_confidence"),
                    "persistence_probability_pct": row.get("persistence_probability_pct"),
                    "breakout_probability_pct": row.get("breakout_probability_pct"),
                    "volatility_expansion_probability_pct": row.get("volatility_expansion_probability_pct"),
                    "bullish_probability_pct": row.get("bullish_probability_pct"),
                    "bearish_probability_pct": row.get("bearish_probability_pct"),
                    "range_probability_pct": row.get("range_probability_pct"),
                    "transition_probability_pct": row.get("transition_probability_pct"),
                    "current_regime_run_length": row.get("current_regime_run_length"),
                    "trend_state": row.get("trend_state"),
                    "volatility_state": row.get("volatility_state"),
                    "momentum_state": row.get("momentum_state"),
                    "trend_strength_state": row.get("trend_strength_state"),
                    "forecast_source": row.get("forecast_source"),
                    "timeframe_rank": row.get("timeframe_rank"),
                }
            )

    candidate_df = pd.DataFrame(rows)

    if candidate_df.empty:
        return candidate_df

    candidate_df = candidate_df.sort_values(
        by=["strategy_score", "timeframe_rank", "symbol"],
        ascending=[False, True, True],
    ).reset_index(drop=True)

    return candidate_df


def build_best_strategy_table(candidate_df: pd.DataFrame) -> pd.DataFrame:
    if candidate_df.empty:
        return candidate_df

    best = (
        candidate_df.sort_values(
            by=["strategy_score"],
            ascending=False,
        )
        .groupby(["symbol", "timeframe"], as_index=False)
        .head(1)
        .reset_index(drop=True)
    )

    return best.sort_values(
        by=["strategy_score", "timeframe_rank", "symbol"],
        ascending=[False, True, True],
    ).reset_index(drop=True)


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


def main(mode: str, permission: str) -> None:
    logger.info("=" * 80)
    logger.info("Starting BACQE Strategy Mapping by Regime")
    logger.info(f"Mode: {mode}")
    logger.info(f"Permission source: {permission}")
    logger.info("=" * 80)

    router_path = get_router_path(mode=mode, permission=permission)

    logger.info(f"Router path: {router_path}")
    logger.info(f"Output root: {OUTPUT_ROOT}")

    if not router_path.exists():
        raise FileNotFoundError(f"Missing router file: {router_path}")

    router_df = pd.read_parquet(router_path)

    if router_df.empty:
        logger.warning("Router input is empty")
        return

    candidate_df = expand_strategy_candidates(router_df)
    best_df = build_best_strategy_table(candidate_df)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    permission_suffix = "" if permission == "all" else f"_{permission}"

    save_outputs(
        candidate_df,
        f"regime_strategy_candidates{permission_suffix}",
        timestamp,
        mode,
    )

    save_outputs(
        best_df,
        f"regime_strategy_best{permission_suffix}",
        timestamp,
        mode,
    )

    logger.info("=" * 80)
    logger.info("Strategy Mapping by Regime completed")
    logger.info(f"Router input rows: {len(router_df)}")
    logger.info(f"Candidate strategy rows: {len(candidate_df)}")
    logger.info(f"Best strategy rows: {len(best_df)}")
    logger.info("=" * 80)

    logger.info("Candidate strategy counts:")
    logger.info(candidate_df["candidate_strategy"].value_counts().to_string())

    logger.info("Strategy type counts:")
    logger.info(candidate_df["strategy_type"].value_counts().to_string())

    logger.info("Top 30 candidate strategies:")
    display_cols = [
        "symbol",
        "timeframe",
        "current_regime",
        "recommended_strategy_family",
        "candidate_strategy",
        "strategy_type",
        "strategy_direction",
        "execution_permission",
        "risk_mode",
        "strategy_score",
    ]

    logger.info(
        candidate_df[display_cols]
        .head(30)
        .to_string(index=False)
    )


if __name__ == "__main__":
    args = parse_args()
    main(mode=args.mode, permission=args.permission)