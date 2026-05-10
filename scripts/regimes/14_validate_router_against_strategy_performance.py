"""
14_validate_router_against_strategy_performance.py
=================================================

BAC Quant Engine - Regime Engine
Stage 14: Validate router decisions against historical regime performance.

Purpose:
- Read latest regime signal router output
- Read latest strategy performance by regime output
- Compare current router recommendations with historical best-performing strategy proxies
- Add evidence scores and recommendation status
- Save validation reports

This script does NOT place trades.
It acts as an evidence layer between the router and any future execution/backtest layer.
"""

from pathlib import Path
from datetime import datetime
import argparse
import logging
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

ROUTER_ROOT = Path("E:/Quant_Lab/data/analysis/regime_signal_router/FTMO")
PERFORMANCE_ROOT = Path("E:/Quant_Lab/data/analysis/regime_strategy_performance/FTMO")
OUTPUT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_router_validation/FTMO")
LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


log_path = LOG_DIR / f"router_validation_{datetime.now():%Y%m%d_%H%M%S}.log"

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


ROUTER_FAMILY_TO_PROXY_STRATEGIES = {
    "mean_reversion_range": {
        "mean_reversion_range_proxy",
    },
    "trend_following_long_bias": {
        "trend_following_long_proxy",
        "trend_following_two_way_proxy",
    },
    "trend_following_short_bias": {
        "trend_following_short_proxy",
        "trend_following_two_way_proxy",
    },
    "trend_following_neutral": {
        "trend_following_two_way_proxy",
    },
    "breakout_watch": {
        "breakout_transition_proxy",
    },
    "directional_resolution_watch": {
        "breakout_transition_proxy",
        "trend_following_two_way_proxy",
    },
    "persistence_monitor": {
        "trend_following_two_way_proxy",
        "mean_reversion_range_proxy",
    },
    "defensive_volatility_filter": {
        "risk_off_proxy",
    },
    "reduced_risk_observation": {
        "risk_off_proxy",
    },
    "no_trade_observation": {
        "risk_off_proxy",
    },
}


def get_mode_suffix(mode: str) -> str:
    return "" if mode == "full" else f"_{mode}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate BACQE router decisions against strategy performance evidence."
    )

    parser.add_argument(
        "--mode",
        choices=["full", "small", "medium", "large"],
        default="small",
        help="Timeframe group to validate.",
    )

    parser.add_argument(
        "--permission",
        choices=["all", "allowed", "watch", "observe"],
        default="all",
        help="Router permission table to validate.",
    )

    parser.add_argument(
        "--min-observations",
        type=int,
        default=1000,
        help="Minimum historical observations required for strong evidence.",
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


def get_performance_paths(mode: str) -> tuple[Path, Path]:
    suffix = get_mode_suffix(mode)

    global_path = PERFORMANCE_ROOT / f"strategy_performance_global_by_regime{suffix}_latest.parquet"
    best_path = PERFORMANCE_ROOT / f"strategy_performance_best_by_regime{suffix}_latest.parquet"

    return global_path, best_path


def classify_evidence_strength(
    observations: float,
    win_rate_pct: float,
    profit_factor: float | None,
    sharpe_proxy: float | None,
    min_observations: int,
) -> str:
    if pd.isna(observations) or observations < min_observations:
        return "insufficient_sample"

    pf = 0 if pd.isna(profit_factor) else float(profit_factor)
    sharpe = 0 if pd.isna(sharpe_proxy) else float(sharpe_proxy)

    if win_rate_pct >= 52 and pf >= 1.10 and sharpe > 0:
        return "strong_positive"

    if win_rate_pct >= 50 and pf >= 1.02:
        return "mild_positive"

    if win_rate_pct >= 48 and pf >= 0.98:
        return "neutral_marginal"

    if pf < 0.95 or win_rate_pct < 47:
        return "negative"

    return "mixed"


def calculate_evidence_score(
    observations: float,
    win_rate_pct: float,
    profit_factor: float | None,
    sharpe_proxy: float | None,
    min_observations: int,
) -> float:
    if pd.isna(observations) or observations <= 0:
        return 0.0

    sample_score = min(float(observations) / min_observations, 2.0) * 10

    pf = 1.0 if pd.isna(profit_factor) else float(profit_factor)
    sharpe = 0.0 if pd.isna(sharpe_proxy) else float(sharpe_proxy)

    win_component = (float(win_rate_pct) - 50) * 2
    pf_component = (pf - 1.0) * 50
    sharpe_component = sharpe * 20

    score = 50 + sample_score + win_component + pf_component + sharpe_component

    return round(max(min(score, 100), 0), 2)


def classify_recommendation_status(
    execution_permission: str,
    alignment_status: str,
    evidence_strength: str,
    evidence_score: float,
) -> str:
    if execution_permission == "observe_only":
        return "observe_only"

    if evidence_strength == "insufficient_sample":
        return "watch_insufficient_evidence"

    if evidence_strength == "negative":
        return "reject_historical_evidence"

    if alignment_status == "aligned" and evidence_strength in {
        "strong_positive",
        "mild_positive",
        "neutral_marginal",
    }:
        if evidence_score >= 65:
            return "approved_evidence_weighted"

        return "watch_low_edge"

    if alignment_status == "not_aligned":
        return "router_mismatch_review"

    return "watch_mixed_evidence"


def build_validation_table(
    router_df: pd.DataFrame,
    global_perf_df: pd.DataFrame,
    best_perf_df: pd.DataFrame,
    min_observations: int,
) -> pd.DataFrame:
    rows = []

    perf_lookup = {}

    for _, perf in global_perf_df.iterrows():
        key = (
            perf.get("timeframe"),
            perf.get("composite_regime"),
            perf.get("strategy_name"),
        )
        perf_lookup[key] = perf

    best_lookup = {}

    for _, best in best_perf_df.iterrows():
        key = (
            best.get("timeframe"),
            best.get("composite_regime"),
        )
        best_lookup[key] = best

    for _, row in router_df.iterrows():
        timeframe = row.get("timeframe")
        regime = row.get("current_regime")
        family = row.get("recommended_strategy_family")
        execution_permission = row.get("execution_permission")

        candidate_proxy_strategies = ROUTER_FAMILY_TO_PROXY_STRATEGIES.get(
            family,
            set(),
        )

        best_key = (timeframe, regime)
        best_row = best_lookup.get(best_key)

        best_strategy = None
        best_win_rate = None
        best_profit_factor = None
        best_observations = None

        if best_row is not None:
            best_strategy = best_row.get("strategy_name")
            best_win_rate = best_row.get("win_rate_mean_pct")
            best_profit_factor = best_row.get("profit_factor_median")
            best_observations = best_row.get("total_observations")

        selected_perf = None
        selected_proxy_strategy = None

        for proxy_strategy in candidate_proxy_strategies:
            key = (timeframe, regime, proxy_strategy)
            perf_row = perf_lookup.get(key)

            if perf_row is None:
                continue

            if selected_perf is None:
                selected_perf = perf_row
                selected_proxy_strategy = proxy_strategy
                continue

            current_score = calculate_evidence_score(
                observations=perf_row.get("total_observations", 0),
                win_rate_pct=perf_row.get("win_rate_mean_pct", 0),
                profit_factor=perf_row.get("profit_factor_median"),
                sharpe_proxy=perf_row.get("sharpe_proxy_median"),
                min_observations=min_observations,
            )

            previous_score = calculate_evidence_score(
                observations=selected_perf.get("total_observations", 0),
                win_rate_pct=selected_perf.get("win_rate_mean_pct", 0),
                profit_factor=selected_perf.get("profit_factor_median"),
                sharpe_proxy=selected_perf.get("sharpe_proxy_median"),
                min_observations=min_observations,
            )

            if current_score > previous_score:
                selected_perf = perf_row
                selected_proxy_strategy = proxy_strategy

        if selected_perf is None:
            evidence_observations = None
            evidence_win_rate = None
            evidence_profit_factor = None
            evidence_sharpe = None
            evidence_strength = "no_matching_performance_data"
            evidence_score = 0.0
            alignment_status = "no_data"
        else:
            evidence_observations = selected_perf.get("total_observations")
            evidence_win_rate = selected_perf.get("win_rate_mean_pct")
            evidence_profit_factor = selected_perf.get("profit_factor_median")
            evidence_sharpe = selected_perf.get("sharpe_proxy_median")

            evidence_strength = classify_evidence_strength(
                observations=evidence_observations,
                win_rate_pct=evidence_win_rate,
                profit_factor=evidence_profit_factor,
                sharpe_proxy=evidence_sharpe,
                min_observations=min_observations,
            )

            evidence_score = calculate_evidence_score(
                observations=evidence_observations,
                win_rate_pct=evidence_win_rate,
                profit_factor=evidence_profit_factor,
                sharpe_proxy=evidence_sharpe,
                min_observations=min_observations,
            )

            if selected_proxy_strategy == best_strategy:
                alignment_status = "aligned"
            elif best_strategy in candidate_proxy_strategies:
                alignment_status = "aligned_family"
            else:
                alignment_status = "not_aligned"

        recommendation_status = classify_recommendation_status(
            execution_permission=execution_permission,
            alignment_status=alignment_status,
            evidence_strength=evidence_strength,
            evidence_score=evidence_score,
        )

        rows.append(
            {
                "symbol": row.get("symbol"),
                "timeframe": timeframe,
                "latest_time": row.get("latest_time"),
                "current_regime": regime,
                "forecast_signal": row.get("forecast_signal"),
                "dashboard_bucket": row.get("dashboard_bucket"),
                "directional_bias": row.get("directional_bias"),
                "recommended_strategy_family": family,
                "selected_proxy_strategy": selected_proxy_strategy,
                "historical_best_strategy": best_strategy,
                "alignment_status": alignment_status,
                "evidence_strength": evidence_strength,
                "evidence_score": evidence_score,
                "recommendation_status": recommendation_status,
                "execution_permission": execution_permission,
                "risk_mode": row.get("risk_mode"),
                "position_sizing_profile": row.get("position_sizing_profile"),
                "primary_direction": row.get("primary_direction"),
                "router_score": row.get("router_score"),
                "priority_score": row.get("priority_score"),
                "current_confidence": row.get("current_confidence"),
                "evidence_observations": evidence_observations,
                "evidence_win_rate_pct": evidence_win_rate,
                "evidence_profit_factor": evidence_profit_factor,
                "evidence_sharpe_proxy": evidence_sharpe,
                "best_strategy_observations": best_observations,
                "best_strategy_win_rate_pct": best_win_rate,
                "best_strategy_profit_factor": best_profit_factor,
                "persistence_probability_pct": row.get("persistence_probability_pct"),
                "breakout_probability_pct": row.get("breakout_probability_pct"),
                "volatility_expansion_probability_pct": row.get("volatility_expansion_probability_pct"),
                "bullish_probability_pct": row.get("bullish_probability_pct"),
                "bearish_probability_pct": row.get("bearish_probability_pct"),
                "range_probability_pct": row.get("range_probability_pct"),
                "transition_probability_pct": row.get("transition_probability_pct"),
                "timeframe_rank": row.get(
                    "timeframe_rank",
                    TIMEFRAME_ORDER.get(timeframe, 999999),
                ),
            }
        )

    validation_df = pd.DataFrame(rows)

    if validation_df.empty:
        return validation_df

    validation_df = validation_df.sort_values(
        by=["evidence_score", "router_score", "timeframe_rank", "symbol"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)

    return validation_df


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


def main(mode: str, permission: str, min_observations: int) -> None:
    logger.info("=" * 80)
    logger.info("Starting BACQE Router Validation Against Strategy Performance")
    logger.info(f"Mode: {mode}")
    logger.info(f"Permission: {permission}")
    logger.info(f"Minimum observations: {min_observations}")
    logger.info("=" * 80)

    router_path = get_router_path(mode=mode, permission=permission)
    global_perf_path, best_perf_path = get_performance_paths(mode=mode)

    logger.info(f"Router path: {router_path}")
    logger.info(f"Global performance path: {global_perf_path}")
    logger.info(f"Best performance path: {best_perf_path}")
    logger.info(f"Output root: {OUTPUT_ROOT}")

    if not router_path.exists():
        raise FileNotFoundError(f"Missing router file: {router_path}")

    if not global_perf_path.exists():
        raise FileNotFoundError(f"Missing global performance file: {global_perf_path}")

    if not best_perf_path.exists():
        raise FileNotFoundError(f"Missing best performance file: {best_perf_path}")

    router_df = pd.read_parquet(router_path)
    global_perf_df = pd.read_parquet(global_perf_path)
    best_perf_df = pd.read_parquet(best_perf_path)

    if router_df.empty:
        logger.warning("Router input is empty")
        return

    validation_df = build_validation_table(
        router_df=router_df,
        global_perf_df=global_perf_df,
        best_perf_df=best_perf_df,
        min_observations=min_observations,
    )

    if validation_df.empty:
        logger.warning("Validation output is empty")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    permission_suffix = "" if permission == "all" else f"_{permission}"

    save_outputs(
        validation_df,
        f"router_strategy_validation{permission_suffix}",
        timestamp,
        mode,
    )

    approved_df = validation_df[
        validation_df["recommendation_status"] == "approved_evidence_weighted"
    ].copy()

    watch_df = validation_df[
        validation_df["recommendation_status"].isin(
            [
                "watch_low_edge",
                "watch_mixed_evidence",
                "watch_insufficient_evidence",
                "router_mismatch_review",
            ]
        )
    ].copy()

    rejected_df = validation_df[
        validation_df["recommendation_status"] == "reject_historical_evidence"
    ].copy()

    save_outputs(
        approved_df,
        f"router_strategy_validation_approved{permission_suffix}",
        timestamp,
        mode,
    )

    save_outputs(
        watch_df,
        f"router_strategy_validation_watch{permission_suffix}",
        timestamp,
        mode,
    )

    save_outputs(
        rejected_df,
        f"router_strategy_validation_rejected{permission_suffix}",
        timestamp,
        mode,
    )

    logger.info("=" * 80)
    logger.info("Router validation completed")
    logger.info(f"Router rows: {len(router_df)}")
    logger.info(f"Validation rows: {len(validation_df)}")
    logger.info(f"Approved rows: {len(approved_df)}")
    logger.info(f"Watch rows: {len(watch_df)}")
    logger.info(f"Rejected rows: {len(rejected_df)}")
    logger.info("=" * 80)

    logger.info("Recommendation status counts:")
    logger.info(validation_df["recommendation_status"].value_counts().to_string())

    logger.info("Evidence strength counts:")
    logger.info(validation_df["evidence_strength"].value_counts().to_string())

    logger.info("Alignment status counts:")
    logger.info(validation_df["alignment_status"].value_counts().to_string())

    logger.info("Top 30 evidence-weighted router decisions:")
    display_cols = [
        "symbol",
        "timeframe",
        "current_regime",
        "recommended_strategy_family",
        "selected_proxy_strategy",
        "historical_best_strategy",
        "alignment_status",
        "evidence_strength",
        "evidence_score",
        "recommendation_status",
        "execution_permission",
        "risk_mode",
    ]

    logger.info(validation_df[display_cols].head(30).to_string(index=False))


if __name__ == "__main__":
    args = parse_args()
    main(
        mode=args.mode,
        permission=args.permission,
        min_observations=args.min_observations,
    )