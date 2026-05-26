"""
BACQE REGIME ENGINE - 53 Build Strategy-Regime Mapping Engine

Maps current strategy environments to suitable strategy families.

Input:
    E:/Quant_Lab/data/analysis/regimes/strategy_router_dashboard_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/regimes/strategy_regime_mapping_latest.csv
    E:/Quant_Lab/reports/regimes/strategy_mapping/strategy_regime_mapping_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "regimes"
    / "strategy_router_dashboard_latest.csv"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "regimes" / "strategy_mapping"


STRATEGY_MAP = {
    "bullish_trend_environment": {
        "primary_strategy": "long_trend_continuation",
        "secondary_strategy": "long_pullback_continuation",
        "avoid_strategy": "aggressive_short_mean_reversion",
        "environment_logic": "Strong bullish cross-timeframe alignment favours continuation or pullback-following research.",
    },
    "bearish_trend_environment": {
        "primary_strategy": "short_trend_continuation",
        "secondary_strategy": "short_pullback_continuation",
        "avoid_strategy": "aggressive_long_mean_reversion",
        "environment_logic": "Strong bearish cross-timeframe alignment favours continuation or pullback-following research.",
    },
    "moderate_bullish_trend_environment": {
        "primary_strategy": "selective_long_trend_continuation",
        "secondary_strategy": "breakout_watch_or_pullback_entry",
        "avoid_strategy": "high_conviction_short_bias",
        "environment_logic": "Moderate bullish alignment exists, but lower confidence or compression requires selectivity.",
    },
    "moderate_bearish_trend_environment": {
        "primary_strategy": "selective_short_trend_continuation",
        "secondary_strategy": "breakdown_watch_or_pullback_entry",
        "avoid_strategy": "high_conviction_long_bias",
        "environment_logic": "Moderate bearish alignment exists, but confirmation and risk controls remain important.",
    },
    "quiet_compression_environment": {
        "primary_strategy": "breakout_watch",
        "secondary_strategy": "range_mean_reversion_research",
        "avoid_strategy": "high_leverage_momentum_chasing",
        "environment_logic": "Quiet compressed states may precede expansion, but can also remain range-bound.",
    },
    "volatile_uncertain_environment": {
        "primary_strategy": "defensive_volatility_research",
        "secondary_strategy": "reduced_size_breakout_research",
        "avoid_strategy": "tight_stop_noise_sensitive_entries",
        "environment_logic": "High volatility without clear alignment requires defensive handling.",
    },
    "mixed_transition_environment": {
        "primary_strategy": "defensive_no_trade_or_observation",
        "secondary_strategy": "low_conviction_range_research",
        "avoid_strategy": "high_conviction_directional_strategy",
        "environment_logic": "Mixed transition environments lack enough cross-timeframe agreement for strong directional routing.",
    },
    "neutral_observation_environment": {
        "primary_strategy": "observation_only",
        "secondary_strategy": "data_collection",
        "avoid_strategy": "all_high_conviction_strategies",
        "environment_logic": "No clear structural edge is visible from current regime alignment.",
    },
}


def load_router_dashboard() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Strategy router dashboard file not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH, low_memory=False)

    if df.empty:
        raise ValueError("Strategy router dashboard file is empty.")

    return df


def map_strategy_environment(row: pd.Series) -> dict:
    environment = row.get("primary_strategy_environment", "neutral_observation_environment")

    mapping = STRATEGY_MAP.get(
        environment,
        STRATEGY_MAP["neutral_observation_environment"],
    )

    return mapping


def classify_strategy_confidence(row: pd.Series) -> str:
    strength = float(row.get("directional_strength_score", 0))
    confidence = float(row.get("avg_regime_confidence", 0))
    priority = str(row.get("research_priority", ""))

    if priority == "high_priority_watchlist" and strength >= 0.75 and confidence >= 0.60:
        return "high_research_confidence"

    if priority == "medium_priority_watchlist" and strength >= 0.50:
        return "medium_research_confidence"

    if priority == "compression_watchlist":
        return "watchlist_research_confidence"

    return "low_research_confidence"


def classify_execution_posture(row: pd.Series) -> str:
    risk_mode = str(row.get("risk_mode", ""))
    priority = str(row.get("research_priority", ""))
    environment = str(row.get("primary_strategy_environment", ""))

    if priority == "high_priority_watchlist" and risk_mode == "normal_research_risk":
        return "research_ready_environment"

    if "compression" in priority or "compression" in environment:
        return "wait_for_expansion_confirmation"

    if "defensive" in risk_mode or "mixed" in environment:
        return "observation_or_defensive_only"

    if priority == "medium_priority_watchlist":
        return "selective_research_environment"

    return "background_monitoring"


def build_mapping(router: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, row in router.iterrows():
        mapping = map_strategy_environment(row)

        record = row.to_dict()
        record.update(mapping)

        record["strategy_confidence"] = classify_strategy_confidence(row)
        record["execution_posture"] = classify_execution_posture(row)
        record["mapping_time_utc"] = datetime.now(timezone.utc).isoformat()

        rows.append(record)

    mapped = pd.DataFrame(rows)

    priority_order = {
        "research_ready_environment": 1,
        "selective_research_environment": 2,
        "wait_for_expansion_confirmation": 3,
        "observation_or_defensive_only": 4,
        "background_monitoring": 5,
    }

    mapped["execution_posture_rank"] = (
        mapped["execution_posture"].map(priority_order).fillna(99).astype(int)
    )

    mapped = mapped.sort_values(
        [
            "execution_posture_rank",
            "directional_strength_score",
            "avg_regime_confidence",
        ],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    return mapped


def build_report(mapped: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    display_cols = [
        "symbol",
        "primary_strategy_environment",
        "primary_strategy",
        "secondary_strategy",
        "avoid_strategy",
        "strategy_confidence",
        "execution_posture",
        "directional_bias",
        "risk_mode",
        "research_priority",
        "directional_strength_score",
        "avg_regime_confidence",
        "environment_logic",
    ]

    lines = []

    lines.append("=" * 130)
    lines.append("BACQE STRATEGY-REGIME MAPPING ENGINE")
    lines.append("=" * 130)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append("-" * 130)

    lines.append("")
    lines.append("STRATEGY MAPPING OUTPUT")
    lines.append("-" * 130)
    lines.append(mapped[display_cols].to_string(index=False))

    lines.append("")
    lines.append("EXECUTION POSTURE SUMMARY")
    lines.append("-" * 130)
    posture_counts = mapped["execution_posture"].value_counts()
    lines.append(posture_counts.to_string())

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 130)
    lines.append("This script maps regime environments to strategy research families.")
    lines.append("It does not produce buy/sell signals.")
    lines.append("primary_strategy means the strategy family most aligned with current structure.")
    lines.append("avoid_strategy identifies strategy families that conflict with current structure.")
    lines.append("execution_posture controls whether the environment is research-ready, selective, defensive, or watch-only.")
    lines.append("=" * 130)

    return "\n".join(lines)


def main() -> None:
    print("=" * 130)
    print("BACQE REGIME ENGINE - 53 BUILD STRATEGY-REGIME MAPPING ENGINE")
    print("=" * 130)
    print(f"Input: {INPUT_PATH}")
    print("-" * 130)

    router = load_router_dashboard()
    mapped = build_mapping(router)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "strategy_regime_mapping_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "strategy_regime_mapping_latest.parquet"
    json_path = OUTPUT_ANALYSIS_DIR / "strategy_regime_mapping_latest.json"
    report_path = OUTPUT_REPORT_DIR / "strategy_regime_mapping_latest.txt"

    mapped.to_csv(csv_path, index=False)
    mapped.to_parquet(parquet_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(mapped.to_dict(orient="records"), f, indent=4, default=str)

    report = build_report(mapped)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Strategy-regime mapping engine created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"JSON:    {json_path}")
    print(f"Report:  {report_path}")
    print("-" * 130)
    print(report)
    print("=" * 130)


if __name__ == "__main__":
    main()