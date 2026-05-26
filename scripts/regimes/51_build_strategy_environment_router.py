"""
BACQE REGIME ENGINE - 51 Build Strategy Environment Router

Reads market regime alignment scores and classifies the current strategy
environment for each symbol.

This is NOT a trading signal engine.
It classifies market structure into strategy-environment suitability.

Input:
    E:/Quant_Lab/data/analysis/regimes/market_regime_alignment_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/regimes/strategy_environment_router_latest.csv
    E:/Quant_Lab/reports/regimes/strategy_environment_router/strategy_environment_router_latest.txt
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
    / "market_regime_alignment_latest.csv"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "regimes" / "strategy_environment_router"


def classify_primary_environment(row: pd.Series) -> str:
    alignment = str(row.get("alignment_label", "")).lower()
    risk_env = str(row.get("risk_environment_label", "")).lower()

    directional_strength = float(row.get("directional_strength_score", 0))
    direction = float(row.get("directional_alignment_score", 0))
    volatility = float(row.get("volatility_alignment_score", 0))
    confidence = float(row.get("avg_regime_confidence", 0))

    if directional_strength >= 0.75 and direction > 0 and confidence >= 0.60:
        if volatility > 0.30:
            return "bullish_trend_high_vol_environment"
        return "bullish_trend_environment"

    if directional_strength >= 0.75 and direction < 0 and confidence >= 0.60:
        if volatility > 0.30:
            return "bearish_trend_high_vol_environment"
        return "bearish_trend_environment"

    if directional_strength >= 0.50 and direction > 0:
        return "moderate_bullish_trend_environment"

    if directional_strength >= 0.50 and direction < 0:
        return "moderate_bearish_trend_environment"

    if volatility <= -0.40:
        return "quiet_compression_environment"

    if volatility >= 0.40:
        return "volatile_uncertain_environment"

    if "mixed" in alignment or "mixed" in risk_env:
        return "mixed_transition_environment"

    return "neutral_observation_environment"


def classify_strategy_family(row: pd.Series) -> str:
    primary = row.get("primary_strategy_environment", "")
    volatility = float(row.get("volatility_alignment_score", 0))
    strength = float(row.get("directional_strength_score", 0))

    if "bullish_trend" in primary or "bearish_trend" in primary:
        if volatility > 0.30:
            return "trend_continuation_with_risk_controls"
        return "trend_following_or_pullback_continuation"

    if "moderate_bullish" in primary or "moderate_bearish" in primary:
        return "selective_trend_following"

    if "quiet_compression" in primary:
        return "breakout_watch_or_mean_reversion_research"

    if "volatile_uncertain" in primary:
        return "defensive_or_volatility_strategy_research"

    if "mixed_transition" in primary:
        if strength >= 0.40:
            return "low_conviction_directional_watch"
        return "neutral_wait_or_range_research"

    return "observation_only"


def classify_risk_mode(row: pd.Series) -> str:
    volatility = float(row.get("volatility_alignment_score", 0))
    confidence = float(row.get("avg_regime_confidence", 0))
    strength = float(row.get("directional_strength_score", 0))

    if volatility >= 0.40:
        return "reduced_size_high_volatility"

    if confidence < 0.55:
        return "reduced_size_low_confidence"

    if strength >= 0.75 and confidence >= 0.60:
        return "normal_research_risk"

    if strength < 0.50:
        return "defensive_low_conviction"

    return "moderate_research_risk"


def classify_directional_bias(row: pd.Series) -> str:
    direction = float(row.get("directional_alignment_score", 0))
    strength = float(row.get("directional_strength_score", 0))

    if strength < 0.50:
        return "no_clear_directional_bias"

    if direction > 0:
        return "bullish_bias"

    if direction < 0:
        return "bearish_bias"

    return "neutral_bias"


def classify_research_priority(row: pd.Series) -> str:
    strength = float(row.get("directional_strength_score", 0))
    confidence = float(row.get("avg_regime_confidence", 0))
    volatility = float(row.get("volatility_alignment_score", 0))

    if strength >= 0.75 and confidence >= 0.60:
        return "high_priority_watchlist"

    if strength >= 0.50 and confidence >= 0.65:
        return "medium_priority_watchlist"

    if volatility <= -0.40:
        return "compression_watchlist"

    return "background_monitor"


def build_router(df: pd.DataFrame) -> pd.DataFrame:
    router = df.copy()

    numeric_cols = [
        "directional_alignment_score",
        "directional_strength_score",
        "volatility_alignment_score",
        "avg_regime_confidence",
    ]

    for col in numeric_cols:
        router[col] = pd.to_numeric(router[col], errors="coerce").fillna(0)

    router["primary_strategy_environment"] = router.apply(classify_primary_environment, axis=1)
    router["strategy_family"] = router.apply(classify_strategy_family, axis=1)
    router["risk_mode"] = router.apply(classify_risk_mode, axis=1)
    router["directional_bias"] = router.apply(classify_directional_bias, axis=1)
    router["research_priority"] = router.apply(classify_research_priority, axis=1)

    router["router_time_utc"] = datetime.now(timezone.utc).isoformat()

    router = router.sort_values(
        [
            "research_priority",
            "directional_strength_score",
            "avg_regime_confidence",
        ],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    return router


def build_report(router: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    display_cols = [
        "symbol",
        "alignment_label",
        "risk_environment_label",
        "primary_strategy_environment",
        "strategy_family",
        "directional_bias",
        "risk_mode",
        "research_priority",
        "directional_alignment_score",
        "directional_strength_score",
        "volatility_alignment_score",
        "avg_regime_confidence",
    ]

    lines = []

    lines.append("=" * 120)
    lines.append("BACQE STRATEGY ENVIRONMENT ROUTER")
    lines.append("=" * 120)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append("-" * 120)

    lines.append("")
    lines.append("STRATEGY ENVIRONMENT ROUTER OUTPUT")
    lines.append("-" * 120)
    lines.append(router[display_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 120)
    lines.append("This is a strategy-environment classifier, not a trade signal engine.")
    lines.append("It asks: what type of strategy behaviour may be suitable for the current structure?")
    lines.append("High priority means structurally interesting, not automatically tradable.")
    lines.append("Risk mode is a research control label, not position-sizing advice.")
    lines.append("Future versions can combine this with macro, microstructure, and performance-by-regime data.")
    lines.append("=" * 120)

    return "\n".join(lines)


def main() -> None:
    print("=" * 120)
    print("BACQE REGIME ENGINE - 51 BUILD STRATEGY ENVIRONMENT ROUTER")
    print("=" * 120)
    print(f"Input: {INPUT_PATH}")
    print("-" * 120)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Market regime alignment file not found: {INPUT_PATH}")

    alignment = pd.read_csv(INPUT_PATH, low_memory=False)

    router = build_router(alignment)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "strategy_environment_router_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "strategy_environment_router_latest.parquet"
    json_path = OUTPUT_ANALYSIS_DIR / "strategy_environment_router_latest.json"
    report_path = OUTPUT_REPORT_DIR / "strategy_environment_router_latest.txt"

    router.to_csv(csv_path, index=False)
    router.to_parquet(parquet_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(router.to_dict(orient="records"), f, indent=4, default=str)

    report = build_report(router)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Strategy environment router created.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"JSON:    {json_path}")
    print(f"Report:  {report_path}")
    print("-" * 120)
    print(report)
    print("=" * 120)


if __name__ == "__main__":
    main()