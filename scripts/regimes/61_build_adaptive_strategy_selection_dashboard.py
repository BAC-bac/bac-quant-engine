"""
BACQE REGIME ENGINE - 61 Build Adaptive Strategy Selection Dashboard

Creates a cleaner decision-support dashboard from the adaptive operator console.

This is NOT a trade signal engine.

It classifies symbols into:
    - PRIORITY RESEARCH
    - PRIMARY WATCHLIST
    - EXPANSION CONFIRMATION
    - DEFENSIVE FILTER
    - BACKGROUND MONITORING
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "bacqe_adaptive_operator_console_latest.csv"

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "bacqe_strategy_selection"


def load_console() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Adaptive operator console not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH, low_memory=False)

    if df.empty:
        raise ValueError("Adaptive operator console is empty.")

    return df


def classify_selection_bucket(row: pd.Series) -> str:
    posture = str(row.get("execution_posture", ""))
    action = str(row.get("recommended_action", ""))
    quality = str(row.get("opportunity_quality", ""))
    score = pd.to_numeric(row.get("opportunity_score"), errors="coerce")

    if posture == "research_ready_environment" and quality == "elite_research_opportunity":
        return "PRIORITY_RESEARCH"

    if posture == "research_ready_environment":
        return "PRIMARY_WATCHLIST"

    if posture == "selective_research_environment":
        return "PRIMARY_WATCHLIST"

    if posture == "wait_for_expansion_confirmation":
        return "EXPANSION_CONFIRMATION"

    if posture == "observation_or_defensive_only":
        return "DEFENSIVE_FILTER"

    if "background" in action or pd.isna(score):
        return "BACKGROUND_MONITORING"

    return "BACKGROUND_MONITORING"


def classify_selection_confidence(row: pd.Series) -> str:
    score = pd.to_numeric(row.get("opportunity_score"), errors="coerce")
    evidence = str(row.get("evidence_quality", ""))
    posture = str(row.get("execution_posture", ""))

    if posture == "observation_or_defensive_only":
        return "blocked_by_environment"

    if pd.isna(score):
        return "unknown"

    if score >= 160 and evidence == "higher":
        return "very_high_research_confidence"

    if score >= 130:
        return "high_research_confidence"

    if score >= 95:
        return "medium_research_confidence"

    if score >= 60:
        return "low_research_confidence"

    return "weak_or_background"


def classify_operator_instruction(row: pd.Series) -> str:
    bucket = row.get("selection_bucket", "")
    posture = str(row.get("execution_posture", ""))

    if bucket == "PRIORITY_RESEARCH":
        return "focus_deep_research_first"

    if bucket == "PRIMARY_WATCHLIST":
        return "monitor_as_primary_watchlist"

    if bucket == "EXPANSION_CONFIRMATION":
        return "wait_for_volatility_or_participation_expansion"

    if bucket == "DEFENSIVE_FILTER":
        return "do_not_activate_directional_research"

    return "background_monitor_only"


def build_selection_dashboard(console: pd.DataFrame) -> pd.DataFrame:
    df = console.copy()

    numeric_cols = [
        "opportunity_score",
        "best_opportunity_score",
        "directional_alignment_score",
        "directional_strength_score",
        "volatility_alignment_score",
        "avg_regime_confidence",
        "elite_count",
        "strong_count",
        "watch_count",
        "defensive_count",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["selection_bucket"] = df.apply(classify_selection_bucket, axis=1)
    df["selection_confidence"] = df.apply(classify_selection_confidence, axis=1)
    df["operator_instruction"] = df.apply(classify_operator_instruction, axis=1)

    bucket_rank = {
        "PRIORITY_RESEARCH": 1,
        "PRIMARY_WATCHLIST": 2,
        "EXPANSION_CONFIRMATION": 3,
        "DEFENSIVE_FILTER": 4,
        "BACKGROUND_MONITORING": 5,
    }

    df["selection_rank"] = df["selection_bucket"].map(bucket_rank).fillna(99).astype(int)
    df["selection_time_utc"] = datetime.now(timezone.utc).isoformat()

    df = df.sort_values(
        ["selection_rank", "opportunity_score", "directional_strength_score"],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    return df


def build_bucket_summary(selection: pd.DataFrame) -> pd.DataFrame:
    summary = (
        selection.groupby("selection_bucket", dropna=False)
        .agg(
            symbols=("symbol", "count"),
            avg_opportunity_score=("opportunity_score", "mean"),
            max_opportunity_score=("opportunity_score", "max"),
            elite_total=("elite_count", "sum"),
            strong_total=("strong_count", "sum"),
            watch_total=("watch_count", "sum"),
            defensive_total=("defensive_count", "sum"),
        )
        .reset_index()
    )

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(6)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    bucket_rank = {
        "PRIORITY_RESEARCH": 1,
        "PRIMARY_WATCHLIST": 2,
        "EXPANSION_CONFIRMATION": 3,
        "DEFENSIVE_FILTER": 4,
        "BACKGROUND_MONITORING": 5,
    }

    summary["bucket_rank"] = summary["selection_bucket"].map(bucket_rank).fillna(99).astype(int)
    summary = summary.sort_values("bucket_rank").reset_index(drop=True)

    return summary


def build_report(selection: pd.DataFrame, summary: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    display_cols = [
        "symbol",
        "selection_bucket",
        "selection_confidence",
        "operator_instruction",
        "execution_posture",
        "research_priority",
        "primary_strategy_environment",
        "primary_strategy",
        "candidate_strategy_name",
        "candidate_timeframe",
        "candidate_composite_regime",
        "opportunity_score",
        "opportunity_quality",
        "recommended_action",
        "directional_bias",
        "risk_mode",
        "elite_count",
        "strong_count",
        "watch_count",
        "defensive_count",
    ]

    available_cols = [col for col in display_cols if col in selection.columns]

    lines = []

    lines.append("=" * 150)
    lines.append("BACQE ADAPTIVE STRATEGY SELECTION DASHBOARD")
    lines.append("=" * 150)
    lines.append(f"Dashboard time UTC: {now_utc}")
    lines.append(f"Input:              {INPUT_PATH}")
    lines.append("-" * 150)

    lines.append("")
    lines.append("SELECTION BUCKET SUMMARY")
    lines.append("-" * 150)
    lines.append(summary.to_string(index=False))

    bucket_order = [
        "PRIORITY_RESEARCH",
        "PRIMARY_WATCHLIST",
        "EXPANSION_CONFIRMATION",
        "DEFENSIVE_FILTER",
        "BACKGROUND_MONITORING",
    ]

    for bucket in bucket_order:
        section = selection[selection["selection_bucket"] == bucket].copy()

        lines.append("")
        lines.append(bucket)
        lines.append("-" * 150)

        if section.empty:
            lines.append("No symbols currently in this bucket.")
        else:
            lines.append(section[available_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 150)
    lines.append("This dashboard is a decision-support surface, not a trade signal engine.")
    lines.append("PRIORITY_RESEARCH means the current environment, routing logic, and historical evidence are aligned for deeper research.")
    lines.append("EXPANSION_CONFIRMATION means structure is promising but participation/volatility expansion is still required.")
    lines.append("DEFENSIVE_FILTER means historical candidates may exist, but current conditions block activation.")
    lines.append("The dashboard is designed to sit above the adaptive operator console as a cleaner shortlist view.")
    lines.append("=" * 150)

    return "\n".join(lines)


def main() -> None:
    print("=" * 150)
    print("BACQE REGIME ENGINE - 61 BUILD ADAPTIVE STRATEGY SELECTION DASHBOARD")
    print("=" * 150)
    print(f"Input: {INPUT_PATH}")
    print("-" * 150)

    console = load_console()
    selection = build_selection_dashboard(console)
    summary = build_bucket_summary(selection)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    selection_csv = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_selection_dashboard_latest.csv"
    selection_parquet = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_selection_dashboard_latest.parquet"
    selection_json = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_selection_dashboard_latest.json"

    summary_csv = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_selection_summary_latest.csv"
    summary_parquet = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_selection_summary_latest.parquet"

    report_path = OUTPUT_REPORT_DIR / "adaptive_strategy_selection_dashboard_latest.txt"

    selection.to_csv(selection_csv, index=False)
    selection.to_parquet(selection_parquet, index=False)

    with open(selection_json, "w", encoding="utf-8") as f:
        json.dump(selection.to_dict(orient="records"), f, indent=4, default=str)

    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    report = build_report(selection, summary)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Adaptive strategy selection dashboard created.")
    print(f"Selection CSV:     {selection_csv}")
    print(f"Selection Parquet: {selection_parquet}")
    print(f"Selection JSON:    {selection_json}")
    print(f"Summary CSV:       {summary_csv}")
    print(f"Summary Parquet:   {summary_parquet}")
    print(f"Report:            {report_path}")
    print("-" * 150)
    print(report)
    print("=" * 150)


if __name__ == "__main__":
    main()