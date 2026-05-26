"""
BACQE REGIME ENGINE - 58 Rank Adaptive Strategy Opportunities

Ranks current strategy-performance candidates into adaptive opportunity watchlists.

Input:
    E:/Quant_Lab/data/analysis/regimes/current_strategy_performance_candidates_all_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/regimes/adaptive_strategy_opportunities_latest.csv
    E:/Quant_Lab/reports/regimes/adaptive_opportunities/adaptive_strategy_opportunities_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "regimes"
    / "current_strategy_performance_candidates_all_latest.csv"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "regimes" / "adaptive_opportunities"


POSTURE_WEIGHT = {
    "research_ready_environment": 30,
    "selective_research_environment": 18,
    "wait_for_expansion_confirmation": 12,
    "observation_or_defensive_only": -25,
    "background_monitoring": -10,
}

QUALITY_WEIGHT = {
    "strong_candidate": 25,
    "watch_candidate": 10,
    "context_only_defensive": -20,
    "background": 0,
}

EVIDENCE_WEIGHT = {
    "higher": 20,
    "medium": 10,
    "low": -10,
}


def load_candidates() -> pd.DataFrame:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Candidate input not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH, low_memory=False)

    if df.empty:
        raise ValueError("Candidate input file is empty.")

    return df


def classify_opportunity_quality(row: pd.Series) -> str:
    score = row.get("opportunity_score", np.nan)
    posture = str(row.get("current_execution_posture", ""))
    evidence = str(row.get("evidence_quality", ""))

    if posture == "observation_or_defensive_only":
        return "defensive_context_only"

    if pd.isna(score):
        return "unclassified"

    if score >= 120 and evidence in {"higher", "medium"}:
        return "elite_research_opportunity"

    if score >= 95 and evidence in {"higher", "medium"}:
        return "strong_research_opportunity"

    if score >= 75:
        return "watchlist_opportunity"

    if score >= 50:
        return "low_conviction_opportunity"

    return "background_or_reject"


def classify_action(row: pd.Series) -> str:
    quality = row.get("opportunity_quality", "")
    posture = row.get("current_execution_posture", "")

    if quality == "elite_research_opportunity":
        return "prioritise_for_deep_research"

    if quality == "strong_research_opportunity":
        return "add_to_primary_watchlist"

    if quality == "watchlist_opportunity":
        if posture == "wait_for_expansion_confirmation":
            return "monitor_for_expansion_confirmation"
        return "add_to_secondary_watchlist"

    if quality == "defensive_context_only":
        return "observe_only_no_strategy_activation"

    return "background_monitor"


def score_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    numeric_cols = [
        "win_rate_pct",
        "profit_factor",
        "sharpe_proxy",
        "total_return_proxy",
        "performance_score",
        "observation_count",
        "trade_count",
        "match_score",
        "adaptive_candidate_score",
    ]

    for col in numeric_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    data["opportunity_score"] = 0.0

    data["opportunity_score"] += data["adaptive_candidate_score"].fillna(0)
    data["opportunity_score"] += data["current_execution_posture"].map(POSTURE_WEIGHT).fillna(0)
    data["opportunity_score"] += data["candidate_quality_label"].map(QUALITY_WEIGHT).fillna(0)
    data["opportunity_score"] += data["evidence_quality"].map(EVIDENCE_WEIGHT).fillna(0)

    data["opportunity_score"] += (data["profit_factor"].fillna(1).clip(lower=0, upper=2.5) - 1) * 8
    data["opportunity_score"] += data["sharpe_proxy"].fillna(0).clip(lower=-2, upper=2) * 5

    data["opportunity_score"] -= data["performance_score"].isna().astype(int) * 5

    data["opportunity_score"] = data["opportunity_score"].round(6)

    data["opportunity_quality"] = data.apply(classify_opportunity_quality, axis=1)
    data["recommended_action"] = data.apply(classify_action, axis=1)

    data["opportunity_time_utc"] = datetime.now(timezone.utc).isoformat()

    return data


def build_symbol_best(opportunities: pd.DataFrame) -> pd.DataFrame:
    ranked = opportunities.sort_values(
        ["symbol", "opportunity_score", "adaptive_candidate_score", "match_score"],
        ascending=[True, False, False, False],
    )

    best = ranked.groupby("symbol", group_keys=False).head(5).reset_index(drop=True)

    return best


def build_dashboard_summary(opportunities: pd.DataFrame) -> pd.DataFrame:
    summary = (
        opportunities.groupby(
            [
                "symbol",
                "current_strategy_environment",
                "current_execution_posture",
                "current_primary_strategy",
            ],
            dropna=False,
        )
        .agg(
            candidates=("candidate_strategy_name", "count"),
            elite_count=("opportunity_quality", lambda s: (s == "elite_research_opportunity").sum()),
            strong_count=("opportunity_quality", lambda s: (s == "strong_research_opportunity").sum()),
            watch_count=("opportunity_quality", lambda s: (s == "watchlist_opportunity").sum()),
            defensive_count=("opportunity_quality", lambda s: (s == "defensive_context_only").sum()),
            best_opportunity_score=("opportunity_score", "max"),
            avg_opportunity_score=("opportunity_score", "mean"),
            best_adaptive_candidate_score=("adaptive_candidate_score", "max"),
        )
        .reset_index()
    )

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(6)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    summary = summary.sort_values(
        ["best_opportunity_score", "strong_count", "watch_count"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    return summary


def build_report(best: pd.DataFrame, summary: pd.DataFrame, all_opps: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    lines = []

    lines.append("=" * 150)
    lines.append("BACQE ADAPTIVE STRATEGY OPPORTUNITY ENGINE")
    lines.append("=" * 150)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append("-" * 150)

    lines.append("")
    lines.append("OPPORTUNITY SUMMARY BY SYMBOL")
    lines.append("-" * 150)
    lines.append(summary.to_string(index=False))

    lines.append("")
    lines.append("BEST CURRENT ADAPTIVE STRATEGY OPPORTUNITIES")
    lines.append("-" * 150)

    display_cols = [
        "symbol",
        "current_strategy_environment",
        "current_execution_posture",
        "current_primary_strategy",
        "candidate_strategy_name",
        "candidate_symbol_scope",
        "candidate_timeframe",
        "candidate_composite_regime",
        "win_rate_pct",
        "profit_factor",
        "sharpe_proxy",
        "evidence_quality",
        "match_score",
        "adaptive_candidate_score",
        "opportunity_score",
        "opportunity_quality",
        "recommended_action",
    ]

    lines.append(best[display_cols].to_string(index=False))

    lines.append("")
    lines.append("QUALITY COUNTS")
    lines.append("-" * 150)
    lines.append(all_opps["opportunity_quality"].value_counts().to_string())

    lines.append("")
    lines.append("ACTION COUNTS")
    lines.append("-" * 150)
    lines.append(all_opps["recommended_action"].value_counts().to_string())

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 150)
    lines.append("This engine ranks adaptive research opportunities, not trades.")
    lines.append("opportunity_score combines current environment posture, historical performance evidence, and compatibility scoring.")
    lines.append("defensive_context_only means historical candidates exist but current posture blocks strategy activation.")
    lines.append("elite/strong opportunities are research-priority candidates, not automatic entries.")
    lines.append("=" * 150)

    return "\n".join(lines)


def main() -> None:
    print("=" * 150)
    print("BACQE REGIME ENGINE - 58 RANK ADAPTIVE STRATEGY OPPORTUNITIES")
    print("=" * 150)

    candidates = load_candidates()
    opportunities = score_opportunities(candidates)
    best = build_symbol_best(opportunities)
    summary = build_dashboard_summary(opportunities)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    all_csv = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_opportunities_all_latest.csv"
    all_parquet = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_opportunities_all_latest.parquet"

    best_csv = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_opportunities_latest.csv"
    best_parquet = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_opportunities_latest.parquet"
    best_json = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_opportunities_latest.json"

    summary_csv = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_opportunity_summary_latest.csv"
    summary_parquet = OUTPUT_ANALYSIS_DIR / "adaptive_strategy_opportunity_summary_latest.parquet"

    report_path = OUTPUT_REPORT_DIR / "adaptive_strategy_opportunities_latest.txt"

    opportunities.to_csv(all_csv, index=False)
    opportunities.to_parquet(all_parquet, index=False)

    best.to_csv(best_csv, index=False)
    best.to_parquet(best_parquet, index=False)

    with open(best_json, "w", encoding="utf-8") as f:
        json.dump(best.to_dict(orient="records"), f, indent=4, default=str)

    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    report = build_report(best, summary, opportunities)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Adaptive strategy opportunities created.")
    print(f"All CSV:       {all_csv}")
    print(f"All Parquet:   {all_parquet}")
    print(f"Best CSV:      {best_csv}")
    print(f"Best Parquet:  {best_parquet}")
    print(f"Best JSON:     {best_json}")
    print(f"Summary CSV:   {summary_csv}")
    print(f"Summary Parquet: {summary_parquet}")
    print(f"Report:        {report_path}")
    print("-" * 150)
    print(report)
    print("=" * 150)


if __name__ == "__main__":
    main()