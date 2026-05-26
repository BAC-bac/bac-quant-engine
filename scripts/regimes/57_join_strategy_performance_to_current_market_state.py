"""
BACQE REGIME ENGINE - 57 Join Strategy Performance To Current Market State

Joins the current BACQE master operator state to the strategy performance registry.

Goal:
    Given current market structure, identify historically compatible strategy candidates.

Inputs:
    E:/Quant_Lab/data/analysis/regimes/bacqe_master_operator_dashboard_latest.csv
    E:/Quant_Lab/data/analysis/regimes/strategy_performance_registry_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/regimes/current_strategy_performance_candidates_latest.csv
    E:/Quant_Lab/reports/regimes/current_strategy_candidates/current_strategy_performance_candidates_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

CURRENT_STATE_PATH = (
    DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "bacqe_master_operator_dashboard_latest.csv"
)

REGISTRY_PATH = (
    DATA_LAKE_ROOT / "data" / "analysis" / "regimes" / "strategy_performance_registry_latest.csv"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regimes"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "regimes" / "current_strategy_candidates"


def safe_float(value, default=np.nan):
    try:
        return float(value)
    except Exception:
        return default


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not CURRENT_STATE_PATH.exists():
        raise FileNotFoundError(f"Current state file not found: {CURRENT_STATE_PATH}")

    if not REGISTRY_PATH.exists():
        raise FileNotFoundError(f"Strategy registry file not found: {REGISTRY_PATH}")

    current = pd.read_csv(CURRENT_STATE_PATH, low_memory=False)
    registry = pd.read_csv(REGISTRY_PATH, low_memory=False)

    if current.empty:
        raise ValueError("Current state file is empty.")

    if registry.empty:
        raise ValueError("Strategy registry file is empty.")

    return current, registry


def clean_registry(registry: pd.DataFrame) -> pd.DataFrame:
    df = registry.copy()

    numeric_cols = [
        "avg_return",
        "median_return",
        "total_return_proxy",
        "win_rate_pct",
        "profit_factor",
        "sharpe_proxy",
        "trade_count",
        "observation_count",
        "timeframe_rank",
        "performance_score",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Avoid infinite values dominating candidate ranking.
    for col in ["profit_factor", "performance_score"]:
        if col in df.columns:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)

    df["symbol"] = df["symbol"].astype(str)
    df["composite_regime"] = df["composite_regime"].astype(str)
    df["strategy_name"] = df["strategy_name"].astype(str)

    return df


def infer_current_regime_family(row: pd.Series) -> str:
    environment = str(row.get("primary_strategy_environment", "")).lower()
    alignment = str(row.get("alignment_label", "")).lower()

    if "bullish_trend" in environment or "bullish" in alignment:
        return "bullish_trend"

    if "bearish_trend" in environment or "bearish" in alignment:
        return "bearish_trend"

    if "compression" in environment:
        return "quiet_or_compressed"

    if "mixed" in environment or "transition" in environment:
        return "transition_or_range"

    return "unknown"


def registry_match_score(current_row: pd.Series, candidate: pd.Series) -> float:
    score = 0.0

    current_symbol = str(current_row.get("symbol"))
    current_primary_strategy = str(current_row.get("primary_strategy", "")).lower()
    current_secondary_strategy = str(current_row.get("secondary_strategy", "")).lower()
    current_environment = str(current_row.get("primary_strategy_environment", "")).lower()

    candidate_symbol = str(candidate.get("symbol"))
    candidate_strategy = str(candidate.get("strategy_name", "")).lower()
    candidate_regime = str(candidate.get("composite_regime", "")).lower()
    candidate_scope = str(candidate.get("performance_scope", "")).lower()
    evidence_quality = str(candidate.get("evidence_quality", "")).lower()

    if candidate_symbol == current_symbol:
        score += 35
    elif candidate_symbol == "GLOBAL":
        score += 15
    else:
        score -= 10

    if current_primary_strategy and current_primary_strategy in candidate_strategy:
        score += 30

    if current_secondary_strategy and current_secondary_strategy in candidate_strategy:
        score += 15

    if "trend" in current_environment and "trend" in candidate_strategy:
        score += 15

    if "bullish" in current_environment and "long" in candidate_strategy:
        score += 15

    if "bearish" in current_environment and "short" in candidate_strategy:
        score += 15

    if "transition" in current_environment and "transition" in candidate_regime:
        score += 10

    if "range" in current_environment and "range" in candidate_regime:
        score += 10

    if candidate_scope == "symbol_regime":
        score += 10
    elif candidate_scope == "global_regime":
        score += 5
    elif candidate_scope == "router_validation":
        score += 8
    elif candidate_scope == "best_by_regime":
        score += 6

    if evidence_quality == "higher":
        score += 20
    elif evidence_quality == "medium":
        score += 10
    elif evidence_quality == "low":
        score -= 10

    return score


def build_candidates(current: pd.DataFrame, registry: pd.DataFrame) -> pd.DataFrame:
    registry = clean_registry(registry)

    rows = []

    # We only need plausible scopes for current routing evidence.
    usable_registry = registry[
        registry["performance_scope"].isin(
            ["symbol_regime", "global_regime", "best_by_regime", "router_validation"]
        )
    ].copy()

    for _, current_row in current.iterrows():
        symbol = str(current_row.get("symbol"))

        # Use same-symbol and GLOBAL evidence first.
        pool = usable_registry[
            (usable_registry["symbol"] == symbol)
            | (usable_registry["symbol"] == "GLOBAL")
        ].copy()

        if pool.empty:
            pool = usable_registry.copy()

        for _, candidate in pool.iterrows():
            record = {
                "symbol": symbol,
                "current_alignment_label": current_row.get("alignment_label"),
                "current_strategy_environment": current_row.get("primary_strategy_environment"),
                "current_primary_strategy": current_row.get("primary_strategy"),
                "current_secondary_strategy": current_row.get("secondary_strategy"),
                "current_execution_posture": current_row.get("execution_posture"),
                "current_research_priority": current_row.get("research_priority"),
                "current_directional_bias": current_row.get("directional_bias"),
                "candidate_strategy_name": candidate.get("strategy_name"),
                "candidate_symbol_scope": candidate.get("symbol"),
                "candidate_timeframe": candidate.get("timeframe"),
                "candidate_composite_regime": candidate.get("composite_regime"),
                "candidate_performance_scope": candidate.get("performance_scope"),
                "win_rate_pct": candidate.get("win_rate_pct"),
                "profit_factor": candidate.get("profit_factor"),
                "sharpe_proxy": candidate.get("sharpe_proxy"),
                "total_return_proxy": candidate.get("total_return_proxy"),
                "performance_score": candidate.get("performance_score"),
                "performance_label": candidate.get("performance_label"),
                "evidence_quality": candidate.get("evidence_quality"),
                "observation_count": candidate.get("observation_count"),
                "trade_count": candidate.get("trade_count"),
                "match_score": registry_match_score(current_row, candidate),
                "join_time_utc": datetime.now(timezone.utc).isoformat(),
            }

            rows.append(record)

    candidates = pd.DataFrame(rows)

    if candidates.empty:
        return candidates

    numeric_cols = [
        "win_rate_pct",
        "profit_factor",
        "sharpe_proxy",
        "total_return_proxy",
        "performance_score",
        "observation_count",
        "trade_count",
        "match_score",
    ]

    for col in numeric_cols:
        candidates[col] = pd.to_numeric(candidates[col], errors="coerce")

    # Combined adaptive score: compatibility first, evidence/performance second.
    candidates["adaptive_candidate_score"] = (
        candidates["match_score"].fillna(0)
        + candidates["performance_score"].fillna(0).clip(lower=-100, upper=100) * 0.25
        + (candidates["profit_factor"].fillna(1).clip(lower=0, upper=3) - 1) * 10
        + candidates["sharpe_proxy"].fillna(0).clip(lower=-3, upper=3) * 5
    )

    candidates["candidate_quality_label"] = "background"

    candidates.loc[
        (candidates["adaptive_candidate_score"] >= 75)
        & (candidates["evidence_quality"].isin(["medium", "higher"])),
        "candidate_quality_label",
    ] = "strong_candidate"

    candidates.loc[
        (candidates["adaptive_candidate_score"] >= 55)
        & (candidates["candidate_quality_label"] == "background"),
        "candidate_quality_label",
    ] = "watch_candidate"

    candidates.loc[
        candidates["current_execution_posture"].isin(["observation_or_defensive_only"]),
        "candidate_quality_label",
    ] = "context_only_defensive"

    candidates["adaptive_candidate_score"] = candidates["adaptive_candidate_score"].round(6)

    candidates = candidates.sort_values(
        ["symbol", "adaptive_candidate_score", "match_score"],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    return candidates


def build_top_candidates(candidates: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame()

    return (
        candidates.groupby("symbol", group_keys=False)
        .head(top_n)
        .reset_index(drop=True)
    )


def build_report(top_candidates: pd.DataFrame, all_candidates: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    lines = []

    lines.append("=" * 140)
    lines.append("BACQE CURRENT STRATEGY PERFORMANCE CANDIDATES")
    lines.append("=" * 140)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Current state:   {CURRENT_STATE_PATH}")
    lines.append(f"Registry:        {REGISTRY_PATH}")
    lines.append("-" * 140)

    lines.append("")
    lines.append("CANDIDATE SUMMARY")
    lines.append("-" * 140)
    lines.append(f"All candidate rows: {len(all_candidates):,}")
    lines.append(f"Top candidate rows: {len(top_candidates):,}")

    if not top_candidates.empty:
        lines.append("")
        lines.append("Quality counts:")
        lines.append(top_candidates["candidate_quality_label"].value_counts().to_string())

    lines.append("")
    lines.append("TOP CURRENT STRATEGY CANDIDATES BY SYMBOL")
    lines.append("-" * 140)

    display_cols = [
        "symbol",
        "current_strategy_environment",
        "current_execution_posture",
        "current_primary_strategy",
        "candidate_strategy_name",
        "candidate_symbol_scope",
        "candidate_timeframe",
        "candidate_composite_regime",
        "candidate_performance_scope",
        "win_rate_pct",
        "profit_factor",
        "sharpe_proxy",
        "performance_score",
        "evidence_quality",
        "match_score",
        "adaptive_candidate_score",
        "candidate_quality_label",
    ]

    if top_candidates.empty:
        lines.append("No candidates created.")
    else:
        lines.append(top_candidates[display_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 140)
    lines.append("This script joins current BACQE market state to historical strategy-performance evidence.")
    lines.append("adaptive_candidate_score combines state compatibility with capped performance evidence.")
    lines.append("context_only_defensive means the current market state is defensive even if historical candidates exist.")
    lines.append("This is still research routing, not trade execution.")
    lines.append("=" * 140)

    return "\n".join(lines)


def main() -> None:
    print("=" * 140)
    print("BACQE REGIME ENGINE - 57 JOIN STRATEGY PERFORMANCE TO CURRENT MARKET STATE")
    print("=" * 140)

    current, registry = load_inputs()

    all_candidates = build_candidates(current, registry)
    top_candidates = build_top_candidates(all_candidates, top_n=10)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    all_csv = OUTPUT_ANALYSIS_DIR / "current_strategy_performance_candidates_all_latest.csv"
    all_parquet = OUTPUT_ANALYSIS_DIR / "current_strategy_performance_candidates_all_latest.parquet"

    top_csv = OUTPUT_ANALYSIS_DIR / "current_strategy_performance_candidates_latest.csv"
    top_parquet = OUTPUT_ANALYSIS_DIR / "current_strategy_performance_candidates_latest.parquet"
    top_json = OUTPUT_ANALYSIS_DIR / "current_strategy_performance_candidates_latest.json"

    report_path = OUTPUT_REPORT_DIR / "current_strategy_performance_candidates_latest.txt"

    all_candidates.to_csv(all_csv, index=False)
    all_candidates.to_parquet(all_parquet, index=False)

    top_candidates.to_csv(top_csv, index=False)
    top_candidates.to_parquet(top_parquet, index=False)

    with open(top_json, "w", encoding="utf-8") as f:
        json.dump(top_candidates.to_dict(orient="records"), f, indent=4, default=str)

    report = build_report(top_candidates, all_candidates)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Current strategy performance candidates created.")
    print(f"All CSV:     {all_csv}")
    print(f"All Parquet: {all_parquet}")
    print(f"Top CSV:     {top_csv}")
    print(f"Top Parquet: {top_parquet}")
    print(f"Top JSON:    {top_json}")
    print(f"Report:      {report_path}")
    print("-" * 140)
    print(report)
    print("=" * 140)


if __name__ == "__main__":
    main()