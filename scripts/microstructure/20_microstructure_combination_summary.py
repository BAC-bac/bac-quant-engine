"""
BACQE MICROSTRUCTURE 20 - COMBINATION SUMMARY

Purpose:
    Summarise feature combinations from Script 19 into a clean research shortlist.

Inputs:
    E:/Quant_Lab/data/analysis/microstructure/feature_combination_audit/
        microstructure_feature_combination_audit_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/combination_summary/
        microstructure_combination_summary_latest.csv
        microstructure_combination_summary_latest.json
        microstructure_combination_summary_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

IMPROVEMENT_LEVELS = {
    "strong_improvement",
    "research_improvement",
    "minor_improvement",
}


PRIORITY_LEVELS = {
    "strong_improvement",
    "research_improvement",
}


def print_header(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_analysis_dir(micro_cfg: dict) -> Path:
    return Path(
        micro_cfg["output"].get(
            "analysis_dir",
            "E:/Quant_Lab/data/analysis/microstructure",
        )
    )


def build_combo_score(row: pd.Series) -> float:
    score = 0.0

    improvement_count = row.get("improvement_count", 0)
    strong_count = row.get("strong_improvement_count", 0)
    research_count = row.get("research_improvement_count", 0)

    avg_improvement = row.get("avg_correlation_improvement", 0)
    max_improvement = row.get("max_correlation_improvement", 0)
    avg_combo_abs_corr = row.get("avg_combination_abs_corr", 0)
    max_combo_abs_corr = row.get("max_combination_abs_corr", 0)

    symbol_count = row.get("symbol_count", 0)
    bar_type_count = row.get("bar_type_count", 0)
    target_count = row.get("target_count", 0)
    avg_sample_size = row.get("avg_combination_sample_size", 0)

    score += min(strong_count * 10, 30)
    score += min(research_count * 4, 20)
    score += min(improvement_count * 0.5, 10)

    score += min(avg_improvement * 500, 15)
    score += min(max_improvement * 300, 15)

    score += min(avg_combo_abs_corr * 150, 10)
    score += min(max_combo_abs_corr * 80, 8)

    score += min(symbol_count * 3, 9)
    score += min(bar_type_count * 3, 9)
    score += min(target_count * 3, 9)

    if avg_sample_size >= 5000:
        score += 8
    elif avg_sample_size >= 1000:
        score += 5
    elif avg_sample_size >= 250:
        score += 3
    elif avg_sample_size < 100:
        score -= 8

    return round(max(0, min(100, score)), 2)


def assign_combo_tier(row: pd.Series) -> str:
    score = row.get("combo_score", 0)
    strong_count = row.get("strong_improvement_count", 0)
    research_count = row.get("research_improvement_count", 0)
    symbols = row.get("symbol_count", 0)

    if score >= 85 and strong_count >= 2:
        return "tier_1_priority_combo"

    if score >= 75 and strong_count >= 1:
        return "tier_2_strong_combo"

    if score >= 65 and research_count >= 3:
        return "tier_3_research_combo"

    if score >= 50:
        return "tier_4_watch_combo"

    if symbols >= 2 and research_count >= 1:
        return "tier_4_cross_symbol_watch"

    return "tier_5_low_priority"


def summarise_pair_level(improvement_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        improvement_df
        .groupby(
            [
                "feature_a",
                "feature_a_family",
                "feature_b",
                "feature_b_family",
                "combination_method",
            ],
            dropna=False,
        )
        .agg(
            improvement_count=("combination_label", "count"),
            strong_improvement_count=("combination_label", lambda s: int((s == "strong_improvement").sum())),
            research_improvement_count=("combination_label", lambda s: int((s == "research_improvement").sum())),
            minor_improvement_count=("combination_label", lambda s: int((s == "minor_improvement").sum())),
            avg_correlation_improvement=("correlation_improvement", "mean"),
            max_correlation_improvement=("correlation_improvement", "max"),
            avg_combination_abs_corr=("combination_abs_corr", "mean"),
            max_combination_abs_corr=("combination_abs_corr", "max"),
            avg_best_individual_abs_corr=("best_individual_abs_corr", "mean"),
            avg_combination_sample_size=("combination_sample_size", "mean"),
            max_combination_sample_size=("combination_sample_size", "max"),
            symbol_count=("symbol", "nunique"),
            symbols=("symbol", lambda s: ",".join(sorted(s.dropna().unique()))),
            bar_type_count=("bar_type", "nunique"),
            bar_types=("bar_type", lambda s: ",".join(sorted(s.dropna().unique()))),
            target_count=("target", "nunique"),
            targets=("target", lambda s: ",".join(sorted(s.dropna().unique()))),
            best_symbol=("symbol", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
            best_bar_type=("bar_type", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
            best_target=("target", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
        )
        .reset_index()
    )

    grouped["combo_score"] = grouped.apply(build_combo_score, axis=1)
    grouped["combo_tier"] = grouped.apply(assign_combo_tier, axis=1)
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    grouped = grouped.sort_values(
        [
            "combo_score",
            "strong_improvement_count",
            "research_improvement_count",
            "max_correlation_improvement",
            "avg_correlation_improvement",
        ],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)

    grouped["combo_rank"] = grouped.index + 1

    return grouped


def summarise_symbol_level(improvement_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        improvement_df
        .groupby(["symbol"], dropna=False)
        .agg(
            improvement_count=("combination_label", "count"),
            strong_improvement_count=("combination_label", lambda s: int((s == "strong_improvement").sum())),
            research_improvement_count=("combination_label", lambda s: int((s == "research_improvement").sum())),
            minor_improvement_count=("combination_label", lambda s: int((s == "minor_improvement").sum())),
            avg_correlation_improvement=("correlation_improvement", "mean"),
            max_correlation_improvement=("correlation_improvement", "max"),
            avg_combination_abs_corr=("combination_abs_corr", "mean"),
            max_combination_abs_corr=("combination_abs_corr", "max"),
            avg_combination_sample_size=("combination_sample_size", "mean"),
            bar_type_count=("bar_type", "nunique"),
            target_count=("target", "nunique"),
        )
        .reset_index()
    )

    grouped["symbol_count"] = 1
    grouped["combo_score"] = grouped.apply(build_combo_score, axis=1)
    grouped["combo_tier"] = grouped.apply(assign_combo_tier, axis=1)

    grouped = grouped.sort_values(
        ["combo_score", "strong_improvement_count", "research_improvement_count"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    grouped["symbol_combo_rank"] = grouped.index + 1

    return grouped


def summarise_bar_type_level(improvement_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        improvement_df
        .groupby(["bar_type"], dropna=False)
        .agg(
            improvement_count=("combination_label", "count"),
            strong_improvement_count=("combination_label", lambda s: int((s == "strong_improvement").sum())),
            research_improvement_count=("combination_label", lambda s: int((s == "research_improvement").sum())),
            minor_improvement_count=("combination_label", lambda s: int((s == "minor_improvement").sum())),
            avg_correlation_improvement=("correlation_improvement", "mean"),
            max_correlation_improvement=("correlation_improvement", "max"),
            avg_combination_abs_corr=("combination_abs_corr", "mean"),
            max_combination_abs_corr=("combination_abs_corr", "max"),
            avg_combination_sample_size=("combination_sample_size", "mean"),
            symbol_count=("symbol", "nunique"),
            target_count=("target", "nunique"),
        )
        .reset_index()
    )

    grouped["bar_type_count"] = 1
    grouped["combo_score"] = grouped.apply(build_combo_score, axis=1)
    grouped["combo_tier"] = grouped.apply(assign_combo_tier, axis=1)

    grouped = grouped.sort_values(
        ["combo_score", "strong_improvement_count", "research_improvement_count"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    grouped["bar_type_combo_rank"] = grouped.index + 1

    return grouped


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 20 - COMBINATION SUMMARY")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    combo_audit_path = (
        analysis_dir
        / "feature_combination_audit"
        / "microstructure_feature_combination_audit_latest.csv"
    )

    report_dir = analysis_dir / "combination_summary"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Combination audit: {combo_audit_path}")
    print(f"Report dir:        {report_dir}")
    print("-" * 90)

    if not combo_audit_path.exists():
        raise FileNotFoundError(
            f"Missing combination audit file: {combo_audit_path}. "
            "Run script 19 first."
        )

    combo_df = pd.read_csv(combo_audit_path)

    improvement_df = combo_df[
        combo_df["combination_label"].isin(IMPROVEMENT_LEVELS)
    ].copy()

    priority_df = combo_df[
        combo_df["combination_label"].isin(PRIORITY_LEVELS)
    ].copy()

    print(f"Combination rows:       {len(combo_df):,}")
    print(f"Improvement rows:       {len(improvement_df):,}")
    print(f"Priority improve rows:  {len(priority_df):,}")
    print("-" * 90)

    if improvement_df.empty:
        raise RuntimeError("No improvement combinations found.")

    pair_summary = summarise_pair_level(improvement_df)
    symbol_summary = summarise_symbol_level(improvement_df)
    bar_type_summary = summarise_bar_type_level(improvement_df)

    csv_pair_path = report_dir / "microstructure_combination_pair_summary_latest.csv"
    json_pair_path = report_dir / "microstructure_combination_pair_summary_latest.json"

    csv_symbol_path = report_dir / "microstructure_combination_symbol_summary_latest.csv"
    csv_bar_type_path = report_dir / "microstructure_combination_bar_type_summary_latest.csv"

    csv_priority_path = report_dir / "microstructure_priority_combinations_latest.csv"

    txt_path = report_dir / "microstructure_combination_summary_latest.txt"

    pair_summary.to_csv(csv_pair_path, index=False)
    symbol_summary.to_csv(csv_symbol_path, index=False)
    bar_type_summary.to_csv(csv_bar_type_path, index=False)
    priority_df.to_csv(csv_priority_path, index=False)

    with open(json_pair_path, "w", encoding="utf-8") as f:
        json.dump(pair_summary.to_dict(orient="records"), f, indent=2, default=str)

    label_counts = combo_df["combination_label"].value_counts(dropna=False).to_dict()
    improvement_label_counts = improvement_df["combination_label"].value_counts(dropna=False).to_dict()
    method_counts = improvement_df["combination_method"].value_counts(dropna=False).to_dict()
    tier_counts = pair_summary["combo_tier"].value_counts(dropna=False).to_dict()

    top_pairs = pair_summary.head(30)[
        [
            "combo_rank",
            "feature_a",
            "feature_a_family",
            "feature_b",
            "feature_b_family",
            "combination_method",
            "improvement_count",
            "strong_improvement_count",
            "research_improvement_count",
            "avg_correlation_improvement",
            "max_correlation_improvement",
            "avg_combination_abs_corr",
            "max_combination_abs_corr",
            "symbol_count",
            "bar_type_count",
            "target_count",
            "best_symbol",
            "best_bar_type",
            "best_target",
            "combo_score",
            "combo_tier",
        ]
    ]

    top_symbols = symbol_summary.head(10)
    top_bar_types = bar_type_summary.head(10)

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE COMBINATION SUMMARY")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Combination rows:      {len(combo_df):,}")
    lines.append(f"Improvement rows:      {len(improvement_df):,}")
    lines.append(f"Priority improve rows: {len(priority_df):,}")
    lines.append(f"Pair summary rows:     {len(pair_summary):,}")
    lines.append("")
    lines.append(f"All combination labels: {label_counts}")
    lines.append(f"Improvement labels:    {improvement_label_counts}")
    lines.append(f"Improvement methods:   {method_counts}")
    lines.append(f"Combo tier counts:     {tier_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 30 COMBINATION PAIRS")
    lines.append("-" * 90)
    lines.append(top_pairs.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("SYMBOL SUMMARY")
    lines.append("-" * 90)
    lines.append(top_symbols.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("BAR TYPE SUMMARY")
    lines.append("-" * 90)
    lines.append(top_bar_types.to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Combination rows:      {len(combo_df):,}")
    print(f"Improvement rows:      {len(improvement_df):,}")
    print(f"Priority improve rows: {len(priority_df):,}")
    print(f"Pair summary rows:     {len(pair_summary):,}")
    print(f"Label counts:          {label_counts}")
    print(f"Improvement labels:    {improvement_label_counts}")
    print(f"Method counts:         {method_counts}")
    print(f"Tier counts:           {tier_counts}")
    print("-" * 90)
    print("[TOP 15 COMBINATION PAIRS]")
    print(top_pairs.head(15).to_string(index=False))
    print("-" * 90)
    print("[DONE] Combination summary complete.")
    print(f"Pair CSV:       {csv_pair_path}")
    print(f"Pair JSON:      {json_pair_path}")
    print(f"Symbol CSV:     {csv_symbol_path}")
    print(f"Bar Type CSV:   {csv_bar_type_path}")
    print(f"Priority CSV:   {csv_priority_path}")
    print(f"TXT output:     {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()