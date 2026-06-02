"""
BACQE MICROSTRUCTURE 18 - VALIDATED FEATURE SUMMARY

Purpose:
    Summarise validated and partially validated microstructure relationships
    from Script 17 into a practical research shortlist.

Inputs:
    E:/Quant_Lab/data/analysis/microstructure/cross_validation_audit/
        microstructure_cross_validation_audit_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/validated_feature_summary/
        microstructure_validated_feature_summary_latest.csv
        microstructure_validated_feature_summary_latest.json
        microstructure_validated_feature_summary_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

VALIDATION_LEVELS = {
    "validated",
    "partially_validated",
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


def build_research_score(row: pd.Series) -> float:
    score = 0.0

    validated_count = row.get("validated_count", 0)
    partially_validated_count = row.get("partially_validated_count", 0)
    total_count = row.get("validated_relationship_count", 0)

    avg_test_abs_corr = row.get("avg_test_abs_correlation", 0)
    max_test_abs_corr = row.get("max_test_abs_correlation", 0)
    avg_decay = row.get("avg_correlation_decay_ratio", 0)

    symbol_count = row.get("symbol_count", 0)
    bar_type_count = row.get("bar_type_count", 0)
    target_count = row.get("target_count", 0)
    dataset_count = row.get("dataset_count", 0)
    avg_test_sample_size = row.get("avg_test_sample_size", 0)

    score += min(validated_count * 1.8, 25)
    score += min(partially_validated_count * 0.8, 12)
    score += min(total_count * 0.2, 8)

    score += min(avg_test_abs_corr * 220, 18)
    score += min(max_test_abs_corr * 90, 12)

    if avg_decay >= 1.0:
        score += 10
    elif avg_decay >= 0.75:
        score += 7
    elif avg_decay >= 0.50:
        score += 4
    elif avg_decay >= 0.25:
        score += 1

    score += min(symbol_count * 3.5, 14)
    score += min(bar_type_count * 3.0, 9)
    score += min(target_count * 3.0, 9)
    score += min(dataset_count * 0.7, 10)

    if avg_test_sample_size >= 5000:
        score += 8
    elif avg_test_sample_size >= 1000:
        score += 6
    elif avg_test_sample_size >= 250:
        score += 3
    elif avg_test_sample_size < 100:
        score -= 8

    return round(max(0, min(100, score)), 2)


def assign_research_tier(row: pd.Series) -> str:
    score = row.get("research_score", 0)
    validated_count = row.get("validated_count", 0)
    symbols = row.get("symbol_count", 0)
    targets = row.get("target_count", 0)
    family = row.get("feature_family", "")

    if score >= 85 and validated_count >= 10 and symbols >= 3 and targets >= 2:
        return "tier_1_priority"

    if score >= 75 and validated_count >= 5:
        return "tier_2_strong"

    if score >= 65:
        return "tier_3_research"

    if score >= 50:
        return "tier_4_watch"

    if family in {"imbalance", "activity"} and validated_count >= 2:
        return "tier_4_microstructure_watch"

    return "tier_5_low_priority"


def summarise_by_feature(validated_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        validated_df
        .groupby(["feature_name", "feature_family"], dropna=False)
        .agg(
            validated_relationship_count=("validation_label", "count"),
            validated_count=("validation_label", lambda s: int((s == "validated").sum())),
            partially_validated_count=("validation_label", lambda s: int((s == "partially_validated").sum())),
            avg_original_abs_correlation=("original_abs_correlation", "mean"),
            avg_train_abs_correlation=("train_abs_correlation", "mean"),
            avg_test_abs_correlation=("test_abs_correlation", "mean"),
            max_test_abs_correlation=("test_abs_correlation", "max"),
            median_test_abs_correlation=("test_abs_correlation", "median"),
            avg_correlation_decay_ratio=("correlation_decay_ratio", "mean"),
            avg_train_sample_size=("train_sample_size", "mean"),
            avg_test_sample_size=("test_sample_size", "mean"),
            max_test_sample_size=("test_sample_size", "max"),
            symbol_count=("symbol", "nunique"),
            symbols=("symbol", lambda s: ",".join(sorted(s.dropna().unique()))),
            bar_type_count=("bar_type", "nunique"),
            bar_types=("bar_type", lambda s: ",".join(sorted(s.dropna().unique()))),
            target_count=("target", "nunique"),
            targets=("target", lambda s: ",".join(sorted(s.dropna().unique()))),
            dataset_count=("file_path", "nunique"),
            best_symbol=("symbol", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
            best_bar_type=("bar_type", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
            best_target=("target", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
        )
        .reset_index()
    )

    grouped["research_score"] = grouped.apply(build_research_score, axis=1)
    grouped["research_tier"] = grouped.apply(assign_research_tier, axis=1)
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    grouped = grouped.sort_values(
        [
            "research_score",
            "validated_count",
            "avg_test_abs_correlation",
            "symbol_count",
            "target_count",
        ],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)

    grouped["research_rank"] = grouped.index + 1

    return grouped


def summarise_by_symbol_feature(validated_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        validated_df
        .groupby(["symbol", "feature_name", "feature_family"], dropna=False)
        .agg(
            validated_relationship_count=("validation_label", "count"),
            validated_count=("validation_label", lambda s: int((s == "validated").sum())),
            partially_validated_count=("validation_label", lambda s: int((s == "partially_validated").sum())),
            avg_original_abs_correlation=("original_abs_correlation", "mean"),
            avg_train_abs_correlation=("train_abs_correlation", "mean"),
            avg_test_abs_correlation=("test_abs_correlation", "mean"),
            max_test_abs_correlation=("test_abs_correlation", "max"),
            median_test_abs_correlation=("test_abs_correlation", "median"),
            avg_correlation_decay_ratio=("correlation_decay_ratio", "mean"),
            avg_train_sample_size=("train_sample_size", "mean"),
            avg_test_sample_size=("test_sample_size", "mean"),
            max_test_sample_size=("test_sample_size", "max"),
            bar_type_count=("bar_type", "nunique"),
            bar_types=("bar_type", lambda s: ",".join(sorted(s.dropna().unique()))),
            target_count=("target", "nunique"),
            targets=("target", lambda s: ",".join(sorted(s.dropna().unique()))),
            dataset_count=("file_path", "nunique"),
            best_bar_type=("bar_type", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
            best_target=("target", lambda s: s.value_counts().idxmax() if not s.dropna().empty else None),
        )
        .reset_index()
    )

    grouped["symbol_count"] = 1
    grouped["research_score"] = grouped.apply(build_research_score, axis=1)
    grouped["research_tier"] = grouped.apply(assign_research_tier, axis=1)
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    grouped = grouped.sort_values(
        [
            "research_score",
            "validated_count",
            "avg_test_abs_correlation",
            "target_count",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    grouped["symbol_research_rank"] = grouped.index + 1

    return grouped


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 18 - VALIDATED FEATURE SUMMARY")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    cv_path = (
        analysis_dir
        / "cross_validation_audit"
        / "microstructure_cross_validation_audit_latest.csv"
    )

    report_dir = analysis_dir / "validated_feature_summary"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Cross-validation audit: {cv_path}")
    print(f"Report dir:             {report_dir}")
    print("-" * 90)

    if not cv_path.exists():
        raise FileNotFoundError(
            f"Missing cross-validation audit file: {cv_path}. "
            "Run script 17 first."
        )

    cv_df = pd.read_csv(cv_path)

    validated_df = cv_df[
        cv_df["validation_label"].isin(VALIDATION_LEVELS)
    ].copy()

    print(f"CV rows:                     {len(cv_df):,}")
    print(f"Validated/partial rows:      {len(validated_df):,}")
    print("-" * 90)

    if validated_df.empty:
        raise RuntimeError("No validated or partially validated rows found.")

    feature_summary = summarise_by_feature(validated_df)
    symbol_feature_summary = summarise_by_symbol_feature(validated_df)

    csv_feature_path = report_dir / "microstructure_validated_feature_summary_latest.csv"
    json_feature_path = report_dir / "microstructure_validated_feature_summary_latest.json"

    csv_symbol_path = report_dir / "microstructure_validated_symbol_feature_summary_latest.csv"
    json_symbol_path = report_dir / "microstructure_validated_symbol_feature_summary_latest.json"

    txt_path = report_dir / "microstructure_validated_feature_summary_latest.txt"

    feature_summary.to_csv(csv_feature_path, index=False)
    symbol_feature_summary.to_csv(csv_symbol_path, index=False)

    with open(json_feature_path, "w", encoding="utf-8") as f:
        json.dump(feature_summary.to_dict(orient="records"), f, indent=2, default=str)

    with open(json_symbol_path, "w", encoding="utf-8") as f:
        json.dump(symbol_feature_summary.to_dict(orient="records"), f, indent=2, default=str)

    tier_counts = feature_summary["research_tier"].value_counts(dropna=False).to_dict()
    family_counts = feature_summary["feature_family"].value_counts(dropna=False).to_dict()

    validation_counts = validated_df["validation_label"].value_counts(dropna=False).to_dict()

    top_features = feature_summary.head(30)[
        [
            "research_rank",
            "feature_name",
            "feature_family",
            "validated_relationship_count",
            "validated_count",
            "partially_validated_count",
            "avg_test_abs_correlation",
            "max_test_abs_correlation",
            "avg_correlation_decay_ratio",
            "symbol_count",
            "bar_type_count",
            "target_count",
            "dataset_count",
            "best_symbol",
            "best_bar_type",
            "best_target",
            "research_score",
            "research_tier",
        ]
    ]

    top_symbol_features = symbol_feature_summary.head(30)[
        [
            "symbol_research_rank",
            "symbol",
            "feature_name",
            "feature_family",
            "validated_relationship_count",
            "validated_count",
            "partially_validated_count",
            "avg_test_abs_correlation",
            "max_test_abs_correlation",
            "avg_correlation_decay_ratio",
            "bar_type_count",
            "target_count",
            "dataset_count",
            "best_bar_type",
            "best_target",
            "research_score",
            "research_tier",
        ]
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE VALIDATED FEATURE SUMMARY")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"CV rows:                {len(cv_df):,}")
    lines.append(f"Validated/partial rows: {len(validated_df):,}")
    lines.append(f"Feature summary rows:   {len(feature_summary):,}")
    lines.append(f"Symbol feature rows:    {len(symbol_feature_summary):,}")
    lines.append("")
    lines.append(f"Validation counts: {validation_counts}")
    lines.append(f"Research tier counts: {tier_counts}")
    lines.append(f"Feature family counts: {family_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 30 VALIDATED FEATURES")
    lines.append("-" * 90)
    lines.append(top_features.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 30 VALIDATED SYMBOL/FEATURE COMBINATIONS")
    lines.append("-" * 90)
    lines.append(top_symbol_features.to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Validated/partial rows: {len(validated_df):,}")
    print(f"Feature summary rows:   {len(feature_summary):,}")
    print(f"Symbol feature rows:    {len(symbol_feature_summary):,}")
    print(f"Validation counts:      {validation_counts}")
    print(f"Research tier counts:   {tier_counts}")
    print(f"Feature family counts:  {family_counts}")
    print("-" * 90)
    print("[TOP 15 VALIDATED FEATURES]")
    print(top_features.head(15).to_string(index=False))
    print("-" * 90)
    print("[DONE] Validated feature summary complete.")
    print(f"Feature CSV:        {csv_feature_path}")
    print(f"Feature JSON:       {json_feature_path}")
    print(f"Symbol CSV:         {csv_symbol_path}")
    print(f"Symbol JSON:        {json_symbol_path}")
    print(f"TXT output:         {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()