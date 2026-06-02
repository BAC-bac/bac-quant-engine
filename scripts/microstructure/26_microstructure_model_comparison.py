"""
BACQE MICROSTRUCTURE 26 - MODEL COMPARISON

Purpose:
    Compare baseline models from Script 23 with advanced models from Script 25.

Inputs:
    baseline_models/microstructure_baseline_models_latest.csv
    advanced_models/microstructure_advanced_models_latest.csv

Outputs:
    model_comparison/
        microstructure_model_comparison_latest.csv
        microstructure_model_comparison_summary_latest.csv
        microstructure_model_comparison_latest.json
        microstructure_model_comparison_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"


BASELINE_MODELS = {
    "logistic_regression",
    "random_forest",
}

ADVANCED_MODELS = {
    "extra_trees",
    "gradient_boosting",
    "hist_gradient_boosting",
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


def model_family(model_name: str) -> str:
    if model_name in BASELINE_MODELS:
        return "baseline"
    if model_name in ADVANCED_MODELS:
        return "advanced"
    return "unknown"


def normalise_model_results(df: pd.DataFrame, source: str) -> pd.DataFrame:
    df = df.copy()

    df["model_source"] = source
    df["model_family"] = df["model_name"].apply(model_family)

    for col in [
        "accuracy",
        "balanced_accuracy",
        "precision",
        "recall",
        "roc_auc",
        "train_rows",
        "test_rows",
        "train_positive_rate",
        "test_positive_rate",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def score_model(row: pd.Series) -> float:
    score = 0.0

    status = row.get("status")
    label = row.get("model_label")
    balanced_accuracy = row.get("balanced_accuracy")
    roc_auc = row.get("roc_auc")
    precision = row.get("precision")
    recall = row.get("recall")
    test_rows = row.get("test_rows", 0)

    if status != "ok":
        return 0.0

    if label in {"strong_baseline", "strong_advanced"}:
        score += 35
    elif label in {"research_baseline", "research_advanced"}:
        score += 25
    elif label in {"weak_baseline", "weak_advanced"}:
        score += 10

    if pd.notna(balanced_accuracy):
        score += max(0, (balanced_accuracy - 0.50) * 250)

    if pd.notna(roc_auc):
        score += max(0, (roc_auc - 0.50) * 220)

    if pd.notna(precision):
        score += max(0, (precision - 0.50) * 60)

    if pd.notna(recall):
        score += max(0, (recall - 0.50) * 30)

    if test_rows >= 5000:
        score += 8
    elif test_rows >= 1000:
        score += 5
    elif test_rows >= 250:
        score += 3
    elif test_rows < 100:
        score -= 8

    return round(max(0, min(100, score)), 2)


def assign_comparison_tier(row: pd.Series) -> str:
    score = row.get("model_score", 0)
    label = row.get("model_label", "")
    family = row.get("model_family", "")

    if score >= 85:
        return "tier_1_model_candidate"

    if score >= 75:
        return "tier_2_strong_model_candidate"

    if score >= 65 and family == "advanced":
        return "tier_3_advanced_research"

    if score >= 65:
        return "tier_3_baseline_research"

    if score >= 50:
        return "tier_4_watch"

    return "tier_5_low_priority"


def build_best_by_area(combined_df: pd.DataFrame) -> pd.DataFrame:
    ok_df = combined_df[combined_df["status"] == "ok"].copy()

    if ok_df.empty:
        return pd.DataFrame()

    group_cols = ["symbol", "bar_type", "parameter", "target"]

    ranked = ok_df.sort_values(
        ["model_score", "roc_auc", "balanced_accuracy", "test_rows"],
        ascending=[False, False, False, False],
        na_position="last",
    ).copy()

    best_df = ranked.groupby(group_cols, dropna=False).head(1).reset_index(drop=True)

    family_summary = (
        ok_df
        .groupby(group_cols + ["model_family"], dropna=False)
        .agg(
            family_model_count=("model_name", "count"),
            family_avg_score=("model_score", "mean"),
            family_max_score=("model_score", "max"),
            family_avg_balanced_accuracy=("balanced_accuracy", "mean"),
            family_max_balanced_accuracy=("balanced_accuracy", "max"),
            family_avg_roc_auc=("roc_auc", "mean"),
            family_max_roc_auc=("roc_auc", "max"),
        )
        .reset_index()
    )

    baseline_summary = family_summary[family_summary["model_family"] == "baseline"].copy()
    advanced_summary = family_summary[family_summary["model_family"] == "advanced"].copy()

    baseline_summary = baseline_summary.rename(
        columns={
            "family_avg_score": "baseline_avg_score",
            "family_max_score": "baseline_max_score",
            "family_avg_balanced_accuracy": "baseline_avg_balanced_accuracy",
            "family_max_balanced_accuracy": "baseline_max_balanced_accuracy",
            "family_avg_roc_auc": "baseline_avg_roc_auc",
            "family_max_roc_auc": "baseline_max_roc_auc",
        }
    )

    advanced_summary = advanced_summary.rename(
        columns={
            "family_avg_score": "advanced_avg_score",
            "family_max_score": "advanced_max_score",
            "family_avg_balanced_accuracy": "advanced_avg_balanced_accuracy",
            "family_max_balanced_accuracy": "advanced_max_balanced_accuracy",
            "family_avg_roc_auc": "advanced_avg_roc_auc",
            "family_max_roc_auc": "advanced_max_roc_auc",
        }
    )

    keep_baseline = group_cols + [
        "baseline_avg_score",
        "baseline_max_score",
        "baseline_avg_balanced_accuracy",
        "baseline_max_balanced_accuracy",
        "baseline_avg_roc_auc",
        "baseline_max_roc_auc",
    ]

    keep_advanced = group_cols + [
        "advanced_avg_score",
        "advanced_max_score",
        "advanced_avg_balanced_accuracy",
        "advanced_max_balanced_accuracy",
        "advanced_avg_roc_auc",
        "advanced_max_roc_auc",
    ]

    comparison = best_df.merge(
        baseline_summary[keep_baseline],
        on=group_cols,
        how="left",
    ).merge(
        advanced_summary[keep_advanced],
        on=group_cols,
        how="left",
    )

    comparison["advanced_score_lift"] = (
        comparison["advanced_max_score"] - comparison["baseline_max_score"]
    )

    comparison["advanced_auc_lift"] = (
        comparison["advanced_max_roc_auc"] - comparison["baseline_max_roc_auc"]
    )

    comparison["advanced_bal_acc_lift"] = (
        comparison["advanced_max_balanced_accuracy"] - comparison["baseline_max_balanced_accuracy"]
    )

    def winner(row: pd.Series) -> str:
        if pd.isna(row.get("baseline_max_score")) and pd.isna(row.get("advanced_max_score")):
            return "unknown"

        if pd.isna(row.get("baseline_max_score")):
            return "advanced"

        if pd.isna(row.get("advanced_max_score")):
            return "baseline"

        if row["advanced_score_lift"] > 2:
            return "advanced"

        if row["advanced_score_lift"] < -2:
            return "baseline"

        return "mixed"

    comparison["family_winner"] = comparison.apply(winner, axis=1)

    comparison = comparison.sort_values(
        ["model_score", "advanced_score_lift", "roc_auc", "balanced_accuracy"],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    comparison["area_rank"] = comparison.index + 1

    return comparison


def summarise_group(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    grouped = (
        df
        .groupby(group_cols, dropna=False)
        .agg(
            model_count=("model_name", "count"),
            ok_count=("status", lambda s: int((s == "ok").sum())),
            baseline_count=("model_family", lambda s: int((s == "baseline").sum())),
            advanced_count=("model_family", lambda s: int((s == "advanced").sum())),
            strong_count=("model_label", lambda s: int(s.astype(str).str.contains("strong").sum())),
            research_count=("model_label", lambda s: int(s.astype(str).str.contains("research").sum())),
            weak_count=("model_label", lambda s: int(s.astype(str).str.contains("weak").sum())),
            avg_score=("model_score", "mean"),
            max_score=("model_score", "max"),
            avg_balanced_accuracy=("balanced_accuracy", "mean"),
            max_balanced_accuracy=("balanced_accuracy", "max"),
            avg_roc_auc=("roc_auc", "mean"),
            max_roc_auc=("roc_auc", "max"),
            avg_test_rows=("test_rows", "mean"),
            max_test_rows=("test_rows", "max"),
        )
        .reset_index()
    )

    grouped = grouped.sort_values(
        ["max_score", "avg_score", "strong_count", "research_count"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    grouped["summary_rank"] = grouped.index + 1

    return grouped


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 26 - MODEL COMPARISON")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    baseline_path = (
        analysis_dir
        / "baseline_models"
        / "microstructure_baseline_models_latest.csv"
    )

    advanced_path = (
        analysis_dir
        / "advanced_models"
        / "microstructure_advanced_models_latest.csv"
    )

    report_dir = analysis_dir / "model_comparison"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Baseline models: {baseline_path}")
    print(f"Advanced models: {advanced_path}")
    print(f"Report dir:      {report_dir}")
    print("-" * 90)

    if not baseline_path.exists():
        raise FileNotFoundError(f"Missing baseline model file: {baseline_path}")

    if not advanced_path.exists():
        raise FileNotFoundError(f"Missing advanced model file: {advanced_path}")

    baseline_df = normalise_model_results(pd.read_csv(baseline_path), "script_23_baseline")
    advanced_df = normalise_model_results(pd.read_csv(advanced_path), "script_25_advanced")

    combined_df = pd.concat([baseline_df, advanced_df], ignore_index=True, sort=False)

    combined_df["model_score"] = combined_df.apply(score_model, axis=1)
    combined_df["comparison_tier"] = combined_df.apply(assign_comparison_tier, axis=1)
    combined_df["comparison_created_at_utc"] = datetime.now(timezone.utc).isoformat()

    combined_df = combined_df.sort_values(
        ["model_score", "roc_auc", "balanced_accuracy", "test_rows"],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    combined_df["overall_model_rank"] = combined_df.index + 1

    area_comparison = build_best_by_area(combined_df)

    symbol_summary = summarise_group(combined_df, ["symbol"])
    bar_type_summary = summarise_group(combined_df, ["bar_type"])
    target_summary = summarise_group(combined_df, ["target"])
    model_summary = summarise_group(combined_df, ["model_name", "model_family"])
    family_summary = summarise_group(combined_df, ["model_family"])
    symbol_bar_type_summary = summarise_group(combined_df, ["symbol", "bar_type"])

    combined_csv = report_dir / "microstructure_model_comparison_latest.csv"
    area_csv = report_dir / "microstructure_model_comparison_by_area_latest.csv"
    symbol_csv = report_dir / "microstructure_model_comparison_by_symbol_latest.csv"
    bar_type_csv = report_dir / "microstructure_model_comparison_by_bar_type_latest.csv"
    target_csv = report_dir / "microstructure_model_comparison_by_target_latest.csv"
    model_csv = report_dir / "microstructure_model_comparison_by_model_latest.csv"
    family_csv = report_dir / "microstructure_model_comparison_by_family_latest.csv"
    symbol_bar_type_csv = report_dir / "microstructure_model_comparison_by_symbol_bar_type_latest.csv"
    json_path = report_dir / "microstructure_model_comparison_latest.json"
    txt_path = report_dir / "microstructure_model_comparison_latest.txt"

    combined_df.to_csv(combined_csv, index=False)
    area_comparison.to_csv(area_csv, index=False)
    symbol_summary.to_csv(symbol_csv, index=False)
    bar_type_summary.to_csv(bar_type_csv, index=False)
    target_summary.to_csv(target_csv, index=False)
    model_summary.to_csv(model_csv, index=False)
    family_summary.to_csv(family_csv, index=False)
    symbol_bar_type_summary.to_csv(symbol_bar_type_csv, index=False)

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_rows": len(baseline_df),
        "advanced_rows": len(advanced_df),
        "combined_rows": len(combined_df),
        "top_models": combined_df.head(50).to_dict(orient="records"),
        "area_comparison": area_comparison.head(50).to_dict(orient="records"),
        "symbol_summary": symbol_summary.to_dict(orient="records"),
        "bar_type_summary": bar_type_summary.to_dict(orient="records"),
        "target_summary": target_summary.to_dict(orient="records"),
        "family_summary": family_summary.to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    label_counts = combined_df["model_label"].value_counts(dropna=False).to_dict()
    tier_counts = combined_df["comparison_tier"].value_counts(dropna=False).to_dict()
    family_counts = combined_df["model_family"].value_counts(dropna=False).to_dict()

    if not area_comparison.empty:
        winner_counts = area_comparison["family_winner"].value_counts(dropna=False).to_dict()
    else:
        winner_counts = {}

    top_models = combined_df.head(40)[
        [
            "overall_model_rank",
            "symbol",
            "bar_type",
            "parameter",
            "model_name",
            "model_family",
            "target",
            "test_rows",
            "accuracy",
            "balanced_accuracy",
            "precision",
            "recall",
            "roc_auc",
            "model_label",
            "model_score",
            "comparison_tier",
        ]
    ]

    top_areas = area_comparison.head(30) if not area_comparison.empty else pd.DataFrame()

    area_cols = [
        "area_rank",
        "symbol",
        "bar_type",
        "parameter",
        "target",
        "model_name",
        "model_family",
        "model_score",
        "roc_auc",
        "balanced_accuracy",
        "model_label",
        "baseline_max_score",
        "advanced_max_score",
        "advanced_score_lift",
        "advanced_auc_lift",
        "advanced_bal_acc_lift",
        "family_winner",
    ]

    available_area_cols = [c for c in area_cols if not top_areas.empty and c in top_areas.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE MODEL COMPARISON")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Baseline rows: {len(baseline_df):,}")
    lines.append(f"Advanced rows: {len(advanced_df):,}")
    lines.append(f"Combined rows: {len(combined_df):,}")
    lines.append("")
    lines.append(f"Model family counts: {family_counts}")
    lines.append(f"Model label counts:  {label_counts}")
    lines.append(f"Comparison tiers:    {tier_counts}")
    lines.append(f"Area winner counts:  {winner_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 40 MODEL RESULTS")
    lines.append("-" * 90)
    lines.append(top_models.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 30 AREA COMPARISON")
    lines.append("-" * 90)

    if top_areas.empty:
        lines.append("No area comparison available.")
    else:
        lines.append(top_areas[available_area_cols].to_string(index=False))

    lines.append("")
    lines.append("-" * 90)
    lines.append("SYMBOL SUMMARY")
    lines.append("-" * 90)
    lines.append(symbol_summary.head(10).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("BAR TYPE SUMMARY")
    lines.append("-" * 90)
    lines.append(bar_type_summary.head(10).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TARGET SUMMARY")
    lines.append("-" * 90)
    lines.append(target_summary.head(10).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("MODEL FAMILY SUMMARY")
    lines.append("-" * 90)
    lines.append(family_summary.to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Baseline rows:        {len(baseline_df):,}")
    print(f"Advanced rows:        {len(advanced_df):,}")
    print(f"Combined rows:        {len(combined_df):,}")
    print(f"Family counts:        {family_counts}")
    print(f"Label counts:         {label_counts}")
    print(f"Comparison tiers:     {tier_counts}")
    print(f"Area winner counts:   {winner_counts}")
    print("-" * 90)
    print("[TOP SYMBOLS]")
    print(symbol_summary.head(10).to_string(index=False))
    print("-" * 90)
    print("[MODEL FAMILY SUMMARY]")
    print(family_summary.to_string(index=False))
    print("-" * 90)
    print("[DONE] Model comparison complete.")
    print(f"Combined CSV:         {combined_csv}")
    print(f"Area CSV:             {area_csv}")
    print(f"Symbol CSV:           {symbol_csv}")
    print(f"Bar Type CSV:         {bar_type_csv}")
    print(f"Target CSV:           {target_csv}")
    print(f"Model CSV:            {model_csv}")
    print(f"Family CSV:           {family_csv}")
    print(f"Symbol/Bar Type CSV:  {symbol_bar_type_csv}")
    print(f"JSON output:          {json_path}")
    print(f"TXT output:           {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()