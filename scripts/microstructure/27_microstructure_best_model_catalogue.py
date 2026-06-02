"""
BACQE MICROSTRUCTURE 27 - BEST MODEL CATALOGUE

Purpose:
    Consolidate baseline, advanced, and comparison model outputs into one
    practical research catalogue.

Inputs:
    model_comparison/
        microstructure_model_comparison_latest.csv
        microstructure_model_comparison_by_area_latest.csv
        microstructure_model_comparison_by_symbol_latest.csv
        microstructure_model_comparison_by_bar_type_latest.csv
        microstructure_model_comparison_by_target_latest.csv
        microstructure_model_comparison_by_model_latest.csv
        microstructure_model_comparison_by_family_latest.csv

Outputs:
    best_model_catalogue/
        microstructure_best_model_catalogue_latest.csv
        microstructure_best_model_catalogue_latest.json
        microstructure_best_model_catalogue_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

PRIORITY_TIERS = {
    "tier_1_model_candidate",
    "tier_2_strong_model_candidate",
    "tier_3_advanced_research",
    "tier_3_baseline_research",
}

PRIORITY_LABELS = {
    "strong_baseline",
    "research_baseline",
    "strong_advanced",
    "research_advanced",
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


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")

    return pd.read_csv(path)


def build_catalogue_score(row: pd.Series) -> float:
    score = 0.0

    model_score = row.get("model_score", 0)
    roc_auc = row.get("roc_auc", 0)
    balanced_accuracy = row.get("balanced_accuracy", 0)
    precision = row.get("precision", 0)
    recall = row.get("recall", 0)
    test_rows = row.get("test_rows", 0)
    model_label = row.get("model_label", "")
    model_family = row.get("model_family", "")

    score += min(float(model_score or 0) * 0.55, 55)

    if pd.notna(roc_auc):
        score += max(0, (roc_auc - 0.50) * 120)

    if pd.notna(balanced_accuracy):
        score += max(0, (balanced_accuracy - 0.50) * 140)

    if pd.notna(precision):
        score += max(0, (precision - 0.50) * 30)

    if pd.notna(recall):
        score += max(0, (recall - 0.50) * 20)

    if model_label in {"strong_baseline", "strong_advanced"}:
        score += 15
    elif model_label in {"research_baseline", "research_advanced"}:
        score += 8
    elif model_label in {"weak_baseline", "weak_advanced"}:
        score += 2

    if model_family == "baseline":
        score += 2

    if test_rows >= 5000:
        score += 6
    elif test_rows >= 1000:
        score += 4
    elif test_rows >= 250:
        score += 2
    elif test_rows < 100:
        score -= 8

    return round(max(0, min(100, score)), 2)


def assign_catalogue_tier(row: pd.Series) -> str:
    score = row.get("catalogue_score", 0)
    label = row.get("model_label", "")
    test_rows = row.get("test_rows", 0)

    if score >= 85 and label in {"strong_baseline", "strong_advanced"} and test_rows >= 250:
        return "tier_1_playbook_candidate"

    if score >= 75 and label in PRIORITY_LABELS:
        return "tier_2_research_candidate"

    if score >= 65:
        return "tier_3_monitor_candidate"

    if score >= 50:
        return "tier_4_watch_only"

    return "tier_5_ignore_for_now"


def build_research_focus(row: pd.Series) -> str:
    symbol = row.get("symbol", "UNKNOWN")
    bar_type = row.get("bar_type", "UNKNOWN")
    target = row.get("target", "UNKNOWN")
    model_name = row.get("model_name", "UNKNOWN")

    return f"{symbol} | {bar_type} | {target} | {model_name}"


def summarise_catalogue(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    grouped = (
        df.groupby(group_cols, dropna=False)
        .agg(
            catalogue_count=("catalogue_tier", "count"),
            tier_1_count=("catalogue_tier", lambda s: int((s == "tier_1_playbook_candidate").sum())),
            tier_2_count=("catalogue_tier", lambda s: int((s == "tier_2_research_candidate").sum())),
            priority_label_count=("model_label", lambda s: int(s.isin(PRIORITY_LABELS).sum())),
            avg_catalogue_score=("catalogue_score", "mean"),
            max_catalogue_score=("catalogue_score", "max"),
            avg_model_score=("model_score", "mean"),
            max_model_score=("model_score", "max"),
            avg_roc_auc=("roc_auc", "mean"),
            max_roc_auc=("roc_auc", "max"),
            avg_balanced_accuracy=("balanced_accuracy", "mean"),
            max_balanced_accuracy=("balanced_accuracy", "max"),
            avg_test_rows=("test_rows", "mean"),
            max_test_rows=("test_rows", "max"),
        )
        .reset_index()
    )

    grouped = grouped.sort_values(
        [
            "tier_1_count",
            "tier_2_count",
            "max_catalogue_score",
            "avg_catalogue_score",
        ],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    grouped["summary_rank"] = grouped.index + 1

    return grouped


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 27 - BEST MODEL CATALOGUE")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    comparison_dir = analysis_dir / "model_comparison"

    comparison_path = comparison_dir / "microstructure_model_comparison_latest.csv"
    area_path = comparison_dir / "microstructure_model_comparison_by_area_latest.csv"

    report_dir = analysis_dir / "best_model_catalogue"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Model comparison: {comparison_path}")
    print(f"Area comparison:  {area_path}")
    print(f"Report dir:       {report_dir}")
    print("-" * 90)

    comparison_df = safe_read_csv(comparison_path)
    area_df = safe_read_csv(area_path)

    priority_df = comparison_df[
        comparison_df["comparison_tier"].isin(PRIORITY_TIERS)
        | comparison_df["model_label"].isin(PRIORITY_LABELS)
    ].copy()

    if priority_df.empty:
        raise RuntimeError("No priority model rows found for catalogue.")

    priority_df["catalogue_score"] = priority_df.apply(build_catalogue_score, axis=1)
    priority_df["catalogue_tier"] = priority_df.apply(assign_catalogue_tier, axis=1)
    priority_df["research_focus"] = priority_df.apply(build_research_focus, axis=1)
    priority_df["catalogue_created_at_utc"] = datetime.now(timezone.utc).isoformat()

    priority_df = priority_df.sort_values(
        [
            "catalogue_score",
            "roc_auc",
            "balanced_accuracy",
            "test_rows",
        ],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    priority_df["catalogue_rank"] = priority_df.index + 1

    symbol_summary = summarise_catalogue(priority_df, ["symbol"])
    bar_type_summary = summarise_catalogue(priority_df, ["bar_type"])
    target_summary = summarise_catalogue(priority_df, ["target"])
    model_summary = summarise_catalogue(priority_df, ["model_name", "model_family"])
    symbol_bar_type_summary = summarise_catalogue(priority_df, ["symbol", "bar_type"])
    symbol_target_summary = summarise_catalogue(priority_df, ["symbol", "target"])

    catalogue_csv = report_dir / "microstructure_best_model_catalogue_latest.csv"
    symbol_csv = report_dir / "microstructure_best_model_catalogue_by_symbol_latest.csv"
    bar_type_csv = report_dir / "microstructure_best_model_catalogue_by_bar_type_latest.csv"
    target_csv = report_dir / "microstructure_best_model_catalogue_by_target_latest.csv"
    model_csv = report_dir / "microstructure_best_model_catalogue_by_model_latest.csv"
    symbol_bar_type_csv = report_dir / "microstructure_best_model_catalogue_by_symbol_bar_type_latest.csv"
    symbol_target_csv = report_dir / "microstructure_best_model_catalogue_by_symbol_target_latest.csv"

    json_path = report_dir / "microstructure_best_model_catalogue_latest.json"
    txt_path = report_dir / "microstructure_best_model_catalogue_latest.txt"

    priority_df.to_csv(catalogue_csv, index=False)
    symbol_summary.to_csv(symbol_csv, index=False)
    bar_type_summary.to_csv(bar_type_csv, index=False)
    target_summary.to_csv(target_csv, index=False)
    model_summary.to_csv(model_csv, index=False)
    symbol_bar_type_summary.to_csv(symbol_bar_type_csv, index=False)
    symbol_target_summary.to_csv(symbol_target_csv, index=False)

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "comparison_rows": len(comparison_df),
        "area_rows": len(area_df),
        "catalogue_rows": len(priority_df),
        "top_catalogue": priority_df.head(50).to_dict(orient="records"),
        "symbol_summary": symbol_summary.to_dict(orient="records"),
        "bar_type_summary": bar_type_summary.to_dict(orient="records"),
        "target_summary": target_summary.to_dict(orient="records"),
        "model_summary": model_summary.to_dict(orient="records"),
        "symbol_bar_type_summary": symbol_bar_type_summary.to_dict(orient="records"),
        "symbol_target_summary": symbol_target_summary.to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    tier_counts = priority_df["catalogue_tier"].value_counts(dropna=False).to_dict()
    label_counts = priority_df["model_label"].value_counts(dropna=False).to_dict()
    family_counts = priority_df["model_family"].value_counts(dropna=False).to_dict()

    display_cols = [
        "catalogue_rank",
        "symbol",
        "bar_type",
        "parameter",
        "target",
        "model_name",
        "model_family",
        "model_label",
        "comparison_tier",
        "test_rows",
        "accuracy",
        "balanced_accuracy",
        "precision",
        "recall",
        "roc_auc",
        "model_score",
        "catalogue_score",
        "catalogue_tier",
        "research_focus",
    ]

    available_display_cols = [c for c in display_cols if c in priority_df.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE BEST MODEL CATALOGUE")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Model comparison rows: {len(comparison_df):,}")
    lines.append(f"Area comparison rows:  {len(area_df):,}")
    lines.append(f"Catalogue rows:        {len(priority_df):,}")
    lines.append("")
    lines.append(f"Catalogue tier counts: {tier_counts}")
    lines.append(f"Model label counts:    {label_counts}")
    lines.append(f"Model family counts:   {family_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 40 BEST MODEL CATALOGUE")
    lines.append("-" * 90)
    lines.append(priority_df[available_display_cols].head(40).to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("SYMBOL SUMMARY")
    lines.append("-" * 90)
    lines.append(symbol_summary.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("BAR TYPE SUMMARY")
    lines.append("-" * 90)
    lines.append(bar_type_summary.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("TARGET SUMMARY")
    lines.append("-" * 90)
    lines.append(target_summary.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("MODEL SUMMARY")
    lines.append("-" * 90)
    lines.append(model_summary.to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Model comparison rows: {len(comparison_df):,}")
    print(f"Catalogue rows:        {len(priority_df):,}")
    print(f"Tier counts:           {tier_counts}")
    print(f"Model label counts:    {label_counts}")
    print(f"Model family counts:   {family_counts}")
    print("-" * 90)
    print("[TOP 20 CATALOGUE ROWS]")
    print(priority_df[available_display_cols].head(20).to_string(index=False))
    print("-" * 90)
    print("[SYMBOL SUMMARY]")
    print(symbol_summary.to_string(index=False))
    print("-" * 90)
    print("[DONE] Best model catalogue complete.")
    print(f"Catalogue CSV:       {catalogue_csv}")
    print(f"Symbol CSV:          {symbol_csv}")
    print(f"Bar Type CSV:        {bar_type_csv}")
    print(f"Target CSV:          {target_csv}")
    print(f"Model CSV:           {model_csv}")
    print(f"Symbol/Bar Type CSV: {symbol_bar_type_csv}")
    print(f"Symbol/Target CSV:   {symbol_target_csv}")
    print(f"JSON output:         {json_path}")
    print(f"TXT output:          {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()