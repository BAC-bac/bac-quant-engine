"""
BACQE MICROSTRUCTURE 24 - MODEL LEADERBOARD

Purpose:
    Summarise baseline model results from Script 23.

Inputs:
    E:/Quant_Lab/data/analysis/microstructure/baseline_models/
        microstructure_baseline_models_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/model_leaderboard/
        microstructure_model_leaderboard_latest.csv
        microstructure_model_leaderboard_latest.json
        microstructure_model_leaderboard_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

POSITIVE_MODEL_LABELS = {
    "strong_baseline",
    "research_baseline",
    "weak_baseline",
}

PRIORITY_MODEL_LABELS = {
    "strong_baseline",
    "research_baseline",
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


def build_group_score(row: pd.Series) -> float:
    score = 0.0

    total = row.get("model_count", 0)
    strong = row.get("strong_baseline_count", 0)
    research = row.get("research_baseline_count", 0)
    weak = row.get("weak_baseline_count", 0)

    avg_bal = row.get("avg_balanced_accuracy", 0)
    max_bal = row.get("max_balanced_accuracy", 0)
    avg_auc = row.get("avg_roc_auc", 0)
    max_auc = row.get("max_roc_auc", 0)
    avg_test_rows = row.get("avg_test_rows", 0)

    score += min(strong * 10, 35)
    score += min(research * 5, 25)
    score += min(weak * 1, 8)
    score += min(total * 0.5, 5)

    if pd.notna(avg_bal):
        score += max(0, (avg_bal - 0.50) * 250)

    if pd.notna(max_bal):
        score += max(0, (max_bal - 0.50) * 150)

    if pd.notna(avg_auc):
        score += max(0, (avg_auc - 0.50) * 200)

    if pd.notna(max_auc):
        score += max(0, (max_auc - 0.50) * 120)

    if avg_test_rows >= 5000:
        score += 8
    elif avg_test_rows >= 1000:
        score += 5
    elif avg_test_rows >= 250:
        score += 3
    elif avg_test_rows < 100:
        score -= 8

    return round(max(0, min(100, score)), 2)


def assign_group_tier(row: pd.Series) -> str:
    score = row.get("group_score", 0)
    strong = row.get("strong_baseline_count", 0)
    research = row.get("research_baseline_count", 0)

    if score >= 85 and strong >= 2:
        return "tier_1_priority_model_area"

    if score >= 75 and strong >= 1:
        return "tier_2_strong_model_area"

    if score >= 65 and research >= 2:
        return "tier_3_research_model_area"

    if score >= 50:
        return "tier_4_watch_model_area"

    return "tier_5_low_priority"


def summarise_group(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    grouped = (
        df
        .groupby(group_cols, dropna=False)
        .agg(
            model_count=("model_label", "count"),
            ok_model_count=("status", lambda s: int((s == "ok").sum())),
            strong_baseline_count=("model_label", lambda s: int((s == "strong_baseline").sum())),
            research_baseline_count=("model_label", lambda s: int((s == "research_baseline").sum())),
            weak_baseline_count=("model_label", lambda s: int((s == "weak_baseline").sum())),
            no_edge_count=("model_label", lambda s: int((s == "no_edge_baseline").sum())),
            avg_accuracy=("accuracy", "mean"),
            max_accuracy=("accuracy", "max"),
            avg_balanced_accuracy=("balanced_accuracy", "mean"),
            max_balanced_accuracy=("balanced_accuracy", "max"),
            avg_precision=("precision", "mean"),
            max_precision=("precision", "max"),
            avg_recall=("recall", "mean"),
            max_recall=("recall", "max"),
            avg_roc_auc=("roc_auc", "mean"),
            max_roc_auc=("roc_auc", "max"),
            avg_test_rows=("test_rows", "mean"),
            max_test_rows=("test_rows", "max"),
            symbol_count=("symbol", "nunique"),
            bar_type_count=("bar_type", "nunique"),
            target_count=("target", "nunique"),
            model_type_count=("model_name", "nunique"),
        )
        .reset_index()
    )

    grouped["group_score"] = grouped.apply(build_group_score, axis=1)
    grouped["group_tier"] = grouped.apply(assign_group_tier, axis=1)
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    grouped = grouped.sort_values(
        [
            "group_score",
            "strong_baseline_count",
            "research_baseline_count",
            "max_roc_auc",
            "max_balanced_accuracy",
        ],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)

    grouped["group_rank"] = grouped.index + 1

    return grouped


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 24 - MODEL LEADERBOARD")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    baseline_path = (
        analysis_dir
        / "baseline_models"
        / "microstructure_baseline_models_latest.csv"
    )

    report_dir = analysis_dir / "model_leaderboard"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Baseline models: {baseline_path}")
    print(f"Report dir:      {report_dir}")
    print("-" * 90)

    if not baseline_path.exists():
        raise FileNotFoundError(
            f"Missing baseline model file: {baseline_path}. "
            "Run script 23 first."
        )

    df = pd.read_csv(baseline_path)

    print(f"Baseline rows: {len(df):,}")
    print("-" * 90)

    overall_summary = summarise_group(df, ["symbol", "bar_type", "parameter", "model_name", "target"])
    symbol_summary = summarise_group(df, ["symbol"])
    bar_type_summary = summarise_group(df, ["bar_type"])
    target_summary = summarise_group(df, ["target"])
    model_summary = summarise_group(df, ["model_name"])
    symbol_bar_type_summary = summarise_group(df, ["symbol", "bar_type"])
    symbol_target_summary = summarise_group(df, ["symbol", "target"])

    priority_df = df[
        df["model_label"].isin(PRIORITY_MODEL_LABELS)
    ].copy()

    csv_overall = report_dir / "microstructure_model_leaderboard_latest.csv"
    csv_symbol = report_dir / "microstructure_model_leaderboard_by_symbol_latest.csv"
    csv_bar_type = report_dir / "microstructure_model_leaderboard_by_bar_type_latest.csv"
    csv_target = report_dir / "microstructure_model_leaderboard_by_target_latest.csv"
    csv_model = report_dir / "microstructure_model_leaderboard_by_model_latest.csv"
    csv_symbol_bar_type = report_dir / "microstructure_model_leaderboard_by_symbol_bar_type_latest.csv"
    csv_symbol_target = report_dir / "microstructure_model_leaderboard_by_symbol_target_latest.csv"
    csv_priority = report_dir / "microstructure_priority_model_results_latest.csv"

    json_path = report_dir / "microstructure_model_leaderboard_latest.json"
    txt_path = report_dir / "microstructure_model_leaderboard_latest.txt"

    overall_summary.to_csv(csv_overall, index=False)
    symbol_summary.to_csv(csv_symbol, index=False)
    bar_type_summary.to_csv(csv_bar_type, index=False)
    target_summary.to_csv(csv_target, index=False)
    model_summary.to_csv(csv_model, index=False)
    symbol_bar_type_summary.to_csv(csv_symbol_bar_type, index=False)
    symbol_target_summary.to_csv(csv_symbol_target, index=False)
    priority_df.to_csv(csv_priority, index=False)

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_rows": len(df),
        "priority_rows": len(priority_df),
        "top_overall": overall_summary.head(50).to_dict(orient="records"),
        "symbol_summary": symbol_summary.to_dict(orient="records"),
        "bar_type_summary": bar_type_summary.to_dict(orient="records"),
        "target_summary": target_summary.to_dict(orient="records"),
        "model_summary": model_summary.to_dict(orient="records"),
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    label_counts = df["model_label"].value_counts(dropna=False).to_dict()
    status_counts = df["status"].value_counts(dropna=False).to_dict()
    priority_counts = priority_df["model_label"].value_counts(dropna=False).to_dict()

    top_overall = overall_summary.head(30)
    top_symbols = symbol_summary.head(10)
    top_bar_types = bar_type_summary.head(10)
    top_targets = target_summary.head(10)
    top_models = model_summary.head(10)
    top_symbol_bar_types = symbol_bar_type_summary.head(15)
    top_symbol_targets = symbol_target_summary.head(15)

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE MODEL LEADERBOARD")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Baseline rows: {len(df):,}")
    lines.append(f"Priority rows: {len(priority_df):,}")
    lines.append("")
    lines.append(f"Status counts: {status_counts}")
    lines.append(f"Model label counts: {label_counts}")
    lines.append(f"Priority label counts: {priority_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 30 OVERALL MODEL AREAS")
    lines.append("-" * 90)
    lines.append(top_overall.to_string(index=False))
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
    lines.append("-" * 90)
    lines.append("TARGET SUMMARY")
    lines.append("-" * 90)
    lines.append(top_targets.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("MODEL SUMMARY")
    lines.append("-" * 90)
    lines.append(top_models.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("SYMBOL / BAR TYPE SUMMARY")
    lines.append("-" * 90)
    lines.append(top_symbol_bar_types.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("SYMBOL / TARGET SUMMARY")
    lines.append("-" * 90)
    lines.append(top_symbol_targets.to_string(index=False))
    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[SUMMARY]")
    print(f"Baseline rows:       {len(df):,}")
    print(f"Priority rows:       {len(priority_df):,}")
    print(f"Status counts:       {status_counts}")
    print(f"Model label counts:  {label_counts}")
    print(f"Priority counts:     {priority_counts}")
    print("-" * 90)
    print("[TOP SYMBOLS]")
    print(top_symbols.to_string(index=False))
    print("-" * 90)
    print("[TOP BAR TYPES]")
    print(top_bar_types.to_string(index=False))
    print("-" * 90)
    print("[TOP TARGETS]")
    print(top_targets.to_string(index=False))
    print("-" * 90)
    print("[TOP MODELS]")
    print(top_models.to_string(index=False))
    print("-" * 90)
    print("[DONE] Model leaderboard complete.")
    print(f"Overall CSV:          {csv_overall}")
    print(f"Symbol CSV:           {csv_symbol}")
    print(f"Bar Type CSV:         {csv_bar_type}")
    print(f"Target CSV:           {csv_target}")
    print(f"Model CSV:            {csv_model}")
    print(f"Symbol/Bar Type CSV:  {csv_symbol_bar_type}")
    print(f"Symbol/Target CSV:    {csv_symbol_target}")
    print(f"Priority CSV:         {csv_priority}")
    print(f"JSON output:          {json_path}")
    print(f"TXT output:           {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()