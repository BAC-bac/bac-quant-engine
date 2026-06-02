"""
BACQE MICROSTRUCTURE 11 - MICROSTRUCTURE FEATURE QUALITY REPORT

Purpose:
    Generate a readable quality report from the microstructure feature inventory
    and validation outputs.

Inputs:
    E:/Quant_Lab/data/analysis/microstructure/feature_inventory/
        microstructure_feature_inventory_latest.csv

    E:/Quant_Lab/data/analysis/microstructure/feature_validation/
        microstructure_feature_validation_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/feature_quality_report/
        microstructure_feature_quality_report_latest.csv
        microstructure_feature_quality_report_latest.json
        microstructure_feature_quality_report_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"


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


def quality_label(row: pd.Series) -> str:
    status = row.get("dataset_status", "unknown")
    row_count = row.get("row_count", 0)
    nan_ratio = row.get("nan_ratio", 1)
    inf_count = row.get("inf_count_total", 0)

    if status in ["missing", "failed_read", "empty", "warning"]:
        return "poor"

    if inf_count and inf_count > 0:
        return "poor"

    if status == "usable_sparse":
        return "usable_sparse"

    if row_count >= 1000 and nan_ratio <= 0.05:
        return "excellent"

    if row_count >= 250 and nan_ratio <= 0.10:
        return "good"

    if row_count >= 100 and nan_ratio <= 0.15:
        return "usable"

    return "review"


def recommended_use(row: pd.Series) -> str:
    label = row.get("quality_label", "unknown")
    bar_type = row.get("bar_type", "")

    if label == "excellent":
        return "priority_research_candidate"

    if label == "good":
        return "research_candidate"

    if label == "usable":
        return "use_with_caution"

    if label == "usable_sparse":
        return "descriptive_only_until_more_data"

    if label == "review":
        return "manual_review"

    return "exclude_for_now"


def build_quality_score(row: pd.Series) -> float:
    """
    Simple transparent score out of 100.

    Rewards:
        - row count
        - low NaN ratio
        - ready status
        - richer imbalance datasets

    Penalises:
        - sparse data
        - high NaN ratio
        - warnings
    """
    score = 50.0

    row_count = row.get("row_count", 0)
    nan_ratio = row.get("nan_ratio", 1.0)
    status = row.get("dataset_status", "unknown")
    bar_type = row.get("bar_type", "")

    if row_count >= 10000:
        score += 25
    elif row_count >= 5000:
        score += 20
    elif row_count >= 1000:
        score += 15
    elif row_count >= 250:
        score += 8
    elif row_count >= 100:
        score += 3
    else:
        score -= 15

    if nan_ratio <= 0.01:
        score += 15
    elif nan_ratio <= 0.05:
        score += 10
    elif nan_ratio <= 0.10:
        score += 3
    else:
        score -= 10

    if status == "ready":
        score += 10
    elif status == "usable_sparse":
        score -= 5
    else:
        score -= 20

    if bar_type == "tick_imbalance_bars":
        score += 5

    return round(max(0, min(100, score)), 2)


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 11 - MICROSTRUCTURE FEATURE QUALITY REPORT")

    config = load_config()
    micro_cfg = config["microstructure"]

    analysis_dir = get_analysis_dir(micro_cfg)

    inventory_path = (
        analysis_dir
        / "feature_inventory"
        / "microstructure_feature_inventory_latest.csv"
    )

    validation_path = (
        analysis_dir
        / "feature_validation"
        / "microstructure_feature_validation_latest.csv"
    )

    report_dir = analysis_dir / "feature_quality_report"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Inventory:   {inventory_path}")
    print(f"Validation:  {validation_path}")
    print(f"Report dir:  {report_dir}")
    print("-" * 90)

    if not inventory_path.exists():
        raise FileNotFoundError(
            f"Missing feature inventory file: {inventory_path}. "
            "Run script 10 first."
        )

    inventory_df = pd.read_csv(inventory_path)

    if validation_path.exists():
        validation_df = pd.read_csv(validation_path)

        validation_cols = [
            "file_path",
            "status",
            "total_inf_count",
            "duplicate_end_time_rows",
        ]

        available_validation_cols = [
            c for c in validation_cols if c in validation_df.columns
        ]

        merged_df = inventory_df.merge(
            validation_df[available_validation_cols],
            on="file_path",
            how="left",
            suffixes=("", "_validation"),
        )
    else:
        merged_df = inventory_df.copy()
        merged_df["status_validation"] = "missing_validation_file"

    if "inf_count_total" not in merged_df.columns:
        if "total_inf_count" in merged_df.columns:
            merged_df["inf_count_total"] = merged_df["total_inf_count"]
        else:
            merged_df["inf_count_total"] = 0

    merged_df["quality_label"] = merged_df.apply(quality_label, axis=1)
    merged_df["recommended_use"] = merged_df.apply(recommended_use, axis=1)
    merged_df["quality_score"] = merged_df.apply(build_quality_score, axis=1)
    merged_df["report_created_at_utc"] = datetime.now(timezone.utc).isoformat()

    sort_cols = ["quality_score", "row_count"]
    merged_df = merged_df.sort_values(sort_cols, ascending=[False, False])

    csv_path = report_dir / "microstructure_feature_quality_report_latest.csv"
    json_path = report_dir / "microstructure_feature_quality_report_latest.json"
    txt_path = report_dir / "microstructure_feature_quality_report_latest.txt"

    merged_df.to_csv(csv_path, index=False)

    records = merged_df.to_dict(orient="records")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)

    status_counts = merged_df["dataset_status"].value_counts(dropna=False).to_dict()
    quality_counts = merged_df["quality_label"].value_counts(dropna=False).to_dict()
    recommended_counts = merged_df["recommended_use"].value_counts(dropna=False).to_dict()
    bar_type_counts = merged_df["bar_type"].value_counts(dropna=False).to_dict()

    top_candidates = merged_df.head(15)[
        [
            "symbol",
            "bar_type",
            "parameter",
            "row_count",
            "column_count",
            "nan_ratio",
            "dataset_status",
            "quality_label",
            "recommended_use",
            "quality_score",
        ]
    ]

    sparse_candidates = merged_df[
        merged_df["dataset_status"] == "usable_sparse"
    ][
        [
            "symbol",
            "bar_type",
            "parameter",
            "row_count",
            "nan_ratio",
            "quality_label",
            "recommended_use",
            "quality_score",
        ]
    ]

    report_lines = []

    report_lines.append("=" * 90)
    report_lines.append("BACQE MICROSTRUCTURE FEATURE QUALITY REPORT")
    report_lines.append("=" * 90)
    report_lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    report_lines.append("")
    report_lines.append(f"Datasets reviewed: {len(merged_df)}")
    report_lines.append(f"Dataset status counts: {status_counts}")
    report_lines.append(f"Quality label counts:  {quality_counts}")
    report_lines.append(f"Recommended use counts: {recommended_counts}")
    report_lines.append(f"Bar type counts: {bar_type_counts}")
    report_lines.append("")
    report_lines.append("-" * 90)
    report_lines.append("TOP FEATURE DATASETS")
    report_lines.append("-" * 90)
    report_lines.append(top_candidates.to_string(index=False))
    report_lines.append("")
    report_lines.append("-" * 90)
    report_lines.append("SPARSE / CAUTION DATASETS")
    report_lines.append("-" * 90)

    if sparse_candidates.empty:
        report_lines.append("No sparse datasets detected.")
    else:
        report_lines.append(sparse_candidates.to_string(index=False))

    report_lines.append("")
    report_lines.append("=" * 90)

    txt_report = "\n".join(report_lines)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(txt_report)

    print("[SUMMARY]")
    print(f"Datasets reviewed:       {len(merged_df)}")
    print(f"Dataset status counts:   {status_counts}")
    print(f"Quality label counts:    {quality_counts}")
    print(f"Recommended use counts:  {recommended_counts}")
    print(f"Bar type counts:         {bar_type_counts}")
    print("-" * 90)

    print("[TOP 15]")
    print(top_candidates.to_string(index=False))

    print("-" * 90)
    print("[DONE] Microstructure feature quality report generated.")
    print(f"CSV output:  {csv_path}")
    print(f"JSON output: {json_path}")
    print(f"TXT output:  {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()