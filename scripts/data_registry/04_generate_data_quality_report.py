"""
BACQE DATA REGISTRY - 04 Generate Data Quality Report

Reads:
    dataset_registry_latest.csv

Creates:
    dataset_quality_report_latest.txt
    dataset_quality_report_latest.json
    dataset_quality_summary_latest.csv
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
REGISTRY_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "data_registry"

REGISTRY_PATH = REGISTRY_DIR / "dataset_registry_latest.csv"

OUTPUT_TXT = REGISTRY_DIR / "dataset_quality_report_latest.txt"
OUTPUT_JSON = REGISTRY_DIR / "dataset_quality_report_latest.json"
OUTPUT_SUMMARY_CSV = REGISTRY_DIR / "dataset_quality_summary_latest.csv"


# =============================================================================
# HELPERS
# =============================================================================

def safe_value_counts(df: pd.DataFrame, column: str) -> dict:
    if column not in df.columns:
        return {}

    return (
        df[column]
        .fillna("missing")
        .value_counts(dropna=False)
        .to_dict()
    )


def top_rows(df: pd.DataFrame, columns: list[str], n: int = 10) -> list[dict]:
    available_cols = [col for col in columns if col in df.columns]

    if not available_cols:
        return []

    return df[available_cols].head(n).to_dict(orient="records")


def build_report(registry: pd.DataFrame) -> tuple[str, dict, pd.DataFrame]:
    now_utc = datetime.now(timezone.utc).isoformat()

    total_files = len(registry)

    analysis_ready_count = int(registry["analysis_ready"].sum()) if "analysis_ready" in registry.columns else 0
    not_ready_count = total_files - analysis_ready_count

    avg_quality_score = round(registry["quality_score"].mean(), 2) if "quality_score" in registry.columns else None
    median_quality_score = round(registry["quality_score"].median(), 2) if "quality_score" in registry.columns else None

    total_size_mb = round(registry["file_size_mb"].sum(), 2) if "file_size_mb" in registry.columns else None
    total_size_gb = round(total_size_mb / 1024, 2) if total_size_mb is not None else None

    failed = registry[registry.get("dataset_status") == "failed_read"].copy()
    stale = registry[registry.get("freshness_category") == "stale"].copy()
    unknown = registry[registry.get("dataset_group") == "unknown"].copy()

    largest = registry.sort_values("file_size_mb", ascending=False).copy() if "file_size_mb" in registry.columns else registry.copy()
    best_quality = registry.sort_values("quality_score", ascending=False).copy() if "quality_score" in registry.columns else registry.copy()
    worst_quality = registry.sort_values("quality_score", ascending=True).copy() if "quality_score" in registry.columns else registry.copy()

    group_summary = (
        registry.groupby("dataset_group", dropna=False)
        .agg(
            files=("file_path", "count"),
            analysis_ready=("analysis_ready", "sum"),
            avg_quality_score=("quality_score", "mean"),
            total_size_mb=("file_size_mb", "sum"),
        )
        .reset_index()
        .sort_values("files", ascending=False)
    )

    group_summary["avg_quality_score"] = group_summary["avg_quality_score"].round(2)
    group_summary["total_size_mb"] = group_summary["total_size_mb"].round(2)
    group_summary["total_size_gb"] = (group_summary["total_size_mb"] / 1024).round(2)

    report_dict = {
        "report_time_utc": now_utc,
        "total_files": total_files,
        "analysis_ready_count": analysis_ready_count,
        "not_ready_count": not_ready_count,
        "analysis_ready_pct": round((analysis_ready_count / total_files) * 100, 2) if total_files else 0,
        "avg_quality_score": avg_quality_score,
        "median_quality_score": median_quality_score,
        "total_size_mb": total_size_mb,
        "total_size_gb": total_size_gb,
        "dataset_group_counts": safe_value_counts(registry, "dataset_group"),
        "dataset_status_counts": safe_value_counts(registry, "dataset_status"),
        "quality_label_counts": safe_value_counts(registry, "quality_label"),
        "freshness_category_counts": safe_value_counts(registry, "freshness_category"),
        "source_guess_counts": safe_value_counts(registry, "source_guess"),
        "failed_read_count": len(failed),
        "stale_count": len(stale),
        "unknown_group_count": len(unknown),
        "largest_files_top_10": top_rows(
            largest,
            ["dataset_group", "source_guess", "file_name", "file_size_mb", "file_path"],
            10,
        ),
        "worst_quality_top_10": top_rows(
            worst_quality,
            ["dataset_group", "dataset_status", "quality_score", "file_name", "error_message", "file_path"],
            10,
        ),
        "best_quality_top_10": top_rows(
            best_quality,
            ["dataset_group", "dataset_status", "quality_score", "file_name", "row_count_profiled", "file_path"],
            10,
        ),
    }

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE DATA QUALITY REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC:        {now_utc}")
    lines.append(f"Registry path:          {REGISTRY_PATH}")
    lines.append("-" * 90)
    lines.append(f"Total files:            {total_files:,}")
    lines.append(f"Analysis ready:         {analysis_ready_count:,}")
    lines.append(f"Not ready:              {not_ready_count:,}")
    lines.append(f"Analysis ready pct:     {report_dict['analysis_ready_pct']}%")
    lines.append(f"Average quality score:  {avg_quality_score}")
    lines.append(f"Median quality score:   {median_quality_score}")
    lines.append(f"Total size:             {total_size_gb} GB")
    lines.append("-" * 90)

    lines.append("\nDATASET GROUP SUMMARY")
    lines.append("-" * 90)
    lines.append(group_summary.to_string(index=False))

    lines.append("\nDATASET STATUS COUNTS")
    lines.append("-" * 90)
    lines.append(pd.Series(report_dict["dataset_status_counts"]).to_string())

    lines.append("\nQUALITY LABEL COUNTS")
    lines.append("-" * 90)
    lines.append(pd.Series(report_dict["quality_label_counts"]).to_string())

    lines.append("\nFRESHNESS CATEGORY COUNTS")
    lines.append("-" * 90)
    lines.append(pd.Series(report_dict["freshness_category_counts"]).to_string())

    lines.append("\nSOURCE GUESS COUNTS")
    lines.append("-" * 90)
    lines.append(pd.Series(report_dict["source_guess_counts"]).to_string())

    lines.append("\nLARGEST FILES - TOP 10")
    lines.append("-" * 90)
    largest_cols = ["dataset_group", "source_guess", "file_name", "file_size_mb", "file_path"]
    lines.append(largest[[col for col in largest_cols if col in largest.columns]].head(10).to_string(index=False))

    lines.append("\nWORST QUALITY FILES - TOP 10")
    lines.append("-" * 90)
    worst_cols = ["dataset_group", "dataset_status", "quality_score", "file_name", "error_message", "file_path"]
    lines.append(worst_quality[[col for col in worst_cols if col in worst_quality.columns]].head(10).to_string(index=False))

    lines.append("\nFAILED READ FILES - SAMPLE")
    lines.append("-" * 90)
    if len(failed) > 0:
        failed_cols = ["dataset_group", "extension", "file_name", "error_message", "file_path"]
        lines.append(failed[[col for col in failed_cols if col in failed.columns]].head(25).to_string(index=False))
    else:
        lines.append("No failed read files found.")

    lines.append("\nUNKNOWN DATASET GROUP FILES")
    lines.append("-" * 90)
    if len(unknown) > 0:
        unknown_cols = ["file_name", "extension", "file_path"]
        lines.append(unknown[[col for col in unknown_cols if col in unknown.columns]].head(25).to_string(index=False))
    else:
        lines.append("No unknown dataset group files found.")

    lines.append("\nRECOMMENDED NEXT ACTIONS")
    lines.append("-" * 90)

    if len(failed) > 0:
        lines.append(f"1. Review failed_read files: {len(failed):,}")

    if len(unknown) > 0:
        lines.append(f"2. Improve dataset grouping rules for unknown files: {len(unknown):,}")

    if len(stale) > 0:
        lines.append(f"3. Review stale datasets: {len(stale):,}")

    lines.append("4. Build the script 05 cycle runner to refresh scan/profile/registry/report in one command.")
    lines.append("5. After the registry cycle is stable, build specialist audits for tick data, regimes, macro and betting data.")

    report_text = "\n".join(lines)

    return report_text, report_dict, group_summary


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=" * 90)
    print("BACQE DATA REGISTRY - 04 GENERATE DATA QUALITY REPORT")
    print("=" * 90)

    if not REGISTRY_PATH.exists():
        raise FileNotFoundError(f"Registry not found: {REGISTRY_PATH}")

    registry = pd.read_csv(REGISTRY_PATH, low_memory=False)

    print(f"Registry rows: {len(registry):,}")
    print("-" * 90)

    report_text, report_dict, group_summary = build_report(registry)

    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)

    OUTPUT_TXT.write_text(report_text, encoding="utf-8")

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(report_dict, f, indent=4)

    group_summary.to_csv(OUTPUT_SUMMARY_CSV, index=False)

    print("[DONE] Data quality report created.")
    print(f"TXT report:     {OUTPUT_TXT}")
    print(f"JSON report:    {OUTPUT_JSON}")
    print(f"CSV summary:    {OUTPUT_SUMMARY_CSV}")
    print("-" * 90)

    print(report_text[:3000])


if __name__ == "__main__":
    main()