"""
BACQE TICK RESEARCH - 02 Build Tick Dataset Summary

Aggregates the tick data audit into a symbol-level summary.

Input:
    E:/Quant_Lab/data/analysis/tick_research/tick_data_audit_latest.csv

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/tick_dataset_summary_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/tick_dataset_summary_latest.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_PATH = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research" / "tick_data_audit_latest.csv"
OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"

OUTPUT_CSV = OUTPUT_DIR / "tick_dataset_summary_latest.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "tick_dataset_summary_latest.parquet"


def classify_tick_depth(total_ticks: int) -> str:
    if total_ticks >= 1_000_000:
        return "deep"
    if total_ticks >= 500_000:
        return "good"
    if total_ticks >= 100_000:
        return "developing"
    if total_ticks > 0:
        return "thin"
    return "empty"


def classify_spread_quality(avg_spread: float | None, bad_spread_count: int | None) -> str:
    if bad_spread_count is not None and bad_spread_count > 0:
        return "needs_review"

    if avg_spread is None or pd.isna(avg_spread):
        return "unknown"

    if avg_spread <= 0:
        return "needs_review"

    return "clean"


def build_summary(audit: pd.DataFrame) -> pd.DataFrame:
    audit["min_tick_time"] = pd.to_datetime(audit["min_tick_time"], errors="coerce", utc=True)
    audit["max_tick_time"] = pd.to_datetime(audit["max_tick_time"], errors="coerce", utc=True)

    grouped = (
        audit.groupby(["symbol", "broker"], dropna=False)
        .agg(
            file_count=("file_path", "count"),
            successful_files=("read_status", lambda s: int((s == "success").sum())),
            failed_files=("read_status", lambda s: int((s == "failed").sum())),
            total_ticks=("row_count", "sum"),
            first_tick_time=("min_tick_time", "min"),
            last_tick_time=("max_tick_time", "max"),
            avg_ticks_per_file=("row_count", "mean"),
            min_ticks_per_file=("row_count", "min"),
            max_ticks_per_file=("row_count", "max"),
            avg_duration_seconds=("duration_seconds", "mean"),
            min_duration_seconds=("duration_seconds", "min"),
            max_duration_seconds=("duration_seconds", "max"),
            avg_spread=("avg_spread", "mean"),
            min_spread=("min_spread", "min"),
            max_spread=("max_spread", "max"),
            bad_spread_count=("bad_spread_count", "sum"),
            duplicate_rows=("duplicate_rows", "sum"),
            total_size_mb=("file_size_mb", "sum"),
        )
        .reset_index()
    )

    grouped["total_ticks"] = grouped["total_ticks"].fillna(0).astype(int)
    grouped["successful_files"] = grouped["successful_files"].fillna(0).astype(int)
    grouped["failed_files"] = grouped["failed_files"].fillna(0).astype(int)

    grouped["avg_ticks_per_file"] = grouped["avg_ticks_per_file"].round(2)
    grouped["avg_duration_seconds"] = grouped["avg_duration_seconds"].round(2)

    grouped["avg_spread"] = grouped["avg_spread"].round(6)
    grouped["min_spread"] = grouped["min_spread"].round(6)
    grouped["max_spread"] = grouped["max_spread"].round(6)

    grouped["total_size_mb"] = grouped["total_size_mb"].round(4)

    grouped["coverage_days"] = (
        grouped["last_tick_time"] - grouped["first_tick_time"]
    ).dt.total_seconds() / 86400

    grouped["coverage_days"] = grouped["coverage_days"].round(2)

    grouped["ticks_per_coverage_day"] = (
        grouped["total_ticks"] / grouped["coverage_days"].replace(0, pd.NA)
    ).round(2)

    grouped["tick_depth_category"] = grouped["total_ticks"].apply(classify_tick_depth)

    grouped["spread_quality"] = grouped.apply(
        lambda row: classify_spread_quality(row.get("avg_spread"), row.get("bad_spread_count")),
        axis=1,
    )

    grouped["analysis_priority_score"] = (
        grouped["total_ticks"].rank(pct=True) * 50
        + grouped["file_count"].rank(pct=True) * 25
        + grouped["coverage_days"].rank(pct=True) * 25
    ).round(2)

    grouped["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    grouped = grouped.sort_values(
        by=["analysis_priority_score", "total_ticks"],
        ascending=[False, False],
    ).reset_index(drop=True)

    return grouped


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 02 BUILD TICK DATASET SUMMARY")
    print("=" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Tick audit not found: {INPUT_PATH}")

    audit = pd.read_csv(INPUT_PATH, low_memory=False)

    print(f"Audit rows: {len(audit):,}")
    print("-" * 90)

    summary = build_summary(audit)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    summary.to_csv(OUTPUT_CSV, index=False)
    summary.to_parquet(OUTPUT_PARQUET, index=False)

    print("[DONE] Tick dataset summary created.")
    print(f"Rows:      {len(summary):,}")
    print(f"CSV:       {OUTPUT_CSV}")
    print(f"Parquet:   {OUTPUT_PARQUET}")
    print("-" * 90)

    display_cols = [
        "symbol",
        "broker",
        "file_count",
        "total_ticks",
        "coverage_days",
        "avg_spread",
        "max_spread",
        "duplicate_rows",
        "tick_depth_category",
        "spread_quality",
        "analysis_priority_score",
    ]

    print(summary[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()