"""
BACQE Script 15
Regime Engine Optimisation Audit

Purpose:
- Audit regime pipeline outputs
- Identify large parquet files and likely recomputation bottlenecks
- Check freshness of feature/classification/router outputs
- Estimate row counts where possible
- Produce CSV + JSON reports for optimisation planning

This script does NOT modify any data.
"""

from pathlib import Path
from datetime import datetime
import json
import os
import pandas as pd


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

REGIME_OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "processed" / "regimes"
REGIME_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis"

REPORT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_optimisation_audit"

WATCH_FOLDERS = {
    "02_features": REGIME_OUTPUT_DIR / "features",
    "03_classified_regimes": REGIME_OUTPUT_DIR / "classified",
    "04_recent_features": REGIME_OUTPUT_DIR / "recent" / "features",
    "04_recent_classified": REGIME_OUTPUT_DIR / "recent" / "classified",
    "05_router": REGIME_ANALYSIS_DIR / "regime_signal_router",
    "06_strategy_mapping": REGIME_ANALYSIS_DIR / "regime_strategy_mapping",
    "07_strategy_performance": REGIME_ANALYSIS_DIR / "regime_strategy_performance",
    "08_router_validation": REGIME_ANALYSIS_DIR / "regime_router_validation",
}

PARQUET_EXTENSIONS = {".parquet"}
CSV_EXTENSIONS = {".csv"}


# ============================================================
# HELPERS
# ============================================================

def bytes_to_mb(size_bytes: int) -> float:
    return round(size_bytes / (1024 * 1024), 3)


def safe_get_mtime(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    except Exception:
        return ""


def try_count_parquet_rows(path: Path):
    """
    Attempts to count parquet rows cheaply using pyarrow metadata.
    Falls back to pandas if pyarrow metadata is unavailable.
    """
    try:
        import pyarrow.parquet as pq
        return pq.ParquetFile(path).metadata.num_rows
    except Exception:
        try:
            return len(pd.read_parquet(path))
        except Exception:
            return None


def try_count_csv_rows(path: Path):
    """
    Counts CSV rows without fully loading the file into pandas.
    Subtracts one for header where possible.
    """
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            row_count = sum(1 for _ in f)
        return max(row_count - 1, 0)
    except Exception:
        return None


def classify_stage_pressure(stage_name: str, size_mb: float, rows):
    """
    Basic heuristic to flag likely optimisation priorities.
    """
    pressure = "low"

    if stage_name in {"02_features", "03_classified_regimes"}:
        pressure = "medium"

    if size_mb >= 250:
        pressure = "high"

    if rows is not None and rows >= 1_000_000:
        pressure = "high"

    if stage_name in {"02_features", "03_classified_regimes"} and size_mb >= 100:
        pressure = "high"

    return pressure


def generate_recommendation(stage_name: str, size_mb: float, rows):
    if stage_name == "02_features":
        return (
            "High priority: consider incremental feature updates, rolling-window recalculation, "
            "feature caching, and partitioning by symbol/timeframe."
        )

    if stage_name == "03_classified_regimes":
        return (
            "High priority: classify only new/changed rows where possible. "
            "Consider storing latest processed timestamp per symbol/timeframe."
        )

    if stage_name == "04_recent_refresh":
        return (
            "Check whether recent refresh outputs are small and fast. "
            "These should remain lightweight compared with full historical outputs."
        )

    if stage_name in {"05_router", "08_router_validation"}:
        return (
            "Review freshness and downstream dependency timing. "
            "Router outputs should depend on latest regime classifications only."
        )

    if size_mb >= 250:
        return "Large file detected: consider partitioning, compression review, or DuckDB/Polars query layer."

    if rows is not None and rows >= 1_000_000:
        return "Large row count detected: consider partitioned storage and incremental appends."

    return "No immediate optimisation concern detected."


# ============================================================
# AUDIT LOGIC
# ============================================================

def audit_folder(stage_name: str, folder: Path):
    records = []

    if not folder.exists():
        records.append({
            "stage": stage_name,
            "folder": str(folder),
            "file_name": None,
            "file_path": None,
            "extension": None,
            "size_mb": 0,
            "rows": None,
            "modified_time": None,
            "pressure": "missing",
            "recommendation": "Folder missing. Check whether this stage exists or output path has changed.",
        })
        return records

    files = [p for p in folder.rglob("*") if p.is_file()]

    if not files:
        records.append({
            "stage": stage_name,
            "folder": str(folder),
            "file_name": None,
            "file_path": None,
            "extension": None,
            "size_mb": 0,
            "rows": None,
            "modified_time": None,
            "pressure": "empty",
            "recommendation": "Folder exists but contains no files.",
        })
        return records

    for path in files:
        ext = path.suffix.lower()
        size_mb = bytes_to_mb(path.stat().st_size)
        rows = None

        if ext in PARQUET_EXTENSIONS:
            rows = try_count_parquet_rows(path)
        elif ext in CSV_EXTENSIONS:
            rows = try_count_csv_rows(path)

        pressure = classify_stage_pressure(stage_name, size_mb, rows)
        recommendation = generate_recommendation(stage_name, size_mb, rows)

        records.append({
            "stage": stage_name,
            "folder": str(folder),
            "file_name": path.name,
            "file_path": str(path),
            "extension": ext,
            "size_mb": size_mb,
            "rows": rows,
            "modified_time": safe_get_mtime(path),
            "pressure": pressure,
            "recommendation": recommendation,
        })

    return records


def build_stage_summary(df: pd.DataFrame) -> pd.DataFrame:
    valid = df[df["file_name"].notna()].copy()

    if valid.empty:
        return pd.DataFrame()

    summary = (
        valid.groupby("stage", dropna=False)
        .agg(
            file_count=("file_name", "count"),
            total_size_mb=("size_mb", "sum"),
            max_file_size_mb=("size_mb", "max"),
            total_rows=("rows", "sum"),
            latest_modified=("modified_time", "max"),
        )
        .reset_index()
    )

    summary["total_size_mb"] = summary["total_size_mb"].round(3)
    summary["max_file_size_mb"] = summary["max_file_size_mb"].round(3)

    summary["optimisation_priority"] = summary.apply(
        lambda row: "high"
        if row["stage"] in {"02_features", "03_classified_regimes"} or row["total_size_mb"] >= 500
        else "medium"
        if row["total_size_mb"] >= 100
        else "low",
        axis=1,
    )

    return summary.sort_values(
        by=["optimisation_priority", "total_size_mb"],
        ascending=[True, False],
    )


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    all_records = []

    print("=" * 90)
    print("BACQE REGIME ENGINE OPTIMISATION AUDIT")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Regime output dir: {REGIME_OUTPUT_DIR}")
    print(f"Report dir: {REPORT_DIR}")
    print("-" * 90)

    for stage_name, folder in WATCH_FOLDERS.items():
        print(f"[AUDIT] {stage_name}: {folder}")
        stage_records = audit_folder(stage_name, folder)
        all_records.extend(stage_records)

    audit_df = pd.DataFrame(all_records)
    summary_df = build_stage_summary(audit_df)

    audit_csv = REPORT_DIR / f"regime_optimisation_audit_{timestamp}.csv"
    summary_csv = REPORT_DIR / f"regime_optimisation_summary_{timestamp}.csv"
    audit_json = REPORT_DIR / f"regime_optimisation_audit_{timestamp}.json"

    audit_df.to_csv(audit_csv, index=False)

    if not summary_df.empty:
        summary_df.to_csv(summary_csv, index=False)

    report_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(PROJECT_ROOT),
        "regime_output_dir": str(REGIME_OUTPUT_DIR),
        "watch_folders": {k: str(v) for k, v in WATCH_FOLDERS.items()},
        "audit_csv": str(audit_csv),
        "summary_csv": str(summary_csv),
        "records": audit_df.to_dict(orient="records"),
        "summary": summary_df.to_dict(orient="records") if not summary_df.empty else [],
        "next_recommended_step": (
            "Use this audit to choose the first optimisation target. "
            "Likely candidates are 02_features and 03_classified_regimes. "
            "Recommended next build: metadata-driven incremental update ledger."
        ),
    }

    with audit_json.open("w", encoding="utf-8") as f:
        json.dump(report_payload, f, indent=4)

    print("-" * 90)
    print("[DONE] Optimisation audit complete.")
    print(f"Audit CSV:   {audit_csv}")
    print(f"Summary CSV: {summary_csv}")
    print(f"Audit JSON:  {audit_json}")

    if not summary_df.empty:
        print("\nStage Summary:")
        print(summary_df.to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()