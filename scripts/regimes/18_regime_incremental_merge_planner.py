"""
BACQE Script 18
Regime Incremental Merge Planner

Purpose:
- Read Script 17 incremental change plan
- Inspect update candidates only
- Classify each candidate into safe merge/action categories
- Produce a dry-run merge plan

This script is read-only.
It does NOT modify parquet files.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"

CHANGE_PLAN_LATEST = LEDGER_DIR / "regime_incremental_change_plan_latest.csv"

OUTPUT_DIR = LEDGER_DIR / "merge_plans"


# ============================================================
# HELPERS
# ============================================================

def safe_datetime(value):
    return pd.to_datetime(value, errors="coerce", utc=True)


def file_exists(path_value) -> bool:
    if pd.isna(path_value) or not str(path_value).strip():
        return False
    return Path(str(path_value)).exists()


def get_file_size_mb(path_value):
    if not file_exists(path_value):
        return None
    return round(Path(str(path_value)).stat().st_size / (1024 * 1024), 4)


def inspect_columns(path_value):
    """
    Reads only parquet/csv headers where possible.
    """
    if not file_exists(path_value):
        return []

    path = Path(str(path_value))

    try:
        if path.suffix.lower() == ".parquet":
            import pyarrow.parquet as pq
            return pq.ParquetFile(path).schema.names

        if path.suffix.lower() == ".csv":
            return list(pd.read_csv(path, nrows=0).columns)

    except Exception:
        return []

    return []


def classify_candidate(row):
    full_path = row.get("example_file_path_full")
    recent_path = row.get("example_file_path_recent")

    full_exists = file_exists(full_path)
    recent_exists = file_exists(recent_path)

    full_ts = safe_datetime(row.get("latest_timestamp_full"))
    recent_ts = safe_datetime(row.get("latest_timestamp_recent"))

    full_rows = pd.to_numeric(row.get("total_rows_full"), errors="coerce")
    recent_rows = pd.to_numeric(row.get("total_rows_recent"), errors="coerce")

    timeframe = str(row.get("timeframe", "")).upper()

    if not full_exists and recent_exists:
        return "missing_full_create_candidate"

    if full_exists and not recent_exists:
        return "ignore_or_monitor"

    if not full_exists and not recent_exists:
        return "invalid_missing_both"

    if pd.isna(full_ts) or pd.isna(recent_ts):
        return "no_timestamp_overlap_check_needed"

    if recent_ts > full_ts:
        return "append_candidate"

    if recent_ts == full_ts:
        if timeframe == "MN1":
            return "missing_or_new_timeframe_review"
        if pd.notna(recent_rows) and recent_rows > 0:
            return "already_aligned_monitor"
        return "ignore_or_monitor"

    if recent_ts < full_ts:
        return "recent_layer_behind_ignore"

    return "manual_review"


def recommended_action(category):
    mapping = {
        "missing_full_create_candidate": (
            "Recent file exists but full base file is missing. Candidate for creating a new full base file "
            "after schema validation."
        ),
        "append_candidate": (
            "Recent layer has newer data than full base. Candidate for append/merge after overlap and duplicate checks."
        ),
        "no_timestamp_overlap_check_needed": (
            "Timestamp missing or unreadable. Needs schema/timestamp inspection before any merge."
        ),
        "missing_or_new_timeframe_review": (
            "Likely new timeframe or naming mismatch. Review before creating full base."
        ),
        "already_aligned_monitor": (
            "Recent and full timestamps match. No merge needed; keep monitoring."
        ),
        "recent_layer_behind_ignore": (
            "Recent layer is behind the full base. Ignore for merge."
        ),
        "ignore_or_monitor": (
            "No actionable recent update detected."
        ),
        "invalid_missing_both": (
            "Both paths appear missing. Investigate ledger/path issue."
        ),
        "manual_review": (
            "Unclassified case. Review manually."
        ),
    }

    return mapping.get(category, "Review manually.")


def risk_level(category):
    if category in {"already_aligned_monitor", "recent_layer_behind_ignore", "ignore_or_monitor"}:
        return "low"

    if category in {"missing_full_create_candidate", "append_candidate"}:
        return "medium"

    return "high"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE REGIME INCREMENTAL MERGE PLANNER")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Change plan:  {CHANGE_PLAN_LATEST}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    if not CHANGE_PLAN_LATEST.exists():
        raise FileNotFoundError(f"Missing change plan: {CHANGE_PLAN_LATEST}")

    df = pd.read_csv(CHANGE_PLAN_LATEST)

    update_df = df[df["update_needed"].astype(str).str.lower().eq("true")].copy()

    print(f"Change plan rows: {len(df)}")
    print(f"Update candidates: {len(update_df)}")

    if update_df.empty:
        print("[DONE] No update candidates found.")
        return

    update_df["merge_category"] = update_df.apply(classify_candidate, axis=1)
    update_df["risk_level"] = update_df["merge_category"].apply(risk_level)
    update_df["planner_recommendation"] = update_df["merge_category"].apply(recommended_action)

    update_df["full_file_exists"] = update_df["example_file_path_full"].apply(file_exists)
    update_df["recent_file_exists"] = update_df["example_file_path_recent"].apply(file_exists)

    update_df["full_file_size_mb_actual"] = update_df["example_file_path_full"].apply(get_file_size_mb)
    update_df["recent_file_size_mb_actual"] = update_df["example_file_path_recent"].apply(get_file_size_mb)

    update_df["full_columns"] = update_df["example_file_path_full"].apply(lambda p: "|".join(inspect_columns(p)))
    update_df["recent_columns"] = update_df["example_file_path_recent"].apply(lambda p: "|".join(inspect_columns(p)))
    update_df["schema_match"] = update_df["full_columns"].eq(update_df["recent_columns"])

    output_cols = [
        "plan_type",
        "broker",
        "timeframe",
        "symbol",
        "merge_category",
        "risk_level",
        "planner_recommendation",
        "schema_match",
        "latest_timestamp_full",
        "latest_timestamp_recent",
        "gap_hours",
        "total_rows_full",
        "total_rows_recent",
        "total_size_mb_full",
        "total_size_mb_recent",
        "full_file_exists",
        "recent_file_exists",
        "full_file_size_mb_actual",
        "recent_file_size_mb_actual",
        "example_file_path_full",
        "example_file_path_recent",
        "full_columns",
        "recent_columns",
    ]

    merge_plan = update_df[output_cols].sort_values(
        by=["risk_level", "merge_category", "broker", "timeframe", "symbol"],
        ascending=True,
    )

    summary = (
        merge_plan.groupby(["plan_type", "broker", "timeframe", "merge_category", "risk_level"], dropna=False)
        .agg(
            candidates=("symbol", "count"),
            schema_matches=("schema_match", "sum"),
            total_recent_rows=("total_rows_recent", "sum"),
            total_recent_size_mb=("total_size_mb_recent", "sum"),
        )
        .reset_index()
        .sort_values(by=["risk_level", "candidates"], ascending=[True, False])
    )

    summary["total_recent_size_mb"] = summary["total_recent_size_mb"].round(4)

    merge_plan_ts = OUTPUT_DIR / f"regime_incremental_merge_plan_{run_timestamp}.csv"
    merge_plan_latest = OUTPUT_DIR / "regime_incremental_merge_plan_latest.csv"

    summary_ts = OUTPUT_DIR / f"regime_incremental_merge_summary_{run_timestamp}.csv"
    summary_latest = OUTPUT_DIR / "regime_incremental_merge_summary_latest.csv"

    json_path = OUTPUT_DIR / f"regime_incremental_merge_plan_{run_timestamp}.json"

    merge_plan.to_csv(merge_plan_ts, index=False)
    merge_plan.to_csv(merge_plan_latest, index=False)

    summary.to_csv(summary_ts, index=False)
    summary.to_csv(summary_latest, index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "change_plan": str(CHANGE_PLAN_LATEST),
        "merge_plan_latest": str(merge_plan_latest),
        "summary_latest": str(summary_latest),
        "total_candidates": int(len(merge_plan)),
        "category_counts": merge_plan["merge_category"].value_counts().to_dict(),
        "risk_counts": merge_plan["risk_level"].value_counts().to_dict(),
        "next_recommended_step": (
            "Inspect merge summary. Only consider Script 19 executor for low/medium risk candidates "
            "after backups and schema validation."
        ),
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Dry-run merge plan created.")
    print(f"Merge plan latest: {merge_plan_latest}")
    print(f"Summary latest:    {summary_latest}")
    print(f"JSON report:       {json_path}")

    print("\nMerge category counts:")
    print(merge_plan["merge_category"].value_counts().to_string())

    print("\nRisk counts:")
    print(merge_plan["risk_level"].value_counts().to_string())

    print("\nSummary preview:")
    print(summary.head(40).to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()