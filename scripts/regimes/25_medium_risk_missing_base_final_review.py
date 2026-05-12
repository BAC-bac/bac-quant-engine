"""
BACQE Script 25
Medium-Risk Missing Base Final Review

Purpose:
- Review remaining medium-risk missing full base candidates
- Exclude already executed low-risk/promoted cases
- Validate source exists, destination absent, schema readable, row counts sensible
- Produce approved and blocked execution plans

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"
BASE_CREATION_DIR = LEDGER_DIR / "base_creation_plans"

CREATION_PLAN_LATEST = BASE_CREATION_DIR / "missing_base_creation_plan_latest.csv"

OUTPUT_DIR = LEDGER_DIR / "medium_risk_final_review"


def path_exists(value) -> bool:
    if pd.isna(value):
        return False
    text = str(value).strip()
    if not text:
        return False
    return Path(text).exists()


def inspect_parquet(path_value) -> dict:
    result = {
        "exists": False,
        "rows": None,
        "size_mb": None,
        "columns": [],
        "latest_timestamp": None,
        "timestamp_column": None,
        "error": None,
    }

    if not path_exists(path_value):
        return result

    path = Path(str(path_value))
    result["exists"] = True
    result["size_mb"] = round(path.stat().st_size / (1024 * 1024), 4)

    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(path)
        result["rows"] = pf.metadata.num_rows
        result["columns"] = pf.schema.names

        df = pd.read_parquet(path)

        datetime_candidates = [
            "timestamp",
            "time",
            "datetime",
            "date",
            "bar_time",
            "event_dt",
            "open_time",
        ]

        for col in datetime_candidates:
            if col in df.columns:
                converted = pd.to_datetime(df[col], errors="coerce", utc=True)
                if converted.notna().any():
                    result["latest_timestamp"] = converted.max().isoformat()
                    result["timestamp_column"] = col
                    break

        if result["latest_timestamp"] is None and isinstance(df.index, pd.DatetimeIndex):
            result["latest_timestamp"] = df.index.max().isoformat()
            result["timestamp_column"] = "index"

    except Exception as exc:
        result["error"] = str(exc)

    return result


def final_review_status(row) -> tuple[str, str]:
    issues = []

    if not row["source_exists_now"]:
        issues.append("source_missing")

    if row["destination_exists_now"]:
        issues.append("destination_already_exists")

    if not row["source_is_parquet"]:
        issues.append("source_not_parquet")

    if not row["destination_is_parquet"]:
        issues.append("destination_not_parquet")

    if pd.isna(row["source_rows_checked"]) or row["source_rows_checked"] <= 0:
        issues.append("source_has_no_rows")

    if not row["source_columns_checked"]:
        issues.append("source_schema_unreadable")

    if row["source_inspection_error"]:
        issues.append("source_inspection_error")

    if not row["destination_folder_exists_now"]:
        issues.append("destination_folder_missing")

    if issues:
        return "blocked", "|".join(issues)

    return "approved", "passed_final_review"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE MEDIUM-RISK MISSING BASE FINAL REVIEW")
    print("=" * 90)
    print(f"Project root:  {PROJECT_ROOT}")
    print(f"Creation plan: {CREATION_PLAN_LATEST}")
    print(f"Output dir:    {OUTPUT_DIR}")
    print("-" * 90)

    if not CREATION_PLAN_LATEST.exists():
        raise FileNotFoundError(f"Missing creation plan: {CREATION_PLAN_LATEST}")

    plan = pd.read_csv(CREATION_PLAN_LATEST)

    review_df = plan[
        plan["approved_for_future_execution"].astype(str).str.lower().eq("true")
        & plan["risk_level"].astype(str).str.lower().eq("medium")
    ].copy()

    print(f"Creation plan rows loaded: {len(plan)}")
    print(f"Medium-risk approved candidates loaded: {len(review_df)}")

    if review_df.empty:
        print("[DONE] No medium-risk candidates to review.")
        return

    review_df["source_exists_now"] = review_df["source_recent_path"].apply(path_exists)
    review_df["destination_exists_now"] = review_df["destination_full_path"].apply(path_exists)
    review_df["destination_folder_exists_now"] = review_df["destination_folder"].apply(path_exists)

    review_df["source_is_parquet"] = review_df["source_recent_path"].astype(str).str.lower().str.endswith(".parquet")
    review_df["destination_is_parquet"] = review_df["destination_full_path"].astype(str).str.lower().str.endswith(".parquet")

    inspections = review_df["source_recent_path"].apply(inspect_parquet)

    review_df["source_rows_checked"] = inspections.apply(lambda x: x["rows"])
    review_df["source_size_mb_checked"] = inspections.apply(lambda x: x["size_mb"])
    review_df["source_columns_checked"] = inspections.apply(lambda x: "|".join(x["columns"]))
    review_df["source_column_count"] = inspections.apply(lambda x: len(x["columns"]))
    review_df["source_latest_timestamp_checked"] = inspections.apply(lambda x: x["latest_timestamp"])
    review_df["source_timestamp_column_checked"] = inspections.apply(lambda x: x["timestamp_column"])
    review_df["source_inspection_error"] = inspections.apply(lambda x: x["error"])

    statuses = review_df.apply(final_review_status, axis=1)
    review_df["final_review_status"] = statuses.apply(lambda x: x[0])
    review_df["final_review_reason"] = statuses.apply(lambda x: x[1])

    review_df["execution_plan_source"] = "script_25_medium_risk_final_review"
    review_df["approved_for_execution"] = review_df["final_review_status"].eq("approved")

    output_cols = [
        "plan_type",
        "broker",
        "timeframe",
        "symbol",
        "risk_level",
        "creation_action",
        "approved_for_execution",
        "final_review_status",
        "final_review_reason",
        "source_exists_now",
        "destination_exists_now",
        "destination_folder_exists_now",
        "source_is_parquet",
        "destination_is_parquet",
        "source_rows_checked",
        "source_size_mb_checked",
        "source_column_count",
        "source_latest_timestamp_checked",
        "source_timestamp_column_checked",
        "source_recent_path",
        "destination_full_path",
        "destination_folder",
        "planner_recommendation",
        "execution_plan_source",
        "source_columns_checked",
        "source_inspection_error",
    ]

    final_review = review_df[output_cols].sort_values(
        by=["final_review_status", "broker", "timeframe", "symbol", "plan_type"],
        ascending=True,
    )

    approved = final_review[final_review["approved_for_execution"].eq(True)].copy()
    blocked = final_review[final_review["approved_for_execution"].eq(False)].copy()

    summary = (
        final_review.groupby(
            ["plan_type", "broker", "timeframe", "final_review_status", "final_review_reason"],
            dropna=False,
        )
        .agg(
            candidates=("symbol", "count"),
            total_rows=("source_rows_checked", "sum"),
            total_size_mb=("source_size_mb_checked", "sum"),
        )
        .reset_index()
        .sort_values(["final_review_status", "timeframe", "candidates"], ascending=[True, True, False])
    )

    summary["total_size_mb"] = summary["total_size_mb"].round(4)

    final_review_latest = OUTPUT_DIR / "medium_risk_final_review_latest.csv"
    final_review_ts = OUTPUT_DIR / f"medium_risk_final_review_{run_ts}.csv"

    approved_latest = OUTPUT_DIR / "medium_risk_approved_execution_latest.csv"
    approved_ts = OUTPUT_DIR / f"medium_risk_approved_execution_{run_ts}.csv"

    blocked_latest = OUTPUT_DIR / "medium_risk_blocked_latest.csv"
    blocked_ts = OUTPUT_DIR / f"medium_risk_blocked_{run_ts}.csv"

    summary_latest = OUTPUT_DIR / "medium_risk_final_review_summary_latest.csv"
    summary_ts = OUTPUT_DIR / f"medium_risk_final_review_summary_{run_ts}.csv"

    json_path = OUTPUT_DIR / f"medium_risk_final_review_{run_ts}.json"

    final_review.to_csv(final_review_latest, index=False)
    final_review.to_csv(final_review_ts, index=False)

    approved.to_csv(approved_latest, index=False)
    approved.to_csv(approved_ts, index=False)

    blocked.to_csv(blocked_latest, index=False)
    blocked.to_csv(blocked_ts, index=False)

    summary.to_csv(summary_latest, index=False)
    summary.to_csv(summary_ts, index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "creation_plan": str(CREATION_PLAN_LATEST),
        "final_review_latest": str(final_review_latest),
        "approved_latest": str(approved_latest),
        "blocked_latest": str(blocked_latest),
        "summary_latest": str(summary_latest),
        "total_reviewed": int(len(final_review)),
        "approved": int(len(approved)),
        "blocked": int(len(blocked)),
        "next_recommended_step": (
            "If approved count is expected and blocked count is zero, build Script 26 as a guarded "
            "medium-risk executor with DRY_RUN=True by default and NO_OVERWRITE=True."
        ),
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Medium-risk final review complete.")
    print(f"Final review latest: {final_review_latest}")
    print(f"Approved latest:     {approved_latest}")
    print(f"Blocked latest:      {blocked_latest}")
    print(f"Summary latest:      {summary_latest}")
    print(f"JSON report:         {json_path}")

    print("-" * 90)
    print(f"Total reviewed: {len(final_review)}")
    print(f"Approved:       {len(approved)}")
    print(f"Blocked:        {len(blocked)}")

    print("\nSummary preview:")
    print(summary.to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()