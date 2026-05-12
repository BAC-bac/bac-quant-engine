"""
BACQE Script 30
Incremental Append Dry-Run Planner

Purpose:
- Read Script 29 append readiness candidates
- If no candidates exist, report GREEN / no action
- If candidates exist, inspect full vs recent files
- Identify rows newer than full max timestamp
- Check duplicate/overlap risk
- Produce an execution-ready dry-run append plan

This script is read-only.
It does NOT append, overwrite, or modify parquet files.
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

READINESS_DIR = LEDGER_DIR / "incremental_append_readiness"
READINESS_CANDIDATES = READINESS_DIR / "incremental_append_readiness_candidates_latest.csv"

OUTPUT_DIR = LEDGER_DIR / "incremental_append_dry_run_plans"

TIMESTAMP_CANDIDATES = [
    "timestamp",
    "time",
    "datetime",
    "date",
    "bar_time",
    "event_dt",
    "open_time",
]


# ============================================================
# HELPERS
# ============================================================

def path_exists(value) -> bool:
    if pd.isna(value):
        return False
    text = str(value).strip()
    if not text:
        return False
    return Path(text).exists()


def find_timestamp_column(columns: list[str]) -> str | None:
    for col in TIMESTAMP_CANDIDATES:
        if col in columns:
            return col
    return None


def read_parquet_for_append_check(path_value: str) -> dict:
    result = {
        "exists": False,
        "rows": None,
        "size_mb": None,
        "columns": [],
        "timestamp_column": None,
        "min_timestamp": None,
        "max_timestamp": None,
        "df": None,
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

        timestamp_col = find_timestamp_column(result["columns"])

        if timestamp_col:
            df = pd.read_parquet(path)
            converted = pd.to_datetime(df[timestamp_col], errors="coerce", utc=True)
            df["_bacqe_timestamp_check"] = converted

            result["timestamp_column"] = timestamp_col

            if converted.notna().any():
                result["min_timestamp"] = converted.min()
                result["max_timestamp"] = converted.max()

            result["df"] = df

        else:
            df = pd.read_parquet(path)

            if isinstance(df.index, pd.DatetimeIndex):
                df = df.copy()
                df["_bacqe_timestamp_check"] = pd.to_datetime(df.index, errors="coerce", utc=True)

                result["timestamp_column"] = "index"

                if df["_bacqe_timestamp_check"].notna().any():
                    result["min_timestamp"] = df["_bacqe_timestamp_check"].min()
                    result["max_timestamp"] = df["_bacqe_timestamp_check"].max()

                result["df"] = df
            else:
                result["df"] = df

    except Exception as exc:
        result["error"] = str(exc)

    return result


def classify_plan_row(full_info: dict, recent_info: dict) -> tuple[str, str]:
    issues = []

    if not full_info["exists"]:
        issues.append("full_missing")

    if not recent_info["exists"]:
        issues.append("recent_missing")

    if full_info["error"]:
        issues.append("full_read_error")

    if recent_info["error"]:
        issues.append("recent_read_error")

    if full_info["columns"] != recent_info["columns"]:
        issues.append("schema_mismatch")

    if full_info["timestamp_column"] != recent_info["timestamp_column"]:
        issues.append("timestamp_column_mismatch")

    if full_info["max_timestamp"] is None:
        issues.append("full_missing_max_timestamp")

    if recent_info["max_timestamp"] is None:
        issues.append("recent_missing_max_timestamp")

    if issues:
        return "blocked", "|".join(issues)

    if recent_info["max_timestamp"] <= full_info["max_timestamp"]:
        return "no_action", "recent_not_newer_than_full"

    return "append_ready", "recent_contains_newer_rows"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE INCREMENTAL APPEND DRY-RUN PLANNER")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Candidates:   {READINESS_CANDIDATES}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    if not READINESS_CANDIDATES.exists():
        raise FileNotFoundError(f"Missing readiness candidates file: {READINESS_CANDIDATES}")

    candidates = pd.read_csv(READINESS_CANDIDATES)

    print(f"Readiness candidate rows loaded: {len(candidates)}")

    records = []

    if candidates.empty:
        status_payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "overall_status": "GREEN",
            "candidate_rows": 0,
            "append_ready": 0,
            "blocked": 0,
            "no_action": 0,
            "message": "No append candidates exist. No dry-run append plan required.",
            "next_recommended_step": (
                "Continue running Script 28 and Script 29 after recent refreshes. "
                "Only run an append executor when Script 30 produces append_ready rows."
            ),
        }

        empty_plan = pd.DataFrame()
        empty_summary = pd.DataFrame([status_payload])

    else:
        for _, row in candidates.iterrows():
            full_path = row.get("full_path")
            recent_path = row.get("recent_path")

            full_info = read_parquet_for_append_check(full_path)
            recent_info = read_parquet_for_append_check(recent_path)

            plan_status, plan_reason = classify_plan_row(full_info, recent_info)

            new_rows_count = 0
            overlap_rows_count = None
            duplicate_timestamp_count = None
            new_min_timestamp = None
            new_max_timestamp = None

            if plan_status == "append_ready":
                recent_df = recent_info["df"]
                full_max_ts = full_info["max_timestamp"]

                new_rows = recent_df[recent_df["_bacqe_timestamp_check"] > full_max_ts].copy()
                overlap_rows = recent_df[recent_df["_bacqe_timestamp_check"] <= full_max_ts].copy()

                new_rows_count = len(new_rows)
                overlap_rows_count = len(overlap_rows)
                duplicate_timestamp_count = int(
                    new_rows["_bacqe_timestamp_check"].duplicated().sum()
                )

                if new_rows_count > 0:
                    new_min_timestamp = new_rows["_bacqe_timestamp_check"].min().isoformat()
                    new_max_timestamp = new_rows["_bacqe_timestamp_check"].max().isoformat()

                if new_rows_count <= 0:
                    plan_status = "no_action"
                    plan_reason = "recent_newer_timestamp_detected_but_no_new_rows_after_filter"

                if duplicate_timestamp_count > 0:
                    plan_status = "blocked"
                    plan_reason = "duplicate_timestamps_in_new_rows"

            record = {
                "plan_type": row.get("plan_type"),
                "broker": row.get("broker"),
                "timeframe": row.get("timeframe"),
                "symbol": row.get("symbol"),
                "plan_status": plan_status,
                "plan_reason": plan_reason,
                "full_path": full_path,
                "recent_path": recent_path,
                "full_exists": full_info["exists"],
                "recent_exists": recent_info["exists"],
                "schema_match": full_info["columns"] == recent_info["columns"],
                "timestamp_column_match": full_info["timestamp_column"] == recent_info["timestamp_column"],
                "timestamp_column": full_info["timestamp_column"],
                "full_rows": full_info["rows"],
                "recent_rows": recent_info["rows"],
                "full_size_mb": full_info["size_mb"],
                "recent_size_mb": recent_info["size_mb"],
                "full_min_timestamp": full_info["min_timestamp"].isoformat() if full_info["min_timestamp"] is not None else None,
                "full_max_timestamp": full_info["max_timestamp"].isoformat() if full_info["max_timestamp"] is not None else None,
                "recent_min_timestamp": recent_info["min_timestamp"].isoformat() if recent_info["min_timestamp"] is not None else None,
                "recent_max_timestamp": recent_info["max_timestamp"].isoformat() if recent_info["max_timestamp"] is not None else None,
                "new_rows_count": new_rows_count,
                "overlap_rows_count": overlap_rows_count,
                "duplicate_timestamp_count": duplicate_timestamp_count,
                "new_min_timestamp": new_min_timestamp,
                "new_max_timestamp": new_max_timestamp,
                "full_error": full_info["error"],
                "recent_error": recent_info["error"],
                "full_column_count": len(full_info["columns"]),
                "recent_column_count": len(recent_info["columns"]),
                "full_columns": "|".join(full_info["columns"]),
                "recent_columns": "|".join(recent_info["columns"]),
            }

            records.append(record)

        empty_plan = pd.DataFrame(records)

        empty_summary = (
            empty_plan.groupby(
                ["plan_type", "broker", "timeframe", "plan_status", "plan_reason"],
                dropna=False,
            )
            .agg(
                candidates=("symbol", "count"),
                total_new_rows=("new_rows_count", "sum"),
                total_overlap_rows=("overlap_rows_count", "sum"),
                max_full_rows=("full_rows", "max"),
                max_recent_rows=("recent_rows", "max"),
            )
            .reset_index()
            .sort_values(["plan_status", "timeframe", "candidates"], ascending=[True, True, False])
        )

        overall_status = "RED" if empty_plan["plan_status"].eq("blocked").any() else (
            "AMBER" if empty_plan["plan_status"].eq("append_ready").any() else "GREEN"
        )

        status_payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "overall_status": overall_status,
            "candidate_rows": int(len(empty_plan)),
            "append_ready": int(empty_plan["plan_status"].eq("append_ready").sum()),
            "blocked": int(empty_plan["plan_status"].eq("blocked").sum()),
            "no_action": int(empty_plan["plan_status"].eq("no_action").sum()),
            "total_new_rows": int(pd.to_numeric(empty_plan["new_rows_count"], errors="coerce").fillna(0).sum()),
            "message": (
                "Append-ready rows exist; review plan before executor."
                if overall_status == "AMBER"
                else "Blocked rows exist; investigate before executor."
                if overall_status == "RED"
                else "No actionable append rows."
            ),
            "next_recommended_step": (
                "If AMBER and no blocked rows, build Script 31 guarded append executor. "
                "If GREEN, continue monitoring after future refreshes. "
                "If RED, inspect blocked rows."
            ),
        }

    plan_latest = OUTPUT_DIR / "incremental_append_dry_run_plan_latest.csv"
    plan_ts = OUTPUT_DIR / f"incremental_append_dry_run_plan_{run_ts}.csv"

    summary_latest = OUTPUT_DIR / "incremental_append_dry_run_summary_latest.csv"
    summary_ts = OUTPUT_DIR / f"incremental_append_dry_run_summary_{run_ts}.csv"

    append_ready_latest = OUTPUT_DIR / "incremental_append_dry_run_append_ready_latest.csv"
    append_ready_ts = OUTPUT_DIR / f"incremental_append_dry_run_append_ready_{run_ts}.csv"

    blocked_latest = OUTPUT_DIR / "incremental_append_dry_run_blocked_latest.csv"
    blocked_ts = OUTPUT_DIR / f"incremental_append_dry_run_blocked_{run_ts}.csv"

    status_latest = OUTPUT_DIR / "incremental_append_dry_run_status_latest.json"
    status_ts = OUTPUT_DIR / f"incremental_append_dry_run_status_{run_ts}.json"

    empty_plan.to_csv(plan_latest, index=False)
    empty_plan.to_csv(plan_ts, index=False)

    empty_summary.to_csv(summary_latest, index=False)
    empty_summary.to_csv(summary_ts, index=False)

    if empty_plan.empty:
        append_ready = pd.DataFrame()
        blocked = pd.DataFrame()
    else:
        append_ready = empty_plan[empty_plan["plan_status"].eq("append_ready")].copy()
        blocked = empty_plan[empty_plan["plan_status"].eq("blocked")].copy()

    append_ready.to_csv(append_ready_latest, index=False)
    append_ready.to_csv(append_ready_ts, index=False)

    blocked.to_csv(blocked_latest, index=False)
    blocked.to_csv(blocked_ts, index=False)

    with status_latest.open("w", encoding="utf-8") as f:
        json.dump(status_payload, f, indent=4)

    with status_ts.open("w", encoding="utf-8") as f:
        json.dump(status_payload, f, indent=4)

    print("-" * 90)
    print("[DONE] Incremental append dry-run planner complete.")
    print(f"Overall status: {status_payload['overall_status']}")
    print(f"Candidate rows: {status_payload['candidate_rows']}")
    print(f"Append ready:   {status_payload['append_ready']}")
    print(f"Blocked:        {status_payload['blocked']}")
    print(f"No action:      {status_payload['no_action']}")
    print("-" * 90)
    print(f"Plan latest:         {plan_latest}")
    print(f"Summary latest:      {summary_latest}")
    print(f"Append-ready latest: {append_ready_latest}")
    print(f"Blocked latest:      {blocked_latest}")
    print(f"Status JSON:         {status_latest}")
    print("=" * 90)


if __name__ == "__main__":
    main()