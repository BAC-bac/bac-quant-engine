"""
BACQE Script 26
Guarded Medium-Risk Missing Base Executor

Purpose:
- Execute Script 25 approved medium-risk missing base candidates
- Default mode is DRY_RUN = True
- Refuses overwrites
- Writes detailed execution logs

This script can write to the data lake only when DRY_RUN = False.
"""

from pathlib import Path
from datetime import datetime
import json
import shutil
import pandas as pd


# ============================================================
# SAFETY CONFIG
# ============================================================

DRY_RUN = True

EXECUTE_MEDIUM_RISK = True
NO_OVERWRITE = True

REQUIRE_SOURCE_EXISTS = True
REQUIRE_DESTINATION_NOT_EXISTS = True

ALLOWED_CREATION_ACTIONS = {
    "dry_run_create_full_base_from_recent",
}


# ============================================================
# PATH CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"

REVIEW_DIR = LEDGER_DIR / "medium_risk_final_review"
APPROVED_LATEST = REVIEW_DIR / "medium_risk_approved_execution_latest.csv"

EXECUTION_LOG_DIR = LEDGER_DIR / "medium_risk_execution_logs"


# ============================================================
# HELPERS
# ============================================================

def as_bool(value) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def validate_candidate(row) -> tuple[bool, list[str]]:
    issues = []

    source = Path(str(row.get("source_recent_path", "")).strip())
    destination = Path(str(row.get("destination_full_path", "")).strip())

    if not as_bool(row.get("approved_for_execution", False)):
        issues.append("not_approved_for_execution")

    if str(row.get("risk_level", "")).lower() != "medium":
        issues.append("not_medium_risk")

    if str(row.get("creation_action", "")) not in ALLOWED_CREATION_ACTIONS:
        issues.append("creation_action_not_allowed")

    if REQUIRE_SOURCE_EXISTS and not source.exists():
        issues.append("source_missing")

    if REQUIRE_DESTINATION_NOT_EXISTS and destination.exists():
        issues.append("destination_already_exists")

    if NO_OVERWRITE and destination.exists():
        issues.append("overwrite_refused")

    if source.suffix.lower() != ".parquet":
        issues.append("source_not_parquet")

    if destination.suffix.lower() != ".parquet":
        issues.append("destination_not_parquet")

    if not EXECUTE_MEDIUM_RISK:
        issues.append("medium_risk_execution_disabled")

    return len(issues) == 0, issues


def copy_file(source: Path, destination: Path):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def process_candidate(row) -> dict:
    source = Path(str(row.get("source_recent_path", "")).strip())
    destination = Path(str(row.get("destination_full_path", "")).strip())

    valid, issues = validate_candidate(row)

    result = {
        "execution_time": datetime.now().isoformat(timespec="seconds"),
        "dry_run": DRY_RUN,
        "plan_type": row.get("plan_type"),
        "broker": row.get("broker"),
        "timeframe": row.get("timeframe"),
        "symbol": row.get("symbol"),
        "risk_level": row.get("risk_level"),
        "creation_action": row.get("creation_action"),
        "source_recent_path": str(source),
        "destination_full_path": str(destination),
        "source_exists_before": source.exists(),
        "destination_exists_before": destination.exists(),
        "valid": valid,
        "validation_issues": "|".join(issues),
        "executed": False,
        "status": None,
        "error": None,
    }

    if not valid:
        result["status"] = "skipped_validation_failed"
        return result

    if DRY_RUN:
        result["status"] = "dry_run_would_copy"
        return result

    try:
        copy_file(source, destination)
        result["executed"] = True
        result["status"] = "copied"
        result["destination_exists_after"] = destination.exists()
        result["destination_size_bytes_after"] = destination.stat().st_size if destination.exists() else None
    except Exception as exc:
        result["status"] = "error"
        result["error"] = str(exc)
        result["destination_exists_after"] = destination.exists()
        result["destination_size_bytes_after"] = destination.stat().st_size if destination.exists() else None

    return result


def main():
    EXECUTION_LOG_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE GUARDED MEDIUM-RISK MISSING BASE EXECUTOR")
    print("=" * 90)
    print(f"Project root:       {PROJECT_ROOT}")
    print(f"Approved plan:      {APPROVED_LATEST}")
    print(f"Execution log dir:  {EXECUTION_LOG_DIR}")
    print("-" * 90)
    print("Safety config:")
    print(f"DRY_RUN:             {DRY_RUN}")
    print(f"EXECUTE_MEDIUM_RISK: {EXECUTE_MEDIUM_RISK}")
    print(f"NO_OVERWRITE:        {NO_OVERWRITE}")
    print("-" * 90)

    if not APPROVED_LATEST.exists():
        raise FileNotFoundError(f"Approved medium-risk plan not found: {APPROVED_LATEST}")

    df = pd.read_csv(APPROVED_LATEST)

    print(f"Approved medium-risk rows loaded: {len(df)}")

    results = [process_candidate(row) for _, row in df.iterrows()]
    results_df = pd.DataFrame(results)

    log_latest = EXECUTION_LOG_DIR / "medium_risk_execution_log_latest.csv"
    log_ts = EXECUTION_LOG_DIR / f"medium_risk_execution_log_{run_ts}.csv"

    summary_latest = EXECUTION_LOG_DIR / "medium_risk_execution_summary_latest.csv"
    summary_ts = EXECUTION_LOG_DIR / f"medium_risk_execution_summary_{run_ts}.csv"

    json_path = EXECUTION_LOG_DIR / f"medium_risk_execution_log_{run_ts}.json"

    results_df.to_csv(log_latest, index=False)
    results_df.to_csv(log_ts, index=False)

    summary = (
        results_df.groupby(
            ["dry_run", "risk_level", "status", "validation_issues"],
            dropna=False,
        )
        .agg(
            candidates=("symbol", "count"),
            executed=("executed", "sum"),
        )
        .reset_index()
        .sort_values(["status", "risk_level", "candidates"], ascending=[True, True, False])
    )

    summary.to_csv(summary_latest, index=False)
    summary.to_csv(summary_ts, index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "dry_run": DRY_RUN,
        "execute_medium_risk": EXECUTE_MEDIUM_RISK,
        "no_overwrite": NO_OVERWRITE,
        "approved_plan": str(APPROVED_LATEST),
        "log_latest": str(log_latest),
        "summary_latest": str(summary_latest),
        "total_candidates": int(len(results_df)),
        "would_copy": int(results_df["status"].eq("dry_run_would_copy").sum()),
        "copied": int(results_df["status"].eq("copied").sum()),
        "skipped": int(results_df["status"].astype(str).str.startswith("skipped").sum()),
        "errors": int(results_df["status"].eq("error").sum()),
        "next_recommended_step": (
            "If dry-run shows 262 would-copy rows and no validation failures, set DRY_RUN=False "
            "to execute. Then reset DRY_RUN=True and rerun Scripts 16 and 17."
        ),
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Guarded medium-risk executor complete.")
    print(f"Execution log latest:     {log_latest}")
    print(f"Execution summary latest: {summary_latest}")
    print(f"JSON report:              {json_path}")

    print("-" * 90)
    print("Status counts:")
    print(results_df["status"].value_counts(dropna=False).to_string())

    print("\nSummary preview:")
    print(summary.to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()