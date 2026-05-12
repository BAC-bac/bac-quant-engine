"""
BACQE Script 24
Promoted Candidate Guarded Executor

Purpose:
- Execute promoted naming-mismatch candidates from Script 23
- Default mode is DRY_RUN = True
- Refuses overwrites
- Keeps promoted execution separate from normal missing-base execution

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
EXECUTE_HIGH_RISK = False

NO_OVERWRITE = True
REQUIRE_SOURCE_EXISTS = True
REQUIRE_DESTINATION_NOT_EXISTS = True

ALLOWED_ACTIONS = {
    "dry_run_create_full_base_from_recent",
}


# ============================================================
# PATH CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"

PROMOTED_DIR = LEDGER_DIR / "promoted_naming_mismatch_candidates"
PROMOTED_EXECUTABLE_LATEST = PROMOTED_DIR / "promoted_naming_mismatch_executable_latest.csv"

EXECUTION_LOG_DIR = LEDGER_DIR / "promoted_naming_mismatch_execution_logs"


# ============================================================
# HELPERS
# ============================================================

def as_bool(value) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def is_candidate_allowed(row) -> tuple[bool, str]:
    if not as_bool(row.get("approved_for_future_execution", False)):
        return False, "not_approved_for_future_execution"

    action = str(row.get("creation_action", ""))

    if action not in ALLOWED_ACTIONS:
        return False, f"action_not_allowed: {action}"

    risk = str(row.get("risk_level", "")).lower()

    if risk == "medium" and EXECUTE_MEDIUM_RISK:
        return True, "allowed_medium_risk"

    if risk == "high" and EXECUTE_HIGH_RISK:
        return True, "allowed_high_risk"

    if risk == "medium" and not EXECUTE_MEDIUM_RISK:
        return False, "medium_risk_disabled"

    if risk == "high" and not EXECUTE_HIGH_RISK:
        return False, "high_risk_disabled"

    return False, f"unknown_or_disabled_risk: {risk}"


def validate_candidate(row) -> tuple[bool, list[str]]:
    issues = []

    source = Path(str(row.get("source_recent_path", "")).strip())
    destination = Path(str(row.get("destination_full_path", "")).strip())

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

    return len(issues) == 0, issues


def copy_file(source: Path, destination: Path):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def process_candidate(row) -> dict:
    source = Path(str(row.get("source_recent_path", "")).strip())
    destination = Path(str(row.get("destination_full_path", "")).strip())

    allowed, allow_reason = is_candidate_allowed(row)
    valid, validation_issues = validate_candidate(row)

    result = {
        "execution_time": datetime.now().isoformat(timespec="seconds"),
        "dry_run": DRY_RUN,
        "plan_type": row.get("plan_type"),
        "broker": row.get("broker"),
        "timeframe": row.get("timeframe"),
        "symbol": row.get("symbol"),
        "risk_level": row.get("risk_level"),
        "creation_action": row.get("creation_action"),
        "promotion_source": row.get("promotion_source"),
        "promotion_reason": row.get("promotion_reason"),
        "source_recent_path": str(source),
        "destination_full_path": str(destination),
        "source_exists_before": source.exists(),
        "destination_exists_before": destination.exists(),
        "allowed": allowed,
        "allow_reason": allow_reason,
        "valid": valid,
        "validation_issues": "|".join(validation_issues),
        "executed": False,
        "status": None,
        "error": None,
    }

    if not allowed:
        result["status"] = "skipped_not_allowed"
        return result

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
    print("BACQE PROMOTED CANDIDATE GUARDED EXECUTOR")
    print("=" * 90)
    print(f"Project root:      {PROJECT_ROOT}")
    print(f"Promoted plan:     {PROMOTED_EXECUTABLE_LATEST}")
    print(f"Execution log dir: {EXECUTION_LOG_DIR}")
    print("-" * 90)
    print("Safety config:")
    print(f"DRY_RUN:              {DRY_RUN}")
    print(f"EXECUTE_MEDIUM_RISK:  {EXECUTE_MEDIUM_RISK}")
    print(f"EXECUTE_HIGH_RISK:    {EXECUTE_HIGH_RISK}")
    print(f"NO_OVERWRITE:         {NO_OVERWRITE}")
    print("-" * 90)

    if not PROMOTED_EXECUTABLE_LATEST.exists():
        raise FileNotFoundError(f"Promoted executable plan not found: {PROMOTED_EXECUTABLE_LATEST}")

    df = pd.read_csv(PROMOTED_EXECUTABLE_LATEST)

    print(f"Promoted executable rows loaded: {len(df)}")

    results = [process_candidate(row) for _, row in df.iterrows()]
    results_df = pd.DataFrame(results)

    log_latest = EXECUTION_LOG_DIR / "promoted_candidate_execution_log_latest.csv"
    log_ts = EXECUTION_LOG_DIR / f"promoted_candidate_execution_log_{run_ts}.csv"

    summary_latest = EXECUTION_LOG_DIR / "promoted_candidate_execution_summary_latest.csv"
    summary_ts = EXECUTION_LOG_DIR / f"promoted_candidate_execution_summary_{run_ts}.csv"

    json_path = EXECUTION_LOG_DIR / f"promoted_candidate_execution_log_{run_ts}.json"

    results_df.to_csv(log_latest, index=False)
    results_df.to_csv(log_ts, index=False)

    summary = (
        results_df.groupby(
            ["dry_run", "risk_level", "status", "allow_reason"],
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
        "execute_high_risk": EXECUTE_HIGH_RISK,
        "no_overwrite": NO_OVERWRITE,
        "candidate_file": str(PROMOTED_EXECUTABLE_LATEST),
        "log_latest": str(log_latest),
        "summary_latest": str(summary_latest),
        "total_candidates": int(len(results_df)),
        "would_copy": int(results_df["status"].eq("dry_run_would_copy").sum()),
        "copied": int(results_df["status"].eq("copied").sum()),
        "skipped": int(results_df["status"].astype(str).str.startswith("skipped").sum()),
        "errors": int(results_df["status"].eq("error").sum()),
        "next_recommended_step": (
            "If dry-run shows 16 would-copy rows and no validation failures, set DRY_RUN=False "
            "to execute promoted candidates only. Then rerun Scripts 16 and 17 to refresh the state."
        ),
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Promoted candidate guarded executor complete.")
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