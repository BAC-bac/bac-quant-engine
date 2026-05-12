"""
BACQE Script 27
Incremental Sync Verification Report

Purpose:
- Verify the missing-base synchronisation phase is complete
- Confirm Script 17 updates_needed is zero
- Check execution logs and candidate reports
- Produce a final green/amber/red verification report

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"

CHANGE_SUMMARY = LEDGER_DIR / "regime_incremental_change_summary_latest.csv"
CHANGE_PLAN = LEDGER_DIR / "regime_incremental_change_plan_latest.csv"

BASE_EXEC_LOG = LEDGER_DIR / "base_creation_execution_logs" / "missing_base_execution_log_latest.csv"
PROMOTED_EXEC_LOG = LEDGER_DIR / "promoted_naming_mismatch_execution_logs" / "promoted_candidate_execution_log_latest.csv"
MEDIUM_EXEC_LOG = LEDGER_DIR / "medium_risk_execution_logs" / "medium_risk_execution_log_latest.csv"

QUARANTINE = LEDGER_DIR / "base_creation_plans" / "missing_base_creation_quarantine_latest.csv"
PROMOTED_EXECUTABLE = LEDGER_DIR / "promoted_naming_mismatch_candidates" / "promoted_naming_mismatch_executable_latest.csv"
MEDIUM_APPROVED = LEDGER_DIR / "medium_risk_final_review" / "medium_risk_approved_execution_latest.csv"
MEDIUM_BLOCKED = LEDGER_DIR / "medium_risk_final_review" / "medium_risk_blocked_latest.csv"

OUTPUT_DIR = LEDGER_DIR / "sync_verification"


def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def status_colour(condition: bool) -> str:
    return "green" if condition else "red"


def count_status(df: pd.DataFrame, column: str, value: str) -> int:
    if df.empty or column not in df.columns:
        return 0
    return int(df[column].astype(str).eq(value).sum())


def make_check(check_name: str, condition: bool, observed, expected, notes: str) -> dict:
    return {
        "check_name": check_name,
        "status": status_colour(condition),
        "passed": condition,
        "observed": observed,
        "expected": expected,
        "notes": notes,
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE INCREMENTAL SYNC VERIFICATION REPORT")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Ledger dir:   {LEDGER_DIR}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    change_summary = read_csv_safe(CHANGE_SUMMARY)
    change_plan = read_csv_safe(CHANGE_PLAN)

    base_log = read_csv_safe(BASE_EXEC_LOG)
    promoted_log = read_csv_safe(PROMOTED_EXEC_LOG)
    medium_log = read_csv_safe(MEDIUM_EXEC_LOG)

    quarantine = read_csv_safe(QUARANTINE)
    promoted_exec = read_csv_safe(PROMOTED_EXECUTABLE)
    medium_approved = read_csv_safe(MEDIUM_APPROVED)
    medium_blocked = read_csv_safe(MEDIUM_BLOCKED)

    checks = []

    total_updates_needed = 0
    if not change_summary.empty and "updates_needed" in change_summary.columns:
        total_updates_needed = int(pd.to_numeric(change_summary["updates_needed"], errors="coerce").fillna(0).sum())

    checks.append(make_check(
        check_name="change_summary_updates_needed_zero",
        condition=total_updates_needed == 0,
        observed=total_updates_needed,
        expected=0,
        notes="Script 17 latest summary should show no remaining incremental missing-base updates.",
    ))

    change_plan_updates = 0
    if not change_plan.empty and "update_needed" in change_plan.columns:
        change_plan_updates = int(change_plan["update_needed"].astype(str).str.lower().eq("true").sum())

    checks.append(make_check(
        check_name="change_plan_update_needed_rows_zero",
        condition=change_plan_updates == 0,
        observed=change_plan_updates,
        expected=0,
        notes="Script 17 latest change plan should contain no update_needed=True rows.",
    ))

    base_copied = count_status(base_log, "status", "copied")
    base_dry_run = count_status(base_log, "status", "dry_run_would_copy")

    checks.append(make_check(
        check_name="low_risk_base_execution_present",
        condition=(base_copied == 284 or base_dry_run == 284),
        observed=f"copied={base_copied}, dry_run_would_copy={base_dry_run}",
        expected="copied=284 after execution, or dry_run_would_copy=284 if latest log was reset dry-run",
        notes="Script 21 should have handled the 284 low-risk MN1 candidates.",
    ))

    promoted_copied = count_status(promoted_log, "status", "copied")
    promoted_dry_run = count_status(promoted_log, "status", "dry_run_would_copy")

    checks.append(make_check(
        check_name="promoted_execution_present",
        condition=(promoted_copied == 16 or promoted_dry_run == 16),
        observed=f"copied={promoted_copied}, dry_run_would_copy={promoted_dry_run}",
        expected="copied=16 after execution, or dry_run_would_copy=16 if latest log was reset dry-run",
        notes="Script 24 should have handled 16 promoted short-symbol candidates.",
    ))

    medium_copied = count_status(medium_log, "status", "copied")
    medium_dry_run = count_status(medium_log, "status", "dry_run_would_copy")

    checks.append(make_check(
        check_name="medium_risk_execution_present",
        condition=(medium_copied == 262 or medium_dry_run == 262),
        observed=f"copied={medium_copied}, dry_run_would_copy={medium_dry_run}",
        expected="copied=262 after execution, or dry_run_would_copy=262 if latest log was reset dry-run",
        notes="Script 26 should have handled the 262 medium-risk candidates.",
    ))

    medium_blocked_count = len(medium_blocked)

    checks.append(make_check(
        check_name="medium_risk_blocked_zero",
        condition=medium_blocked_count == 0,
        observed=medium_blocked_count,
        expected=0,
        notes="Script 25 should have no blocked medium-risk candidates.",
    ))

    promoted_count = len(promoted_exec)

    checks.append(make_check(
        check_name="promoted_candidate_count_expected",
        condition=promoted_count == 16,
        observed=promoted_count,
        expected=16,
        notes="Promoted short-symbol candidate plan should contain 16 rows.",
    ))

    quarantine_count = len(quarantine)

    checks.append(make_check(
        check_name="quarantine_reviewed_count_expected",
        condition=quarantine_count == 16,
        observed=quarantine_count,
        expected=16,
        notes="Original quarantine should contain 16 reviewed rows; this is retained as audit trail.",
    ))

    checks_df = pd.DataFrame(checks)

    if checks_df["status"].eq("red").any():
        overall_status = "red"
    else:
        overall_status = "green"

    overall = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_status": overall_status,
        "all_checks_passed": bool(checks_df["passed"].all()),
        "checks_total": int(len(checks_df)),
        "checks_passed": int(checks_df["passed"].sum()),
        "checks_failed": int((~checks_df["passed"]).sum()),
        "total_updates_needed": total_updates_needed,
        "change_plan_update_rows": change_plan_updates,
        "low_risk_base_copied": base_copied,
        "promoted_copied": promoted_copied,
        "medium_risk_copied": medium_copied,
        "next_recommended_step": (
            "If green, missing-base synchronisation is complete. "
            "Next phase: design true incremental append/caching optimisation."
        ),
    }

    report_latest = OUTPUT_DIR / "incremental_sync_verification_latest.csv"
    report_ts = OUTPUT_DIR / f"incremental_sync_verification_{run_ts}.csv"

    overall_latest = OUTPUT_DIR / "incremental_sync_verification_overall_latest.json"
    overall_ts = OUTPUT_DIR / f"incremental_sync_verification_overall_{run_ts}.json"

    checks_df.to_csv(report_latest, index=False)
    checks_df.to_csv(report_ts, index=False)

    with overall_latest.open("w", encoding="utf-8") as f:
        json.dump(overall, f, indent=4)

    with overall_ts.open("w", encoding="utf-8") as f:
        json.dump(overall, f, indent=4)

    print("[DONE] Incremental sync verification complete.")
    print(f"Overall status: {overall_status.upper()}")
    print(f"Checks passed:  {overall['checks_passed']} / {overall['checks_total']}")
    print(f"Report latest:  {report_latest}")
    print(f"Overall JSON:   {overall_latest}")
    print("-" * 90)
    print(checks_df.to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()