"""
BACQE Script 21
Guarded Missing Base Executor

Purpose:
- Execute approved missing full base creation candidates from Script 20
- Copy recent parquet files into the full base layer
- Default mode is DRY_RUN = True
- Refuses overwrites
- Can restrict execution to low-risk candidates only

IMPORTANT:
This is the first script in this optimisation sequence that can write to the data lake.
Review configuration before changing DRY_RUN to False.
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

EXECUTE_LOW_RISK_ONLY = True
EXECUTE_MEDIUM_RISK = False
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
CREATION_PLAN_DIR = LEDGER_DIR / "base_creation_plans"

EXECUTABLE_CANDIDATES_LATEST = (
    CREATION_PLAN_DIR / "missing_base_creation_executable_candidates_latest.csv"
)

EXECUTION_LOG_DIR = LEDGER_DIR / "base_creation_execution_logs"


# ============================================================
# HELPERS
# ============================================================

def as_bool(value) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def path_exists(path_value) -> bool:
    if pd.isna(path_value):
        return False
    text = str(path_value).strip()
    if not text:
        return False
    return Path(text).exists()


def is_candidate_allowed(row) -> tuple[bool, str]:
    if not as_bool(row.get("approved_for_future_execution", False)):
        return False, "not_approved_for_future_execution"

    action = str(row.get("creation_action", ""))

    if action not in ALLOWED_ACTIONS:
        return False, f"action_not_allowed: {action}"

    risk = str(row.get("risk_level", "")).lower()

    if risk == "low" and EXECUTE_LOW_RISK_ONLY:
        return True, "allowed_low_risk"

    if risk == "medium" and EXECUTE_MEDIUM_RISK:
        return True, "allowed_medium_risk"

    if risk == "high" and EXECUTE_HIGH_RISK:
        return True, "allowed_high_risk"

    if risk == "low" and not EXECUTE_LOW_RISK_ONLY:
        return False, "low_risk_disabled"

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

    if not source.suffix.lower() == ".parquet":
        issues.append("source_not_parquet")

    if not destination.suffix.lower() == ".parquet":
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
    print("BACQE GUARDED MISSING BASE EXECUTOR")
    print("=" * 90)
    print(f"Project root:       {PROJECT_ROOT}")
    print(f"Executable plan:    {EXECUTABLE_CANDIDATES_LATEST}")
    print(f"Execution log dir:  {EXECUTION_LOG_DIR}")
    print("-" * 90)
    print("Safety config:")
    print(f"DRY_RUN: {DRY_RUN}")
    print(f"EXECUTE_LOW_RISK_ONLY: {EXECUTE_LOW_RISK_ONLY}")
    print(f"EXECUTE_MEDIUM_RISK:   {EXECUTE_MEDIUM_RISK}")
    print(f"EXECUTE_HIGH_RISK:     {EXECUTE_HIGH_RISK}")
    print(f"NO_OVERWRITE:          {NO_OVERWRITE}")
    print("-" * 90)

    if not EXECUTABLE_CANDIDATES_LATEST.exists():
        raise FileNotFoundError(
            f"Executable candidates file not found: {EXECUTABLE_CANDIDATES_LATEST}"
        )

    df = pd.read_csv(EXECUTABLE_CANDIDATES_LATEST)

    print(f"Executable candidate rows loaded: {len(df)}")

    results = []

    for _, row in df.iterrows():
        results.append(process_candidate(row))

    results_df = pd.DataFrame(results)

    log_latest = EXECUTION_LOG_DIR / "missing_base_execution_log_latest.csv"
    log_ts = EXECUTION_LOG_DIR / f"missing_base_execution_log_{run_ts}.csv"

    summary_latest = EXECUTION_LOG_DIR / "missing_base_execution_summary_latest.csv"
    summary_ts = EXECUTION_LOG_DIR / f"missing_base_execution_summary_{run_ts}.csv"

    json_path = EXECUTION_LOG_DIR / f"missing_base_execution_log_{run_ts}.json"

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
        "execute_low_risk_only": EXECUTE_LOW_RISK_ONLY,
        "execute_medium_risk": EXECUTE_MEDIUM_RISK,
        "execute_high_risk": EXECUTE_HIGH_RISK,
        "no_overwrite": NO_OVERWRITE,
        "candidate_file": str(EXECUTABLE_CANDIDATES_LATEST),
        "log_latest": str(log_latest),
        "summary_latest": str(summary_latest),
        "total_candidates": int(len(results_df)),
        "would_copy": int(results_df["status"].eq("dry_run_would_copy").sum()),
        "copied": int(results_df["status"].eq("copied").sum()),
        "skipped": int(results_df["status"].astype(str).str.startswith("skipped").sum()),
        "errors": int(results_df["status"].eq("error").sum()),
        "next_recommended_step": (
            "If DRY_RUN produced expected low-risk MN1 copy actions only, review output. "
            "Then optionally set DRY_RUN=False to execute low-risk candidates only. "
            "After execution, rerun Scripts 16, 17, 18, 19, and 20 to refresh state."
        ),
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Guarded missing base executor complete.")
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