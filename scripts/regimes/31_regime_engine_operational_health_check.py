"""
BACQE Script 31
Regime Engine Operational Health Check

Purpose:
- Produce a single operational health report for the regime engine
- Check sync verification status
- Check append readiness status
- Check ledger freshness
- Check recent processed regime outputs
- Return GREEN / AMBER / RED status

This script is read-only.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"

SYNC_JSON = LEDGER_DIR / "sync_verification" / "incremental_sync_verification_overall_latest.json"
APPEND_READINESS_JSON = LEDGER_DIR / "incremental_append_readiness" / "incremental_append_readiness_latest.json"
LEDGER_LATEST = LEDGER_DIR / "regime_incremental_ledger_latest.csv"
CHANGE_SUMMARY = LEDGER_DIR / "regime_incremental_change_summary_latest.csv"

RECENT_FEATURES_DIR = DATA_LAKE_ROOT / "data" / "processed" / "regimes" / "recent" / "features"
RECENT_CLASSIFIED_DIR = DATA_LAKE_ROOT / "data" / "processed" / "regimes" / "recent" / "classified"

OUTPUT_DIR = LEDGER_DIR / "operational_health"

MAX_LEDGER_AGE_HOURS = 24
MAX_RECENT_OUTPUT_AGE_HOURS = 24


def read_json_safe(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def file_age_hours(path: Path):
    if not path.exists():
        return None
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    return round((now - modified).total_seconds() / 3600, 3)


def latest_file_age_hours(folder: Path):
    if not folder.exists():
        return None

    files = [p for p in folder.rglob("*") if p.is_file()]

    if not files:
        return None

    latest = max(files, key=lambda p: p.stat().st_mtime)
    return file_age_hours(latest)


def make_check(name, passed, observed, expected, notes):
    return {
        "check_name": name,
        "status": "green" if passed else "red",
        "passed": passed,
        "observed": observed,
        "expected": expected,
        "notes": notes,
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    sync = read_json_safe(SYNC_JSON)
    readiness = read_json_safe(APPEND_READINESS_JSON)

    checks = []

    sync_status = sync.get("overall_status", "missing").lower()
    checks.append(make_check(
        "sync_verification_green",
        sync_status == "green",
        sync_status,
        "green",
        "Latest Script 27 sync verification should be green.",
    ))

    append_status = readiness.get("overall_status", "missing").upper()
    checks.append(make_check(
        "append_readiness_not_red",
        append_status in {"GREEN", "AMBER"},
        append_status,
        "GREEN or AMBER",
        "Append readiness should not be RED.",
    ))

    append_blocked = readiness.get("blocked", None)
    checks.append(make_check(
        "append_blocked_zero",
        append_blocked == 0,
        append_blocked,
        0,
        "Append readiness should have no blocked rows.",
    ))

    ledger_age = file_age_hours(LEDGER_LATEST)
    checks.append(make_check(
        "ledger_recent_enough",
        ledger_age is not None and ledger_age <= MAX_LEDGER_AGE_HOURS,
        ledger_age,
        f"<= {MAX_LEDGER_AGE_HOURS} hours",
        "Incremental ledger should be reasonably fresh.",
    ))

    recent_features_age = latest_file_age_hours(RECENT_FEATURES_DIR)
    checks.append(make_check(
        "recent_features_fresh_enough",
        recent_features_age is not None and recent_features_age <= MAX_RECENT_OUTPUT_AGE_HOURS,
        recent_features_age,
        f"<= {MAX_RECENT_OUTPUT_AGE_HOURS} hours",
        "Recent feature outputs should be fresh.",
    ))

    recent_classified_age = latest_file_age_hours(RECENT_CLASSIFIED_DIR)
    checks.append(make_check(
        "recent_classified_fresh_enough",
        recent_classified_age is not None and recent_classified_age <= MAX_RECENT_OUTPUT_AGE_HOURS,
        recent_classified_age,
        f"<= {MAX_RECENT_OUTPUT_AGE_HOURS} hours",
        "Recent classified outputs should be fresh.",
    ))

    change_updates = None
    if CHANGE_SUMMARY.exists():
        change_df = pd.read_csv(CHANGE_SUMMARY)
        if "updates_needed" in change_df.columns:
            change_updates = int(pd.to_numeric(change_df["updates_needed"], errors="coerce").fillna(0).sum())

    checks.append(make_check(
        "change_updates_zero",
        change_updates == 0,
        change_updates,
        0,
        "Latest change summary should show no missing-base updates required.",
    ))

    checks_df = pd.DataFrame(checks)

    if checks_df["status"].eq("red").any():
        overall_status = "RED"
    elif append_status == "AMBER":
        overall_status = "AMBER"
    else:
        overall_status = "GREEN"

    overall = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_status": overall_status,
        "checks_total": int(len(checks_df)),
        "checks_passed": int(checks_df["passed"].sum()),
        "checks_failed": int((~checks_df["passed"]).sum()),
        "sync_status": sync_status,
        "append_readiness_status": append_status,
        "append_candidates": readiness.get("append_candidates"),
        "append_blocked": append_blocked,
        "ledger_age_hours": ledger_age,
        "recent_features_age_hours": recent_features_age,
        "recent_classified_age_hours": recent_classified_age,
        "next_recommended_step": (
            "If GREEN, no action required. "
            "If AMBER, inspect append candidates and run Script 30. "
            "If RED, inspect failed checks before proceeding."
        ),
    }

    report_latest = OUTPUT_DIR / "regime_engine_operational_health_latest.csv"
    report_ts = OUTPUT_DIR / f"regime_engine_operational_health_{run_ts}.csv"

    overall_latest = OUTPUT_DIR / "regime_engine_operational_health_latest.json"
    overall_ts = OUTPUT_DIR / f"regime_engine_operational_health_{run_ts}.json"

    checks_df.to_csv(report_latest, index=False)
    checks_df.to_csv(report_ts, index=False)

    with overall_latest.open("w", encoding="utf-8") as f:
        json.dump(overall, f, indent=4)

    with overall_ts.open("w", encoding="utf-8") as f:
        json.dump(overall, f, indent=4)

    print("=" * 90)
    print("BACQE REGIME ENGINE OPERATIONAL HEALTH CHECK")
    print("=" * 90)
    print(f"Overall status: {overall_status}")
    print(f"Checks passed:  {overall['checks_passed']} / {overall['checks_total']}")
    print(f"Report latest:  {report_latest}")
    print(f"Overall JSON:   {overall_latest}")
    print("-" * 90)
    print(checks_df.to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()