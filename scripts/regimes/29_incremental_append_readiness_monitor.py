"""
BACQE Script 29
Incremental Append Readiness Monitor

Purpose:
- Lightweight operational monitor for incremental append readiness
- Reads Script 28 latest append design audit
- Reports GREEN if no append candidates or blockers exist
- Reports AMBER if append candidates exist
- Reports RED if blocked cases exist
- Produces latest CSV/JSON status outputs

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"
APPEND_DESIGN_DIR = LEDGER_DIR / "incremental_append_design"

APPEND_AUDIT_LATEST = APPEND_DESIGN_DIR / "incremental_append_design_audit_latest.csv"

OUTPUT_DIR = LEDGER_DIR / "incremental_append_readiness"


def load_audit() -> pd.DataFrame:
    if not APPEND_AUDIT_LATEST.exists():
        raise FileNotFoundError(f"Missing append design audit: {APPEND_AUDIT_LATEST}")

    return pd.read_csv(APPEND_AUDIT_LATEST)


def determine_overall_status(audit: pd.DataFrame) -> str:
    blocked = audit["append_feasibility"].astype(str).eq("blocked").sum()
    append_candidates = audit["append_feasibility"].astype(str).eq("append_candidate").sum()

    if blocked > 0:
        return "RED"

    if append_candidates > 0:
        return "AMBER"

    return "GREEN"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE INCREMENTAL APPEND READINESS MONITOR")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Append audit: {APPEND_AUDIT_LATEST}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    audit = load_audit()

    total_checked = len(audit)
    append_candidates = int(audit["append_feasibility"].astype(str).eq("append_candidate").sum())
    already_aligned = int(audit["append_feasibility"].astype(str).eq("already_aligned").sum())
    blocked = int(audit["append_feasibility"].astype(str).eq("blocked").sum())
    recent_behind = int(audit["append_feasibility"].astype(str).eq("recent_behind").sum())

    overall_status = determine_overall_status(audit)

    summary = (
        audit.groupby(
            ["plan_type", "broker", "timeframe", "append_feasibility", "append_reason"],
            dropna=False,
        )
        .agg(
            files_checked=("symbol", "count"),
            total_full_rows=("full_rows", "sum"),
            total_recent_rows=("recent_rows", "sum"),
            max_gap_hours=("gap_hours", "max"),
        )
        .reset_index()
        .sort_values(
            ["append_feasibility", "broker", "timeframe", "plan_type"],
            ascending=True,
        )
    )

    summary["max_gap_hours"] = summary["max_gap_hours"].fillna(0).round(4)

    append_candidate_rows = audit[
        audit["append_feasibility"].astype(str).eq("append_candidate")
    ].copy()

    blocked_rows = audit[
        audit["append_feasibility"].astype(str).eq("blocked")
    ].copy()

    status_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_status": overall_status,
        "append_audit_latest": str(APPEND_AUDIT_LATEST),
        "total_checked": total_checked,
        "already_aligned": already_aligned,
        "append_candidates": append_candidates,
        "blocked": blocked,
        "recent_behind": recent_behind,
        "interpretation": (
            "GREEN means no append action is currently required. "
            "AMBER means recent data is ahead of full base and append planning is required. "
            "RED means blocked schema/timestamp/path cases must be reviewed before append planning."
        ),
        "next_recommended_step": (
            "If GREEN, no append action needed. "
            "If AMBER, build/run Script 30 dry-run append planner. "
            "If RED, inspect blocked rows before continuing."
        ),
    }

    status_latest_json = OUTPUT_DIR / "incremental_append_readiness_latest.json"
    status_ts_json = OUTPUT_DIR / f"incremental_append_readiness_{run_ts}.json"

    summary_latest = OUTPUT_DIR / "incremental_append_readiness_summary_latest.csv"
    summary_ts = OUTPUT_DIR / f"incremental_append_readiness_summary_{run_ts}.csv"

    candidates_latest = OUTPUT_DIR / "incremental_append_readiness_candidates_latest.csv"
    candidates_ts = OUTPUT_DIR / f"incremental_append_readiness_candidates_{run_ts}.csv"

    blocked_latest = OUTPUT_DIR / "incremental_append_readiness_blocked_latest.csv"
    blocked_ts = OUTPUT_DIR / f"incremental_append_readiness_blocked_{run_ts}.csv"

    with status_latest_json.open("w", encoding="utf-8") as f:
        json.dump(status_payload, f, indent=4)

    with status_ts_json.open("w", encoding="utf-8") as f:
        json.dump(status_payload, f, indent=4)

    summary.to_csv(summary_latest, index=False)
    summary.to_csv(summary_ts, index=False)

    append_candidate_rows.to_csv(candidates_latest, index=False)
    append_candidate_rows.to_csv(candidates_ts, index=False)

    blocked_rows.to_csv(blocked_latest, index=False)
    blocked_rows.to_csv(blocked_ts, index=False)

    print("[DONE] Incremental append readiness monitor complete.")
    print(f"Overall status: {overall_status}")
    print(f"Total checked:  {total_checked}")
    print(f"Aligned:        {already_aligned}")
    print(f"Append needed:  {append_candidates}")
    print(f"Blocked:        {blocked}")
    print(f"Recent behind:  {recent_behind}")
    print("-" * 90)
    print(f"Status JSON:    {status_latest_json}")
    print(f"Summary CSV:    {summary_latest}")
    print(f"Candidates CSV: {candidates_latest}")
    print(f"Blocked CSV:    {blocked_latest}")
    print("=" * 90)


if __name__ == "__main__":
    main()