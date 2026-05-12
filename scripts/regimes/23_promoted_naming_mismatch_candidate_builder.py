"""
BACQE Script 23
Promoted Naming Mismatch Candidate Builder

Purpose:
- Read Script 22 naming mismatch inspection output
- Select rows promoted to genuine missing candidates
- Build a separate executable candidate plan
- Preserve a clear audit trail:
  quarantined -> inspected -> promoted -> executable

This script is read-only.
"""

from pathlib import Path
from datetime import datetime
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"

INSPECTION_DIR = LEDGER_DIR / "naming_mismatch_inspection"
INSPECTION_LATEST = INSPECTION_DIR / "naming_mismatch_inspection_latest.csv"

OUTPUT_DIR = LEDGER_DIR / "promoted_naming_mismatch_candidates"


PROMOTED_STATUS = "promote_to_genuine_missing_candidate"


def path_exists(value) -> bool:
    if pd.isna(value):
        return False
    text = str(value).strip()
    if not text:
        return False
    return Path(text).exists()


def classify_risk(timeframe: str) -> str:
    tf = str(timeframe).upper()

    if tf == "MN1":
        return "low"

    return "medium"


def infer_creation_action(row) -> str:
    source = row.get("source_recent_path")
    destination = row.get("destination_full_path")

    if not path_exists(source):
        return "blocked_missing_recent_source"

    if path_exists(destination):
        return "blocked_destination_already_exists"

    return "dry_run_create_full_base_from_recent"


def approved_for_execution(action: str) -> bool:
    return action == "dry_run_create_full_base_from_recent"


def recommendation(row) -> str:
    action = row["creation_action"]
    symbol = row["symbol"]

    if action == "dry_run_create_full_base_from_recent":
        return (
            f"Promoted after naming mismatch inspection. Symbol {symbol} appears to be a false "
            "short-symbol collision and can be handled as a genuine missing base candidate."
        )

    if action == "blocked_missing_recent_source":
        return "Blocked: recent source file no longer exists."

    if action == "blocked_destination_already_exists":
        return "Blocked: destination already exists. Refresh ledgers before execution."

    return "Manual review required."


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE PROMOTED NAMING MISMATCH CANDIDATE BUILDER")
    print("=" * 90)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Inspection:   {INSPECTION_LATEST}")
    print(f"Output dir:   {OUTPUT_DIR}")
    print("-" * 90)

    if not INSPECTION_LATEST.exists():
        raise FileNotFoundError(f"Missing inspection file: {INSPECTION_LATEST}")

    df = pd.read_csv(INSPECTION_LATEST)

    promoted = df[df["suggested_status"].eq(PROMOTED_STATUS)].copy()

    print(f"Inspection rows loaded: {len(df)}")
    print(f"Promoted rows: {len(promoted)}")

    if promoted.empty:
        print("[DONE] No promoted rows found.")
        return

    promoted["source_exists_now"] = promoted["source_recent_path"].apply(path_exists)
    promoted["destination_exists_now"] = promoted["destination_full_path"].apply(path_exists)
    promoted["destination_folder"] = promoted["destination_full_path"].apply(
        lambda p: str(Path(str(p)).parent) if not pd.isna(p) else ""
    )
    promoted["destination_folder_exists_now"] = promoted["destination_folder"].apply(path_exists)

    promoted["creation_action"] = promoted.apply(infer_creation_action, axis=1)
    promoted["approved_for_future_execution"] = promoted["creation_action"].apply(approved_for_execution)
    promoted["risk_level"] = promoted["timeframe"].apply(classify_risk)
    promoted["planner_recommendation"] = promoted.apply(recommendation, axis=1)

    promoted["promotion_reason"] = promoted["likely_collision_reason"]
    promoted["promotion_source"] = "script_22_naming_mismatch_inspection"

    output_cols = [
        "plan_type",
        "broker",
        "timeframe",
        "symbol",
        "risk_level",
        "creation_action",
        "approved_for_future_execution",
        "planner_recommendation",
        "promotion_reason",
        "promotion_source",
        "source_exists_now",
        "destination_exists_now",
        "destination_folder_exists_now",
        "source_recent_path",
        "destination_full_path",
        "destination_folder",
        "source_rows",
        "source_size_mb",
        "source_latest_timestamp",
        "source_timestamp_column",
        "best_match_path",
        "schema_match_with_best",
        "suggested_resolution",
    ]

    promoted_plan = promoted[output_cols].sort_values(
        by=["risk_level", "broker", "timeframe", "symbol", "plan_type"],
        ascending=True,
    )

    executable = promoted_plan[promoted_plan["approved_for_future_execution"].eq(True)].copy()
    blocked = promoted_plan[promoted_plan["approved_for_future_execution"].eq(False)].copy()

    summary = (
        promoted_plan.groupby(
            ["plan_type", "broker", "timeframe", "risk_level", "creation_action"],
            dropna=False,
        )
        .agg(
            candidates=("symbol", "count"),
            approved=("approved_for_future_execution", "sum"),
            source_rows=("source_rows", "sum"),
            source_size_mb=("source_size_mb", "sum"),
        )
        .reset_index()
        .sort_values(["risk_level", "candidates"], ascending=[True, False])
    )

    summary["source_size_mb"] = summary["source_size_mb"].round(4)

    plan_latest = OUTPUT_DIR / "promoted_naming_mismatch_plan_latest.csv"
    plan_ts = OUTPUT_DIR / f"promoted_naming_mismatch_plan_{run_ts}.csv"

    executable_latest = OUTPUT_DIR / "promoted_naming_mismatch_executable_latest.csv"
    executable_ts = OUTPUT_DIR / f"promoted_naming_mismatch_executable_{run_ts}.csv"

    blocked_latest = OUTPUT_DIR / "promoted_naming_mismatch_blocked_latest.csv"
    blocked_ts = OUTPUT_DIR / f"promoted_naming_mismatch_blocked_{run_ts}.csv"

    summary_latest = OUTPUT_DIR / "promoted_naming_mismatch_summary_latest.csv"
    summary_ts = OUTPUT_DIR / f"promoted_naming_mismatch_summary_{run_ts}.csv"

    json_path = OUTPUT_DIR / f"promoted_naming_mismatch_plan_{run_ts}.json"

    promoted_plan.to_csv(plan_latest, index=False)
    promoted_plan.to_csv(plan_ts, index=False)

    executable.to_csv(executable_latest, index=False)
    executable.to_csv(executable_ts, index=False)

    blocked.to_csv(blocked_latest, index=False)
    blocked.to_csv(blocked_ts, index=False)

    summary.to_csv(summary_latest, index=False)
    summary.to_csv(summary_ts, index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "inspection_file": str(INSPECTION_LATEST),
        "plan_latest": str(plan_latest),
        "executable_latest": str(executable_latest),
        "blocked_latest": str(blocked_latest),
        "summary_latest": str(summary_latest),
        "total_promoted": int(len(promoted_plan)),
        "approved_for_future_execution": int(promoted_plan["approved_for_future_execution"].sum()),
        "blocked": int((~promoted_plan["approved_for_future_execution"]).sum()),
        "next_recommended_step": (
            "Use a guarded executor for promoted naming mismatch candidates only. "
            "Keep medium risk disabled unless specifically executing this reviewed promoted plan."
        ),
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Promoted naming mismatch candidate plan created.")
    print(f"Plan latest:       {plan_latest}")
    print(f"Executable latest: {executable_latest}")
    print(f"Blocked latest:    {blocked_latest}")
    print(f"Summary latest:    {summary_latest}")
    print(f"JSON report:       {json_path}")

    print("-" * 90)
    print(f"Total promoted: {len(promoted_plan)}")
    print(f"Approved: {int(promoted_plan['approved_for_future_execution'].sum())}")
    print(f"Blocked: {int((~promoted_plan['approved_for_future_execution']).sum())}")

    print("\nSummary preview:")
    print(summary.to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()