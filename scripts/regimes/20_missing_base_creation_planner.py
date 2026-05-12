"""
BACQE Script 20
Missing Base Creation Planner

Purpose:
- Read Script 19 missing full base investigation
- Build a dry-run creation plan for genuine missing full base files
- Exclude possible naming mismatches from execution
- Produce reviewed source/destination paths before any production writes

This script is read-only.
It does NOT copy, create, overwrite, or modify parquet files.
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

REGIME_PROCESSED_DIR = DATA_LAKE_ROOT / "data" / "processed" / "regimes"
LEDGER_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "regime_incremental_ledger"

INVESTIGATION_DIR = LEDGER_DIR / "missing_full_investigation"
INVESTIGATION_LATEST = INVESTIGATION_DIR / "missing_full_base_investigation_latest.csv"

OUTPUT_DIR = LEDGER_DIR / "base_creation_plans"

FULL_STAGE_DIRS = {
    "feature_update": REGIME_PROCESSED_DIR / "features",
    "classification_update": REGIME_PROCESSED_DIR / "classified",
}

RECENT_STAGE_DIRS = {
    "feature_update": REGIME_PROCESSED_DIR / "recent" / "features",
    "classification_update": REGIME_PROCESSED_DIR / "recent" / "classified",
}


# ============================================================
# HELPERS
# ============================================================

def bool_from_any(value) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def path_exists(path_value) -> bool:
    if pd.isna(path_value):
        return False
    path_text = str(path_value).strip()
    if not path_text:
        return False
    return Path(path_text).exists()


def infer_destination_path(row) -> str:
    plan_type = row["plan_type"]
    broker = row["broker"]
    timeframe = row["timeframe"]
    symbol = row["symbol"]

    base_dir = FULL_STAGE_DIRS.get(plan_type)

    if base_dir is None:
        return ""

    if plan_type == "feature_update":
        filename = f"{symbol}_{timeframe}_features.parquet"
    elif plan_type == "classification_update":
        filename = f"{symbol}_{timeframe}_classified.parquet"
    else:
        filename = f"{symbol}_{timeframe}.parquet"

    return str(base_dir / str(broker) / str(timeframe) / filename)


def infer_destination_folder(row) -> str:
    destination_path = infer_destination_path(row)
    if not destination_path:
        return ""
    return str(Path(destination_path).parent)


def classify_creation_action(row) -> str:
    missing_case = str(row.get("missing_case", ""))

    source_path = row.get("recent_file_path")
    destination_path = row.get("destination_full_path")

    source_exists = path_exists(source_path)
    destination_exists = path_exists(destination_path)

    if missing_case == "possible_naming_mismatch":
        return "quarantine_manual_review"

    if missing_case != "genuine_missing_full_candidate":
        return "ignore_non_genuine_case"

    if not source_exists:
        return "blocked_missing_recent_source"

    if destination_exists:
        return "blocked_destination_already_exists"

    return "dry_run_create_full_base_from_recent"


def approval_status(action: str) -> bool:
    """
    This is only approval for future execution planning.
    Script 20 itself remains read-only.
    """
    return action == "dry_run_create_full_base_from_recent"


def risk_level(action: str, timeframe: str) -> str:
    tf = str(timeframe).upper()

    if action != "dry_run_create_full_base_from_recent":
        return "high"

    if tf == "MN1":
        return "low"

    if tf == "W1":
        return "medium"

    return "medium"


def recommended_action(row) -> str:
    action = row["creation_action"]
    timeframe = str(row.get("timeframe", "")).upper()

    if action == "dry_run_create_full_base_from_recent":
        if timeframe == "MN1":
            return "Safe candidate: create missing monthly full base file from recent layer after final review."
        return "Candidate: create missing full base file from recent layer after symbol/timeframe review."

    if action == "quarantine_manual_review":
        return "Do not create automatically. Review naming mismatch and similar files manually."

    if action == "blocked_missing_recent_source":
        return "Blocked: recent source file no longer exists."

    if action == "blocked_destination_already_exists":
        return "Blocked: destination full file already exists. Refresh ledger before proceeding."

    return "No creation action recommended."


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 90)
    print("BACQE MISSING BASE CREATION PLANNER")
    print("=" * 90)
    print(f"Project root:   {PROJECT_ROOT}")
    print(f"Investigation:  {INVESTIGATION_LATEST}")
    print(f"Output dir:     {OUTPUT_DIR}")
    print("-" * 90)

    if not INVESTIGATION_LATEST.exists():
        raise FileNotFoundError(f"Missing investigation file: {INVESTIGATION_LATEST}")

    df = pd.read_csv(INVESTIGATION_LATEST)

    df["source_recent_path"] = df["recent_file_path"]
    df["destination_full_path"] = df.apply(infer_destination_path, axis=1)
    df["destination_folder"] = df.apply(infer_destination_folder, axis=1)

    df["source_exists_now"] = df["source_recent_path"].apply(path_exists)
    df["destination_exists_now"] = df["destination_full_path"].apply(path_exists)
    df["destination_folder_exists_now"] = df["destination_folder"].apply(path_exists)

    df["creation_action"] = df.apply(classify_creation_action, axis=1)
    df["approved_for_future_execution"] = df["creation_action"].apply(approval_status)
    df["risk_level"] = df.apply(
        lambda row: risk_level(row["creation_action"], row["timeframe"]),
        axis=1,
    )
    df["planner_recommendation"] = df.apply(recommended_action, axis=1)

    output_cols = [
        "plan_type",
        "broker",
        "timeframe",
        "symbol",
        "missing_case",
        "creation_action",
        "approved_for_future_execution",
        "risk_level",
        "planner_recommendation",
        "source_exists_now",
        "destination_exists_now",
        "destination_folder_exists_now",
        "source_recent_path",
        "destination_full_path",
        "destination_folder",
        "latest_timestamp_recent",
        "total_rows_recent",
        "total_size_mb_recent",
        "similar_file_count",
        "similar_files",
    ]

    creation_plan = df[output_cols].sort_values(
        by=[
            "approved_for_future_execution",
            "risk_level",
            "broker",
            "timeframe",
            "symbol",
            "plan_type",
        ],
        ascending=[False, True, True, True, True, True],
    )

    executable_plan = creation_plan[
        creation_plan["approved_for_future_execution"].eq(True)
    ].copy()

    quarantine_plan = creation_plan[
        creation_plan["approved_for_future_execution"].eq(False)
    ].copy()

    summary = (
        creation_plan.groupby(
            ["plan_type", "broker", "timeframe", "missing_case", "creation_action", "risk_level"],
            dropna=False,
        )
        .agg(
            candidates=("symbol", "count"),
            approved=("approved_for_future_execution", "sum"),
            source_exists=("source_exists_now", "sum"),
            destination_exists=("destination_exists_now", "sum"),
            destination_folder_exists=("destination_folder_exists_now", "sum"),
            total_recent_rows=("total_rows_recent", "sum"),
            total_recent_size_mb=("total_size_mb_recent", "sum"),
        )
        .reset_index()
        .sort_values(["approved", "risk_level", "candidates"], ascending=[False, True, False])
    )

    summary["total_recent_size_mb"] = summary["total_recent_size_mb"].round(4)

    plan_latest = OUTPUT_DIR / "missing_base_creation_plan_latest.csv"
    plan_ts = OUTPUT_DIR / f"missing_base_creation_plan_{run_ts}.csv"

    executable_latest = OUTPUT_DIR / "missing_base_creation_executable_candidates_latest.csv"
    executable_ts = OUTPUT_DIR / f"missing_base_creation_executable_candidates_{run_ts}.csv"

    quarantine_latest = OUTPUT_DIR / "missing_base_creation_quarantine_latest.csv"
    quarantine_ts = OUTPUT_DIR / f"missing_base_creation_quarantine_{run_ts}.csv"

    summary_latest = OUTPUT_DIR / "missing_base_creation_summary_latest.csv"
    summary_ts = OUTPUT_DIR / f"missing_base_creation_summary_{run_ts}.csv"

    json_path = OUTPUT_DIR / f"missing_base_creation_plan_{run_ts}.json"

    creation_plan.to_csv(plan_latest, index=False)
    creation_plan.to_csv(plan_ts, index=False)

    executable_plan.to_csv(executable_latest, index=False)
    executable_plan.to_csv(executable_ts, index=False)

    quarantine_plan.to_csv(quarantine_latest, index=False)
    quarantine_plan.to_csv(quarantine_ts, index=False)

    summary.to_csv(summary_latest, index=False)
    summary.to_csv(summary_ts, index=False)

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "investigation_file": str(INVESTIGATION_LATEST),
        "creation_plan_latest": str(plan_latest),
        "executable_candidates_latest": str(executable_latest),
        "quarantine_latest": str(quarantine_latest),
        "summary_latest": str(summary_latest),
        "total_candidates": int(len(creation_plan)),
        "approved_for_future_execution": int(creation_plan["approved_for_future_execution"].sum()),
        "quarantined_or_blocked": int((~creation_plan["approved_for_future_execution"]).sum()),
        "creation_action_counts": creation_plan["creation_action"].value_counts().to_dict(),
        "risk_counts": creation_plan["risk_level"].value_counts().to_dict(),
        "next_recommended_step": (
            "Review executable candidates and quarantine files. "
            "If sensible, build Script 21 as a guarded executor with backups, dry-run switch, "
            "and strict no-overwrite behaviour."
        ),
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)

    print("-" * 90)
    print("[DONE] Missing base creation plan created.")
    print(f"Creation plan latest:        {plan_latest}")
    print(f"Executable candidates latest:{executable_latest}")
    print(f"Quarantine latest:           {quarantine_latest}")
    print(f"Summary latest:              {summary_latest}")
    print(f"JSON report:                 {json_path}")

    print("-" * 90)
    print(f"Total candidates: {len(creation_plan)}")
    print(f"Approved for future execution: {int(creation_plan['approved_for_future_execution'].sum())}")
    print(f"Quarantined or blocked: {int((~creation_plan['approved_for_future_execution']).sum())}")

    print("\nCreation action counts:")
    print(creation_plan["creation_action"].value_counts().to_string())

    print("\nRisk counts:")
    print(creation_plan["risk_level"].value_counts().to_string())

    print("\nSummary preview:")
    print(summary.head(60).to_string(index=False))

    print("=" * 90)


if __name__ == "__main__":
    main()