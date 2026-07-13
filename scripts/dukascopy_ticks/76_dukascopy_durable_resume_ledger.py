"""
BACQE DUKASCOPY 76 - DURABLE RESUME LEDGER

Purpose:
    Maintain persistent job history across multiple Script 75 overnight runs.

Capabilities:
    - Stable job IDs derived from source, symbol, stage and command
    - Persistent cross-run ledger
    - Statuses:
        planned
        running
        completed
        failed
        interrupted
        retry_exhausted
        no_longer_required
    - Detect jobs left running after abnormal shutdown
    - Import outcomes from Script 75's latest job ledger
    - Reconcile jobs against current recovery/onboarding/global actions
    - Track attempts and retry limits
    - Produce a safe resume plan
    - Preserve completed history without blindly rerunning work

This script does not execute heavy jobs.
It maintains durable state and creates the resume plan consumed by the
overnight orchestration layer.
"""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from pathlib import Path
import argparse
import json

import pandas as pd


BASE_DIR = Path("E:/Quant_Lab")
ANALYSIS_ROOT = BASE_DIR / "data" / "analysis"

SCRIPT_75_ROOT = (
    ANALYSIS_ROOT
    / "dukascopy_overnight_master_orchestrator"
)

SCRIPT_75_LATEST_LEDGER = (
    SCRIPT_75_ROOT
    / "dukascopy_overnight_job_ledger_latest.csv"
)

RECOVERY_PLAN_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_pipeline_recovery_engine"
    / "dukascopy_pipeline_recovery_plan_latest.csv"
)

ONBOARDING_ACTIONS_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_symbol_onboarding"
    / "dukascopy_symbol_onboarding_actions_latest.csv"
)

GLOBAL_ACTIONS_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_extended_horizons"
    / "global_cohort_decision_engine"
    / "extended_horizon_global_actions_latest.csv"
)

REPORT_ROOT = (
    ANALYSIS_ROOT
    / "dukascopy_durable_resume_ledger"
)

LEDGER_PATH = REPORT_ROOT / "dukascopy_durable_job_ledger.csv"
RESUME_PLAN_PATH = REPORT_ROOT / "dukascopy_resume_plan_latest.csv"
STATE_PATH = REPORT_ROOT / "dukascopy_resume_ledger_state_latest.json"
REPORT_PATH = REPORT_ROOT / "dukascopy_resume_ledger_report_latest.txt"


LEDGER_COLUMNS = [
    "job_id",
    "source",
    "symbol",
    "stage",
    "priority",
    "command",
    "status",
    "attempt_count",
    "max_attempts",
    "first_seen_at",
    "last_planned_at",
    "last_started_at",
    "last_finished_at",
    "last_run_id",
    "last_return_code",
    "last_error",
    "last_log_path",
    "currently_required",
    "updated_at",
]


ACTIVE_STATUSES = {
    "planned",
    "running",
    "failed",
    "interrupted",
}

TERMINAL_STATUSES = {
    "completed",
    "retry_exhausted",
    "no_longer_required",
}


PRIORITY_RANK = {
    "critical": 1,
    "high": 2,
    "medium": 3,
    "low": 4,
    "complete": 99,
    "blocked": 100,
}


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_output_directory() -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)


def empty_ledger() -> pd.DataFrame:
    return pd.DataFrame(columns=LEDGER_COLUMNS)


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read CSV {path}: {type(exc).__name__}: {exc}"
        ) from exc


def clean_text(value: object, default: str = "") -> str:
    if pd.isna(value):
        return default

    return str(value).strip()


def clean_integer(value: object, default: int = 0) -> int:
    if pd.isna(value):
        return default

    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def stable_job_id(
    source: str,
    symbol: str,
    stage: str,
    command: str,
) -> str:
    identity = "|".join(
        [
            source.strip().lower(),
            symbol.strip().upper(),
            stage.strip().upper(),
            " ".join(command.strip().split()),
        ]
    )

    return sha256(identity.encode("utf-8")).hexdigest()[:20]


def normalise_ledger(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return empty_ledger()

    df = df.copy()

    for column in LEDGER_COLUMNS:
        if column not in df.columns:
            df[column] = ""

    df = df[LEDGER_COLUMNS]

    string_columns = [
        "job_id",
        "source",
        "symbol",
        "stage",
        "priority",
        "command",
        "status",
        "first_seen_at",
        "last_planned_at",
        "last_started_at",
        "last_finished_at",
        "last_run_id",
        "last_error",
        "last_log_path",
        "updated_at",
    ]

    for column in string_columns:
        df[column] = df[column].fillna("").astype(str)

    df["attempt_count"] = pd.to_numeric(
        df["attempt_count"],
        errors="coerce",
    ).fillna(0).astype(int)

    df["max_attempts"] = pd.to_numeric(
        df["max_attempts"],
        errors="coerce",
    ).fillna(3).astype(int)

    df["last_return_code"] = pd.to_numeric(
        df["last_return_code"],
        errors="coerce",
    ).fillna(-1).astype(int)

    df["currently_required"] = (
        df["currently_required"]
        .astype(str)
        .str.lower()
        .isin(["true", "1", "yes"])
    )

    return df


def load_durable_ledger() -> pd.DataFrame:
    if not LEDGER_PATH.exists():
        return empty_ledger()

    return normalise_ledger(safe_read_csv(LEDGER_PATH))


def save_durable_ledger(ledger: pd.DataFrame) -> None:
    ledger = normalise_ledger(ledger)

    ledger = ledger.sort_values(
        by=[
            "currently_required",
            "status",
            "priority",
            "symbol",
            "stage",
            "job_id",
        ],
        ascending=[
            False,
            True,
            True,
            True,
            True,
            True,
        ],
    )

    ledger.to_csv(LEDGER_PATH, index=False)


def mark_abandoned_running_jobs(
    ledger: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    if ledger.empty:
        return ledger, 0

    ledger = ledger.copy()

    running_mask = ledger["status"] == "running"
    interrupted_count = int(running_mask.sum())

    if interrupted_count:
        ledger.loc[running_mask, "status"] = "interrupted"
        ledger.loc[running_mask, "last_error"] = (
            "Job was left in running state when the durable ledger "
            "was next reconciled."
        )
        ledger.loc[running_mask, "last_finished_at"] = now_text()
        ledger.loc[running_mask, "updated_at"] = now_text()

    return ledger, interrupted_count


def action_rows_from_recovery() -> list[dict]:
    df = safe_read_csv(RECOVERY_PLAN_PATH)

    if df.empty:
        return []

    if "recovery_status" in df.columns:
        df = df[
            df["recovery_status"].astype(str) == "planned"
        ]

    rows: list[dict] = []

    for _, row in df.iterrows():
        command = clean_text(row.get("recovery_command"))

        if not command:
            continue

        rows.append(
            {
                "source": "recovery_engine",
                "symbol": clean_text(row.get("symbol"), "UNKNOWN"),
                "stage": clean_text(row.get("script_stage"), "UNKNOWN"),
                "priority": clean_text(row.get("priority"), "critical"),
                "command": command,
            }
        )

    return rows


def action_rows_from_onboarding() -> list[dict]:
    df = safe_read_csv(ONBOARDING_ACTIONS_PATH)

    if df.empty:
        return []

    if "onboarding_status" in df.columns:
        df = df[
            df["onboarding_status"]
            .astype(str)
            .isin(["action_required", "definition_error"])
        ]

    rows: list[dict] = []

    for _, row in df.iterrows():
        command = clean_text(row.get("command"))

        if not command:
            continue

        rows.append(
            {
                "source": "onboarding_engine",
                "symbol": clean_text(row.get("symbol"), "UNKNOWN"),
                "stage": clean_text(row.get("next_stage"), "UNKNOWN"),
                "priority": clean_text(row.get("priority"), "medium"),
                "command": command,
            }
        )

    return rows


def action_rows_from_global_cohort() -> list[dict]:
    df = safe_read_csv(GLOBAL_ACTIONS_PATH)

    if df.empty:
        return []

    if "decision_status" in df.columns:
        df = df[
            df["decision_status"].astype(str) == "action_required"
        ]

    rows: list[dict] = []

    for _, row in df.iterrows():
        command = clean_text(row.get("command"))

        if not command:
            continue

        rows.append(
            {
                "source": "global_cohort_decision_engine",
                "symbol": "GLOBAL",
                "stage": clean_text(row.get("stage_key"), "UNKNOWN"),
                "priority": clean_text(row.get("priority"), "medium"),
                "command": command,
            }
        )

    return rows


def collect_current_actions() -> pd.DataFrame:
    rows = (
        action_rows_from_recovery()
        + action_rows_from_onboarding()
        + action_rows_from_global_cohort()
    )

    if not rows:
        return pd.DataFrame(
            columns=[
                "job_id",
                "source",
                "symbol",
                "stage",
                "priority",
                "command",
            ]
        )

    actions = pd.DataFrame(rows)

    actions["job_id"] = actions.apply(
        lambda row: stable_job_id(
            source=row["source"],
            symbol=row["symbol"],
            stage=row["stage"],
            command=row["command"],
        ),
        axis=1,
    )

    actions = actions.drop_duplicates(
        subset=["job_id"],
        keep="first",
    )

    actions["priority_rank"] = (
        actions["priority"]
        .map(PRIORITY_RANK)
        .fillna(50)
        .astype(int)
    )

    return actions.sort_values(
        by=["priority_rank", "stage", "symbol"],
        ascending=[True, True, True],
    )


def add_or_refresh_current_actions(
    ledger: pd.DataFrame,
    actions: pd.DataFrame,
    max_attempts: int,
) -> pd.DataFrame:
    ledger = normalise_ledger(ledger).copy()

    if not ledger.empty:
        ledger["currently_required"] = False

    current_time = now_text()

    for _, action in actions.iterrows():
        job_id = clean_text(action["job_id"])
        match = ledger["job_id"] == job_id

        if match.any():
            index = ledger.index[match][0]

            ledger.at[index, "currently_required"] = True
            ledger.at[index, "last_planned_at"] = current_time
            ledger.at[index, "updated_at"] = current_time
            ledger.at[index, "priority"] = clean_text(
                action["priority"],
                "medium",
            )

            status = clean_text(ledger.at[index, "status"])

            # A completed job may become required again if the state engines
            # still identify the exact same command as necessary.
            if status in {
                "completed",
                "no_longer_required",
            }:
                ledger.at[index, "status"] = "planned"
                ledger.at[index, "last_error"] = (
                    "The state engines still require this job after a "
                    "previous completion."
                )

            attempts = clean_integer(
                ledger.at[index, "attempt_count"],
                0,
            )

            allowed_attempts = clean_integer(
                ledger.at[index, "max_attempts"],
                max_attempts,
            )

            if (
                status in {"failed", "interrupted", "retry_exhausted"}
                and attempts >= allowed_attempts
            ):
                ledger.at[index, "status"] = "retry_exhausted"

        else:
            new_row = {
                "job_id": job_id,
                "source": clean_text(action["source"]),
                "symbol": clean_text(action["symbol"], "UNKNOWN"),
                "stage": clean_text(action["stage"], "UNKNOWN"),
                "priority": clean_text(action["priority"], "medium"),
                "command": clean_text(action["command"]),
                "status": "planned",
                "attempt_count": 0,
                "max_attempts": max_attempts,
                "first_seen_at": current_time,
                "last_planned_at": current_time,
                "last_started_at": "",
                "last_finished_at": "",
                "last_run_id": "",
                "last_return_code": -1,
                "last_error": "",
                "last_log_path": "",
                "currently_required": True,
                "updated_at": current_time,
            }

            ledger = pd.concat(
                [ledger, pd.DataFrame([new_row])],
                ignore_index=True,
            )

    no_longer_required_mask = (
        (~ledger["currently_required"])
        & ledger["status"].isin(ACTIVE_STATUSES)
    )

    ledger.loc[
        no_longer_required_mask,
        "status",
    ] = "no_longer_required"

    ledger.loc[
        no_longer_required_mask,
        "updated_at",
    ] = current_time

    return normalise_ledger(ledger)


def import_script_75_results(
    ledger: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    latest = safe_read_csv(SCRIPT_75_LATEST_LEDGER)

    if latest.empty:
        return ledger, 0

    ledger = normalise_ledger(ledger).copy()
    imported = 0

    for _, job in latest.iterrows():
        source = clean_text(job.get("source"))
        symbol = clean_text(job.get("symbol"), "UNKNOWN")
        stage = clean_text(job.get("stage"), "UNKNOWN")
        command = clean_text(job.get("command"))

        if not command:
            continue

        job_id = stable_job_id(
            source=source,
            symbol=symbol,
            stage=stage,
            command=command,
        )

        match = ledger["job_id"] == job_id

        if match.any():
            index = ledger.index[match][0]
        else:
            new_row = {
                "job_id": job_id,
                "source": source,
                "symbol": symbol,
                "stage": stage,
                "priority": clean_text(job.get("priority"), "medium"),
                "command": command,
                "status": "planned",
                "attempt_count": 0,
                "max_attempts": 3,
                "first_seen_at": clean_text(
                    job.get("started_at"),
                    now_text(),
                ),
                "last_planned_at": "",
                "last_started_at": "",
                "last_finished_at": "",
                "last_run_id": "",
                "last_return_code": -1,
                "last_error": "",
                "last_log_path": "",
                "currently_required": False,
                "updated_at": now_text(),
            }

            ledger = pd.concat(
                [ledger, pd.DataFrame([new_row])],
                ignore_index=True,
            )

            index = ledger.index[-1]

        imported_run_id = clean_text(job.get("run_id"))
        imported_finished_at = clean_text(job.get("finished_at"))

        # Avoid counting the same Script 75 result more than once.
        same_result_already_imported = (
            clean_text(ledger.at[index, "last_run_id"]) == imported_run_id
            and clean_text(ledger.at[index, "last_finished_at"])
            == imported_finished_at
        )

        if same_result_already_imported:
            continue

        ledger.at[index, "attempt_count"] = (
            clean_integer(ledger.at[index, "attempt_count"], 0) + 1
        )

        ledger.at[index, "last_started_at"] = clean_text(
            job.get("started_at")
        )

        ledger.at[index, "last_finished_at"] = imported_finished_at
        ledger.at[index, "last_run_id"] = imported_run_id

        return_code = clean_integer(
            job.get("return_code"),
            -1,
        )

        ledger.at[index, "last_return_code"] = return_code
        ledger.at[index, "last_log_path"] = clean_text(
            job.get("log_path")
        )
        ledger.at[index, "updated_at"] = now_text()

        job_status = clean_text(job.get("status"))

        if return_code == 0 and job_status == "ok":
            ledger.at[index, "status"] = "completed"
            ledger.at[index, "last_error"] = ""
        else:
            attempts = clean_integer(
                ledger.at[index, "attempt_count"],
                0,
            )

            allowed = clean_integer(
                ledger.at[index, "max_attempts"],
                3,
            )

            if attempts >= allowed:
                ledger.at[index, "status"] = "retry_exhausted"
            else:
                ledger.at[index, "status"] = "failed"

            ledger.at[index, "last_error"] = (
                f"Script 75 result status={job_status}, "
                f"return_code={return_code}"
            )

        imported += 1

    return normalise_ledger(ledger), imported


def build_resume_plan(
    ledger: pd.DataFrame,
) -> pd.DataFrame:
    if ledger.empty:
        return pd.DataFrame(
            columns=LEDGER_COLUMNS
            + ["priority_rank", "resume_eligible", "resume_reason"]
        )

    plan = ledger.copy()

    plan["priority_rank"] = (
        plan["priority"]
        .map(PRIORITY_RANK)
        .fillna(50)
        .astype(int)
    )

    plan["resume_eligible"] = (
        plan["currently_required"]
        & plan["status"].isin(
            ["planned", "failed", "interrupted"]
        )
        & (
            plan["attempt_count"]
            < plan["max_attempts"]
        )
    )

    def resume_reason(row: pd.Series) -> str:
        if not bool(row["currently_required"]):
            return "Job is no longer required by the current state engines."

        if row["status"] == "retry_exhausted":
            return "Retry limit has been reached."

        if row["status"] == "completed":
            return "Job has completed and is not currently queued for retry."

        if row["attempt_count"] >= row["max_attempts"]:
            return "Retry limit has been reached."

        if row["status"] == "interrupted":
            return "Interrupted job is eligible for a safe retry."

        if row["status"] == "failed":
            return "Failed job remains required and is eligible for retry."

        if row["status"] == "planned":
            return "New required job is ready to run."

        return f"Job status {row['status']} is not resume eligible."

    plan["resume_reason"] = plan.apply(
        resume_reason,
        axis=1,
    )

    eligible = plan[plan["resume_eligible"]].copy()

    eligible = eligible.sort_values(
        by=[
            "priority_rank",
            "attempt_count",
            "stage",
            "symbol",
        ],
        ascending=[
            True,
            True,
            True,
            True,
        ],
    )

    return eligible


def write_state_json(
    ledger: pd.DataFrame,
    resume_plan: pd.DataFrame,
    interrupted_jobs: int,
    imported_results: int,
    current_actions: int,
) -> None:
    state = {
        "updated_at": now_text(),
        "ledger_rows": len(ledger),
        "currently_required_jobs": int(
            ledger["currently_required"].sum()
        ) if not ledger.empty else 0,
        "completed_jobs": int(
            (ledger["status"] == "completed").sum()
        ) if not ledger.empty else 0,
        "failed_jobs": int(
            (ledger["status"] == "failed").sum()
        ) if not ledger.empty else 0,
        "interrupted_jobs": int(
            (ledger["status"] == "interrupted").sum()
        ) if not ledger.empty else 0,
        "retry_exhausted_jobs": int(
            (ledger["status"] == "retry_exhausted").sum()
        ) if not ledger.empty else 0,
        "resume_plan_jobs": len(resume_plan),
        "interrupted_reconciled_this_run": interrupted_jobs,
        "script_75_results_imported_this_run": imported_results,
        "current_actions_detected": current_actions,
    }

    STATE_PATH.write_text(
        json.dumps(state, indent=2),
        encoding="utf-8",
    )


def write_report(
    ledger: pd.DataFrame,
    resume_plan: pd.DataFrame,
    interrupted_jobs: int,
    imported_results: int,
    current_actions: int,
) -> None:
    with open(REPORT_PATH, "w", encoding="utf-8") as file:
        file.write(
            "BACQE DUKASCOPY 76 - DURABLE RESUME LEDGER\n"
        )
        file.write("=" * 100 + "\n\n")

        file.write("RECONCILIATION SUMMARY\n")
        file.write("-" * 100 + "\n")
        file.write(f"Updated at: {now_text()}\n")
        file.write(
            f"Current actions detected: {current_actions}\n"
        )
        file.write(
            f"Interrupted jobs reconciled: {interrupted_jobs}\n"
        )
        file.write(
            f"Script 75 results imported: {imported_results}\n"
        )
        file.write(
            f"Durable ledger rows: {len(ledger)}\n"
        )
        file.write(
            f"Resume-plan jobs: {len(resume_plan)}\n"
        )

        file.write("\nSTATUS COUNTS\n")
        file.write("-" * 100 + "\n")

        if ledger.empty:
            file.write("No durable jobs recorded.\n")
        else:
            file.write(
                ledger["status"].value_counts().to_string()
            )

        file.write("\n\nRESUME PLAN\n")
        file.write("-" * 100 + "\n")

        if resume_plan.empty:
            file.write("No jobs require resumption.\n")
        else:
            display_columns = [
                "job_id",
                "source",
                "symbol",
                "stage",
                "priority",
                "status",
                "attempt_count",
                "max_attempts",
                "resume_reason",
                "command",
            ]

            file.write(
                resume_plan[display_columns].to_string(
                    index=False
                )
            )

        file.write("\n\nFULL DURABLE LEDGER\n")
        file.write("-" * 100 + "\n")

        if ledger.empty:
            file.write("No durable jobs recorded.\n")
        else:
            file.write(ledger.to_string(index=False))


def main(max_attempts: int) -> None:
    ensure_output_directory()

    ledger = load_durable_ledger()

    ledger, interrupted_jobs = mark_abandoned_running_jobs(
        ledger
    )

    ledger, imported_results = import_script_75_results(
        ledger
    )

    current_actions = collect_current_actions()

    ledger = add_or_refresh_current_actions(
        ledger=ledger,
        actions=current_actions,
        max_attempts=max_attempts,
    )

    resume_plan = build_resume_plan(ledger)

    save_durable_ledger(ledger)
    resume_plan.to_csv(RESUME_PLAN_PATH, index=False)

    write_state_json(
        ledger=ledger,
        resume_plan=resume_plan,
        interrupted_jobs=interrupted_jobs,
        imported_results=imported_results,
        current_actions=len(current_actions),
    )

    write_report(
        ledger=ledger,
        resume_plan=resume_plan,
        interrupted_jobs=interrupted_jobs,
        imported_results=imported_results,
        current_actions=len(current_actions),
    )

    print("=" * 100)
    print("BACQE DUKASCOPY 76 - DURABLE RESUME LEDGER")
    print("=" * 100)
    print(f"Current actions detected: {len(current_actions)}")
    print(f"Interrupted jobs reconciled: {interrupted_jobs}")
    print(f"Script 75 results imported: {imported_results}")
    print(f"Durable ledger rows: {len(ledger)}")
    print(f"Resume-plan jobs: {len(resume_plan)}")
    print("-" * 100)

    if ledger.empty:
        print("[CLEAN] No durable jobs have been recorded.")
    else:
        print("STATUS COUNTS")
        print(ledger["status"].value_counts().to_string())

    print("-" * 100)

    if resume_plan.empty:
        print("[COMPLETE] No jobs require resumption.")
    else:
        display_columns = [
            "job_id",
            "source",
            "symbol",
            "stage",
            "priority",
            "status",
            "attempt_count",
            "max_attempts",
            "resume_reason",
            "command",
        ]

        print("RESUME PLAN")
        print(
            resume_plan[display_columns].to_string(
                index=False
            )
        )

    print("-" * 100)
    print(f"Durable ledger: {LEDGER_PATH}")
    print(f"Resume plan:    {RESUME_PLAN_PATH}")
    print(f"State:          {STATE_PATH}")
    print(f"Report:         {REPORT_PATH}")
    print("=" * 100)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help=(
            "Maximum attempts allowed for each stable job ID. "
            "Default: 3."
        ),
    )

    args = parser.parse_args()

    if args.max_attempts < 1:
        raise ValueError("--max-attempts must be at least 1.")

    main(max_attempts=args.max_attempts)