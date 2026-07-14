"""
BACQE DUKASCOPY 80 - AUTONOMOUS EXECUTION CONTROLLER

Purpose:
    Act as the single execution-decision authority for the Dukascopy
    autonomous research control layer.

Script 80 reads the validated and resource-approved workload plan created
by Scripts 78 and 79, performs final pre-execution checks, and issues one
of the following directives:

    execute
    no_work
    blocked
    wait
    manual_review

Design principle:
    Script 80 decides.
    Script 75 executes.

Inputs:
    Script 67 - Pipeline verification failures
    Script 68 - Recovery plan
    Script 76 - Durable resume ledger and resume plan
    Script 78 - Resource-aware scheduler state and approved schedule
    Script 79 - Workload execution plan and planner state
    Script 75 - Latest orchestrator run state

Outputs:
    - Latest execution directive CSV
    - Latest execution directive JSON
    - Decision history ledger
    - Human-readable controller report
    - Non-damaging self-test outputs

This script does not execute subprocesses.
It does not alter research outputs.
It does not mark jobs complete.
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

REPORT_ROOT = (
    ANALYSIS_ROOT
    / "dukascopy_autonomous_execution_controller"
)

DIRECTIVE_CSV_PATH = (
    REPORT_ROOT
    / "dukascopy_execution_directive_latest.csv"
)

DIRECTIVE_JSON_PATH = (
    REPORT_ROOT
    / "dukascopy_execution_directive_latest.json"
)

DECISION_HISTORY_PATH = (
    REPORT_ROOT
    / "dukascopy_execution_decision_history.csv"
)

REPORT_PATH = (
    REPORT_ROOT
    / "dukascopy_execution_controller_report_latest.txt"
)

SELF_TEST_ROOT = REPORT_ROOT / "self_test"

SELF_TEST_RESULTS_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_execution_controller_self_test_results_latest.csv"
)

SELF_TEST_STATE_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_execution_controller_self_test_state_latest.json"
)

SELF_TEST_REPORT_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_execution_controller_self_test_report_latest.txt"
)


PIPELINE_FAILURES_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_pipeline_verification_engine"
    / "dukascopy_pipeline_verification_failures_latest.csv"
)

RECOVERY_PLAN_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_pipeline_recovery_engine"
    / "dukascopy_pipeline_recovery_plan_latest.csv"
)

DURABLE_LEDGER_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_durable_resume_ledger"
    / "dukascopy_durable_job_ledger.csv"
)

RESUME_PLAN_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_durable_resume_ledger"
    / "dukascopy_resume_plan_latest.csv"
)

SCHEDULER_STATE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_resource_aware_scheduler"
    / "dukascopy_resource_schedule_state_latest.json"
)

APPROVED_SCHEDULE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_resource_aware_scheduler"
    / "dukascopy_approved_overnight_schedule_latest.csv"
)

PLANNER_STATE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_workload_execution_planner"
    / "dukascopy_workload_execution_state_latest.json"
)

EXECUTION_PLAN_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_workload_execution_planner"
    / "dukascopy_workload_execution_plan_latest.csv"
)

ORCHESTRATOR_STATE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_overnight_master_orchestrator"
    / "run_state"
    / "dukascopy_overnight_master_latest.json"
)


DIRECTIVE_COLUMNS = [
    "decision_id",
    "generated_at",
    "directive",
    "controller_status",
    "job_id",
    "source",
    "symbol",
    "stage",
    "priority",
    "command",
    "execution_position",
    "attempt_count",
    "max_attempts",
    "estimated_runtime_minutes",
    "estimated_ram_gb",
    "estimated_disk_growth_gb",
    "decision_reason",
    "blocking_reason",
    "scheduler_status",
    "planner_status",
    "pipeline_failure_count",
    "recovery_action_count",
]


VALID_DIRECTIVES = {
    "execute",
    "no_work",
    "blocked",
    "wait",
    "manual_review",
}


ACTIVE_ORCHESTRATOR_STATUSES = {
    "starting",
    "running",
}


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_output_directories() -> None:
    REPORT_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    SELF_TEST_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as exc:
        raise RuntimeError(
            f"Failed reading {path}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


def safe_read_json(path: Path) -> dict:
    if not path.exists():
        return {}

    try:
        return json.loads(
            path.read_text(
                encoding="utf-8"
            )
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed reading {path}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


def clean_text(
    value: object,
    default: str = "",
) -> str:
    if pd.isna(value):
        return default

    text = str(value).strip()

    if not text or text.lower() == "nan":
        return default

    return text


def clean_int(
    value: object,
    default: int = 0,
) -> int:
    try:
        if pd.isna(value):
            return default

        return int(float(value))
    except (TypeError, ValueError):
        return default


def clean_float(
    value: object,
    default: float = 0.0,
) -> float:
    try:
        if pd.isna(value):
            return default

        return float(value)
    except (TypeError, ValueError):
        return default


def clean_bool(
    value: object,
    default: bool = False,
) -> bool:
    if pd.isna(value):
        return default

    if isinstance(value, bool):
        return value

    return (
        str(value)
        .strip()
        .lower()
        in {
            "true",
            "1",
            "yes",
            "y",
        }
    )


def stable_decision_id(
    directive: str,
    job_id: str,
    command: str,
    generated_at: str,
) -> str:
    identity = "|".join(
        [
            directive,
            job_id,
            " ".join(command.split()),
            generated_at,
        ]
    )

    return sha256(
        identity.encode("utf-8")
    ).hexdigest()[:24]


def empty_directive_row() -> dict:
    return {
        column: ""
        for column in DIRECTIVE_COLUMNS
    }


def pipeline_failure_count(
    failures: pd.DataFrame,
) -> int:
    if failures.empty:
        return 0

    return len(failures)


def planned_recovery_count(
    recovery: pd.DataFrame,
) -> int:
    if recovery.empty:
        return 0

    if "recovery_status" not in recovery.columns:
        return len(recovery)

    return int(
        (
            recovery[
                "recovery_status"
            ].astype(str)
            == "planned"
        ).sum()
    )


def select_first_planned_job(
    execution_plan: pd.DataFrame,
) -> pd.Series | None:
    if execution_plan.empty:
        return None

    plan = execution_plan.copy()

    if "planner_status" in plan.columns:
        plan = plan[
            plan["planner_status"]
            .astype(str)
            .str.lower()
            == "planned"
        ]

    if plan.empty:
        return None

    if "execution_position" in plan.columns:
        plan = plan.sort_values(
            by="execution_position",
            ascending=True,
        )

    return plan.iloc[0]


def job_is_in_approved_schedule(
    job: pd.Series,
    approved_schedule: pd.DataFrame,
) -> bool:
    if approved_schedule.empty:
        return False

    job_id = clean_text(
        job.get("job_id")
    )

    command = clean_text(
        job.get("command")
    )

    if job_id and "job_id" in approved_schedule.columns:
        matching_ids = (
            approved_schedule["job_id"]
            .fillna("")
            .astype(str)
        )

        if bool(
            (matching_ids == job_id).any()
        ):
            return True

    if command and "command" in approved_schedule.columns:
        matching_commands = (
            approved_schedule["command"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        return bool(
            (matching_commands == command).any()
        )

    return False


def durable_job_status(
    job: pd.Series,
    durable_ledger: pd.DataFrame,
) -> tuple[str, int, int]:
    if durable_ledger.empty:
        return "unrecorded", 0, 3

    job_id = clean_text(
        job.get("job_id")
    )

    command = clean_text(
        job.get("command")
    )

    match = pd.DataFrame()

    if job_id and "job_id" in durable_ledger.columns:
        match = durable_ledger[
            durable_ledger["job_id"]
            .fillna("")
            .astype(str)
            == job_id
        ]

    if (
        match.empty
        and command
        and "command"
        in durable_ledger.columns
    ):
        match = durable_ledger[
            durable_ledger["command"]
            .fillna("")
            .astype(str)
            .str.strip()
            == command
        ]

    if match.empty:
        return "unrecorded", 0, 3

    row = match.iloc[0]

    return (
        clean_text(
            row.get("status"),
            "unknown",
        ),
        clean_int(
            row.get("attempt_count"),
            0,
        ),
        clean_int(
            row.get("max_attempts"),
            3,
        ),
    )


def current_resume_job_ids(
    resume_plan: pd.DataFrame,
) -> set[str]:
    if resume_plan.empty:
        return set()

    plan = resume_plan.copy()

    if "resume_eligible" in plan.columns:
        plan = plan[
            plan["resume_eligible"]
            .astype(str)
            .str.lower()
            .isin(
                [
                    "true",
                    "1",
                    "yes",
                ]
            )
        ]

    if "job_id" not in plan.columns:
        return set()

    return set(
        plan["job_id"]
        .dropna()
        .astype(str)
        .str.strip()
    )


def build_directive(
    execution_plan: pd.DataFrame,
    approved_schedule: pd.DataFrame,
    pipeline_failures: pd.DataFrame,
    recovery_plan: pd.DataFrame,
    durable_ledger: pd.DataFrame,
    resume_plan: pd.DataFrame,
    scheduler_state: dict,
    planner_state: dict,
    orchestrator_state: dict,
    allow_active_orchestrator: bool,
) -> dict:
    generated_at = now_text()

    scheduler_status = clean_text(
        scheduler_state.get(
            "schedule_status"
        ),
        "unknown",
    )

    planner_status = clean_text(
        planner_state.get(
            "plan_status"
        ),
        "unknown",
    )

    failures = pipeline_failure_count(
        pipeline_failures
    )

    recovery_actions = planned_recovery_count(
        recovery_plan
    )

    orchestrator_status = clean_text(
        orchestrator_state.get("status"),
        "unknown",
    )

    directive = empty_directive_row()

    directive.update(
        {
            "generated_at": generated_at,
            "scheduler_status": scheduler_status,
            "planner_status": planner_status,
            "pipeline_failure_count": failures,
            "recovery_action_count": recovery_actions,
        }
    )

    # --------------------------------------------------------------
    # Guard 1: Another active orchestrator run
    # --------------------------------------------------------------

    if (
        orchestrator_status
        in ACTIVE_ORCHESTRATOR_STATUSES
        and not allow_active_orchestrator
    ):
        directive.update(
            {
                "directive": "wait",
                "controller_status": "orchestrator_active",
                "decision_reason": (
                    "A Script 75 orchestrator run is already active."
                ),
                "blocking_reason": (
                    f"Latest orchestrator status is "
                    f"{orchestrator_status}."
                ),
            }
        )

        directive["decision_id"] = (
            stable_decision_id(
                directive="wait",
                job_id="",
                command="",
                generated_at=generated_at,
            )
        )

        return directive

    # --------------------------------------------------------------
    # Guard 2: Verification failures without recovery work
    # --------------------------------------------------------------

    if failures > 0 and recovery_actions == 0:
        directive.update(
            {
                "directive": "manual_review",
                "controller_status": (
                    "unresolved_verification_failure"
                ),
                "decision_reason": (
                    "Pipeline verification failures exist, but no "
                    "planned recovery action is available."
                ),
                "blocking_reason": (
                    f"{failures} verification failure(s) remain."
                ),
            }
        )

        directive["decision_id"] = (
            stable_decision_id(
                directive="manual_review",
                job_id="",
                command="",
                generated_at=generated_at,
            )
        )

        return directive

    # --------------------------------------------------------------
    # Guard 3: No planner work
    # --------------------------------------------------------------

    first_job = select_first_planned_job(
        execution_plan
    )

    if first_job is None:
        if planner_status == "all_jobs_blocked":
            directive.update(
                {
                    "directive": "blocked",
                    "controller_status": (
                        "planner_blocked_all_jobs"
                    ),
                    "decision_reason": (
                        "Script 79 reported that all approved jobs "
                        "were blocked."
                    ),
                    "blocking_reason": (
                        "No valid workload execution job is available."
                    ),
                }
            )

        elif scheduler_status == "all_jobs_deferred":
            directive.update(
                {
                    "directive": "blocked",
                    "controller_status": (
                        "scheduler_deferred_all_jobs"
                    ),
                    "decision_reason": (
                        "Script 78 deferred every candidate job."
                    ),
                    "blocking_reason": (
                        "No job currently fits the configured resource "
                        "or runtime budget."
                    ),
                }
            )

        else:
            directive.update(
                {
                    "directive": "no_work",
                    "controller_status": "healthy_no_work",
                    "decision_reason": (
                        "No executable workload is currently planned."
                    ),
                    "blocking_reason": "",
                }
            )

        directive["decision_id"] = (
            stable_decision_id(
                directive=directive["directive"],
                job_id="",
                command="",
                generated_at=generated_at,
            )
        )

        return directive

    # --------------------------------------------------------------
    # Final job validation
    # --------------------------------------------------------------

    job_id = clean_text(
        first_job.get("job_id")
    )

    source = clean_text(
        first_job.get("source"),
        "unknown",
    )

    symbol = clean_text(
        first_job.get("symbol"),
        "UNKNOWN",
    )

    stage = clean_text(
        first_job.get("stage"),
        "UNKNOWN",
    )

    priority = clean_text(
        first_job.get("priority"),
        "medium",
    )

    command = clean_text(
        first_job.get("command")
    )

    execution_position = clean_int(
        first_job.get("execution_position"),
        0,
    )

    durable_status, attempt_count, max_attempts = (
        durable_job_status(
            job=first_job,
            durable_ledger=durable_ledger,
        )
    )

    resume_job_ids = current_resume_job_ids(
        resume_plan
    )

    if not command:
        directive.update(
            {
                "directive": "blocked",
                "controller_status": "empty_command",
                "decision_reason": (
                    "The first planned job has no executable command."
                ),
                "blocking_reason": (
                    f"Job {job_id or 'unknown'} cannot be executed."
                ),
            }
        )

    elif not job_is_in_approved_schedule(
        job=first_job,
        approved_schedule=approved_schedule,
    ):
        directive.update(
            {
                "directive": "blocked",
                "controller_status": (
                    "job_not_resource_approved"
                ),
                "decision_reason": (
                    "The first Script 79 job is not present in "
                    "Script 78's approved schedule."
                ),
                "blocking_reason": (
                    "Resource approval could not be independently "
                    "confirmed."
                ),
            }
        )

    elif attempt_count >= max_attempts:
        directive.update(
            {
                "directive": "manual_review",
                "controller_status": "retry_limit_reached",
                "decision_reason": (
                    "The selected job has reached its retry limit."
                ),
                "blocking_reason": (
                    f"attempt_count={attempt_count}, "
                    f"max_attempts={max_attempts}"
                ),
            }
        )

    elif durable_status == "running":
        directive.update(
            {
                "directive": "wait",
                "controller_status": (
                    "durable_job_already_running"
                ),
                "decision_reason": (
                    "The durable ledger says the selected job is "
                    "already running."
                ),
                "blocking_reason": (
                    "Starting a duplicate job could corrupt outputs "
                    "or waste resources."
                ),
            }
        )

    elif (
        durable_status
        in {
            "failed",
            "interrupted",
        }
        and job_id
        and resume_job_ids
        and job_id not in resume_job_ids
    ):
        directive.update(
            {
                "directive": "blocked",
                "controller_status": (
                    "resume_authorisation_missing"
                ),
                "decision_reason": (
                    "The durable ledger records an interrupted or "
                    "failed job, but Script 76 has not authorised "
                    "its resumption."
                ),
                "blocking_reason": (
                    f"Durable job status is {durable_status}."
                ),
            }
        )

    else:
        directive.update(
            {
                "directive": "execute",
                "controller_status": "execution_authorised",
                "decision_reason": (
                    "The highest-priority planned job passed final "
                    "verification, resource, dependency, retry and "
                    "duplicate-execution checks."
                ),
                "blocking_reason": "",
            }
        )

    directive.update(
        {
            "job_id": job_id,
            "source": source,
            "symbol": symbol,
            "stage": stage,
            "priority": priority,
            "command": command,
            "execution_position": execution_position,
            "attempt_count": attempt_count,
            "max_attempts": max_attempts,
            "estimated_runtime_minutes": clean_int(
                first_job.get(
                    "estimated_runtime_minutes"
                ),
                0,
            ),
            "estimated_ram_gb": clean_float(
                first_job.get(
                    "estimated_ram_gb"
                ),
                0.0,
            ),
            "estimated_disk_growth_gb": clean_float(
                first_job.get(
                    "estimated_disk_growth_gb"
                ),
                0.0,
            ),
        }
    )

    directive["decision_id"] = (
        stable_decision_id(
            directive=directive["directive"],
            job_id=job_id,
            command=command,
            generated_at=generated_at,
        )
    )

    return directive


def normalise_directive_frame(
    directive: dict,
) -> pd.DataFrame:
    row = {
        column: directive.get(
            column,
            "",
        )
        for column in DIRECTIVE_COLUMNS
    }

    return pd.DataFrame([row])


def append_decision_history(
    directive: dict,
) -> None:
    new_row = normalise_directive_frame(
        directive
    )

    history = safe_read_csv(
        DECISION_HISTORY_PATH
    )

    if history.empty:
        combined = new_row
    else:
        for column in DIRECTIVE_COLUMNS:
            if column not in history.columns:
                history[column] = ""

        history = history[
            DIRECTIVE_COLUMNS
        ]

        combined = pd.concat(
            [
                history,
                new_row,
            ],
            ignore_index=True,
        )

    combined.to_csv(
        DECISION_HISTORY_PATH,
        index=False,
    )


def write_outputs(
    directive: dict,
) -> None:
    directive_frame = (
        normalise_directive_frame(
            directive
        )
    )

    directive_frame.to_csv(
        DIRECTIVE_CSV_PATH,
        index=False,
    )

    DIRECTIVE_JSON_PATH.write_text(
        json.dumps(
            directive,
            indent=2,
        ),
        encoding="utf-8",
    )

    append_decision_history(
        directive
    )

    with open(
        REPORT_PATH,
        "w",
        encoding="utf-8",
    ) as file:
        file.write(
            "BACQE DUKASCOPY 80 - "
            "AUTONOMOUS EXECUTION CONTROLLER\n"
        )
        file.write("=" * 110 + "\n\n")

        file.write("EXECUTION DIRECTIVE\n")
        file.write("-" * 110 + "\n")

        for key in DIRECTIVE_COLUMNS:
            file.write(
                f"{key}: "
                f"{directive.get(key, '')}\n"
            )

        file.write("\nCONTROL INTERPRETATION\n")
        file.write("-" * 110 + "\n")

        directive_type = directive[
            "directive"
        ]

        if directive_type == "execute":
            file.write(
                "Script 75 may execute exactly one job using the "
                "command recorded above.\n"
            )

        elif directive_type == "no_work":
            file.write(
                "No heavy job should run. Script 75 may finalise "
                "the overnight cycle normally.\n"
            )

        elif directive_type == "wait":
            file.write(
                "Script 75 should not start another heavy job until "
                "the active condition clears.\n"
            )

        elif directive_type == "blocked":
            file.write(
                "Automated execution is blocked. The control layer "
                "should refresh state before trying again.\n"
            )

        elif directive_type == "manual_review":
            file.write(
                "Human review is required before automated execution "
                "continues.\n"
            )


def test_result(
    test_name: str,
    passed: bool,
    expected: str,
    observed: str,
) -> dict:
    return {
        "test_name": test_name,
        "test_status": (
            "pass"
            if passed
            else "fail"
        ),
        "expected": expected,
        "observed": observed,
    }


def synthetic_job() -> dict:
    return {
        "job_id": "self_test_job_001",
        "source": "onboarding_engine",
        "symbol": "EURUSD",
        "stage": "EH03",
        "priority": "high",
        "command": (
            "python SELF_TEST_ONLY_CONTROLLER_JOB.py"
        ),
        "execution_position": 1,
        "attempt_count": 0,
        "max_attempts": 3,
        "estimated_runtime_minutes": 30,
        "estimated_ram_gb": 4.0,
        "estimated_disk_growth_gb": 2.0,
        "planner_status": "planned",
    }


def synthetic_approved_schedule() -> pd.DataFrame:
    row = synthetic_job()

    row.update(
        {
            "schedule_status": "approved",
            "schedule_position": 1,
        }
    )

    return pd.DataFrame([row])


def synthetic_execution_plan() -> pd.DataFrame:
    return pd.DataFrame(
        [synthetic_job()]
    )


def run_self_test() -> int:
    """
    Run deterministic in-memory controller tests.

    No real control files are modified.
    No synthetic command is executed.
    """

    ensure_output_directories()

    results: list[dict] = []

    empty = pd.DataFrame()

    healthy_scheduler = {
        "schedule_status": "ready",
    }

    no_work_scheduler = {
        "schedule_status": "no_work",
    }

    healthy_planner = {
        "plan_status": "ready",
    }

    no_work_planner = {
        "plan_status": "no_work",
    }

    completed_orchestrator = {
        "status": "complete",
    }

    active_orchestrator = {
        "status": "running",
    }

    # --------------------------------------------------------------
    # Test 1: Healthy no-work directive
    # --------------------------------------------------------------

    directive = build_directive(
        execution_plan=empty,
        approved_schedule=empty,
        pipeline_failures=empty,
        recovery_plan=empty,
        durable_ledger=empty,
        resume_plan=empty,
        scheduler_state=no_work_scheduler,
        planner_state=no_work_planner,
        orchestrator_state=completed_orchestrator,
        allow_active_orchestrator=False,
    )

    results.append(
        test_result(
            test_name="healthy_no_work",
            passed=(
                directive["directive"]
                == "no_work"
            ),
            expected="no_work",
            observed=directive["directive"],
        )
    )

    # --------------------------------------------------------------
    # Test 2: Execute authorised job
    # --------------------------------------------------------------

    directive = build_directive(
        execution_plan=synthetic_execution_plan(),
        approved_schedule=synthetic_approved_schedule(),
        pipeline_failures=empty,
        recovery_plan=empty,
        durable_ledger=empty,
        resume_plan=empty,
        scheduler_state=healthy_scheduler,
        planner_state=healthy_planner,
        orchestrator_state=completed_orchestrator,
        allow_active_orchestrator=False,
    )

    results.append(
        test_result(
            test_name="authorised_execution",
            passed=(
                directive["directive"]
                == "execute"
                and directive["stage"]
                == "EH03"
            ),
            expected="execute EH03",
            observed=(
                f"{directive['directive']} "
                f"{directive['stage']}"
            ),
        )
    )

    # --------------------------------------------------------------
    # Test 3: Active orchestrator causes wait
    # --------------------------------------------------------------

    directive = build_directive(
        execution_plan=synthetic_execution_plan(),
        approved_schedule=synthetic_approved_schedule(),
        pipeline_failures=empty,
        recovery_plan=empty,
        durable_ledger=empty,
        resume_plan=empty,
        scheduler_state=healthy_scheduler,
        planner_state=healthy_planner,
        orchestrator_state=active_orchestrator,
        allow_active_orchestrator=False,
    )

    results.append(
        test_result(
            test_name="active_orchestrator_wait",
            passed=(
                directive["directive"]
                == "wait"
            ),
            expected="wait",
            observed=directive["directive"],
        )
    )

    # --------------------------------------------------------------
    # Test 4: Verification failure without recovery
    # --------------------------------------------------------------

    failures = pd.DataFrame(
        [
            {
                "symbol": "EURUSD",
                "stage_key": "EH03",
            }
        ]
    )

    directive = build_directive(
        execution_plan=synthetic_execution_plan(),
        approved_schedule=synthetic_approved_schedule(),
        pipeline_failures=failures,
        recovery_plan=empty,
        durable_ledger=empty,
        resume_plan=empty,
        scheduler_state=healthy_scheduler,
        planner_state=healthy_planner,
        orchestrator_state=completed_orchestrator,
        allow_active_orchestrator=False,
    )

    results.append(
        test_result(
            test_name=(
                "verification_failure_manual_review"
            ),
            passed=(
                directive["directive"]
                == "manual_review"
            ),
            expected="manual_review",
            observed=directive["directive"],
        )
    )

    # --------------------------------------------------------------
    # Test 5: Job missing from approved schedule
    # --------------------------------------------------------------

    directive = build_directive(
        execution_plan=synthetic_execution_plan(),
        approved_schedule=empty,
        pipeline_failures=empty,
        recovery_plan=empty,
        durable_ledger=empty,
        resume_plan=empty,
        scheduler_state=healthy_scheduler,
        planner_state=healthy_planner,
        orchestrator_state=completed_orchestrator,
        allow_active_orchestrator=False,
    )

    results.append(
        test_result(
            test_name=(
                "resource_approval_required"
            ),
            passed=(
                directive["directive"]
                == "blocked"
            ),
            expected="blocked",
            observed=directive["directive"],
        )
    )

    # --------------------------------------------------------------
    # Test 6: Retry limit reached
    # --------------------------------------------------------------

    durable_ledger = pd.DataFrame(
        [
            {
                "job_id": "self_test_job_001",
                "command": (
                    "python SELF_TEST_ONLY_CONTROLLER_JOB.py"
                ),
                "status": "failed",
                "attempt_count": 3,
                "max_attempts": 3,
            }
        ]
    )

    directive = build_directive(
        execution_plan=synthetic_execution_plan(),
        approved_schedule=synthetic_approved_schedule(),
        pipeline_failures=empty,
        recovery_plan=empty,
        durable_ledger=durable_ledger,
        resume_plan=empty,
        scheduler_state=healthy_scheduler,
        planner_state=healthy_planner,
        orchestrator_state=completed_orchestrator,
        allow_active_orchestrator=False,
    )

    results.append(
        test_result(
            test_name="retry_limit_manual_review",
            passed=(
                directive["directive"]
                == "manual_review"
            ),
            expected="manual_review",
            observed=directive["directive"],
        )
    )

    # --------------------------------------------------------------
    # Test 7: Planner blocked
    # --------------------------------------------------------------

    directive = build_directive(
        execution_plan=empty,
        approved_schedule=synthetic_approved_schedule(),
        pipeline_failures=empty,
        recovery_plan=empty,
        durable_ledger=empty,
        resume_plan=empty,
        scheduler_state=healthy_scheduler,
        planner_state={
            "plan_status": "all_jobs_blocked",
        },
        orchestrator_state=completed_orchestrator,
        allow_active_orchestrator=False,
    )

    results.append(
        test_result(
            test_name="planner_blocked",
            passed=(
                directive["directive"]
                == "blocked"
            ),
            expected="blocked",
            observed=directive["directive"],
        )
    )

    results_frame = pd.DataFrame(
        results
    )

    passed = int(
        (
            results_frame["test_status"]
            == "pass"
        ).sum()
    )

    failed = int(
        (
            results_frame["test_status"]
            == "fail"
        ).sum()
    )

    results_frame.to_csv(
        SELF_TEST_RESULTS_PATH,
        index=False,
    )

    state = {
        "generated_at": now_text(),
        "self_test_mode": True,
        "real_control_files_modified": False,
        "synthetic_commands_executed": False,
        "tests_total": len(results_frame),
        "tests_passed": passed,
        "tests_failed": failed,
        "overall_status": (
            "pass"
            if failed == 0
            else "fail"
        ),
    }

    SELF_TEST_STATE_PATH.write_text(
        json.dumps(
            state,
            indent=2,
        ),
        encoding="utf-8",
    )

    with open(
        SELF_TEST_REPORT_PATH,
        "w",
        encoding="utf-8",
    ) as file:
        file.write(
            "BACQE DUKASCOPY 80 - "
            "AUTONOMOUS EXECUTION CONTROLLER SELF-TEST\n"
        )
        file.write("=" * 100 + "\n\n")

        file.write(
            "Synthetic in-memory controller state only.\n"
        )
        file.write(
            "No genuine BACQE control file was modified.\n"
        )
        file.write(
            "No synthetic command was executed.\n\n"
        )

        file.write(
            results_frame.to_string(
                index=False
            )
        )

    print("=" * 100)
    print(
        "BACQE DUKASCOPY 80 - "
        "AUTONOMOUS EXECUTION CONTROLLER SELF-TEST"
    )
    print("=" * 100)
    print(
        "Synthetic in-memory state only:  True"
    )
    print(
        "Real control files modified:     False"
    )
    print(
        "Synthetic commands executed:     False"
    )
    print("-" * 100)
    print(
        results_frame.to_string(
            index=False
        )
    )
    print("-" * 100)
    print(
        f"Tests passed: {passed}/"
        f"{len(results_frame)}"
    )
    print(f"Tests failed: {failed}")

    if failed == 0:
        print(
            "[PASS] All execution-controller tests passed."
        )
    else:
        print(
            "[FAIL] One or more controller tests failed."
        )

    print("-" * 100)
    print(
        f"Results: {SELF_TEST_RESULTS_PATH}"
    )
    print(
        f"State:   {SELF_TEST_STATE_PATH}"
    )
    print(
        f"Report:  {SELF_TEST_REPORT_PATH}"
    )
    print("=" * 100)

    return 0 if failed == 0 else 1


def main(
    allow_active_orchestrator: bool,
    append_history: bool,
) -> None:
    ensure_output_directories()

    pipeline_failures = safe_read_csv(
        PIPELINE_FAILURES_PATH
    )

    recovery_plan = safe_read_csv(
        RECOVERY_PLAN_PATH
    )

    durable_ledger = safe_read_csv(
        DURABLE_LEDGER_PATH
    )

    resume_plan = safe_read_csv(
        RESUME_PLAN_PATH
    )

    scheduler_state = safe_read_json(
        SCHEDULER_STATE_PATH
    )

    approved_schedule = safe_read_csv(
        APPROVED_SCHEDULE_PATH
    )

    planner_state = safe_read_json(
        PLANNER_STATE_PATH
    )

    execution_plan = safe_read_csv(
        EXECUTION_PLAN_PATH
    )

    orchestrator_state = safe_read_json(
        ORCHESTRATOR_STATE_PATH
    )

    directive = build_directive(
        execution_plan=execution_plan,
        approved_schedule=approved_schedule,
        pipeline_failures=pipeline_failures,
        recovery_plan=recovery_plan,
        durable_ledger=durable_ledger,
        resume_plan=resume_plan,
        scheduler_state=scheduler_state,
        planner_state=planner_state,
        orchestrator_state=orchestrator_state,
        allow_active_orchestrator=(
            allow_active_orchestrator
        ),
    )

    if append_history:
        write_outputs(
            directive=directive
        )
    else:
        directive_frame = (
            normalise_directive_frame(
                directive
            )
        )

        directive_frame.to_csv(
            DIRECTIVE_CSV_PATH,
            index=False,
        )

        DIRECTIVE_JSON_PATH.write_text(
            json.dumps(
                directive,
                indent=2,
            ),
            encoding="utf-8",
        )

    print("=" * 110)
    print(
        "BACQE DUKASCOPY 80 - "
        "AUTONOMOUS EXECUTION CONTROLLER"
    )
    print("=" * 110)
    print(
        f"Directive:              "
        f"{directive['directive']}"
    )
    print(
        f"Controller status:      "
        f"{directive['controller_status']}"
    )
    print(
        f"Scheduler status:       "
        f"{directive['scheduler_status']}"
    )
    print(
        f"Planner status:         "
        f"{directive['planner_status']}"
    )
    print(
        f"Pipeline failures:      "
        f"{directive['pipeline_failure_count']}"
    )
    print(
        f"Recovery actions:       "
        f"{directive['recovery_action_count']}"
    )

    if directive["directive"] == "execute":
        print("-" * 110)
        print("AUTHORISED JOB")
        print(
            f"Job ID:                 "
            f"{directive['job_id']}"
        )
        print(
            f"Source:                 "
            f"{directive['source']}"
        )
        print(
            f"Symbol:                 "
            f"{directive['symbol']}"
        )
        print(
            f"Stage:                  "
            f"{directive['stage']}"
        )
        print(
            f"Priority:               "
            f"{directive['priority']}"
        )
        print(
            f"Execution position:     "
            f"{directive['execution_position']}"
        )
        print(
            f"Estimated runtime:      "
            f"{directive['estimated_runtime_minutes']} minutes"
        )
        print(
            f"Estimated RAM:          "
            f"{directive['estimated_ram_gb']:.2f} GB"
        )
        print(
            f"Estimated disk growth:  "
            f"{directive['estimated_disk_growth_gb']:.2f} GB"
        )
        print(
            f"Command:                "
            f"{directive['command']}"
        )

    print("-" * 110)
    print("DECISION REASON")
    print(
        directive["decision_reason"]
    )

    if directive["blocking_reason"]:
        print("-" * 110)
        print("BLOCKING REASON")
        print(
            directive["blocking_reason"]
        )

    print("-" * 110)
    print(
        f"Directive CSV:  {DIRECTIVE_CSV_PATH}"
    )
    print(
        f"Directive JSON: {DIRECTIVE_JSON_PATH}"
    )

    if append_history:
        print(
            f"History:        {DECISION_HISTORY_PATH}"
        )
        print(
            f"Report:         {REPORT_PATH}"
        )

    print("=" * 110)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--self-test",
        action="store_true",
        help=(
            "Run deterministic, non-damaging in-memory "
            "controller tests."
        ),
    )

    parser.add_argument(
        "--allow-active-orchestrator",
        action="store_true",
        help=(
            "Allow Script 80 to issue a directive while Script 75 "
            "is marked running. This will be required when Script 80 "
            "is later called from inside Script 75."
        ),
    )

    parser.add_argument(
        "--no-history",
        action="store_true",
        help=(
            "Write only the latest directive and do not append "
            "to the controller decision-history ledger."
        ),
    )

    args = parser.parse_args()

    if args.self_test:
        raise SystemExit(
            run_self_test()
        )

    main(
        allow_active_orchestrator=(
            args.allow_active_orchestrator
        ),
        append_history=(
            not args.no_history
        ),
    )