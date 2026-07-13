"""
BACQE DUKASCOPY 75 - OVERNIGHT MASTER ORCHESTRATOR

Purpose:
    Safely advance the Dukascopy ingestion and extended-horizon pipelines
    while unattended.

Operating principles:
    - Sequential execution only
    - One heavy job at a time
    - Refresh state before every decision
    - Verify state after every completed job
    - Prioritise recovery work
    - Stop immediately on failure by default
    - Respect maximum runtime and maximum job limits
    - Prevent duplicate concurrent overnight runs
    - Maintain detailed logs and machine-readable run state

Control flow:
    1. Refresh ingestion state and governance outputs
    2. Refresh extended-horizon state and cohort outputs
    3. Check recovery actions
    4. Check symbol-onboarding actions
    5. Check global EH11-EH13 actions
    6. Execute one required command
    7. Refresh and verify state
    8. Repeat until complete, failed, or limits reached

This first version is deliberately conservative.
It does not run jobs in parallel.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
import argparse
import json
import os
import shlex
import subprocess
import sys
import time

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BASE_DIR = Path("E:/Quant_Lab")

ANALYSIS_ROOT = BASE_DIR / "data" / "analysis"

REPORT_ROOT = (
    ANALYSIS_ROOT
    / "dukascopy_overnight_master_orchestrator"
)

LOG_ROOT = REPORT_ROOT / "logs"
RUN_STATE_ROOT = REPORT_ROOT / "run_state"

LOCK_PATH = RUN_STATE_ROOT / "dukascopy_overnight_master.lock"
LATEST_STATE_PATH = RUN_STATE_ROOT / "dukascopy_overnight_master_latest.json"
LATEST_LEDGER_PATH = REPORT_ROOT / "dukascopy_overnight_job_ledger_latest.csv"
LATEST_REPORT_PATH = REPORT_ROOT / "dukascopy_overnight_report_latest.txt"


CONTROL_SCRIPTS = [
    Path("scripts/dukascopy_ticks/65_dukascopy_pipeline_state_registry.py"),
    Path("scripts/dukascopy_ticks/66_dukascopy_pipeline_decision_engine.py"),
    Path("scripts/dukascopy_ticks/67_dukascopy_pipeline_verification_engine.py"),
    Path("scripts/dukascopy_ticks/68_dukascopy_pipeline_recovery_engine.py"),
    Path("scripts/dukascopy_ticks/69_dukascopy_research_queue_manager.py"),
    Path("scripts/dukascopy_ticks/70_extended_horizon_state_registry.py"),
    Path("scripts/dukascopy_ticks/71_extended_horizon_decision_engine.py"),
    Path("scripts/dukascopy_ticks/72_extended_horizon_global_cohort_registry.py"),
    Path("scripts/dukascopy_ticks/73_extended_horizon_global_cohort_decision_engine.py"),
    Path("scripts/dukascopy_ticks/74_dukascopy_new_symbol_onboarding_engine.py"),
    Path("scripts/dukascopy_ticks/76_dukascopy_durable_resume_ledger.py"),
    Path("scripts/dukascopy_ticks/77_dukascopy_morning_intelligence_report.py"),
]


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

PIPELINE_FAILURES_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_pipeline_verification_engine"
    / "dukascopy_pipeline_verification_failures_latest.csv"
)

RESUME_PLAN_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_durable_resume_ledger"
    / "dukascopy_resume_plan_latest.csv"
)


@dataclass
class RunState:
    run_id: str
    started_at: str
    updated_at: str
    finished_at: str
    status: str
    jobs_started: int
    jobs_succeeded: int
    jobs_failed: int
    control_refreshes: int
    max_jobs: int
    max_runtime_minutes: int
    elapsed_minutes: float
    last_command: str
    last_result: str
    stop_reason: str
    log_path: str


@dataclass
class JobRecord:
    run_id: str
    job_number: int
    source: str
    symbol: str
    stage: str
    priority: str
    command: str
    started_at: str
    finished_at: str
    elapsed_seconds: float
    return_code: int
    status: str
    log_path: str


def timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_id_now() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_directories() -> None:
    LOG_ROOT.mkdir(parents=True, exist_ok=True)
    RUN_STATE_ROOT.mkdir(parents=True, exist_ok=True)


def process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False

    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_lock(run_id: str, force: bool) -> None:
    if LOCK_PATH.exists():
        try:
            lock_data = json.loads(
                LOCK_PATH.read_text(encoding="utf-8")
            )
        except Exception:
            lock_data = {}

        existing_pid = int(lock_data.get("pid", 0))
        existing_run_id = str(lock_data.get("run_id", "unknown"))

        if process_is_running(existing_pid) and not force:
            raise RuntimeError(
                "Another overnight orchestrator appears to be running.\n"
                f"Run ID: {existing_run_id}\n"
                f"PID: {existing_pid}\n"
                f"Lock: {LOCK_PATH}"
            )

        print(
            "[WARNING] Removing stale or force-overridden lock: "
            f"{LOCK_PATH}"
        )
        LOCK_PATH.unlink(missing_ok=True)

    lock_data = {
        "run_id": run_id,
        "pid": os.getpid(),
        "created_at": timestamp(),
    }

    LOCK_PATH.write_text(
        json.dumps(lock_data, indent=2),
        encoding="utf-8",
    )


def release_lock() -> None:
    LOCK_PATH.unlink(missing_ok=True)


def write_state(state: RunState) -> None:
    state.updated_at = timestamp()

    LATEST_STATE_PATH.write_text(
        json.dumps(asdict(state), indent=2),
        encoding="utf-8",
    )


def append_master_log(log_path: Path, message: str) -> None:
    line = f"{timestamp()} | {message}"

    print(line)

    with open(log_path, "a", encoding="utf-8") as file:
        file.write(line + "\n")


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as exc:
        raise RuntimeError(
            f"Failed reading control CSV {path}: {exc}"
        ) from exc


def command_to_args(command: str) -> list[str]:
    """
    Convert a generated BACQE command into a subprocess argument list.

    On Windows, shlex.split(..., posix=False) can preserve surrounding
    quotation marks. Strip those quotes before passing arguments to
    subprocess.Popen.
    """

    parts = shlex.split(command, posix=False)

    if not parts:
        raise ValueError("Cannot execute an empty command.")

    cleaned_parts = []

    for part in parts:
        part = str(part).strip()

        if (
            len(part) >= 2
            and part[0] == part[-1]
            and part[0] in {'"', "'"}
        ):
            part = part[1:-1]

        cleaned_parts.append(part)

    if cleaned_parts[0].lower() in {
        "python",
        "python.exe",
        "py",
    }:
        cleaned_parts[0] = sys.executable

    return cleaned_parts

def run_streaming_command(
    command: str,
    log_path: Path,
    master_log_path: Path,
    dry_run: bool,
) -> tuple[int, float]:
    append_master_log(
        master_log_path,
        f"COMMAND | {command}",
    )

    if dry_run:
        append_master_log(
            master_log_path,
            "DRY RUN | Command not executed.",
        )
        return 0, 0.0

    args = command_to_args(command)

    started = time.monotonic()

    with open(log_path, "w", encoding="utf-8") as job_log:
        job_log.write(f"COMMAND: {command}\n")
        job_log.write("=" * 100 + "\n")
        job_log.flush()

        process = subprocess.Popen(
            args,
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert process.stdout is not None

        for line in process.stdout:
            print(line, end="")
            job_log.write(line)
            job_log.flush()

        process.wait()

        elapsed = time.monotonic() - started

        job_log.write("\n" + "=" * 100 + "\n")
        job_log.write(f"RETURN CODE: {process.returncode}\n")
        job_log.write(f"ELAPSED SECONDS: {elapsed:.2f}\n")

    append_master_log(
        master_log_path,
        (
            f"RESULT | return_code={process.returncode} "
            f"elapsed_seconds={elapsed:.2f}"
        ),
    )

    return int(process.returncode), elapsed


def refresh_control_state(
    run_id: str,
    refresh_number: int,
    master_log_path: Path,
    dry_run: bool,
) -> None:
    append_master_log(
        master_log_path,
        f"CONTROL REFRESH {refresh_number} START",
    )

    for index, relative_script in enumerate(
        CONTROL_SCRIPTS,
        start=1,
    ):
        script_path = PROJECT_ROOT / relative_script

        if not script_path.exists():
            raise FileNotFoundError(
                f"Missing control script: {script_path}"
            )

        command = f"python {relative_script}"

        log_path = (
            LOG_ROOT
            / (
                f"{run_id}_refresh_{refresh_number:03d}_"
                f"{index:02d}_{relative_script.stem}.log"
            )
        )

        return_code, _ = run_streaming_command(
            command=command,
            log_path=log_path,
            master_log_path=master_log_path,
            dry_run=dry_run,
        )

        if return_code != 0:
            raise RuntimeError(
                f"Control refresh failed: {relative_script} "
                f"returned code {return_code}"
            )

    append_master_log(
        master_log_path,
        f"CONTROL REFRESH {refresh_number} COMPLETE",
    )


def durable_resume_action() -> dict | None:
    resume_plan = safe_read_csv(RESUME_PLAN_PATH)

    if resume_plan.empty:
        return None

    if "resume_eligible" in resume_plan.columns:
        resume_plan = resume_plan[
            resume_plan["resume_eligible"].astype(str).str.lower()
            .isin(["true", "1", "yes"])
        ]

    if resume_plan.empty:
        return None

    if "priority_rank" in resume_plan.columns:
        resume_plan = resume_plan.sort_values(
            ["priority_rank", "attempt_count", "symbol"],
            ascending=[True, True, True],
        )

    row = resume_plan.iloc[0]

    command = str(row.get("command", "")).strip()

    if not command:
        return None

    return {
        "source": "durable_resume_ledger",
        "symbol": str(row.get("symbol", "UNKNOWN")),
        "stage": str(row.get("stage", "UNKNOWN")),
        "priority": str(row.get("priority", "medium")),
        "command": command,
    }


def recovery_action() -> dict | None:
    recovery = safe_read_csv(RECOVERY_PLAN_PATH)

    if recovery.empty:
        return None

    if "recovery_status" in recovery.columns:
        recovery = recovery[
            recovery["recovery_status"].astype(str) == "planned"
        ]

    if recovery.empty:
        return None

    if "priority_rank" in recovery.columns:
        recovery = recovery.sort_values(
            ["priority_rank", "symbol"],
            ascending=[True, True],
        )

    row = recovery.iloc[0]

    command = str(row.get("recovery_command", "")).strip()

    if not command:
        return None

    return {
        "source": "recovery_engine",
        "symbol": str(row.get("symbol", "UNKNOWN")),
        "stage": str(row.get("script_stage", "UNKNOWN")),
        "priority": str(row.get("priority", "critical")),
        "command": command,
    }


def onboarding_action() -> dict | None:
    actions = safe_read_csv(ONBOARDING_ACTIONS_PATH)

    if actions.empty:
        return None

    actions = actions[
        actions["onboarding_status"].astype(str).isin(
            ["action_required", "definition_error"]
        )
    ]

    if actions.empty:
        return None

    if "priority_rank" in actions.columns:
        actions = actions.sort_values(
            ["priority_rank", "symbol"],
            ascending=[True, True],
        )

    row = actions.iloc[0]

    command = str(row.get("command", "")).strip()

    if not command:
        return None

    return {
        "source": "onboarding_engine",
        "symbol": str(row.get("symbol", "UNKNOWN")),
        "stage": str(row.get("next_stage", "UNKNOWN")),
        "priority": str(row.get("priority", "medium")),
        "command": command,
    }


def global_cohort_action() -> dict | None:
    actions = safe_read_csv(GLOBAL_ACTIONS_PATH)

    if actions.empty:
        return None

    actions = actions[
        actions["decision_status"].astype(str) == "action_required"
    ]

    if actions.empty:
        return None

    if "priority_rank" in actions.columns:
        actions = actions.sort_values(
            ["priority_rank", "stage_key"],
            ascending=[True, True],
        )

    row = actions.iloc[0]

    command = str(row.get("command", "")).strip()

    if not command:
        return None

    return {
        "source": "global_cohort_decision_engine",
        "symbol": "GLOBAL",
        "stage": str(row.get("stage_key", "UNKNOWN")),
        "priority": str(row.get("priority", "medium")),
        "command": command,
    }


def select_next_action() -> dict | None:
    """
    Action precedence:
        1. Durable resume plan
        2. Recovery
        3. Symbol onboarding / ingestion / EH01-EH10
        4. Global cohort EH11-EH13
    """

    resume = durable_resume_action()

    if resume is not None:
        return resume

    recovery = recovery_action()

    if recovery is not None:
        return recovery

    onboarding = onboarding_action()

    if onboarding is not None:
        if onboarding["symbol"] == "GLOBAL":
            global_action = global_cohort_action()

            if global_action is not None:
                return global_action

        return onboarding

    global_action = global_cohort_action()

    if global_action is not None:
        return global_action

    return None


def unresolved_verification_failures() -> int:
    failures = safe_read_csv(PIPELINE_FAILURES_PATH)

    if failures.empty:
        return 0

    return len(failures)


def save_ledger(records: list[JobRecord]) -> None:
    columns = list(JobRecord.__annotations__.keys())

    if not records:
        pd.DataFrame(columns=columns).to_csv(
            LATEST_LEDGER_PATH,
            index=False,
        )
        return

    pd.DataFrame(
        [asdict(record) for record in records]
    ).to_csv(
        LATEST_LEDGER_PATH,
        index=False,
    )


def write_report(
    state: RunState,
    records: list[JobRecord],
) -> None:
    with open(
        LATEST_REPORT_PATH,
        "w",
        encoding="utf-8",
    ) as file:
        file.write(
            "BACQE DUKASCOPY 75 - OVERNIGHT MASTER ORCHESTRATOR\n"
        )
        file.write("=" * 100 + "\n\n")

        file.write("RUN STATE\n")
        file.write("-" * 100 + "\n")

        for key, value in asdict(state).items():
            file.write(f"{key}: {value}\n")

        file.write("\nJOB LEDGER\n")
        file.write("-" * 100 + "\n")

        if records:
            frame = pd.DataFrame(
                [asdict(record) for record in records]
            )
            file.write(frame.to_string(index=False))
        else:
            file.write("No heavy jobs executed.\n")


def elapsed_minutes(start_monotonic: float) -> float:
    return (time.monotonic() - start_monotonic) / 60.0


def main(
    max_runtime_minutes: int,
    max_jobs: int,
    stop_on_error: bool,
    dry_run: bool,
    force_lock: bool,
) -> int:
    ensure_directories()

    run_id = run_id_now()

    master_log_path = (
        LOG_ROOT
        / f"dukascopy_overnight_master_{run_id}.log"
    )

    state = RunState(
        run_id=run_id,
        started_at=timestamp(),
        updated_at=timestamp(),
        finished_at="",
        status="starting",
        jobs_started=0,
        jobs_succeeded=0,
        jobs_failed=0,
        control_refreshes=0,
        max_jobs=max_jobs,
        max_runtime_minutes=max_runtime_minutes,
        elapsed_minutes=0.0,
        last_command="",
        last_result="",
        stop_reason="",
        log_path=str(master_log_path),
    )

    records: list[JobRecord] = []

    start_monotonic = time.monotonic()

    acquire_lock(
        run_id=run_id,
        force=force_lock,
    )

    write_state(state)

    append_master_log(
        master_log_path,
        (
            "START | "
            f"run_id={run_id} "
            f"max_runtime_minutes={max_runtime_minutes} "
            f"max_jobs={max_jobs} "
            f"dry_run={dry_run}"
        ),
    )

    try:
        state.status = "running"
        write_state(state)

        while True:
            current_elapsed = elapsed_minutes(start_monotonic)
            state.elapsed_minutes = round(current_elapsed, 2)

            if current_elapsed >= max_runtime_minutes:
                state.status = "stopped_by_limit"
                state.stop_reason = (
                    "Maximum runtime reached before starting another job."
                )
                break

            if state.jobs_started >= max_jobs:
                state.status = "stopped_by_limit"
                state.stop_reason = (
                    "Maximum number of heavy jobs reached."
                )
                break

            state.control_refreshes += 1
            write_state(state)

            refresh_control_state(
                run_id=run_id,
                refresh_number=state.control_refreshes,
                master_log_path=master_log_path,
                dry_run=dry_run,
            )

            verification_failures = (
                unresolved_verification_failures()
            )

            append_master_log(
                master_log_path,
                (
                    "STATE | "
                    f"verification_failures={verification_failures}"
                ),
            )

            action = select_next_action()

            if action is None:
                state.status = "complete"
                state.stop_reason = (
                    "No recovery, onboarding, or global cohort actions remain."
                )
                break

            command = action["command"]

            # Prevent endless governance-only loops.
            governance_only_commands = {
                "python scripts/dukascopy_ticks/"
                "72_extended_horizon_global_cohort_registry.py",
                "python scripts/dukascopy_ticks/"
                "73_extended_horizon_global_cohort_decision_engine.py",
            }

            if command in governance_only_commands:
                state.status = "governance_stall"
                state.stop_reason = (
                    "The selected action is governance-only after a full "
                    "control refresh. No concrete heavy command was found."
                )
                state.last_command = command
                state.last_result = "not_executed"
                break

            state.jobs_started += 1
            state.last_command = command
            write_state(state)

            job_number = state.jobs_started

            job_log_path = (
                LOG_ROOT
                / (
                    f"{run_id}_job_{job_number:03d}_"
                    f"{action['source']}_"
                    f"{action['symbol']}_"
                    f"{action['stage']}.log"
                )
            )

            job_started_at = timestamp()

            return_code, job_elapsed = run_streaming_command(
                command=command,
                log_path=job_log_path,
                master_log_path=master_log_path,
                dry_run=dry_run,
            )

            job_finished_at = timestamp()

            job_status = (
                "ok"
                if return_code == 0
                else "error"
            )

            records.append(
                JobRecord(
                    run_id=run_id,
                    job_number=job_number,
                    source=action["source"],
                    symbol=action["symbol"],
                    stage=action["stage"],
                    priority=action["priority"],
                    command=command,
                    started_at=job_started_at,
                    finished_at=job_finished_at,
                    elapsed_seconds=round(job_elapsed, 2),
                    return_code=return_code,
                    status=job_status,
                    log_path=str(job_log_path),
                )
            )

            save_ledger(records)

            if return_code == 0:
                state.jobs_succeeded += 1
                state.last_result = "ok"

                append_master_log(
                    master_log_path,
                    (
                        f"JOB {job_number} COMPLETE | "
                        f"symbol={action['symbol']} "
                        f"stage={action['stage']}"
                    ),
                )

            else:
                state.jobs_failed += 1
                state.last_result = (
                    f"error_return_code_{return_code}"
                )

                append_master_log(
                    master_log_path,
                    (
                        f"JOB {job_number} FAILED | "
                        f"symbol={action['symbol']} "
                        f"stage={action['stage']} "
                        f"return_code={return_code}"
                    ),
                )

                if stop_on_error:
                    state.status = "failed"
                    state.stop_reason = (
                        f"Heavy job failed with return code {return_code}."
                    )
                    break

            state.elapsed_minutes = round(
                elapsed_minutes(start_monotonic),
                2,
            )

            write_state(state)
            write_report(state, records)

        state.elapsed_minutes = round(
            elapsed_minutes(start_monotonic),
            2,
        )
        state.finished_at = timestamp()

        if not state.stop_reason:
            state.stop_reason = "Run finished."

        write_state(state)
        save_ledger(records)
        write_report(state, records)

        append_master_log(
            master_log_path,
            (
                "FINISH | "
                f"status={state.status} "
                f"jobs_started={state.jobs_started} "
                f"jobs_succeeded={state.jobs_succeeded} "
                f"jobs_failed={state.jobs_failed} "
                f"elapsed_minutes={state.elapsed_minutes:.2f} "
                f"reason={state.stop_reason}"
            ),
        )

        return 0 if state.status in {
            "complete",
            "stopped_by_limit",
        } else 1

    except KeyboardInterrupt:
        state.status = "interrupted"
        state.stop_reason = "KeyboardInterrupt received."
        state.finished_at = timestamp()
        state.elapsed_minutes = round(
            elapsed_minutes(start_monotonic),
            2,
        )

        write_state(state)
        save_ledger(records)
        write_report(state, records)

        append_master_log(
            master_log_path,
            "INTERRUPTED | KeyboardInterrupt received.",
        )

        return 130

    except Exception as exc:
        state.status = "failed"
        state.stop_reason = (
            f"{type(exc).__name__}: {exc}"
        )
        state.finished_at = timestamp()
        state.elapsed_minutes = round(
            elapsed_minutes(start_monotonic),
            2,
        )
        state.last_result = "exception"

        write_state(state)
        save_ledger(records)
        write_report(state, records)

        append_master_log(
            master_log_path,
            (
                "FATAL ERROR | "
                f"{type(exc).__name__}: {exc}"
            ),
        )

        return 1

    finally:
        release_lock()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=480,
        help=(
            "Maximum total run time. Default: 480 minutes "
            "(8 hours)."
        ),
    )

    parser.add_argument(
        "--max-jobs",
        type=int,
        default=25,
        help=(
            "Maximum number of heavy processing jobs in one run. "
            "Default: 25."
        ),
    )

    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help=(
            "Continue after a heavy job failure. "
            "Default behaviour is to stop immediately."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Refresh and display planned commands without executing "
            "heavy work."
        ),
    )

    parser.add_argument(
        "--force-lock",
        action="store_true",
        help=(
            "Remove an existing lock and start anyway. "
            "Use only when certain no other run is active."
        ),
    )

    args = parser.parse_args()

    exit_code = main(
        max_runtime_minutes=args.max_runtime_minutes,
        max_jobs=args.max_jobs,
        stop_on_error=not args.continue_on_error,
        dry_run=args.dry_run,
        force_lock=args.force_lock,
    )

    raise SystemExit(exit_code)