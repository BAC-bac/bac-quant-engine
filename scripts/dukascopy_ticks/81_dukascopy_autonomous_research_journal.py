"""
BACQE DUKASCOPY 81 - AUTONOMOUS RESEARCH JOURNAL

Purpose:
    Maintain a durable, append-only institutional record of autonomous
    Dukascopy research operations.

The journal brings together evidence from:

    Script 75 - Overnight master orchestrator
    Script 76 - Durable resume ledger
    Script 77 - Morning intelligence
    Script 78 - Resource-aware scheduler
    Script 79 - Workload execution planner
    Script 80 - Autonomous execution controller

Each journal entry records:

    - What decision BACQE made
    - Why the decision was made
    - What job was selected
    - Whether the job was executed
    - Whether execution succeeded
    - What resources were available
    - Whether the pipeline remained verified
    - Whether research and cohort stages were complete
    - Whether human attention was required
    - What BACQE recommended next

Design principles:
    - Append-only institutional memory
    - Idempotent imports
    - Stable event identifiers
    - Evidence-based records
    - No heavy job execution
    - No modification of research outputs
    - Searchable CSV and JSON outputs
    - Human-readable chronological report

Outputs:
    dukascopy_autonomous_research_journal.csv
    dukascopy_autonomous_research_journal_latest.csv
    dukascopy_autonomous_research_journal_summary_latest.csv
    dukascopy_autonomous_research_journal_state_latest.json
    dukascopy_autonomous_research_journal_report_latest.txt

This script does not execute research.
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
    / "dukascopy_autonomous_research_journal"
)

JOURNAL_PATH = (
    REPORT_ROOT
    / "dukascopy_autonomous_research_journal.csv"
)

LATEST_ENTRY_PATH = (
    REPORT_ROOT
    / "dukascopy_autonomous_research_journal_latest.csv"
)

SUMMARY_PATH = (
    REPORT_ROOT
    / "dukascopy_autonomous_research_journal_summary_latest.csv"
)

STATE_PATH = (
    REPORT_ROOT
    / "dukascopy_autonomous_research_journal_state_latest.json"
)

REPORT_PATH = (
    REPORT_ROOT
    / "dukascopy_autonomous_research_journal_report_latest.txt"
)

SELF_TEST_ROOT = REPORT_ROOT / "self_test"

SELF_TEST_RESULTS_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_research_journal_self_test_results_latest.csv"
)

SELF_TEST_STATE_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_research_journal_self_test_state_latest.json"
)

SELF_TEST_REPORT_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_research_journal_self_test_report_latest.txt"
)


ORCHESTRATOR_STATE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_overnight_master_orchestrator"
    / "run_state"
    / "dukascopy_overnight_master_latest.json"
)

ORCHESTRATOR_LEDGER_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_overnight_master_orchestrator"
    / "dukascopy_overnight_job_ledger_latest.csv"
)

DURABLE_STATE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_durable_resume_ledger"
    / "dukascopy_resume_ledger_state_latest.json"
)

DURABLE_LEDGER_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_durable_resume_ledger"
    / "dukascopy_durable_job_ledger.csv"
)

MORNING_STATE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_morning_intelligence"
    / "dukascopy_morning_intelligence_state_latest.json"
)

MORNING_ATTENTION_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_morning_intelligence"
    / "dukascopy_morning_attention_items_latest.csv"
)

SCHEDULER_STATE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_resource_aware_scheduler"
    / "dukascopy_resource_schedule_state_latest.json"
)

PLANNER_STATE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_workload_execution_planner"
    / "dukascopy_workload_execution_state_latest.json"
)

CONTROLLER_DIRECTIVE_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_autonomous_execution_controller"
    / "dukascopy_execution_directive_latest.json"
)

CONTROLLER_HISTORY_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_autonomous_execution_controller"
    / "dukascopy_execution_decision_history.csv"
)

PIPELINE_FAILURES_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_pipeline_verification_engine"
    / "dukascopy_pipeline_verification_failures_latest.csv"
)

EH_SYMBOL_SUMMARY_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_extended_horizons"
    / "research_state_registry"
    / "extended_horizon_research_symbol_summary_latest.csv"
)

GLOBAL_COHORT_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_extended_horizons"
    / "global_cohort_registry"
    / "extended_horizon_global_cohort_registry_latest.csv"
)


JOURNAL_COLUMNS = [
    "event_id",
    "recorded_at",
    "event_type",
    "event_status",
    "severity",
    "run_id",
    "decision_id",
    "directive",
    "controller_status",
    "source",
    "symbol",
    "stage",
    "priority",
    "job_id",
    "command",
    "execution_position",
    "job_started_at",
    "job_finished_at",
    "job_elapsed_seconds",
    "job_return_code",
    "job_status",
    "decision_reason",
    "blocking_reason",
    "orchestrator_status",
    "orchestrator_started_at",
    "orchestrator_finished_at",
    "orchestrator_elapsed_minutes",
    "jobs_started",
    "jobs_succeeded",
    "jobs_failed",
    "stop_reason",
    "scheduler_status",
    "scheduler_candidate_jobs",
    "scheduler_approved_jobs",
    "scheduler_deferred_jobs",
    "estimated_runtime_minutes",
    "estimated_disk_growth_gb",
    "disk_free_gb",
    "disk_used_pct",
    "ram_available_gb",
    "ram_used_pct",
    "planner_status",
    "planner_jobs",
    "planner_blocked_jobs",
    "planner_symbol_switches",
    "durable_ledger_rows",
    "resume_plan_jobs",
    "interrupted_jobs",
    "retry_exhausted_jobs",
    "pipeline_verified",
    "pipeline_failure_count",
    "eh_symbols_complete",
    "eh_symbols_tracked",
    "global_cohort_current",
    "global_stages_current",
    "global_stages_tracked",
    "morning_overall_status",
    "attention_item_count",
    "recommended_next_action",
    "journal_summary",
]


SEVERITY_RANK = {
    "critical": 1,
    "high": 2,
    "medium": 3,
    "low": 4,
    "information": 5,
    "none": 99,
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
            f"Failed reading CSV {path}: "
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
            f"Failed reading JSON {path}: "
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


def stable_event_id(
    event_type: str,
    run_id: str,
    decision_id: str,
    job_id: str,
    directive: str,
    event_status: str,
    job_finished_at: str,
) -> str:
    identity = "|".join(
        [
            clean_text(event_type),
            clean_text(run_id),
            clean_text(decision_id),
            clean_text(job_id),
            clean_text(directive),
            clean_text(event_status),
            clean_text(job_finished_at),
        ]
    )

    return sha256(
        identity.encode("utf-8")
    ).hexdigest()[:24]


def empty_journal() -> pd.DataFrame:
    return pd.DataFrame(
        columns=JOURNAL_COLUMNS
    )


def normalise_journal(
    journal: pd.DataFrame,
) -> pd.DataFrame:
    if journal.empty:
        return empty_journal()

    journal = journal.copy()

    for column in JOURNAL_COLUMNS:
        if column not in journal.columns:
            journal[column] = ""

    return journal[
        JOURNAL_COLUMNS
    ].copy()


def load_journal() -> pd.DataFrame:
    if not JOURNAL_PATH.exists():
        return empty_journal()

    return normalise_journal(
        safe_read_csv(JOURNAL_PATH)
    )


def pipeline_verified_state(
    failures: pd.DataFrame,
) -> tuple[bool, int]:
    failure_count = len(failures)

    return (
        failure_count == 0,
        failure_count,
    )


def extended_horizon_state(
    summary: pd.DataFrame,
) -> tuple[int, int]:
    if summary.empty:
        return 0, 0

    tracked = len(summary)

    if "research_status" not in summary.columns:
        return 0, tracked

    completed = int(
        (
            summary["research_status"]
            .astype(str)
            == "complete"
        ).sum()
    )

    return completed, tracked


def global_cohort_state(
    cohort: pd.DataFrame,
) -> tuple[bool, int, int]:
    if cohort.empty:
        return False, 0, 0

    tracked = len(cohort)

    if "stage_status" not in cohort.columns:
        return False, 0, tracked

    current = int(
        (
            cohort["stage_status"]
            .astype(str)
            == "complete_for_current_cohort"
        ).sum()
    )

    return (
        current == tracked and tracked > 0,
        current,
        tracked,
    )


def attention_item_count(
    attention: pd.DataFrame,
) -> int:
    if attention.empty:
        return 0

    if "priority" not in attention.columns:
        return len(attention)

    return int(
        (
            attention["priority"]
            .astype(str)
            != "none"
        ).sum()
    )


def determine_event_type(
    controller: dict,
    orchestrator: dict,
    job: pd.Series | None,
) -> str:
    directive = clean_text(
        controller.get("directive")
    )

    if job is not None:
        return "job_execution"

    if directive == "execute":
        return "execution_authorisation"

    if directive == "no_work":
        return "no_work_decision"

    if directive == "wait":
        return "wait_decision"

    if directive == "blocked":
        return "blocked_decision"

    if directive == "manual_review":
        return "manual_review_decision"

    if clean_text(
        orchestrator.get("status")
    ) == "complete":
        return "orchestrator_completion"

    return "control_observation"


def determine_event_status(
    controller: dict,
    orchestrator: dict,
    job: pd.Series | None,
) -> str:
    if job is not None:
        return clean_text(
            job.get("status"),
            "unknown",
        )

    directive = clean_text(
        controller.get("directive"),
        "unknown",
    )

    if directive == "execute":
        return "authorised"

    if directive == "no_work":
        return "complete"

    if directive == "wait":
        return "waiting"

    if directive == "blocked":
        return "blocked"

    if directive == "manual_review":
        return "manual_review_required"

    return clean_text(
        orchestrator.get("status"),
        "unknown",
    )


def determine_severity(
    controller: dict,
    orchestrator: dict,
    job: pd.Series | None,
    pipeline_failures: int,
    attention_items: int,
) -> str:
    directive = clean_text(
        controller.get("directive")
    )

    if job is not None:
        return_code = clean_int(
            job.get("return_code"),
            -1,
        )

        status = clean_text(
            job.get("status")
        )

        if return_code != 0 or status == "error":
            return "critical"

        return "information"

    if directive == "manual_review":
        return "critical"

    if directive == "blocked":
        return "high"

    if pipeline_failures > 0:
        return "critical"

    if directive == "wait":
        return "medium"

    if attention_items > 0:
        return "high"

    orchestrator_status = clean_text(
        orchestrator.get("status")
    )

    if orchestrator_status in {
        "failed",
        "blocked_by_controller",
        "manual_review_required",
    }:
        return "critical"

    return "information"


def build_journal_summary(
    event_type: str,
    event_status: str,
    controller: dict,
    orchestrator: dict,
    job: pd.Series | None,
    morning: dict,
) -> str:
    directive = clean_text(
        controller.get("directive"),
        "unknown",
    )

    symbol = clean_text(
        controller.get("symbol"),
        "UNKNOWN",
    )

    stage = clean_text(
        controller.get("stage"),
        "UNKNOWN",
    )

    if job is not None:
        job_symbol = clean_text(
            job.get("symbol"),
            symbol,
        )

        job_stage = clean_text(
            job.get("stage"),
            stage,
        )

        job_status = clean_text(
            job.get("status"),
            event_status,
        )

        return (
            f"BACQE executed {job_stage} for {job_symbol}. "
            f"Execution status: {job_status}."
        )

    if event_type == "no_work_decision":
        return (
            "BACQE inspected the full autonomous control layer and "
            "correctly determined that no executable work remained."
        )

    if event_type == "execution_authorisation":
        return (
            f"BACQE authorised {stage} for {symbol} after the job "
            "passed verification, resource, dependency and retry checks."
        )

    if event_type == "blocked_decision":
        return (
            "BACQE blocked autonomous execution because one or more "
            "safety or dependency conditions were not satisfied."
        )

    if event_type == "manual_review_decision":
        return (
            "BACQE halted autonomous execution and requested human "
            "review before continuing."
        )

    if event_type == "wait_decision":
        return (
            "BACQE elected to wait because another active or unresolved "
            "condition prevented safe execution."
        )

    recommended = clean_text(
        morning.get(
            "recommended_next_action"
        )
    )

    if recommended:
        return recommended

    return (
        f"BACQE recorded event_type={event_type}, "
        f"event_status={event_status}, directive={directive}."
    )


def latest_job_for_controller(
    controller: dict,
    orchestrator_ledger: pd.DataFrame,
) -> pd.Series | None:
    if orchestrator_ledger.empty:
        return None

    controller_job_id = clean_text(
        controller.get("job_id")
    )

    controller_command = clean_text(
        controller.get("command")
    )

    if (
        controller_job_id
        and "job_id"
        in orchestrator_ledger.columns
    ):
        matches = orchestrator_ledger[
            orchestrator_ledger["job_id"]
            .fillna("")
            .astype(str)
            == controller_job_id
        ]

        if not matches.empty:
            return matches.iloc[-1]

    if (
        controller_command
        and "command"
        in orchestrator_ledger.columns
    ):
        matches = orchestrator_ledger[
            orchestrator_ledger["command"]
            .fillna("")
            .astype(str)
            .str.strip()
            == controller_command
        ]

        if not matches.empty:
            return matches.iloc[-1]

    return None


def build_event_row(
    controller: dict,
    orchestrator: dict,
    orchestrator_ledger: pd.DataFrame,
    durable_state: dict,
    scheduler_state: dict,
    planner_state: dict,
    morning_state: dict,
    attention: pd.DataFrame,
    pipeline_failures: pd.DataFrame,
    eh_summary: pd.DataFrame,
    global_cohort: pd.DataFrame,
) -> dict:
    recorded_at = now_text()

    matching_job = latest_job_for_controller(
        controller=controller,
        orchestrator_ledger=orchestrator_ledger,
    )

    pipeline_verified, failure_count = (
        pipeline_verified_state(
            pipeline_failures
        )
    )

    eh_complete, eh_tracked = (
        extended_horizon_state(
            eh_summary
        )
    )

    (
        cohort_current,
        global_current,
        global_tracked,
    ) = global_cohort_state(
        global_cohort
    )

    attention_count = attention_item_count(
        attention
    )

    event_type = determine_event_type(
        controller=controller,
        orchestrator=orchestrator,
        job=matching_job,
    )

    event_status = determine_event_status(
        controller=controller,
        orchestrator=orchestrator,
        job=matching_job,
    )

    severity = determine_severity(
        controller=controller,
        orchestrator=orchestrator,
        job=matching_job,
        pipeline_failures=failure_count,
        attention_items=attention_count,
    )

    resources = scheduler_state.get(
        "resources",
        {},
    )

    run_id = clean_text(
        orchestrator.get("run_id")
    )

    decision_id = clean_text(
        controller.get("decision_id")
    )

    job_id = clean_text(
        controller.get("job_id")
    )

    job_finished_at = (
        clean_text(
            matching_job.get("finished_at")
        )
        if matching_job is not None
        else ""
    )

    directive = clean_text(
        controller.get("directive"),
        "unknown",
    )

    event_id = stable_event_id(
        event_type=event_type,
        run_id=run_id,
        decision_id=decision_id,
        job_id=job_id,
        directive=directive,
        event_status=event_status,
        job_finished_at=job_finished_at,
    )

    summary = build_journal_summary(
        event_type=event_type,
        event_status=event_status,
        controller=controller,
        orchestrator=orchestrator,
        job=matching_job,
        morning=morning_state,
    )

    row = {
        "event_id": event_id,
        "recorded_at": recorded_at,
        "event_type": event_type,
        "event_status": event_status,
        "severity": severity,
        "run_id": run_id,
        "decision_id": decision_id,
        "directive": directive,
        "controller_status": clean_text(
            controller.get("controller_status")
        ),
        "source": clean_text(
            controller.get("source")
        ),
        "symbol": clean_text(
            controller.get("symbol")
        ),
        "stage": clean_text(
            controller.get("stage")
        ),
        "priority": clean_text(
            controller.get("priority")
        ),
        "job_id": job_id,
        "command": clean_text(
            controller.get("command")
        ),
        "execution_position": clean_int(
            controller.get("execution_position"),
            0,
        ),
        "job_started_at": (
            clean_text(
                matching_job.get("started_at")
            )
            if matching_job is not None
            else ""
        ),
        "job_finished_at": job_finished_at,
        "job_elapsed_seconds": (
            clean_float(
                matching_job.get("elapsed_seconds"),
                0.0,
            )
            if matching_job is not None
            else 0.0
        ),
        "job_return_code": (
            clean_int(
                matching_job.get("return_code"),
                -1,
            )
            if matching_job is not None
            else -1
        ),
        "job_status": (
            clean_text(
                matching_job.get("status")
            )
            if matching_job is not None
            else ""
        ),
        "decision_reason": clean_text(
            controller.get("decision_reason")
        ),
        "blocking_reason": clean_text(
            controller.get("blocking_reason")
        ),
        "orchestrator_status": clean_text(
            orchestrator.get("status")
        ),
        "orchestrator_started_at": clean_text(
            orchestrator.get("started_at")
        ),
        "orchestrator_finished_at": clean_text(
            orchestrator.get("finished_at")
        ),
        "orchestrator_elapsed_minutes": clean_float(
            orchestrator.get("elapsed_minutes"),
            0.0,
        ),
        "jobs_started": clean_int(
            orchestrator.get("jobs_started"),
            0,
        ),
        "jobs_succeeded": clean_int(
            orchestrator.get("jobs_succeeded"),
            0,
        ),
        "jobs_failed": clean_int(
            orchestrator.get("jobs_failed"),
            0,
        ),
        "stop_reason": clean_text(
            orchestrator.get("stop_reason")
        ),
        "scheduler_status": clean_text(
            scheduler_state.get("schedule_status")
        ),
        "scheduler_candidate_jobs": clean_int(
            scheduler_state.get("candidate_jobs"),
            0,
        ),
        "scheduler_approved_jobs": clean_int(
            scheduler_state.get("approved_jobs"),
            0,
        ),
        "scheduler_deferred_jobs": clean_int(
            scheduler_state.get("deferred_jobs"),
            0,
        ),
        "estimated_runtime_minutes": clean_int(
            scheduler_state.get(
                "estimated_approved_runtime_minutes"
            ),
            0,
        ),
        "estimated_disk_growth_gb": clean_float(
            scheduler_state.get(
                "estimated_approved_disk_growth_gb"
            ),
            0.0,
        ),
        "disk_free_gb": clean_float(
            resources.get("disk_free_gb"),
            0.0,
        ),
        "disk_used_pct": clean_float(
            resources.get("disk_used_pct"),
            0.0,
        ),
        "ram_available_gb": clean_float(
            resources.get("ram_available_gb"),
            0.0,
        ),
        "ram_used_pct": clean_float(
            resources.get("ram_used_pct"),
            0.0,
        ),
        "planner_status": clean_text(
            planner_state.get("plan_status")
        ),
        "planner_jobs": clean_int(
            planner_state.get("planned_jobs"),
            0,
        ),
        "planner_blocked_jobs": clean_int(
            planner_state.get("blocked_jobs"),
            0,
        ),
        "planner_symbol_switches": clean_int(
            planner_state.get(
                "estimated_symbol_switches"
            ),
            0,
        ),
        "durable_ledger_rows": clean_int(
            durable_state.get("ledger_rows"),
            0,
        ),
        "resume_plan_jobs": clean_int(
            durable_state.get("resume_plan_jobs"),
            0,
        ),
        "interrupted_jobs": clean_int(
            durable_state.get("interrupted_jobs"),
            0,
        ),
        "retry_exhausted_jobs": clean_int(
            durable_state.get(
                "retry_exhausted_jobs"
            ),
            0,
        ),
        "pipeline_verified": pipeline_verified,
        "pipeline_failure_count": failure_count,
        "eh_symbols_complete": eh_complete,
        "eh_symbols_tracked": eh_tracked,
        "global_cohort_current": cohort_current,
        "global_stages_current": global_current,
        "global_stages_tracked": global_tracked,
        "morning_overall_status": clean_text(
            morning_state.get("overall_status")
        ),
        "attention_item_count": attention_count,
        "recommended_next_action": clean_text(
            morning_state.get(
                "recommended_next_action"
            )
        ),
        "journal_summary": summary,
    }

    return {
        column: row.get(
            column,
            "",
        )
        for column in JOURNAL_COLUMNS
    }


def append_event(
    journal: pd.DataFrame,
    event: dict,
) -> tuple[pd.DataFrame, bool]:
    journal = normalise_journal(
        journal
    )

    event_id = clean_text(
        event.get("event_id")
    )

    if (
        not journal.empty
        and event_id
        and bool(
            (
                journal["event_id"]
                .astype(str)
                == event_id
            ).any()
        )
    ):
        return journal, False

    event_frame = pd.DataFrame([event], columns=JOURNAL_COLUMNS, )

    if journal.empty:
        updated = event_frame.copy()
    else:
        updated = pd.concat([journal, event_frame, ], ignore_index=True, )

    return normalise_journal(updated), True


def build_summary(
    journal: pd.DataFrame,
) -> pd.DataFrame:
    if journal.empty:
        return pd.DataFrame(
            columns=[
                "summary_type",
                "summary_key",
                "event_count",
                "latest_recorded_at",
            ]
        )

    frames: list[pd.DataFrame] = []

    for summary_type, column in [
        ("event_type", "event_type"),
        ("event_status", "event_status"),
        ("severity", "severity"),
        ("directive", "directive"),
        ("symbol", "symbol"),
        ("stage", "stage"),
    ]:
        if column not in journal.columns:
            continue

        source = journal[
            journal[column]
            .fillna("")
            .astype(str)
            .str.strip()
            != ""
        ]

        if source.empty:
            continue

        summary = (
            source.groupby(
                column,
                dropna=False,
            )
            .agg(
                event_count=(
                    "event_id",
                    "count",
                ),
                latest_recorded_at=(
                    "recorded_at",
                    "max",
                ),
            )
            .reset_index()
            .rename(
                columns={
                    column: "summary_key"
                }
            )
        )

        summary.insert(
            0,
            "summary_type",
            summary_type,
        )

        frames.append(summary)

    if not frames:
        return pd.DataFrame(
            columns=[
                "summary_type",
                "summary_key",
                "event_count",
                "latest_recorded_at",
            ]
        )

    return pd.concat(
        frames,
        ignore_index=True,
    )


def write_report(
    journal: pd.DataFrame,
    latest_event: dict,
    summary: pd.DataFrame,
    added: bool,
) -> None:
    with open(
        REPORT_PATH,
        "w",
        encoding="utf-8",
    ) as file:
        file.write(
            "BACQE DUKASCOPY 81 - "
            "AUTONOMOUS RESEARCH JOURNAL\n"
        )
        file.write("=" * 110 + "\n\n")

        file.write("JOURNAL UPDATE\n")
        file.write("-" * 110 + "\n")
        file.write(
            f"Generated: {now_text()}\n"
        )
        file.write(
            f"Entry appended: {added}\n"
        )
        file.write(
            f"Total journal entries: "
            f"{len(journal)}\n"
        )

        file.write("\nLATEST EVENT\n")
        file.write("-" * 110 + "\n")

        for column in JOURNAL_COLUMNS:
            file.write(
                f"{column}: "
                f"{latest_event.get(column, '')}\n"
            )

        file.write("\nSUMMARY\n")
        file.write("-" * 110 + "\n")

        if summary.empty:
            file.write(
                "No summary rows available.\n"
            )
        else:
            file.write(
                summary.to_string(
                    index=False
                )
            )

        file.write("\n\nRECENT JOURNAL HISTORY\n")
        file.write("-" * 110 + "\n")

        if journal.empty:
            file.write(
                "No journal entries available.\n"
            )
        else:
            display_columns = [
                "recorded_at",
                "event_type",
                "event_status",
                "severity",
                "run_id",
                "directive",
                "symbol",
                "stage",
                "journal_summary",
            ]

            file.write(
                journal.tail(25)[
                    display_columns
                ].to_string(
                    index=False
                )
            )


def write_outputs(
    journal: pd.DataFrame,
    latest_event: dict,
    added: bool,
) -> None:
    journal = normalise_journal(
        journal
    )

    journal.to_csv(
        JOURNAL_PATH,
        index=False,
    )

    pd.DataFrame(
        [latest_event]
    )[JOURNAL_COLUMNS].to_csv(
        LATEST_ENTRY_PATH,
        index=False,
    )

    summary = build_summary(
        journal
    )

    summary.to_csv(
        SUMMARY_PATH,
        index=False,
    )

    severity_counts = (
        journal["severity"]
        .value_counts()
        .to_dict()
        if not journal.empty
        else {}
    )

    event_type_counts = (
        journal["event_type"]
        .value_counts()
        .to_dict()
        if not journal.empty
        else {}
    )

    state = {
        "generated_at": now_text(),
        "entry_appended": added,
        "latest_event_id": clean_text(
            latest_event.get("event_id")
        ),
        "latest_event_type": clean_text(
            latest_event.get("event_type")
        ),
        "latest_event_status": clean_text(
            latest_event.get("event_status")
        ),
        "latest_severity": clean_text(
            latest_event.get("severity")
        ),
        "journal_entries": len(journal),
        "unique_runs": (
            int(
                journal["run_id"]
                .replace("", pd.NA)
                .dropna()
                .nunique()
            )
            if not journal.empty
            else 0
        ),
        "executed_jobs": (
            int(
                (
                    journal["event_type"]
                    == "job_execution"
                ).sum()
            )
            if not journal.empty
            else 0
        ),
        "no_work_decisions": (
            int(
                (
                    journal["event_type"]
                    == "no_work_decision"
                ).sum()
            )
            if not journal.empty
            else 0
        ),
        "critical_events": clean_int(
            severity_counts.get(
                "critical",
                0,
            )
        ),
        "severity_counts": severity_counts,
        "event_type_counts": event_type_counts,
    }

    STATE_PATH.write_text(
        json.dumps(
            state,
            indent=2,
        ),
        encoding="utf-8",
    )

    write_report(
        journal=journal,
        latest_event=latest_event,
        summary=summary,
        added=added,
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


def synthetic_context() -> dict:
    return {
        "controller": {
            "decision_id": "synthetic_decision_001",
            "directive": "no_work",
            "controller_status": "healthy_no_work",
            "decision_reason": (
                "No executable workload is currently planned."
            ),
            "blocking_reason": "",
            "source": "",
            "symbol": "",
            "stage": "",
            "priority": "",
            "job_id": "",
            "command": "",
            "execution_position": 0,
        },
        "orchestrator": {
            "run_id": "synthetic_run_001",
            "status": "complete",
            "started_at": "2026-01-01 22:00:00",
            "finished_at": "2026-01-01 22:10:00",
            "elapsed_minutes": 10.0,
            "jobs_started": 0,
            "jobs_succeeded": 0,
            "jobs_failed": 0,
            "stop_reason": (
                "Script 80 confirmed that no executable "
                "workload remains."
            ),
        },
        "durable_state": {
            "ledger_rows": 0,
            "resume_plan_jobs": 0,
            "interrupted_jobs": 0,
            "retry_exhausted_jobs": 0,
        },
        "scheduler_state": {
            "schedule_status": "no_work",
            "candidate_jobs": 0,
            "approved_jobs": 0,
            "deferred_jobs": 0,
            "estimated_approved_runtime_minutes": 0,
            "estimated_approved_disk_growth_gb": 0.0,
            "resources": {
                "disk_free_gb": 4500.0,
                "disk_used_pct": 60.0,
                "ram_available_gb": 50.0,
                "ram_used_pct": 20.0,
            },
        },
        "planner_state": {
            "plan_status": "no_work",
            "planned_jobs": 0,
            "blocked_jobs": 0,
            "estimated_symbol_switches": 0,
        },
        "morning_state": {
            "overall_status": "healthy",
            "recommended_next_action": (
                "Continue with the next scheduled BACQE "
                "research priority."
            ),
        },
    }


def run_self_test() -> int:
    """
    Run non-damaging in-memory journal tests.

    No genuine journal or BACQE control files are modified.
    """

    ensure_output_directories()

    results: list[dict] = []

    context = synthetic_context()

    event = build_event_row(
        controller=context["controller"],
        orchestrator=context["orchestrator"],
        orchestrator_ledger=pd.DataFrame(),
        durable_state=context["durable_state"],
        scheduler_state=context["scheduler_state"],
        planner_state=context["planner_state"],
        morning_state=context["morning_state"],
        attention=pd.DataFrame(),
        pipeline_failures=pd.DataFrame(),
        eh_summary=pd.DataFrame(
            [
                {
                    "symbol": "EURUSD",
                    "research_status": "complete",
                }
            ]
        ),
        global_cohort=pd.DataFrame(
            [
                {
                    "stage_key": "EH11",
                    "stage_status": (
                        "complete_for_current_cohort"
                    ),
                }
            ]
        ),
    )

    results.append(
        test_result(
            test_name="no_work_event_type",
            passed=(
                event["event_type"]
                == "no_work_decision"
            ),
            expected="no_work_decision",
            observed=event["event_type"],
        )
    )

    results.append(
        test_result(
            test_name="healthy_event_severity",
            passed=(
                event["severity"]
                == "information"
            ),
            expected="information",
            observed=event["severity"],
        )
    )

    journal = empty_journal()

    journal, first_added = append_event(
        journal,
        event,
    )

    journal, duplicate_added = append_event(
        journal,
        event,
    )

    results.append(
        test_result(
            test_name="first_event_appended",
            passed=(
                first_added
                and len(journal) == 1
            ),
            expected="True and 1 row",
            observed=(
                f"{first_added} and "
                f"{len(journal)} row(s)"
            ),
        )
    )

    results.append(
        test_result(
            test_name="duplicate_event_rejected",
            passed=(
                not duplicate_added
                and len(journal) == 1
            ),
            expected="False and 1 row",
            observed=(
                f"{duplicate_added} and "
                f"{len(journal)} row(s)"
            ),
        )
    )

    controller_execute = {
        "decision_id": "synthetic_decision_002",
        "directive": "execute",
        "controller_status": "execution_authorised",
        "decision_reason": "Synthetic authorised execution.",
        "blocking_reason": "",
        "source": "onboarding_engine",
        "symbol": "EURUSD",
        "stage": "EH03",
        "priority": "high",
        "job_id": "synthetic_job_001",
        "command": "python SELF_TEST_ONLY_JOB.py",
        "execution_position": 1,
    }

    orchestrator_ledger = pd.DataFrame(
        [
            {
                "job_id": "synthetic_job_001",
                "symbol": "EURUSD",
                "stage": "EH03",
                "command": "python SELF_TEST_ONLY_JOB.py",
                "started_at": "2026-01-01 22:01:00",
                "finished_at": "2026-01-01 22:05:00",
                "elapsed_seconds": 240.0,
                "return_code": 0,
                "status": "ok",
            }
        ]
    )

    execution_event = build_event_row(
        controller=controller_execute,
        orchestrator={
            **context["orchestrator"],
            "run_id": "synthetic_run_002",
            "jobs_started": 1,
            "jobs_succeeded": 1,
        },
        orchestrator_ledger=orchestrator_ledger,
        durable_state=context["durable_state"],
        scheduler_state={
            **context["scheduler_state"],
            "schedule_status": "ready",
            "candidate_jobs": 1,
            "approved_jobs": 1,
        },
        planner_state={
            **context["planner_state"],
            "plan_status": "ready",
            "planned_jobs": 1,
        },
        morning_state=context["morning_state"],
        attention=pd.DataFrame(),
        pipeline_failures=pd.DataFrame(),
        eh_summary=pd.DataFrame(),
        global_cohort=pd.DataFrame(),
    )

    results.append(
        test_result(
            test_name="job_execution_detected",
            passed=(
                execution_event["event_type"]
                == "job_execution"
                and execution_event["job_status"]
                == "ok"
            ),
            expected="job_execution and ok",
            observed=(
                f"{execution_event['event_type']} "
                f"and {execution_event['job_status']}"
            ),
        )
    )

    failure_ledger = orchestrator_ledger.copy()

    failure_ledger.loc[
        0,
        "return_code",
    ] = 1

    failure_ledger.loc[
        0,
        "status",
    ] = "error"

    failure_event = build_event_row(
        controller=controller_execute,
        orchestrator={
            **context["orchestrator"],
            "run_id": "synthetic_run_003",
            "jobs_started": 1,
            "jobs_failed": 1,
        },
        orchestrator_ledger=failure_ledger,
        durable_state=context["durable_state"],
        scheduler_state=context["scheduler_state"],
        planner_state=context["planner_state"],
        morning_state=context["morning_state"],
        attention=pd.DataFrame(),
        pipeline_failures=pd.DataFrame(),
        eh_summary=pd.DataFrame(),
        global_cohort=pd.DataFrame(),
    )

    results.append(
        test_result(
            test_name="failed_job_critical",
            passed=(
                failure_event["severity"]
                == "critical"
            ),
            expected="critical",
            observed=failure_event["severity"],
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
        "real_journal_modified": False,
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
            "BACQE DUKASCOPY 81 - "
            "AUTONOMOUS RESEARCH JOURNAL SELF-TEST\n"
        )
        file.write("=" * 100 + "\n\n")
        file.write(
            "Synthetic in-memory journal events only.\n"
        )
        file.write(
            "No real BACQE journal was modified.\n"
        )
        file.write(
            "No real control output was modified.\n"
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
        "BACQE DUKASCOPY 81 - "
        "AUTONOMOUS RESEARCH JOURNAL SELF-TEST"
    )
    print("=" * 100)
    print(
        "Synthetic in-memory events only: True"
    )
    print(
        "Real journal modified:            False"
    )
    print(
        "Real control files modified:      False"
    )
    print(
        "Synthetic commands executed:      False"
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
    print(
        f"Tests failed: {failed}"
    )

    if failed == 0:
        print(
            "[PASS] All autonomous-journal tests passed."
        )
    else:
        print(
            "[FAIL] One or more journal tests failed."
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


def main() -> None:
    ensure_output_directories()

    controller = safe_read_json(
        CONTROLLER_DIRECTIVE_PATH
    )

    orchestrator = safe_read_json(
        ORCHESTRATOR_STATE_PATH
    )

    orchestrator_ledger = safe_read_csv(
        ORCHESTRATOR_LEDGER_PATH
    )

    durable_state = safe_read_json(
        DURABLE_STATE_PATH
    )

    scheduler_state = safe_read_json(
        SCHEDULER_STATE_PATH
    )

    planner_state = safe_read_json(
        PLANNER_STATE_PATH
    )

    morning_state = safe_read_json(
        MORNING_STATE_PATH
    )

    attention = safe_read_csv(
        MORNING_ATTENTION_PATH
    )

    pipeline_failures = safe_read_csv(
        PIPELINE_FAILURES_PATH
    )

    eh_summary = safe_read_csv(
        EH_SYMBOL_SUMMARY_PATH
    )

    global_cohort = safe_read_csv(
        GLOBAL_COHORT_PATH
    )

    event = build_event_row(
        controller=controller,
        orchestrator=orchestrator,
        orchestrator_ledger=orchestrator_ledger,
        durable_state=durable_state,
        scheduler_state=scheduler_state,
        planner_state=planner_state,
        morning_state=morning_state,
        attention=attention,
        pipeline_failures=pipeline_failures,
        eh_summary=eh_summary,
        global_cohort=global_cohort,
    )

    journal = load_journal()

    journal, added = append_event(
        journal=journal,
        event=event,
    )

    write_outputs(
        journal=journal,
        latest_event=event,
        added=added,
    )

    print("=" * 110)
    print(
        "BACQE DUKASCOPY 81 - "
        "AUTONOMOUS RESEARCH JOURNAL"
    )
    print("=" * 110)
    print(
        f"Event appended:          {added}"
    )
    print(
        f"Event ID:                "
        f"{event['event_id']}"
    )
    print(
        f"Event type:              "
        f"{event['event_type']}"
    )
    print(
        f"Event status:            "
        f"{event['event_status']}"
    )
    print(
        f"Severity:                "
        f"{event['severity']}"
    )
    print(
        f"Run ID:                  "
        f"{event['run_id']}"
    )
    print(
        f"Directive:               "
        f"{event['directive']}"
    )
    print(
        f"Symbol / stage:          "
        f"{event['symbol'] or '-'} / "
        f"{event['stage'] or '-'}"
    )
    print(
        f"Pipeline verified:       "
        f"{event['pipeline_verified']}"
    )
    print(
        f"EH symbols complete:     "
        f"{event['eh_symbols_complete']}/"
        f"{event['eh_symbols_tracked']}"
    )
    print(
        f"Global cohort current:   "
        f"{event['global_cohort_current']}"
    )
    print(
        f"Attention items:         "
        f"{event['attention_item_count']}"
    )
    print("-" * 110)
    print("JOURNAL SUMMARY")
    print(
        event["journal_summary"]
    )
    print("-" * 110)
    print(
        f"Journal entries:         "
        f"{len(journal)}"
    )
    print(
        f"Journal:       {JOURNAL_PATH}"
    )
    print(
        f"Latest entry:  {LATEST_ENTRY_PATH}"
    )
    print(
        f"Summary:       {SUMMARY_PATH}"
    )
    print(
        f"State:         {STATE_PATH}"
    )
    print(
        f"Report:        {REPORT_PATH}"
    )
    print("=" * 110)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--self-test",
        action="store_true",
        help=(
            "Run deterministic, non-damaging in-memory "
            "journal tests."
        ),
    )

    args = parser.parse_args()

    if args.self_test:
        raise SystemExit(
            run_self_test()
        )

    main()