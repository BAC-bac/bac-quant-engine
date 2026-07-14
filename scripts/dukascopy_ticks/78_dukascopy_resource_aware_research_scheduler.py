"""
BACQE DUKASCOPY 78 - RESOURCE-AWARE RESEARCH SCHEDULER

Purpose:
    Convert the current BACQE Dukascopy action queues into a safe,
    resource-aware overnight execution schedule.

Reads:
    Script 68 - Recovery plan
    Script 73 - Global cohort actions
    Script 74 - Symbol onboarding actions
    Script 76 - Durable resume plan

Inspects:
    - Available disk space
    - Available system memory, when psutil is installed
    - Configured runtime budget
    - Maximum scheduled jobs
    - Per-stage estimated runtime, RAM use and disk growth

Outputs:
    - Full scheduling registry
    - Approved overnight schedule
    - Deferred jobs
    - Command file
    - JSON state
    - Human-readable report

This script does not execute research.
It decides what Script 75 may safely execute.
"""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from pathlib import Path
import argparse
import json
import shutil

import pandas as pd


try:
    import psutil
except ImportError:
    psutil = None


BASE_DIR = Path("E:/Quant_Lab")
ANALYSIS_ROOT = BASE_DIR / "data" / "analysis"

REPORT_ROOT = (
    ANALYSIS_ROOT
    / "dukascopy_resource_aware_scheduler"
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

RESUME_PLAN_PATH = (
    ANALYSIS_ROOT
    / "dukascopy_durable_resume_ledger"
    / "dukascopy_resume_plan_latest.csv"
)


REGISTRY_PATH = (
    REPORT_ROOT
    / "dukascopy_resource_schedule_registry_latest.csv"
)

SCHEDULE_PATH = (
    REPORT_ROOT
    / "dukascopy_approved_overnight_schedule_latest.csv"
)

DEFERRED_PATH = (
    REPORT_ROOT
    / "dukascopy_deferred_schedule_latest.csv"
)

COMMANDS_PATH = (
    REPORT_ROOT
    / "dukascopy_approved_overnight_commands_latest.txt"
)

STATE_PATH = (
    REPORT_ROOT
    / "dukascopy_resource_schedule_state_latest.json"
)

REPORT_PATH = (
    REPORT_ROOT
    / "dukascopy_resource_schedule_report_latest.txt"
)

SELF_TEST_ROOT = REPORT_ROOT / "self_test"

SELF_TEST_RESULTS_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_scheduler_self_test_results_latest.csv"
)

SELF_TEST_REPORT_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_scheduler_self_test_report_latest.txt"
)

SELF_TEST_STATE_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_scheduler_self_test_state_latest.json"
)


PRIORITY_RANK = {
    "critical": 1,
    "high": 2,
    "medium": 3,
    "low": 4,
    "complete": 99,
    "blocked": 100,
}


SOURCE_RANK = {
    "durable_resume_ledger": 1,
    "recovery_engine": 2,
    "onboarding_engine": 3,
    "global_cohort_decision_engine": 4,
}


# Conservative first-pass estimates.
#
# These are planning estimates, not promises. They can later be replaced
# with empirical medians calculated from Script 75's durable history.
STAGE_RESOURCE_PROFILES = {
    "RAW": {
        "runtime_minutes": 180,
        "ram_gb": 2.0,
        "disk_growth_gb": 80.0,
        "weight": "heavy",
    },
    "08": {
        "runtime_minutes": 180,
        "ram_gb": 4.0,
        "disk_growth_gb": 60.0,
        "weight": "heavy",
    },
    "09": {
        "runtime_minutes": 120,
        "ram_gb": 4.0,
        "disk_growth_gb": 30.0,
        "weight": "heavy",
    },
    "10": {
        "runtime_minutes": 150,
        "ram_gb": 5.0,
        "disk_growth_gb": 30.0,
        "weight": "heavy",
    },
    "23": {
        "runtime_minutes": 180,
        "ram_gb": 8.0,
        "disk_growth_gb": 50.0,
        "weight": "heavy",
    },
    "30": {
        "runtime_minutes": 180,
        "ram_gb": 8.0,
        "disk_growth_gb": 50.0,
        "weight": "heavy",
    },
    "EH01": {
        "runtime_minutes": 90,
        "ram_gb": 6.0,
        "disk_growth_gb": 20.0,
        "weight": "heavy",
    },
    "EH02": {
        "runtime_minutes": 240,
        "ram_gb": 10.0,
        "disk_growth_gb": 20.0,
        "weight": "very_heavy",
    },
    "EH03": {
        "runtime_minutes": 30,
        "ram_gb": 4.0,
        "disk_growth_gb": 2.0,
        "weight": "medium",
    },
    "EH04": {
        "runtime_minutes": 60,
        "ram_gb": 6.0,
        "disk_growth_gb": 5.0,
        "weight": "medium",
    },
    "EH05": {
        "runtime_minutes": 60,
        "ram_gb": 6.0,
        "disk_growth_gb": 5.0,
        "weight": "medium",
    },
    "EH06": {
        "runtime_minutes": 120,
        "ram_gb": 8.0,
        "disk_growth_gb": 8.0,
        "weight": "heavy",
    },
    "EH07": {
        "runtime_minutes": 180,
        "ram_gb": 10.0,
        "disk_growth_gb": 10.0,
        "weight": "very_heavy",
    },
    "EH08": {
        "runtime_minutes": 120,
        "ram_gb": 8.0,
        "disk_growth_gb": 10.0,
        "weight": "heavy",
    },
    "EH09": {
        "runtime_minutes": 180,
        "ram_gb": 10.0,
        "disk_growth_gb": 15.0,
        "weight": "very_heavy",
    },
    "EH10": {
        "runtime_minutes": 60,
        "ram_gb": 6.0,
        "disk_growth_gb": 5.0,
        "weight": "medium",
    },
    "EH11": {
        "runtime_minutes": 120,
        "ram_gb": 8.0,
        "disk_growth_gb": 5.0,
        "weight": "heavy",
    },
    "EH12": {
        "runtime_minutes": 120,
        "ram_gb": 8.0,
        "disk_growth_gb": 5.0,
        "weight": "heavy",
    },
    "EH13": {
        "runtime_minutes": 30,
        "ram_gb": 4.0,
        "disk_growth_gb": 2.0,
        "weight": "medium",
    }, # Synthetic stages used only by --self-test.
    # These can never be generated by the real BACQE pipelines.
    "SELFTEST_SHORT": {"runtime_minutes": 10, "ram_gb": 1.0, "disk_growth_gb": 1.0, "weight": "self_test", },
    "SELFTEST_DISK": {"runtime_minutes": 10, "ram_gb": 1.0, "disk_growth_gb": 150.0, "weight": "self_test", },
    "SELFTEST_RAM": {"runtime_minutes": 10, "ram_gb": 12.0, "disk_growth_gb": 1.0, "weight": "self_test", },
}


DEFAULT_RESOURCE_PROFILE = {
    "runtime_minutes": 60,
    "ram_gb": 4.0,
    "disk_growth_gb": 5.0,
    "weight": "unknown",
}


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_output_directory() -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as exc:
        raise RuntimeError(
            f"Unable to read {path}: {type(exc).__name__}: {exc}"
        ) from exc


def clean_text(value: object, default: str = "") -> str:
    if pd.isna(value):
        return default

    text = str(value).strip()

    if not text or text.lower() == "nan":
        return default

    return text


def clean_int(value: object, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return default

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
            source.lower().strip(),
            symbol.upper().strip(),
            stage.upper().strip(),
            " ".join(command.split()),
        ]
    )

    return sha256(
        identity.encode("utf-8")
    ).hexdigest()[:20]


def system_resources() -> dict:
    disk = shutil.disk_usage(BASE_DIR)

    disk_total_gb = disk.total / (1024 ** 3)
    disk_used_gb = disk.used / (1024 ** 3)
    disk_free_gb = disk.free / (1024 ** 3)

    resources = {
        "disk_total_gb": round(disk_total_gb, 2),
        "disk_used_gb": round(disk_used_gb, 2),
        "disk_free_gb": round(disk_free_gb, 2),
        "disk_used_pct": round(
            disk.used / disk.total * 100,
            2,
        ),
        "memory_inspection_available": psutil is not None,
        "ram_total_gb": None,
        "ram_available_gb": None,
        "ram_used_pct": None,
    }

    if psutil is not None:
        memory = psutil.virtual_memory()

        resources.update(
            {
                "ram_total_gb": round(
                    memory.total / (1024 ** 3),
                    2,
                ),
                "ram_available_gb": round(
                    memory.available / (1024 ** 3),
                    2,
                ),
                "ram_used_pct": round(
                    memory.percent,
                    2,
                ),
            }
        )

    return resources


def rows_from_resume_plan() -> list[dict]:
    df = safe_read_csv(RESUME_PLAN_PATH)

    if df.empty:
        return []

    if "resume_eligible" in df.columns:
        eligible = (
            df["resume_eligible"]
            .astype(str)
            .str.lower()
            .isin(["true", "1", "yes"])
        )

        df = df[eligible]

    rows = []

    for _, row in df.iterrows():
        command = clean_text(row.get("command"))

        if not command:
            continue

        rows.append(
            {
                "source": "durable_resume_ledger",
                "symbol": clean_text(
                    row.get("symbol"),
                    "UNKNOWN",
                ),
                "stage": clean_text(
                    row.get("stage"),
                    "UNKNOWN",
                ),
                "priority": clean_text(
                    row.get("priority"),
                    "medium",
                ),
                "command": command,
                "attempt_count": clean_int(
                    row.get("attempt_count"),
                    0,
                ),
                "max_attempts": clean_int(
                    row.get("max_attempts"),
                    3,
                ),
            }
        )

    return rows


def rows_from_recovery_plan() -> list[dict]:
    df = safe_read_csv(RECOVERY_PLAN_PATH)

    if df.empty:
        return []

    if "recovery_status" in df.columns:
        df = df[
            df["recovery_status"].astype(str)
            == "planned"
        ]

    rows = []

    for _, row in df.iterrows():
        command = clean_text(
            row.get("recovery_command")
        )

        if not command:
            continue

        rows.append(
            {
                "source": "recovery_engine",
                "symbol": clean_text(
                    row.get("symbol"),
                    "UNKNOWN",
                ),
                "stage": clean_text(
                    row.get("script_stage"),
                    "UNKNOWN",
                ),
                "priority": clean_text(
                    row.get("priority"),
                    "critical",
                ),
                "command": command,
                "attempt_count": 0,
                "max_attempts": 3,
            }
        )

    return rows


def rows_from_onboarding_actions() -> list[dict]:
    df = safe_read_csv(ONBOARDING_ACTIONS_PATH)

    if df.empty:
        return []

    if "onboarding_status" in df.columns:
        df = df[
            df["onboarding_status"]
            .astype(str)
            .isin(
                [
                    "action_required",
                    "definition_error",
                ]
            )
        ]

    rows = []

    for _, row in df.iterrows():
        command = clean_text(row.get("command"))

        if not command:
            continue

        rows.append(
            {
                "source": "onboarding_engine",
                "symbol": clean_text(
                    row.get("symbol"),
                    "UNKNOWN",
                ),
                "stage": clean_text(
                    row.get("next_stage"),
                    "UNKNOWN",
                ),
                "priority": clean_text(
                    row.get("priority"),
                    "medium",
                ),
                "command": command,
                "attempt_count": 0,
                "max_attempts": 3,
            }
        )

    return rows


def rows_from_global_actions() -> list[dict]:
    df = safe_read_csv(GLOBAL_ACTIONS_PATH)

    if df.empty:
        return []

    if "decision_status" in df.columns:
        df = df[
            df["decision_status"].astype(str)
            == "action_required"
        ]

    rows = []

    for _, row in df.iterrows():
        command = clean_text(row.get("command"))

        if not command:
            continue

        rows.append(
            {
                "source": (
                    "global_cohort_decision_engine"
                ),
                "symbol": "GLOBAL",
                "stage": clean_text(
                    row.get("stage_key"),
                    "UNKNOWN",
                ),
                "priority": clean_text(
                    row.get("priority"),
                    "medium",
                ),
                "command": command,
                "attempt_count": 0,
                "max_attempts": 3,
            }
        )

    return rows


def prepare_candidate_jobs(
    rows: list[dict],
) -> pd.DataFrame:
    """
    Convert action dictionaries into the canonical scheduler job table.

    This function is shared by:
        - the real BACQE action collectors;
        - the non-damaging in-memory self-test suite.
    """

    columns = [
        "job_id",
        "source",
        "symbol",
        "stage",
        "priority",
        "command",
        "attempt_count",
        "max_attempts",
        "source_rank",
        "priority_rank",
    ]

    if not rows:
        return pd.DataFrame(columns=columns)

    jobs = pd.DataFrame(rows)

    jobs["job_id"] = jobs.apply(
        lambda row: stable_job_id(
            source=clean_text(row["source"]),
            symbol=clean_text(
                row["symbol"],
                "UNKNOWN",
            ),
            stage=clean_text(
                row["stage"],
                "UNKNOWN",
            ),
            command=clean_text(row["command"]),
        ),
        axis=1,
    )

    jobs["source_rank"] = (
        jobs["source"]
        .map(SOURCE_RANK)
        .fillna(50)
        .astype(int)
    )

    jobs["priority_rank"] = (
        jobs["priority"]
        .map(PRIORITY_RANK)
        .fillna(50)
        .astype(int)
    )

    jobs = jobs.sort_values(
        by=[
            "source_rank",
            "priority_rank",
            "attempt_count",
        ],
        ascending=[
            True,
            True,
            True,
        ],
    )

    # The same command may appear in several action sources.
    # Retain the highest-authority source only.
    jobs = jobs.drop_duplicates(
        subset=["symbol", "stage", "command"],
        keep="first",
    )

    return jobs.reset_index(drop=True)


def collect_candidate_jobs() -> pd.DataFrame:
    rows = (
        rows_from_resume_plan()
        + rows_from_recovery_plan()
        + rows_from_onboarding_actions()
        + rows_from_global_actions()
    )

    return prepare_candidate_jobs(rows)


def profile_for_stage(stage: str) -> dict:
    return STAGE_RESOURCE_PROFILES.get(
        stage.upper(),
        DEFAULT_RESOURCE_PROFILE,
    )


def add_resource_estimates(
    jobs: pd.DataFrame,
) -> pd.DataFrame:
    if jobs.empty:
        return jobs.copy()

    jobs = jobs.copy()

    profiles = jobs["stage"].apply(
        profile_for_stage
    )

    jobs["estimated_runtime_minutes"] = (
        profiles.apply(
            lambda profile: profile[
                "runtime_minutes"
            ]
        )
    )

    jobs["estimated_ram_gb"] = (
        profiles.apply(
            lambda profile: profile["ram_gb"]
        )
    )

    jobs["estimated_disk_growth_gb"] = (
        profiles.apply(
            lambda profile: profile[
                "disk_growth_gb"
            ]
        )
    )

    jobs["job_weight"] = (
        profiles.apply(
            lambda profile: profile["weight"]
        )
    )

    return jobs


def schedule_jobs(
    jobs: pd.DataFrame,
    resources: dict,
    max_runtime_minutes: int,
    max_jobs: int,
    minimum_free_disk_gb: float,
    minimum_available_ram_gb: float,
    disk_safety_buffer_gb: float,
) -> pd.DataFrame:
    if jobs.empty:
        columns = list(jobs.columns) + [
            "schedule_status",
            "schedule_position",
            "cumulative_runtime_minutes",
            "cumulative_disk_growth_gb",
            "decision_reason",
        ]

        return pd.DataFrame(columns=columns)

    jobs = jobs.copy()

    jobs = jobs.sort_values(
        by=[
            "priority_rank",
            "source_rank",
            "attempt_count",
            "estimated_runtime_minutes",
            "symbol",
            "stage",
        ],
        ascending=[
            True,
            True,
            True,
            True,
            True,
            True,
        ],
    )

    scheduled_count = 0
    cumulative_runtime = 0
    cumulative_disk_growth = 0.0

    schedule_statuses = []
    positions = []
    cumulative_runtimes = []
    cumulative_disk_values = []
    reasons = []

    available_ram_gb = resources.get(
        "ram_available_gb"
    )

    disk_free_gb = float(
        resources["disk_free_gb"]
    )

    for _, row in jobs.iterrows():
        runtime = int(
            row["estimated_runtime_minutes"]
        )

        ram_gb = float(
            row["estimated_ram_gb"]
        )

        disk_growth_gb = float(
            row["estimated_disk_growth_gb"]
        )

        next_runtime = (
            cumulative_runtime + runtime
        )

        next_disk_growth = (
            cumulative_disk_growth
            + disk_growth_gb
        )

        projected_free_disk = (
            disk_free_gb
            - next_disk_growth
            - disk_safety_buffer_gb
        )

        status = "approved"
        reason = (
            "Job fits within the configured overnight "
            "resource budget."
        )

        if scheduled_count >= max_jobs:
            status = "deferred_job_limit"
            reason = (
                f"Maximum scheduled-job limit of "
                f"{max_jobs} reached."
            )

        elif next_runtime > max_runtime_minutes:
            status = "deferred_runtime_budget"
            reason = (
                f"Scheduling the job would increase estimated "
                f"runtime to {next_runtime} minutes, above the "
                f"{max_runtime_minutes}-minute budget."
            )

        elif disk_free_gb < minimum_free_disk_gb:
            status = "blocked_low_disk"
            reason = (
                f"Current free disk space is "
                f"{disk_free_gb:.2f} GB, below the minimum "
                f"{minimum_free_disk_gb:.2f} GB."
            )

        elif projected_free_disk < minimum_free_disk_gb:
            status = "deferred_disk_budget"
            reason = (
                f"Projected free disk after this schedule would "
                f"be {projected_free_disk:.2f} GB, below the "
                f"{minimum_free_disk_gb:.2f} GB minimum."
            )

        elif (
            available_ram_gb is not None
            and available_ram_gb
            < minimum_available_ram_gb
        ):
            status = "blocked_low_ram"
            reason = (
                f"Current available RAM is "
                f"{available_ram_gb:.2f} GB, below the "
                f"{minimum_available_ram_gb:.2f} GB minimum."
            )

        elif (
            available_ram_gb is not None
            and ram_gb > available_ram_gb
        ):
            status = "deferred_ram_requirement"
            reason = (
                f"Estimated job RAM requirement is "
                f"{ram_gb:.2f} GB but only "
                f"{available_ram_gb:.2f} GB is currently "
                "available."
            )

        elif (
            clean_int(row["attempt_count"], 0)
            >= clean_int(row["max_attempts"], 3)
        ):
            status = "blocked_retry_exhausted"
            reason = (
                "The job has reached its retry limit."
            )

        if status == "approved":
            scheduled_count += 1
            cumulative_runtime = next_runtime
            cumulative_disk_growth = (
                next_disk_growth
            )
            position = scheduled_count
        else:
            position = 0

        schedule_statuses.append(status)
        positions.append(position)
        cumulative_runtimes.append(
            cumulative_runtime
        )
        cumulative_disk_values.append(
            round(cumulative_disk_growth, 2)
        )
        reasons.append(reason)

    jobs["schedule_status"] = (
        schedule_statuses
    )

    jobs["schedule_position"] = positions

    jobs["cumulative_runtime_minutes"] = (
        cumulative_runtimes
    )

    jobs["cumulative_disk_growth_gb"] = (
        cumulative_disk_values
    )

    jobs["decision_reason"] = reasons

    return jobs


def build_state(
    registry: pd.DataFrame,
    resources: dict,
    max_runtime_minutes: int,
    max_jobs: int,
    minimum_free_disk_gb: float,
    minimum_available_ram_gb: float,
    disk_safety_buffer_gb: float,
) -> dict:
    approved = registry[
        registry["schedule_status"]
        == "approved"
    ]

    deferred = registry[
        registry["schedule_status"]
        != "approved"
    ]

    return {
        "generated_at": now_text(),
        "candidate_jobs": len(registry),
        "approved_jobs": len(approved),
        "deferred_jobs": len(deferred),
        "estimated_approved_runtime_minutes": (
            int(
                approved[
                    "estimated_runtime_minutes"
                ].sum()
            )
            if not approved.empty
            else 0
        ),
        "estimated_approved_disk_growth_gb": (
            round(
                approved[
                    "estimated_disk_growth_gb"
                ].sum(),
                2,
            )
            if not approved.empty
            else 0.0
        ),
        "runtime_budget_minutes": (
            max_runtime_minutes
        ),
        "maximum_jobs": max_jobs,
        "minimum_free_disk_gb": (
            minimum_free_disk_gb
        ),
        "minimum_available_ram_gb": (
            minimum_available_ram_gb
        ),
        "disk_safety_buffer_gb": (
            disk_safety_buffer_gb
        ),
        "resources": resources,
        "schedule_status": (
            "ready"
            if len(approved) > 0
            else "no_work"
            if len(registry) == 0
            else "all_jobs_deferred"
        ),
    }


def write_outputs(
    registry: pd.DataFrame,
    state: dict,
) -> None:
    registry.to_csv(
        REGISTRY_PATH,
        index=False,
    )

    approved = registry[
        registry["schedule_status"]
        == "approved"
    ].copy()

    approved = approved.sort_values(
        by="schedule_position"
    )

    deferred = registry[
        registry["schedule_status"]
        != "approved"
    ].copy()

    approved.to_csv(
        SCHEDULE_PATH,
        index=False,
    )

    deferred.to_csv(
        DEFERRED_PATH,
        index=False,
    )

    with open(
        COMMANDS_PATH,
        "w",
        encoding="utf-8",
    ) as file:
        if approved.empty:
            file.write(
                "# No jobs approved for overnight execution.\n"
            )
        else:
            for command in approved["command"]:
                file.write(
                    clean_text(command) + "\n"
                )

    STATE_PATH.write_text(
        json.dumps(
            state,
            indent=2,
        ),
        encoding="utf-8",
    )

    with open(
        REPORT_PATH,
        "w",
        encoding="utf-8",
    ) as file:
        file.write(
            "BACQE DUKASCOPY 78 - RESOURCE-AWARE "
            "RESEARCH SCHEDULER\n"
        )
        file.write("=" * 110 + "\n\n")

        file.write("SCHEDULER STATE\n")
        file.write("-" * 110 + "\n")

        for key, value in state.items():
            if key != "resources":
                file.write(f"{key}: {value}\n")

        file.write("\nSYSTEM RESOURCES\n")
        file.write("-" * 110 + "\n")

        for key, value in state[
            "resources"
        ].items():
            file.write(f"{key}: {value}\n")

        file.write("\nAPPROVED OVERNIGHT SCHEDULE\n")
        file.write("-" * 110 + "\n")

        if approved.empty:
            file.write(
                "No jobs approved for execution.\n"
            )
        else:
            display_columns = [
                "schedule_position",
                "source",
                "symbol",
                "stage",
                "priority",
                "job_weight",
                "estimated_runtime_minutes",
                "estimated_ram_gb",
                "estimated_disk_growth_gb",
                "command",
            ]

            file.write(
                approved[
                    display_columns
                ].to_string(index=False)
            )

        file.write("\n\nDEFERRED OR BLOCKED JOBS\n")
        file.write("-" * 110 + "\n")

        if deferred.empty:
            file.write(
                "No jobs were deferred or blocked.\n"
            )
        else:
            display_columns = [
                "source",
                "symbol",
                "stage",
                "priority",
                "schedule_status",
                "decision_reason",
                "command",
            ]

            file.write(
                deferred[
                    display_columns
                ].to_string(index=False)
            )

        file.write("\n\nFULL SCHEDULING REGISTRY\n")
        file.write("-" * 110 + "\n")

        if registry.empty:
            file.write(
                "No candidate jobs were detected.\n"
            )
        else:
            file.write(
                registry.to_string(index=False)
            )


def synthetic_job(
    name: str,
    source: str,
    symbol: str,
    stage: str,
    priority: str,
    command: str | None = None,
) -> dict:
    """
    Build a synthetic scheduler job.

    The command is deliberately harmless and is never executed.
    """

    return {
        "source": source,
        "symbol": symbol,
        "stage": stage,
        "priority": priority,
        "command": (
            command
            or f"python SELF_TEST_ONLY_{name}.py"
        ),
        "attempt_count": 0,
        "max_attempts": 3,
    }


def synthetic_resources(
    disk_free_gb: float = 5000.0,
    ram_available_gb: float = 32.0,
) -> dict:
    """
    Return artificial system resources for deterministic tests.
    """

    return {
        "disk_total_gb": 10000.0,
        "disk_used_gb": 10000.0 - disk_free_gb,
        "disk_free_gb": disk_free_gb,
        "disk_used_pct": round(
            (10000.0 - disk_free_gb)
            / 10000.0
            * 100,
            2,
        ),
        "memory_inspection_available": True,
        "ram_total_gb": 64.0,
        "ram_available_gb": ram_available_gb,
        "ram_used_pct": round(
            (64.0 - ram_available_gb)
            / 64.0
            * 100,
            2,
        ),
    }


def test_result(
    test_name: str,
    passed: bool,
    expected: str,
    observed: str,
) -> dict:
    return {
        "test_name": test_name,
        "test_status": "pass" if passed else "fail",
        "expected": expected,
        "observed": observed,
    }


def run_scheduler_self_test() -> int:
    """
    Exercise scheduler behaviour using synthetic in-memory data only.

    No genuine BACQE state files or action files are modified.
    No synthetic command is executed.
    """

    SELF_TEST_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    results: list[dict] = []

    # ------------------------------------------------------------------
    # TEST 1: Priority ordering
    # ------------------------------------------------------------------

    priority_rows = [
        synthetic_job(
            name="medium",
            source="onboarding_engine",
            symbol="TEST01",
            stage="SELFTEST_SHORT",
            priority="medium",
        ),
        synthetic_job(
            name="critical",
            source="recovery_engine",
            symbol="TEST02",
            stage="SELFTEST_SHORT",
            priority="critical",
        ),
        synthetic_job(
            name="high",
            source="onboarding_engine",
            symbol="TEST03",
            stage="SELFTEST_SHORT",
            priority="high",
        ),
    ]

    priority_jobs = add_resource_estimates(
        prepare_candidate_jobs(priority_rows)
    )

    priority_schedule = schedule_jobs(
        jobs=priority_jobs,
        resources=synthetic_resources(),
        max_runtime_minutes=480,
        max_jobs=10,
        minimum_free_disk_gb=1000.0,
        minimum_available_ram_gb=8.0,
        disk_safety_buffer_gb=100.0,
    )

    approved_priority = (
        priority_schedule[
            priority_schedule["schedule_status"]
            == "approved"
        ]
        .sort_values("schedule_position")
    )

    observed_priorities = (
        approved_priority["priority"].tolist()
    )

    expected_priorities = [
        "critical",
        "high",
        "medium",
    ]

    results.append(
        test_result(
            test_name="priority_ordering",
            passed=(
                observed_priorities
                == expected_priorities
            ),
            expected=str(expected_priorities),
            observed=str(observed_priorities),
        )
    )

    # ------------------------------------------------------------------
    # TEST 2: Deduplication
    # ------------------------------------------------------------------

    duplicate_command = (
        "python SELF_TEST_ONLY_DUPLICATE.py"
    )

    duplicate_rows = [
        synthetic_job(
            name="duplicate_resume",
            source="durable_resume_ledger",
            symbol="TEST04",
            stage="EH03",
            priority="high",
            command=duplicate_command,
        ),
        synthetic_job(
            name="duplicate_onboarding",
            source="onboarding_engine",
            symbol="TEST04",
            stage="EH03",
            priority="high",
            command=duplicate_command,
        ),
    ]

    deduplicated_jobs = prepare_candidate_jobs(
        duplicate_rows
    )

    retained_source = (
        clean_text(
            deduplicated_jobs.iloc[0]["source"]
        )
        if len(deduplicated_jobs) == 1
        else ""
    )

    results.append(
        test_result(
            test_name="duplicate_job_removal",
            passed=(
                len(deduplicated_jobs) == 1
                and retained_source
                == "durable_resume_ledger"
            ),
            expected=(
                "1 job retained from "
                "durable_resume_ledger"
            ),
            observed=(
                f"{len(deduplicated_jobs)} job(s), "
                f"source={retained_source}"
            ),
        )
    )

    # ------------------------------------------------------------------
    # TEST 3: Runtime-budget deferral
    # ------------------------------------------------------------------

    runtime_rows = [
        synthetic_job(
            name="runtime_first",
            source="onboarding_engine",
            symbol="TEST05",
            stage="EH07",
            priority="high",
        ),
        synthetic_job(
            name="runtime_second",
            source="onboarding_engine",
            symbol="TEST06",
            stage="EH07",
            priority="high",
        ),
    ]

    runtime_jobs = add_resource_estimates(
        prepare_candidate_jobs(runtime_rows)
    )

    runtime_schedule = schedule_jobs(
        jobs=runtime_jobs,
        resources=synthetic_resources(),
        max_runtime_minutes=300,
        max_jobs=10,
        minimum_free_disk_gb=1000.0,
        minimum_available_ram_gb=8.0,
        disk_safety_buffer_gb=100.0,
    )

    runtime_statuses = (
        runtime_schedule[
            "schedule_status"
        ].tolist()
    )

    results.append(
        test_result(
            test_name="runtime_budget_deferral",
            passed=(
                runtime_statuses.count("approved")
                == 1
                and runtime_statuses.count(
                    "deferred_runtime_budget"
                )
                == 1
            ),
            expected=(
                "1 approved and 1 "
                "deferred_runtime_budget"
            ),
            observed=str(runtime_statuses),
        )
    )

    # ------------------------------------------------------------------
    # TEST 4: Maximum-job limit
    # ------------------------------------------------------------------

    job_limit_rows = [
        synthetic_job(
            name=f"job_limit_{number}",
            source="onboarding_engine",
            symbol=f"TEST{number + 10}",
            stage="SELFTEST_SHORT",
            priority="medium",
        )
        for number in range(3)
    ]

    job_limit_jobs = add_resource_estimates(
        prepare_candidate_jobs(job_limit_rows)
    )

    job_limit_schedule = schedule_jobs(
        jobs=job_limit_jobs,
        resources=synthetic_resources(),
        max_runtime_minutes=480,
        max_jobs=2,
        minimum_free_disk_gb=1000.0,
        minimum_available_ram_gb=8.0,
        disk_safety_buffer_gb=100.0,
    )

    job_limit_statuses = (
        job_limit_schedule[
            "schedule_status"
        ].tolist()
    )

    results.append(
        test_result(
            test_name="maximum_job_limit",
            passed=(
                job_limit_statuses.count("approved")
                == 2
                and job_limit_statuses.count(
                    "deferred_job_limit"
                )
                == 1
            ),
            expected=(
                "2 approved and 1 "
                "deferred_job_limit"
            ),
            observed=str(job_limit_statuses),
        )
    )

    # ------------------------------------------------------------------
    # TEST 5: Projected disk-space protection
    # ------------------------------------------------------------------

    disk_rows = [
        synthetic_job(
            name="disk_protection",
            source="onboarding_engine",
            symbol="TEST20",
            stage="SELFTEST_DISK",
            priority="high",
        )
    ]

    disk_jobs = add_resource_estimates(
        prepare_candidate_jobs(disk_rows)
    )

    disk_schedule = schedule_jobs(
        jobs=disk_jobs,
        resources=synthetic_resources(
            disk_free_gb=1200.0,
        ),
        max_runtime_minutes=480,
        max_jobs=10,
        minimum_free_disk_gb=1000.0,
        minimum_available_ram_gb=8.0,
        disk_safety_buffer_gb=100.0,
    )

    disk_status = clean_text(
        disk_schedule.iloc[0][
            "schedule_status"
        ]
    )

    results.append(
        test_result(
            test_name="projected_disk_protection",
            passed=(
                disk_status
                == "deferred_disk_budget"
            ),
            expected="deferred_disk_budget",
            observed=disk_status,
        )
    )

    # ------------------------------------------------------------------
    # TEST 6: Per-job RAM requirement
    # ------------------------------------------------------------------

    ram_rows = [
        synthetic_job(
            name="ram_requirement",
            source="onboarding_engine",
            symbol="TEST21",
            stage="SELFTEST_RAM",
            priority="high",
        )
    ]

    ram_jobs = add_resource_estimates(
        prepare_candidate_jobs(ram_rows)
    )

    ram_schedule = schedule_jobs(
        jobs=ram_jobs,
        resources=synthetic_resources(
            ram_available_gb=10.0,
        ),
        max_runtime_minutes=480,
        max_jobs=10,
        minimum_free_disk_gb=1000.0,
        minimum_available_ram_gb=8.0,
        disk_safety_buffer_gb=100.0,
    )

    ram_status = clean_text(
        ram_schedule.iloc[0][
            "schedule_status"
        ]
    )

    results.append(
        test_result(
            test_name="job_ram_requirement",
            passed=(
                ram_status
                == "deferred_ram_requirement"
            ),
            expected="deferred_ram_requirement",
            observed=ram_status,
        )
    )

    # ------------------------------------------------------------------
    # TEST 7: Minimum currently available RAM
    # ------------------------------------------------------------------

    low_ram_rows = [
        synthetic_job(
            name="low_ram_guard",
            source="onboarding_engine",
            symbol="TEST22",
            stage="SELFTEST_SHORT",
            priority="high",
        )
    ]

    low_ram_jobs = add_resource_estimates(
        prepare_candidate_jobs(low_ram_rows)
    )

    low_ram_schedule = schedule_jobs(
        jobs=low_ram_jobs,
        resources=synthetic_resources(
            ram_available_gb=4.0,
        ),
        max_runtime_minutes=480,
        max_jobs=10,
        minimum_free_disk_gb=1000.0,
        minimum_available_ram_gb=8.0,
        disk_safety_buffer_gb=100.0,
    )

    low_ram_status = clean_text(
        low_ram_schedule.iloc[0][
            "schedule_status"
        ]
    )

    results.append(
        test_result(
            test_name="minimum_available_ram_guard",
            passed=(
                low_ram_status
                == "blocked_low_ram"
            ),
            expected="blocked_low_ram",
            observed=low_ram_status,
        )
    )

    results_frame = pd.DataFrame(results)

    results_frame.to_csv(
        SELF_TEST_RESULTS_PATH,
        index=False,
    )

    passed_tests = int(
        (
            results_frame["test_status"]
            == "pass"
        ).sum()
    )

    failed_tests = int(
        (
            results_frame["test_status"]
            == "fail"
        ).sum()
    )

    state = {
        "generated_at": now_text(),
        "self_test_mode": True,
        "real_action_files_modified": False,
        "synthetic_commands_executed": False,
        "tests_total": len(results_frame),
        "tests_passed": passed_tests,
        "tests_failed": failed_tests,
        "overall_status": (
            "pass"
            if failed_tests == 0
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
            "BACQE DUKASCOPY 78 - "
            "RESOURCE-AWARE SCHEDULER SELF-TEST\n"
        )
        file.write("=" * 100 + "\n\n")

        file.write(
            "This test used synthetic in-memory jobs only.\n"
        )
        file.write(
            "No genuine BACQE action files were modified.\n"
        )
        file.write(
            "No synthetic command was executed.\n\n"
        )

        file.write(
            f"Tests passed: {passed_tests}/"
            f"{len(results_frame)}\n"
        )
        file.write(
            f"Tests failed: {failed_tests}\n\n"
        )

        file.write("TEST RESULTS\n")
        file.write("-" * 100 + "\n")
        file.write(
            results_frame.to_string(index=False)
        )

    print("=" * 100)
    print(
        "BACQE DUKASCOPY 78 - "
        "RESOURCE-AWARE SCHEDULER SELF-TEST"
    )
    print("=" * 100)
    print(
        "Synthetic in-memory jobs only: True"
    )
    print(
        "Real action files modified:     False"
    )
    print(
        "Synthetic commands executed:    False"
    )
    print("-" * 100)
    print(
        results_frame.to_string(index=False)
    )
    print("-" * 100)
    print(
        f"Tests passed: {passed_tests}/"
        f"{len(results_frame)}"
    )
    print(f"Tests failed: {failed_tests}")

    if failed_tests == 0:
        print(
            "[PASS] All scheduler safety tests passed."
        )
    else:
        print(
            "[FAIL] One or more scheduler tests failed."
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

    return 0 if failed_tests == 0 else 1


def main(
    max_runtime_minutes: int,
    max_jobs: int,
    minimum_free_disk_gb: float,
    minimum_available_ram_gb: float,
    disk_safety_buffer_gb: float,
) -> None:
    ensure_output_directory()

    resources = system_resources()

    candidate_jobs = collect_candidate_jobs()

    candidate_jobs = add_resource_estimates(
        candidate_jobs
    )

    registry = schedule_jobs(
        jobs=candidate_jobs,
        resources=resources,
        max_runtime_minutes=max_runtime_minutes,
        max_jobs=max_jobs,
        minimum_free_disk_gb=minimum_free_disk_gb,
        minimum_available_ram_gb=(
            minimum_available_ram_gb
        ),
        disk_safety_buffer_gb=(
            disk_safety_buffer_gb
        ),
    )

    state = build_state(
        registry=registry,
        resources=resources,
        max_runtime_minutes=max_runtime_minutes,
        max_jobs=max_jobs,
        minimum_free_disk_gb=minimum_free_disk_gb,
        minimum_available_ram_gb=(
            minimum_available_ram_gb
        ),
        disk_safety_buffer_gb=(
            disk_safety_buffer_gb
        ),
    )

    write_outputs(
        registry=registry,
        state=state,
    )

    print("=" * 110)
    print(
        "BACQE DUKASCOPY 78 - RESOURCE-AWARE "
        "RESEARCH SCHEDULER"
    )
    print("=" * 110)

    print("SYSTEM RESOURCES")
    print("-" * 110)
    print(
        f"Disk free:             "
        f"{resources['disk_free_gb']:.2f} GB"
    )
    print(
        f"Disk used:             "
        f"{resources['disk_used_pct']:.2f}%"
    )

    if resources["memory_inspection_available"]:
        print(
            f"RAM available:         "
            f"{resources['ram_available_gb']:.2f} GB"
        )
        print(
            f"RAM used:              "
            f"{resources['ram_used_pct']:.2f}%"
        )
    else:
        print(
            "RAM inspection:        unavailable "
            "(psutil not installed)"
        )

    print("-" * 110)
    print(f"Candidate jobs:         {len(registry)}")
    print(f"Approved jobs:          {state['approved_jobs']}")
    print(f"Deferred jobs:          {state['deferred_jobs']}")
    print(
        f"Estimated runtime:      "
        f"{state['estimated_approved_runtime_minutes']} minutes"
    )
    print(
        f"Estimated disk growth:  "
        f"{state['estimated_approved_disk_growth_gb']:.2f} GB"
    )
    print(
        f"Schedule status:        "
        f"{state['schedule_status']}"
    )

    print("-" * 110)

    approved = registry[
        registry["schedule_status"]
        == "approved"
    ]

    if approved.empty:
        if registry.empty:
            print(
                "[COMPLETE] No candidate jobs require scheduling."
            )
        else:
            print(
                "[DEFERRED] No candidate jobs passed all "
                "resource and runtime checks."
            )
    else:
        print("APPROVED OVERNIGHT SCHEDULE")
        print(
            approved[
                [
                    "schedule_position",
                    "source",
                    "symbol",
                    "stage",
                    "priority",
                    "job_weight",
                    "estimated_runtime_minutes",
                    "estimated_ram_gb",
                    "estimated_disk_growth_gb",
                    "command",
                ]
            ].to_string(index=False)
        )

    print("-" * 110)
    print(f"Registry:  {REGISTRY_PATH}")
    print(f"Schedule:  {SCHEDULE_PATH}")
    print(f"Deferred:  {DEFERRED_PATH}")
    print(f"Commands:  {COMMANDS_PATH}")
    print(f"State:     {STATE_PATH}")
    print(f"Report:    {REPORT_PATH}")
    print("=" * 110)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=480,
        help=(
            "Maximum estimated overnight workload. "
            "Default: 480 minutes."
        ),
    )

    parser.add_argument(
        "--max-jobs",
        type=int,
        default=10,
        help=(
            "Maximum jobs approved in one overnight schedule. "
            "Default: 10."
        ),
    )

    parser.add_argument(
        "--minimum-free-disk-gb",
        type=float,
        default=1000.0,
        help=(
            "Minimum free space that must remain on the "
            "data-lake drive. Default: 1000 GB."
        ),
    )

    parser.add_argument(
        "--minimum-available-ram-gb",
        type=float,
        default=8.0,
        help=(
            "Minimum available RAM required before approving "
            "work. Default: 8 GB."
        ),
    )

    parser.add_argument(
        "--disk-safety-buffer-gb",
        type=float,
        default=100.0,
        help=(
            "Additional disk-space buffer applied to projected "
            "growth. Default: 100 GB."
        ),
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
        help=(
            "Run deterministic synthetic scheduler tests. "
            "No real BACQE actions are modified or executed."
        ),
    )

    args = parser.parse_args()

    if args.self_test:
        raise SystemExit(
            run_scheduler_self_test()
        )

    if args.max_runtime_minutes < 1:
        raise ValueError(
            "--max-runtime-minutes must be at least 1."
        )

    if args.max_jobs < 1:
        raise ValueError(
            "--max-jobs must be at least 1."
        )

    if args.minimum_free_disk_gb < 0:
        raise ValueError(
            "--minimum-free-disk-gb cannot be negative."
        )

    if args.minimum_available_ram_gb < 0:
        raise ValueError(
            "--minimum-available-ram-gb cannot be negative."
        )

    if args.disk_safety_buffer_gb < 0:
        raise ValueError(
            "--disk-safety-buffer-gb cannot be negative."
        )

    main(
        max_runtime_minutes=(
            args.max_runtime_minutes
        ),
        max_jobs=args.max_jobs,
        minimum_free_disk_gb=(
            args.minimum_free_disk_gb
        ),
        minimum_available_ram_gb=(
            args.minimum_available_ram_gb
        ),
        disk_safety_buffer_gb=(
            args.disk_safety_buffer_gb
        ),
    )