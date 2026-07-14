"""
BACQE DUKASCOPY 79 - WORKLOAD EXECUTION PLANNER

Purpose:
    Convert Script 78's resource-approved overnight schedule into a
    deterministic, dependency-safe and efficiency-aware execution plan.

Responsibilities:
    - Read only jobs approved by Script 78
    - Validate stage dependencies
    - Prioritise recovery work
    - Keep symbol-level work ahead of dependent global work
    - Reduce unnecessary symbol switching where safe
    - Preserve critical/high-priority ordering
    - Produce the exact sequential plan Script 75 should execute
    - Detect invalid or blocked workload combinations
    - Support a non-damaging in-memory self-test

This script does not execute jobs.
It plans their order.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import argparse
import json

import pandas as pd


BASE_DIR = Path("E:/Quant_Lab")
ANALYSIS_ROOT = BASE_DIR / "data" / "analysis"

SCHEDULER_ROOT = (
    ANALYSIS_ROOT
    / "dukascopy_resource_aware_scheduler"
)

APPROVED_SCHEDULE_PATH = (
    SCHEDULER_ROOT
    / "dukascopy_approved_overnight_schedule_latest.csv"
)

REPORT_ROOT = (
    ANALYSIS_ROOT
    / "dukascopy_workload_execution_planner"
)

PLAN_PATH = (
    REPORT_ROOT
    / "dukascopy_workload_execution_plan_latest.csv"
)

BLOCKED_PATH = (
    REPORT_ROOT
    / "dukascopy_workload_execution_blocked_latest.csv"
)

COMMANDS_PATH = (
    REPORT_ROOT
    / "dukascopy_workload_execution_commands_latest.txt"
)

STATE_PATH = (
    REPORT_ROOT
    / "dukascopy_workload_execution_state_latest.json"
)

REPORT_PATH = (
    REPORT_ROOT
    / "dukascopy_workload_execution_report_latest.txt"
)

SELF_TEST_ROOT = REPORT_ROOT / "self_test"

SELF_TEST_RESULTS_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_workload_planner_self_test_results_latest.csv"
)

SELF_TEST_STATE_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_workload_planner_self_test_state_latest.json"
)

SELF_TEST_REPORT_PATH = (
    SELF_TEST_ROOT
    / "dukascopy_workload_planner_self_test_report_latest.txt"
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


STAGE_ORDER = {
    "RAW": 1,
    "08": 2,
    "09": 3,
    "10": 4,
    "23": 5,
    "30": 6,
    "EH01": 7,
    "EH02": 8,
    "EH03": 9,
    "EH04": 10,
    "EH05": 11,
    "EH06": 12,
    "EH07": 13,
    "EH08": 14,
    "EH09": 15,
    "EH10": 16,
    "EH11": 17,
    "EH12": 18,
    "EH13": 19,
}


GLOBAL_STAGES = {
    "EH11",
    "EH12",
    "EH13",
}


GLOBAL_DEPENDENCIES = {
    "EH11": None,
    "EH12": "EH11",
    "EH13": "EH12",
}


EXPECTED_COLUMNS = [
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
    "estimated_runtime_minutes",
    "estimated_ram_gb",
    "estimated_disk_growth_gb",
    "job_weight",
    "schedule_status",
    "schedule_position",
]


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_output_directories() -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    SELF_TEST_ROOT.mkdir(parents=True, exist_ok=True)


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


def normalise_schedule(
    schedule: pd.DataFrame,
) -> pd.DataFrame:
    if schedule.empty:
        return pd.DataFrame(
            columns=EXPECTED_COLUMNS
        )

    schedule = schedule.copy()

    for column in EXPECTED_COLUMNS:
        if column not in schedule.columns:
            schedule[column] = ""

    schedule = schedule[
        EXPECTED_COLUMNS
    ].copy()

    text_columns = [
        "job_id",
        "source",
        "symbol",
        "stage",
        "priority",
        "command",
        "job_weight",
        "schedule_status",
    ]

    for column in text_columns:
        schedule[column] = (
            schedule[column]
            .fillna("")
            .astype(str)
            .str.strip()
        )

    schedule["symbol"] = (
        schedule["symbol"]
        .str.upper()
    )

    schedule["stage"] = (
        schedule["stage"]
        .str.upper()
    )

    schedule["priority"] = (
        schedule["priority"]
        .str.lower()
    )

    integer_columns = [
        "attempt_count",
        "max_attempts",
        "source_rank",
        "priority_rank",
        "estimated_runtime_minutes",
        "schedule_position",
    ]

    for column in integer_columns:
        schedule[column] = pd.to_numeric(
            schedule[column],
            errors="coerce",
        ).fillna(0).astype(int)

    float_columns = [
        "estimated_ram_gb",
        "estimated_disk_growth_gb",
    ]

    for column in float_columns:
        schedule[column] = pd.to_numeric(
            schedule[column],
            errors="coerce",
        ).fillna(0.0)

    return schedule


def validate_approved_schedule(
    schedule: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split the incoming schedule into valid and blocked rows.
    """

    if schedule.empty:
        return schedule.copy(), schedule.copy()

    valid_rows = []
    blocked_rows = []

    for _, row in schedule.iterrows():
        reasons: list[str] = []

        command = clean_text(row["command"])
        symbol = clean_text(
            row["symbol"],
            "UNKNOWN",
        )
        stage = clean_text(
            row["stage"],
            "UNKNOWN",
        ).upper()

        schedule_status = clean_text(
            row["schedule_status"]
        ).lower()

        if schedule_status != "approved":
            reasons.append(
                "Job is not marked approved by Script 78."
            )

        if not command:
            reasons.append(
                "Job command is empty."
            )

        if not symbol:
            reasons.append(
                "Job symbol is empty."
            )

        if stage not in STAGE_ORDER:
            reasons.append(
                f"Unknown stage: {stage}"
            )

        attempt_count = clean_int(
            row["attempt_count"],
            0,
        )

        max_attempts = clean_int(
            row["max_attempts"],
            3,
        )

        if attempt_count >= max_attempts:
            reasons.append(
                "Job has reached or exceeded its retry limit."
            )

        output_row = row.to_dict()

        if reasons:
            output_row[
                "planner_status"
            ] = "blocked_invalid_job"

            output_row[
                "planner_reason"
            ] = " ".join(reasons)

            blocked_rows.append(output_row)

        else:
            output_row[
                "planner_status"
            ] = "eligible"

            output_row[
                "planner_reason"
            ] = (
                "Job passed workload-planner validation."
            )

            valid_rows.append(output_row)

    valid = pd.DataFrame(valid_rows)
    blocked = pd.DataFrame(blocked_rows)

    return valid, blocked


def add_planning_metadata(
    jobs: pd.DataFrame,
) -> pd.DataFrame:
    if jobs.empty:
        return jobs.copy()

    jobs = jobs.copy()

    jobs["stage_rank"] = (
        jobs["stage"]
        .map(STAGE_ORDER)
        .fillna(999)
        .astype(int)
    )

    jobs["priority_rank"] = (
        jobs["priority"]
        .map(PRIORITY_RANK)
        .fillna(
            jobs.get(
                "priority_rank",
                pd.Series(
                    50,
                    index=jobs.index,
                ),
            )
        )
        .astype(int)
    )

    jobs["source_rank"] = (
        jobs["source"]
        .map(SOURCE_RANK)
        .fillna(
            jobs.get(
                "source_rank",
                pd.Series(
                    50,
                    index=jobs.index,
                ),
            )
        )
        .astype(int)
    )

    jobs["is_global_stage"] = (
        jobs["stage"].isin(
            GLOBAL_STAGES
        )
    )

    jobs["symbol_group"] = jobs.apply(
        lambda row: (
            "GLOBAL"
            if bool(row["is_global_stage"])
            else clean_text(
                row["symbol"],
                "UNKNOWN",
            )
        ),
        axis=1,
    )

    return jobs


def enforce_global_dependencies(
    jobs: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ensure EH12 cannot precede EH11 and EH13 cannot precede EH12.

    A missing earlier global stage does not automatically block a later
    stage, because the earlier stage may already be complete outside the
    current overnight schedule. Script 73 remains the source of truth.

    However, when both dependent stages are present in tonight's plan,
    their relative ordering is enforced.
    """

    if jobs.empty:
        return jobs.copy(), pd.DataFrame()

    jobs = jobs.copy()
    blocked_rows = []

    duplicate_global_mask = (
        jobs["is_global_stage"]
        & jobs.duplicated(
            subset=["stage"],
            keep="first",
        )
    )

    if duplicate_global_mask.any():
        duplicates = jobs[
            duplicate_global_mask
        ].copy()

        duplicates[
            "planner_status"
        ] = "blocked_duplicate_global_stage"

        duplicates[
            "planner_reason"
        ] = (
            "Only one job per global stage may appear "
            "in the same execution plan."
        )

        blocked_rows.append(duplicates)

        jobs = jobs[
            ~duplicate_global_mask
        ].copy()

    blocked = (
        pd.concat(
            blocked_rows,
            ignore_index=True,
        )
        if blocked_rows
        else pd.DataFrame()
    )

    return jobs, blocked


def dependency_first_plan(
    jobs: pd.DataFrame,
) -> pd.DataFrame:
    """
    Safest ordering strategy.

    Order:
        1. Priority
        2. Source authority
        3. Stage dependency
        4. Symbol
        5. Existing Script 78 position
    """

    if jobs.empty:
        return jobs.copy()

    return jobs.sort_values(
        by=[
            "priority_rank",
            "source_rank",
            "stage_rank",
            "symbol_group",
            "schedule_position",
        ],
        ascending=[
            True,
            True,
            True,
            True,
            True,
        ],
    ).reset_index(drop=True)


def symbol_clustered_plan(
    jobs: pd.DataFrame,
) -> pd.DataFrame:
    """
    Efficiency-oriented strategy.

    Recovery jobs remain first.
    Symbol-level work is clustered by symbol.
    Global stages remain last and retain EH11 -> EH12 -> EH13 order.
    """

    if jobs.empty:
        return jobs.copy()

    jobs = jobs.copy()

    jobs["execution_class"] = jobs.apply(
        lambda row: (
            1
            if row["source"] == "recovery_engine"
            else 3
            if bool(row["is_global_stage"])
            else 2
        ),
        axis=1,
    )

    return jobs.sort_values(
        by=[
            "execution_class",
            "priority_rank",
            "symbol_group",
            "stage_rank",
            "source_rank",
            "schedule_position",
        ],
        ascending=[
            True,
            True,
            True,
            True,
            True,
            True,
        ],
    ).reset_index(drop=True)


def build_execution_plan(
    approved_jobs: pd.DataFrame,
    strategy: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid, invalid = validate_approved_schedule(
        approved_jobs
    )

    if valid.empty:
        return valid, invalid

    valid = add_planning_metadata(valid)

    valid, dependency_blocked = (
        enforce_global_dependencies(valid)
    )

    blocked_frames = [
        frame
        for frame in [
            invalid,
            dependency_blocked,
        ]
        if not frame.empty
    ]

    blocked = (
        pd.concat(
            blocked_frames,
            ignore_index=True,
        )
        if blocked_frames
        else pd.DataFrame()
    )

    if strategy == "symbol_clustered":
        plan = symbol_clustered_plan(valid)
    else:
        plan = dependency_first_plan(valid)

    plan = plan.copy()

    plan["execution_position"] = (
        range(1, len(plan) + 1)
    )

    plan["planner_status"] = "planned"

    plan["planner_reason"] = (
        "Job included in the approved sequential "
        f"execution plan using strategy={strategy}."
    )

    plan["cumulative_runtime_minutes"] = (
        plan[
            "estimated_runtime_minutes"
        ].cumsum()
    )

    plan["cumulative_disk_growth_gb"] = (
        plan[
            "estimated_disk_growth_gb"
        ].cumsum()
        .round(2)
    )

    plan["previous_symbol"] = (
        plan["symbol_group"].shift(1)
    )

    plan["symbol_switch"] = (
        plan["previous_symbol"].notna()
        & (
            plan["previous_symbol"]
            != plan["symbol_group"]
        )
    )

    plan["symbol_switch"] = (
        plan["symbol_switch"].astype(bool)
    )

    return plan, blocked


def build_state(
    plan: pd.DataFrame,
    blocked: pd.DataFrame,
    strategy: str,
) -> dict:
    symbol_switches = (
        int(plan["symbol_switch"].sum())
        if not plan.empty
        and "symbol_switch" in plan.columns
        else 0
    )

    return {
        "generated_at": now_text(),
        "planning_strategy": strategy,
        "input_approved_jobs": (
            len(plan) + len(blocked)
        ),
        "planned_jobs": len(plan),
        "blocked_jobs": len(blocked),
        "estimated_runtime_minutes": (
            int(
                plan[
                    "estimated_runtime_minutes"
                ].sum()
            )
            if not plan.empty
            else 0
        ),
        "estimated_disk_growth_gb": (
            round(
                plan[
                    "estimated_disk_growth_gb"
                ].sum(),
                2,
            )
            if not plan.empty
            else 0.0
        ),
        "estimated_symbol_switches": (
            symbol_switches
        ),
        "global_jobs": (
            int(
                plan["is_global_stage"].sum()
            )
            if not plan.empty
            and "is_global_stage"
            in plan.columns
            else 0
        ),
        "plan_status": (
            "ready"
            if not plan.empty
            else "no_work"
            if blocked.empty
            else "all_jobs_blocked"
        ),
    }


def write_outputs(
    plan: pd.DataFrame,
    blocked: pd.DataFrame,
    state: dict,
) -> None:
    plan.to_csv(
        PLAN_PATH,
        index=False,
    )

    blocked.to_csv(
        BLOCKED_PATH,
        index=False,
    )

    with open(
        COMMANDS_PATH,
        "w",
        encoding="utf-8",
    ) as file:
        if plan.empty:
            file.write(
                "# No workload execution commands planned.\n"
            )
        else:
            for command in plan["command"]:
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
            "BACQE DUKASCOPY 79 - "
            "WORKLOAD EXECUTION PLANNER\n"
        )
        file.write("=" * 110 + "\n\n")

        file.write("PLANNER STATE\n")
        file.write("-" * 110 + "\n")

        for key, value in state.items():
            file.write(f"{key}: {value}\n")

        file.write("\nEXECUTION PLAN\n")
        file.write("-" * 110 + "\n")

        if plan.empty:
            file.write(
                "No jobs were planned for execution.\n"
            )
        else:
            display_columns = [
                "execution_position",
                "source",
                "symbol",
                "stage",
                "priority",
                "job_weight",
                "estimated_runtime_minutes",
                "estimated_ram_gb",
                "estimated_disk_growth_gb",
                "cumulative_runtime_minutes",
                "symbol_switch",
                "command",
            ]

            file.write(
                plan[
                    display_columns
                ].to_string(index=False)
            )

        file.write(
            "\n\nBLOCKED PLANNER JOBS\n"
        )
        file.write("-" * 110 + "\n")

        if blocked.empty:
            file.write(
                "No jobs were blocked by the planner.\n"
            )
        else:
            display_columns = [
                column
                for column in [
                    "source",
                    "symbol",
                    "stage",
                    "priority",
                    "planner_status",
                    "planner_reason",
                    "command",
                ]
                if column in blocked.columns
            ]

            file.write(
                blocked[
                    display_columns
                ].to_string(index=False)
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


def synthetic_approved_job(
    job_id: str,
    source: str,
    symbol: str,
    stage: str,
    priority: str,
    position: int,
) -> dict:
    return {
        "job_id": job_id,
        "source": source,
        "symbol": symbol,
        "stage": stage,
        "priority": priority,
        "command": (
            f"python SELF_TEST_ONLY_{job_id}.py"
        ),
        "attempt_count": 0,
        "max_attempts": 3,
        "source_rank": SOURCE_RANK.get(
            source,
            50,
        ),
        "priority_rank": PRIORITY_RANK.get(
            priority,
            50,
        ),
        "estimated_runtime_minutes": 10,
        "estimated_ram_gb": 1.0,
        "estimated_disk_growth_gb": 1.0,
        "job_weight": "self_test",
        "schedule_status": "approved",
        "schedule_position": position,
    }


def run_self_test() -> int:
    """
    Run deterministic in-memory planner tests.

    No real Script 78 output is changed.
    No synthetic command is executed.
    """

    ensure_output_directories()

    results: list[dict] = []

    # Test 1: Dependency-first stage ordering.
    rows = [
        synthetic_approved_job(
            "eh04",
            "onboarding_engine",
            "EURUSD",
            "EH04",
            "high",
            1,
        ),
        synthetic_approved_job(
            "eh03",
            "onboarding_engine",
            "EURUSD",
            "EH03",
            "high",
            2,
        ),
    ]

    plan, blocked = build_execution_plan(
        normalise_schedule(
            pd.DataFrame(rows)
        ),
        strategy="dependency_first",
    )

    observed = plan["stage"].tolist()

    results.append(
        test_result(
            "dependency_stage_ordering",
            passed=(
                observed == ["EH03", "EH04"]
                and blocked.empty
            ),
            expected="['EH03', 'EH04']",
            observed=str(observed),
        )
    )

    # Test 2: Recovery precedes normal work.
    rows = [
        synthetic_approved_job(
            "normal",
            "onboarding_engine",
            "EURUSD",
            "EH03",
            "critical",
            1,
        ),
        synthetic_approved_job(
            "recovery",
            "recovery_engine",
            "GBPUSD",
            "23",
            "critical",
            2,
        ),
    ]

    plan, _ = build_execution_plan(
        normalise_schedule(
            pd.DataFrame(rows)
        ),
        strategy="symbol_clustered",
    )

    observed = plan["source"].tolist()

    results.append(
        test_result(
            "recovery_first",
            passed=(
                observed[0]
                == "recovery_engine"
            ),
            expected=(
                "recovery_engine in position 1"
            ),
            observed=str(observed),
        )
    )

    # Test 3: Global stage ordering.
    rows = [
        synthetic_approved_job(
            "eh13",
            "global_cohort_decision_engine",
            "GLOBAL",
            "EH13",
            "medium",
            1,
        ),
        synthetic_approved_job(
            "eh11",
            "global_cohort_decision_engine",
            "GLOBAL",
            "EH11",
            "high",
            2,
        ),
        synthetic_approved_job(
            "eh12",
            "global_cohort_decision_engine",
            "GLOBAL",
            "EH12",
            "high",
            3,
        ),
    ]

    plan, _ = build_execution_plan(
        normalise_schedule(
            pd.DataFrame(rows)
        ),
        strategy="dependency_first",
    )

    observed = plan["stage"].tolist()

    results.append(
        test_result(
            "global_stage_ordering",
            passed=(
                observed
                == ["EH11", "EH12", "EH13"]
            ),
            expected=(
                "['EH11', 'EH12', 'EH13']"
            ),
            observed=str(observed),
        )
    )

    # Test 4: Invalid unknown stage blocked.
    rows = [
        synthetic_approved_job(
            "invalid",
            "onboarding_engine",
            "EURUSD",
            "UNKNOWN_STAGE",
            "high",
            1,
        )
    ]

    plan, blocked = build_execution_plan(
        normalise_schedule(
            pd.DataFrame(rows)
        ),
        strategy="dependency_first",
    )

    results.append(
        test_result(
            "unknown_stage_blocked",
            passed=(
                plan.empty
                and len(blocked) == 1
            ),
            expected=(
                "0 planned, 1 blocked"
            ),
            observed=(
                f"{len(plan)} planned, "
                f"{len(blocked)} blocked"
            ),
        )
    )

    # Test 5: Retry-exhausted job blocked.
    retry_row = synthetic_approved_job(
        "retry",
        "durable_resume_ledger",
        "EURUSD",
        "EH03",
        "high",
        1,
    )

    retry_row["attempt_count"] = 3
    retry_row["max_attempts"] = 3

    plan, blocked = build_execution_plan(
        normalise_schedule(
            pd.DataFrame([retry_row])
        ),
        strategy="dependency_first",
    )

    results.append(
        test_result(
            "retry_exhausted_blocked",
            passed=(
                plan.empty
                and len(blocked) == 1
            ),
            expected=(
                "0 planned, 1 blocked"
            ),
            observed=(
                f"{len(plan)} planned, "
                f"{len(blocked)} blocked"
            ),
        )
    )

    # Test 6: Symbol clustering reduces switching.
    rows = [
        synthetic_approved_job(
            "eur03",
            "onboarding_engine",
            "EURUSD",
            "EH03",
            "medium",
            1,
        ),
        synthetic_approved_job(
            "gbp03",
            "onboarding_engine",
            "GBPUSD",
            "EH03",
            "medium",
            2,
        ),
        synthetic_approved_job(
            "eur04",
            "onboarding_engine",
            "EURUSD",
            "EH04",
            "medium",
            3,
        ),
        synthetic_approved_job(
            "gbp04",
            "onboarding_engine",
            "GBPUSD",
            "EH04",
            "medium",
            4,
        ),
    ]

    clustered, _ = build_execution_plan(
        normalise_schedule(
            pd.DataFrame(rows)
        ),
        strategy="symbol_clustered",
    )

    observed_symbols = (
        clustered["symbol"].tolist()
    )

    expected_options = [
        [
            "EURUSD",
            "EURUSD",
            "GBPUSD",
            "GBPUSD",
        ],
        [
            "GBPUSD",
            "GBPUSD",
            "EURUSD",
            "EURUSD",
        ],
    ]

    results.append(
        test_result(
            "symbol_clustering",
            passed=(
                observed_symbols
                in expected_options
            ),
            expected=str(expected_options),
            observed=str(observed_symbols),
        )
    )

    results_frame = pd.DataFrame(
        results
    )

    passed = int(
        (
            results_frame[
                "test_status"
            ]
            == "pass"
        ).sum()
    )

    failed = int(
        (
            results_frame[
                "test_status"
            ]
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
        "real_schedule_modified": False,
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
            "BACQE DUKASCOPY 79 - "
            "WORKLOAD EXECUTION PLANNER SELF-TEST\n"
        )
        file.write("=" * 100 + "\n\n")
        file.write(
            "Synthetic in-memory schedule only.\n"
        )
        file.write(
            "No real Script 78 output was modified.\n"
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
        "BACQE DUKASCOPY 79 - "
        "WORKLOAD EXECUTION PLANNER SELF-TEST"
    )
    print("=" * 100)
    print(
        "Synthetic in-memory schedule only: True"
    )
    print(
        "Real Script 78 output modified:      False"
    )
    print(
        "Synthetic commands executed:         False"
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
            "[PASS] All workload-planner tests passed."
        )
    else:
        print(
            "[FAIL] One or more workload-planner tests failed."
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
    strategy: str,
) -> None:
    ensure_output_directories()

    approved_schedule = normalise_schedule(
        safe_read_csv(
            APPROVED_SCHEDULE_PATH
        )
    )

    plan, blocked = build_execution_plan(
        approved_jobs=approved_schedule,
        strategy=strategy,
    )

    state = build_state(
        plan=plan,
        blocked=blocked,
        strategy=strategy,
    )

    write_outputs(
        plan=plan,
        blocked=blocked,
        state=state,
    )

    print("=" * 110)
    print(
        "BACQE DUKASCOPY 79 - "
        "WORKLOAD EXECUTION PLANNER"
    )
    print("=" * 110)
    print(
        f"Planning strategy:       "
        f"{strategy}"
    )
    print(
        f"Approved input jobs:     "
        f"{state['input_approved_jobs']}"
    )
    print(
        f"Planned jobs:            "
        f"{state['planned_jobs']}"
    )
    print(
        f"Blocked jobs:            "
        f"{state['blocked_jobs']}"
    )
    print(
        f"Estimated runtime:       "
        f"{state['estimated_runtime_minutes']} minutes"
    )
    print(
        f"Estimated disk growth:   "
        f"{state['estimated_disk_growth_gb']:.2f} GB"
    )
    print(
        f"Estimated symbol switches: "
        f"{state['estimated_symbol_switches']}"
    )
    print(
        f"Plan status:             "
        f"{state['plan_status']}"
    )
    print("-" * 110)

    if plan.empty:
        if blocked.empty:
            print(
                "[COMPLETE] No approved workload requires planning."
            )
        else:
            print(
                "[BLOCKED] All approved jobs failed planner validation."
            )
    else:
        print("WORKLOAD EXECUTION PLAN")
        print(
            plan[
                [
                    "execution_position",
                    "source",
                    "symbol",
                    "stage",
                    "priority",
                    "job_weight",
                    "estimated_runtime_minutes",
                    "estimated_ram_gb",
                    "estimated_disk_growth_gb",
                    "symbol_switch",
                    "command",
                ]
            ].to_string(index=False)
        )

    print("-" * 110)
    print(f"Plan:     {PLAN_PATH}")
    print(f"Blocked:  {BLOCKED_PATH}")
    print(f"Commands: {COMMANDS_PATH}")
    print(f"State:    {STATE_PATH}")
    print(f"Report:   {REPORT_PATH}")
    print("=" * 110)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--strategy",
        choices=[
            "dependency_first",
            "symbol_clustered",
        ],
        default="dependency_first",
        help=(
            "Planning strategy. dependency_first is the safest "
            "default. symbol_clustered reduces symbol switching "
            "where dependency rules permit."
        ),
    )

    parser.add_argument(
        "--self-test",
        action="store_true",
        help=(
            "Run deterministic, non-damaging in-memory tests."
        ),
    )

    args = parser.parse_args()

    if args.self_test:
        raise SystemExit(
            run_self_test()
        )

    main(
        strategy=args.strategy
    )