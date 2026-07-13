from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = PROJECT_ROOT / "scripts" / "information_data"
REPORT_DIR = PROJECT_ROOT / "reports"

SUMMARY_FILE = REPORT_DIR / "information_data_suite_latest.txt"


@dataclass(frozen=True)
class SuiteTask:
    task_id: str
    name: str
    script_name: str
    required: bool = True


@dataclass
class TaskResult:
    task_id: str
    name: str
    script_name: str
    status: str
    return_code: int | None
    duration_seconds: float
    stdout: str
    stderr: str
    note: str = ""


TASKS = [
    SuiteTask(
        task_id="INFO-01",
        name="Cross-asset macro snapshot",
        script_name="01_collect_cross_asset_macro_snapshot.py",
    ),
    SuiteTask(
        task_id="INFO-02",
        name="Economic calendar snapshot",
        script_name="02_collect_economic_calendar_snapshot.py",
        required=False,
    ),
    SuiteTask(
        task_id="INFO-03",
        name="Financial headlines RSS",
        script_name="03_collect_financial_headlines_rss.py",
    ),
    SuiteTask(
        task_id="INFO-04",
        name="FRED macro series",
        script_name="04_collect_fred_macro_series.py",
    ),
    SuiteTask(
        task_id="INFO-05",
        name="Bank of England Bank Rate",
        script_name="05_collect_boe_bank_rate.py",
    ),
    SuiteTask(
        task_id="INFO-06",
        name="US Treasury average interest rates",
        script_name="06_collect_us_treasury_yield_curve.py",
    ),
    SuiteTask(
        task_id="INFO-07",
        name="CFTC COT TFF positioning",
        script_name="07_collect_cftc_cot_tff.py",
    ),
    SuiteTask(
        task_id="INFO-00",
        name="Information data audit",
        script_name="00_information_data_audit.py",
    ),
]


def classify_result(
    task: SuiteTask,
    return_code: int,
    stdout: str,
    stderr: str,
) -> tuple[str, str]:
    combined = f"{stdout}\n{stderr}".lower()

    if return_code != 0:
        return "FAILED", f"Process exited with code {return_code}"

    optional_skip_markers = [
        "api_key is not set",
        "no economic calendar rows returned",
        "payment required",
        "endpoint appears unavailable",
    ]

    if not task.required and any(marker in combined for marker in optional_skip_markers):
        return "SKIPPED", "Optional source unavailable or missing credentials"

    warning_markers = [
        "[warn]",
        "warning:",
    ]

    if any(marker in combined for marker in warning_markers):
        return "PASS_WITH_WARNINGS", "Completed with warnings"

    return "PASS", "Completed successfully"


def run_task(task: SuiteTask) -> TaskResult:
    script_path = SCRIPT_DIR / task.script_name

    if not script_path.exists():
        return TaskResult(
            task_id=task.task_id,
            name=task.name,
            script_name=task.script_name,
            status="FAILED",
            return_code=None,
            duration_seconds=0.0,
            stdout="",
            stderr="",
            note=f"Script not found: {script_path}",
        )

    start_time = time.perf_counter()

    process = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    duration_seconds = time.perf_counter() - start_time

    status, note = classify_result(
        task=task,
        return_code=process.returncode,
        stdout=process.stdout,
        stderr=process.stderr,
    )

    return TaskResult(
        task_id=task.task_id,
        name=task.name,
        script_name=task.script_name,
        status=status,
        return_code=process.returncode,
        duration_seconds=duration_seconds,
        stdout=process.stdout,
        stderr=process.stderr,
        note=note,
    )


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes, remaining_seconds = divmod(seconds, 60)

    if minutes < 60:
        return f"{int(minutes)}m {remaining_seconds:.0f}s"

    hours, remaining_minutes = divmod(int(minutes), 60)

    return f"{hours}h {remaining_minutes}m {remaining_seconds:.0f}s"


def print_task_result(result: TaskResult) -> None:
    status_width = 18

    print(
        f"{result.task_id:<8} "
        f"{result.name:<42} "
        f"{result.status:<{status_width}} "
        f"{format_duration(result.duration_seconds):>10}"
    )

    if result.note:
        print(f"{'':<9}Note: {result.note}")


def write_summary(
    results: list[TaskResult],
    suite_started_utc: datetime,
    total_duration_seconds: float,
) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    pass_statuses = {"PASS", "PASS_WITH_WARNINGS"}
    failed = [result for result in results if result.status == "FAILED"]
    skipped = [result for result in results if result.status == "SKIPPED"]
    passed = [result for result in results if result.status in pass_statuses]

    lines = [
        "=" * 100,
        "BACQE INFORMATION DATA SUITE SUMMARY",
        "=" * 100,
        f"Started UTC:    {suite_started_utc.isoformat()}",
        f"Completed UTC:  {datetime.now(timezone.utc).isoformat()}",
        f"Project root:   {PROJECT_ROOT}",
        f"Python:         {sys.executable}",
        f"Duration:       {format_duration(total_duration_seconds)}",
        "",
        "-" * 100,
        "TASK RESULTS",
        "-" * 100,
    ]

    for result in results:
        lines.append(
            f"{result.task_id:<8} "
            f"{result.name:<42} "
            f"{result.status:<18} "
            f"{format_duration(result.duration_seconds):>10}"
        )

        if result.note:
            lines.append(f"         Note: {result.note}")

    lines.extend(
        [
            "",
            "-" * 100,
            "SUITE TOTALS",
            "-" * 100,
            f"Passed:              {len(passed)}",
            f"Skipped:             {len(skipped)}",
            f"Failed:              {len(failed)}",
            f"Tasks attempted:     {len(results)}",
            "",
        ]
    )

    if failed:
        lines.extend(
            [
                "-" * 100,
                "FAILED TASK DETAILS",
                "-" * 100,
            ]
        )

        for result in failed:
            lines.extend(
                [
                    "",
                    f"{result.task_id} - {result.name}",
                    f"Script: {result.script_name}",
                    f"Return code: {result.return_code}",
                    f"Note: {result.note}",
                    "",
                    "STDOUT:",
                    result.stdout.strip() or "<empty>",
                    "",
                    "STDERR:",
                    result.stderr.strip() or "<empty>",
                ]
            )

    warning_results = [
        result
        for result in results
        if result.status in {"PASS_WITH_WARNINGS", "SKIPPED"}
    ]

    if warning_results:
        lines.extend(
            [
                "",
                "-" * 100,
                "WARNINGS AND SKIPS",
                "-" * 100,
            ]
        )

        for result in warning_results:
            lines.extend(
                [
                    "",
                    f"{result.task_id} - {result.name}",
                    f"Status: {result.status}",
                    f"Note: {result.note}",
                ]
            )

    lines.extend(
        [
            "",
            "=" * 100,
            "OVERALL RESULT",
            "=" * 100,
        ]
    )

    if failed:
        lines.append("FAILED — one or more required tasks failed.")
    elif skipped or warning_results:
        lines.append("PASS WITH WARNINGS — suite completed with skips or warnings.")
    else:
        lines.append("PASS — all tasks completed successfully.")

    SUMMARY_FILE.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    suite_started_utc = datetime.now(timezone.utc)
    suite_start_time = time.perf_counter()

    print("=" * 100)
    print("BACQE INFORMATION DATA SUITE")
    print("=" * 100)
    print(f"Started UTC:  {suite_started_utc.isoformat()}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Python:       {sys.executable}")
    print("-" * 100)

    results: list[TaskResult] = []

    for task in TASKS:
        print()
        print(f"[RUN] {task.task_id} - {task.name}")

        result = run_task(task)
        results.append(result)

        print_task_result(result)

        if result.stdout.strip():
            print()
            print(result.stdout.rstrip())

        if result.stderr.strip():
            print()
            print("[STDERR]")
            print(result.stderr.rstrip())

    total_duration_seconds = time.perf_counter() - suite_start_time

    write_summary(
        results=results,
        suite_started_utc=suite_started_utc,
        total_duration_seconds=total_duration_seconds,
    )

    failed_required_tasks = [
        result
        for result, task in zip(results, TASKS)
        if result.status == "FAILED" and task.required
    ]

    passed_count = sum(
        result.status in {"PASS", "PASS_WITH_WARNINGS"}
        for result in results
    )
    skipped_count = sum(
        result.status == "SKIPPED"
        for result in results
    )
    failed_count = sum(
        result.status == "FAILED"
        for result in results
    )

    print()
    print("=" * 100)
    print("BACQE INFORMATION DATA SUITE COMPLETE")
    print("=" * 100)
    print(f"Passed:       {passed_count}")
    print(f"Skipped:      {skipped_count}")
    print(f"Failed:       {failed_count}")
    print(f"Duration:     {format_duration(total_duration_seconds)}")
    print(f"Summary file: {SUMMARY_FILE}")

    if failed_required_tasks:
        print("Overall:      FAILED")
        return 1

    if skipped_count or failed_count:
        print("Overall:      PASS WITH WARNINGS")
        return 0

    print("Overall:      PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())