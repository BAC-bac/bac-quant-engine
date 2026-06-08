from pathlib import Path
from datetime import datetime
import subprocess
import csv
from io import StringIO
import pandas as pd


QUANT_ROOT = Path("E:/Quant_Lab")
REPORT_DIR = QUANT_ROOT / "data/analysis/sentinel/scheduler_automation_health"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = REPORT_DIR / "scheduler_automation_health_latest.csv"
OUTPUT_JSON = REPORT_DIR / "scheduler_automation_health_latest.json"
OUTPUT_TXT = REPORT_DIR / "scheduler_automation_health_latest.txt"


TASK_NAME_FILTERS = [
    "BACQE",
    "Quant",
]


def run_powershell(command: str) -> str:
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr)

    return result.stdout.strip()


def classify_task(row: dict) -> tuple[str, str]:
    result = str(row.get("LastTaskResult", "")).strip()
    state = str(row.get("State", "")).strip()
    missed = str(row.get("NumberOfMissedRuns", "0")).strip()

    if result in {"0", "0x0"} and missed in {"0", ""}:
        return "ok", "Task last completed successfully."

    if state.lower() == "disabled":
        return "warning", "Task is disabled."

    if missed not in {"0", ""}:
        return "warning", "Task has missed scheduled runs."

    return "warning", f"Review task result/state. LastTaskResult={result}, State={state}"


def main() -> None:
    print("=" * 90)
    print("BACQE SENTINEL 07 - SCHEDULER AUTOMATION HEALTH")
    print("=" * 90)

    checked_at = datetime.now()

    filter_expression = " -or ".join(
        [f"$_.TaskName -like '*{name_filter}*'" for name_filter in TASK_NAME_FILTERS]
    )

    command = f"""
    Get-ScheduledTask |
    Where-Object {{{filter_expression}}} |
    ForEach-Object {{
        $info = Get-ScheduledTaskInfo -TaskName $_.TaskName -TaskPath $_.TaskPath
        [PSCustomObject]@{{
            TaskName = $_.TaskName
            TaskPath = $_.TaskPath
            State = $_.State
            LastRunTime = $info.LastRunTime
            NextRunTime = $info.NextRunTime
            LastTaskResult = $info.LastTaskResult
            NumberOfMissedRuns = $info.NumberOfMissedRuns
        }}
    }} | ConvertTo-Csv -NoTypeInformation
    """

    output = run_powershell(command)

    rows = []

    if not output:
        rows.append({
            "checked_at": checked_at,
            "task_name": "NO_MATCHING_TASKS_FOUND",
            "task_path": None,
            "state": None,
            "last_run_time": None,
            "next_run_time": None,
            "last_task_result": None,
            "number_of_missed_runs": None,
            "health_status": "warning",
            "notes": "No scheduled tasks matching BACQE or Quant were found.",
        })
    else:
        reader = csv.DictReader(StringIO(output))

        for ps_row in reader:
            health_status, notes = classify_task(ps_row)

            rows.append({
                "checked_at": checked_at,
                "task_name": ps_row.get("TaskName"),
                "task_path": ps_row.get("TaskPath"),
                "state": ps_row.get("State"),
                "last_run_time": ps_row.get("LastRunTime"),
                "next_run_time": ps_row.get("NextRunTime"),
                "last_task_result": ps_row.get("LastTaskResult"),
                "number_of_missed_runs": ps_row.get("NumberOfMissedRuns"),
                "health_status": health_status,
                "notes": notes,
            })

    df = pd.DataFrame(rows)

    ok_count = int((df["health_status"] == "ok").sum())
    warning_count = int((df["health_status"] == "warning").sum())
    critical_count = int((df["health_status"] == "critical").sum())

    df.to_csv(OUTPUT_CSV, index=False)
    df.to_json(OUTPUT_JSON, orient="records", indent=2, date_format="iso")

    txt_lines = [
        "=" * 90,
        "BACQE SENTINEL 07 - SCHEDULER AUTOMATION HEALTH",
        "=" * 90,
        f"Checked at: {checked_at}",
        f"Tasks discovered: {len(df)}",
        f"OK: {ok_count}",
        f"Warnings: {warning_count}",
        f"Critical: {critical_count}",
        "-" * 90,
        df.to_string(index=False),
        "=" * 90,
    ]

    OUTPUT_TXT.write_text("\n".join(txt_lines), encoding="utf-8")

    print(df)
    print("-" * 90)
    print(f"Tasks discovered: {len(df)}")
    print(f"OK: {ok_count}")
    print(f"Warnings: {warning_count}")
    print(f"Critical: {critical_count}")
    print("-" * 90)
    print(f"[SAVED] {OUTPUT_CSV}")
    print(f"[SAVED] {OUTPUT_JSON}")
    print(f"[SAVED] {OUTPUT_TXT}")
    print("=" * 90)


if __name__ == "__main__":
    main()