from __future__ import annotations

"""BACQE EH17 - Discovery Science Orchestrator.

Runs EH14 -> EH15 -> EH16 in dependency order, validates their scientific
artifacts, records an append-only execution ledger, and distinguishes expected
insufficient-history states from genuine operational failures.
"""

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ENGINE_VERSION = "1.0.0"
WIDTH = 110

DEFAULT_ANALYSIS_ROOT = Path(r"E:\Quant_Lab\data\analysis\dukascopy_extended_horizons")
DEFAULT_SCRIPT_DIR = Path(__file__).resolve().parent

ENGINE_SPECS = {
    "EH14": {
        "label": "Candidate Census",
        "script": "14_extended_horizon_candidate_census_engine.py",
        "required_outputs": [
            ("candidate_census/candidate_family_registry_latest.csv", {"min_rows": 1}),
            ("candidate_census/candidate_family_members_latest.csv", {"min_rows": 1}),
        ],
    },
    "EH15": {
        "label": "Evolution Memory",
        "script": "15_evolution_memory_baseline.py",
        "required_outputs": [
            ("evolution_memory/edge_family_history.csv", {"min_rows": 1}),
            ("evolution_memory/family_observation_summary_latest.csv", {"min_rows": 1}),
            ("evolution_memory/evolution_memory_state_latest.json", {}),
        ],
    },
    "EH16": {
        "label": "Evolution Analytics",
        "script": "16_edge_family_evolution_analytics.py",
        "required_outputs": [
            ("evolution_analytics/edge_family_evolution_analytics_latest.csv", {"min_rows": 1}),
            ("evolution_analytics/edge_family_evolution_summary_latest.csv", {"min_rows": 1}),
            ("evolution_analytics/edge_family_evolution_state_latest.json", {}),
        ],
    },
}

ENGINE_ORDER = ["EH14", "EH15", "EH16"]

LEDGER_COLUMNS = [
    "ledger_schema_version", "orchestrator_version", "run_id", "run_started_utc",
    "run_finished_utc", "overall_status", "engine_id", "engine_label", "engine_status",
    "return_code", "duration_seconds", "command", "stdout_tail", "stderr_tail",
    "validation_ok", "validation_message", "snapshot_id", "candidate_rows",
    "edge_family_count", "historical_snapshots", "family_observations",
    "evolution_events", "analysis_status",
]


@dataclass
class EngineResult:
    engine_id: str
    engine_label: str
    engine_status: str
    return_code: int | None
    duration_seconds: float
    command: str
    stdout_tail: str
    stderr_tail: str
    validation_ok: bool
    validation_message: str
    skipped_reason: str = ""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(value: datetime | None = None) -> str:
    return (value or utc_now()).isoformat(timespec="seconds")


def stable_id(prefix: str, *parts: object, length: int = 20) -> str:
    payload = "|".join("" if value is None else str(value) for value in parts)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]
    return f"{prefix}_{digest}"


def atomic_write_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", delete=False, dir=path.parent, suffix=".tmp"
    ) as handle:
        temp = Path(handle.name)
        handle.write(text)
    temp.replace(path)


def atomic_write_json(payload: dict[str, Any], path: Path) -> None:
    atomic_write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", path)


def atomic_write_csv(rows: list[dict[str, Any]], columns: list[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", newline="", delete=False, dir=path.parent, suffix=".tmp"
    ) as handle:
        temp = Path(handle.name)
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    temp.replace(path)


def append_csv_idempotent(
    new_rows: list[dict[str, Any]], columns: list[str], path: Path, identity: tuple[str, ...]
) -> int:
    existing: list[dict[str, Any]] = []
    if path.exists():
        with path.open("r", encoding="utf-8", newline="") as handle:
            existing = list(csv.DictReader(handle))

    existing_keys = {
        tuple(str(row.get(column, "")) for column in identity) for row in existing
    }
    additions = [
        row for row in new_rows
        if tuple(str(row.get(column, "")) for column in identity) not in existing_keys
    ]
    if additions:
        atomic_write_csv(existing + additions, columns, path)
    elif not path.exists():
        atomic_write_csv([], columns, path)
    return len(additions)


class RunLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.acquired = False

    def __enter__(self) -> "RunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise RuntimeError(
                f"Discovery Science orchestrator lock already exists: {self.path}"
            ) from exc
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {"pid": os.getpid(), "created_utc": utc_iso()},
                    indent=2,
                )
            )
        self.acquired = True
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self.acquired:
            self.path.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run and validate BACQE Discovery Science engines EH14-EH16."
    )
    parser.add_argument("--analysis-root", type=Path, default=DEFAULT_ANALYSIS_ROOT)
    parser.add_argument("--script-dir", type=Path, default=DEFAULT_SCRIPT_DIR)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--start-at", choices=ENGINE_ORDER, default="EH14")
    parser.add_argument("--stop-after", choices=ENGINE_ORDER, default="EH16")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=0)
    parser.add_argument("--tail-lines", type=int, default=25)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def selected_engines(start_at: str, stop_after: str) -> list[str]:
    start = ENGINE_ORDER.index(start_at)
    stop = ENGINE_ORDER.index(stop_after)
    if stop < start:
        raise ValueError("--stop-after cannot precede --start-at.")
    return ENGINE_ORDER[start: stop + 1]


def tail_text(text: str, line_count: int) -> str:
    lines = (text or "").splitlines()
    return "\n".join(lines[-line_count:])


def count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def validate_file(path: Path, rules: dict[str, Any]) -> tuple[bool, str]:
    if not path.exists():
        return False, f"missing output: {path}"
    if path.stat().st_size == 0:
        return False, f"empty output: {path}"
    if path.suffix.lower() == ".csv":
        rows = count_csv_rows(path)
        minimum = int(rules.get("min_rows", 0))
        if rows < minimum:
            return False, f"{path.name} has {rows} rows; expected at least {minimum}"
        return True, f"{path.name}: {rows} rows"
    if path.suffix.lower() == ".json":
        try:
            with path.open("r", encoding="utf-8") as handle:
                json.load(handle)
        except (json.JSONDecodeError, OSError) as exc:
            return False, f"invalid JSON {path}: {exc}"
        return True, f"{path.name}: valid JSON"
    return True, f"{path.name}: present"


def validate_engine_outputs(engine_id: str, analysis_root: Path) -> tuple[bool, str]:
    messages: list[str] = []
    valid = True
    for relative, rules in ENGINE_SPECS[engine_id]["required_outputs"]:
        ok, message = validate_file(analysis_root / relative, rules)
        valid = valid and ok
        messages.append(message)
    return valid, "; ".join(messages)


def engine_command(
    python_executable: str, script_path: Path, analysis_root: Path
) -> list[str]:
    return [
        python_executable,
        str(script_path),
        "--analysis-root",
        str(analysis_root),
    ]


def run_engine(
    engine_id: str,
    python_executable: str,
    script_dir: Path,
    analysis_root: Path,
    dry_run: bool,
    timeout_seconds: int,
    tail_lines: int,
) -> EngineResult:
    spec = ENGINE_SPECS[engine_id]
    script_path = script_dir / spec["script"]
    command = engine_command(python_executable, script_path, analysis_root)
    command_text = subprocess.list2cmdline(command)

    if not script_path.exists():
        return EngineResult(
            engine_id, spec["label"], "FAIL", None, 0.0, command_text, "",
            "", False, f"engine script not found: {script_path}",
        )

    if dry_run:
        return EngineResult(
            engine_id, spec["label"], "DRY_RUN", None, 0.0, command_text, "",
            "", True, "command constructed; execution intentionally skipped",
        )

    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            cwd=script_dir,
            text=True,
            capture_output=True,
            timeout=timeout_seconds or None,
            check=False,
        )
        duration = time.perf_counter() - started
    except subprocess.TimeoutExpired as exc:
        return EngineResult(
            engine_id, spec["label"], "FAIL", None,
            time.perf_counter() - started, command_text,
            tail_text(exc.stdout or "", tail_lines),
            tail_text(exc.stderr or "", tail_lines),
            False, f"engine timed out after {timeout_seconds} seconds",
        )
    except OSError as exc:
        return EngineResult(
            engine_id, spec["label"], "FAIL", None,
            time.perf_counter() - started, command_text, "", str(exc),
            False, f"engine could not be started: {exc}",
        )

    validation_ok, validation_message = validate_engine_outputs(engine_id, analysis_root)
    process_ok = completed.returncode == 0
    status = "PASS" if process_ok and validation_ok else "FAIL"
    return EngineResult(
        engine_id=engine_id,
        engine_label=spec["label"],
        engine_status=status,
        return_code=completed.returncode,
        duration_seconds=round(duration, 3),
        command=command_text,
        stdout_tail=tail_text(completed.stdout, tail_lines),
        stderr_tail=tail_text(completed.stderr, tail_lines),
        validation_ok=validation_ok,
        validation_message=validation_message,
    )


def skipped_result(engine_id: str, reason: str) -> EngineResult:
    spec = ENGINE_SPECS[engine_id]
    return EngineResult(
        engine_id, spec["label"], "SKIPPED", None, 0.0, "", "", "",
        False, reason, reason,
    )


def first_nonempty(row: dict[str, str], names: Iterable[str]) -> str:
    for name in names:
        value = row.get(name, "")
        if str(value).strip():
            return str(value).strip()
    return ""


def safe_int(value: object, default: int = 0) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return default


def collect_metrics(analysis_root: Path) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "snapshot_id": "",
        "candidate_rows": 0,
        "edge_family_count": 0,
        "historical_snapshots": 0,
        "family_observations": 0,
        "evolution_events": 0,
        "analysis_status": "",
    }

    registry = analysis_root / "candidate_census" / "candidate_family_registry_latest.csv"
    if registry.exists():
        rows = read_csv_rows(registry)
        metrics["candidate_rows"] = len(rows)
        family_ids = {
            first_nonempty(row, ["edge_family_id", "family_id"])
            for row in rows
            if first_nonempty(row, ["edge_family_id", "family_id"])
        }
        metrics["edge_family_count"] = len(family_ids) or len(rows)

    history = analysis_root / "evolution_memory" / "edge_family_history.csv"
    if history.exists():
        rows = read_csv_rows(history)
        metrics["family_observations"] = len(rows)
        snapshots = {
            row.get("snapshot_id", "").strip()
            for row in rows if row.get("snapshot_id", "").strip()
        }
        metrics["historical_snapshots"] = len(snapshots)
        if rows:
            ordered = sorted(
                rows,
                key=lambda row: (
                    row.get("observation_utc", ""),
                    row.get("snapshot_id", ""),
                ),
            )
            metrics["snapshot_id"] = ordered[-1].get("snapshot_id", "")

    events = analysis_root / "evolution_analytics" / "family_evolution_events.csv"
    if events.exists():
        metrics["evolution_events"] = count_csv_rows(events)

    summary = (
        analysis_root / "evolution_analytics"
        / "edge_family_evolution_summary_latest.csv"
    )
    if summary.exists():
        rows = read_csv_rows(summary)
        if rows:
            first = rows[0]
            metrics["analysis_status"] = first_nonempty(
                first,
                ["analysis_status", "status", "evolution_status"],
            )
            for row in rows:
                key = first_nonempty(row, ["metric", "measure", "name"]).lower()
                value = first_nonempty(row, ["value", "metric_value"])
                if key in {"analysis_status", "evolution_status"} and value:
                    metrics["analysis_status"] = value

    state = (
        analysis_root / "evolution_analytics"
        / "edge_family_evolution_state_latest.json"
    )
    if state.exists():
        try:
            payload = json.loads(state.read_text(encoding="utf-8"))
            metrics["analysis_status"] = str(
                payload.get("analysis_status")
                or payload.get("status")
                or metrics["analysis_status"]
            )
        except (json.JSONDecodeError, OSError):
            pass

    return metrics


def determine_overall_status(
    results: list[EngineResult], metrics: dict[str, Any], dry_run: bool
) -> str:
    if dry_run:
        return "DRY_RUN"
    if any(result.engine_status == "FAIL" for result in results):
        passed = sum(result.engine_status == "PASS" for result in results)
        return "PARTIAL" if passed else "FAIL"
    if any(result.engine_status == "SKIPPED" for result in results):
        return "PARTIAL"

    status = str(metrics.get("analysis_status", "")).strip().lower()
    snapshots = safe_int(metrics.get("historical_snapshots"))
    if status == "insufficient_history" or snapshots < 2:
        return "BASELINE_ESTABLISHED"
    return "PASS"


def build_summary_rows(
    run_id: str,
    started_utc: str,
    finished_utc: str,
    overall_status: str,
    results: list[EngineResult],
    metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in results:
        rows.append({
            "run_id": run_id,
            "run_started_utc": started_utc,
            "run_finished_utc": finished_utc,
            "overall_status": overall_status,
            **asdict(result),
            **metrics,
        })
    return rows


def ledger_rows(
    summary_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in summary_rows:
        rows.append({
            "ledger_schema_version": "1.0",
            "orchestrator_version": ENGINE_VERSION,
            **row,
        })
    return rows


def render_report(
    run_id: str,
    started_utc: str,
    finished_utc: str,
    overall_status: str,
    results: list[EngineResult],
    metrics: dict[str, Any],
    added_ledger_rows: int,
) -> str:
    lines = [
        "=" * WIDTH,
        "BACQE DISCOVERY SCIENCE ORCHESTRATOR",
        "=" * WIDTH,
        f"Engine version:            {ENGINE_VERSION}",
        f"Run ID:                    {run_id}",
        f"Started UTC:               {started_utc}",
        f"Finished UTC:              {finished_utc}",
        "-" * WIDTH,
    ]
    for result in results:
        lines.extend([
            f"{result.engine_id} {result.engine_label}",
            f"  Status:                  {result.engine_status}",
            f"  Return code:             {'' if result.return_code is None else result.return_code}",
            f"  Duration seconds:        {result.duration_seconds:.3f}",
            f"  Validation:              {'PASS' if result.validation_ok else 'FAIL'}",
            f"  Detail:                  {result.validation_message}",
        ])
        if result.stderr_tail:
            lines.append("  STDERR tail:")
            lines.extend(f"    {line}" for line in result.stderr_tail.splitlines())
        lines.append("-" * WIDTH)

    lines.extend([
        "DISCOVERY SCIENCE EVIDENCE",
        f"Candidate rows:             {metrics['candidate_rows']}",
        f"Edge families:              {metrics['edge_family_count']}",
        f"Latest snapshot ID:         {metrics['snapshot_id']}",
        f"Historical snapshots:       {metrics['historical_snapshots']}",
        f"Family observations:        {metrics['family_observations']}",
        f"Evolution events:           {metrics['evolution_events']}",
        f"EH16 analysis status:       {metrics['analysis_status']}",
        f"Ledger rows appended:       {added_ledger_rows}",
        "-" * WIDTH,
        f"OVERALL STATUS:             {overall_status}",
    ])
    if overall_status == "BASELINE_ESTABLISHED":
        lines.append(
            "Interpretation: EH14-EH16 completed successfully; genuine evolution "
            "requires at least two distinct EH15 snapshots."
        )
    elif overall_status == "PASS":
        lines.append(
            "Interpretation: the Discovery Science pipeline completed and longitudinal "
            "evolution evidence is available."
        )
    elif overall_status in {"FAIL", "PARTIAL"}:
        lines.append(
            "Interpretation: one or more operational stages failed or were skipped; "
            "inspect the engine details above."
        )
    lines.append("=" * WIDTH)
    return "\n".join(lines) + "\n"


def execute(args: argparse.Namespace) -> tuple[str, int]:
    analysis_root = args.analysis_root.resolve()
    script_dir = args.script_dir.resolve()
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir
        else analysis_root / "discovery_science_orchestrator"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    selected = selected_engines(args.start_at, args.stop_after)
    started = utc_now()
    started_utc = utc_iso(started)
    run_id = stable_id(
        "dsrun",
        started_utc,
        args.start_at,
        args.stop_after,
        analysis_root,
        os.getpid(),
    )

    lock_path = output_dir / ".discovery_science_orchestrator.lock"
    with RunLock(lock_path):
        results: list[EngineResult] = []
        upstream_failed = False
        for engine_id in selected:
            if upstream_failed and not args.continue_on_failure:
                results.append(
                    skipped_result(
                        engine_id,
                        "upstream dependency failed; execution stopped safely",
                    )
                )
                continue

            result = run_engine(
                engine_id=engine_id,
                python_executable=args.python,
                script_dir=script_dir,
                analysis_root=analysis_root,
                dry_run=args.dry_run,
                timeout_seconds=args.timeout_seconds,
                tail_lines=args.tail_lines,
            )
            results.append(result)
            if result.engine_status == "FAIL":
                upstream_failed = True

        metrics = collect_metrics(analysis_root)
        finished = utc_now()
        finished_utc = utc_iso(finished)
        overall_status = determine_overall_status(results, metrics, args.dry_run)
        summary_rows = build_summary_rows(
            run_id, started_utc, finished_utc, overall_status, results, metrics
        )

        summary_path = output_dir / "discovery_science_orchestrator_summary_latest.csv"
        report_path = output_dir / "discovery_science_orchestrator_report_latest.txt"
        state_path = output_dir / "discovery_science_orchestrator_state_latest.json"
        ledger_path = output_dir / "discovery_science_engine_runs.csv"

        result_columns = list(asdict(results[0]).keys()) if results else []
        summary_columns = [
            "run_id", "run_started_utc", "run_finished_utc", "overall_status",
            *result_columns,
            "snapshot_id", "candidate_rows", "edge_family_count",
            "historical_snapshots", "family_observations", "evolution_events",
            "analysis_status",
        ]
        # Preserve order while removing any duplicate column names.
        summary_columns = list(dict.fromkeys(summary_columns))
        atomic_write_csv(summary_rows, summary_columns, summary_path)

        new_ledger_rows = ledger_rows(summary_rows)
        added = append_csv_idempotent(
            new_ledger_rows,
            LEDGER_COLUMNS,
            ledger_path,
            identity=("run_id", "engine_id"),
        )

        report = render_report(
            run_id, started_utc, finished_utc, overall_status, results, metrics, added
        )
        atomic_write_text(report, report_path)

        state = {
            "orchestrator_version": ENGINE_VERSION,
            "run_id": run_id,
            "run_started_utc": started_utc,
            "run_finished_utc": finished_utc,
            "overall_status": overall_status,
            "selected_engines": selected,
            "metrics": metrics,
            "results": [asdict(result) for result in results],
            "outputs": {
                "summary": str(summary_path),
                "report": str(report_path),
                "state": str(state_path),
                "ledger": str(ledger_path),
            },
        }
        atomic_write_json(state, state_path)

    print(report, end="")
    return overall_status, 0 if overall_status in {
        "PASS", "BASELINE_ESTABLISHED", "DRY_RUN"
    } else 1


def self_test() -> int:
    checks: list[tuple[str, bool]] = []

    def check(name: str, condition: bool) -> None:
        checks.append((name, bool(condition)))

    with tempfile.TemporaryDirectory(prefix="bacqe_eh17_") as temp_name:
        root = Path(temp_name)
        scripts = root / "scripts"
        analysis = root / "analysis"
        output = root / "orchestrator"
        scripts.mkdir()
        analysis.mkdir()

        check("engine selection", selected_engines("EH14", "EH16") == ENGINE_ORDER)
        try:
            selected_engines("EH16", "EH14")
            invalid_range = False
        except ValueError:
            invalid_range = True
        check("invalid engine range rejected", invalid_range)

        # Test lock exclusivity.
        lock_path = output / ".lock"
        output.mkdir()
        with RunLock(lock_path):
            try:
                with RunLock(lock_path):
                    second_lock_failed = False
            except RuntimeError:
                second_lock_failed = True
        check("concurrent lock rejected", second_lock_failed)
        check("lock cleaned", not lock_path.exists())

        # Build synthetic engine scripts that produce the expected artifacts.
        script_template = r"""
import argparse, csv, json
from pathlib import Path
p = argparse.ArgumentParser()
p.add_argument("--analysis-root", type=Path, required=True)
a = p.parse_args()
root = a.analysis_root
ENGINE_BODY
"""
        bodies = {
            "EH14": """
d = root / "candidate_census"; d.mkdir(parents=True, exist_ok=True)
for name, rows in {
 "candidate_family_registry_latest.csv": [
  {"edge_family_id":"F1"}, {"edge_family_id":"F2"}
 ],
 "candidate_family_members_latest.csv": [
  {"edge_family_id":"F1"}, {"edge_family_id":"F2"}
 ]}.items():
 with (d/name).open("w", newline="", encoding="utf-8") as h:
  w=csv.DictWriter(h, fieldnames=["edge_family_id"]); w.writeheader(); w.writerows(rows)
""",
            "EH15": """
d = root / "evolution_memory"; d.mkdir(parents=True, exist_ok=True)
rows=[{"snapshot_id":"S1","observation_utc":"2026-07-19T08:52:38+00:00","edge_family_id":"F1"},
      {"snapshot_id":"S1","observation_utc":"2026-07-19T08:52:38+00:00","edge_family_id":"F2"}]
for name in ["edge_family_history.csv","family_observation_summary_latest.csv"]:
 with (d/name).open("w", newline="", encoding="utf-8") as h:
  w=csv.DictWriter(h, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
(d/"evolution_memory_state_latest.json").write_text(json.dumps({"snapshot_id":"S1"}))
""",
            "EH16": """
d = root / "evolution_analytics"; d.mkdir(parents=True, exist_ok=True)
with (d/"edge_family_evolution_analytics_latest.csv").open("w", newline="", encoding="utf-8") as h:
 w=csv.DictWriter(h, fieldnames=["edge_family_id","analysis_status"]); w.writeheader()
 w.writerows([{"edge_family_id":"F1","analysis_status":"insufficient_history"},
              {"edge_family_id":"F2","analysis_status":"insufficient_history"}])
with (d/"edge_family_evolution_summary_latest.csv").open("w", newline="", encoding="utf-8") as h:
 w=csv.DictWriter(h, fieldnames=["analysis_status"]); w.writeheader()
 w.writerow({"analysis_status":"insufficient_history"})
with (d/"family_evolution_events.csv").open("w", newline="", encoding="utf-8") as h:
 csv.writer(h).writerow(["event_id"])
(d/"edge_family_evolution_state_latest.json").write_text(
 json.dumps({"analysis_status":"insufficient_history"}))
""",
        }
        for engine_id, spec in ENGINE_SPECS.items():
            text = script_template.replace("ENGINE_BODY", bodies[engine_id])
            (scripts / spec["script"]).write_text(text, encoding="utf-8")

        args = argparse.Namespace(
            analysis_root=analysis,
            script_dir=scripts,
            output_dir=output,
            python=sys.executable,
            start_at="EH14",
            stop_after="EH16",
            dry_run=False,
            continue_on_failure=False,
            timeout_seconds=30,
            tail_lines=10,
            self_test=False,
        )
        status, code = execute(args)
        check("baseline status", status == "BASELINE_ESTABLISHED")
        check("baseline exit code", code == 0)
        check("summary written", (output / "discovery_science_orchestrator_summary_latest.csv").exists())
        check("report written", (output / "discovery_science_orchestrator_report_latest.txt").exists())
        check("state written", (output / "discovery_science_orchestrator_state_latest.json").exists())
        check("ledger written", (output / "discovery_science_engine_runs.csv").exists())

        summary_rows = read_csv_rows(
            output / "discovery_science_orchestrator_summary_latest.csv"
        )
        check("three engines recorded", len(summary_rows) == 3)
        check("all engines passed", all(row["engine_status"] == "PASS" for row in summary_rows))

        metrics = collect_metrics(analysis)
        check("candidate metrics", metrics["candidate_rows"] == 2)
        check("family metrics", metrics["edge_family_count"] == 2)
        check("snapshot metrics", metrics["historical_snapshots"] == 1)
        check("observation metrics", metrics["family_observations"] == 2)
        check("insufficient history recognised", metrics["analysis_status"] == "insufficient_history")

        # Validate dry-run.
        dry_args = argparse.Namespace(**vars(args))
        dry_args.output_dir = root / "dry_output"
        dry_args.dry_run = True
        dry_status, dry_code = execute(dry_args)
        check("dry run status", dry_status == "DRY_RUN")
        check("dry run exit code", dry_code == 0)

        # Validate failure propagation with a missing EH14 script.
        failure_scripts = root / "failure_scripts"
        failure_scripts.mkdir()
        fail_args = argparse.Namespace(**vars(args))
        fail_args.script_dir = failure_scripts
        fail_args.output_dir = root / "failure_output"
        fail_status, fail_code = execute(fail_args)
        check("failure status", fail_status == "FAIL")
        check("failure exit code", fail_code == 1)
        fail_rows = read_csv_rows(
            fail_args.output_dir / "discovery_science_orchestrator_summary_latest.csv"
        )
        check("downstream engines skipped", [r["engine_status"] for r in fail_rows] == ["FAIL", "SKIPPED", "SKIPPED"])

        # Validate file rules.
        valid, _ = validate_engine_outputs("EH14", analysis)
        check("output validation", valid)

        # Validate idempotent ledger append for same identity.
        ledger = root / "idempotent.csv"
        sample = [{column: "" for column in LEDGER_COLUMNS}]
        sample[0].update({"run_id": "R1", "engine_id": "EH14"})
        first = append_csv_idempotent(sample, LEDGER_COLUMNS, ledger, ("run_id", "engine_id"))
        second = append_csv_idempotent(sample, LEDGER_COLUMNS, ledger, ("run_id", "engine_id"))
        check("ledger first append", first == 1)
        check("ledger duplicate prevented", second == 0)
        check("ledger remains one row", count_csv_rows(ledger) == 1)

    passed = sum(ok for _, ok in checks)
    print("=" * WIDTH)
    print("BACQE EH17 SELF-TEST")
    print("=" * WIDTH)
    for name, ok in checks:
        print(f"{'PASS' if ok else 'FAIL':<6} {name}")
    print("-" * WIDTH)
    print(f"Passed: {passed}/{len(checks)}")
    print("=" * WIDTH)
    return 0 if passed == len(checks) else 1


def main() -> int:
    args = parse_args()
    if args.self_test:
        return self_test()
    try:
        _, code = execute(args)
        return code
    except (ValueError, RuntimeError, OSError) as exc:
        print(f"EH17 ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())