from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

ENGINE_ID = "CS06"
ENGINE_NAME = "Experiment Execution Ledger"
ENGINE_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"
SOURCE_ENGINE_ID = "CS05"

REQUIRED_REGISTRY_COLUMNS = (
    "registry_rank", "registry_id", "experiment_id", "candidate_id",
    "experiment_title", "experiment_family", "primary_filter",
    "registry_status", "review_status", "execution_authorised",
    "protocol_revision", "protocol_fingerprint", "minimum_observations",
    "minimum_sole_veto_observations", "maximum_calendar_days",
    "priority_score", "priority_band", "principal_risk", "stopping_rule",
    "success_criteria", "failure_criteria", "inconclusive_criteria",
    "risk_guardrails", "source_designs_sha256", "source_audit_sha256",
    "generated_utc", "engine_version",
)

VALID_EXECUTION_STATUSES = {
    "AWAITING_APPROVAL", "READY", "RUNNING", "PAUSED", "STOPPED",
    "COMPLETED", "INVALIDATED", "CANCELLED",
}
ACTIVE_REGISTRY_STATUSES = {"APPROVED", "RUNNING", "PAUSED", "COMPLETED"}
TERMINAL_EXECUTION_STATUSES = {"STOPPED", "COMPLETED", "INVALIDATED", "CANCELLED"}
VALID_STOP_REASONS = {
    "", "TARGET_REACHED", "CALENDAR_LIMIT", "GUARDRAIL_BREACH",
    "MANUAL_PAUSE", "MANUAL_CANCEL", "DATA_QUALITY_FAILURE",
    "PROTOCOL_INVALIDATED",
}

LEDGER_COLUMNS = (
    "ledger_rank", "ledger_id", "registry_id", "experiment_id", "candidate_id",
    "experiment_title", "experiment_family", "primary_filter",
    "execution_status", "execution_authorised", "registry_status",
    "review_status", "protocol_revision", "protocol_fingerprint",
    "execution_run_id", "execution_environment", "strategy_version",
    "parameter_set_id", "dataset_id", "dataset_sha256", "started_utc",
    "last_observation_utc", "ended_utc", "elapsed_calendar_days",
    "observations_collected", "sole_veto_observations_collected",
    "minimum_observations", "minimum_sole_veto_observations",
    "maximum_calendar_days", "observation_progress", "sole_veto_progress", "evidence_target_reached",
    "calendar_limit_reached", "guardrail_breach", "data_quality_passed",
    "interim_review_due", "stopping_rule_triggered", "stop_reason",
    "current_evidence_state", "operator_notes", "results_path",
    "registered_utc", "last_updated_utc", "priority_score", "priority_band",
    "principal_risk", "stopping_rule", "success_criteria", "failure_criteria",
    "inconclusive_criteria", "risk_guardrails", "source_engine_id",
    "source_engine_version", "source_registry_sha256", "source_audit_sha256",
    "source_generated_utc", "generated_utc", "engine_version",
)

HISTORY_COLUMNS = (
    "history_id", "ledger_id", "registry_id", "experiment_id", "event_utc",
    "event_type", "from_execution_status", "to_execution_status",
    "observations_collected", "sole_veto_observations_collected",
    "protocol_revision", "actor", "event_reason", "source_registry_sha256",
    "engine_version",
)

READINESS_COLUMNS = (
    "readiness_rank", "ledger_id", "registry_id", "experiment_id",
    "experiment_title", "primary_filter", "execution_status",
    "readiness_class", "readiness_reason", "execution_authorised",
    "registry_status", "review_status", "minimum_observations",
    "minimum_sole_veto_observations", "maximum_calendar_days", "priority_score", "priority_band",
)

MUTABLE_COLUMNS = (
    "execution_run_id", "execution_environment", "strategy_version",
    "parameter_set_id", "dataset_id", "dataset_sha256", "started_utc",
    "last_observation_utc", "ended_utc", "observations_collected",
    "sole_veto_observations_collected", "guardrail_breach",
    "data_quality_passed", "stop_reason", "current_evidence_state",
    "operator_notes", "results_path",
)


class ExecutionLedgerError(ValueError):
    """Raised when CS06 cannot produce a valid execution ledger."""


@dataclass(frozen=True)
class ValidationResult:
    passed: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class SourceLineage:
    engine_id: str
    engine_name: str
    engine_version: str
    generated_utc: str
    registry_sha256: str
    audit_sha256: str


@dataclass(frozen=True)
class OutputPaths:
    output_directory: Path
    ledger_csv: Path
    history_csv: Path
    readiness_csv: Path
    report_txt: Path
    audit_json: Path


@dataclass(frozen=True)
class LedgerOutputs:
    ledger: pd.DataFrame
    history: pd.DataFrame
    readiness: pd.DataFrame
    validation: ValidationResult
    source_lineage: SourceLineage
    paths: OutputPaths


def _require_columns(frame: pd.DataFrame, required: Iterable[str], label: str) -> list[str]:
    return [f"{label} missing required column: {c}" for c in required if c not in frame.columns]


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _normalise(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_ledger_id(registry_id: str) -> str:
    token = hashlib.sha256(f"{registry_id}|CS06".encode()).hexdigest()[:12].upper()
    return f"CS06-LED-{token}"


def _history_id(ledger_id: str, event_type: str, event_utc: str, observations: int) -> str:
    token = hashlib.sha256(
        f"{ledger_id}|{event_type}|{event_utc}|{observations}".encode()
    ).hexdigest()[:16].upper()
    return f"CS06-HIST-{token}"


def validate_inputs(registry: pd.DataFrame, audit: dict[str, Any]) -> ValidationResult:
    errors = _require_columns(registry, REQUIRED_REGISTRY_COLUMNS, "experiment registry")
    warnings: list[str] = []
    if registry.empty:
        errors.append("experiment registry contains no rows")
    if not errors:
        if registry["registry_id"].duplicated().any():
            errors.append("registry_id values must be unique")
        if registry["experiment_id"].duplicated().any():
            errors.append("experiment_id values must be unique")
        authorised = registry["execution_authorised"].map(_to_bool)
        invalid_authority = authorised & ~registry["registry_status"].isin(ACTIVE_REGISTRY_STATUSES)
        if invalid_authority.any():
            errors.append("execution authority is inconsistent with registry lifecycle status")
        active_without_authority = registry["registry_status"].isin({"RUNNING", "PAUSED", "COMPLETED"}) & ~authorised
        if active_without_authority.any():
            errors.append("active or completed registry rows must be execution authorised")
        if (~authorised).any():
            warnings.append("One or more experiments lack execution authority and will remain AWAITING_APPROVAL.")
    if audit.get("engine_id") != SOURCE_ENGINE_ID:
        errors.append("source audit must identify engine_id CS05")
    if not audit.get("validation", {}).get("passed", False):
        errors.append("source CS05 audit validation did not pass")
    policy = audit.get("policy", {})
    if policy.get("execution_requires_explicit_authorisation") is not True:
        errors.append("CS05 audit must require explicit execution authorisation")
    if policy.get("registration_implies_approval") is not False:
        errors.append("CS05 audit must state registration_implies_approval=false")
    return ValidationResult(not errors, tuple(errors), tuple(warnings))


def validate_existing_ledger(ledger: pd.DataFrame) -> ValidationResult:
    if ledger.empty:
        return ValidationResult(True, (), ())
    errors = _require_columns(ledger, LEDGER_COLUMNS, "existing execution ledger")
    if not errors:
        if ledger["ledger_id"].duplicated().any():
            errors.append("ledger_id values must be unique")
        if ledger["registry_id"].duplicated().any():
            errors.append("registry_id values must be unique")
        unknown = sorted(set(ledger["execution_status"].astype(str)) - VALID_EXECUTION_STATUSES)
        if unknown:
            errors.append(f"invalid execution statuses: {', '.join(unknown)}")
        stop_reasons = sorted(set(ledger["stop_reason"].fillna("").astype(str)) - VALID_STOP_REASONS)
        if stop_reasons:
            errors.append(f"invalid stop reasons: {', '.join(stop_reasons)}")
        obs = pd.to_numeric(ledger["observations_collected"], errors="coerce")
        sole = pd.to_numeric(ledger["sole_veto_observations_collected"], errors="coerce")
        if obs.isna().any() or sole.isna().any() or (obs < 0).any() or (sole < 0).any():
            errors.append("observation counters must be non-negative numeric values")
        running = ledger["execution_status"].isin({"RUNNING", "PAUSED", "STOPPED", "COMPLETED"})
        if (running & ~ledger["execution_authorised"].map(_to_bool)).any():
            errors.append("started execution records must be authorised")
        if (ledger["execution_status"].eq("RUNNING") & ledger["started_utc"].fillna("").astype(str).eq("")).any():
            errors.append("RUNNING records require started_utc")
    return ValidationResult(not errors, tuple(errors), ())


def _initial_status(row: pd.Series) -> str:
    authorised = _to_bool(row["execution_authorised"])
    status = str(row["registry_status"])
    if not authorised:
        return "AWAITING_APPROVAL"
    if status == "APPROVED":
        return "READY"
    if status == "RUNNING":
        return "RUNNING"
    if status == "PAUSED":
        return "PAUSED"
    if status == "COMPLETED":
        return "COMPLETED"
    return "AWAITING_APPROVAL"


def _new_row(row: pd.Series, registry_hash: str, audit_hash: str, generated_utc: str) -> dict[str, Any]:
    status = _initial_status(row)
    return {
        "ledger_rank": int(row["registry_rank"]),
        "ledger_id": _stable_ledger_id(str(row["registry_id"])),
        "registry_id": row["registry_id"], "experiment_id": row["experiment_id"],
        "candidate_id": row["candidate_id"], "experiment_title": row["experiment_title"],
        "experiment_family": row["experiment_family"], "primary_filter": row["primary_filter"],
        "execution_status": status, "execution_authorised": _to_bool(row["execution_authorised"]),
        "registry_status": row["registry_status"], "review_status": row["review_status"],
        "protocol_revision": int(row["protocol_revision"]), "protocol_fingerprint": row["protocol_fingerprint"],
        "execution_run_id": "", "execution_environment": "", "strategy_version": "",
        "parameter_set_id": "", "dataset_id": "", "dataset_sha256": "",
        "started_utc": "", "last_observation_utc": "", "ended_utc": "",
        "elapsed_calendar_days": 0, "observations_collected": 0,
        "sole_veto_observations_collected": 0,
        "minimum_observations": int(row["minimum_observations"]),
        "minimum_sole_veto_observations": int(row["minimum_sole_veto_observations"]),
        "maximum_calendar_days": int(row["maximum_calendar_days"]),
        "observation_progress": 0.0, "sole_veto_progress": 0.0,
        "evidence_target_reached": False, "calendar_limit_reached": False,
        "guardrail_breach": False, "data_quality_passed": True,
        "interim_review_due": False, "stopping_rule_triggered": False,
        "stop_reason": "", "current_evidence_state": "NO_EVIDENCE",
        "operator_notes": "", "results_path": "",
        "registered_utc": generated_utc, "last_updated_utc": generated_utc,
        "priority_score": float(row["priority_score"]), "priority_band": row["priority_band"],
        "principal_risk": row["principal_risk"], "stopping_rule": row["stopping_rule"],
        "success_criteria": row["success_criteria"], "failure_criteria": row["failure_criteria"],
        "inconclusive_criteria": row["inconclusive_criteria"], "risk_guardrails": row["risk_guardrails"],
        "source_engine_id": SOURCE_ENGINE_ID, "source_engine_version": row["engine_version"],
        "source_registry_sha256": registry_hash, "source_audit_sha256": audit_hash,
        "source_generated_utc": row["generated_utc"], "generated_utc": generated_utc,
        "engine_version": ENGINE_VERSION,
    }


def _recalculate(row: dict[str, Any], generated_utc: str) -> dict[str, Any]:
    obs = max(0, int(float(row.get("observations_collected", 0) or 0)))
    sole = max(0, int(float(row.get("sole_veto_observations_collected", 0) or 0)))
    min_obs = max(1, int(float(row["minimum_observations"])))
    min_sole = max(0, int(float(row["minimum_sole_veto_observations"])))
    row["observations_collected"] = obs
    row["sole_veto_observations_collected"] = sole
    row["observation_progress"] = round(min(obs / min_obs, 1.0), 6)
    row["sole_veto_progress"] = round(1.0 if min_sole == 0 else min(sole / min_sole, 1.0), 6)
    row["evidence_target_reached"] = obs >= min_obs and sole >= min_sole
    row["guardrail_breach"] = _to_bool(row.get("guardrail_breach", False))
    row["data_quality_passed"] = _to_bool(row.get("data_quality_passed", True))
    row["calendar_limit_reached"] = int(float(row.get("elapsed_calendar_days", 0) or 0)) >= int(row.get("maximum_calendar_days", 10**9) or 10**9)
    row["interim_review_due"] = row["execution_status"] == "RUNNING" and obs > 0 and obs % max(100, min_obs // 4) == 0
    if row["guardrail_breach"]:
        row["stopping_rule_triggered"] = True
        row["stop_reason"] = "GUARDRAIL_BREACH"
    elif not row["data_quality_passed"]:
        row["stopping_rule_triggered"] = True
        row["stop_reason"] = "DATA_QUALITY_FAILURE"
    elif row["evidence_target_reached"]:
        row["stopping_rule_triggered"] = True
        row["stop_reason"] = row.get("stop_reason") or "TARGET_REACHED"
    elif row["calendar_limit_reached"]:
        row["stopping_rule_triggered"] = True
        row["stop_reason"] = row.get("stop_reason") or "CALENDAR_LIMIT"
    else:
        row["stopping_rule_triggered"] = False
        if row.get("stop_reason") in {"TARGET_REACHED", "CALENDAR_LIMIT", "GUARDRAIL_BREACH", "DATA_QUALITY_FAILURE"}:
            row["stop_reason"] = ""
    if obs == 0:
        row["current_evidence_state"] = "NO_EVIDENCE"
    elif row["evidence_target_reached"]:
        row["current_evidence_state"] = "TARGET_REACHED"
    elif obs >= min_obs * 0.5:
        row["current_evidence_state"] = "PARTIAL_MATURE"
    else:
        row["current_evidence_state"] = "PARTIAL_EARLY"
    row["last_updated_utc"] = generated_utc
    row["generated_utc"] = generated_utc
    return row


def merge_ledger(
    registry: pd.DataFrame,
    existing: pd.DataFrame | None,
    registry_hash: str,
    audit_hash: str,
    generated_utc: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    existing_map = {} if existing is None or existing.empty else {
        str(r["registry_id"]): r for _, r in existing.iterrows()
    }
    rows: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    for _, source in registry.sort_values("registry_rank").iterrows():
        registry_id = str(source["registry_id"])
        previous = existing_map.get(registry_id)
        if previous is None:
            current = _new_row(source, registry_hash, audit_hash, generated_utc)
            event_type, from_status = "LEDGER_REGISTERED", ""
        else:
            current = previous.to_dict()
            from_status = str(current["execution_status"])
            for field in (
                "experiment_id", "candidate_id", "experiment_title", "experiment_family",
                "primary_filter", "registry_status", "review_status", "protocol_revision",
                "protocol_fingerprint", "minimum_observations",
                "minimum_sole_veto_observations", "maximum_calendar_days", "priority_score", "priority_band",
                "principal_risk", "stopping_rule", "success_criteria", "failure_criteria",
                "inconclusive_criteria", "risk_guardrails",
            ):
                current[field] = source[field]
            current["execution_authorised"] = _to_bool(source["execution_authorised"])
            current["source_registry_sha256"] = registry_hash
            current["source_audit_sha256"] = audit_hash
            current["source_generated_utc"] = source["generated_utc"]
            desired = _initial_status(source)
            if from_status in {"AWAITING_APPROVAL", "READY"}:
                current["execution_status"] = desired
            event_type = ""
            if int(previous["protocol_revision"]) != int(source["protocol_revision"]):
                event_type = "PROTOCOL_REVISION_SYNCED"
            elif from_status != current["execution_status"]:
                event_type = "AUTHORITY_STATUS_SYNCED"
        current = _recalculate(current, generated_utc)
        rows.append(current)
        if event_type:
            events.append({
                "history_id": _history_id(current["ledger_id"], event_type, generated_utc, int(current["observations_collected"])),
                "ledger_id": current["ledger_id"], "registry_id": registry_id,
                "experiment_id": current["experiment_id"], "event_utc": generated_utc,
                "event_type": event_type, "from_execution_status": from_status,
                "to_execution_status": current["execution_status"],
                "observations_collected": int(current["observations_collected"]),
                "sole_veto_observations_collected": int(current["sole_veto_observations_collected"]),
                "protocol_revision": int(current["protocol_revision"]), "actor": "CS06",
                "event_reason": "Execution ledger synchronised with governed CS05 registry.",
                "source_registry_sha256": registry_hash, "engine_version": ENGINE_VERSION,
            })
    ledger = pd.DataFrame(rows, columns=LEDGER_COLUMNS).sort_values(
        ["ledger_rank", "ledger_id"]
    ).reset_index(drop=True)
    history = pd.DataFrame(events, columns=HISTORY_COLUMNS)
    return ledger, history


def validate_ledger(ledger: pd.DataFrame, registry: pd.DataFrame) -> ValidationResult:
    errors = _require_columns(ledger, LEDGER_COLUMNS, "execution ledger")
    if not errors:
        if len(ledger) != len(registry):
            errors.append("execution ledger row count must match registry row count")
        if set(ledger["registry_id"]) != set(registry["registry_id"]):
            errors.append("execution ledger must preserve all registry identities")
        if ledger["ledger_id"].duplicated().any():
            errors.append("ledger_id values must be unique")
        invalid = ledger["execution_status"].isin({"RUNNING", "PAUSED", "STOPPED", "COMPLETED"}) & ~ledger["execution_authorised"].map(_to_bool)
        if invalid.any():
            errors.append("unauthorised experiments cannot enter execution states")
    return ValidationResult(not errors, tuple(errors), ())


def build_readiness(ledger: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in ledger.iterrows():
        status = str(row["execution_status"])
        if status == "AWAITING_APPROVAL":
            klass, reason = "BLOCKED", "Awaiting CS05 review and explicit execution authority."
        elif status == "READY":
            missing = [c for c in ("strategy_version", "parameter_set_id", "dataset_id") if not _normalise(row[c])]
            if missing:
                klass, reason = "CONFIGURATION_REQUIRED", "Populate " + ", ".join(missing) + " before execution."
            else:
                klass, reason = "READY_TO_START", "Governance and execution metadata are complete."
        elif status == "RUNNING":
            klass, reason = "IN_PROGRESS", "Experiment is accumulating governed observations."
        elif status in TERMINAL_EXECUTION_STATUSES:
            klass, reason = "TERMINAL", "Experiment execution has reached a terminal state."
        else:
            klass, reason = "REVIEW_REQUIRED", "Execution state requires human review."
        rows.append({
            "ledger_id": row["ledger_id"], "registry_id": row["registry_id"],
            "experiment_id": row["experiment_id"], "experiment_title": row["experiment_title"],
            "primary_filter": row["primary_filter"], "execution_status": status,
            "readiness_class": klass, "readiness_reason": reason,
            "execution_authorised": row["execution_authorised"],
            "registry_status": row["registry_status"], "review_status": row["review_status"],
            "minimum_observations": row["minimum_observations"],
            "minimum_sole_veto_observations": row["minimum_sole_veto_observations"],
            "maximum_calendar_days": row["maximum_calendar_days"],
            "priority_score": row["priority_score"], "priority_band": row["priority_band"],
        })
    frame = pd.DataFrame(rows)
    order = {"READY_TO_START": 0, "CONFIGURATION_REQUIRED": 1, "IN_PROGRESS": 2, "REVIEW_REQUIRED": 3, "BLOCKED": 4, "TERMINAL": 5}
    frame["_order"] = frame["readiness_class"].map(order).fillna(99)
    frame = frame.sort_values(["_order", "priority_score", "experiment_id"], ascending=[True, False, True]).drop(columns=["_order"]).reset_index(drop=True)
    frame.insert(0, "readiness_rank", range(1, len(frame) + 1))
    return frame.loc[:, READINESS_COLUMNS]


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _atomic_write_csv(path: Path, frame: pd.DataFrame) -> None:
    _atomic_write_text(path, frame.to_csv(index=False))


def _report(ledger: pd.DataFrame, readiness: pd.DataFrame, validation: ValidationResult, generated_utc: str) -> str:
    lines = [
        "=" * 100, "BACQE CONVEX SURVIVAL CS06 - EXPERIMENT EXECUTION LEDGER", "=" * 100,
        f"Generated UTC:              {generated_utc}", f"Engine version:             {ENGINE_VERSION}",
        f"Source engine:              CS05", f"Ledger experiments:         {len(ledger)}",
        f"Validation:                 {'PASS' if validation.passed else 'FAIL'}", "",
        "INSTITUTIONAL PURPOSE", "-" * 100,
        "CS06 records what has actually occurred during governed experiment execution.",
        "It does not approve experiments, alter protocols, infer scientific outcomes or authorise production changes.", "",
        "EXECUTION STATUS COUNTS", "-" * 100,
    ]
    for k, v in ledger["execution_status"].value_counts().sort_index().items():
        lines.append(f"{k:<40} {v}")
    lines += ["", "READINESS COUNTS", "-" * 100]
    for k, v in readiness["readiness_class"].value_counts().sort_index().items():
        lines.append(f"{k:<40} {v}")
    lines += ["", "EXECUTION LEDGER", "-" * 100]
    for _, row in ledger.head(10).iterrows():
        lines += [
            f"{int(row['ledger_rank']):2d}. {row['experiment_title']}",
            f"    Ledger ID:              {row['ledger_id']}",
            f"    Execution status:       {row['execution_status']}",
            f"    Execution authorised:   {str(bool(row['execution_authorised'])).upper()}",
            f"    Observations:           {int(row['observations_collected']):,} / {int(row['minimum_observations']):,}",
            f"    Sole-veto observations: {int(row['sole_veto_observations_collected']):,} / {int(row['minimum_sole_veto_observations']):,}",
            f"    Evidence state:         {row['current_evidence_state']}", "",
        ]
    if validation.warnings:
        lines += ["VALIDATION WARNINGS", "-" * 100] + [f"- {w}" for w in validation.warnings] + [""]
    lines += [
        "GOVERNANCE INTERPRETATION", "-" * 100,
        "AWAITING_APPROVAL means CS05 has not granted explicit execution authority.",
        "READY means governance allows execution but required execution metadata may still be incomplete.",
        "A stopping-rule trigger records an operational condition; it does not determine the scientific conclusion.",
        "=" * 100,
    ]
    return "\n".join(lines) + "\n"


def run_execution_ledger(
    registry_path: Path,
    audit_path: Path,
    output_dir: Path,
    existing_ledger_path: Path | None = None,
    existing_history_path: Path | None = None,
) -> LedgerOutputs:
    registry_path, audit_path, output_dir = Path(registry_path), Path(audit_path), Path(output_dir)
    if not registry_path.exists() or not audit_path.exists():
        raise FileNotFoundError("CS06 source registry or audit does not exist")
    registry = pd.read_csv(registry_path)
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    validation = validate_inputs(registry, audit)
    if not validation.passed:
        raise ExecutionLedgerError("; ".join(validation.errors))
    output_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = output_dir / "experiment_execution_ledger_latest.csv"
    history_path = output_dir / "experiment_execution_history.csv"
    readiness_path = output_dir / "experiment_execution_readiness_latest.csv"
    report_path = output_dir / "experiment_execution_ledger_report_latest.txt"
    audit_out = output_dir / "experiment_execution_ledger_audit_latest.json"
    existing_ledger_path = existing_ledger_path or (ledger_path if ledger_path.exists() else None)
    existing_history_path = existing_history_path or (history_path if history_path.exists() else None)
    existing = pd.read_csv(existing_ledger_path) if existing_ledger_path and Path(existing_ledger_path).exists() else None
    old_history = pd.read_csv(existing_history_path) if existing_history_path and Path(existing_history_path).exists() else pd.DataFrame(columns=HISTORY_COLUMNS)
    if existing is not None:
        existing_validation = validate_existing_ledger(existing)
        if not existing_validation.passed:
            raise ExecutionLedgerError("; ".join(existing_validation.errors))
    generated_utc = datetime.now(timezone.utc).isoformat()
    registry_hash, audit_hash = _sha256(registry_path), _sha256(audit_path)
    ledger, new_events = merge_ledger(registry, existing, registry_hash, audit_hash, generated_utc)
    ledger_validation = validate_ledger(ledger, registry)
    if not ledger_validation.passed:
        raise ExecutionLedgerError("; ".join(ledger_validation.errors))
    history = pd.concat([old_history, new_events], ignore_index=True).drop_duplicates("history_id", keep="first") if not new_events.empty else old_history.copy()
    history = history.loc[:, HISTORY_COLUMNS]
    readiness = build_readiness(ledger)
    combined_validation = ValidationResult(True, (), validation.warnings)
    paths = OutputPaths(output_dir, ledger_path, history_path, readiness_path, report_path, audit_out)
    _atomic_write_csv(ledger_path, ledger)
    _atomic_write_csv(history_path, history)
    _atomic_write_csv(readiness_path, readiness)
    _atomic_write_text(report_path, _report(ledger, readiness, combined_validation, generated_utc))
    lineage = SourceLineage("CS05", audit.get("engine_name", "Experiment Registry Engine"), audit.get("engine_version", ""), audit.get("generated_utc", ""), registry_hash, audit_hash)
    payload = {
        "engine_id": ENGINE_ID, "engine_name": ENGINE_NAME, "engine_version": ENGINE_VERSION,
        "schema_version": SCHEMA_VERSION, "generated_utc": generated_utc,
        "ledger_count": len(ledger), "history_count": len(history),
        "new_history_event_count": len(new_events), "readiness_count": len(readiness),
        "execution_status_counts": ledger["execution_status"].value_counts().sort_index().to_dict(),
        "readiness_class_counts": readiness["readiness_class"].value_counts().sort_index().to_dict(),
        "policy": {
            "execution_requires_explicit_authorisation": True,
            "ledger_is_observational_not_approving": True,
            "history_is_append_only": True,
            "stopping_trigger_is_not_scientific_conclusion": True,
            "production_changes_authorised": False,
        },
        "source_lineage": asdict(lineage), "validation": asdict(combined_validation),
        "output_paths": {k: str(v) for k, v in asdict(paths).items()},
    }
    _atomic_write_text(audit_out, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return LedgerOutputs(ledger, history, readiness, combined_validation, lineage, paths)
