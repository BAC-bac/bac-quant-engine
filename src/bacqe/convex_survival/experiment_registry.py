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

ENGINE_ID = "CS05"
ENGINE_NAME = "Experiment Registry Engine"
ENGINE_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"
SOURCE_ENGINE_ID = "CS04"

REQUIRED_DESIGN_COLUMNS = (
    "design_rank", "experiment_id", "candidate_id", "experiment_title",
    "experiment_family", "primary_filter", "research_question",
    "null_hypothesis", "alternative_hypothesis", "objective", "design_status",
    "minimum_observations", "minimum_sole_veto_observations",
    "maximum_calendar_days", "stopping_rule", "success_criteria",
    "failure_criteria", "inconclusive_criteria", "risk_guardrails",
    "priority_score", "priority_band", "sample_adequacy_score",
    "confounding_risk_score", "principal_risk", "source_candidates_sha256",
    "source_audit_sha256", "generated_utc", "engine_version",
)

VALID_REGISTRY_STATUSES = {
    "DRAFT", "UNDER_REVIEW", "APPROVED", "RUNNING", "PAUSED",
    "COMPLETED", "REJECTED", "CANCELLED", "ARCHIVED",
}
VALID_REVIEW_STATUSES = {"PENDING", "IN_REVIEW", "APPROVED", "REJECTED"}
VALID_OUTCOMES = {"", "SUPPORTED", "REJECTED", "INCONCLUSIVE", "INVALIDATED"}
TERMINAL_STATUSES = {"COMPLETED", "REJECTED", "CANCELLED", "ARCHIVED"}

REGISTRY_COLUMNS = (
    "registry_rank", "registry_id", "experiment_id", "candidate_id",
    "experiment_title", "experiment_family", "primary_filter", "registry_status",
    "review_status", "approval_required", "execution_authorised", "owner",
    "reviewer", "registered_utc", "last_updated_utc", "approved_utc",
    "planned_start_utc", "actual_start_utc", "actual_end_utc", "outcome",
    "decision_summary", "linked_results_path", "protocol_revision",
    "protocol_fingerprint", "source_design_rank", "source_design_status",
    "minimum_observations", "minimum_sole_veto_observations",
    "maximum_calendar_days", "priority_score", "priority_band",
    "sample_adequacy_score", "confounding_risk_score", "principal_risk",
    "research_question", "null_hypothesis", "alternative_hypothesis",
    "objective", "stopping_rule", "success_criteria", "failure_criteria",
    "inconclusive_criteria", "risk_guardrails", "source_engine_id",
    "source_engine_version", "source_designs_sha256", "source_audit_sha256",
    "source_generated_utc", "generated_utc", "engine_version",
)

HISTORY_COLUMNS = (
    "history_id", "registry_id", "experiment_id", "event_utc", "event_type",
    "from_status", "to_status", "from_review_status", "to_review_status",
    "protocol_revision", "protocol_fingerprint", "actor", "event_reason",
    "source_designs_sha256", "engine_version",
)

MUTABLE_COLUMNS = (
    "registry_status", "review_status", "execution_authorised", "owner", "reviewer",
    "approved_utc", "planned_start_utc", "actual_start_utc", "actual_end_utc",
    "outcome", "decision_summary", "linked_results_path",
)


class ExperimentRegistryError(ValueError):
    """Raised when CS05 cannot produce a valid governed experiment registry."""


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
    designs_sha256: str
    audit_sha256: str


@dataclass(frozen=True)
class OutputPaths:
    output_directory: Path
    registry_csv: Path
    history_csv: Path
    report_txt: Path
    audit_json: Path


@dataclass(frozen=True)
class RegistryOutputs:
    registry: pd.DataFrame
    history: pd.DataFrame
    validation: ValidationResult
    source_lineage: SourceLineage
    paths: OutputPaths


def _require_columns(frame: pd.DataFrame, required: Iterable[str], label: str) -> list[str]:
    return [f"{label} missing required column: {column}" for column in required if column not in frame.columns]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_registry_id(experiment_id: str) -> str:
    token = hashlib.sha256(f"{experiment_id}|CS05".encode("utf-8")).hexdigest()[:12].upper()
    return f"CS05-REG-{token}"


def _history_id(registry_id: str, event_type: str, event_utc: str, revision: int) -> str:
    token = hashlib.sha256(
        f"{registry_id}|{event_type}|{event_utc}|{revision}".encode("utf-8")
    ).hexdigest()[:16].upper()
    return f"CS05-HIST-{token}"


def _normalise(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def protocol_fingerprint(row: pd.Series) -> str:
    fields = (
        "experiment_id", "candidate_id", "experiment_title", "experiment_family",
        "primary_filter", "research_question", "null_hypothesis",
        "alternative_hypothesis", "objective", "design_status",
        "minimum_observations", "minimum_sole_veto_observations",
        "maximum_calendar_days", "stopping_rule", "success_criteria",
        "failure_criteria", "inconclusive_criteria", "risk_guardrails",
    )
    canonical = "|".join(f"{field}={_normalise(row.get(field))}" for field in fields)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_inputs(designs: pd.DataFrame, audit: dict[str, Any]) -> ValidationResult:
    errors = _require_columns(designs, REQUIRED_DESIGN_COLUMNS, "experiment designs")
    warnings: list[str] = []
    if designs.empty:
        errors.append("experiment designs contain no rows")
    if not errors:
        if designs["experiment_id"].duplicated().any():
            errors.append("experiment_id values must be unique")
        if designs["candidate_id"].duplicated().any():
            errors.append("candidate_id values must be unique")
        if not designs["design_rank"].is_unique:
            errors.append("design_rank values must be unique")
        if set(designs["design_status"].astype(str)) != {"DRAFT_PROTOCOL"}:
            errors.append("CS05 v1.0.0 accepts only CS04 DRAFT_PROTOCOL rows")
        for column in (
            "minimum_observations", "maximum_calendar_days", "priority_score",
            "sample_adequacy_score", "confounding_risk_score",
        ):
            values = pd.to_numeric(designs[column], errors="coerce")
            if values.isna().any():
                errors.append(f"{column} must be numeric")
        for column in ("priority_score", "sample_adequacy_score", "confounding_risk_score"):
            values = pd.to_numeric(designs[column], errors="coerce")
            if ((values < 0) | (values > 1)).any():
                errors.append(f"{column} must contain values in [0, 1]")
        if (pd.to_numeric(designs["minimum_observations"], errors="coerce") <= 0).any():
            errors.append("minimum_observations must be positive")
    if audit.get("engine_id") != SOURCE_ENGINE_ID:
        errors.append("source audit must identify engine_id CS04")
    if not audit.get("validation", {}).get("passed", False):
        errors.append("source CS04 audit validation did not pass")
    policy = audit.get("policy", {})
    if policy.get("production_changes_authorised") is not False:
        errors.append("CS04 audit must explicitly state production_changes_authorised=false")
    if (designs.get("priority_band", pd.Series(dtype=str)).astype(str) == "REPLICATION_REQUIRED").any():
        warnings.append("Replication-required protocols remain unapproved and must collect the specified evidence before conclusion.")
    return ValidationResult(not errors, tuple(errors), tuple(warnings))


def validate_existing_registry(registry: pd.DataFrame) -> ValidationResult:
    if registry.empty:
        return ValidationResult(True, (), ())
    errors = _require_columns(registry, REGISTRY_COLUMNS, "existing registry")
    warnings: list[str] = []
    if not errors:
        if registry["registry_id"].duplicated().any():
            errors.append("existing registry_id values must be unique")
        if registry["experiment_id"].duplicated().any():
            errors.append("existing experiment_id values must be unique")
        unknown_statuses = sorted(set(registry["registry_status"].astype(str)) - VALID_REGISTRY_STATUSES)
        if unknown_statuses:
            errors.append(f"existing registry contains invalid statuses: {', '.join(unknown_statuses)}")
        unknown_reviews = sorted(set(registry["review_status"].astype(str)) - VALID_REVIEW_STATUSES)
        if unknown_reviews:
            errors.append(f"existing registry contains invalid review statuses: {', '.join(unknown_reviews)}")
        unknown_outcomes = sorted(set(registry["outcome"].fillna("").astype(str)) - VALID_OUTCOMES)
        if unknown_outcomes:
            errors.append(f"existing registry contains invalid outcomes: {', '.join(unknown_outcomes)}")
        authorised = registry["execution_authorised"].map(_to_bool)
        invalid_auth = authorised & ~registry["registry_status"].isin({"APPROVED", "RUNNING", "PAUSED", "COMPLETED"})
        if invalid_auth.any():
            errors.append("execution_authorised may only be true for approved or active experiments")
        unauthorised_active = registry["registry_status"].isin({"RUNNING", "PAUSED", "COMPLETED"}) & ~authorised
        if unauthorised_active.any():
            errors.append("running, paused or completed experiments must be execution authorised")
    return ValidationResult(not errors, tuple(errors), tuple(warnings))


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _new_registry_row(design: pd.Series, designs_hash: str, audit_hash: str, generated_utc: str) -> dict[str, Any]:
    fingerprint = protocol_fingerprint(design)
    return {
        "registry_rank": int(design["design_rank"]),
        "registry_id": _stable_registry_id(str(design["experiment_id"])),
        "experiment_id": design["experiment_id"],
        "candidate_id": design["candidate_id"],
        "experiment_title": design["experiment_title"],
        "experiment_family": design["experiment_family"],
        "primary_filter": design["primary_filter"],
        "registry_status": "DRAFT",
        "review_status": "PENDING",
        "approval_required": True,
        "execution_authorised": False,
        "owner": "",
        "reviewer": "",
        "registered_utc": generated_utc,
        "last_updated_utc": generated_utc,
        "approved_utc": "",
        "planned_start_utc": "",
        "actual_start_utc": "",
        "actual_end_utc": "",
        "outcome": "",
        "decision_summary": "",
        "linked_results_path": "",
        "protocol_revision": 1,
        "protocol_fingerprint": fingerprint,
        "source_design_rank": int(design["design_rank"]),
        "source_design_status": design["design_status"],
        "minimum_observations": int(design["minimum_observations"]),
        "minimum_sole_veto_observations": int(design["minimum_sole_veto_observations"]),
        "maximum_calendar_days": int(design["maximum_calendar_days"]),
        "priority_score": round(float(design["priority_score"]), 6),
        "priority_band": design["priority_band"],
        "sample_adequacy_score": round(float(design["sample_adequacy_score"]), 6),
        "confounding_risk_score": round(float(design["confounding_risk_score"]), 6),
        "principal_risk": design["principal_risk"],
        "research_question": design["research_question"],
        "null_hypothesis": design["null_hypothesis"],
        "alternative_hypothesis": design["alternative_hypothesis"],
        "objective": design["objective"],
        "stopping_rule": design["stopping_rule"],
        "success_criteria": design["success_criteria"],
        "failure_criteria": design["failure_criteria"],
        "inconclusive_criteria": design["inconclusive_criteria"],
        "risk_guardrails": design["risk_guardrails"],
        "source_engine_id": SOURCE_ENGINE_ID,
        "source_engine_version": design["engine_version"],
        "source_designs_sha256": designs_hash,
        "source_audit_sha256": audit_hash,
        "source_generated_utc": design["generated_utc"],
        "generated_utc": generated_utc,
        "engine_version": ENGINE_VERSION,
    }


def _history_event(row: dict[str, Any], event_type: str, event_utc: str,
                   from_status: str = "", from_review_status: str = "",
                   actor: str = "CS05", reason: str = "") -> dict[str, Any]:
    revision = int(row["protocol_revision"])
    return {
        "history_id": _history_id(row["registry_id"], event_type, event_utc, revision),
        "registry_id": row["registry_id"],
        "experiment_id": row["experiment_id"],
        "event_utc": event_utc,
        "event_type": event_type,
        "from_status": from_status,
        "to_status": row["registry_status"],
        "from_review_status": from_review_status,
        "to_review_status": row["review_status"],
        "protocol_revision": revision,
        "protocol_fingerprint": row["protocol_fingerprint"],
        "actor": actor,
        "event_reason": reason,
        "source_designs_sha256": row["source_designs_sha256"],
        "engine_version": ENGINE_VERSION,
    }


def merge_registry(designs: pd.DataFrame, existing_registry: pd.DataFrame | None,
                   designs_hash: str, audit_hash: str,
                   generated_utc: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    generated_utc = generated_utc or datetime.now(timezone.utc).isoformat()
    existing_registry = existing_registry if existing_registry is not None else pd.DataFrame(columns=REGISTRY_COLUMNS)
    existing_by_experiment = {
        str(row["experiment_id"]): row for _, row in existing_registry.iterrows()
    }
    registry_rows: list[dict[str, Any]] = []
    history_rows: list[dict[str, Any]] = []

    for _, design in designs.sort_values(["design_rank", "experiment_id"], kind="mergesort").iterrows():
        experiment_id = str(design["experiment_id"])
        fresh = _new_registry_row(design, designs_hash, audit_hash, generated_utc)
        prior = existing_by_experiment.get(experiment_id)
        if prior is None:
            registry_rows.append(fresh)
            history_rows.append(_history_event(fresh, "REGISTERED", generated_utc, reason="CS04 protocol registered as governed draft."))
            continue

        row = fresh.copy()
        for column in MUTABLE_COLUMNS:
            row[column] = prior.get(column, row[column])
        row["registry_id"] = prior["registry_id"]
        row["registered_utc"] = prior["registered_utc"]
        previous_fingerprint = _normalise(prior.get("protocol_fingerprint"))
        if previous_fingerprint != fresh["protocol_fingerprint"]:
            row["protocol_revision"] = int(prior.get("protocol_revision", 1)) + 1
            row["last_updated_utc"] = generated_utc
            registry_rows.append(row)
            history_rows.append(_history_event(
                row, "PROTOCOL_REVISED", generated_utc,
                from_status=_normalise(prior.get("registry_status")),
                from_review_status=_normalise(prior.get("review_status")),
                reason="Source CS04 protocol fingerprint changed; governed fields were preserved.",
            ))
        else:
            row["protocol_revision"] = int(prior.get("protocol_revision", 1))
            row["protocol_fingerprint"] = previous_fingerprint
            row["last_updated_utc"] = prior.get("last_updated_utc", generated_utc)
            registry_rows.append(row)

    registry = pd.DataFrame(registry_rows, columns=REGISTRY_COLUMNS)
    history = pd.DataFrame(history_rows, columns=HISTORY_COLUMNS)
    return registry, history


def validate_registry(registry: pd.DataFrame, designs: pd.DataFrame) -> ValidationResult:
    errors = _require_columns(registry, REGISTRY_COLUMNS, "registry")
    warnings: list[str] = []
    if registry.empty:
        errors.append("registry contains no rows")
    if not errors:
        if registry["registry_id"].duplicated().any():
            errors.append("registry_id values must be unique")
        if registry["experiment_id"].duplicated().any():
            errors.append("experiment_id values must be unique")
        if set(registry["experiment_id"].astype(str)) != set(designs["experiment_id"].astype(str)):
            errors.append("registry must preserve one-to-one experiment identity")
        if set(registry["candidate_id"].astype(str)) != set(designs["candidate_id"].astype(str)):
            errors.append("registry must preserve candidate identity")
        if (~registry["registry_status"].isin(VALID_REGISTRY_STATUSES)).any():
            errors.append("registry contains invalid lifecycle status")
        if (~registry["review_status"].isin(VALID_REVIEW_STATUSES)).any():
            errors.append("registry contains invalid review status")
        if registry["protocol_revision"].astype(int).lt(1).any():
            errors.append("protocol_revision must be at least 1")
        if registry["protocol_fingerprint"].fillna("").str.len().ne(64).any():
            errors.append("protocol_fingerprint must be a SHA256 value")
        unauthorised_running = registry["registry_status"].isin({"RUNNING", "PAUSED", "COMPLETED"}) & ~registry["execution_authorised"].map(_to_bool)
        if unauthorised_running.any():
            errors.append("running, paused or completed experiments must be execution authorised")
        completed_without_outcome = registry["registry_status"].eq("COMPLETED") & registry["outcome"].fillna("").eq("")
        if completed_without_outcome.any():
            errors.append("completed experiments must record an outcome")
        if registry["registry_status"].eq("DRAFT").all():
            warnings.append("All experiments remain DRAFT; no protocol has yet passed human review or received execution authority.")
    return ValidationResult(not errors, tuple(errors), tuple(warnings))


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def _atomic_csv(path: Path, frame: pd.DataFrame) -> None:
    _atomic_text(path, frame.to_csv(index=False, lineterminator="\n"))


def render_report(registry: pd.DataFrame, history: pd.DataFrame,
                  validation: ValidationResult, lineage: SourceLineage,
                  generated_utc: str) -> str:
    lines = [
        "=" * 100,
        "BACQE CONVEX SURVIVAL CS05 - EXPERIMENT REGISTRY ENGINE",
        "=" * 100,
        f"Generated UTC:              {generated_utc}",
        f"Engine version:             {ENGINE_VERSION}",
        f"Source engine:              {lineage.engine_id} v{lineage.engine_version}",
        f"Registered experiments:     {len(registry)}",
        f"New history events:         {len(history)}",
        f"Validation:                 {'PASS' if validation.passed else 'FAIL'}",
        "",
        "INSTITUTIONAL PURPOSE",
        "-" * 100,
        "CS05 establishes the governed lifecycle record for every CS04 experiment protocol.",
        "Registration does not constitute scientific approval, execution authority or permission to change production rules.",
        "",
        "REGISTRY STATUS COUNTS",
        "-" * 100,
    ]
    for name, count in registry["registry_status"].value_counts().sort_index().items():
        lines.append(f"{name:<40} {count}")
    lines += ["", "REVIEW STATUS COUNTS", "-" * 100]
    for name, count in registry["review_status"].value_counts().sort_index().items():
        lines.append(f"{name:<40} {count}")
    lines += ["", "REGISTERED EXPERIMENTS", "-" * 100]
    for _, row in registry.sort_values(["registry_rank", "registry_id"]).head(10).iterrows():
        lines += [
            f"{int(row['registry_rank']):2d}. {row['experiment_title']}",
            f"    Registry ID:            {row['registry_id']}",
            f"    Experiment ID:          {row['experiment_id']}",
            f"    Registry status:        {row['registry_status']}",
            f"    Review status:          {row['review_status']}",
            f"    Execution authorised:   {str(_to_bool(row['execution_authorised'])).upper()}",
            f"    Protocol revision:      {int(row['protocol_revision'])}",
            f"    Minimum observations:   {int(row['minimum_observations']):,}",
            f"    Principal risk:         {row['principal_risk']}",
            "",
        ]
    if validation.warnings:
        lines += ["VALIDATION WARNINGS", "-" * 100, *[f"- {warning}" for warning in validation.warnings], ""]
    lines += [
        "GOVERNANCE INTERPRETATION", "-" * 100,
        "DRAFT means the protocol is registered but has not completed review.",
        "Execution requires an explicit approved lifecycle state and execution_authorised=true.",
        "Protocol revisions preserve the registry identity and human-managed governance fields while recording a new history event.",
        "=" * 100,
    ]
    return "\n".join(lines) + "\n"


def run_experiment_registry(designs_csv: Path, source_audit_json: Path,
                            output_directory: Path,
                            existing_registry_csv: Path | None = None,
                            existing_history_csv: Path | None = None) -> RegistryOutputs:
    designs_csv = Path(designs_csv)
    source_audit_json = Path(source_audit_json)
    output_directory = Path(output_directory)
    designs = pd.read_csv(designs_csv)
    audit = json.loads(source_audit_json.read_text(encoding="utf-8"))
    input_validation = validate_inputs(designs, audit)
    if not input_validation.passed:
        raise ExperimentRegistryError("; ".join(input_validation.errors))

    registry_path = Path(existing_registry_csv) if existing_registry_csv else output_directory / "experiment_registry_latest.csv"
    history_path = Path(existing_history_csv) if existing_history_csv else output_directory / "experiment_registry_history.csv"
    existing_registry = pd.read_csv(registry_path) if registry_path.exists() else pd.DataFrame(columns=REGISTRY_COLUMNS)
    existing_history = pd.read_csv(history_path) if history_path.exists() else pd.DataFrame(columns=HISTORY_COLUMNS)
    existing_validation = validate_existing_registry(existing_registry)
    if not existing_validation.passed:
        raise ExperimentRegistryError("; ".join(existing_validation.errors))

    generated_utc = datetime.now(timezone.utc).isoformat()
    designs_hash, audit_hash = _sha256(designs_csv), _sha256(source_audit_json)
    registry, new_history = merge_registry(designs, existing_registry, designs_hash, audit_hash, generated_utc)
    output_validation = validate_registry(registry, designs)
    if not output_validation.passed:
        raise ExperimentRegistryError("; ".join(output_validation.errors))

    history = pd.concat([existing_history, new_history], ignore_index=True)
    if not history.empty:
        history = history.drop_duplicates(subset=["history_id"], keep="first")
        history = history.sort_values(["event_utc", "history_id"], kind="mergesort").reset_index(drop=True)
    validation = ValidationResult(
        True, (), tuple(dict.fromkeys(input_validation.warnings + existing_validation.warnings + output_validation.warnings))
    )
    lineage = SourceLineage(
        engine_id=audit["engine_id"], engine_name=audit["engine_name"],
        engine_version=audit["engine_version"], generated_utc=audit["generated_utc"],
        designs_sha256=designs_hash, audit_sha256=audit_hash,
    )
    paths = OutputPaths(
        output_directory=output_directory,
        registry_csv=output_directory / "experiment_registry_latest.csv",
        history_csv=output_directory / "experiment_registry_history.csv",
        report_txt=output_directory / "experiment_registry_report_latest.txt",
        audit_json=output_directory / "experiment_registry_audit_latest.json",
    )
    _atomic_csv(paths.registry_csv, registry)
    _atomic_csv(paths.history_csv, history)
    _atomic_text(paths.report_txt, render_report(registry, new_history, validation, lineage, generated_utc))
    audit_output = {
        "engine_id": ENGINE_ID,
        "engine_name": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "generated_utc": generated_utc,
        "registry_count": int(len(registry)),
        "history_count": int(len(history)),
        "new_history_event_count": int(len(new_history)),
        "registry_status_counts": registry["registry_status"].value_counts().sort_index().to_dict(),
        "review_status_counts": registry["review_status"].value_counts().sort_index().to_dict(),
        "protocol_revision_counts": registry["protocol_revision"].astype(int).value_counts().sort_index().to_dict(),
        "source_lineage": asdict(lineage),
        "validation": asdict(validation),
        "policy": {
            "registration_implies_approval": False,
            "execution_requires_explicit_authorisation": True,
            "production_changes_authorised": False,
            "human_governance_fields_preserved_on_rerun": True,
            "history_is_append_only": True,
        },
        "output_paths": {key: str(value) for key, value in asdict(paths).items()},
    }
    _atomic_text(paths.audit_json, json.dumps(audit_output, indent=2, sort_keys=True) + "\n")
    return RegistryOutputs(registry, history, validation, lineage, paths)
