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

ENGINE_ID = "CS08"
ENGINE_NAME = "Institutional Research Governance Dashboard"
ENGINE_VERSION = "1.0.1"
SCHEMA_VERSION = "1.0.0"

SOURCE_ENGINES = ("CS03", "CS04", "CS05", "CS06", "CS07")

REQUIRED = {
    "CS03": ("candidate_id", "priority_rank", "priority_score", "priority_band", "candidate_type", "primary_filter"),
    "CS04": ("experiment_id", "candidate_id", "design_status", "experiment_family", "minimum_observations", "maximum_calendar_days"),
    "CS05": ("registry_id", "experiment_id", "registry_status", "review_status", "execution_authorised", "protocol_revision"),
    "CS06": ("ledger_id", "registry_id", "experiment_id", "execution_status", "execution_authorised", "observations_collected", "evidence_target_reached", "guardrail_breach", "data_quality_passed"),
    "CS07": ("evidence_id", "ledger_id", "experiment_id", "evidence_state", "scientific_recommendation", "review_priority", "review_reason", "combined_progress", "replication_required", "final_conclusion_authorised", "production_changes_authorised"),
}

DASHBOARD_COLUMNS = (
    "dashboard_rank", "governance_id", "candidate_id", "experiment_id", "registry_id", "ledger_id", "evidence_id",
    "experiment_title", "experiment_family", "primary_filter", "priority_score", "priority_band", "design_status",
    "registry_status", "review_status", "execution_authorised", "execution_status", "observations_collected",
    "combined_progress", "evidence_state", "scientific_recommendation", "review_priority", "review_reason",
    "replication_required", "evidence_target_reached", "guardrail_breach", "data_quality_passed", "protocol_revision",
    "pipeline_stage", "attention_required", "attention_reason", "production_ready", "generated_utc", "engine_version",
)

HEALTH_COLUMNS = (
    "health_rank", "health_dimension", "score", "status", "weight", "weighted_score", "numerator", "denominator",
    "interpretation", "generated_utc", "engine_version",
)

REVIEW_COLUMNS = (
    "review_rank", "governance_id", "experiment_id", "experiment_title", "primary_filter", "pipeline_stage",
    "review_priority", "attention_reason", "scientific_recommendation", "evidence_state", "execution_status",
    "combined_progress", "protocol_revision",
)

READINESS_COLUMNS = (
    "readiness_id", "overall_readiness_score", "overall_readiness_status", "institutional_health_score",
    "designed_experiments", "registered_experiments", "authorised_experiments", "running_experiments",
    "completed_experiments", "assessable_evidence", "ready_for_review", "ready_for_replication",
    "replication_complete", "critical_actions", "high_actions", "production_ready_experiments",
    "next_institutional_action", "production_changes_authorised", "generated_utc", "engine_version",
)


class InstitutionalGovernanceError(ValueError):
    pass


@dataclass(frozen=True)
class ValidationResult:
    passed: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class OutputPaths:
    output_directory: Path
    dashboard_csv: Path
    health_csv: Path
    review_queue_csv: Path
    readiness_csv: Path
    report_txt: Path
    audit_json: Path


@dataclass(frozen=True)
class GovernanceOutputs:
    dashboard: pd.DataFrame
    health: pd.DataFrame
    review_queue: pd.DataFrame
    readiness: pd.DataFrame
    validation: ValidationResult
    paths: OutputPaths


def _required(frame: pd.DataFrame, columns: Iterable[str], label: str) -> list[str]:
    return [f"{label} missing required column: {column}" for column in columns if column not in frame.columns]


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return default if pd.isna(number) else number
    except (TypeError, ValueError):
        return default


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _governance_id(experiment_id: str) -> str:
    token = hashlib.sha256(f"{experiment_id}|CS08".encode()).hexdigest()[:12].upper()
    return f"CS08-GOV-{token}"


def validate_sources(frames: dict[str, pd.DataFrame], audits: dict[str, dict[str, Any]]) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    for engine in SOURCE_ENGINES:
        frame = frames.get(engine)
        audit = audits.get(engine)
        if frame is None:
            errors.append(f"missing source frame: {engine}")
            continue
        errors.extend(_required(frame, REQUIRED[engine], engine))
        if frame.empty:
            errors.append(f"{engine} source contains no rows")
        if audit is None:
            errors.append(f"missing source audit: {engine}")
            continue
        if audit.get("engine_id") != engine:
            errors.append(f"{engine} audit identifies {audit.get('engine_id')!r}")
        if not audit.get("validation", {}).get("passed", False):
            errors.append(f"{engine} source audit validation did not pass")
        if audit.get("policy", {}).get("production_changes_authorised") is True:
            errors.append(f"{engine} improperly authorises production changes")

    if not errors:
        identities = {
            "CS03": set(frames["CS03"].candidate_id.astype(str)),
            "CS04": set(frames["CS04"].candidate_id.astype(str)),
            "CS05": set(frames["CS05"].experiment_id.astype(str)),
            "CS06": set(frames["CS06"].experiment_id.astype(str)),
            "CS07": set(frames["CS07"].experiment_id.astype(str)),
        }
        if identities["CS03"] != identities["CS04"]:
            errors.append("CS03 and CS04 candidate identities do not match")
        if not (identities["CS05"] == identities["CS06"] == identities["CS07"]):
            errors.append("CS05, CS06 and CS07 experiment identities do not match")
        if frames["CS07"].final_conclusion_authorised.map(_bool).any():
            errors.append("CS07 contains an unauthorised final scientific conclusion")
        if frames["CS07"].production_changes_authorised.map(_bool).any():
            errors.append("CS07 contains unauthorised production authority")
        if not frames["CS05"].execution_authorised.map(_bool).any():
            warnings.append("No experiment has execution authority; institutional readiness will remain governance-limited.")
        if set(frames["CS07"].evidence_state.astype(str)) == {"NO_EVIDENCE"}:
            warnings.append("No governed evidence is currently assessable.")
    return ValidationResult(not errors, tuple(errors), tuple(warnings))


def _pipeline_stage(row: pd.Series) -> str:
    if _bool(row.get("guardrail_breach")) or not _bool(row.get("data_quality_passed", True)):
        return "INVALIDATED_REVIEW"
    if str(row.get("evidence_state")) not in {"NO_EVIDENCE", "INSUFFICIENT"}:
        return "EVIDENCE_ASSESSMENT"
    if str(row.get("execution_status")) in {"RUNNING", "PAUSED", "STOPPED", "COMPLETED"}:
        return "EXECUTION"
    if _bool(row.get("execution_authorised")):
        return "READY_FOR_EXECUTION"
    if str(row.get("registry_status")) in {"APPROVED", "ACTIVE"}:
        return "GOVERNED_PROTOCOL"
    return "AWAITING_GOVERNANCE"


def _attention(row: pd.Series) -> tuple[bool, str]:
    priority = str(row.get("review_priority", "LOW"))
    recommendation = str(row.get("scientific_recommendation", ""))
    if _bool(row.get("guardrail_breach")) or not _bool(row.get("data_quality_passed", True)):
        return True, "Immediate human review required for guardrail or data-quality failure."
    if priority in {"CRITICAL", "HIGH"}:
        return True, str(row.get("review_reason", "High-priority scientific review required."))
    if recommendation == "AWAIT_APPROVAL":
        return True, "Human governance decision required before execution can begin."
    if recommendation == "REQUIRES_EXECUTION":
        return True, "Authorised protocol requires execution configuration and launch."
    return False, str(row.get("review_reason", "Continue governed monitoring."))


def build_dashboard(frames: dict[str, pd.DataFrame], generated_utc: str) -> pd.DataFrame:
    c = frames["CS03"].copy()
    d = frames["CS04"].copy()
    r = frames["CS05"].copy()
    l = frames["CS06"].copy()
    e = frames["CS07"].copy()

    merged = c.merge(d, on="candidate_id", how="inner", suffixes=("_candidate", "_design"))
    merged = merged.merge(r, on="experiment_id", how="inner", suffixes=("", "_registry"))
    merged = merged.merge(l, on=["experiment_id", "registry_id"], how="inner", suffixes=("", "_ledger"))
    merged = merged.merge(e, on=["experiment_id", "registry_id", "ledger_id"], how="inner", suffixes=("", "_evidence"))

    rows: list[dict[str, Any]] = []
    for _, source in merged.iterrows():
        exp_id = str(source["experiment_id"])
        stage = _pipeline_stage(source)
        attention, reason = _attention(source)
        production_ready = False
        rows.append({
            "dashboard_rank": int(_num(source.get("priority_rank"), len(rows) + 1)),
            "governance_id": _governance_id(exp_id),
            "candidate_id": source["candidate_id"],
            "experiment_id": exp_id,
            "registry_id": source["registry_id"],
            "ledger_id": source["ledger_id"],
            "evidence_id": source["evidence_id"],
            "experiment_title": source.get("experiment_title", source.get("candidate_title", exp_id)),
            "experiment_family": source.get("experiment_family", source.get("candidate_type", "UNKNOWN")),
            "primary_filter": source.get("primary_filter", source.get("primary_filter_candidate", "")),
            "priority_score": round(_num(source.get("priority_score_candidate", source.get("priority_score"))), 8),
            "priority_band": source.get("priority_band_candidate", source.get("priority_band", "")),
            "design_status": source.get("design_status", ""),
            "registry_status": source.get("registry_status", ""),
            "review_status": source.get("review_status", ""),
            "execution_authorised": _bool(source.get("execution_authorised_evidence", source.get("execution_authorised_ledger", source.get("execution_authorised")))),
            "execution_status": source.get("execution_status_evidence", source.get("execution_status", "")),
            "observations_collected": int(_num(source.get("observations_collected_evidence", source.get("observations_collected")))),
            "combined_progress": round(_num(source.get("combined_progress")), 6),
            "evidence_state": source.get("evidence_state", ""),
            "scientific_recommendation": source.get("scientific_recommendation", ""),
            "review_priority": source.get("review_priority", "LOW"),
            "review_reason": source.get("review_reason", ""),
            "replication_required": _bool(source.get("replication_required")),
            "evidence_target_reached": _bool(source.get("evidence_target_reached_evidence", source.get("evidence_target_reached"))),
            "guardrail_breach": _bool(source.get("guardrail_breach_evidence", source.get("guardrail_breach"))),
            "data_quality_passed": _bool(source.get("data_quality_passed_evidence", source.get("data_quality_passed", True))),
            "protocol_revision": int(_num(source.get("protocol_revision_evidence", source.get("protocol_revision", 1)), 1)),
            "pipeline_stage": stage,
            "attention_required": attention,
            "attention_reason": reason,
            "production_ready": production_ready,
            "generated_utc": generated_utc,
            "engine_version": ENGINE_VERSION,
        })
    dashboard = pd.DataFrame(rows).sort_values(["dashboard_rank", "experiment_id"]).reset_index(drop=True)
    dashboard["dashboard_rank"] = range(1, len(dashboard) + 1)
    return dashboard.loc[:, DASHBOARD_COLUMNS]


def _status(score: float) -> str:
    if score >= 90:
        return "GREEN"
    if score >= 70:
        return "AMBER"
    if score >= 40:
        return "RED"
    return "FOUNDATIONAL"


def build_health(dashboard: pd.DataFrame, frames: dict[str, pd.DataFrame], generated_utc: str) -> pd.DataFrame:
    n = max(1, len(dashboard))
    dimensions = [
        ("SCIENTIFIC_INTEGRITY", 100.0 if not dashboard.production_ready.map(_bool).any() else 0.0, 0.20, n, n, "No engine has converted evidence into an automatic scientific conclusion."),
        ("GOVERNANCE_INTEGRITY", 100.0 * dashboard.registry_id.notna().sum() / n, 0.20, int(dashboard.registry_id.notna().sum()), n, "All designed experiments remain traceable through the governed registry."),
        ("EXECUTION_INTEGRITY", 100.0 * ((~dashboard.execution_authorised.map(_bool)) | dashboard.execution_status.ne("AWAITING_APPROVAL")).sum() / n, 0.15, int(((~dashboard.execution_authorised.map(_bool)) | dashboard.execution_status.ne("AWAITING_APPROVAL")).sum()), n, "Execution states are consistent with explicit authority."),
        ("EVIDENCE_INTEGRITY", 100.0 * dashboard.data_quality_passed.map(_bool).sum() / n, 0.20, int(dashboard.data_quality_passed.map(_bool).sum()), n, "Evidence records pass current data-quality controls."),
        ("REPLICATION_INTEGRITY", 100.0 * ((~dashboard.evidence_target_reached.map(_bool)) | dashboard.replication_required.map(_bool)).sum() / n, 0.15, int(((~dashboard.evidence_target_reached.map(_bool)) | dashboard.replication_required.map(_bool)).sum()), n, "Evidence cannot bypass required replication."),
        ("OPERATIONAL_READINESS", 100.0 * dashboard.combined_progress.mean(), 0.10, int((dashboard.combined_progress > 0).sum()), n, "Measures governed progress, not profitability or live-trading permission."),
    ]
    rows = []
    for rank, (name, score, weight, numerator, denominator, interpretation) in enumerate(dimensions, 1):
        score = round(float(score), 2)
        rows.append({
            "health_rank": rank, "health_dimension": name, "score": score, "status": _status(score), "weight": weight,
            "weighted_score": round(score * weight, 2), "numerator": numerator, "denominator": denominator,
            "interpretation": interpretation, "generated_utc": generated_utc, "engine_version": ENGINE_VERSION,
        })
    return pd.DataFrame(rows).loc[:, HEALTH_COLUMNS]


def build_review_queue(dashboard: pd.DataFrame) -> pd.DataFrame:
    queue = dashboard[dashboard.attention_required.map(_bool)].copy()
    order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    queue["_order"] = queue.review_priority.map(order).fillna(9)
    queue = queue.sort_values(["_order", "dashboard_rank"]).drop(columns="_order").reset_index(drop=True)
    queue.insert(0, "review_rank", range(1, len(queue) + 1))
    return queue.rename(columns={"attention_reason": "attention_reason"}).loc[:, REVIEW_COLUMNS]


def build_readiness(dashboard: pd.DataFrame, health: pd.DataFrame, generated_utc: str) -> pd.DataFrame:
    health_score = round(float(health.weighted_score.sum()), 2)
    assessable = int(dashboard.evidence_state.ne("NO_EVIDENCE").sum())
    ready_review = int(dashboard.scientific_recommendation.eq("READY_FOR_SCIENTIFIC_REVIEW").sum())
    ready_replication = int(dashboard.scientific_recommendation.eq("READY_FOR_REPLICATION").sum())
    replication_complete = ready_review
    governance_progress = 100.0 * dashboard.execution_authorised.map(_bool).mean()
    evidence_progress = 100.0 * dashboard.combined_progress.mean()
    replication_progress = 100.0 * replication_complete / max(1, len(dashboard))
    readiness_score = round(0.35 * governance_progress + 0.35 * evidence_progress + 0.30 * replication_progress, 2)
    status = "PRODUCTION_REVIEW_READY" if readiness_score >= 90 else "SCIENTIFIC_REVIEW" if readiness_score >= 70 else "EXECUTION_ACTIVE" if readiness_score >= 30 else "FOUNDATIONAL"
    critical = int(dashboard.review_priority.eq("CRITICAL").sum())
    high = int(dashboard.review_priority.eq("HIGH").sum())
    if not dashboard.execution_authorised.map(_bool).any():
        next_action = "Conduct human governance review and explicitly approve selected CS05 protocols before execution."
    elif dashboard.execution_status.eq("READY").any():
        next_action = "Configure and launch the highest-priority authorised experiment under CS06 controls."
    elif dashboard.execution_status.eq("RUNNING").any():
        next_action = "Continue governed evidence collection and address any CS07 review triggers."
    elif ready_replication:
        next_action = "Design and govern independent replication before any production consideration."
    elif ready_review:
        next_action = "Convene human scientific review; CS08 does not make the final conclusion."
    else:
        next_action = "Continue the governed research lifecycle without changing production rules."
    row = {
        "readiness_id": "CS08-READINESS-INSTITUTION",
        "overall_readiness_score": readiness_score,
        "overall_readiness_status": status,
        "institutional_health_score": health_score,
        "designed_experiments": len(dashboard),
        "registered_experiments": int(dashboard.registry_id.notna().sum()),
        "authorised_experiments": int(dashboard.execution_authorised.map(_bool).sum()),
        "running_experiments": int(dashboard.execution_status.eq("RUNNING").sum()),
        "completed_experiments": int(dashboard.execution_status.eq("COMPLETED").sum()),
        "assessable_evidence": assessable,
        "ready_for_review": ready_review,
        "ready_for_replication": ready_replication,
        "replication_complete": replication_complete,
        "critical_actions": critical,
        "high_actions": high,
        "production_ready_experiments": 0,
        "next_institutional_action": next_action,
        "production_changes_authorised": False,
        "generated_utc": generated_utc,
        "engine_version": ENGINE_VERSION,
    }
    return pd.DataFrame([row]).loc[:, READINESS_COLUMNS]


def validate_outputs(dashboard: pd.DataFrame, health: pd.DataFrame, readiness: pd.DataFrame) -> ValidationResult:
    errors = _required(dashboard, DASHBOARD_COLUMNS, "dashboard") + _required(health, HEALTH_COLUMNS, "health") + _required(readiness, READINESS_COLUMNS, "readiness")
    if dashboard.empty:
        errors.append("dashboard contains no experiments")
    if dashboard.experiment_id.duplicated().any():
        errors.append("dashboard experiment_id values must be unique")
    if dashboard.governance_id.duplicated().any():
        errors.append("governance_id values must be unique")
    if dashboard.production_ready.map(_bool).any():
        errors.append("CS08 cannot declare an experiment production ready")
    if readiness.production_changes_authorised.map(_bool).any():
        errors.append("CS08 cannot authorise production changes")
    if not health.score.between(0, 100).all():
        errors.append("health scores must be within 0..100")
    return ValidationResult(not errors, tuple(errors), ())


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
            stream.write(text)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _report(dashboard: pd.DataFrame, health: pd.DataFrame, review: pd.DataFrame, readiness: pd.DataFrame, validation: ValidationResult, generated_utc: str) -> str:
    r = readiness.iloc[0]
    lines = [
        "=" * 108,
        "BACQE CONVEX SURVIVAL CS08 - INSTITUTIONAL RESEARCH GOVERNANCE DASHBOARD",
        "=" * 108,
        f"Generated UTC:                 {generated_utc}",
        f"Engine version:                {ENGINE_VERSION}",
        f"Experiments governed:          {len(dashboard)}",
        f"Institutional health:          {r.institutional_health_score:.2f}/100",
        f"Research readiness:            {r.overall_readiness_score:.2f}/100 ({r.overall_readiness_status})",
        f"Validation:                    {'PASS' if validation.passed else 'FAIL'}",
        "",
        "INSTITUTIONAL PURPOSE",
        "-" * 108,
        "CS08 consolidates the governed research lifecycle into one read-only institutional view.",
        "It does not approve experiments, alter protocols, determine final scientific conclusions or authorise production changes.",
        "",
        "PIPELINE SUMMARY",
        "-" * 108,
        f"Designed experiments:          {int(r.designed_experiments)}",
        f"Registered experiments:        {int(r.registered_experiments)}",
        f"Authorised experiments:        {int(r.authorised_experiments)}",
        f"Running experiments:           {int(r.running_experiments)}",
        f"Completed experiments:         {int(r.completed_experiments)}",
        f"Assessable evidence:           {int(r.assessable_evidence)}",
        f"Ready for replication:         {int(r.ready_for_replication)}",
        f"Ready for scientific review:   {int(r.ready_for_review)}",
        f"Production-ready experiments:  {int(r.production_ready_experiments)}",
        "",
        "INSTITUTIONAL HEALTH",
        "-" * 108,
    ]
    for _, row in health.iterrows():
        lines.append(f"{row.health_dimension:<32} {row.score:6.2f}/100  {row.status}")
    lines += ["", "EVIDENCE STATE COUNTS", "-" * 108]
    for state, count in dashboard.evidence_state.value_counts().sort_index().items():
        lines.append(f"{state:<38} {count}")
    lines += ["", "EXECUTION STATUS COUNTS", "-" * 108]
    for state, count in dashboard.execution_status.value_counts().sort_index().items():
        lines.append(f"{state:<38} {count}")
    lines += ["", "HUMAN ATTENTION QUEUE", "-" * 108]
    if review.empty:
        lines.append("No immediate human actions are currently queued.")
    else:
        for _, row in review.head(10).iterrows():
            lines += [
                f"{int(row.review_rank):2d}. {row.experiment_title}",
                f"    Priority:                  {row.review_priority}",
                f"    Stage:                     {row.pipeline_stage}",
                f"    Action:                    {row.attention_reason}",
                "",
            ]
    lines += [
        "NEXT INSTITUTIONAL ACTION",
        "-" * 108,
        str(r.next_institutional_action),
        "",
    ]
    if validation.warnings:
        lines += ["VALIDATION WARNINGS", "-" * 108] + [f"- {warning}" for warning in validation.warnings] + [""]
    lines += [
        "GOVERNANCE INTERPRETATION",
        "-" * 108,
        "Institutional health measures process integrity; research readiness measures governed lifecycle progress.",
        "Neither score measures expected profitability, validates an edge or grants permission to trade live.",
        "Cross-symbol and independent replication remain required before production consideration.",
        "=" * 108,
    ]
    return "\n".join(lines) + "\n"


def run_institutional_governance(source_paths: dict[str, Path], audit_paths: dict[str, Path], output_dir: Path) -> GovernanceOutputs:
    source_paths = {key: Path(value) for key, value in source_paths.items()}
    audit_paths = {key: Path(value) for key, value in audit_paths.items()}
    output_dir = Path(output_dir)
    missing = [str(path) for path in [*source_paths.values(), *audit_paths.values()] if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing CS08 source files: " + "; ".join(missing))
    frames = {engine: pd.read_csv(source_paths[engine]) for engine in SOURCE_ENGINES}
    audits = {engine: json.loads(audit_paths[engine].read_text(encoding="utf-8")) for engine in SOURCE_ENGINES}
    validation = validate_sources(frames, audits)
    if not validation.passed:
        raise InstitutionalGovernanceError("; ".join(validation.errors))

    generated_utc = datetime.now(timezone.utc).isoformat()
    dashboard = build_dashboard(frames, generated_utc)
    health = build_health(dashboard, frames, generated_utc)
    review = build_review_queue(dashboard)
    readiness = build_readiness(dashboard, health, generated_utc)
    output_validation = validate_outputs(dashboard, health, readiness)
    if not output_validation.passed:
        raise InstitutionalGovernanceError("; ".join(output_validation.errors))
    combined = ValidationResult(True, (), validation.warnings)

    paths = OutputPaths(
        output_directory=output_dir,
        dashboard_csv=output_dir / "institutional_dashboard_latest.csv",
        health_csv=output_dir / "institutional_health_latest.csv",
        review_queue_csv=output_dir / "institutional_review_queue_latest.csv",
        readiness_csv=output_dir / "institutional_readiness_latest.csv",
        report_txt=output_dir / "institutional_governance_report_latest.txt",
        audit_json=output_dir / "institutional_dashboard_audit_latest.json",
    )
    _atomic_write(paths.dashboard_csv, dashboard.to_csv(index=False))
    _atomic_write(paths.health_csv, health.to_csv(index=False))
    _atomic_write(paths.review_queue_csv, review.to_csv(index=False))
    _atomic_write(paths.readiness_csv, readiness.to_csv(index=False))
    _atomic_write(paths.report_txt, _report(dashboard, health, review, readiness, combined, generated_utc))

    audit_payload = {
        "engine_id": ENGINE_ID,
        "engine_name": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "generated_utc": generated_utc,
        "dashboard_count": len(dashboard),
        "health_dimension_count": len(health),
        "review_queue_count": len(review),
        "readiness_score": float(readiness.iloc[0].overall_readiness_score),
        "institutional_health_score": float(readiness.iloc[0].institutional_health_score),
        "pipeline_stage_counts": dashboard.pipeline_stage.value_counts().sort_index().to_dict(),
        "evidence_state_counts": dashboard.evidence_state.value_counts().sort_index().to_dict(),
        "execution_status_counts": dashboard.execution_status.value_counts().sort_index().to_dict(),
        "policy": {
            "dashboard_is_read_only": True,
            "human_governance_required": True,
            "final_scientific_conclusion_authorised": False,
            "cross_symbol_replication_required_before_production": True,
            "production_changes_authorised": False,
            "phase_one_complete": True,
        },
        "source_lineage": {
            engine: {
                "engine_id": audits[engine].get("engine_id"),
                "engine_version": audits[engine].get("engine_version"),
                "generated_utc": audits[engine].get("generated_utc"),
                "data_sha256": _sha256(source_paths[engine]),
                "audit_sha256": _sha256(audit_paths[engine]),
            }
            for engine in SOURCE_ENGINES
        },
        "validation": asdict(combined),
        "output_paths": {key: str(value) for key, value in asdict(paths).items()},
    }
    _atomic_write(paths.audit_json, json.dumps(audit_payload, indent=2, sort_keys=True) + "\n")
    return GovernanceOutputs(dashboard, health, review, readiness, combined, paths)
