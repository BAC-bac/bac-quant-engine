from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

ENGINE_ID = "CS04"
ENGINE_NAME = "Experiment Design Engine"
ENGINE_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"
SOURCE_ENGINE_ID = "CS03"

REQUIRED_CANDIDATE_COLUMNS = (
    "priority_rank", "candidate_id", "candidate_type", "primary_filter",
    "candidate_title", "research_question", "null_hypothesis",
    "alternative_hypothesis", "control_definition", "treatment_definition",
    "evidence_basis", "sample_adequacy_score", "confounding_risk_score",
    "information_gain_proxy", "expected_research_value", "priority_score",
    "priority_band", "principal_risk", "recommended_next_action",
    "source_engine_id", "source_engine_version", "generated_utc", "engine_version",
)

SUPPORTED_TYPES = {
    "THRESHOLD_SENSITIVITY",
    "REPLICATION_EXPANSION",
    "INTERACTION_ISOLATION",
    "DATA_QUALITY_INVESTIGATION",
}

DESIGN_COLUMNS = (
    "design_rank", "experiment_id", "candidate_id", "experiment_title",
    "experiment_family", "primary_filter", "secondary_filter", "research_question",
    "null_hypothesis", "alternative_hypothesis", "objective", "design_status",
    "control_arm", "treatment_arms", "allocation_method", "unit_of_analysis",
    "primary_metrics", "secondary_metrics", "risk_guardrails", "minimum_observations",
    "minimum_sole_veto_observations", "maximum_calendar_days", "interim_review_interval",
    "stopping_rule", "success_criteria", "failure_criteria", "inconclusive_criteria",
    "confounding_controls", "data_quality_checks", "execution_instructions",
    "expected_information_gain_band", "estimated_cost_band", "priority_score",
    "priority_band", "sample_adequacy_score", "confounding_risk_score",
    "principal_risk", "source_candidate_rank", "source_candidate_engine_id",
    "source_candidate_engine_version", "source_candidate_generated_utc",
    "source_candidates_sha256", "source_audit_sha256", "generated_utc", "engine_version",
)

MANIFEST_COLUMNS = (
    "design_rank", "experiment_id", "candidate_id", "experiment_title",
    "experiment_family", "primary_filter", "design_status", "minimum_observations",
    "maximum_calendar_days", "primary_metrics", "stopping_rule", "success_criteria",
    "priority_band", "priority_score", "principal_risk",
)

class ExperimentDesignError(ValueError):
    """Raised when CS04 cannot create institutionally valid protocols."""

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
    candidates_sha256: str
    audit_sha256: str

@dataclass(frozen=True)
class OutputPaths:
    output_directory: Path
    designs_csv: Path
    execution_manifest_csv: Path
    report_txt: Path
    audit_json: Path

@dataclass(frozen=True)
class DesignOutputs:
    designs: pd.DataFrame
    execution_manifest: pd.DataFrame
    validation: ValidationResult
    source_lineage: SourceLineage
    paths: OutputPaths


def _require_columns(frame: pd.DataFrame, required: Iterable[str], label: str) -> list[str]:
    return [f"{label} missing required column: {c}" for c in required if c not in frame.columns]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_id(candidate_id: str, family: str) -> str:
    token = hashlib.sha256(f"{candidate_id}|{family}|CS04".encode()).hexdigest()[:10].upper()
    return f"CS04-{family}-{token}"


def _band(value: float, low: float = 0.05, high: float = 0.15) -> str:
    if value >= high:
        return "HIGH"
    if value >= low:
        return "MEDIUM"
    return "LOW"


def _cost_band(candidate_type: str) -> str:
    return {
        "THRESHOLD_SENSITIVITY": "MEDIUM",
        "REPLICATION_EXPANSION": "LOW",
        "INTERACTION_ISOLATION": "HIGH",
        "DATA_QUALITY_INVESTIGATION": "LOW",
    }[candidate_type]


def validate_inputs(candidates: pd.DataFrame, audit: dict[str, Any]) -> ValidationResult:
    errors = _require_columns(candidates, REQUIRED_CANDIDATE_COLUMNS, "candidate analysis")
    warnings: list[str] = []
    if not candidates.empty and not errors:
        unknown = sorted(set(candidates.get("candidate_type", [])) - SUPPORTED_TYPES)
        if unknown:
            errors.append(f"unsupported candidate types: {', '.join(unknown)}")
        if candidates["candidate_id"].duplicated().any():
            errors.append("candidate_id values must be unique")
        if not candidates["priority_rank"].is_unique:
            errors.append("priority_rank values must be unique")
        if set(candidates["source_engine_id"].astype(str)) != {"CS02"}:
            errors.append("CS03 candidate rows must retain CS02 as source_engine_id")
        for col in ("sample_adequacy_score", "confounding_risk_score", "priority_score"):
            values = pd.to_numeric(candidates[col], errors="coerce")
            if values.isna().any() or ((values < 0) | (values > 1)).any():
                errors.append(f"{col} must contain finite values in [0, 1]")
    if audit.get("engine_id") != SOURCE_ENGINE_ID:
        errors.append("source audit must identify engine_id CS03")
    if not audit.get("validation", {}).get("passed", False):
        errors.append("source CS03 audit validation did not pass")
    if len(candidates) < 1:
        errors.append("candidate analysis contains no rows")
    if (pd.to_numeric(candidates.get("sample_adequacy_score", pd.Series(dtype=float)), errors="coerce") < 0.4).any():
        warnings.append("One or more CS03 candidates have low sample adequacy; CS04 protocols require replication or conservative stopping rules.")
    return ValidationResult(not errors, tuple(errors), tuple(warnings))


def _minimum_observations(row: pd.Series) -> int:
    adequacy = float(row["sample_adequacy_score"])
    base = {
        "THRESHOLD_SENSITIVITY": 600,
        "REPLICATION_EXPANSION": 1000,
        "INTERACTION_ISOLATION": 1500,
        "DATA_QUALITY_INVESTIGATION": 250,
    }[row["candidate_type"]]
    multiplier = 1.0 + max(0.0, 0.5 - adequacy)
    return int(math.ceil(base * multiplier / 50.0) * 50)


def _protocol(row: pd.Series) -> dict[str, Any]:
    family = str(row["candidate_type"])
    filt = str(row["primary_filter"])
    secondary = "" if pd.isna(row.get("secondary_filter")) else str(row.get("secondary_filter"))
    minimum = _minimum_observations(row)
    sole_min = 0
    max_days = 45
    review = "Every 250 observations"
    unit = "One evaluated trade opportunity"
    metrics = "qualification_rate; survival_rate; sole_veto_rate"
    secondary_metrics = "trade_count; expected_R; realised_R; maximum_drawdown; profit_factor"
    guardrails = "Do not alter live production rules; reject any treatment with materially worse drawdown or rule-integrity failures."
    confounders = "Freeze all non-tested filters, symbol universe, timeframe, costs, data pipeline and evaluation code."
    checks = "Schema validation; missingness; duplicate observations; treatment assignment integrity; invariant checks; source-hash verification."

    if family == "THRESHOLD_SENSITIVITY":
        treatments = f"Current value (control); bounded tightening of {filt}; bounded relaxation of {filt}. Exact numeric levels must be supplied by the strategy configuration before execution."
        allocation = "Deterministic replay of the identical observation set through every arm"
        stopping = f"Stop after at least {minimum} common observations per arm and 30 sole-veto observations overall, or after {max_days} calendar days; stop early for a guardrail breach."
        success = "A treatment produces a reproducible, practically material change in qualification or survival, survives cost sensitivity, and does not breach risk guardrails."
        failure = "No practically material difference is observed, results reverse across subperiods, or any risk guardrail is breached."
        sole_min = 30
        objective = f"Estimate the local sensitivity of outcomes to bounded changes in {filt} without treating the exercise as production optimisation."
        instructions = "Create immutable control and treatment configurations; replay identical observations; compare paired outcomes; report effect sizes and confidence intervals; retain production settings until formal review."
    elif family == "REPLICATION_EXPANSION":
        treatments = "No parameter treatment. Continue the unchanged production decision policy while collecting a larger, more representative sample."
        allocation = "Prospective accumulation under one frozen policy, segmented by time and market regime"
        stopping = f"Stop after at least {minimum} new observations and 30 new sole-veto observations for {filt}, or after 60 calendar days; do not stop merely because an interim result looks favourable."
        success = "The direction and practical magnitude of marginal influence remain stable across at least two non-overlapping segments with acceptable uncertainty."
        failure = "The apparent influence collapses toward zero, reverses direction, or is explained by concentration in one segment."
        sole_min = 30
        max_days = 60
        objective = f"Determine whether the current marginal evidence for {filt} survives a larger and more representative sample."
        instructions = "Freeze policy and instrumentation; collect all eligible near misses; repeat CS01-CS03 attribution at each review; compare effect stability by subperiod and regime."
    elif family == "INTERACTION_ISOLATION":
        partner = secondary or "the highest-frequency co-failing filter"
        treatments = f"Four-cell factorial replay: neither {filt} nor {partner} active; {filt} only; {partner} only; both active. Production remains unchanged."
        allocation = "Deterministic factorial replay on the same observations"
        stopping = f"Stop after at least {minimum} common observations, including at least 50 observations in each informative interaction cell, or after 60 calendar days."
        success = "The interaction term is stable, practically material and distinguishable from both main effects across validation segments."
        failure = "Interaction estimates are unstable, cells remain sparse, or the apparent effect is explained by one main effect."
        max_days = 60
        objective = f"Identify whether {filt} has an independent effect or is redundant/conditional on {partner}."
        instructions = "Construct a pre-specified factorial replay; estimate main and interaction effects; require cell-balance diagnostics; avoid interpreting sparse cells."
    else:
        treatments = f"No strategy treatment. Trace {filt} from raw event through ingestion, feature calculation, veto assignment and CS02 aggregation."
        allocation = "Full-population reconciliation with targeted stratified samples"
        stopping = f"Stop after reconciling at least {minimum} observations and 100% of identified discrepancies, or after 30 calendar days with unresolved issues explicitly logged."
        success = "Counts, identities and transformations reconcile end-to-end with no unexplained material discrepancies."
        failure = "Material discrepancies remain unexplained, lineage is incomplete, or reruns are non-deterministic."
        max_days = 30
        objective = f"Establish whether the observed evidence for {filt} is genuine or produced by data, schema or attribution defects."
        instructions = "Reconcile hashes and counts across stages; sample raw records; reproduce veto logic independently; document every discrepancy and remediation."
        metrics = "row_count_reconciliation; discrepancy_count; unexplained_discrepancy_rate; rerun_determinism"
        secondary_metrics = "missingness; duplicate_rate; schema_drift; lineage_completeness"

    return {
        "experiment_id": _stable_id(str(row["candidate_id"]), family),
        "experiment_title": f"{family.replace('_', ' ').title()}: {filt}",
        "experiment_family": family,
        "objective": objective,
        "design_status": "DRAFT_PROTOCOL",
        "control_arm": str(row["control_definition"]),
        "treatment_arms": treatments,
        "allocation_method": allocation,
        "unit_of_analysis": unit,
        "primary_metrics": metrics,
        "secondary_metrics": secondary_metrics,
        "risk_guardrails": guardrails,
        "minimum_observations": minimum,
        "minimum_sole_veto_observations": sole_min,
        "maximum_calendar_days": max_days,
        "interim_review_interval": review,
        "stopping_rule": stopping,
        "success_criteria": success,
        "failure_criteria": failure,
        "inconclusive_criteria": "Classify as inconclusive when minimum evidence is not reached, estimates are unstable, or uncertainty spans both immaterial and material effects.",
        "confounding_controls": confounders,
        "data_quality_checks": checks,
        "execution_instructions": instructions,
        "expected_information_gain_band": _band(float(row["information_gain_proxy"])),
        "estimated_cost_band": _cost_band(family),
    }


def build_experiment_designs(candidates: pd.DataFrame, candidates_sha256: str, audit_sha256: str, generated_utc: str | None = None) -> pd.DataFrame:
    generated_utc = generated_utc or datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    ordered = candidates.sort_values(["priority_rank", "candidate_id"], kind="mergesort")
    for _, source in ordered.iterrows():
        protocol = _protocol(source)
        rows.append({
            "design_rank": int(source["priority_rank"]),
            **protocol,
            "candidate_id": source["candidate_id"],
            "primary_filter": source["primary_filter"],
            "secondary_filter": "" if pd.isna(source.get("secondary_filter")) else source.get("secondary_filter"),
            "research_question": source["research_question"],
            "null_hypothesis": source["null_hypothesis"],
            "alternative_hypothesis": source["alternative_hypothesis"],
            "priority_score": round(float(source["priority_score"]), 6),
            "priority_band": source["priority_band"],
            "sample_adequacy_score": round(float(source["sample_adequacy_score"]), 6),
            "confounding_risk_score": round(float(source["confounding_risk_score"]), 6),
            "principal_risk": source["principal_risk"],
            "source_candidate_rank": int(source["priority_rank"]),
            "source_candidate_engine_id": SOURCE_ENGINE_ID,
            "source_candidate_engine_version": source["engine_version"],
            "source_candidate_generated_utc": source["generated_utc"],
            "source_candidates_sha256": candidates_sha256,
            "source_audit_sha256": audit_sha256,
            "generated_utc": generated_utc,
            "engine_version": ENGINE_VERSION,
        })
    return pd.DataFrame(rows, columns=DESIGN_COLUMNS)


def validate_designs(designs: pd.DataFrame, candidate_ids: set[str]) -> ValidationResult:
    errors = _require_columns(designs, DESIGN_COLUMNS, "experiment designs")
    warnings: list[str] = []
    if designs.empty:
        errors.append("no experiment designs generated")
    if designs["experiment_id"].duplicated().any():
        errors.append("experiment_id values must be unique")
    if set(designs["candidate_id"]) != candidate_ids:
        errors.append("designs must preserve one-to-one candidate identity")
    if (pd.to_numeric(designs["minimum_observations"], errors="coerce") <= 0).any():
        errors.append("minimum_observations must be positive")
    required_text = ("objective", "stopping_rule", "success_criteria", "failure_criteria", "risk_guardrails")
    for col in required_text:
        if designs[col].fillna("").astype(str).str.strip().eq("").any():
            errors.append(f"{col} must be populated")
    if (designs["design_status"] != "DRAFT_PROTOCOL").any():
        errors.append("new protocols must begin in DRAFT_PROTOCOL status")
    if (designs["priority_band"] == "REPLICATION_REQUIRED").any():
        warnings.append("Replication-required candidates were converted into conservative protocols and must not be treated as approval to change production rules.")
    return ValidationResult(not errors, tuple(errors), tuple(warnings))


def build_manifest(designs: pd.DataFrame) -> pd.DataFrame:
    return designs.loc[:, MANIFEST_COLUMNS].copy()


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(temp_name, path)
    except Exception:
        try: os.unlink(temp_name)
        except OSError: pass
        raise


def _atomic_csv(path: Path, frame: pd.DataFrame) -> None:
    _atomic_text(path, frame.to_csv(index=False, lineterminator="\n"))


def render_report(designs: pd.DataFrame, validation: ValidationResult, lineage: SourceLineage, generated_utc: str) -> str:
    lines = [
        "=" * 100,
        "BACQE CONVEX SURVIVAL CS04 - EXPERIMENT DESIGN ENGINE",
        "=" * 100,
        f"Generated UTC:              {generated_utc}",
        f"Engine version:             {ENGINE_VERSION}",
        f"Source engine:              {lineage.engine_id} v{lineage.engine_version}",
        f"Protocols generated:        {len(designs)}",
        f"Validation:                 {'PASS' if validation.passed else 'FAIL'}",
        "",
        "INSTITUTIONAL PURPOSE",
        "-" * 100,
        "CS04 converts CS03 research candidates into draft, reproducible experiment protocols.",
        "A protocol is not permission to alter production rules. Numeric treatment levels require configuration-backed approval before execution.",
        "",
        "PROTOCOL FAMILY COUNTS",
        "-" * 100,
    ]
    for name, count in designs["experiment_family"].value_counts().sort_index().items():
        lines.append(f"{name:<40} {count}")
    lines += ["", "TOP DRAFT PROTOCOLS", "-" * 100]
    for _, row in designs.head(10).iterrows():
        lines += [
            f"{int(row['design_rank']):2d}. {row['experiment_title']}",
            f"    Experiment ID:          {row['experiment_id']}",
            f"    Source candidate:       {row['candidate_id']}",
            f"    Status:                 {row['design_status']}",
            f"    Minimum observations:   {int(row['minimum_observations']):,}",
            f"    Maximum calendar days:  {int(row['maximum_calendar_days'])}",
            f"    Objective:              {row['objective']}",
            f"    Stopping rule:          {row['stopping_rule']}",
            f"    Success criteria:       {row['success_criteria']}",
            "",
        ]
    if validation.warnings:
        lines += ["VALIDATION WARNINGS", "-" * 100, *[f"- {w}" for w in validation.warnings], ""]
    lines += [
        "SCIENTIFIC INTERPRETATION", "-" * 100,
        "CS04 specifies how evidence should be gathered and judged before a conclusion is allowed.",
        "DRAFT_PROTOCOL means the design is structurally complete but still requires strategy-specific numeric configuration and human approval.",
        "=" * 100,
    ]
    return "\n".join(lines) + "\n"


def run_experiment_design(candidate_csv: Path, source_audit_json: Path, output_directory: Path) -> DesignOutputs:
    candidate_csv, source_audit_json, output_directory = map(Path, (candidate_csv, source_audit_json, output_directory))
    candidates = pd.read_csv(candidate_csv)
    audit = json.loads(source_audit_json.read_text(encoding="utf-8"))
    input_validation = validate_inputs(candidates, audit)
    if not input_validation.passed:
        raise ExperimentDesignError("; ".join(input_validation.errors))
    generated_utc = datetime.now(timezone.utc).isoformat()
    candidate_hash, audit_hash = _sha256(candidate_csv), _sha256(source_audit_json)
    designs = build_experiment_designs(candidates, candidate_hash, audit_hash, generated_utc)
    output_validation = validate_designs(designs, set(candidates["candidate_id"]))
    if not output_validation.passed:
        raise ExperimentDesignError("; ".join(output_validation.errors))
    validation = ValidationResult(True, (), tuple(dict.fromkeys(input_validation.warnings + output_validation.warnings)))
    lineage = SourceLineage(
        engine_id=audit["engine_id"], engine_name=audit["engine_name"], engine_version=audit["engine_version"],
        generated_utc=audit["generated_utc"], candidates_sha256=candidate_hash, audit_sha256=audit_hash,
    )
    paths = OutputPaths(
        output_directory, output_directory / "experiment_designs_latest.csv",
        output_directory / "experiment_execution_manifest_latest.csv",
        output_directory / "experiment_design_report_latest.txt",
        output_directory / "experiment_design_audit_latest.json",
    )
    manifest = build_manifest(designs)
    _atomic_csv(paths.designs_csv, designs)
    _atomic_csv(paths.execution_manifest_csv, manifest)
    _atomic_text(paths.report_txt, render_report(designs, validation, lineage, generated_utc))
    audit_output = {
        "engine_id": ENGINE_ID, "engine_name": ENGINE_NAME, "engine_version": ENGINE_VERSION,
        "schema_version": SCHEMA_VERSION, "generated_utc": generated_utc,
        "protocol_count": len(designs), "execution_manifest_count": len(manifest),
        "protocol_family_counts": designs["experiment_family"].value_counts().sort_index().to_dict(),
        "design_status_counts": designs["design_status"].value_counts().sort_index().to_dict(),
        "source_lineage": asdict(lineage), "validation": asdict(validation),
        "output_paths": {k: str(v) for k, v in asdict(paths).items()},
        "policy": {
            "production_changes_authorised": False,
            "numeric_treatment_levels_finalised": False,
            "new_protocol_status": "DRAFT_PROTOCOL",
            "minimum_sole_veto_target": 30,
        },
    }
    _atomic_text(paths.audit_json, json.dumps(audit_output, indent=2, sort_keys=True) + "\n")
    return DesignOutputs(designs, manifest, validation, lineage, paths)
