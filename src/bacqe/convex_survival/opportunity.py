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


ENGINE_ID = "CS03"
ENGINE_NAME = "Candidate Opportunity Analysis"
ENGINE_VERSION = "1.0.0"
SCHEMA_VERSION = "1.0.0"

FILTER_SUMMARY_REQUIRED_COLUMNS = (
    "rank",
    "filter",
    "failures",
    "failure_pct",
    "failed_row_participation_pct",
    "first_vetoes",
    "first_veto_pct",
    "sole_vetoes",
    "sole_veto_pct",
    "average_cofail_count",
    "marginal_influence_pct",
    "marginal_influence_class",
    "research_priority",
)

RECOMMENDATION_REQUIRED_COLUMNS = (
    "recommendation_rank",
    "recommendation_id",
    "recommendation_type",
    "priority",
    "filter",
    "title",
    "reason",
    "evidence",
    "proposed_experiment",
    "status",
)

NUMERIC_SUMMARY_COLUMNS = (
    "rank",
    "failures",
    "failure_pct",
    "failed_row_participation_pct",
    "first_vetoes",
    "first_veto_pct",
    "sole_vetoes",
    "sole_veto_pct",
    "average_cofail_count",
    "marginal_influence_pct",
)

PERCENT_COLUMNS = (
    "failure_pct",
    "failed_row_participation_pct",
    "first_veto_pct",
    "sole_veto_pct",
    "marginal_influence_pct",
)

CANDIDATE_COLUMNS = (
    "priority_rank",
    "candidate_id",
    "candidate_type",
    "primary_filter",
    "secondary_filter",
    "candidate_title",
    "research_question",
    "null_hypothesis",
    "alternative_hypothesis",
    "control_definition",
    "treatment_definition",
    "evidence_basis",
    "failures",
    "failure_pct",
    "first_vetoes",
    "first_veto_pct",
    "sole_vetoes",
    "sole_veto_pct",
    "average_cofail_count",
    "marginal_influence_pct",
    "evidence_relevance_score",
    "uncertainty_score",
    "hypothesis_separation_score",
    "decision_relevance_score",
    "feasibility_score",
    "sample_adequacy_score",
    "experiment_cost_score",
    "confounding_risk_score",
    "information_gain_proxy",
    "expected_research_value",
    "priority_score",
    "priority_band",
    "recommendation_status",
    "principal_risk",
    "recommended_next_action",
    "source_recommendation_id",
    "source_engine_id",
    "source_engine_version",
    "source_generated_utc",
    "source_summary_sha256",
    "source_recommendations_sha256",
    "generated_utc",
    "engine_version",
)

QUEUE_COLUMNS = (
    "priority_rank",
    "candidate_id",
    "candidate_title",
    "candidate_type",
    "primary_filter",
    "secondary_filter",
    "research_question",
    "priority_score",
    "priority_band",
    "expected_information_gain_band",
    "estimated_cost_band",
    "principal_risk",
    "recommended_next_action",
    "recommendation_status",
)

DEFAULT_WEIGHTS = {
    "evidence_marginal": 0.40,
    "evidence_sole_veto": 0.25,
    "evidence_first_veto": 0.15,
    "evidence_failure_participation": 0.20,
    "erv_evidence": 0.35,
    "erv_uncertainty": 0.30,
    "erv_hypothesis_separation": 0.20,
    "erv_decision_relevance": 0.15,
    "confounding_penalty": 0.50,
    "cost_penalty": 0.35,
}


class OpportunityAnalysisError(ValueError):
    """Raised when CS03 cannot produce institutionally valid outputs."""


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
    source_path: str
    summary_sha256: str
    recommendations_sha256: str
    audit_sha256: str


@dataclass(frozen=True)
class OutputPaths:
    output_directory: Path
    analysis_csv: Path
    priority_queue_csv: Path
    report_txt: Path
    audit_json: Path


@dataclass(frozen=True)
class AnalysisOutputs:
    candidates: pd.DataFrame
    priority_queue: pd.DataFrame
    validation: ValidationResult
    source_lineage: SourceLineage
    paths: OutputPaths


def normalise_filter_name(value: Any) -> str:
    text = str(value or "").strip().upper()
    normalised = "".join(
        character if character.isalnum() else "_"
        for character in text
    )
    while "__" in normalised:
        normalised = normalised.replace("__", "_")
    return normalised.strip("_")


def _require_columns(
    frame: pd.DataFrame,
    required: Iterable[str],
    label: str,
) -> list[str]:
    return [
        f"{label} missing required column: {column}"
        for column in required
        if column not in frame.columns
    ]


def _coerce_numeric(
    frame: pd.DataFrame,
    columns: Iterable[str],
) -> pd.DataFrame:
    prepared = frame.copy()
    for column in columns:
        prepared[column] = pd.to_numeric(
            prepared[column],
            errors="coerce",
        )
    return prepared


def prepare_filter_summary(frame: pd.DataFrame) -> pd.DataFrame:
    errors = _require_columns(
        frame,
        FILTER_SUMMARY_REQUIRED_COLUMNS,
        "filter summary",
    )
    if errors:
        raise OpportunityAnalysisError("; ".join(errors))

    prepared = frame.copy()
    prepared["filter"] = prepared["filter"].map(
        normalise_filter_name
    )
    prepared = _coerce_numeric(
        prepared,
        NUMERIC_SUMMARY_COLUMNS,
    )
    prepared["marginal_influence_class"] = (
        prepared["marginal_influence_class"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )
    prepared["research_priority"] = (
        prepared["research_priority"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )
    return prepared


def prepare_recommendations(frame: pd.DataFrame) -> pd.DataFrame:
    errors = _require_columns(
        frame,
        RECOMMENDATION_REQUIRED_COLUMNS,
        "recommendations",
    )
    if errors:
        raise OpportunityAnalysisError("; ".join(errors))

    prepared = frame.copy()
    prepared["filter"] = prepared["filter"].map(
        normalise_filter_name
    )
    prepared["recommendation_rank"] = pd.to_numeric(
        prepared["recommendation_rank"],
        errors="coerce",
    )
    for column in (
        "recommendation_id",
        "recommendation_type",
        "priority",
        "title",
        "reason",
        "evidence",
        "proposed_experiment",
        "status",
    ):
        prepared[column] = (
            prepared[column]
            .fillna("")
            .astype(str)
            .str.strip()
        )
    prepared["recommendation_type"] = (
        prepared["recommendation_type"].str.upper()
    )
    prepared["priority"] = prepared["priority"].str.upper()
    prepared["status"] = prepared["status"].str.upper()
    return prepared


def validate_inputs(
    summary: pd.DataFrame,
    recommendations: pd.DataFrame,
    audit: dict[str, Any],
) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    errors.extend(
        _require_columns(
            summary,
            FILTER_SUMMARY_REQUIRED_COLUMNS,
            "filter summary",
        )
    )
    errors.extend(
        _require_columns(
            recommendations,
            RECOMMENDATION_REQUIRED_COLUMNS,
            "recommendations",
        )
    )

    if errors:
        return ValidationResult(
            passed=False,
            errors=tuple(errors),
            warnings=(),
        )

    prepared_summary = prepare_filter_summary(summary)
    prepared_recommendations = prepare_recommendations(
        recommendations
    )

    if prepared_summary.empty:
        errors.append("filter summary contains no rows")
    if prepared_recommendations.empty:
        errors.append("recommendations contain no rows")

    blank_filters = prepared_summary["filter"].eq("")
    if blank_filters.any():
        errors.append("filter summary contains blank filter names")

    duplicates = prepared_summary["filter"].duplicated(
        keep=False
    )
    if duplicates.any():
        names = sorted(
            prepared_summary.loc[duplicates, "filter"].unique()
        )
        errors.append(
            "filter summary contains duplicate filters: "
            + ", ".join(names)
        )

    for column in NUMERIC_SUMMARY_COLUMNS:
        if prepared_summary[column].isna().any():
            errors.append(
                f"filter summary column {column} contains "
                "non-numeric or missing values"
            )

    for column in PERCENT_COLUMNS:
        invalid = ~prepared_summary[column].between(
            0.0,
            100.0,
            inclusive="both",
        )
        if invalid.any():
            errors.append(
                f"filter summary column {column} must be "
                "within [0, 100]"
            )

    for column in (
        "failures",
        "first_vetoes",
        "sole_vetoes",
        "average_cofail_count",
    ):
        if (prepared_summary[column] < 0).any():
            errors.append(
                f"filter summary column {column} contains "
                "negative values"
            )

    summary_filters = set(prepared_summary["filter"])
    recommendation_filters = set(
        prepared_recommendations["filter"]
    )
    unknown = sorted(
        recommendation_filters - summary_filters
    )
    if unknown:
        errors.append(
            "recommendations reference filters absent from "
            "the summary: "
            + ", ".join(unknown)
        )

    duplicate_ids = prepared_recommendations[
        "recommendation_id"
    ].duplicated(keep=False)
    if duplicate_ids.any():
        errors.append(
            "recommendations contain duplicate "
            "recommendation_id values"
        )

    required_audit_keys = (
        "engine_id",
        "engine_name",
        "engine_version",
        "generated_utc",
        "dataset_summary",
        "validation",
    )
    for key in required_audit_keys:
        if key not in audit:
            errors.append(
                f"source audit missing required key: {key}"
            )

    if audit.get("engine_id") != "CS02":
        errors.append(
            "source audit engine_id must be CS02"
        )

    audit_validation = audit.get("validation", {})
    if audit_validation.get("passed") is not True:
        errors.append(
            "source CS02 audit did not record a passing "
            "validation result"
        )

    sole_veto_rows = (
        audit.get("dataset_summary", {})
        .get("sole_veto_rows")
    )
    if isinstance(sole_veto_rows, int) and sole_veto_rows < 30:
        warnings.append(
            "CS02 contains fewer than 30 sole-veto rows; "
            "marginal attribution is sample-limited and "
            "CS03 scores will apply a sample penalty."
        )

    if len(prepared_summary) < 2:
        warnings.append(
            "Only one filter is available; relative "
            "normalisation has limited interpretive value."
        )

    return ValidationResult(
        passed=not errors,
        errors=tuple(errors),
        warnings=tuple(warnings),
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(
            lambda: file_handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)
    return digest.hexdigest()


def load_source_audit(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file_handle:
        value = json.load(file_handle)
    if not isinstance(value, dict):
        raise OpportunityAnalysisError(
            "source audit must contain a JSON object"
        )
    return value


def build_source_lineage(
    summary_path: Path,
    recommendations_path: Path,
    audit_path: Path,
    audit: dict[str, Any],
) -> SourceLineage:
    return SourceLineage(
        engine_id=str(audit["engine_id"]),
        engine_name=str(audit["engine_name"]),
        engine_version=str(audit["engine_version"]),
        generated_utc=str(audit["generated_utc"]),
        source_path=str(audit.get("source_path", "")),
        summary_sha256=sha256_file(summary_path),
        recommendations_sha256=sha256_file(
            recommendations_path
        ),
        audit_sha256=sha256_file(audit_path),
    )


def _safe_max_normalise(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
    maximum = float(numeric.max())
    if maximum <= 0.0:
        return pd.Series(
            0.0,
            index=series.index,
            dtype=float,
        )
    return (numeric / maximum).clip(0.0, 1.0)


def _uncertainty_from_count(
    count: pd.Series,
    target_count: float = 30.0,
) -> pd.Series:
    numeric = pd.to_numeric(count, errors="coerce").fillna(0.0)
    # Highest uncertainty occurs around a small but non-zero sample.
    evidence_presence = (numeric / target_count).clip(0.0, 1.0)
    saturation = 1.0 - (
        numeric / (numeric + target_count)
    )
    return (
        0.35
        + 0.65 * evidence_presence * saturation
    ).clip(0.0, 1.0)


def _sample_adequacy(
    sole_vetoes: pd.Series,
    first_vetoes: pd.Series,
) -> pd.Series:
    sole = (
        pd.to_numeric(sole_vetoes, errors="coerce")
        .fillna(0.0)
    )
    first = (
        pd.to_numeric(first_vetoes, errors="coerce")
        .fillna(0.0)
    )
    sole_component = (sole / 30.0).clip(0.0, 1.0)
    first_component = (first / 100.0).clip(0.0, 1.0)
    return (
        0.75 * sole_component
        + 0.25 * first_component
    ).clip(0.05, 1.0)


def _cost_score(row: pd.Series) -> float:
    cofail = float(row["average_cofail_count"])
    if cofail >= 7.0:
        return 0.80
    if cofail >= 6.0:
        return 0.65
    if cofail >= 4.0:
        return 0.50
    return 0.35


def _confounding_score(row: pd.Series) -> float:
    cofail = float(row["average_cofail_count"])
    participation = float(
        row["failed_row_participation_pct"]
    ) / 100.0
    cofail_component = min(cofail / 8.0, 1.0)
    saturation_component = max(
        0.0,
        (participation - 0.80) / 0.20,
    )
    return min(
        1.0,
        0.75 * cofail_component
        + 0.25 * saturation_component,
    )


def _decision_relevance(row: pd.Series) -> float:
    first = min(float(row["first_veto_pct"]) / 25.0, 1.0)
    marginal = min(
        float(row["marginal_influence_pct"]) / 0.10,
        1.0,
    )
    return max(0.20, 0.60 * first + 0.40 * marginal)


def _hypothesis_separation(row: pd.Series) -> float:
    has_marginal = float(row["sole_vetoes"]) > 0
    has_ordering = float(row["first_vetoes"]) > 0
    if has_marginal and has_ordering:
        return 0.90
    if has_ordering:
        return 0.70
    return 0.45


def _candidate_type(row: pd.Series) -> str:
    sole = int(row["sole_vetoes"])
    first = int(row["first_vetoes"])
    cofail = float(row["average_cofail_count"])

    if sole < 5 and first >= 100:
        return "REPLICATION_EXPANSION"
    if cofail >= 6.5 and sole == 0:
        return "INTERACTION_ISOLATION"
    if sole == 0 and first < 25:
        return "DATA_QUALITY_INVESTIGATION"
    return "THRESHOLD_SENSITIVITY"


def _candidate_text(
    filter_name: str,
    candidate_type: str,
) -> dict[str, str]:
    if candidate_type == "REPLICATION_EXPANSION":
        return {
            "title": (
                f"Expand marginal evidence for {filter_name}"
            ),
            "question": (
                f"Does {filter_name} retain meaningful "
                "marginal influence when BACQE collects a "
                "larger and more representative near-miss sample?"
            ),
            "null": (
                f"{filter_name} has no stable marginal "
                "influence beyond sampling variation."
            ),
            "alternative": (
                f"{filter_name} has stable marginal influence "
                "that persists across an expanded sample."
            ),
            "control": (
                "Current CS02 attribution evidence and the "
                "unchanged production decision policy."
            ),
            "treatment": (
                "Collect additional observations under the "
                "same decision policy, then repeat attribution "
                "and compare effect stability."
            ),
            "risk": (
                "The current sole-veto sample is too small to "
                "support precise marginal conclusions."
            ),
            "action": (
                "Expand evidence before considering any "
                "parameter intervention."
            ),
        }
    if candidate_type == "INTERACTION_ISOLATION":
        return {
            "title": (
                f"Isolate interaction structure around "
                f"{filter_name}"
            ),
            "question": (
                f"Is the apparent influence of {filter_name} "
                "independent, redundant, or conditional on "
                "other frequently co-failing filters?"
            ),
            "null": (
                f"{filter_name} contributes no separable "
                "information after controlling for co-failures."
            ),
            "alternative": (
                f"{filter_name} contributes separable or "
                "conditional information within the filter set."
            ),
            "control": (
                "Baseline outcomes using the complete current "
                "filter set."
            ),
            "treatment": (
                "Replay controlled leave-one-filter-out and "
                "paired-interaction variants while holding all "
                "other rules constant."
            ),
            "risk": (
                "High co-failure density may make a single-filter "
                "effect non-identifiable."
            ),
            "action": (
                "Run interaction-isolation analysis before a "
                "threshold experiment."
            ),
        }
    if candidate_type == "DATA_QUALITY_INVESTIGATION":
        return {
            "title": (
                f"Audit evidence quality for {filter_name}"
            ),
            "question": (
                f"Is the weak attribution evidence for "
                f"{filter_name} genuine or caused by ordering, "
                "instrumentation, rarity, or incomplete coverage?"
            ),
            "null": (
                f"The low observed influence of {filter_name} "
                "accurately represents its decision contribution."
            ),
            "alternative": (
                f"The low observed influence of {filter_name} "
                "is an artefact of evidence collection or filter "
                "ordering."
            ),
            "control": (
                "Current CS01 instrumentation and CS02 summary."
            ),
            "treatment": (
                "Audit event capture, filter ordering, coverage "
                "and missingness without changing the trading "
                "decision."
            ),
            "risk": (
                "A rare or late-evaluated filter may appear "
                "uninfluential despite diagnostic blind spots."
            ),
            "action": (
                "Verify instrumentation and ordering before "
                "allocating optimisation research."
            ),
        }
    return {
        "title": (
            f"Test controlled sensitivity of {filter_name}"
        ),
        "question": (
            f"How sensitive are qualification and survival "
            f"outcomes to a controlled change in {filter_name} "
            "while every other decision rule remains fixed?"
        ),
        "null": (
            f"A controlled change in {filter_name} does not "
            "materially change qualification or survival outcomes."
        ),
        "alternative": (
            f"A controlled change in {filter_name} produces a "
            "material and reproducible change in qualification "
            "or survival outcomes."
        ),
        "control": (
            "Current production value and complete unchanged "
            "filter set."
        ),
        "treatment": (
            f"One controlled relaxation and one controlled "
            f"tightening of {filter_name}, with all other rules "
            "held constant."
        ),
        "risk": (
            "Observed influence may be confounded by frequent "
            "co-failures and limited sole-veto evidence."
        ),
        "action": (
            "Design a bounded sensitivity experiment; do not "
            "weaken or remove the filter operationally."
        ),
    }


def stable_candidate_id(
    candidate_type: str,
    primary_filter: str,
    secondary_filter: str,
    research_question: str,
    control_definition: str,
    treatment_definition: str,
) -> str:
    canonical = "|".join(
        (
            candidate_type.strip().upper(),
            normalise_filter_name(primary_filter),
            normalise_filter_name(secondary_filter),
            " ".join(research_question.split()),
            " ".join(control_definition.split()),
            " ".join(treatment_definition.split()),
        )
    )
    digest = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()[:10].upper()
    return (
        f"CS03-{candidate_type.strip().upper()}-"
        f"{normalise_filter_name(primary_filter)}-{digest}"
    )


def _priority_band(
    score: float,
    sample_adequacy: float,
    confounding: float,
) -> str:
    if sample_adequacy < 0.20:
        return "REPLICATION_REQUIRED"
    if confounding >= 0.85:
        return "BLOCKED_BY_CONFOUNDING"
    if score >= 0.40:
        return "HIGH_PRIORITY"
    if score >= 0.20:
        return "MEDIUM_PRIORITY"
    return "LOW_PRIORITY"


def _information_band(score: float) -> str:
    if score >= 0.60:
        return "HIGH"
    if score >= 0.35:
        return "MEDIUM"
    return "LOW"


def _cost_band(score: float) -> str:
    if score >= 0.70:
        return "HIGH"
    if score >= 0.45:
        return "MEDIUM"
    return "LOW"


def build_candidates(
    summary: pd.DataFrame,
    recommendations: pd.DataFrame,
    source_lineage: SourceLineage,
    generated_utc: datetime,
    weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    weights = dict(DEFAULT_WEIGHTS if weights is None else weights)
    prepared_summary = prepare_filter_summary(summary)
    prepared_recommendations = prepare_recommendations(
        recommendations
    )

    merged = prepared_summary.merge(
        prepared_recommendations[
            [
                "recommendation_id",
                "recommendation_type",
                "filter",
                "status",
                "reason",
                "evidence",
                "proposed_experiment",
            ]
        ],
        on="filter",
        how="left",
        validate="one_to_one",
    )

    marginal_norm = _safe_max_normalise(
        merged["marginal_influence_pct"]
    )
    sole_norm = _safe_max_normalise(
        merged["sole_vetoes"]
    )
    first_norm = _safe_max_normalise(
        merged["first_vetoes"]
    )
    participation_norm = _safe_max_normalise(
        merged["failed_row_participation_pct"]
    )

    merged["evidence_relevance_score"] = (
        weights["evidence_marginal"] * marginal_norm
        + weights["evidence_sole_veto"] * sole_norm
        + weights["evidence_first_veto"] * first_norm
        + weights["evidence_failure_participation"]
        * participation_norm
    ).clip(0.0, 1.0)

    merged["uncertainty_score"] = _uncertainty_from_count(
        merged["sole_vetoes"]
    )
    merged["sample_adequacy_score"] = _sample_adequacy(
        merged["sole_vetoes"],
        merged["first_vetoes"],
    )
    merged["hypothesis_separation_score"] = merged.apply(
        _hypothesis_separation,
        axis=1,
    )
    merged["decision_relevance_score"] = merged.apply(
        _decision_relevance,
        axis=1,
    )
    merged["feasibility_score"] = merged.apply(
        lambda row: max(
            0.25,
            1.0
            - 0.08 * float(
                row["average_cofail_count"]
            ),
        ),
        axis=1,
    )
    merged["experiment_cost_score"] = merged.apply(
        _cost_score,
        axis=1,
    )
    merged["confounding_risk_score"] = merged.apply(
        _confounding_score,
        axis=1,
    )

    merged["expected_research_value"] = (
        weights["erv_evidence"]
        * merged["evidence_relevance_score"]
        + weights["erv_uncertainty"]
        * merged["uncertainty_score"]
        + weights["erv_hypothesis_separation"]
        * merged["hypothesis_separation_score"]
        + weights["erv_decision_relevance"]
        * merged["decision_relevance_score"]
    ).clip(0.0, 1.0)

    merged["information_gain_proxy"] = (
        merged["uncertainty_score"]
        * merged["evidence_relevance_score"]
        * merged["hypothesis_separation_score"]
        * merged["sample_adequacy_score"]
    ).clip(0.0, 1.0)

    merged["priority_score"] = (
        merged["expected_research_value"]
        * merged["feasibility_score"]
        * merged["sample_adequacy_score"]
        * (
            1.0
            - weights["confounding_penalty"]
            * merged["confounding_risk_score"]
        )
        * (
            1.0
            - weights["cost_penalty"]
            * merged["experiment_cost_score"]
        )
    ).clip(0.0, 1.0)

    rows: list[dict[str, Any]] = []
    generated_text = generated_utc.astimezone(
        timezone.utc
    ).isoformat()

    for _, row in merged.iterrows():
        candidate_type = _candidate_type(row)
        texts = _candidate_text(
            row["filter"],
            candidate_type,
        )
        candidate_id = stable_candidate_id(
            candidate_type=candidate_type,
            primary_filter=row["filter"],
            secondary_filter="",
            research_question=texts["question"],
            control_definition=texts["control"],
            treatment_definition=texts["treatment"],
        )
        sample = float(row["sample_adequacy_score"])
        confounding = float(row["confounding_risk_score"])
        priority_score = float(row["priority_score"])
        priority_band = (
            "REPLICATION_REQUIRED"
            if candidate_type == "REPLICATION_EXPANSION"
            else _priority_band(
                priority_score,
                sample,
                confounding,
            )
        )

        evidence_basis = (
            f"CS02 failures={int(row['failures']):,}; "
            f"first_vetoes={int(row['first_vetoes']):,}; "
            f"sole_vetoes={int(row['sole_vetoes']):,}; "
            f"marginal_influence="
            f"{float(row['marginal_influence_pct']):.6f}%; "
            f"average_cofail_count="
            f"{float(row['average_cofail_count']):.6f}."
        )

        rows.append(
            {
                "priority_rank": 0,
                "candidate_id": candidate_id,
                "candidate_type": candidate_type,
                "primary_filter": row["filter"],
                "secondary_filter": "",
                "candidate_title": texts["title"],
                "research_question": texts["question"],
                "null_hypothesis": texts["null"],
                "alternative_hypothesis": texts["alternative"],
                "control_definition": texts["control"],
                "treatment_definition": texts["treatment"],
                "evidence_basis": evidence_basis,
                "failures": int(row["failures"]),
                "failure_pct": float(row["failure_pct"]),
                "first_vetoes": int(row["first_vetoes"]),
                "first_veto_pct": float(row["first_veto_pct"]),
                "sole_vetoes": int(row["sole_vetoes"]),
                "sole_veto_pct": float(row["sole_veto_pct"]),
                "average_cofail_count": float(
                    row["average_cofail_count"]
                ),
                "marginal_influence_pct": float(
                    row["marginal_influence_pct"]
                ),
                "evidence_relevance_score": float(
                    row["evidence_relevance_score"]
                ),
                "uncertainty_score": float(
                    row["uncertainty_score"]
                ),
                "hypothesis_separation_score": float(
                    row["hypothesis_separation_score"]
                ),
                "decision_relevance_score": float(
                    row["decision_relevance_score"]
                ),
                "feasibility_score": float(
                    row["feasibility_score"]
                ),
                "sample_adequacy_score": sample,
                "experiment_cost_score": float(
                    row["experiment_cost_score"]
                ),
                "confounding_risk_score": confounding,
                "information_gain_proxy": float(
                    row["information_gain_proxy"]
                ),
                "expected_research_value": float(
                    row["expected_research_value"]
                ),
                "priority_score": priority_score,
                "priority_band": priority_band,
                "recommendation_status": "PENDING",
                "principal_risk": texts["risk"],
                "recommended_next_action": texts["action"],
                "source_recommendation_id": str(
                    row.get("recommendation_id", "")
                    if pd.notna(row.get("recommendation_id", ""))
                    else ""
                ),
                "source_engine_id": source_lineage.engine_id,
                "source_engine_version": (
                    source_lineage.engine_version
                ),
                "source_generated_utc": (
                    source_lineage.generated_utc
                ),
                "source_summary_sha256": (
                    source_lineage.summary_sha256
                ),
                "source_recommendations_sha256": (
                    source_lineage.recommendations_sha256
                ),
                "generated_utc": generated_text,
                "engine_version": ENGINE_VERSION,
            }
        )

    candidates = pd.DataFrame(rows)
    candidates = candidates.sort_values(
        by=[
            "priority_score",
            "information_gain_proxy",
            "evidence_relevance_score",
            "primary_filter",
        ],
        ascending=(False, False, False, True),
        kind="mergesort",
    ).reset_index(drop=True)
    candidates["priority_rank"] = (
        candidates.index + 1
    )
    for column in (
        "evidence_relevance_score",
        "uncertainty_score",
        "hypothesis_separation_score",
        "decision_relevance_score",
        "feasibility_score",
        "sample_adequacy_score",
        "experiment_cost_score",
        "confounding_risk_score",
        "information_gain_proxy",
        "expected_research_value",
        "priority_score",
    ):
        candidates[column] = candidates[column].round(8)

    return candidates.loc[:, CANDIDATE_COLUMNS]


def build_priority_queue(
    candidates: pd.DataFrame,
) -> pd.DataFrame:
    queue = candidates[
        [
            "priority_rank",
            "candidate_id",
            "candidate_title",
            "candidate_type",
            "primary_filter",
            "secondary_filter",
            "research_question",
            "priority_score",
            "priority_band",
            "information_gain_proxy",
            "experiment_cost_score",
            "principal_risk",
            "recommended_next_action",
            "recommendation_status",
        ]
    ].copy()
    queue["expected_information_gain_band"] = queue[
        "information_gain_proxy"
    ].map(_information_band)
    queue["estimated_cost_band"] = queue[
        "experiment_cost_score"
    ].map(_cost_band)
    queue = queue.drop(
        columns=[
            "information_gain_proxy",
            "experiment_cost_score",
        ]
    )
    return queue.loc[:, QUEUE_COLUMNS]


def validate_candidates(
    candidates: pd.DataFrame,
) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    errors.extend(
        _require_columns(
            candidates,
            CANDIDATE_COLUMNS,
            "candidate analysis",
        )
    )
    if errors:
        return ValidationResult(
            False,
            tuple(errors),
            (),
        )

    if candidates.empty:
        errors.append("candidate analysis contains no rows")

    if candidates["candidate_id"].duplicated().any():
        errors.append("candidate IDs are not unique")

    expected_ranks = list(
        range(1, len(candidates) + 1)
    )
    if candidates["priority_rank"].tolist() != expected_ranks:
        errors.append(
            "priority_rank must be contiguous and deterministic"
        )

    score_columns = (
        "evidence_relevance_score",
        "uncertainty_score",
        "hypothesis_separation_score",
        "decision_relevance_score",
        "feasibility_score",
        "sample_adequacy_score",
        "experiment_cost_score",
        "confounding_risk_score",
        "information_gain_proxy",
        "expected_research_value",
        "priority_score",
    )
    for column in score_columns:
        numeric = pd.to_numeric(
            candidates[column],
            errors="coerce",
        )
        if numeric.isna().any():
            errors.append(
                f"candidate score {column} contains "
                "non-finite values"
            )
        elif (~numeric.between(0.0, 1.0)).any():
            errors.append(
                f"candidate score {column} must be within [0, 1]"
            )

    for column in (
        "candidate_id",
        "candidate_type",
        "primary_filter",
        "candidate_title",
        "research_question",
        "null_hypothesis",
        "alternative_hypothesis",
        "control_definition",
        "treatment_definition",
        "evidence_basis",
        "priority_band",
    ):
        if (
            candidates[column]
            .fillna("")
            .astype(str)
            .str.strip()
            .eq("")
            .any()
        ):
            errors.append(
                f"candidate column {column} contains blanks"
            )

    valid_bands = {
        "HIGH_PRIORITY",
        "MEDIUM_PRIORITY",
        "LOW_PRIORITY",
        "REPLICATION_REQUIRED",
        "BLOCKED_BY_CONFOUNDING",
    }
    invalid_bands = sorted(
        set(candidates["priority_band"]) - valid_bands
    )
    if invalid_bands:
        errors.append(
            "candidate analysis contains invalid priority bands: "
            + ", ".join(invalid_bands)
        )

    if (
        candidates["sample_adequacy_score"] < 0.20
    ).mean() > 0.50:
        warnings.append(
            "More than half of candidates are sample-limited; "
            "the queue should be treated as an evidence-expansion "
            "programme rather than parameter optimisation."
        )

    return ValidationResult(
        passed=not errors,
        errors=tuple(errors),
        warnings=tuple(warnings),
    )


def default_output_paths(
    output_directory: Path,
) -> OutputPaths:
    return OutputPaths(
        output_directory=output_directory,
        analysis_csv=(
            output_directory
            / "candidate_opportunity_analysis_latest.csv"
        ),
        priority_queue_csv=(
            output_directory
            / "experiment_priority_queue_latest.csv"
        ),
        report_txt=(
            output_directory
            / "candidate_opportunity_analysis_report_latest.txt"
        ),
        audit_json=(
            output_directory
            / "candidate_opportunity_analysis_audit_latest.json"
        ),
    )


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )
    try:
        with os.fdopen(
            file_descriptor,
            "w",
            encoding="utf-8",
            newline="\n",
        ) as file_handle:
            file_handle.write(content)
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _atomic_write_csv(
    path: Path,
    frame: pd.DataFrame,
) -> None:
    _atomic_write_text(
        path,
        frame.to_csv(index=False, lineterminator="\n"),
    )


def build_report(
    candidates: pd.DataFrame,
    validation: ValidationResult,
    source_lineage: SourceLineage,
    generated_utc: datetime,
) -> str:
    separator = "=" * 100
    lines = [
        separator,
        "BACQE CONVEX SURVIVAL CS03 - CANDIDATE OPPORTUNITY ANALYSIS",
        separator,
        f"Generated UTC:              {generated_utc.astimezone(timezone.utc).isoformat()}",
        f"Engine version:             {ENGINE_VERSION}",
        f"Source engine:              {source_lineage.engine_id} v{source_lineage.engine_version}",
        f"Candidates generated:       {len(candidates):,}",
        f"Validation:                 {'PASS' if validation.passed else 'FAIL'}",
        "",
        "INSTITUTIONAL PURPOSE",
        "-" * 100,
        (
            "CS03 prioritises scientific experiments by expected research value. "
            "It does not instruct BACQE to weaken, remove or optimise trading filters."
        ),
        (
            "The information-gain measure is an explicit heuristic proxy, not an "
            "exact Bayesian or information-theoretic quantity."
        ),
        "",
        "PRIORITY BAND COUNTS",
        "-" * 100,
    ]

    counts = (
        candidates["priority_band"]
        .value_counts()
        .sort_index()
    )
    for band, count in counts.items():
        lines.append(f"{band:<30}{int(count):>10,}")

    lines.extend(
        [
            "",
            "TOP RESEARCH OPPORTUNITIES",
            "-" * 100,
        ]
    )

    for row in candidates.head(10).itertuples(
        index=False
    ):
        lines.extend(
            [
                (
                    f"{row.priority_rank:>2}. "
                    f"{row.candidate_title}"
                ),
                f"    Candidate ID:           {row.candidate_id}",
                f"    Candidate type:         {row.candidate_type}",
                f"    Priority score:         {row.priority_score:.6f}",
                f"    Priority band:          {row.priority_band}",
                f"    Information gain proxy: {row.information_gain_proxy:.6f}",
                f"    Sample adequacy:        {row.sample_adequacy_score:.6f}",
                f"    Confounding risk:       {row.confounding_risk_score:.6f}",
                f"    Research question:      {row.research_question}",
                f"    Principal risk:         {row.principal_risk}",
                "",
            ]
        )

    lines.extend(
        [
            "VALIDATION WARNINGS",
            "-" * 100,
        ]
    )
    if validation.warnings:
        for warning in validation.warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- None.")

    lines.extend(
        [
            "",
            "SCIENTIFIC INTERPRETATION",
            "-" * 100,
            (
                "A high rank means that the candidate currently offers a favourable "
                "combination of relevance, uncertainty reduction, feasibility and "
                "experimental clarity relative to cost and confounding."
            ),
            (
                "It does not mean the associated filter is defective, unnecessary or "
                "likely to improve profitability if changed."
            ),
            separator,
            "",
        ]
    )
    return "\n".join(lines)


def build_audit(
    candidates: pd.DataFrame,
    priority_queue: pd.DataFrame,
    validation: ValidationResult,
    input_validation: ValidationResult,
    source_lineage: SourceLineage,
    paths: OutputPaths,
    generated_utc: datetime,
    weights: dict[str, float],
) -> dict[str, Any]:
    return {
        "engine_id": ENGINE_ID,
        "engine_name": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "generated_utc": generated_utc.astimezone(
            timezone.utc
        ).isoformat(),
        "source_lineage": asdict(source_lineage),
        "candidate_count": int(len(candidates)),
        "priority_queue_count": int(len(priority_queue)),
        "candidate_type_counts": {
            str(key): int(value)
            for key, value in (
                candidates["candidate_type"]
                .value_counts()
                .sort_index()
                .items()
            )
        },
        "priority_band_counts": {
            str(key): int(value)
            for key, value in (
                candidates["priority_band"]
                .value_counts()
                .sort_index()
                .items()
            )
        },
        "score_policy": {
            "description": (
                "Transparent heuristic expected-research-value "
                "policy; not a Bayesian posterior calculation."
            ),
            "weights": weights,
        },
        "input_validation": asdict(input_validation),
        "validation": asdict(validation),
        "output_paths": {
            key: str(value)
            for key, value in asdict(paths).items()
        },
    }


def run_analysis(
    summary_path: Path,
    recommendations_path: Path,
    source_audit_path: Path,
    output_directory: Path,
    generated_utc: datetime | None = None,
    weights: dict[str, float] | None = None,
) -> AnalysisOutputs:
    generated_utc = generated_utc or datetime.now(
        timezone.utc
    )
    weights = dict(DEFAULT_WEIGHTS if weights is None else weights)

    summary = pd.read_csv(summary_path)
    recommendations = pd.read_csv(
        recommendations_path
    )
    audit = load_source_audit(source_audit_path)

    input_validation = validate_inputs(
        summary,
        recommendations,
        audit,
    )
    if not input_validation.passed:
        raise OpportunityAnalysisError(
            "CS03 input validation failed: "
            + "; ".join(input_validation.errors)
        )

    source_lineage = build_source_lineage(
        summary_path=summary_path,
        recommendations_path=recommendations_path,
        audit_path=source_audit_path,
        audit=audit,
    )

    candidates = build_candidates(
        summary=summary,
        recommendations=recommendations,
        source_lineage=source_lineage,
        generated_utc=generated_utc,
        weights=weights,
    )
    output_validation = validate_candidates(candidates)
    combined_validation = ValidationResult(
        passed=output_validation.passed,
        errors=output_validation.errors,
        warnings=(
            input_validation.warnings
            + output_validation.warnings
        ),
    )
    if not combined_validation.passed:
        raise OpportunityAnalysisError(
            "CS03 output validation failed: "
            + "; ".join(combined_validation.errors)
        )

    priority_queue = build_priority_queue(candidates)
    paths = default_output_paths(output_directory)
    report = build_report(
        candidates=candidates,
        validation=combined_validation,
        source_lineage=source_lineage,
        generated_utc=generated_utc,
    )
    audit_payload = build_audit(
        candidates=candidates,
        priority_queue=priority_queue,
        validation=combined_validation,
        input_validation=input_validation,
        source_lineage=source_lineage,
        paths=paths,
        generated_utc=generated_utc,
        weights=weights,
    )

    output_directory.mkdir(parents=True, exist_ok=True)
    _atomic_write_csv(paths.analysis_csv, candidates)
    _atomic_write_csv(
        paths.priority_queue_csv,
        priority_queue,
    )
    _atomic_write_text(paths.report_txt, report)
    _atomic_write_text(
        paths.audit_json,
        json.dumps(
            audit_payload,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )

    return AnalysisOutputs(
        candidates=candidates,
        priority_queue=priority_queue,
        validation=combined_validation,
        source_lineage=source_lineage,
        paths=paths,
    )
