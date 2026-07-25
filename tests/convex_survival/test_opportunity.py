from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from bacqe.convex_survival.opportunity import (
    OpportunityAnalysisError,
    SourceLineage,
    build_candidates,
    build_priority_queue,
    normalise_filter_name,
    run_analysis,
    stable_candidate_id,
    validate_candidates,
    validate_inputs,
)


def sample_summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "rank": 1,
                "filter": "HIGH_SIGNAL",
                "failures": 500,
                "failure_pct": 50.0,
                "failed_row_participation_pct": 50.0,
                "first_vetoes": 120,
                "first_veto_pct": 12.0,
                "sole_vetoes": 30,
                "sole_veto_pct": 60.0,
                "average_cofail_count": 2.0,
                "marginal_influence_pct": 6.0,
                "marginal_influence_class": "HIGH",
                "research_priority": "HIGH",
            },
            {
                "rank": 2,
                "filter": "UNCERTAIN",
                "failures": 450,
                "failure_pct": 45.0,
                "failed_row_participation_pct": 45.0,
                "first_vetoes": 130,
                "first_veto_pct": 13.0,
                "sole_vetoes": 2,
                "sole_veto_pct": 4.0,
                "average_cofail_count": 5.0,
                "marginal_influence_pct": 0.4,
                "marginal_influence_class": "LOW",
                "research_priority": "MEDIUM",
            },
            {
                "rank": 3,
                "filter": "CONFOUNDED",
                "failures": 900,
                "failure_pct": 90.0,
                "failed_row_participation_pct": 90.0,
                "first_vetoes": 100,
                "first_veto_pct": 10.0,
                "sole_vetoes": 0,
                "sole_veto_pct": 0.0,
                "average_cofail_count": 8.0,
                "marginal_influence_pct": 0.0,
                "marginal_influence_class": "NONE",
                "research_priority": "LOW",
            },
        ]
    )


def sample_recommendations() -> pd.DataFrame:
    rows = []
    for rank, filter_name in enumerate(
        ("HIGH_SIGNAL", "UNCERTAIN", "CONFOUNDED"),
        start=1,
    ):
        rows.append(
            {
                "recommendation_rank": rank,
                "recommendation_id": (
                    f"CS02-{rank:03d}-"
                    f"{filter_name.lower()}"
                ),
                "recommendation_type": (
                    "CONTROLLED_FILTER_SENSITIVITY"
                ),
                "priority": "MEDIUM",
                "filter": filter_name,
                "title": f"Test {filter_name}",
                "reason": "Synthetic evidence.",
                "evidence": "Synthetic evidence.",
                "proposed_experiment": "Controlled experiment.",
                "status": "PENDING",
            }
        )
    return pd.DataFrame(rows)


def sample_audit() -> dict[str, object]:
    return {
        "engine_id": "CS02",
        "engine_name": "Filter Attribution Analysis",
        "engine_version": "1.0.0",
        "generated_utc": (
            "2026-01-01T00:00:00+00:00"
        ),
        "source_path": "synthetic.parquet",
        "dataset_summary": {
            "rows": 1000,
            "sole_veto_rows": 32,
        },
        "validation": {
            "passed": True,
            "errors": [],
            "warnings": [],
        },
    }


def lineage() -> SourceLineage:
    return SourceLineage(
        engine_id="CS02",
        engine_name="Filter Attribution Analysis",
        engine_version="1.0.0",
        generated_utc="2026-01-01T00:00:00+00:00",
        source_path="synthetic.parquet",
        summary_sha256="a" * 64,
        recommendations_sha256="b" * 64,
        audit_sha256="c" * 64,
    )


def generated_utc() -> datetime:
    return datetime(
        2026,
        1,
        2,
        tzinfo=timezone.utc,
    )


def test_normalise_filter_name() -> None:
    assert normalise_filter_name(
        "EMA Separation"
    ) == "EMA_SEPARATION"


def test_validation_accepts_consistent_inputs() -> None:
    result = validate_inputs(
        sample_summary(),
        sample_recommendations(),
        sample_audit(),
    )
    assert result.passed
    assert result.errors == ()


def test_validation_rejects_unknown_filter() -> None:
    recommendations = sample_recommendations()
    recommendations.loc[0, "filter"] = "UNKNOWN"
    result = validate_inputs(
        sample_summary(),
        recommendations,
        sample_audit(),
    )
    assert not result.passed
    assert any(
        "absent from the summary" in error
        for error in result.errors
    )


def test_validation_rejects_non_cs02_audit() -> None:
    audit = sample_audit()
    audit["engine_id"] = "CS01"
    result = validate_inputs(
        sample_summary(),
        sample_recommendations(),
        audit,
    )
    assert not result.passed


def test_high_signal_candidate_ranks_first() -> None:
    candidates = build_candidates(
        sample_summary(),
        sample_recommendations(),
        lineage(),
        generated_utc(),
    )
    assert (
        candidates.iloc[0]["primary_filter"]
        == "HIGH_SIGNAL"
    )


def test_confounded_candidate_is_penalised() -> None:
    candidates = build_candidates(
        sample_summary(),
        sample_recommendations(),
        lineage(),
        generated_utc(),
    )
    high = candidates.loc[
        candidates["primary_filter"].eq("HIGH_SIGNAL")
    ].iloc[0]
    confounded = candidates.loc[
        candidates["primary_filter"].eq("CONFOUNDED")
    ].iloc[0]
    assert (
        confounded["confounding_risk_score"]
        > high["confounding_risk_score"]
    )
    assert (
        confounded["priority_score"]
        < high["priority_score"]
    )


def test_small_marginal_sample_requires_replication() -> None:
    candidates = build_candidates(
        sample_summary(),
        sample_recommendations(),
        lineage(),
        generated_utc(),
    )
    uncertain = candidates.loc[
        candidates["primary_filter"].eq("UNCERTAIN")
    ].iloc[0]
    assert uncertain["candidate_type"] == (
        "REPLICATION_EXPANSION"
    )
    assert uncertain["priority_band"] == (
        "REPLICATION_REQUIRED"
    )


def test_candidate_ids_are_stable() -> None:
    first = build_candidates(
        sample_summary(),
        sample_recommendations(),
        lineage(),
        generated_utc(),
    )
    second = build_candidates(
        sample_summary(),
        sample_recommendations(),
        lineage(),
        generated_utc(),
    )
    assert first["candidate_id"].tolist() == (
        second["candidate_id"].tolist()
    )


def test_candidate_id_does_not_depend_on_rank() -> None:
    candidate_id = stable_candidate_id(
        candidate_type="THRESHOLD_SENSITIVITY",
        primary_filter="ADX",
        secondary_filter="",
        research_question="Does ADX matter?",
        control_definition="Baseline.",
        treatment_definition="Controlled change.",
    )
    assert candidate_id.startswith(
        "CS03-THRESHOLD_SENSITIVITY-ADX-"
    )


def test_output_validation_accepts_candidates() -> None:
    candidates = build_candidates(
        sample_summary(),
        sample_recommendations(),
        lineage(),
        generated_utc(),
    )
    result = validate_candidates(candidates)
    assert result.passed


def test_priority_queue_preserves_candidate_identity() -> None:
    candidates = build_candidates(
        sample_summary(),
        sample_recommendations(),
        lineage(),
        generated_utc(),
    )
    queue = build_priority_queue(candidates)
    assert queue["candidate_id"].tolist() == (
        candidates["candidate_id"].tolist()
    )


def test_run_analysis_writes_outputs(tmp_path) -> None:
    summary_path = tmp_path / "summary.csv"
    recommendations_path = tmp_path / "recommendations.csv"
    audit_path = tmp_path / "audit.json"
    output_directory = tmp_path / "outputs"

    sample_summary().to_csv(summary_path, index=False)
    sample_recommendations().to_csv(
        recommendations_path,
        index=False,
    )
    audit_path.write_text(
        json.dumps(sample_audit()),
        encoding="utf-8",
    )

    outputs = run_analysis(
        summary_path=summary_path,
        recommendations_path=recommendations_path,
        source_audit_path=audit_path,
        output_directory=output_directory,
        generated_utc=generated_utc(),
    )

    assert outputs.validation.passed
    assert outputs.paths.analysis_csv.exists()
    assert outputs.paths.priority_queue_csv.exists()
    assert outputs.paths.report_txt.exists()
    assert outputs.paths.audit_json.exists()


def test_run_analysis_rejects_invalid_input(tmp_path) -> None:
    summary_path = tmp_path / "summary.csv"
    recommendations_path = tmp_path / "recommendations.csv"
    audit_path = tmp_path / "audit.json"

    invalid = sample_summary().drop(
        columns=["sole_vetoes"]
    )
    invalid.to_csv(summary_path, index=False)
    sample_recommendations().to_csv(
        recommendations_path,
        index=False,
    )
    audit_path.write_text(
        json.dumps(sample_audit()),
        encoding="utf-8",
    )

    with pytest.raises(OpportunityAnalysisError):
        run_analysis(
            summary_path=summary_path,
            recommendations_path=recommendations_path,
            source_audit_path=audit_path,
            output_directory=tmp_path / "outputs",
            generated_utc=generated_utc(),
        )
