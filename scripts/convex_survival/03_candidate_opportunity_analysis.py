from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from bacqe.convex_survival.opportunity import (
    ENGINE_VERSION,
    SourceLineage,
    build_candidates,
    build_priority_queue,
    run_analysis,
    validate_candidates,
    validate_inputs,
)


def default_quant_lab_root() -> Path:
    configured = os.getenv("BACQE_QUANT_LAB")
    if configured:
        return Path(configured)
    return Path(r"E:\Quant_Lab")


def default_cs02_directory() -> Path:
    return (
        default_quant_lab_root()
        / "data"
        / "analysis"
        / "convex_survival"
        / "filter_attribution_analysis"
    )


def default_output_directory() -> Path:
    return (
        default_quant_lab_root()
        / "data"
        / "analysis"
        / "convex_survival"
        / "candidate_opportunity_analysis"
    )


def build_parser() -> argparse.ArgumentParser:
    source_directory = default_cs02_directory()
    parser = argparse.ArgumentParser(
        description=(
            "BACQE Convex Survival CS03 — "
            "Candidate Opportunity Analysis"
        )
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=(
            source_directory
            / "filter_attribution_summary_latest.csv"
        ),
        help="Path to the CS02 filter summary CSV.",
    )
    parser.add_argument(
        "--recommendations",
        type=Path,
        default=(
            source_directory
            / "experiment_candidates_latest.csv"
        ),
        help=(
            "Path to the CS02 evidence-derived "
            "recommendation CSV."
        ),
    )
    parser.add_argument(
        "--source-audit",
        type=Path,
        default=(
            source_directory
            / "filter_attribution_analysis_audit_latest.json"
        ),
        help="Path to the CS02 audit JSON.",
    )
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=default_output_directory(),
        help=(
            "Directory for CS03 reports and "
            "machine-readable outputs."
        ),
    )
    parser.add_argument(
        "--skip-self-tests",
        action="store_true",
        help=(
            "Skip deterministic internal self-tests. "
            "Not recommended for normal operation."
        ),
    )
    return parser


def _self_test_summary() -> pd.DataFrame:
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


def _self_test_recommendations() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "recommendation_rank": 1,
                "recommendation_id": "CS02-001-high-signal",
                "recommendation_type": (
                    "CONTROLLED_FILTER_SENSITIVITY"
                ),
                "priority": "HIGH",
                "filter": "HIGH_SIGNAL",
                "title": "Test HIGH_SIGNAL",
                "reason": "Synthetic self-test evidence.",
                "evidence": "Synthetic.",
                "proposed_experiment": "Controlled test.",
                "status": "PENDING",
            },
            {
                "recommendation_rank": 2,
                "recommendation_id": "CS02-002-confounded",
                "recommendation_type": (
                    "CONTROLLED_FILTER_SENSITIVITY"
                ),
                "priority": "LOW",
                "filter": "CONFOUNDED",
                "title": "Test CONFOUNDED",
                "reason": "Synthetic self-test evidence.",
                "evidence": "Synthetic.",
                "proposed_experiment": "Interaction test.",
                "status": "PENDING",
            },
        ]
    )


def _self_test_audit() -> dict[str, object]:
    return {
        "engine_id": "CS02",
        "engine_name": "Filter Attribution Analysis",
        "engine_version": "1.0.0",
        "generated_utc": (
            "2026-01-01T00:00:00+00:00"
        ),
        "dataset_summary": {
            "rows": 1000,
            "sole_veto_rows": 30,
        },
        "validation": {
            "passed": True,
            "errors": [],
            "warnings": [],
        },
    }


def run_self_tests() -> None:
    summary = _self_test_summary()
    recommendations = _self_test_recommendations()
    audit = _self_test_audit()

    input_validation = validate_inputs(
        summary,
        recommendations,
        audit,
    )
    assert input_validation.passed, (
        input_validation.errors
    )

    lineage = SourceLineage(
        engine_id="CS02",
        engine_name="Filter Attribution Analysis",
        engine_version="1.0.0",
        generated_utc=(
            "2026-01-01T00:00:00+00:00"
        ),
        source_path="SELF_TEST",
        summary_sha256="a" * 64,
        recommendations_sha256="b" * 64,
        audit_sha256="c" * 64,
    )
    generated_utc = datetime(
        2026,
        1,
        2,
        tzinfo=timezone.utc,
    )
    candidates = build_candidates(
        summary,
        recommendations,
        lineage,
        generated_utc,
    )
    validation = validate_candidates(candidates)
    assert validation.passed, validation.errors
    assert (
        candidates.iloc[0]["primary_filter"]
        == "HIGH_SIGNAL"
    )
    assert (
        candidates.iloc[1]["priority_band"]
        in {
            "REPLICATION_REQUIRED",
            "BLOCKED_BY_CONFOUNDING",
        }
    )

    repeated = build_candidates(
        summary,
        recommendations,
        lineage,
        generated_utc,
    )
    pd.testing.assert_frame_equal(
        candidates,
        repeated,
    )
    queue = build_priority_queue(candidates)
    assert len(queue) == len(candidates)
    assert queue["candidate_id"].is_unique


def print_header() -> None:
    separator = "=" * 100
    print(separator)
    print(
        "BACQE CONVEX SURVIVAL "
        "CS03 - CANDIDATE OPPORTUNITY ANALYSIS"
    )
    print(separator)
    print(f"Engine version:             {ENGINE_VERSION}")


def main() -> int:
    arguments = build_parser().parse_args()
    print_header()

    if not arguments.skip_self_tests:
        print("Running deterministic self-tests.")
        try:
            run_self_tests()
        except Exception as exc:
            print(
                "SELF-TEST RESULT:           FAIL\n"
                f"Reason:                     {exc}",
                file=sys.stderr,
            )
            return 1
        print("Self-tests passed.")

    print(f"Summary input:              {arguments.summary}")
    print(
        "Recommendations input:      "
        f"{arguments.recommendations}"
    )
    print(
        f"Source audit:               "
        f"{arguments.source_audit}"
    )
    print(
        f"Output directory:           "
        f"{arguments.output_directory}"
    )

    try:
        outputs = run_analysis(
            summary_path=arguments.summary,
            recommendations_path=(
                arguments.recommendations
            ),
            source_audit_path=arguments.source_audit,
            output_directory=(
                arguments.output_directory
            ),
            generated_utc=datetime.now(timezone.utc),
        )
    except Exception as exc:
        print(
            "ENGINE RESULT:              FAIL\n"
            f"Reason:                     {exc}",
            file=sys.stderr,
        )
        return 1

    print("-" * 100)
    print("ENGINE RESULT:              PASS")
    print(
        f"Candidates generated:       "
        f"{len(outputs.candidates):,}"
    )
    print(
        f"Priority queue rows:        "
        f"{len(outputs.priority_queue):,}"
    )
    print(
        f"Candidate analysis CSV:     "
        f"{outputs.paths.analysis_csv}"
    )
    print(
        f"Priority queue CSV:         "
        f"{outputs.paths.priority_queue_csv}"
    )
    print(
        f"Analysis report:            "
        f"{outputs.paths.report_txt}"
    )
    print(
        f"Audit JSON:                 "
        f"{outputs.paths.audit_json}"
    )
    if outputs.validation.warnings:
        print("Warnings:")
        for warning in outputs.validation.warnings:
            print(f"  - {warning}")
    print("=" * 100)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
