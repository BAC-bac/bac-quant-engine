from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from bacqe.convex_survival.analysis import (
    build_filter_interactions,
    build_filter_summary,
    build_outcome_funnel,
    build_recommendations,
    parse_vetoes,
    prepare_attribution_frame,
    run_analysis,
    validate_attribution_frame,
)


def sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "all_pass": True,
                "fail_count": 0,
                "sole_veto": "",
                "first_veto": "",
                "all_vetoes": "",
                "symbol": "EURUSD",
                "timeframe": "PERIOD_H1",
                "run_id": "TEST_RUN",
                "observed_utc": (
                    "2026-01-01T00:00:00+00:00"
                ),
            },
            {
                "all_pass": False,
                "fail_count": 1,
                "sole_veto": "EMA Separation",
                "first_veto": "EMA Separation",
                "all_vetoes": "EMA Separation",
                "symbol": "EURUSD",
                "timeframe": "PERIOD_H1",
                "run_id": "TEST_RUN",
                "observed_utc": (
                    "2026-01-01T01:00:00+00:00"
                ),
            },
            {
                "all_pass": False,
                "fail_count": 2,
                "sole_veto": "",
                "first_veto": "SPREAD",
                "all_vetoes": "SPREAD|ATR",
                "symbol": "EURUSD",
                "timeframe": "PERIOD_H1",
                "run_id": "TEST_RUN",
                "observed_utc": (
                    "2026-01-01T02:00:00+00:00"
                ),
            },
            {
                "all_pass": False,
                "fail_count": 1,
                "sole_veto": "EMA_SEPARATION",
                "first_veto": "EMA_SEPARATION",
                "all_vetoes": "EMA_SEPARATION",
                "symbol": "EURUSD",
                "timeframe": "PERIOD_H1",
                "run_id": "TEST_RUN",
                "observed_utc": (
                    "2026-01-01T03:00:00+00:00"
                ),
            },
        ]
    )


def test_parse_vetoes_normalizes_delimiters() -> None:
    assert parse_vetoes(
        "EMA Separation|ATR,Spread"
    ) == (
        "EMA_SEPARATION",
        "ATR",
        "SPREAD",
    )


def test_validation_accepts_consistent_frame() -> None:
    prepared = prepare_attribution_frame(
        sample_frame()
    )

    result = validate_attribution_frame(prepared)

    assert result.passed
    assert result.errors == ()


def test_validation_rejects_fail_count_mismatch() -> None:
    frame = sample_frame()
    frame.loc[1, "fail_count"] = 2

    prepared = prepare_attribution_frame(frame)
    result = validate_attribution_frame(prepared)

    assert not result.passed
    assert any(
        "fail_count inconsistent" in error
        for error in result.errors
    )


def test_filter_summary_measures_marginal_influence() -> None:
    prepared = prepare_attribution_frame(
        sample_frame()
    )

    summary = build_filter_summary(prepared)

    ema = summary.loc[
        summary["filter"].eq("EMA_SEPARATION")
    ].iloc[0]

    assert int(ema["failures"]) == 2
    assert int(ema["first_vetoes"]) == 2
    assert int(ema["sole_vetoes"]) == 2
    assert ema["marginal_influence_pct"] == 100.0
    assert ema["marginal_influence_class"] == "LOW"


def test_filter_interactions_count_pairs() -> None:
    prepared = prepare_attribution_frame(
        sample_frame()
    )

    interactions = build_filter_interactions(
        prepared
    )

    interaction = interactions.iloc[0]

    assert interaction["filter_a"] == "ATR"
    assert interaction["filter_b"] == "SPREAD"
    assert int(interaction["cofailure_rows"]) == 1


def test_outcome_funnel_is_complete() -> None:
    prepared = prepare_attribution_frame(
        sample_frame()
    )

    funnel = build_outcome_funnel(prepared)

    assert int(
        funnel.loc[
            funnel["category"].eq(
                "ALL_OBSERVATIONS"
            ),
            "rows",
        ].iloc[0]
    ) == 4

    assert int(
        funnel.loc[
            funnel["category"].eq(
                "QUALIFIED_ALL_PASS"
            ),
            "rows",
        ].iloc[0]
    ) == 1

    assert int(
        funnel.loc[
            funnel["category"].eq(
                "NEAR_MISS_ONE_VETO"
            ),
            "rows",
        ].iloc[0]
    ) == 2


def test_recommendations_are_deterministic() -> None:
    prepared = prepare_attribution_frame(
        sample_frame()
    )

    summary = build_filter_summary(prepared)
    recommendations = build_recommendations(summary)

    assert recommendations.iloc[0]["filter"] == (
        "EMA_SEPARATION"
    )

    assert recommendations.iloc[0][
        "recommendation_id"
    ] == "CS02-001-ema-separation"


def test_run_analysis_writes_outputs(
    tmp_path,
) -> None:
    source_path = tmp_path / "source.parquet"
    output_directory = tmp_path / "outputs"

    sample_frame().to_parquet(
        source_path,
        index=False,
    )

    outputs = run_analysis(
        source_path=source_path,
        output_directory=output_directory,
        generated_utc=datetime(
            2026,
            1,
            2,
            tzinfo=timezone.utc,
        ),
    )

    assert outputs.validation.passed
    assert outputs.paths.summary_csv.exists()
    assert outputs.paths.recommendations_csv.exists()
    assert outputs.paths.report_txt.exists()
    assert outputs.paths.audit_json.exists()

    written_summary = pd.read_csv(
        outputs.paths.summary_csv
    )

    assert not written_summary.empty