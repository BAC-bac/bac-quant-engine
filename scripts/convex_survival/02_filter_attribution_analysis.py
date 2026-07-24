from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from bacqe.convex_survival.analysis import (
    ENGINE_VERSION,
    build_filter_summary,
    build_outcome_funnel,
    build_recommendations,
    prepare_attribution_frame,
    run_analysis,
    validate_attribution_frame,
)

import pandas as pd


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_quant_lab_root() -> Path:
    configured = os.getenv("BACQE_QUANT_LAB")

    if configured:
        return Path(configured)

    return Path(r"E:\Quant_Lab")


def default_input_path() -> Path:
    staging_directory = (
        default_quant_lab_root()
        / "data"
        / "staging"
        / "convex_survival"
        / "filter_attribution"
    )

    parquet_files = sorted(
        staging_directory.glob("*.parquet"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    if not parquet_files:
        return (
            staging_directory
            / "filter_attribution_latest.parquet"
        )

    return parquet_files[0]


def default_output_directory() -> Path:
    return (
        default_quant_lab_root()
        / "data"
        / "analysis"
        / "convex_survival"
        / "filter_attribution_analysis"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "BACQE Convex Survival CS02 — "
            "Filter Attribution Analysis"
        )
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=default_input_path(),
        help=(
            "Path to the staged CS01 Parquet dataset."
        ),
    )

    parser.add_argument(
        "--output-directory",
        type=Path,
        default=default_output_directory(),
        help=(
            "Directory for CS02 reports and "
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


def build_self_test_frame() -> pd.DataFrame:
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
                "run_id": "SELF_TEST",
                "observed_utc": (
                    "2026-01-01T00:00:00+00:00"
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
                "run_id": "SELF_TEST",
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
                "run_id": "SELF_TEST",
                "observed_utc": (
                    "2026-01-01T02:00:00+00:00"
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
                "run_id": "SELF_TEST",
                "observed_utc": (
                    "2026-01-01T03:00:00+00:00"
                ),
            },
        ]
    )


def run_self_tests() -> None:
    frame = prepare_attribution_frame(
        build_self_test_frame()
    )

    validation = validate_attribution_frame(frame)

    assert validation.passed, validation.errors

    summary = build_filter_summary(frame)

    assert list(summary["filter"]) == [
        "EMA_SEPARATION",
        "SPREAD",
        "ATR",
    ]

    ema_row = summary.loc[
        summary["filter"].eq("EMA_SEPARATION")
    ].iloc[0]

    assert int(ema_row["failures"]) == 2
    assert int(ema_row["first_vetoes"]) == 2
    assert int(ema_row["sole_vetoes"]) == 2
    assert float(
        ema_row["marginal_influence_pct"]
    ) == 100.0

    spread_row = summary.loc[
        summary["filter"].eq("SPREAD")
    ].iloc[0]

    assert int(spread_row["failures"]) == 1
    assert int(spread_row["first_vetoes"]) == 1
    assert int(spread_row["sole_vetoes"]) == 0

    funnel = build_outcome_funnel(frame)

    all_pass_rows = funnel.loc[
        funnel["category"].eq(
            "QUALIFIED_ALL_PASS"
        ),
        "rows",
    ].iloc[0]

    near_miss_rows = funnel.loc[
        funnel["category"].eq(
            "NEAR_MISS_ONE_VETO"
        ),
        "rows",
    ].iloc[0]

    assert int(all_pass_rows) == 1
    assert int(near_miss_rows) == 2

    recommendations = build_recommendations(summary)

    assert not recommendations.empty
    assert (
        recommendations.iloc[0]["filter"]
        == "EMA_SEPARATION"
    )


def print_header() -> None:
    separator = "=" * 100

    print(separator)
    print(
        "BACQE CONVEX SURVIVAL "
        "CS02 - FILTER ATTRIBUTION ANALYSIS"
    )
    print(separator)
    print(f"Engine version:             {ENGINE_VERSION}")


def main() -> int:
    arguments = build_parser().parse_args()
    print(f"Input:                      {arguments.input}")

    print_header()

    if not arguments.skip_self_tests:
        print("Running deterministic self-tests.")

        try:
            run_self_tests()
        except Exception as exc:
            print(
                f"SELF-TEST RESULT:           FAIL\n"
                f"Reason:                     {exc}",
                file=sys.stderr,
            )
            return 1

        print("Self-tests passed.")

    print(f"Input:                      {arguments.input}")
    print(
        f"Output directory:           "
        f"{arguments.output_directory}"
    )

    try:
        outputs = run_analysis(
            source_path=arguments.input,
            output_directory=(
                arguments.output_directory
            ),
            generated_utc=datetime.now(timezone.utc),
        )
    except Exception as exc:
        print(
            f"ENGINE RESULT:              FAIL\n"
            f"Reason:                     {exc}",
            file=sys.stderr,
        )
        return 1

    print("-" * 100)
    print("ENGINE RESULT:              PASS")
    print(
        f"Rows analysed:              "
        f"{outputs.dataset_summary.rows:,}"
    )
    print(
        f"Filters identified:         "
        f"{len(outputs.filter_summary):,}"
    )
    print(
        f"Recommendations generated: "
        f"{len(outputs.recommendations):,}"
    )
    print(
        f"Summary CSV:                "
        f"{outputs.paths.summary_csv}"
    )
    print(
        f"Recommendations CSV:        "
        f"{outputs.paths.recommendations_csv}"
    )
    print(
        f"Analysis report:            "
        f"{outputs.paths.report_txt}"
    )
    print(
        f"Audit JSON:                 "
        f"{outputs.paths.audit_json}"
    )
    print("=" * 100)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())