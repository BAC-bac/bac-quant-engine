from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd


ENGINE_ID = "CS02"
ENGINE_NAME = "Filter Attribution Analysis"
ENGINE_VERSION = "1.0.0"

REQUIRED_COLUMNS = {
    "all_pass",
    "fail_count",
    "sole_veto",
    "first_veto",
    "all_vetoes",
}

TIMESTAMP_CANDIDATES = (
    "observed_utc",
    "bar_time",
    "timestamp",
    "time",
    "datetime",
)

RUN_ID_CANDIDATES = (
    "run_id",
    "research_run_id",
    "experiment_id",
)

SYMBOL_CANDIDATES = (
    "symbol",
    "instrument",
)

TIMEFRAME_CANDIDATES = (
    "timeframe",
    "period",
)


@dataclass(frozen=True)
class AnalysisPaths:
    output_directory: Path
    summary_csv: Path
    recommendations_csv: Path
    report_txt: Path
    audit_json: Path


@dataclass(frozen=True)
class DatasetSummary:
    rows: int
    columns: int
    failed_rows: int
    all_pass_rows: int
    sole_veto_rows: int
    multi_veto_rows: int
    earliest_timestamp: str | None
    latest_timestamp: str | None
    run_ids: tuple[str, ...]
    symbols: tuple[str, ...]
    timeframes: tuple[str, ...]


@dataclass(frozen=True)
class ValidationResult:
    passed: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class EngineOutputs:
    paths: AnalysisPaths
    dataset_summary: DatasetSummary
    filter_summary: pd.DataFrame
    recommendations: pd.DataFrame
    outcome_funnel: pd.DataFrame
    validation: ValidationResult


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def build_analysis_paths(output_directory: Path) -> AnalysisPaths:
    output_directory = output_directory.resolve()

    return AnalysisPaths(
        output_directory=output_directory,
        summary_csv=output_directory
        / "filter_attribution_summary_latest.csv",
        recommendations_csv=output_directory
        / "experiment_candidates_latest.csv",
        report_txt=output_directory
        / "filter_attribution_analysis_report_latest.txt",
        audit_json=output_directory
        / "filter_attribution_analysis_audit_latest.json",
    )


def coerce_bool_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)

    true_values = {
        "1",
        "true",
        "t",
        "yes",
        "y",
        "pass",
        "passed",
    }

    false_values = {
        "0",
        "false",
        "f",
        "no",
        "n",
        "fail",
        "failed",
        "",
        "none",
        "nan",
        "null",
    }

    def convert(value: Any) -> bool:
        if pd.isna(value):
            return False

        if isinstance(value, bool):
            return value

        if isinstance(value, (int, float)):
            return bool(value)

        normalized = str(value).strip().lower()

        if normalized in true_values:
            return True

        if normalized in false_values:
            return False

        raise ValueError(
            f"Cannot interpret value as boolean: {value!r}"
        )

    return series.map(convert).astype(bool)


def normalize_filter_name(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""

    normalized = str(value).strip()

    if not normalized:
        return ""

    normalized = normalized.upper()
    normalized = re.sub(r"[\s\-]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)

    return normalized.strip("_")


def parse_vetoes(value: Any) -> tuple[str, ...]:
    if value is None or pd.isna(value):
        return ()

    if isinstance(value, (list, tuple, set)):
        raw_values: Iterable[Any] = value
    else:
        text = str(value).strip()

        if not text:
            return ()

        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)

                if isinstance(parsed, list):
                    raw_values = parsed
                else:
                    raw_values = [text]
            except json.JSONDecodeError:
                raw_values = re.split(r"[|,;]+", text)
        else:
            raw_values = re.split(r"[|,;]+", text)

    normalized = [
        normalize_filter_name(item)
        for item in raw_values
    ]

    return tuple(
        dict.fromkeys(
            item
            for item in normalized
            if item
        )
    )


def prepare_attribution_frame(frame: pd.DataFrame) -> pd.DataFrame:
    missing = REQUIRED_COLUMNS.difference(frame.columns)

    if missing:
        raise ValueError(
            "Required CS01 columns are missing: "
            + ", ".join(sorted(missing))
        )

    prepared = frame.copy()

    prepared["all_pass"] = coerce_bool_series(prepared["all_pass"])

    prepared["_sole_veto_normalized"] = (prepared["sole_veto"].map(normalize_filter_name))

    prepared["fail_count"] = pd.to_numeric(prepared["fail_count"], errors="coerce", )

    prepared["_first_veto_normalized"] = (prepared["first_veto"].map(normalize_filter_name))

    prepared["_veto_tuple"] = (prepared["all_vetoes"].map(parse_vetoes))

    prepared["_parsed_fail_count"] = (prepared["_veto_tuple"].map(len).astype("int64"))

    return prepared


def validate_attribution_frame(
    frame: pd.DataFrame,
) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    if frame.empty:
        errors.append("Input dataset is empty.")

        return ValidationResult(
            passed=False,
            errors=tuple(errors),
            warnings=tuple(warnings),
        )

    if frame["fail_count"].isna().any():
        count = int(frame["fail_count"].isna().sum())
        errors.append(
            f"{count:,} rows have a non-numeric fail_count."
        )

    numeric_fail_count = (
        frame["fail_count"]
        .fillna(-1)
        .astype("int64")
    )

    negative_fail_count = numeric_fail_count.lt(0)

    if negative_fail_count.any():
        errors.append(
            f"{int(negative_fail_count.sum()):,} rows have "
            "a negative fail_count."
        )

    mismatch_count = numeric_fail_count.ne(
        frame["_parsed_fail_count"]
    )

    if mismatch_count.any():
        errors.append(
            f"{int(mismatch_count.sum()):,} rows have a "
            "fail_count inconsistent with all_vetoes."
        )

    all_pass_mismatch = frame["all_pass"].ne(
        numeric_fail_count.eq(0)
    )

    if all_pass_mismatch.any():
        errors.append(
            f"{int(all_pass_mismatch.sum()):,} rows violate "
            "the all_pass ↔ fail_count == 0 invariant."
        )

    single_veto_missing_name = (numeric_fail_count.eq(1) & frame["_sole_veto_normalized"].eq(""))

    if single_veto_missing_name.any():
        errors.append(f"{int(single_veto_missing_name.sum()):,} rows with "
                      "fail_count == 1 do not identify a sole_veto.")

    non_single_veto_with_name = (numeric_fail_count.ne(1) & frame["_sole_veto_normalized"].ne(""))

    if non_single_veto_with_name.any():
        errors.append(f"{int(non_single_veto_with_name.sum()):,} rows with "
                      "fail_count != 1 unexpectedly identify a sole_veto.")

    sole_veto_not_in_all_vetoes = frame.apply(
        lambda row: (row["_parsed_fail_count"] == 1 and row["_sole_veto_normalized"] not in row["_veto_tuple"]),
        axis=1, )

    if sole_veto_not_in_all_vetoes.any():
        errors.append(f"{int(sole_veto_not_in_all_vetoes.sum()):,} rows "
                      "have a sole_veto absent from all_vetoes.")

    sole_veto_not_equal_first_veto = (
            numeric_fail_count.eq(1) & frame["_sole_veto_normalized"].ne(frame["_first_veto_normalized"]))

    if sole_veto_not_equal_first_veto.any():
        errors.append(f"{int(sole_veto_not_equal_first_veto.sum()):,} "
                      "single-veto rows have different sole_veto and "
                      "first_veto values.")

    failed_without_first_veto = (
        numeric_fail_count.gt(0)
        & frame["_first_veto_normalized"].eq("")
    )

    if failed_without_first_veto.any():
        errors.append(
            f"{int(failed_without_first_veto.sum()):,} failed "
            "rows do not identify a first_veto."
        )

    passing_with_first_veto = (
        numeric_fail_count.eq(0)
        & frame["_first_veto_normalized"].ne("")
    )

    if passing_with_first_veto.any():
        errors.append(
            f"{int(passing_with_first_veto.sum()):,} passing "
            "rows unexpectedly identify a first_veto."
        )

    first_veto_not_in_all_vetoes = frame.apply(
        lambda row: (
            row["_parsed_fail_count"] > 0
            and row["_first_veto_normalized"]
            not in row["_veto_tuple"]
        ),
        axis=1,
    )

    if first_veto_not_in_all_vetoes.any():
        errors.append(
            f"{int(first_veto_not_in_all_vetoes.sum()):,} rows "
            "have a first_veto absent from all_vetoes."
        )

    duplicate_index_count = int(frame.index.duplicated().sum())

    if duplicate_index_count:
        warnings.append(
            f"The DataFrame index contains "
            f"{duplicate_index_count:,} duplicates."
        )

    return ValidationResult(
        passed=not errors,
        errors=tuple(errors),
        warnings=tuple(warnings),
    )


def safe_percentage(
    numerator: int | float,
    denominator: int | float,
) -> float:
    if denominator == 0:
        return 0.0

    return float(numerator) / float(denominator) * 100.0


def classify_marginal_influence(
    sole_vetoes: int,
    failures: int,
    marginal_influence_pct: float,
) -> str:
    if sole_vetoes == 0:
        return "NONE"

    if sole_vetoes >= 10 and marginal_influence_pct >= 10.0:
        return "HIGH"

    if sole_vetoes >= 3 and marginal_influence_pct >= 3.0:
        return "MEDIUM"

    return "LOW"


def classify_research_priority(
    sole_vetoes: int,
    first_vetoes: int,
    marginal_influence_pct: float,
) -> str:
    if sole_vetoes >= 10 and marginal_influence_pct >= 10.0:
        return "HIGH"

    if sole_vetoes >= 3:
        return "MEDIUM"

    if first_vetoes > 0:
        return "LOW"

    return "OBSERVE"


def build_filter_summary(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    total_rows = len(frame)
    failed_rows = int(frame["_parsed_fail_count"].gt(0).sum())
    first_veto_total = int(
        frame["_first_veto_normalized"].ne("").sum()
    )
    sole_veto_total = int(
        frame["_parsed_fail_count"].eq(1).sum()
    )

    filter_names = sorted(
        {
            filter_name
            for vetoes in frame["_veto_tuple"]
            for filter_name in vetoes
        }
        | {
            value
            for value in frame["_first_veto_normalized"]
            if value
        }
    )

    rows: list[dict[str, Any]] = []

    for filter_name in filter_names:
        contains_filter = frame["_veto_tuple"].map(
            lambda vetoes: filter_name in vetoes
        )

        first_veto_mask = (
            frame["_first_veto_normalized"]
            .eq(filter_name)
        )

        sole_veto_mask = (
            frame["_parsed_fail_count"].eq(1)
            & contains_filter
        )

        failures = int(contains_filter.sum())
        first_vetoes = int(first_veto_mask.sum())
        sole_vetoes = int(sole_veto_mask.sum())

        if failures:
            average_cofail_count = float(
                (
                    frame.loc[
                        contains_filter,
                        "_parsed_fail_count",
                    ]
                    - 1
                ).mean()
            )
        else:
            average_cofail_count = 0.0

        failure_pct = safe_percentage(
            failures,
            total_rows,
        )

        failed_row_participation_pct = safe_percentage(
            failures,
            failed_rows,
        )

        first_veto_pct = safe_percentage(
            first_vetoes,
            first_veto_total,
        )

        sole_veto_pct = safe_percentage(
            sole_vetoes,
            sole_veto_total,
        )

        marginal_influence_pct = safe_percentage(
            sole_vetoes,
            failures,
        )

        influence_class = classify_marginal_influence(
            sole_vetoes=sole_vetoes,
            failures=failures,
            marginal_influence_pct=marginal_influence_pct,
        )

        research_priority = classify_research_priority(
            sole_vetoes=sole_vetoes,
            first_vetoes=first_vetoes,
            marginal_influence_pct=marginal_influence_pct,
        )

        rows.append(
            {
                "filter": filter_name,
                "failures": failures,
                "failure_pct": failure_pct,
                "failed_row_participation_pct": (
                    failed_row_participation_pct
                ),
                "first_vetoes": first_vetoes,
                "first_veto_pct": first_veto_pct,
                "sole_vetoes": sole_vetoes,
                "sole_veto_pct": sole_veto_pct,
                "average_cofail_count": average_cofail_count,
                "marginal_influence_pct": (
                    marginal_influence_pct
                ),
                "marginal_influence_class": influence_class,
                "research_priority": research_priority,
            }
        )

    columns = [
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
    ]

    summary = pd.DataFrame(rows, columns=columns)

    if summary.empty:
        return summary

    priority_order = {
        "HIGH": 0,
        "MEDIUM": 1,
        "LOW": 2,
        "OBSERVE": 3,
    }

    summary["_priority_order"] = (
        summary["research_priority"]
        .map(priority_order)
        .fillna(99)
    )

    summary = (
        summary.sort_values(
            by=[
                "_priority_order",
                "sole_vetoes",
                "marginal_influence_pct",
                "first_vetoes",
                "failures",
                "filter",
            ],
            ascending=[
                True,
                False,
                False,
                False,
                False,
                True,
            ],
            kind="mergesort",
        )
        .drop(columns="_priority_order")
        .reset_index(drop=True)
    )

    summary.insert(
        0,
        "rank",
        range(1, len(summary) + 1),
    )

    return summary


def build_filter_interactions(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    pair_counts: dict[tuple[str, str], int] = {}

    for vetoes in frame["_veto_tuple"]:
        unique_vetoes = sorted(set(vetoes))

        for left_index, left_filter in enumerate(
            unique_vetoes
        ):
            for right_filter in unique_vetoes[
                left_index + 1 :
            ]:
                pair = (left_filter, right_filter)
                pair_counts[pair] = (
                    pair_counts.get(pair, 0) + 1
                )

    rows = [
        {
            "filter_a": pair[0],
            "filter_b": pair[1],
            "cofailure_rows": count,
        }
        for pair, count in pair_counts.items()
    ]

    interactions = pd.DataFrame(
        rows,
        columns=[
            "filter_a",
            "filter_b",
            "cofailure_rows",
        ],
    )

    if interactions.empty:
        return interactions

    return interactions.sort_values(
        by=[
            "cofailure_rows",
            "filter_a",
            "filter_b",
        ],
        ascending=[False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)


def build_outcome_funnel(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    total_rows = len(frame)
    fail_counts = frame["_parsed_fail_count"]

    categories = [
        (
            "ALL_OBSERVATIONS",
            total_rows,
            "All observations supplied by CS01.",
        ),
        (
            "QUALIFIED_ALL_PASS",
            int(fail_counts.eq(0).sum()),
            "No decision filters vetoed the observation.",
        ),
        (
            "NEAR_MISS_ONE_VETO",
            int(fail_counts.eq(1).sum()),
            "Exactly one filter prevented qualification.",
        ),
        (
            "TWO_VETOES",
            int(fail_counts.eq(2).sum()),
            "Exactly two filters vetoed the observation.",
        ),
        (
            "THREE_OR_MORE_VETOES",
            int(fail_counts.ge(3).sum()),
            "Three or more filters vetoed the observation.",
        ),
    ]

    rows = []

    for stage_order, (
        category,
        count,
        interpretation,
    ) in enumerate(categories, start=1):
        rows.append(
            {
                "stage_order": stage_order,
                "category": category,
                "rows": count,
                "pct_of_total": safe_percentage(
                    count,
                    total_rows,
                ),
                "interpretation": interpretation,
            }
        )

    return pd.DataFrame(rows)


def first_nonempty_values(
    frame: pd.DataFrame,
    candidates: Sequence[str],
) -> tuple[str, ...]:
    for column in candidates:
        if column not in frame.columns:
            continue

        values = sorted(
            {
                str(value).strip()
                for value in frame[column].dropna()
                if str(value).strip()
            }
        )

        if values:
            return tuple(values)

    return ()


def detect_timestamp_range(
    frame: pd.DataFrame,
) -> tuple[str | None, str | None]:
    for column in TIMESTAMP_CANDIDATES:
        if column not in frame.columns:
            continue

        parsed = pd.to_datetime(
            frame[column],
            utc=True,
            errors="coerce",
        ).dropna()

        if parsed.empty:
            continue

        return (
            parsed.min().isoformat(),
            parsed.max().isoformat(),
        )

    return None, None


def build_dataset_summary(
    frame: pd.DataFrame,
) -> DatasetSummary:
    earliest_timestamp, latest_timestamp = (
        detect_timestamp_range(frame)
    )

    fail_counts = frame["_parsed_fail_count"]

    return DatasetSummary(
        rows=len(frame),
        columns=len(
            [
                column
                for column in frame.columns
                if not column.startswith("_")
            ]
        ),
        failed_rows=int(fail_counts.gt(0).sum()),
        all_pass_rows=int(fail_counts.eq(0).sum()),
        sole_veto_rows=int(fail_counts.eq(1).sum()),
        multi_veto_rows=int(fail_counts.ge(2).sum()),
        earliest_timestamp=earliest_timestamp,
        latest_timestamp=latest_timestamp,
        run_ids=first_nonempty_values(
            frame,
            RUN_ID_CANDIDATES,
        ),
        symbols=first_nonempty_values(
            frame,
            SYMBOL_CANDIDATES,
        ),
        timeframes=first_nonempty_values(
            frame,
            TIMEFRAME_CANDIDATES,
        ),
    )


def build_recommendations(
    filter_summary: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
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
    ]

    if filter_summary.empty:
        return pd.DataFrame(columns=columns)

    eligible = filter_summary[
        filter_summary["research_priority"].isin(
            ["HIGH", "MEDIUM", "LOW"]
        )
    ].copy()

    rows: list[dict[str, Any]] = []

    for recommendation_rank, row in enumerate(
        eligible.itertuples(index=False),
        start=1,
    ):
        filter_slug = re.sub(
            r"[^a-z0-9]+",
            "-",
            row.filter.lower(),
        ).strip("-")

        recommendation_id = (
            f"CS02-{recommendation_rank:03d}-"
            f"{filter_slug}"
        )

        title = (
            f"Evaluate the marginal contribution of "
            f"{row.filter}"
        )

        reason = (
            f"{row.filter} recorded "
            f"{row.sole_vetoes:,} sole vetoes and was the "
            f"first veto on {row.first_vetoes:,} observations."
        )

        evidence = (
            f"Failures={row.failures:,}; "
            f"failure_pct={row.failure_pct:.4f}%; "
            f"sole_vetoes={row.sole_vetoes:,}; "
            f"marginal_influence="
            f"{row.marginal_influence_pct:.4f}%; "
            f"average_cofail_count="
            f"{row.average_cofail_count:.4f}."
        )

        proposed_experiment = (
            f"Run a controlled threshold sensitivity experiment "
            f"for {row.filter}, holding every other decision "
            f"filter constant. Compare qualification count, "
            f"trade outcome quality, drawdown, tail loss and "
            f"survival characteristics against the baseline."
        )

        rows.append(
            {
                "recommendation_rank": (
                    recommendation_rank
                ),
                "recommendation_id": recommendation_id,
                "recommendation_type": (
                    "CONTROLLED_FILTER_SENSITIVITY"
                ),
                "priority": row.research_priority,
                "filter": row.filter,
                "title": title,
                "reason": reason,
                "evidence": evidence,
                "proposed_experiment": proposed_experiment,
                "status": "PENDING",
            }
        )

    return pd.DataFrame(rows, columns=columns)


def format_collection(values: Sequence[str]) -> str:
    if not values:
        return "Not available"

    return ", ".join(values)


def format_optional(value: str | None) -> str:
    return value if value else "Not available"


def render_report(
    *,
    generated_utc: datetime,
    source_path: Path,
    dataset_summary: DatasetSummary,
    validation: ValidationResult,
    filter_summary: pd.DataFrame,
    outcome_funnel: pd.DataFrame,
    interactions: pd.DataFrame,
    recommendations: pd.DataFrame,
) -> str:
    separator = "=" * 100
    section = "-" * 100

    lines: list[str] = [
        separator,
        "BACQE CONVEX SURVIVAL",
        "CS02 - FILTER ATTRIBUTION ANALYSIS",
        separator,
        f"Generated UTC:              {utc_iso(generated_utc)}",
        f"Engine version:             {ENGINE_VERSION}",
        f"Source:                     {source_path}",
        f"Validation result:          "
        f"{'PASS' if validation.passed else 'FAIL'}",
        "",
        section,
        "DATASET SUMMARY",
        section,
        f"Rows analysed:              "
        f"{dataset_summary.rows:,}",
        f"Columns analysed:           "
        f"{dataset_summary.columns:,}",
        f"All-pass rows:              "
        f"{dataset_summary.all_pass_rows:,}",
        f"Failed rows:                "
        f"{dataset_summary.failed_rows:,}",
        f"Sole-veto rows:             "
        f"{dataset_summary.sole_veto_rows:,}",
        f"Multi-veto rows:            "
        f"{dataset_summary.multi_veto_rows:,}",
        f"Earliest timestamp:         "
        f"{format_optional(dataset_summary.earliest_timestamp)}",
        f"Latest timestamp:           "
        f"{format_optional(dataset_summary.latest_timestamp)}",
        f"Run IDs:                    "
        f"{format_collection(dataset_summary.run_ids)}",
        f"Symbols:                    "
        f"{format_collection(dataset_summary.symbols)}",
        f"Timeframes:                 "
        f"{format_collection(dataset_summary.timeframes)}",
        "",
    ]

    if validation.warnings:
        lines.extend(
            [
                section,
                "VALIDATION WARNINGS",
                section,
            ]
        )

        lines.extend(
            f"- {warning}"
            for warning in validation.warnings
        )

        lines.append("")

    lines.extend(
        [
            section,
            "DECISION OUTCOME FUNNEL",
            section,
        ]
    )

    for row in outcome_funnel.itertuples(index=False):
        lines.append(
            f"{row.stage_order:>2}. "
            f"{row.category:<30} "
            f"{row.rows:>10,} "
            f"({row.pct_of_total:>8.4f}%)"
        )

    lines.extend(
        [
            "",
            section,
            "FILTER ATTRIBUTION RANKING",
            section,
        ]
    )

    if filter_summary.empty:
        lines.append(
            "No filter attribution records were available."
        )
    else:
        header = (
            f"{'Rank':>4}  "
            f"{'Filter':<28} "
            f"{'Failures':>10} "
            f"{'First':>10} "
            f"{'Sole':>10} "
            f"{'Marginal %':>12} "
            f"{'Influence':>10} "
            f"{'Priority':>10}"
        )

        lines.append(header)
        lines.append("-" * len(header))

        for row in filter_summary.itertuples(index=False):
            lines.append(
                f"{row.rank:>4}  "
                f"{row.filter:<28} "
                f"{row.failures:>10,} "
                f"{row.first_vetoes:>10,} "
                f"{row.sole_vetoes:>10,} "
                f"{row.marginal_influence_pct:>11.4f}% "
                f"{row.marginal_influence_class:>10} "
                f"{row.research_priority:>10}"
            )

    lines.extend(
        [
            "",
            section,
            "LEADING FILTER INTERACTIONS",
            section,
        ]
    )

    if interactions.empty:
        lines.append(
            "No multi-filter interactions were observed."
        )
    else:
        for rank, row in enumerate(
            interactions.head(20).itertuples(index=False),
            start=1,
        ):
            lines.append(
                f"{rank:>2}. "
                f"{row.filter_a:<25} + "
                f"{row.filter_b:<25} "
                f"{row.cofailure_rows:>10,}"
            )

    lines.extend(
        [
            "",
            section,
            "RESEARCH RECOMMENDATIONS",
            section,
        ]
    )

    if recommendations.empty:
        lines.append(
            "No controlled filter experiments were recommended."
        )
    else:
        for row in recommendations.itertuples(index=False):
            lines.extend(
                [
                    f"Recommendation "
                    f"{row.recommendation_rank}",
                    "",
                    f"ID:                          "
                    f"{row.recommendation_id}",
                    f"Priority:                    "
                    f"{row.priority}",
                    f"Filter:                      "
                    f"{row.filter}",
                    f"Title:                       "
                    f"{row.title}",
                    f"Reason:                      "
                    f"{row.reason}",
                    f"Evidence:                    "
                    f"{row.evidence}",
                    f"Proposed experiment:         "
                    f"{row.proposed_experiment}",
                    "",
                ]
            )

    lines.extend(
        [
            section,
            "INTERPRETATION NOTE",
            section,
            (
                "CS02 ranks filters by observable attribution. "
                "A high failure count does not necessarily imply "
                "high marginal influence. Sole vetoes identify "
                "observations where one filter independently "
                "prevented qualification. Recommendations are "
                "research candidates, not instructions to remove "
                "or weaken a filter."
            ),
            "",
            separator,
            "END OF REPORT",
            separator,
            "",
        ]
    )

    return "\n".join(lines)


def write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(
        path.suffix + ".tmp"
    )
    temporary_path.write_text(
        content,
        encoding="utf-8",
    )
    temporary_path.replace(path)


def write_csv_atomic(
    frame: pd.DataFrame,
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(
        path.suffix + ".tmp"
    )
    frame.to_csv(
        temporary_path,
        index=False,
        encoding="utf-8",
    )
    temporary_path.replace(path)


def write_json_atomic(
    path: Path,
    payload: dict[str, Any],
) -> None:
    content = json.dumps(
        payload,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
    )

    write_text_atomic(path, content + "\n")


def run_analysis(
    *,
    source_path: Path,
    output_directory: Path,
    generated_utc: datetime | None = None,
) -> EngineOutputs:
    source_path = source_path.resolve()

    if not source_path.exists():
        raise FileNotFoundError(
            f"CS01 staged Parquet does not exist: "
            f"{source_path}"
        )

    generated_utc = generated_utc or utc_now()
    paths = build_analysis_paths(output_directory)

    raw_frame = pd.read_parquet(source_path)
    frame = prepare_attribution_frame(raw_frame)

    validation = validate_attribution_frame(frame)

    if not validation.passed:
        joined_errors = "\n".join(
            f"- {error}"
            for error in validation.errors
        )

        raise ValueError(
            "CS02 input validation failed:\n"
            f"{joined_errors}"
        )

    dataset_summary = build_dataset_summary(frame)
    filter_summary = build_filter_summary(frame)
    interactions = build_filter_interactions(frame)
    outcome_funnel = build_outcome_funnel(frame)
    recommendations = build_recommendations(
        filter_summary
    )

    report = render_report(
        generated_utc=generated_utc,
        source_path=source_path,
        dataset_summary=dataset_summary,
        validation=validation,
        filter_summary=filter_summary,
        outcome_funnel=outcome_funnel,
        interactions=interactions,
        recommendations=recommendations,
    )

    write_csv_atomic(
        filter_summary,
        paths.summary_csv,
    )

    write_csv_atomic(
        recommendations,
        paths.recommendations_csv,
    )

    write_text_atomic(
        paths.report_txt,
        report,
    )

    audit_payload = {
        "engine_id": ENGINE_ID,
        "engine_name": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        "generated_utc": utc_iso(generated_utc),
        "source_path": str(source_path),
        "validation": {
            "passed": validation.passed,
            "errors": list(validation.errors),
            "warnings": list(validation.warnings),
        },
        "dataset_summary": asdict(dataset_summary),
        "output_paths": {
            key: str(value)
            for key, value in asdict(paths).items()
        },
        "filter_count": int(len(filter_summary)),
        "recommendation_count": int(
            len(recommendations)
        ),
        "interaction_count": int(
            len(interactions)
        ),
    }

    write_json_atomic(
        paths.audit_json,
        audit_payload,
    )

    return EngineOutputs(
        paths=paths,
        dataset_summary=dataset_summary,
        filter_summary=filter_summary,
        recommendations=recommendations,
        outcome_funnel=outcome_funnel,
        validation=validation,
    )