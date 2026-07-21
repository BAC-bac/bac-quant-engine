#!/usr/bin/env python3
"""
BACQE EH20 - Experiment Registry Engine
=======================================

Purpose
-------
Create and maintain BACQE's canonical institutional registry of research
experiments.

EH20 converts research recommendations and manually declared experiments into
persistent experiment records with deterministic identifiers, lifecycle status,
provenance, timestamps, and append-safe historical snapshots.

Scientific contract
-------------------
EH20 does not claim that an experiment has been executed, validated, or proven.
It records what experiments exist, where they came from, and their current
institutional state.

Primary responsibilities
------------------------
1. Ingest candidate experiments from EH19 recommendations and optional manual input.
2. Assign deterministic experiment identifiers.
3. Preserve the first-seen timestamp for existing experiments.
4. Update mutable registry fields without duplicating experiments.
5. Write a canonical latest registry.
6. Append immutable snapshot history.
7. Produce an institutional text report and run manifest.
8. Execute deterministic self-tests.

Non-responsibilities
--------------------
- Scheduling experiments.
- Executing experiments.
- Determining scientific validity.
- Managing compute resources.
- Generating unsupported hypotheses.

Engine version
--------------
1.0.0
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd


ENGINE_NAME = "BACQE EH20 - EXPERIMENT REGISTRY ENGINE"
ENGINE_VERSION = "1.0.0"
DEFAULT_RESEARCH_ROOT = Path(
    r"E:\Quant_Lab\data\analysis\dukascopy_extended_horizons"
)
DEFAULT_EH19_DIRNAME = "research_recommendations"
DEFAULT_OUTPUT_DIRNAME = "experiment_registry"

VALID_STATUSES: tuple[str, ...] = (
    "proposed",
    "planned",
    "queued",
    "running",
    "completed",
    "validated",
    "published",
    "retired",
    "archived",
    "blocked",
    "cancelled",
)

TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"published", "retired", "archived", "cancelled"}
)

STATUS_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "proposed": frozenset(
        {"proposed", "planned", "blocked", "cancelled", "archived"}
    ),
    "planned": frozenset(
        {"planned", "queued", "blocked", "cancelled", "archived"}
    ),
    "queued": frozenset(
        {"queued", "running", "blocked", "cancelled", "planned"}
    ),
    "running": frozenset(
        {"running", "completed", "blocked", "cancelled", "queued"}
    ),
    "completed": frozenset(
        {"completed", "validated", "running", "retired", "archived"}
    ),
    "validated": frozenset(
        {"validated", "published", "retired", "archived", "running"}
    ),
    "published": frozenset({"published", "retired", "archived"}),
    "retired": frozenset({"retired", "archived"}),
    "archived": frozenset({"archived"}),
    "blocked": frozenset(
        {"blocked", "planned", "queued", "running", "cancelled", "archived"}
    ),
    "cancelled": frozenset({"cancelled", "archived", "proposed"}),
}

CANONICAL_COLUMNS: tuple[str, ...] = (
    "experiment_id",
    "experiment_key",
    "experiment_title",
    "objective",
    "hypothesis",
    "research_domain",
    "research_family",
    "edge_family_id",
    "analysis_mode",
    "research_rank",
    "symbol",
    "feature_family",
    "threshold_side",
    "context_type",
    "parent_context",
    "target",
    "horizon",
    "side",
    "experiment_type",
    "priority",
    "priority_score",
    "status",
    "owner",
    "source_engine",
    "source_record_id",
    "source_path",
    "dependency_ids",
    "dataset_refs",
    "code_refs",
    "evidence_refs",
    "recommendation_reason",
    "supporting_evidence",
    "evidence_limit",
    "recommended_research_status",
    "recommended_next_step",
    "created_utc",
    "first_seen_utc",
    "last_seen_utc",
    "last_updated_utc",
    "completed_utc",
    "validated_utc",
    "published_utc",
    "retired_utc",
    "registry_version",
    "is_active",
    "record_hash",
)

IMMUTABLE_COLUMNS: frozenset[str] = frozenset(
    {
        "experiment_id",
        "experiment_key",
        "created_utc",
        "first_seen_utc",
    }
)

LIST_COLUMNS: frozenset[str] = frozenset(
    {
        "dependency_ids",
        "dataset_refs",
        "code_refs",
        "evidence_refs",
    }
)

SOURCE_ALIASES: Mapping[str, tuple[str, ...]] = {
    "source_record_id": (
        "recommendation_id",
        "candidate_id",
        "record_id",
        "source_record_id",
    ),
    "experiment_title": (
        "experiment_title",
        "recommendation_title",
        "title",
        "research_action",
        "recommended_action",
    ),
    "objective": (
        "objective",
        "research_objective",
        "recommended_next_step",
        "recommendation_reason",
        "recommendation",
        "recommended_action",
        "rationale",
    ),
    "hypothesis": (
        "hypothesis",
        "research_hypothesis",
        "testable_hypothesis",
    ),
    "research_domain": (
        "research_domain",
        "domain",
        "branch",
        "research_area",
    ),
    "research_family": (
        "research_family",
        "edge_family_id",
        "edge_family",
        "candidate_family",
        "family",
    ),
    "edge_family_id": ("edge_family_id",),
    "analysis_mode": ("analysis_mode",),
    "research_rank": ("research_rank",),
    "symbol": ("symbol", "instrument", "market"),
    "feature_family": (
        "feature_family",
        "feature",
        "feature_name",
        "signal_family",
    ),
    "threshold_side": ("threshold_side",),
    "context_type": ("context_type",),
    "parent_context": ("parent_context",),
    "target": ("target", "target_name"),
    "horizon": ("horizon", "forecast_horizon"),
    "side": ("side", "direction", "trade_side"),
    "experiment_type": (
        "experiment_type",
        "recommended_action",
        "recommendation_type",
        "research_type",
        "action_type",
    ),
    "priority": (
        "priority",
        "priority_band",
        "priority_label",
        "recommendation_priority",
    ),
    "priority_score": (
        "priority_score",
        "research_priority_score",
        "recommendation_score",
        "score",
        "rank_score",
    ),
    "owner": ("owner", "research_owner"),
    "dependency_ids": ("dependency_ids", "dependencies"),
    "dataset_refs": ("dataset_refs", "datasets", "dataset_paths"),
    "code_refs": ("code_refs", "scripts", "script_paths"),
    "evidence_refs": (
        "evidence_refs",
        "evidence_paths",
    ),
    "recommendation_reason": ("recommendation_reason",),
    "supporting_evidence": ("supporting_evidence",),
    "evidence_limit": ("evidence_limit",),
    "recommended_research_status": ("recommended_research_status",),
    "recommended_next_step": ("recommended_next_step",),
}


@dataclass(frozen=True)
class EnginePaths:
    research_root: Path
    eh19_input: Path
    manual_input: Path | None
    output_dir: Path
    history_dir: Path
    latest_registry: Path
    latest_report: Path
    latest_manifest: Path


@dataclass(frozen=True)
class RunSummary:
    generated_utc: str
    engine_version: str
    source_rows: int
    manual_rows: int
    candidate_rows: int
    previous_registry_rows: int
    new_experiments: int
    updated_experiments: int
    unchanged_experiments: int
    total_registry_rows: int
    active_experiments: int
    terminal_experiments: int
    blocked_experiments: int
    snapshot_path: str
    registry_path: str
    report_path: str
    status: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: datetime | None = None) -> str:
    current = value or utc_now()
    return current.astimezone(timezone.utc).isoformat(timespec="seconds")


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return " ".join(str(value).strip().split())


def normalize_token(value: Any) -> str:
    text = normalize_text(value).lower()
    return "".join(character for character in text if character.isalnum() or character in {"_", "-", "."})


def normalize_priority(value: Any) -> str:
    text = normalize_text(value).lower()
    aliases = {
        "critical": "critical",
        "urgent": "critical",
        "very_high": "high",
        "very high": "high",
        "high": "high",
        "medium": "medium",
        "moderate": "medium",
        "normal": "medium",
        "low": "low",
        "defer": "low",
    }
    return aliases.get(text, text or "unclassified")


def normalize_status(value: Any) -> str:
    text = normalize_text(value).lower()
    aliases = {
        "recommended": "proposed",
        "recommendation": "proposed",
        "candidate": "proposed",
        "new": "proposed",
        "pending": "planned",
        "in_progress": "running",
        "in progress": "running",
        "done": "completed",
        "complete": "completed",
        "pass": "validated",
        "failed": "blocked",
        "inactive": "retired",
    }
    status = aliases.get(text, text or "proposed")
    if status not in VALID_STATUSES:
        raise ValueError(
            f"Unsupported experiment status {status!r}. "
            f"Expected one of: {', '.join(VALID_STATUSES)}"
        )
    return status


def normalize_list(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass

    if isinstance(value, (list, tuple, set, frozenset)):
        items = [normalize_text(item) for item in value]
    else:
        text = normalize_text(value)
        if not text:
            return ""
        separators = ("|", ";", "\n")
        items = [text]
        for separator in separators:
            expanded: list[str] = []
            for item in items:
                expanded.extend(item.split(separator))
            items = expanded

    cleaned = sorted({item for item in (normalize_text(item) for item in items) if item})
    return "|".join(cleaned)


def safe_float(value: Any) -> float | None:
    text = normalize_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def select_alias(row: Mapping[str, Any], canonical_name: str) -> Any:
    for alias in SOURCE_ALIASES.get(canonical_name, (canonical_name,)):
        if alias in row and normalize_text(row[alias]):
            return row[alias]
    return ""


def first_existing_file(paths: Sequence[Path]) -> Path | None:
    for path in paths:
        if path.exists() and path.is_file():
            return path
    return None


def discover_eh19_input(research_root: Path) -> Path:
    candidates = (
        research_root
        / DEFAULT_EH19_DIRNAME
        / "research_recommendation_queue_latest.csv",
        research_root
        / DEFAULT_EH19_DIRNAME
        / "research_recommendation_register_latest.csv",
        research_root
        / DEFAULT_EH19_DIRNAME
        / "research_recommendation_portfolio_latest.csv",
        research_root
        / "research_recommendations_latest.csv",
    )
    discovered = first_existing_file(candidates)
    return discovered or candidates[0]


def build_paths(
    research_root: Path,
    eh19_input: Path | None,
    manual_input: Path | None,
    output_dir: Path | None,
) -> EnginePaths:
    resolved_output = output_dir or research_root / DEFAULT_OUTPUT_DIRNAME
    return EnginePaths(
        research_root=research_root,
        eh19_input=eh19_input or discover_eh19_input(research_root),
        manual_input=manual_input,
        output_dir=resolved_output,
        history_dir=resolved_output / "history",
        latest_registry=resolved_output / "experiment_registry_latest.csv",
        latest_report=resolved_output / "experiment_registry_report_latest.txt",
        latest_manifest=resolved_output / "experiment_registry_manifest_latest.json",
    )


def read_csv_if_exists(path: Path, required: bool) -> pd.DataFrame:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Required input does not exist: {path}")
        logging.info("Optional input not present: %s", path)
        return pd.DataFrame()

    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    logging.info("Loaded %s rows from %s", len(frame), path)
    return frame


def make_experiment_key(record: Mapping[str, Any]) -> str:
    components = (
        normalize_token(record.get("research_domain")),
        normalize_token(record.get("edge_family_id")),
        normalize_token(record.get("target")),
        normalize_token(record.get("feature_family")),
        normalize_token(record.get("threshold_side")),
        normalize_token(record.get("context_type")),
        normalize_token(record.get("parent_context")),
        normalize_token(record.get("experiment_type")),
        normalize_token(record.get("symbol")),
        normalize_token(record.get("horizon")),
    )
    meaningful = [component for component in components if component]
    if not meaningful:
        source_id = normalize_token(record.get("source_record_id"))
        title = normalize_token(record.get("experiment_title"))
        meaningful = [component for component in (source_id, title) if component]
    if not meaningful:
        raise ValueError("Cannot create an experiment key from an empty record.")
    return "::".join(meaningful)


def make_experiment_id(experiment_key: str) -> str:
    digest = hashlib.sha256(experiment_key.encode("utf-8")).hexdigest()[:16].upper()
    return f"EXP-{digest}"


def calculate_record_hash(record: Mapping[str, Any]) -> str:
    payload = {
        column: normalize_text(record.get(column))
        for column in CANONICAL_COLUMNS
        if column not in {"record_hash", "last_seen_utc", "last_updated_utc"}
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def infer_title(record: Mapping[str, Any]) -> str:
    explicit = normalize_text(record.get("experiment_title"))
    if explicit:
        return explicit

    parts = [
        normalize_text(record.get("experiment_type")).replace("_", " ").title(),
        normalize_text(record.get("edge_family_id"))
        or normalize_text(record.get("research_family")),
        normalize_text(record.get("feature_family")),
    ]
    title = " - ".join(part for part in parts if part)
    return title or "Untitled BACQE Experiment"


def canonicalize_candidate(
    raw_row: Mapping[str, Any],
    source_engine: str,
    source_path: Path,
    observed_utc: str,
) -> dict[str, Any]:
    record: dict[str, Any] = {}

    for canonical_name in SOURCE_ALIASES:
        record[canonical_name] = select_alias(raw_row, canonical_name)

    record["source_engine"] = source_engine
    record["source_path"] = str(source_path)
    record["status"] = normalize_status(raw_row.get("status", "proposed"))
    record["priority"] = normalize_priority(record.get("priority"))
    record["priority_score"] = safe_float(record.get("priority_score"))
    record["owner"] = normalize_text(record.get("owner")) or "BACQE"
    record["research_domain"] = (
        normalize_text(record.get("research_domain"))
        or "dukascopy_extended_horizons"
    )
    record["experiment_type"] = (
        normalize_text(record.get("experiment_type"))
        or "research_recommendation"
    )

    for column in LIST_COLUMNS:
        record[column] = normalize_list(record.get(column))

    record["objective"] = normalize_text(record.get("objective"))
    record["hypothesis"] = normalize_text(record.get("hypothesis"))
    record["research_family"] = normalize_text(record.get("research_family"))
    record["edge_family_id"] = (
        normalize_text(record.get("edge_family_id"))
        or record["research_family"]
    )
    if not record["research_family"]:
        record["research_family"] = record["edge_family_id"]
    record["analysis_mode"] = normalize_text(record.get("analysis_mode")).lower()
    record["research_rank"] = normalize_text(record.get("research_rank"))
    record["symbol"] = normalize_text(record.get("symbol")).upper()
    record["feature_family"] = normalize_text(record.get("feature_family"))
    record["threshold_side"] = normalize_text(record.get("threshold_side")).lower()
    record["context_type"] = normalize_text(record.get("context_type"))
    record["parent_context"] = normalize_text(record.get("parent_context"))
    record["target"] = normalize_text(record.get("target"))
    record["horizon"] = normalize_text(record.get("horizon"))
    record["side"] = normalize_text(record.get("side")).lower()
    record["recommendation_reason"] = normalize_text(
        record.get("recommendation_reason")
    )
    record["supporting_evidence"] = normalize_text(
        record.get("supporting_evidence")
    )
    record["evidence_limit"] = normalize_text(record.get("evidence_limit"))
    record["recommended_research_status"] = normalize_text(
        record.get("recommended_research_status")
    )
    record["recommended_next_step"] = normalize_text(
        record.get("recommended_next_step")
    )
    record["source_record_id"] = normalize_text(record.get("source_record_id"))
    record["experiment_title"] = infer_title(record)

    record["experiment_key"] = make_experiment_key(record)
    record["experiment_id"] = make_experiment_id(record["experiment_key"])
    record["created_utc"] = observed_utc
    record["first_seen_utc"] = observed_utc
    record["last_seen_utc"] = observed_utc
    record["last_updated_utc"] = observed_utc
    record["completed_utc"] = ""
    record["validated_utc"] = ""
    record["published_utc"] = ""
    record["retired_utc"] = ""
    record["registry_version"] = ENGINE_VERSION
    record["is_active"] = record["status"] not in TERMINAL_STATUSES
    record["record_hash"] = ""

    return record


def canonicalize_source(
    frame: pd.DataFrame,
    source_engine: str,
    source_path: Path,
    observed_utc: str,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    records = [
        canonicalize_candidate(
            raw_row=row,
            source_engine=source_engine,
            source_path=source_path,
            observed_utc=observed_utc,
        )
        for row in frame.to_dict(orient="records")
    ]
    result = pd.DataFrame.from_records(records)
    return result.reindex(columns=CANONICAL_COLUMNS)


def validate_transition(previous_status: str, new_status: str) -> None:
    previous = normalize_status(previous_status)
    current = normalize_status(new_status)
    allowed = STATUS_TRANSITIONS[previous]
    if current not in allowed:
        raise ValueError(
            f"Invalid experiment lifecycle transition: "
            f"{previous!r} -> {current!r}"
        )


def merge_text_lists(previous: Any, current: Any) -> str:
    combined = []
    for value in (previous, current):
        normalized = normalize_list(value)
        if normalized:
            combined.extend(normalized.split("|"))
    return "|".join(sorted(set(combined)))


def lifecycle_timestamps(
    previous: Mapping[str, Any],
    current_status: str,
    observed_utc: str,
) -> dict[str, str]:
    timestamps = {
        "completed_utc": normalize_text(previous.get("completed_utc")),
        "validated_utc": normalize_text(previous.get("validated_utc")),
        "published_utc": normalize_text(previous.get("published_utc")),
        "retired_utc": normalize_text(previous.get("retired_utc")),
    }

    if current_status == "completed" and not timestamps["completed_utc"]:
        timestamps["completed_utc"] = observed_utc
    elif current_status == "validated":
        if not timestamps["completed_utc"]:
            timestamps["completed_utc"] = observed_utc
        if not timestamps["validated_utc"]:
            timestamps["validated_utc"] = observed_utc
    elif current_status == "published":
        if not timestamps["completed_utc"]:
            timestamps["completed_utc"] = observed_utc
        if not timestamps["validated_utc"]:
            timestamps["validated_utc"] = observed_utc
        if not timestamps["published_utc"]:
            timestamps["published_utc"] = observed_utc
    elif current_status == "retired" and not timestamps["retired_utc"]:
        timestamps["retired_utc"] = observed_utc

    return timestamps


def rows_equal_for_update(
    previous: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> bool:
    ignored = {
        "record_hash",
        "last_seen_utc",
        "last_updated_utc",
        "created_utc",
        "first_seen_utc",
        "completed_utc",
        "validated_utc",
        "published_utc",
        "retired_utc",
    }
    for column in CANONICAL_COLUMNS:
        if column in ignored:
            continue
        previous_value = previous.get(column)
        candidate_value = candidate.get(column)
        if column in LIST_COLUMNS:
            if normalize_list(previous_value) != normalize_list(candidate_value):
                return False
        elif normalize_text(previous_value) != normalize_text(candidate_value):
            return False
    return True


def merge_candidate_with_previous(
    previous: Mapping[str, Any],
    candidate: Mapping[str, Any],
    observed_utc: str,
) -> tuple[dict[str, Any], bool]:
    validate_transition(
        normalize_text(previous.get("status")) or "proposed",
        normalize_text(candidate.get("status")) or "proposed",
    )

    changed = not rows_equal_for_update(previous, candidate)
    merged = dict(previous)

    for column in CANONICAL_COLUMNS:
        if column in IMMUTABLE_COLUMNS:
            continue
        if column in LIST_COLUMNS:
            merged[column] = merge_text_lists(previous.get(column), candidate.get(column))
            continue
        if column in {
            "record_hash",
            "last_seen_utc",
            "last_updated_utc",
            "completed_utc",
            "validated_utc",
            "published_utc",
            "retired_utc",
        }:
            continue

        candidate_value = candidate.get(column)
        if normalize_text(candidate_value):
            merged[column] = candidate_value

    merged["last_seen_utc"] = observed_utc
    if changed:
        merged["last_updated_utc"] = observed_utc

    current_status = normalize_status(merged.get("status"))
    merged.update(lifecycle_timestamps(previous, current_status, observed_utc))
    merged["registry_version"] = ENGINE_VERSION
    merged["is_active"] = current_status not in TERMINAL_STATUSES
    merged["record_hash"] = calculate_record_hash(merged)
    return merged, changed


def normalize_previous_registry(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    result = frame.copy()
    for column in CANONICAL_COLUMNS:
        if column not in result.columns:
            result[column] = ""

    result = result.reindex(columns=CANONICAL_COLUMNS)
    result["experiment_id"] = result["experiment_id"].map(normalize_text)
    result["experiment_key"] = result["experiment_key"].map(normalize_text)
    result["status"] = result["status"].map(normalize_status)
    result["is_active"] = result["status"].map(
        lambda status: status not in TERMINAL_STATUSES
    )

    duplicates = result["experiment_id"].duplicated(keep=False)
    if duplicates.any():
        ids = sorted(result.loc[duplicates, "experiment_id"].unique())
        raise ValueError(
            "Previous registry contains duplicate experiment identifiers: "
            + ", ".join(ids)
        )

    return result


def deduplicate_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.reindex(columns=CANONICAL_COLUMNS)

    ordered = frame.copy()
    ordered["_priority_score_sort"] = pd.to_numeric(
        ordered["priority_score"], errors="coerce"
    ).fillna(float("-inf"))
    ordered["_source_sort"] = ordered["source_engine"].map(normalize_text)

    ordered = ordered.sort_values(
        by=["experiment_id", "_priority_score_sort", "_source_sort"],
        ascending=[True, False, True],
        kind="mergesort",
    )

    merged_records: list[dict[str, Any]] = []
    for _, group in ordered.groupby("experiment_id", sort=True):
        base = group.iloc[0].to_dict()
        for column in LIST_COLUMNS:
            base[column] = normalize_list(
                "|".join(
                    normalize_list(value)
                    for value in group[column].tolist()
                    if normalize_list(value)
                )
            )

        source_engines = normalize_list(group["source_engine"].tolist())
        source_paths = normalize_list(group["source_path"].tolist())
        source_ids = normalize_list(group["source_record_id"].tolist())
        base["source_engine"] = source_engines
        base["source_path"] = source_paths
        base["source_record_id"] = source_ids
        merged_records.append(base)

    result = pd.DataFrame.from_records(merged_records)
    return result.reindex(columns=CANONICAL_COLUMNS)


def merge_registry(
    previous_registry: pd.DataFrame,
    candidates: pd.DataFrame,
    observed_utc: str,
) -> tuple[pd.DataFrame, int, int, int]:
    previous = normalize_previous_registry(previous_registry)
    candidate_frame = deduplicate_candidates(candidates)

    previous_by_id = {
        row["experiment_id"]: row
        for row in previous.to_dict(orient="records")
    }

    output_records: dict[str, dict[str, Any]] = {
        experiment_id: dict(row)
        for experiment_id, row in previous_by_id.items()
    }

    new_count = 0
    updated_count = 0
    unchanged_count = 0

    for candidate in candidate_frame.to_dict(orient="records"):
        experiment_id = normalize_text(candidate["experiment_id"])
        if experiment_id not in previous_by_id:
            candidate["record_hash"] = calculate_record_hash(candidate)
            output_records[experiment_id] = candidate
            new_count += 1
            continue

        merged, changed = merge_candidate_with_previous(
            previous=previous_by_id[experiment_id],
            candidate=candidate,
            observed_utc=observed_utc,
        )
        output_records[experiment_id] = merged
        if changed:
            updated_count += 1
        else:
            unchanged_count += 1

    result = pd.DataFrame.from_records(list(output_records.values()))
    if result.empty:
        result = pd.DataFrame(columns=CANONICAL_COLUMNS)

    result = result.reindex(columns=CANONICAL_COLUMNS)
    result["priority_score"] = pd.to_numeric(
        result["priority_score"], errors="coerce"
    )
    result["is_active"] = result["status"].map(
        lambda status: normalize_status(status) not in TERMINAL_STATUSES
    )

    result = result.sort_values(
        by=[
            "is_active",
            "priority_score",
            "priority",
            "research_domain",
            "research_family",
            "symbol",
            "experiment_id",
        ],
        ascending=[False, False, True, True, True, True, True],
        na_position="last",
        kind="mergesort",
    ).reset_index(drop=True)

    if result["experiment_id"].duplicated().any():
        raise AssertionError("Merged registry contains duplicate experiment IDs.")

    return result, new_count, updated_count, unchanged_count


def dataframe_sha256(frame: pd.DataFrame) -> str:
    canonical = frame.reindex(columns=CANONICAL_COLUMNS).fillna("")
    payload = canonical.to_csv(index=False, lineterminator="\n")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def write_csv_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temporary, index=False, lineterminator="\n")
    temporary.replace(path)


def write_text_atomic(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8", newline="\n")
    temporary.replace(path)


def write_json_atomic(payload: Mapping[str, Any], path: Path) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    write_text_atomic(text, path)


def count_by(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    if frame.empty or column not in frame.columns:
        return pd.DataFrame(columns=[column, "experiments"])
    values = frame[column].map(lambda value: normalize_text(value) or "unclassified")
    summary = (
        values.value_counts(dropna=False)
        .rename_axis(column)
        .reset_index(name="experiments")
    )
    return summary


def render_table(frame: pd.DataFrame, max_rows: int = 20) -> str:
    if frame.empty:
        return "(none)"
    return frame.head(max_rows).to_string(index=False)


def build_report(
    registry: pd.DataFrame,
    summary: RunSummary,
    paths: EnginePaths,
    registry_hash: str,
) -> str:
    active = registry[registry["is_active"].astype(bool)].copy()
    top_priority = active.copy()
    top_priority["_score"] = pd.to_numeric(
        top_priority["priority_score"], errors="coerce"
    )
    top_priority = top_priority.sort_values(
        by=["_score", "priority", "experiment_id"],
        ascending=[False, True, True],
        na_position="last",
        kind="mergesort",
    )
    top_priority = top_priority[
        [
            "experiment_id",
            "priority",
            "priority_score",
            "status",
            "symbol",
            "research_family",
            "experiment_title",
        ]
    ]

    separator = "=" * 112
    sub_separator = "-" * 112

    lines = [
        separator,
        "BACQE EH20 - EXPERIMENT REGISTRY REPORT",
        separator,
        f"Generated UTC:              {summary.generated_utc}",
        f"Engine version:             {summary.engine_version}",
        f"EH19 source:                {paths.eh19_input}",
        f"Manual source:              {paths.manual_input or '(not configured)'}",
        f"Canonical registry:         {paths.latest_registry}",
        f"Historical snapshot:        {summary.snapshot_path}",
        f"Registry SHA-256:           {registry_hash}",
        sub_separator,
        f"EH19 source rows:           {summary.source_rows:,}",
        f"Manual source rows:         {summary.manual_rows:,}",
        f"Candidate rows:             {summary.candidate_rows:,}",
        f"Previous registry rows:     {summary.previous_registry_rows:,}",
        f"New experiments:            {summary.new_experiments:,}",
        f"Updated experiments:        {summary.updated_experiments:,}",
        f"Unchanged experiments:      {summary.unchanged_experiments:,}",
        f"Total registry rows:        {summary.total_registry_rows:,}",
        f"Active experiments:         {summary.active_experiments:,}",
        f"Terminal experiments:       {summary.terminal_experiments:,}",
        f"Blocked experiments:        {summary.blocked_experiments:,}",
        f"Engine status:              {summary.status}",
        separator,
        "SCIENTIFIC INTERPRETATION",
        sub_separator,
        (
            "EH20 records the institutional existence and lifecycle state of "
            "experiments. It does not assert that proposed work has been run, "
            "that completed work is valid, or that recommendations constitute "
            "scientific evidence."
        ),
        "",
        "STATUS DISTRIBUTION",
        sub_separator,
        render_table(count_by(registry, "status")),
        "",
        "PRIORITY DISTRIBUTION",
        sub_separator,
        render_table(count_by(active, "priority")),
        "",
        "RESEARCH DOMAIN DISTRIBUTION",
        sub_separator,
        render_table(count_by(registry, "research_domain")),
        "",
        "RESEARCH FAMILY DISTRIBUTION",
        sub_separator,
        render_table(count_by(registry, "research_family")),
        "",
        "TOP ACTIVE EXPERIMENTS",
        sub_separator,
        render_table(top_priority, max_rows=25),
        separator,
        "END OF REPORT",
        separator,
        "",
    ]
    return "\n".join(lines)


def make_snapshot_path(history_dir: Path, generated: datetime) -> Path:
    stamp = generated.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return history_dir / f"experiment_registry_snapshot_{stamp}.csv"


def load_previous_registry(path: Path) -> pd.DataFrame:
    return read_csv_if_exists(path, required=False)


def validate_registry(frame: pd.DataFrame) -> None:
    missing_columns = [
        column for column in CANONICAL_COLUMNS if column not in frame.columns
    ]
    if missing_columns:
        raise AssertionError(
            "Registry is missing canonical columns: "
            + ", ".join(missing_columns)
        )

    if frame["experiment_id"].duplicated().any():
        raise AssertionError("Registry contains duplicate experiment IDs.")

    invalid_statuses = sorted(
        set(frame["status"].map(normalize_text)) - set(VALID_STATUSES)
    )
    if invalid_statuses:
        raise AssertionError(
            "Registry contains invalid statuses: " + ", ".join(invalid_statuses)
        )

    for row in frame.to_dict(orient="records"):
        expected_id = make_experiment_id(normalize_text(row["experiment_key"]))
        if normalize_text(row["experiment_id"]) != expected_id:
            raise AssertionError(
                f"Experiment ID mismatch for key {row['experiment_key']!r}"
            )


def run_self_tests() -> None:
    observed_1 = "2026-01-01T00:00:00+00:00"
    observed_2 = "2026-01-02T00:00:00+00:00"

    source = pd.DataFrame(
        [
            {
                "recommendation_id": "REC-001",
                "recommendation_title": "Replicate spread edge",
                "recommendation": "Replicate the spread edge on GBPJPY.",
                "domain": "dukascopy_extended_horizons",
                "edge_family": "spread",
                "symbol": "GBPJPY",
                "feature": "spread_range",
                "threshold_side": "upper",
                "target": "future_return_1000",
                "horizon": "1000",
                "side": "long",
                "recommendation_type": "replication",
                "priority": "high",
                "recommendation_score": "0.91",
            }
        ]
    )

    first = canonicalize_source(
        source,
        source_engine="EH19",
        source_path=Path("eh19.csv"),
        observed_utc=observed_1,
    )
    registry_1, new_1, updated_1, unchanged_1 = merge_registry(
        previous_registry=pd.DataFrame(),
        candidates=first,
        observed_utc=observed_1,
    )
    assert new_1 == 1
    assert updated_1 == 0
    assert unchanged_1 == 0
    assert len(registry_1) == 1

    assert registry_1.loc[0, "threshold_side"] == "upper"
    assert registry_1.loc[0, "side"] == "long"

    second = canonicalize_source(
        source,
        source_engine="EH19",
        source_path=Path("eh19.csv"),
        observed_utc=observed_2,
    )
    registry_2, new_2, updated_2, unchanged_2 = merge_registry(
        previous_registry=registry_1,
        candidates=second,
        observed_utc=observed_2,
    )
    assert new_2 == 0
    assert updated_2 == 0
    assert unchanged_2 == 1
    assert registry_2.loc[0, "first_seen_utc"] == observed_1
    assert registry_2.loc[0, "last_seen_utc"] == observed_2

    changed_source = source.copy()
    changed_source.loc[0, "priority"] = "critical"
    changed = canonicalize_source(
        changed_source,
        source_engine="EH19",
        source_path=Path("eh19.csv"),
        observed_utc=observed_2,
    )
    registry_3, new_3, updated_3, unchanged_3 = merge_registry(
        previous_registry=registry_1,
        candidates=changed,
        observed_utc=observed_2,
    )
    assert new_3 == 0
    assert updated_3 == 1
    assert unchanged_3 == 0
    assert registry_3.loc[0, "priority"] == "critical"
    assert registry_3.loc[0, "first_seen_utc"] == observed_1

    duplicate_candidates = pd.concat([first, first], ignore_index=True)
    deduplicated = deduplicate_candidates(duplicate_candidates)
    assert len(deduplicated) == 1

    key_1 = make_experiment_key(first.iloc[0].to_dict())
    key_2 = make_experiment_key(first.iloc[0].to_dict())
    assert key_1 == key_2
    assert make_experiment_id(key_1) == make_experiment_id(key_2)

    validate_transition("proposed", "planned")
    try:
        validate_transition("archived", "running")
    except ValueError:
        pass
    else:
        raise AssertionError("Invalid lifecycle transition was accepted.")

    validate_registry(registry_3)


def execute(paths: EnginePaths, run_tests: bool) -> RunSummary:
    generated = utc_now()
    observed_utc = utc_text(generated)

    paths.output_dir.mkdir(parents=True, exist_ok=True)
    paths.history_dir.mkdir(parents=True, exist_ok=True)

    if run_tests:
        logging.info("Running deterministic self-tests.")
        run_self_tests()
        logging.info("Self-tests passed.")

    eh19_frame = read_csv_if_exists(paths.eh19_input, required=True)
    manual_frame = (
        read_csv_if_exists(paths.manual_input, required=False)
        if paths.manual_input
        else pd.DataFrame()
    )
    previous_registry = load_previous_registry(paths.latest_registry)

    eh19_candidates = canonicalize_source(
        frame=eh19_frame,
        source_engine="EH19",
        source_path=paths.eh19_input,
        observed_utc=observed_utc,
    )
    manual_candidates = canonicalize_source(
        frame=manual_frame,
        source_engine="MANUAL",
        source_path=paths.manual_input or Path(""),
        observed_utc=observed_utc,
    )

    candidate_frames = [frame for frame in (eh19_candidates, manual_candidates,) if not frame.empty]

    if candidate_frames:
        candidates = pd.concat(candidate_frames, ignore_index=True, ).reindex(
        columns=CANONICAL_COLUMNS,
    )
    else:
        candidates = pd.DataFrame(columns=CANONICAL_COLUMNS, )

    registry, new_count, updated_count, unchanged_count = merge_registry(
        previous_registry=previous_registry,
        candidates=candidates,
        observed_utc=observed_utc,
    )
    validate_registry(registry)

    snapshot_path = make_snapshot_path(paths.history_dir, generated)
    registry_hash = dataframe_sha256(registry)

    summary = RunSummary(
        generated_utc=observed_utc,
        engine_version=ENGINE_VERSION,
        source_rows=len(eh19_frame),
        manual_rows=len(manual_frame),
        candidate_rows=len(candidates),
        previous_registry_rows=len(previous_registry),
        new_experiments=new_count,
        updated_experiments=updated_count,
        unchanged_experiments=unchanged_count,
        total_registry_rows=len(registry),
        active_experiments=int(registry["is_active"].astype(bool).sum()),
        terminal_experiments=int((~registry["is_active"].astype(bool)).sum()),
        blocked_experiments=int((registry["status"] == "blocked").sum()),
        snapshot_path=str(snapshot_path),
        registry_path=str(paths.latest_registry),
        report_path=str(paths.latest_report),
        status="PASS",
    )

    report = build_report(
        registry=registry,
        summary=summary,
        paths=paths,
        registry_hash=registry_hash,
    )

    manifest = {
        "engine": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        "generated_utc": observed_utc,
        "status": summary.status,
        "inputs": {
            "eh19_input": str(paths.eh19_input),
            "manual_input": str(paths.manual_input) if paths.manual_input else None,
            "previous_registry": str(paths.latest_registry),
        },
        "outputs": {
            "latest_registry": str(paths.latest_registry),
            "snapshot": str(snapshot_path),
            "report": str(paths.latest_report),
            "manifest": str(paths.latest_manifest),
        },
        "metrics": asdict(summary),
        "registry_sha256": registry_hash,
        "canonical_columns": list(CANONICAL_COLUMNS),
        "valid_statuses": list(VALID_STATUSES),
    }

    write_csv_atomic(registry, paths.latest_registry)
    write_csv_atomic(registry, snapshot_path)
    write_text_atomic(report, paths.latest_report)
    write_json_atomic(manifest, paths.latest_manifest)

    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create and maintain BACQE's canonical institutional "
            "experiment registry."
        )
    )
    parser.add_argument(
        "--research-root",
        type=Path,
        default=DEFAULT_RESEARCH_ROOT,
        help="Extended Horizons analysis root.",
    )
    parser.add_argument(
        "--eh19-input",
        type=Path,
        default=None,
        help="Explicit EH19 recommendations CSV.",
    )
    parser.add_argument(
        "--manual-input",
        type=Path,
        default=None,
        help="Optional manually maintained experiment CSV.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="EH20 output directory.",
    )
    parser.add_argument(
        "--skip-self-tests",
        action="store_true",
        help="Skip deterministic self-tests.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser


def print_summary(summary: RunSummary) -> None:
    separator = "=" * 112
    print(separator)
    print(ENGINE_NAME)
    print(separator)
    print(f"Generated UTC:              {summary.generated_utc}")
    print(f"Engine version:             {summary.engine_version}")
    print(f"EH19 source rows:           {summary.source_rows:,}")
    print(f"Manual source rows:         {summary.manual_rows:,}")
    print(f"Candidate rows:             {summary.candidate_rows:,}")
    print(f"Previous registry rows:     {summary.previous_registry_rows:,}")
    print(f"New experiments:            {summary.new_experiments:,}")
    print(f"Updated experiments:        {summary.updated_experiments:,}")
    print(f"Unchanged experiments:      {summary.unchanged_experiments:,}")
    print(f"Total registry rows:        {summary.total_registry_rows:,}")
    print(f"Active experiments:         {summary.active_experiments:,}")
    print(f"Terminal experiments:       {summary.terminal_experiments:,}")
    print(f"Blocked experiments:        {summary.blocked_experiments:,}")
    print(f"Registry:                   {summary.registry_path}")
    print(f"Snapshot:                   {summary.snapshot_path}")
    print(f"Report:                     {summary.report_path}")
    print(f"Status:                     {summary.status}")
    print(separator)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)

    try:
        paths = build_paths(
            research_root=args.research_root,
            eh19_input=args.eh19_input,
            manual_input=args.manual_input,
            output_dir=args.output_dir,
        )
        summary = execute(
            paths=paths,
            run_tests=not args.skip_self_tests,
        )
        print_summary(summary)
        return 0
    except Exception as exc:
        logging.exception("EH20 failed: %s", exc)
        print("=" * 112, file=sys.stderr)
        print(ENGINE_NAME, file=sys.stderr)
        print("=" * 112, file=sys.stderr)
        print(f"Status:                     FAIL", file=sys.stderr)
        print(f"Error:                      {exc}", file=sys.stderr)
        print("=" * 112, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
