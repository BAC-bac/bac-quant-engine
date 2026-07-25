#!/usr/bin/env python3
from __future__ import annotations

"""
BACQE Extended Horizons shared institutional foundation.

This module contains shared infrastructure used by Evidence Schema v2 engines.
It deliberately contains no engine-specific scientific calculations.
"""

import hashlib
import json
import os
import tempfile
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


FOUNDATION_ID = "EH00"
FOUNDATION_VERSION = "1.0.0"
EVIDENCE_SCHEMA_VERSION = "2.0"
METHODOLOGY_BOUNDARY_V2 = "v2_native"
METHODOLOGY_BOUNDARY_V1 = "v1_legacy"

ALLOWED_METHODOLOGY_BOUNDARIES = {
    METHODOLOGY_BOUNDARY_V1,
    METHODOLOGY_BOUNDARY_V2,
    "migrated",
}

ALLOWED_EVIDENCE_CLASSES = {
    "hypothesis_generation",
    "in_sample_robustness",
    "retrospective_persistence",
    "structural_analogue_transfer",
    "literal_parameter_transfer",
    "temporal_attribution",
    "walk_forward_validation",
    "independent_replication",
    "paper_trading",
    "live_observation",
}

ALLOWED_EVIDENCE_STATUSES = {
    "not_tested",
    "insufficient_data",
    "data_unavailable",
    "invalid_contract",
    "fail",
    "watchlist",
    "pass_secondary",
    "pass_primary",
}

EVIDENCE_STATUS_ORDER = {
    "invalid_contract": 0,
    "data_unavailable": 1,
    "insufficient_data": 2,
    "not_tested": 3,
    "fail": 4,
    "watchlist": 5,
    "pass_secondary": 6,
    "pass_primary": 7,
}


@dataclass(frozen=True)
class EngineMetadata:
    engine_id: str
    engine_name: str
    engine_version: str
    methodology_version: str
    evidence_schema_version: str = EVIDENCE_SCHEMA_VERSION
    methodology_boundary: str = METHODOLOGY_BOUNDARY_V2

    def validate(self) -> None:
        require_non_empty_text(self.engine_id, "engine_id")
        require_non_empty_text(self.engine_name, "engine_name")
        require_semantic_version(self.engine_version, "engine_version")
        require_non_empty_text(self.methodology_version, "methodology_version")
        require_semantic_version(
            self.evidence_schema_version,
            "evidence_schema_version",
            allow_two_part=True,
        )
        if self.methodology_boundary not in ALLOWED_METHODOLOGY_BOUNDARIES:
            raise ValueError(
                "Invalid methodology_boundary "
                f"{self.methodology_boundary!r}; expected one of "
                f"{sorted(ALLOWED_METHODOLOGY_BOUNDARIES)}"
            )

    def as_dict(self) -> dict[str, str]:
        self.validate()
        return asdict(self)


@dataclass(frozen=True)
class RunPaths:
    input_root: str
    output_root: str
    report_root: str

    @classmethod
    def from_paths(
        cls,
        input_root: Path,
        output_root: Path,
        report_root: Path,
    ) -> "RunPaths":
        return cls(
            input_root=str(input_root),
            output_root=str(output_root),
            report_root=str(report_root),
        )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_text() -> str:
    return utc_now().isoformat(timespec="seconds")


def require_non_empty_text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be non-empty text.")


def require_semantic_version(
    value: str,
    field_name: str,
    *,
    allow_two_part: bool = False,
) -> None:
    require_non_empty_text(value, field_name)
    parts = value.split(".")
    allowed_lengths = {2, 3} if allow_two_part else {3}
    if len(parts) not in allowed_lengths or any(not part.isdigit() for part in parts):
        expected = "MAJOR.MINOR or MAJOR.MINOR.PATCH" if allow_two_part else "MAJOR.MINOR.PATCH"
        raise ValueError(f"{field_name} must use {expected}: {value!r}")


def require_columns(
    frame: pd.DataFrame,
    required: Sequence[str],
    *,
    frame_name: str = "dataframe",
) -> None:
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {missing}")


def require_allowed_value(
    value: str,
    allowed: set[str],
    field_name: str,
) -> None:
    if value not in allowed:
        raise ValueError(
            f"Invalid {field_name} {value!r}; expected one of {sorted(allowed)}"
        )


def validate_evidence_status(value: str) -> None:
    require_allowed_value(value, ALLOWED_EVIDENCE_STATUSES, "evidence_status")


def validate_evidence_class(value: str) -> None:
    require_allowed_value(value, ALLOWED_EVIDENCE_CLASSES, "evidence_class")


def evidence_status_rank(value: str) -> int:
    validate_evidence_status(value)
    return EVIDENCE_STATUS_ORDER[value]


def normalise_positive_ints(
    values: Iterable[int],
    *,
    field_name: str,
) -> list[int]:
    normalised = sorted({int(value) for value in values})
    if not normalised:
        raise ValueError(f"{field_name} must contain at least one integer.")
    invalid = [value for value in normalised if value <= 0]
    if invalid:
        raise ValueError(f"{field_name} must contain positive integers: {invalid}")
    return normalised


def validate_numeric_series(
    series: pd.Series,
    *,
    field_name: str,
    allow_zero: bool = True,
    allow_negative: bool = True,
) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.isna().any():
        raise ValueError(
            f"{field_name} contains {int(numeric.isna().sum())} "
            "missing or non-numeric value(s)."
        )

    values = numeric.to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise ValueError(f"{field_name} contains non-finite values.")

    if not allow_negative and (values < 0).any():
        raise ValueError(f"{field_name} contains negative values.")

    if not allow_zero and (values == 0).any():
        raise ValueError(f"{field_name} contains zero values.")

    return numeric.astype(float).reset_index(drop=True)


def validate_timestamp_series(
    series: pd.Series,
    *,
    field_name: str,
    require_unique: bool = True,
    require_monotonic: bool = True,
) -> pd.Series:
    timestamps = pd.to_datetime(series, utc=True, errors="coerce")

    if timestamps.isna().any():
        raise ValueError(
            f"{field_name} contains {int(timestamps.isna().sum())} "
            "invalid timestamp value(s)."
        )

    if require_unique and timestamps.duplicated().any():
        raise ValueError(
            f"{field_name} contains {int(timestamps.duplicated().sum())} "
            "duplicate timestamp value(s)."
        )

    if require_monotonic and not timestamps.is_monotonic_increasing:
        raise ValueError(f"{field_name} is not chronologically increasing.")

    return timestamps.reset_index(drop=True)


def canonical_json_bytes(payload: Any) -> bytes:
    if is_dataclass(payload) and not isinstance(payload, type):
        payload = asdict(payload)
    text = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return text.encode("utf-8")


def deterministic_id(
    prefix: str,
    payload: Any,
    *,
    length: int = 20,
) -> str:
    require_non_empty_text(prefix, "prefix")
    if length < 8 or length > 64:
        raise ValueError("length must be between 8 and 64.")
    digest = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()[:length]
    return f"{prefix}_{digest}"


def file_fingerprint(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    if not path.is_file():
        raise ValueError(f"Not a file: {path}")

    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "size_bytes": stat.st_size,
        "modified_time_ns": stat.st_mtime_ns,
    }


def file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive.")

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        suffix=".tmp",
        delete=False,
        dir=path.parent,
    ) as handle:
        temporary = Path(handle.name)
        handle.write(text)

    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    atomic_write_text(
        path,
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )
        + "\n",
    )


def atomic_write_csv(path: Path, frame: pd.DataFrame) -> None:
    atomic_write_text(path, frame.to_csv(index=False))


def atomic_write_parquet(
    path: Path,
    frame: pd.DataFrame,
    *,
    index: bool = False,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        suffix=".parquet",
        delete=False,
        dir=path.parent,
    ) as handle:
        temporary = Path(handle.name)

    try:
        frame.to_parquet(temporary, index=index)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def build_run_manifest(
    metadata: EngineMetadata,
    *,
    overall_status: str,
    configuration: Mapping[str, Any],
    inputs: Sequence[Mapping[str, Any]],
    outputs: Sequence[Mapping[str, Any]],
    metrics: Mapping[str, Any] | None = None,
    warnings: Sequence[str] | None = None,
    errors: Sequence[str] | None = None,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    metadata.validate()
    require_non_empty_text(overall_status, "overall_status")

    return {
        **metadata.as_dict(),
        "foundation_id": FOUNDATION_ID,
        "foundation_version": FOUNDATION_VERSION,
        "generated_utc": generated_utc or utc_now_text(),
        "overall_status": overall_status,
        "configuration": dict(configuration),
        "inputs": [dict(item) for item in inputs],
        "outputs": [dict(item) for item in outputs],
        "metrics": dict(metrics or {}),
        "warnings": list(warnings or []),
        "errors": list(errors or []),
    }


def write_run_manifest(
    path: Path,
    metadata: EngineMetadata,
    *,
    overall_status: str,
    configuration: Mapping[str, Any],
    inputs: Sequence[Mapping[str, Any]],
    outputs: Sequence[Mapping[str, Any]],
    metrics: Mapping[str, Any] | None = None,
    warnings: Sequence[str] | None = None,
    errors: Sequence[str] | None = None,
) -> dict[str, Any]:
    manifest = build_run_manifest(
        metadata,
        overall_status=overall_status,
        configuration=configuration,
        inputs=inputs,
        outputs=outputs,
        metrics=metrics,
        warnings=warnings,
        errors=errors,
    )
    atomic_write_json(path, manifest)
    return manifest


def print_engine_header(
    metadata: EngineMetadata,
    *,
    fields: Mapping[str, Any] | None = None,
    width: int = 110,
) -> None:
    metadata.validate()
    print("=" * width)
    print(metadata.engine_name)
    print("=" * width)
    print(f"Engine ID:                  {metadata.engine_id}")
    print(f"Engine version:             {metadata.engine_version}")
    print(f"Evidence schema:            {metadata.evidence_schema_version}")
    print(f"Methodology version:        {metadata.methodology_version}")
    print(f"Methodology boundary:       {metadata.methodology_boundary}")

    for label, value in (fields or {}).items():
        print(f"{label + ':':<28}{value}")

    print("-" * width)


def run_foundation_self_tests() -> None:
    metadata = EngineMetadata(
        engine_id="EH99",
        engine_name="BACQE TEST ENGINE",
        engine_version="1.2.3",
        methodology_version="EH99_TEST_V1.0",
    )
    metadata.validate()

    assert normalise_positive_ints(
        [20, 10, 20],
        field_name="horizons",
    ) == [10, 20]

    assert deterministic_id("test", {"b": 2, "a": 1}) == deterministic_id(
        "test", {"a": 1, "b": 2}
    )

    validate_evidence_status("not_tested")
    validate_evidence_class("hypothesis_generation")
    assert evidence_status_rank("pass_primary") > evidence_status_rank("watchlist")

    frame = pd.DataFrame({"a": [1], "b": [2]})
    require_columns(frame, ["a", "b"])

    numeric = validate_numeric_series(
        pd.Series([1.0, 2.0]),
        field_name="test_numeric",
        allow_zero=False,
        allow_negative=False,
    )
    assert numeric.tolist() == [1.0, 2.0]

    timestamps = validate_timestamp_series(
        pd.Series(["2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z"]),
        field_name="test_timestamp",
    )
    assert len(timestamps) == 2

    manifest = build_run_manifest(
        metadata,
        overall_status="PASS",
        configuration={"mode": "self_test"},
        inputs=[],
        outputs=[],
        metrics={"tests": 1},
    )
    assert manifest["foundation_id"] == FOUNDATION_ID
    assert manifest["foundation_version"] == FOUNDATION_VERSION
    assert manifest["evidence_schema_version"] == EVIDENCE_SCHEMA_VERSION

    print("EH00 foundation deterministic self-tests passed.")
