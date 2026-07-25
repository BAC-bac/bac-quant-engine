"""Deterministic ingestion for MQL5 Convex Survival attribution CSV files."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .schemas import (
    ALLOWED_BIAS_VALUES,
    BOOLEAN_COLUMNS,
    ENGINE_VERSION,
    REQUIRED_COLUMNS,
    SCHEMA_VERSION,
)


class AttributionIngestionError(RuntimeError):
    """Raised when source evidence fails a mandatory validation."""


@dataclass(frozen=True)
class IngestionResult:
    source_csv: str
    source_sha256: str
    staged_parquet: str
    audit_json: str
    audit_report: str
    row_count: int
    column_count: int
    duplicate_rows: int
    run_ids: tuple[str, ...]
    symbols: tuple[str, ...]
    timeframes: tuple[str, ...]
    min_bar_time: str
    max_bar_time: str
    all_pass_rows: int
    sole_veto_rows: int


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _normalise_text(series: pd.Series) -> pd.Series:
    values = series.astype("string").str.strip()
    return values.replace({"": pd.NA})


def _parse_boolean_column(series: pd.Series, column: str) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    invalid_mask = numeric.notna() & ~numeric.isin([0, 1])
    if invalid_mask.any():
        examples = numeric.loc[invalid_mask].drop_duplicates().head(5).tolist()
        raise AttributionIngestionError(
            f"Column '{column}' contains values outside 0/1: {examples}"
        )
    if numeric.isna().any():
        raise AttributionIngestionError(
            f"Column '{column}' contains {int(numeric.isna().sum())} missing or malformed values."
        )
    return numeric.astype("boolean")


def load_and_validate(source_csv: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not source_csv.exists():
        raise AttributionIngestionError(f"Source CSV does not exist: {source_csv}")
    if not source_csv.is_file():
        raise AttributionIngestionError(f"Source path is not a file: {source_csv}")

    try:
        frame = pd.read_csv(source_csv, low_memory=False)
    except Exception as exc:
        raise AttributionIngestionError(f"Unable to read CSV: {exc}") from exc

    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise AttributionIngestionError(
            "Missing mandatory columns: " + ", ".join(missing_columns)
        )

    if frame.empty:
        raise AttributionIngestionError("Source CSV contains no data rows.")

    original_column_order = frame.columns.tolist()
    unknown_columns = sorted(set(frame.columns) - set(REQUIRED_COLUMNS))

    for column in ("run_id", "symbol", "timeframe", "bias"):
        frame[column] = _normalise_text(frame[column])
        if frame[column].isna().any():
            raise AttributionIngestionError(
                f"Mandatory identity column '{column}' contains missing values."
            )

    frame["bias"] = frame["bias"].str.upper()
    invalid_biases = sorted(set(frame["bias"].dropna()) - ALLOWED_BIAS_VALUES)
    if invalid_biases:
        raise AttributionIngestionError(
            f"Unexpected bias values: {invalid_biases}. Allowed: {sorted(ALLOWED_BIAS_VALUES)}"
        )

    frame["bar_time"] = pd.to_datetime(
        frame["bar_time"], format="%Y.%m.%d %H:%M", errors="coerce", utc=True
    )
    malformed_times = int(frame["bar_time"].isna().sum())
    if malformed_times:
        raise AttributionIngestionError(
            f"bar_time contains {malformed_times} malformed timestamps."
        )

    numeric_columns = (
        "spread_points",
        "atr",
        "atr_ma",
        "atr_long_ma",
        "atr_past_expansion",
        "atr_past_rising",
        "adx",
        "ema_sep_atr_ratio",
        "fail_count",
    )
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        if frame[column].isna().any():
            raise AttributionIngestionError(
                f"Numeric column '{column}' contains "
                f"{int(frame[column].isna().sum())} missing or malformed values."
            )

    frame["spread_points"] = frame["spread_points"].astype("int64")
    frame["fail_count"] = frame["fail_count"].astype("int64")
    if (frame["fail_count"] < 0).any():
        raise AttributionIngestionError("fail_count contains negative values.")

    for column in BOOLEAN_COLUMNS:
        frame[column] = _parse_boolean_column(frame[column], column)

    for column in ("sole_veto", "first_veto", "all_vetoes"):
        frame[column] = _normalise_text(frame[column])

    # Attribution invariants.
    contradiction_all_pass = frame["all_pass"] & (frame["fail_count"] != 0)
    contradiction_failed = (~frame["all_pass"]) & (frame["fail_count"] == 0)
    sole_veto_mismatch = frame["sole_veto"].notna() & (frame["fail_count"] != 1)
    first_veto_missing = (~frame["all_pass"]) & frame["first_veto"].isna()
    vetoes_missing = (~frame["all_pass"]) & frame["all_vetoes"].isna()

    invariant_failures = {
        "all_pass_with_nonzero_fail_count": int(contradiction_all_pass.sum()),
        "failed_with_zero_fail_count": int(contradiction_failed.sum()),
        "sole_veto_when_fail_count_not_one": int(sole_veto_mismatch.sum()),
        "failed_rows_missing_first_veto": int(first_veto_missing.sum()),
        "failed_rows_missing_all_vetoes": int(vetoes_missing.sum()),
    }
    if any(invariant_failures.values()):
        raise AttributionIngestionError(
            "Attribution invariant failure: " + json.dumps(invariant_failures, sort_keys=True)
        )

    duplicate_rows = int(frame.duplicated().sum())
    duplicate_keys = int(
        frame.duplicated(subset=["run_id", "bar_time", "symbol", "timeframe"], keep=False).sum()
    )

    frame = frame.sort_values(
        ["run_id", "symbol", "timeframe", "bar_time"], kind="mergesort"
    ).reset_index(drop=True)

    audit = {
        "schema_version": SCHEMA_VERSION,
        "engine_version": ENGINE_VERSION,
        "row_count": int(len(frame)),
        "column_count": int(len(frame.columns)),
        "original_column_order": original_column_order,
        "unknown_columns_preserved": unknown_columns,
        "duplicate_rows": duplicate_rows,
        "duplicate_identity_keys": duplicate_keys,
        "run_ids": sorted(frame["run_id"].unique().tolist()),
        "symbols": sorted(frame["symbol"].unique().tolist()),
        "timeframes": sorted(frame["timeframe"].unique().tolist()),
        "bias_values": sorted(frame["bias"].unique().tolist()),
        "min_bar_time": frame["bar_time"].min().isoformat(),
        "max_bar_time": frame["bar_time"].max().isoformat(),
        "all_pass_rows": int(frame["all_pass"].sum()),
        "sole_veto_rows": int(frame["sole_veto"].notna().sum()),
        "null_counts": {column: int(value) for column, value in frame.isna().sum().items()},
        "invariant_failures": invariant_failures,
    }
    return frame, audit


def _safe_token(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value.strip())
    return cleaned.strip("_") or "unknown"


def _write_text_report(path: Path, audit: dict[str, Any]) -> None:
    lines = [
        "=" * 88,
        "BACQE CONVEX SURVIVAL - ENGINE 01 FILTER ATTRIBUTION INGESTION",
        "=" * 88,
        f"Generated UTC:              {audit['generated_utc']}",
        f"Engine version:             {audit['engine_version']}",
        f"Schema version:             {audit['schema_version']}",
        f"Source CSV:                 {audit['source_csv']}",
        f"Source SHA-256:             {audit['source_sha256']}",
        f"Source size bytes:          {audit['source_size_bytes']}",
        f"Rows:                       {audit['row_count']:,}",
        f"Columns:                    {audit['column_count']}",
        f"Duplicate rows:             {audit['duplicate_rows']:,}",
        f"Duplicate identity keys:    {audit['duplicate_identity_keys']:,}",
        f"Run IDs:                    {', '.join(audit['run_ids'])}",
        f"Symbols:                    {', '.join(audit['symbols'])}",
        f"Timeframes:                 {', '.join(audit['timeframes'])}",
        f"Bias values:                {', '.join(audit['bias_values'])}",
        f"Minimum bar time:           {audit['min_bar_time']}",
        f"Maximum bar time:           {audit['max_bar_time']}",
        f"All-pass rows:              {audit['all_pass_rows']:,}",
        f"Sole-veto rows:             {audit['sole_veto_rows']:,}",
        f"Unknown columns preserved:  {', '.join(audit['unknown_columns_preserved']) or 'None'}",
        f"Staged Parquet:             {audit['staged_parquet']}",
        "-" * 88,
        "VALIDATION RESULT: PASS",
        "Raw source was not modified. The staged dataset is deterministic and lineage-stamped.",
        "=" * 88,
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def ingest_attribution_csv(
    source_csv: Path,
    staging_root: Path,
    report_root: Path,
) -> IngestionResult:
    source_csv = source_csv.expanduser().resolve()
    staging_root = staging_root.expanduser().resolve()
    report_root = report_root.expanduser().resolve()

    frame, audit = load_and_validate(source_csv)
    source_hash = sha256_file(source_csv)
    generated_utc = datetime.now(timezone.utc).isoformat()

    run_token = _safe_token("-".join(audit["run_ids"]))
    symbol_token = _safe_token("-".join(audit["symbols"]))
    timeframe_token = _safe_token("-".join(audit["timeframes"]))
    stem = f"{run_token}_{symbol_token}_{timeframe_token}_filter_attribution"

    staging_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    parquet_path = staging_root / f"{stem}.parquet"
    audit_json_path = report_root / f"{stem}_ingestion_audit.json"
    audit_report_path = report_root / f"{stem}_ingestion_report.txt"

    frame["source_sha256"] = source_hash
    frame["ingestion_schema_version"] = SCHEMA_VERSION

    try:
        frame.to_parquet(parquet_path, index=False)
    except ImportError as exc:
        raise AttributionIngestionError(
            "Parquet support is unavailable. Install pyarrow with: python -m pip install pyarrow"
        ) from exc
    except Exception as exc:
        raise AttributionIngestionError(f"Unable to write staged Parquet: {exc}") from exc

    # Read-back verification guards against a superficially successful but unusable output.
    try:
        verified = pd.read_parquet(parquet_path)
    except Exception as exc:
        raise AttributionIngestionError(f"Unable to read back staged Parquet: {exc}") from exc
    if len(verified) != len(frame):
        raise AttributionIngestionError(
            f"Parquet row-count verification failed: wrote {len(frame)}, read {len(verified)}."
        )

    audit.update(
        {
            "generated_utc": generated_utc,
            "source_csv": str(source_csv),
            "source_sha256": source_hash,
            "source_size_bytes": source_csv.stat().st_size,
            "staged_parquet": str(parquet_path),
            "staged_parquet_size_bytes": parquet_path.stat().st_size,
            "parquet_readback_rows": int(len(verified)),
            "status": "PASS",
        }
    )
    audit_json_path.write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
    _write_text_report(audit_report_path, audit)

    return IngestionResult(
        source_csv=str(source_csv),
        source_sha256=source_hash,
        staged_parquet=str(parquet_path),
        audit_json=str(audit_json_path),
        audit_report=str(audit_report_path),
        row_count=int(audit["row_count"]),
        column_count=int(audit["column_count"]),
        duplicate_rows=int(audit["duplicate_rows"]),
        run_ids=tuple(audit["run_ids"]),
        symbols=tuple(audit["symbols"]),
        timeframes=tuple(audit["timeframes"]),
        min_bar_time=str(audit["min_bar_time"]),
        max_bar_time=str(audit["max_bar_time"]),
        all_pass_rows=int(audit["all_pass_rows"]),
        sole_veto_rows=int(audit["sole_veto_rows"]),
    )


def result_as_dict(result: IngestionResult) -> dict[str, Any]:
    return asdict(result)
