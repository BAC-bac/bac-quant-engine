#!/usr/bin/env python3
from __future__ import annotations

"""
BACQE EH01 - Build Extended Horizon Targets
Evidence Schema v2 implementation using the EH00 shared foundation.

Target definition:
    future_return_h = future_price / current_price - 1

Return type:
    simple_return

Return unit:
    decimal_fraction
"""

import argparse
from pathlib import Path
import sys
from typing import Any

import numpy as np
import pandas as pd

DUKASCOPY_TICKS_DIR = Path(__file__).resolve().parents[1] / "dukascopy_ticks"
if str(DUKASCOPY_TICKS_DIR) not in sys.path:
    sys.path.insert(0, str(DUKASCOPY_TICKS_DIR))

from dukascopy_feature_contract import (  # noqa: E402
    APPROVED_TARGET_PRICE_BASIS,
    CAUSAL_LOOKBACK_SPEC_VERSION,
    FEATURE_ROLE_CONTRACT_VERSION,
    FEATURE_SCHEMA_VERSION,
    TARGET_CONTRACT_VERSION,
    feature_contract_fingerprint,
    predictor_columns,
    require_target,
)

from extended_horizons_foundation import (
    EngineMetadata,
    atomic_write_csv,
    atomic_write_parquet,
    deterministic_id,
    file_fingerprint,
    normalise_positive_ints,
    print_engine_header,
    require_columns,
    validate_numeric_series,
    validate_timestamp_series,
    write_run_manifest,
)


ENGINE_METADATA = EngineMetadata(
    engine_id="EH01",
    engine_name="BACQE EH01 - BUILD EXTENDED HORIZON TARGETS",
    engine_version="2.0.0",
    methodology_version="EH01_TARGETS_V2.0",
)

TARGET_DEFINITION_VERSION = "simple_forward_return_v2"
RETURN_TYPE = "simple_return"
RETURN_UNIT = "decimal_fraction"

DEFAULT_SYMBOL = "EURJPY"
DEFAULT_HORIZONS = [2500, 5000, 10000, 20000]

BASE_DIR = Path("E:/Quant_Lab")
INPUT_ROOT = BASE_DIR / "data" / "processed" / "dukascopy_engineered_features"
OUTPUT_ROOT = (
    BASE_DIR
    / "data"
    / "processed"
    / "dukascopy_extended_horizon_features"
)
REPORT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "target_build"
)

TARGET_PRICE_COLUMN = APPROVED_TARGET_PRICE_BASIS
TIMESTAMP_COLUMN_CANDIDATES = [
    "timestamp",
    "datetime",
    "date_time",
    "time",
    "utc_timestamp",
    "timestamp_utc",
    "event_time",
]


def detect_column(
    columns: list[str],
    candidates: list[str],
    *,
    label: str,
) -> str:
    lower_map = {column.lower(): column for column in columns}
    for candidate in candidates:
        if candidate in lower_map:
            return lower_map[candidate]

    raise ValueError(
        f"Could not detect {label} column. "
        f"Tried {candidates}; available columns: {columns[:40]}"
    )


def detect_timestamp_column(columns: list[str]) -> str | None:
    lower_map = {column.lower(): column for column in columns}
    for candidate in TIMESTAMP_COLUMN_CANDIDATES:
        if candidate in lower_map:
            return lower_map[candidate]
    return None


def find_parquet_files(symbol: str, input_root: Path) -> list[Path]:
    symbol_root = input_root / f"symbol={symbol}"

    if not symbol_root.exists():
        raise FileNotFoundError(f"Input symbol folder not found: {symbol_root}")

    files = sorted(
        path
        for path in symbol_root.rglob("*.parquet")
        if path.is_file()
    )

    if not files:
        raise FileNotFoundError(f"No parquet files found under: {symbol_root}")

    return files


def read_columns(path: Path, columns: list[str]) -> pd.DataFrame:
    try:
        return pd.read_parquet(path, columns=columns)
    except Exception:
        frame = pd.read_parquet(path)
        require_columns(frame, columns, frame_name=str(path))
        return frame[columns]


def read_validated_price_series(
    path: Path,
    price_column: str,
) -> pd.Series:
    frame = read_columns(path, [price_column])
    return validate_numeric_series(
        frame[price_column],
        field_name=f"{path}::{price_column}",
        allow_zero=False,
        allow_negative=False,
    )


def read_validated_timestamp_series(
    path: Path,
    timestamp_column: str,
) -> pd.Series:
    frame = read_columns(path, [timestamp_column])
    return validate_timestamp_series(
        frame[timestamp_column],
        field_name=f"{path}::{timestamp_column}",
    )


def temporal_bounds(
    path: Path,
    timestamp_column: str,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    timestamps = read_validated_timestamp_series(path, timestamp_column)

    if timestamps.empty:
        raise ValueError(f"{path} contains zero rows.")

    return timestamps.iloc[0], timestamps.iloc[-1]


def order_and_validate_files(
    files: list[Path],
    timestamp_column: str | None,
) -> tuple[
    list[Path],
    dict[Path, tuple[pd.Timestamp | None, pd.Timestamp | None]],
]:
    if timestamp_column is None:
        return files, {path: (None, None) for path in files}

    bounds: dict[
        Path,
        tuple[pd.Timestamp | None, pd.Timestamp | None],
    ] = {
        path: temporal_bounds(path, timestamp_column)
        for path in files
    }

    ordered = sorted(
        files,
        key=lambda path: (bounds[path][0], str(path)),
    )

    previous_path: Path | None = None
    previous_last: pd.Timestamp | None = None

    for path in ordered:
        first, last = bounds[path]

        if first is None or last is None:
            raise AssertionError("Timestamp bounds unexpectedly missing.")

        if previous_last is not None and first <= previous_last:
            raise ValueError(
                "Cross-file chronology overlap or reversal: "
                f"{previous_path} ends {previous_last}; "
                f"{path} starts {first}."
            )

        previous_path = path
        previous_last = last

    return ordered, bounds


def build_source_snapshot_id(
    *,
    symbol: str,
    horizons: list[int],
    price_column: str,
    timestamp_column: str | None,
    current_path: Path,
    future_paths_used: list[Path],
) -> str:
    payload = {
        "engine": ENGINE_METADATA.as_dict(),
        "target_definition_version": TARGET_DEFINITION_VERSION,
        "symbol": symbol,
        "horizons": horizons,
        "price_column": price_column,
        "timestamp_column": timestamp_column or "",
        "files": [
            file_fingerprint(path)
            for path in [current_path, *future_paths_used]
        ],
    }
    return deterministic_id("eh01src", payload)


def read_price_and_timestamp_block(
    path: Path,
    *,
    price_column: str,
    timestamp_column: str | None,
) -> tuple[pd.Series, pd.Series | None]:
    columns = [price_column]
    if timestamp_column:
        columns.append(timestamp_column)

    frame = read_columns(path, columns)

    prices = validate_numeric_series(
        frame[price_column],
        field_name=f"{path}::{price_column}",
        allow_zero=False,
        allow_negative=False,
    )

    timestamps = None
    if timestamp_column:
        timestamps = validate_timestamp_series(
            frame[timestamp_column],
            field_name=f"{path}::{timestamp_column}",
        )

    return prices, timestamps


def build_targets_for_file(
    *,
    current_path: Path,
    future_paths: list[Path],
    horizons: list[int],
    price_column: str,
    timestamp_column: str | None,
    symbol: str,
    file_index: int,
    bounds: dict[
        Path,
        tuple[pd.Timestamp | None, pd.Timestamp | None],
    ],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    current_frame = pd.read_parquet(current_path)

    if current_frame.empty:
        raise ValueError(f"Current file is empty: {current_path}")

    require_columns(
        current_frame,
        [price_column],
        frame_name=str(current_path),
    )

    current_prices = validate_numeric_series(
        current_frame[price_column],
        field_name=f"{current_path}::{price_column}",
        allow_zero=False,
        allow_negative=False,
    )

    current_timestamps = None
    if timestamp_column:
        require_columns(
            current_frame,
            [timestamp_column],
            frame_name=str(current_path),
        )
        current_timestamps = validate_timestamp_series(
            current_frame[timestamp_column],
            field_name=f"{current_path}::{timestamp_column}",
        )

    max_horizon = max(horizons)

    # Evidence Schema v2 correction:
    # the combined sequence must contain the full current file plus enough
    # future observations to calculate the maximum requested horizon.
    required_rows = len(current_frame) + max_horizon
    rows_available = len(current_frame)

    price_blocks = [current_prices]
    timestamp_blocks = (
        [current_timestamps]
        if current_timestamps is not None
        else []
    )
    future_paths_used: list[Path] = []

    for future_path in future_paths:
        if rows_available >= required_rows:
            break

        future_prices, future_timestamps = read_price_and_timestamp_block(
            future_path,
            price_column=price_column,
            timestamp_column=timestamp_column,
        )

        price_blocks.append(future_prices)
        rows_available += len(future_prices)
        future_paths_used.append(future_path)

        if future_timestamps is not None:
            timestamp_blocks.append(future_timestamps)

    combined_prices = pd.concat(price_blocks, ignore_index=True)

    if timestamp_column:
        combined_timestamps = pd.concat(
            timestamp_blocks,
            ignore_index=True,
        )

        if not combined_timestamps.is_monotonic_increasing:
            raise ValueError(
                f"Combined chronology is not increasing for {current_path}."
            )

        if combined_timestamps.duplicated().any():
            raise ValueError(
                f"Combined chronology contains duplicate timestamps "
                f"for {current_path}."
            )

    first_timestamp, last_timestamp = bounds[current_path]

    report: dict[str, Any] = {
        **ENGINE_METADATA.as_dict(),
        "target_definition_version": TARGET_DEFINITION_VERSION,
        "return_type": RETURN_TYPE,
        "return_unit": RETURN_UNIT,
        "target_price_basis": price_column,
        "symbol": symbol,
        "file_index": file_index,
        "input_file": str(current_path),
        "output_file": "",
        "status": "",
        "error": "",
        "rows": len(current_frame),
        "timestamp_column": timestamp_column or "",
        "input_first_timestamp_utc": (
            first_timestamp.isoformat()
            if first_timestamp is not None
            else ""
        ),
        "input_last_timestamp_utc": (
            last_timestamp.isoformat()
            if last_timestamp is not None
            else ""
        ),
        "future_files_used": len(future_paths_used),
        "rows_available_for_forward_calculation": len(combined_prices),
        "required_rows_for_max_horizon": required_rows,
        "source_snapshot_id": build_source_snapshot_id(
            symbol=symbol,
            horizons=horizons,
            price_column=price_column,
            timestamp_column=timestamp_column,
            current_path=current_path,
            future_paths_used=future_paths_used,
        ),
    }

    for horizon in horizons:
        target_column = f"future_return_{horizon}"

        values = (
            combined_prices.shift(-horizon)
            / combined_prices
            - 1.0
        ).iloc[: len(current_frame)].to_numpy(dtype=float)

        current_frame[target_column] = values

        valid_count = int(np.isfinite(values).sum())
        report[f"{target_column}_valid"] = valid_count
        report[f"{target_column}_missing"] = (
            len(values) - valid_count
        )

    return current_frame, report


def output_path_for(
    *,
    symbol: str,
    input_path: Path,
    input_root: Path,
    output_root: Path,
) -> Path:
    input_symbol_root = input_root / f"symbol={symbol}"
    output_symbol_root = output_root / f"symbol={symbol}"

    return (
        output_symbol_root
        / input_path.relative_to(input_symbol_root)
    )


def run_engine_self_tests() -> None:
    prices = pd.Series([100.0, 101.0, 102.0, 103.0])
    returns = prices.shift(-2) / prices - 1.0

    assert abs(float(returns.iloc[0]) - 0.02) < 1e-12
    assert pd.isna(returns.iloc[2])

    horizons = normalise_positive_ints(
        [20, 10, 20],
        field_name="horizons",
    )
    assert horizons == [10, 20]

    current_rows = 100
    max_horizon = 20
    required_rows = current_rows + max_horizon
    assert required_rows == 120
    assert current_rows < required_rows

    first = deterministic_id(
        "eh01test",
        {"symbol": "EURJPY", "horizon": 2500},
    )
    second = deterministic_id(
        "eh01test",
        {"horizon": 2500, "symbol": "EURJPY"},
    )
    assert first == second

    print("EH01 deterministic self-tests passed.")


def main(
    *,
    symbol: str,
    horizons: list[int],
    input_root: Path,
    output_root: Path,
    report_root: Path,
    price_column: str = TARGET_PRICE_COLUMN,
) -> int:
    symbol = symbol.upper().strip()
    horizons = normalise_positive_ints(
        horizons,
        field_name="horizons",
    )

    print_engine_header(
        ENGINE_METADATA,
        fields={
            "Target definition": TARGET_DEFINITION_VERSION,
            "Return type": RETURN_TYPE,
            "Return unit": RETURN_UNIT,
            "Symbol": symbol,
            "Horizons": horizons,
            "Input root": input_root,
            "Output root": output_root,
            "Report root": report_root,
        },
    )

    print("Running deterministic self-tests.")
    run_engine_self_tests()

    files = find_parquet_files(symbol, input_root)

    if price_column != TARGET_PRICE_COLUMN:
        raise ValueError(
            f"EH01 target price basis must be {TARGET_PRICE_COLUMN!r} under the D2 contract; "
            f"got {price_column!r}."
        )

    first_frame = pd.read_parquet(files[0])
    if first_frame.empty:
        raise ValueError(f"First input file is empty: {files[0]}")

    require_columns(
        first_frame,
        [price_column],
        frame_name=str(files[0]),
    )
    predictor_columns(first_frame, fail_on_unknown_numeric=True)
    timestamp_column = detect_timestamp_column(
        list(first_frame.columns)
    )

    files, bounds = order_and_validate_files(
        files,
        timestamp_column,
    )

    print(f"Files found:                {len(files)}")
    print(f"Price column:               {price_column}")
    print(
        f"Timestamp column:           "
        f"{timestamp_column or 'not detected'}"
    )

    warnings: list[str] = []
    if timestamp_column is None:
        warning = (
            "No timestamp column detected; cross-file chronology "
            "could not be independently verified."
        )
        warnings.append(warning)
        print(f"WARNING: {warning}")

    print("-" * 110)

    report_rows: list[dict[str, Any]] = []
    output_records: list[dict[str, Any]] = []
    errors: list[str] = []

    successful = 0
    failed = 0

    for zero_index, current_path in enumerate(files):
        file_index = zero_index + 1

        try:
            output_frame, report = build_targets_for_file(
                current_path=current_path,
                future_paths=files[zero_index + 1 :],
                horizons=horizons,
                price_column=price_column,
                timestamp_column=timestamp_column,
                symbol=symbol,
                file_index=file_index,
                bounds=bounds,
            )
            for horizon in horizons:
                target_name = f"future_return_{horizon}"
                require_target(
                    target_name,
                    approved_extra_targets=[target_name],
                )

            output_path = output_path_for(
                symbol=symbol,
                input_path=current_path,
                input_root=input_root,
                output_root=output_root,
            )
            atomic_write_parquet(output_path, output_frame)

            report["status"] = "ok"
            report["output_file"] = str(output_path)
            successful += 1

            output_records.append(
                {
                    "path": str(output_path),
                    "rows": len(output_frame),
                    "source_snapshot_id": report[
                        "source_snapshot_id"
                    ],
                }
            )

            validity = " ".join(
                f"h{h}="
                f"{report[f'future_return_{h}_valid']}"
                for h in horizons
            )

            print(
                f"[OK] {file_index:>4}/{len(files)} "
                f"rows={len(output_frame):>8} "
                f"future_files={report['future_files_used']:>3} "
                f"{validity}"
            )

        except Exception as exc:
            error_text = f"{type(exc).__name__}: {exc}"
            errors.append(
                f"{current_path}: {error_text}"
            )
            failed += 1

            report = {
                **ENGINE_METADATA.as_dict(),
                "target_definition_version": (
                    TARGET_DEFINITION_VERSION
                ),
                "return_type": RETURN_TYPE,
                "return_unit": RETURN_UNIT,
                "target_price_basis": price_column,
                "symbol": symbol,
                "file_index": file_index,
                "input_file": str(current_path),
                "output_file": "",
                "status": "error",
                "error": error_text,
            }

            print(
                f"[ERROR] {file_index:>4}/{len(files)} "
                f"{current_path.name} :: {error_text}"
            )

        report_rows.append(report)

    report_frame = pd.DataFrame(report_rows)

    report_path = (
        report_root
        / f"{symbol.lower()}_extended_horizon_target_build_latest.csv"
    )
    manifest_path = (
        report_root
        / f"{symbol.lower()}_extended_horizon_target_build_manifest_latest.json"
    )

    atomic_write_csv(report_path, report_frame)

    overall_status = "PASS" if failed == 0 else "FAIL"

    input_records = [
        file_fingerprint(path)
        for path in files
    ]

    write_run_manifest(
        manifest_path,
        ENGINE_METADATA,
        overall_status=overall_status,
        configuration={
            "symbol": symbol,
            "horizons": horizons,
            "target_definition_version": TARGET_DEFINITION_VERSION,
            "return_type": RETURN_TYPE,
            "return_unit": RETURN_UNIT,
            "target_price_basis": price_column,
            "d2_feature_schema_version": FEATURE_SCHEMA_VERSION,
            "feature_role_contract_version": FEATURE_ROLE_CONTRACT_VERSION,
            "causal_lookback_spec_version": CAUSAL_LOOKBACK_SPEC_VERSION,
            "target_contract_version": TARGET_CONTRACT_VERSION,
            "feature_contract_fingerprint": feature_contract_fingerprint(),
            "timestamp_column": timestamp_column or "",
            "input_root": str(input_root),
            "output_root": str(output_root),
            "report_root": str(report_root),
        },
        inputs=input_records,
        outputs=[
            *output_records,
            {
                "path": str(report_path),
                "type": "target_build_report",
                "rows": len(report_frame),
            },
        ],
        metrics={
            "input_file_count": len(files),
            "successful_file_count": successful,
            "failed_file_count": failed,
        },
        warnings=warnings,
        errors=errors,
    )

    print("-" * 110)
    print(f"Successful files:           {successful}")
    print(f"Failed files:               {failed}")
    print(f"Report:                     {report_path}")
    print(f"Manifest:                   {manifest_path}")
    print(f"Overall status:             {overall_status}")
    print("=" * 110)

    return 0 if failed == 0 else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BACQE EH01 - Build Extended Horizon Targets"
    )
    parser.add_argument(
        "--symbol",
        default=DEFAULT_SYMBOL,
    )
    parser.add_argument(
        "--horizons",
        nargs="+",
        type=int,
        default=DEFAULT_HORIZONS,
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=INPUT_ROOT,
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=OUTPUT_ROOT,
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=REPORT_ROOT,
    )
    parser.add_argument(
        "--price-column",
        default=TARGET_PRICE_COLUMN,
        choices=[TARGET_PRICE_COLUMN],
        help="Explicit D2-approved target price basis.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()

    if arguments.self_test:
        run_engine_self_tests()
        raise SystemExit(0)

    raise SystemExit(
        main(
            symbol=arguments.symbol,
            horizons=arguments.horizons,
            input_root=arguments.input_root,
            output_root=arguments.output_root,
            report_root=arguments.report_root,
            price_column=arguments.price_column,
        )
    )
