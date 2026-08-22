"""BACQE DUKASCOPY 30 - continuous event-time forward horizons (D2)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from dukascopy_feature_contract import (
    APPROVED_TARGET_PRICE_BASIS,
    SCRIPT30_FORWARD_HORIZONS,
    TARGET_CONTRACT_VERSION,
    require_target,
    validate_feature_parquet,
    write_feature_parquet,
)


DEFAULT_SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")
HORIZONS = list(SCRIPT30_FORWARD_HORIZONS)


def get_input_root(symbol: str) -> Path:
    return QUANT_LAB / "data" / "processed" / "dukascopy_engineered_features" / f"symbol={symbol.upper().strip()}"


def get_output_root(symbol: str) -> Path:
    return QUANT_LAB / "data" / "processed" / "dukascopy_horizon_features" / f"symbol={symbol.upper().strip()}"


def get_report_root(symbol: str) -> Path:
    return QUANT_LAB / "data" / "analysis" / "dukascopy_horizon_expansion" / f"symbol={symbol.upper().strip()}"


def discover_files(input_root: Path) -> list[Path]:
    return sorted(input_root.rglob("*.parquet")) if input_root.exists() else []


def get_output_path(input_path: Path, output_root: Path) -> Path:
    year = next((part for part in input_path.parts if part.startswith("year=")), None)
    month = next((part for part in input_path.parts if part.startswith("month=")), None)
    name = input_path.name.replace("_engineered_features.parquet", "_horizon_features.parquet")
    return (output_root / year / month / name) if year and month else output_root / name


def _timestamps(frame: pd.DataFrame, label: str) -> pd.Series:
    if "timestamp_utc" not in frame:
        raise ValueError(f"{label} lacks timestamp_utc")
    values = pd.to_datetime(frame["timestamp_utc"], utc=True, errors="coerce")
    if values.isna().any() or not values.is_monotonic_increasing:
        raise ValueError(f"{label} has invalid/non-monotonic chronology")
    if values.duplicated().any():
        raise ValueError(f"{label} contains duplicate timestamps")
    return values


def add_forward_horizons(
    df: pd.DataFrame,
    horizons: list[int] | tuple[int, ...] | None = None,
) -> pd.DataFrame:
    horizons = list(horizons or HORIZONS)
    output = df.copy()
    for horizon in horizons:
        target = f"future_return_{int(horizon)}"
        require_target(target, approved_extra_targets=[target])
        output[target] = (
            output[APPROVED_TARGET_PRICE_BASIS].shift(-int(horizon))
            / output[APPROVED_TARGET_PRICE_BASIS]
            - 1.0
        )
    return output


def build_partition_targets(
    current: pd.DataFrame,
    *,
    future: pd.DataFrame | None = None,
    horizons: list[int] | tuple[int, ...] | None = None,
) -> pd.DataFrame:
    horizons = list(horizons or HORIZONS)
    if APPROVED_TARGET_PRICE_BASIS not in current:
        raise ValueError(f"Current partition lacks approved price basis {APPROVED_TARGET_PRICE_BASIS!r}")
    current = current.copy().reset_index(drop=True)
    current["timestamp_utc"] = _timestamps(current, "current partition")
    future_frame = pd.DataFrame(columns=current.columns)
    if future is not None and not future.empty:
        future_frame = future.copy().reset_index(drop=True)
        future_frame["timestamp_utc"] = _timestamps(future_frame, "future target support")
        future_frame = future_frame.head(max(horizons)).reset_index(drop=True)
    combined = pd.concat([current, future_frame], ignore_index=True)
    _timestamps(combined, "continuous target stream")
    combined = add_forward_horizons(combined, horizons)
    output = combined.iloc[: len(current)].copy().reset_index(drop=True)
    if not output["timestamp_utc"].equals(current["timestamp_utc"]):
        raise AssertionError("Script 30 changed current-partition row ownership")
    return output.replace([np.inf, -np.inf], np.nan)


def _ordered_files(files: list[Path]) -> list[Path]:
    bounds: list[tuple[pd.Timestamp, pd.Timestamp, Path]] = []
    for path in files:
        values = _timestamps(pd.read_parquet(path, columns=["timestamp_utc"]), str(path))
        if values.empty:
            raise ValueError(f"Empty engineered partition: {path}")
        bounds.append((values.iloc[0], values.iloc[-1], path))
    bounds.sort(key=lambda row: (row[0], str(row[2])))
    for previous, current in zip(bounds, bounds[1:]):
        if current[0] <= previous[1]:
            raise ValueError(f"Engineered partition overlap/reversal: {previous[2]} -> {current[2]}")
    return [row[2] for row in bounds]


def _future_rows(files: list[Path], index: int, required: int) -> tuple[pd.DataFrame, list[str]]:
    frames: list[pd.DataFrame] = []
    sources: list[str] = []
    available = 0
    for path in files[index + 1 :]:
        if available >= required:
            break
        frame = pd.read_parquet(path)
        take = min(required - available, len(frame))
        frames.append(frame.head(take))
        sources.append(str(path))
        available += take
    return (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()), sources


def run_horizon_expansion(
    symbol: str = DEFAULT_SYMBOL,
    horizons: list[int] | None = None,
    *,
    allow_production_write: bool = False,
) -> tuple[Path, Path]:
    symbol = symbol.upper().strip()
    horizons = sorted(set(int(value) for value in (horizons or HORIZONS)))
    input_root = get_input_root(symbol)
    output_root = get_output_root(symbol)
    report_root = get_report_root(symbol)
    files = _ordered_files(discover_files(input_root))
    if files and not allow_production_write:
        raise PermissionError(
            "D2 horizon regeneration is not authorised. Re-run with "
            "--allow-production-write only during an explicitly approved regeneration."
        )
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []
    for index, path in enumerate(files):
        certification = validate_feature_parquet(path)
        if not certification["certified"]:
            raise ValueError(f"Uncertified D2 feature input {path}: {certification['reason']}")
        current = pd.read_parquet(path)
        future, sources = _future_rows(files, index, max(horizons))
        output = build_partition_targets(current, future=future, horizons=horizons)
        output_path = get_output_path(path, output_root)
        write_feature_parquet(
            output,
            output_path,
            {
                **certification["metadata"],
                "source_partition": str(path),
                "output_row_owner": str(path),
                "target_semantics": "event_time_tick_count",
                "target_contract_version": TARGET_CONTRACT_VERSION,
                "target_horizons": json.dumps(horizons),
                "future_support_partitions": json.dumps(sources),
            },
        )
        rows.append({"status": "ok", "input_file": str(path), "output_file": str(output_path), "rows_in": len(current), "rows_out": len(output), "future_support_rows": len(future)})
    report_csv = report_root / f"{symbol}_horizon_expansion_d2_latest.csv"
    report_json = report_root / f"{symbol}_horizon_expansion_d2_manifest_latest.json"
    pd.DataFrame(rows).to_csv(report_csv, index=False)
    report_json.write_text(
        json.dumps(
            {
                "status": "PASS",
                "symbol": symbol,
                "target_contract_version": TARGET_CONTRACT_VERSION,
                "target_semantics": "event_time_tick_count",
                "target_price_basis": APPROVED_TARGET_PRICE_BASIS,
                "horizons": horizons,
                "partition_count": len(files),
                "production_regeneration_authorised": allow_production_write,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return report_csv, report_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build continuous event-time Dukascopy targets")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--allow-production-write", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_horizon_expansion(
        arguments.symbol,
        allow_production_write=arguments.allow_production_write,
    )
