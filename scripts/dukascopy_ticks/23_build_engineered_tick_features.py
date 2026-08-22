"""BACQE DUKASCOPY 23 - continuous causal engineered tick features (D2)."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any

import numpy as np
import pandas as pd

from dukascopy_contract import (
    NORMALISATION_SCHEMA_VERSION,
    SYMBOL_METADATA_SCHEMA_VERSION,
    get_symbol_metadata,
    registry_fingerprint,
    validate_normalised_parquet,
)
from dukascopy_feature_contract import (
    APPROVED_TARGET_PRICE_BASIS,
    CAUSAL_LOOKBACK_SPEC_VERSION,
    FEATURE_ROLE_CONTRACT_VERSION,
    FEATURE_SCHEMA_VERSION,
    ROLLING_WINDOWS,
    SCRIPT23_FORWARD_HORIZONS,
    TARGET_CONTRACT_VERSION,
    feature_contract_fingerprint,
    maximum_causal_lookback,
    predictor_columns,
    write_feature_parquet,
)


DEFAULT_SYMBOL = "EURUSD"
DEFAULT_QUANT_LAB = Path(r"E:\Quant_Lab")
REQUIRED_INPUT_COLUMNS = {"timestamp_utc", "bid", "ask", "mid", "spread"}
DATE_PATTERN = re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)")


@dataclass(frozen=True)
class PartitionDescriptor:
    path: Path
    partition_date: str
    first_timestamp: pd.Timestamp
    last_timestamp: pd.Timestamp
    rows: int


def build_input_root(symbol: str, quant_lab: Path = DEFAULT_QUANT_LAB) -> Path:
    return quant_lab / "data" / "processed" / "dukascopy_ticks" / f"symbol={symbol}"


def build_output_root(symbol: str, quant_lab: Path = DEFAULT_QUANT_LAB) -> Path:
    return quant_lab / "data" / "processed" / "dukascopy_engineered_features" / f"symbol={symbol}"


def build_report_root(quant_lab: Path = DEFAULT_QUANT_LAB) -> Path:
    return quant_lab / "data" / "analysis" / "dukascopy_feature_engineering"


def discover_input_files(input_root: Path) -> list[Path]:
    return sorted(input_root.rglob("*.parquet")) if input_root.exists() else []


def get_output_path(input_path: Path, output_root: Path) -> Path:
    year = next((part for part in input_path.parts if part.startswith("year=")), None)
    month = next((part for part in input_path.parts if part.startswith("month=")), None)
    output_name = input_path.name.replace("_ticks.parquet", "_engineered_features.parquet")
    return (output_root / year / month / output_name) if year and month else output_root / output_name


def partition_date_from_path(path: Path) -> str:
    matches = sorted(set(DATE_PATTERN.findall(path.name)))
    if len(matches) != 1:
        raise ValueError(f"Ambiguous partition date in {path.name!r}: {matches}")
    return matches[0]


def validated_timestamps(frame: pd.DataFrame, *, label: str) -> pd.Series:
    if "timestamp_utc" not in frame:
        raise ValueError(f"{label} lacks timestamp_utc")
    timestamps = pd.to_datetime(frame["timestamp_utc"], utc=True, errors="coerce")
    if timestamps.isna().any():
        raise ValueError(f"{label} contains invalid/null timestamps")
    if not timestamps.is_monotonic_increasing:
        raise ValueError(f"{label} chronology is non-monotonic")
    if timestamps.duplicated().any():
        raise ValueError(f"{label} contains duplicate timestamps")
    return timestamps


def validate_partition_frame(
    frame: pd.DataFrame,
    *,
    label: str,
    expected_date: str | None = None,
) -> pd.DataFrame:
    missing = REQUIRED_INPUT_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"{label} missing required columns: {sorted(missing)}")
    output = frame.copy().reset_index(drop=True)
    timestamps = validated_timestamps(output, label=label)
    output["timestamp_utc"] = timestamps
    if expected_date is not None and not output.empty:
        owned_dates = set(timestamps.dt.strftime("%Y-%m-%d"))
        if owned_dates != {expected_date}:
            raise ValueError(
                f"{label} row ownership {sorted(owned_dates)} does not match {expected_date}"
            )
    return output


def describe_partition(path: Path) -> PartitionDescriptor:
    partition_date = partition_date_from_path(path)
    frame = pd.read_parquet(path, columns=["timestamp_utc"])
    timestamps = validated_timestamps(frame, label=str(path))
    if timestamps.empty:
        raise ValueError(f"Empty storage partition: {path}")
    owned_dates = set(timestamps.dt.strftime("%Y-%m-%d"))
    if owned_dates != {partition_date}:
        raise ValueError(f"{path} owns dates {sorted(owned_dates)}, expected {partition_date}")
    return PartitionDescriptor(path, partition_date, timestamps.iloc[0], timestamps.iloc[-1], len(frame))


def order_and_validate_partitions(paths: list[Path]) -> list[PartitionDescriptor]:
    descriptors = sorted(
        (describe_partition(path) for path in paths),
        key=lambda item: (item.first_timestamp, str(item.path)),
    )
    previous: PartitionDescriptor | None = None
    dates: set[str] = set()
    for item in descriptors:
        if item.partition_date in dates:
            raise ValueError(f"Ambiguous duplicate storage date {item.partition_date}")
        dates.add(item.partition_date)
        if previous is not None and item.first_timestamp <= previous.last_timestamp:
            raise ValueError(
                f"Partition chronology overlaps/reverses: {previous.path} -> {item.path}"
            )
        previous = item
    return descriptors


def add_basic_tick_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for lag in (1, 5, 10, 25):
        df[f"mid_return_{lag}"] = df["mid"].pct_change(lag, fill_method=None)
    df["mid_diff_1"] = df["mid"].diff()
    df["bid_diff_1"] = df["bid"].diff()
    df["ask_diff_1"] = df["ask"].diff()
    df["spread_change"] = df["spread"].diff()
    df["spread_pct_change"] = df["spread"].pct_change(fill_method=None)
    df["tick_direction"] = np.sign(df["mid_diff_1"]).fillna(0)
    df["up_tick"] = (df["tick_direction"] > 0).astype(int)
    df["down_tick"] = (df["tick_direction"] < 0).astype(int)
    df["flat_tick"] = (df["tick_direction"] == 0).astype(int)
    df["signed_tick"] = df["tick_direction"]
    df["absolute_mid_move"] = df["mid_diff_1"].abs()
    df["price_acceleration"] = df["mid_diff_1"].diff()
    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for window in ROLLING_WINDOWS:
        df[f"rolling_return_mean_{window}"] = df["mid_return_1"].rolling(window).mean()
        df[f"rolling_return_std_{window}"] = df["mid_return_1"].rolling(window).std()
        df[f"rolling_spread_mean_{window}"] = df["spread"].rolling(window).mean()
        df[f"rolling_spread_std_{window}"] = df["spread"].rolling(window).std()
        df[f"rolling_up_ticks_{window}"] = df["up_tick"].rolling(window).sum()
        df[f"rolling_down_ticks_{window}"] = df["down_tick"].rolling(window).sum()
        df[f"rolling_flat_ticks_{window}"] = df["flat_tick"].rolling(window).sum()
        up = df[f"rolling_up_ticks_{window}"]
        down = df[f"rolling_down_ticks_{window}"]
        df[f"tick_imbalance_{window}"] = up - down
        df[f"tick_imbalance_ratio_{window}"] = df[f"tick_imbalance_{window}"] / window
        df[f"buy_pressure_{window}"] = up / window
        df[f"sell_pressure_{window}"] = down / window
        df[f"order_flow_ratio_{window}"] = (up - down) / (up + down).replace(0, np.nan)
        df[f"rolling_abs_move_mean_{window}"] = df["absolute_mid_move"].rolling(window).mean()
        df[f"rolling_abs_move_sum_{window}"] = df["absolute_mid_move"].rolling(window).sum()
    return df


def add_zscore_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for window in ROLLING_WINDOWS:
        spread_mean = df["spread"].rolling(window).mean()
        spread_std = df["spread"].rolling(window).std().replace(0, np.nan)
        df[f"spread_zscore_{window}"] = (df["spread"] - spread_mean) / spread_std
        volatility = df[f"rolling_return_std_{window}"]
        vol_mean = volatility.rolling(window).mean()
        vol_std = volatility.rolling(window).std().replace(0, np.nan)
        df[f"volatility_zscore_{window}"] = (volatility - vol_mean) / vol_std
    return df


def add_causal_features(df: pd.DataFrame) -> pd.DataFrame:
    output = add_basic_tick_features(df)
    output = add_rolling_features(output)
    output = add_zscore_features(output)
    numeric = output.select_dtypes(include=[np.number]).columns
    output[numeric] = output[numeric].replace([np.inf, -np.inf], np.nan)
    return output


def add_forward_returns(
    df: pd.DataFrame,
    horizons: tuple[int, ...] = SCRIPT23_FORWARD_HORIZONS,
) -> pd.DataFrame:
    output = df.copy()
    for horizon in horizons:
        output[f"future_return_{horizon}"] = (
            output[APPROVED_TARGET_PRICE_BASIS].shift(-horizon)
            / output[APPROVED_TARGET_PRICE_BASIS]
            - 1.0
        )
    return output


def build_partition_output(
    current: pd.DataFrame,
    *,
    carry_in: pd.DataFrame | None = None,
    future: pd.DataFrame | None = None,
    target_horizons: tuple[int, ...] = SCRIPT23_FORWARD_HORIZONS,
) -> pd.DataFrame:
    current = validate_partition_frame(current, label="current partition")
    carry = (
        pd.DataFrame(columns=current.columns)
        if carry_in is None or carry_in.empty
        else validate_partition_frame(carry_in, label="carry-in")
    )
    future_frame = (
        pd.DataFrame(columns=current.columns)
        if future is None or future.empty
        else validate_partition_frame(future, label="future target support")
    )
    carry = carry.tail(maximum_causal_lookback()).reset_index(drop=True)
    future_frame = future_frame.head(max(target_horizons, default=0)).reset_index(drop=True)
    blocks = [frame for frame in (carry, current, future_frame) if not frame.empty]
    combined = pd.concat(blocks, ignore_index=True)
    validated_timestamps(combined, label="combined continuous stream")
    combined = add_causal_features(combined)
    combined = add_forward_returns(combined, target_horizons)
    start = len(carry)
    owned = combined.iloc[start : start + len(current)].copy().reset_index(drop=True)
    if not owned["timestamp_utc"].equals(current["timestamp_utc"].reset_index(drop=True)):
        raise AssertionError("Current-partition row ownership changed")
    predictor_columns(owned, fail_on_unknown_numeric=True)
    return owned


def _future_support(
    descriptors: list[PartitionDescriptor],
    index: int,
    rows: int,
) -> tuple[pd.DataFrame, list[str]]:
    frames: list[pd.DataFrame] = []
    sources: list[str] = []
    available = 0
    for descriptor in descriptors[index + 1 :]:
        if available >= rows:
            break
        frame = pd.read_parquet(descriptor.path)
        frame = validate_partition_frame(
            frame,
            label=str(descriptor.path),
            expected_date=descriptor.partition_date,
        )
        take = min(rows - available, len(frame))
        frames.append(frame.head(take))
        sources.append(str(descriptor.path))
        available += take
    return (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()), sources


def run_feature_engineering(
    symbol: str = DEFAULT_SYMBOL,
    quant_lab: Path = DEFAULT_QUANT_LAB,
    *,
    allow_production_write: bool = False,
) -> Path | None:
    approved = get_symbol_metadata(symbol)
    input_root = build_input_root(approved.symbol, quant_lab)
    output_root = build_output_root(approved.symbol, quant_lab)
    report_root = build_report_root(quant_lab)
    files = discover_input_files(input_root)
    if not files:
        return None
    if not allow_production_write:
        raise PermissionError(
            "D2 production regeneration is not authorised. Re-run with "
            "--allow-production-write only during an explicitly approved D1->D2 regeneration."
        )
    descriptors = order_and_validate_partitions(files)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    history = pd.DataFrame()
    results: list[dict[str, Any]] = []
    previous_date: pd.Timestamp | None = None
    for index, descriptor in enumerate(descriptors):
        certification = validate_normalised_parquet(
            descriptor.path,
            expected_symbol=approved.symbol,
        )
        if not certification["certified"]:
            raise ValueError(
                f"Uncertified D1 input {descriptor.path}: {certification['reason']}"
            )
        current = validate_partition_frame(
            pd.read_parquet(descriptor.path),
            label=str(descriptor.path),
            expected_date=descriptor.partition_date,
        )
        future, future_sources = _future_support(
            descriptors,
            index,
            max(SCRIPT23_FORWARD_HORIZONS),
        )
        carry = (
            history.tail(maximum_causal_lookback()).copy()
            if not history.empty
            else pd.DataFrame()
        )
        output = build_partition_output(current, carry_in=carry, future=future)
        output_path = get_output_path(descriptor.path, output_root)
        current_date = pd.Timestamp(descriptor.partition_date)
        calendar_gap_days = (
            0
            if previous_date is None
            else max(0, int((current_date - previous_date).days) - 1)
        )
        d1_footer = certification["metadata"]
        lineage = {
            "d1_normalisation_schema_version": d1_footer["normalisation_schema_version"],
            "d1_symbol_metadata_version": d1_footer["symbol_metadata_schema_version"],
            "d1_symbol_registry_fingerprint": d1_footer["symbol_registry_fingerprint"],
            "source_partition": str(descriptor.path),
            "source_partition_date": descriptor.partition_date,
            "source_partition_rows": len(current),
            "carry_in_rows": len(carry),
            "carry_in_first_timestamp": "" if carry.empty else carry["timestamp_utc"].iloc[0].isoformat(),
            "carry_in_last_timestamp": "" if carry.empty else carry["timestamp_utc"].iloc[-1].isoformat(),
            "future_support_partitions": json.dumps(future_sources),
            "output_row_owner": str(descriptor.path),
            "output_first_timestamp": output["timestamp_utc"].iloc[0].isoformat(),
            "output_last_timestamp": output["timestamp_utc"].iloc[-1].isoformat(),
            "unclassified_calendar_gap_days": calendar_gap_days,
        }
        write_feature_parquet(output, output_path, lineage)
        results.append(
            {
                "status": "ok",
                "input_file": str(descriptor.path),
                "output_file": str(output_path),
                "rows_in": len(current),
                "rows_out": len(output),
                "carry_in_rows": len(carry),
                "future_support_rows": len(future),
                "unclassified_calendar_gap_days": calendar_gap_days,
            }
        )
        history = pd.concat([history, current], ignore_index=True).tail(
            maximum_causal_lookback()
        ).reset_index(drop=True)
        previous_date = current_date
    report_path = report_root / (
        f"{approved.symbol}_dukascopy_feature_engineering_d2_latest.csv"
    )
    pd.DataFrame(results).to_csv(report_path, index=False)
    manifest = {
        "status": "PASS",
        "symbol": approved.symbol,
        "d1_normalisation_schema_version": NORMALISATION_SCHEMA_VERSION,
        "d1_symbol_metadata_version": SYMBOL_METADATA_SCHEMA_VERSION,
        "d1_symbol_registry_fingerprint": registry_fingerprint(),
        "d2_feature_schema_version": FEATURE_SCHEMA_VERSION,
        "feature_role_contract_version": FEATURE_ROLE_CONTRACT_VERSION,
        "causal_lookback_spec_version": CAUSAL_LOOKBACK_SPEC_VERSION,
        "target_contract_version": TARGET_CONTRACT_VERSION,
        "feature_contract_fingerprint": feature_contract_fingerprint(),
        "maximum_causal_lookback": maximum_causal_lookback(),
        "target_price_basis": APPROVED_TARGET_PRICE_BASIS,
        "partition_count": len(descriptors),
        "production_regeneration_authorised": allow_production_write,
    }
    manifest_path = report_root / (
        f"{approved.symbol}_dukascopy_feature_engineering_d2_manifest_latest.json"
    )
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return report_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build D2 continuous causal Dukascopy features"
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument(
        "--allow-production-write",
        action="store_true",
        help="Explicitly authorise a later D1->D2 production regeneration.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_feature_engineering(
        arguments.symbol,
        allow_production_write=arguments.allow_production_write,
    )
