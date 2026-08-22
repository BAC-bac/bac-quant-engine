"""BACQE Dukascopy 08 - certified date-range tick normalisation."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import lzma
from pathlib import Path
import struct

import pandas as pd

from dukascopy_contract import (
    NORMALISATION_SCHEMA_VERSION,
    SYMBOL_METADATA_SCHEMA_VERSION,
    DukascopySymbolMetadata,
    file_sha256,
    get_symbol_metadata,
    registry_fingerprint,
    write_normalised_parquet,
)


DATA_ROOT = Path(r"E:\Quant_Lab\data")
RAW_ROOT = DATA_ROOT / "raw" / "dukascopy_ticks"
PROCESSED_ROOT = DATA_ROOT / "processed" / "dukascopy_ticks"
REPORT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "normalisation_reports"
DEFAULT_SYMBOL = "EURUSD"
DEFAULT_START_DATE = "2023-01-01"
DEFAULT_END_DATE = "2025-12-31"
SOURCE = "dukascopy"
RECORD_SIZE = 20
EXPECTED_HOURS_PER_DAY = 24


@dataclass
class HourDecodeResult:
    hour: int
    path: Path
    status: str
    frame: pd.DataFrame
    compressed_bytes: int = 0
    decompressed_bytes: int = 0
    records_decoded: int = 0
    trailing_bytes: int = 0
    source_sha256: str = ""
    absence_classification: str = ""
    error: str = ""

    def report_row(self) -> dict:
        return {
            "hour": self.hour,
            "input_path": str(self.path),
            "status": self.status,
            "absence_classification": self.absence_classification,
            "compressed_bytes": self.compressed_bytes,
            "decompressed_bytes": self.decompressed_bytes,
            "records_decoded": self.records_decoded,
            "trailing_bytes": self.trailing_bytes,
            "source_sha256": self.source_sha256,
            "error": self.error,
        }


def date_range(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def raw_day_dir(symbol: str, dt: datetime) -> Path:
    return RAW_ROOT / f"symbol={symbol}" / f"year={dt.year:04d}" / f"month={dt.month:02d}"


def raw_bi5_path(symbol: str, dt: datetime, hour: int) -> Path:
    return raw_day_dir(symbol, dt) / f"{symbol}_{dt.strftime('%Y-%m-%d')}_{hour:02d}h_ticks.bi5"


def processed_output_path(
    symbol: str,
    dt: datetime,
    output_root: Path | None = None,
) -> Path:
    root = output_root or PROCESSED_ROOT
    return (
        root / f"symbol={symbol}" / f"year={dt.year:04d}" / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_ticks.parquet"
    )


def quality_report_path(symbol: str, dt: datetime) -> Path:
    return REPORT_ROOT / f"{symbol}_{dt.strftime('%Y-%m-%d')}_quality_report.csv"


def hour_coverage_report_path(symbol: str, dt: datetime) -> Path:
    return REPORT_ROOT / f"{symbol}_{dt.strftime('%Y-%m-%d')}_hour_coverage.csv"


def decode_hour_path(
    file_path: Path,
    symbol: str,
    dt: datetime,
    hour: int,
    metadata: DukascopySymbolMetadata,
) -> HourDecodeResult:
    if not file_path.exists():
        return HourDecodeResult(
            hour, file_path, "missing_file", pd.DataFrame(),
            absence_classification="market_closure_or_missing_acquisition_unresolved",
        )
    compressed_bytes = file_path.stat().st_size
    if compressed_bytes == 0:
        return HourDecodeResult(
            hour, file_path, "empty_file", pd.DataFrame(),
            absence_classification="empty_source_file",
        )
    source_hash = file_sha256(file_path)
    try:
        raw_bytes = lzma.decompress(file_path.read_bytes())
    except Exception as exc:
        return HourDecodeResult(
            hour, file_path, "decode_failure", pd.DataFrame(),
            compressed_bytes=compressed_bytes, source_sha256=source_hash, error=repr(exc),
        )
    trailing_bytes = len(raw_bytes) % RECORD_SIZE
    if trailing_bytes:
        return HourDecodeResult(
            hour, file_path, "decode_failure", pd.DataFrame(), compressed_bytes,
            len(raw_bytes), trailing_bytes=trailing_bytes, source_sha256=source_hash,
            error=f"decompressed payload is not divisible by {RECORD_SIZE}",
        )
    if not raw_bytes:
        return HourDecodeResult(
            hour, file_path, "empty_file", pd.DataFrame(), compressed_bytes,
            source_sha256=source_hash, absence_classification="empty_decompressed_payload",
        )

    base_time = datetime(dt.year, dt.month, dt.day, hour, tzinfo=timezone.utc)
    rows = []
    for offset in range(0, len(raw_bytes), RECORD_SIZE):
        time_delta_ms, ask_raw, bid_raw, ask_volume, bid_volume = struct.unpack(
            ">IIIff", raw_bytes[offset : offset + RECORD_SIZE]
        )
        ask = ask_raw / metadata.raw_price_scale
        bid = bid_raw / metadata.raw_price_scale
        rows.append({
            "timestamp_utc": base_time + timedelta(milliseconds=time_delta_ms),
            "symbol": symbol,
            "source": SOURCE,
            "bid": bid,
            "ask": ask,
            "mid": (bid + ask) / 2,
            "spread": ask - bid,
            "spread_points": (ask - bid) / metadata.point_size,
            "bid_volume": float(bid_volume),
            "ask_volume": float(ask_volume),
            "quote_volume": float(bid_volume + ask_volume),
            "hour": hour,
        })
    frame = pd.DataFrame(rows)
    return HourDecodeResult(
        hour, file_path, "decoded", frame, compressed_bytes, len(raw_bytes), len(frame),
        source_sha256=source_hash,
    )


def decode_hour_file(
    symbol: str,
    dt: datetime,
    hour: int,
    metadata: DukascopySymbolMetadata | None = None,
) -> HourDecodeResult:
    approved = metadata or get_symbol_metadata(symbol)
    return decode_hour_path(raw_bi5_path(approved.symbol, dt, hour), approved.symbol, dt, hour, approved)


def clean_ticks(
    frame: pd.DataFrame,
    metadata: DukascopySymbolMetadata,
) -> tuple[pd.DataFrame, dict]:
    report = {
        "original_rows": len(frame), "removed_null_timestamps": 0,
        "removed_null_prices": 0, "removed_non_positive_prices": 0,
        "removed_crossed_spread": 0, "removed_negative_volume": 0,
        "removed_duplicates": 0, "final_rows": 0,
    }
    cleaned = frame.copy()
    before = len(cleaned); cleaned = cleaned.dropna(subset=["timestamp_utc"])
    report["removed_null_timestamps"] = before - len(cleaned)
    before = len(cleaned); cleaned = cleaned.dropna(subset=["bid", "ask"])
    report["removed_null_prices"] = before - len(cleaned)
    before = len(cleaned); cleaned = cleaned[(cleaned["bid"] > 0) & (cleaned["ask"] > 0)]
    report["removed_non_positive_prices"] = before - len(cleaned)
    before = len(cleaned); cleaned = cleaned[cleaned["ask"] >= cleaned["bid"]]
    report["removed_crossed_spread"] = before - len(cleaned)
    before = len(cleaned); cleaned = cleaned[(cleaned["bid_volume"] >= 0) & (cleaned["ask_volume"] >= 0)]
    report["removed_negative_volume"] = before - len(cleaned)
    cleaned = cleaned.sort_values("timestamp_utc", kind="mergesort").reset_index(drop=True)
    before = len(cleaned)
    cleaned = cleaned.drop_duplicates(
        subset=["timestamp_utc", "bid", "ask", "bid_volume", "ask_volume"]
    ).reset_index(drop=True)
    report["removed_duplicates"] = before - len(cleaned)
    cleaned["mid"] = (cleaned["bid"] + cleaned["ask"]) / 2
    cleaned["spread"] = cleaned["ask"] - cleaned["bid"]
    cleaned["spread_points"] = cleaned["spread"] / metadata.point_size
    cleaned["quote_volume"] = cleaned["bid_volume"] + cleaned["ask_volume"]
    report["final_rows"] = len(cleaned)
    return cleaned, report


def build_coverage(results: list[HourDecodeResult]) -> dict:
    present = sum(result.status != "missing_file" for result in results)
    decoded = sum(result.status == "decoded" for result in results)
    missing = sum(result.status == "missing_file" for result in results)
    empty = sum(result.status == "empty_file" for result in results)
    failures = sum(result.status == "decode_failure" for result in results)
    if decoded == EXPECTED_HOURS_PER_DAY and not (missing or empty or failures):
        status = "complete_coverage"
    elif decoded:
        status = "incomplete_coverage"
    else:
        status = "no_decoded_data"
    return {
        "expected_hourly_files": EXPECTED_HOURS_PER_DAY,
        "files_present": present,
        "files_missing": missing,
        "empty_files": empty,
        "decode_failures": failures,
        "successfully_decoded_files": decoded,
        "rows_decoded": sum(result.records_decoded for result in results),
        "coverage_percentage": decoded / EXPECTED_HOURS_PER_DAY * 100.0,
        "coverage_status": status,
        "market_closure_classification": "unresolved_no_repository_calendar",
    }


def combined_source_fingerprint(results: list[HourDecodeResult]) -> str:
    parts = [f"{result.hour:02d}:{result.source_sha256}" for result in results if result.source_sha256]
    return sha256("|".join(parts).encode()).hexdigest() if parts else ""


def add_lineage_columns(
    frame: pd.DataFrame,
    metadata: DukascopySymbolMetadata,
    coverage: dict,
    source_fingerprint: str,
) -> pd.DataFrame:
    output = frame.copy()
    output["normalisation_schema_version"] = NORMALISATION_SCHEMA_VERSION
    output["symbol_metadata_version"] = SYMBOL_METADATA_SCHEMA_VERSION
    output["symbol_registry_fingerprint"] = registry_fingerprint()
    output["raw_price_scale"] = metadata.raw_price_scale
    output["point_size"] = metadata.point_size
    output["pip_size"] = metadata.pip_size
    output["coverage_status"] = coverage["coverage_status"]
    output["source_fingerprint"] = source_fingerprint
    return output


def build_quality_report(
    frame: pd.DataFrame,
    clean_report: dict,
    coverage: dict,
    metadata: DukascopySymbolMetadata,
    source_fingerprint: str,
    processing_start_date: str,
    processing_end_date: str,
) -> pd.DataFrame:
    row = {
        **clean_report, **coverage, "symbol": metadata.symbol,
        "normalisation_schema_version": NORMALISATION_SCHEMA_VERSION,
        "symbol_metadata_version": SYMBOL_METADATA_SCHEMA_VERSION,
        "symbol_registry_fingerprint": registry_fingerprint(),
        "raw_price_scale": metadata.raw_price_scale,
        "point_size": metadata.point_size, "pip_size": metadata.pip_size,
        "source_fingerprint": source_fingerprint,
        "processing_start_date": processing_start_date,
        "processing_end_date": processing_end_date,
        "rows_surviving_cleaning": len(frame),
        "first_timestamp_utc": frame["timestamp_utc"].min() if not frame.empty else None,
        "last_timestamp_utc": frame["timestamp_utc"].max() if not frame.empty else None,
        "avg_spread_points": frame["spread_points"].mean() if not frame.empty else None,
        "min_spread_points": frame["spread_points"].min() if not frame.empty else None,
        "max_spread_points": frame["spread_points"].max() if not frame.empty else None,
    }
    return pd.DataFrame([row])


def run_normalisation(
    symbol: str = DEFAULT_SYMBOL,
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    output_root: Path | None = None,
    overwrite_existing: bool = False,
) -> Path:
    metadata = get_symbol_metadata(symbol)  # fail before creating outputs
    symbol = metadata.symbol
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    print("=" * 90)
    print("BACQE DUKASCOPY 08 - CERTIFIED NORMALISATION")
    print(f"Symbol: {symbol} | Range: {start_date} to {end_date}")
    print(f"Scale: {metadata.raw_price_scale} | Point: {metadata.point_size}")
    range_rows = []
    for dt in date_range(start, end):
        hour_results = [decode_hour_file(symbol, dt, hour, metadata) for hour in range(24)]
        coverage = build_coverage(hour_results)
        source_fingerprint = combined_source_fingerprint(hour_results)
        frames = [result.frame for result in hour_results if result.status == "decoded"]
        raw_frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if raw_frame.empty:
            cleaned = pd.DataFrame()
            clean_report = {
                "original_rows": 0, "removed_null_timestamps": 0,
                "removed_null_prices": 0, "removed_non_positive_prices": 0,
                "removed_crossed_spread": 0, "removed_negative_volume": 0,
                "removed_duplicates": 0, "final_rows": 0,
            }
            output_path = None
        else:
            cleaned, clean_report = clean_ticks(raw_frame, metadata)
            if cleaned.empty:
                coverage = {**coverage, "coverage_status": "invalid_cleaned_output"}
                output_path = None
            else:
                cleaned = add_lineage_columns(cleaned, metadata, coverage, source_fingerprint)
                output_path = processed_output_path(symbol, dt, output_root=output_root)
                if output_path.exists() and not overwrite_existing:
                    raise FileExistsError(
                        f"Refusing to overwrite existing processed ticks: {output_path}. "
                        "Use a versioned --output-root for D1 regeneration."
                    )
                write_normalised_parquet(cleaned, output_path, {
                    "symbol": symbol, "raw_price_scale": metadata.raw_price_scale,
                    "point_size": metadata.point_size, "pip_size": metadata.pip_size,
                    "coverage_status": coverage["coverage_status"],
                    "source_fingerprint": source_fingerprint,
                    "processing_date": dt.strftime("%Y-%m-%d"),
                })

        REPORT_ROOT.mkdir(parents=True, exist_ok=True)
        quality_path = quality_report_path(symbol, dt)
        hour_path = hour_coverage_report_path(symbol, dt)
        build_quality_report(
            cleaned, clean_report, coverage, metadata, source_fingerprint, start_date, end_date
        ).to_csv(quality_path, index=False)
        pd.DataFrame([result.report_row() for result in hour_results]).to_csv(hour_path, index=False)
        range_rows.append({
            "symbol": symbol, "date": dt.strftime("%Y-%m-%d"), **coverage,
            "rows_surviving_cleaning": len(cleaned),
            "normalisation_schema_version": NORMALISATION_SCHEMA_VERSION,
            "symbol_metadata_version": SYMBOL_METADATA_SCHEMA_VERSION,
            "source_fingerprint": source_fingerprint,
            "output_path": str(output_path) if output_path else "",
            "quality_report_path": str(quality_path),
            "hour_coverage_report_path": str(hour_path),
        })
        print(
            f"[{dt.strftime('%Y-%m-%d')}] {coverage['coverage_status']} | "
            f"decoded_hours={coverage['successfully_decoded_files']}/24 | rows={len(cleaned):,}"
        )
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_ROOT / f"{symbol}_{start_date}_to_{end_date}_normalisation_report.csv"
    pd.DataFrame(range_rows).to_csv(report_path, index=False)
    print(f"[DONE] Range report: {report_path}")
    return report_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalise Dukascopy BI5 ticks under the certified D1 contract.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Optional versioned processed-tick root; existing files are not overwritten.",
    )
    parser.add_argument("--overwrite-existing", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_normalisation(
        args.symbol,
        args.start_date,
        args.end_date,
        output_root=args.output_root,
        overwrite_existing=args.overwrite_existing,
    )


if __name__ == "__main__":
    main()
