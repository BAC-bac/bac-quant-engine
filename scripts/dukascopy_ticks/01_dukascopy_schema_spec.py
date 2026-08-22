"""
BACQE DUKASCOPY 01 - SCHEMA SPECIFICATION

Purpose:
    Define the expected BACQE-compatible schema for Dukascopy historical tick data.

This script does not download data.
It documents the target structure before we build the downloader and normaliser.
"""

from pathlib import Path
from pprint import pprint

from dukascopy_contract import (
    NORMALISATION_SCHEMA_VERSION,
    SYMBOL_METADATA_SCHEMA_VERSION,
    certified_symbols,
    registry_payload,
)


# =============================================================================
# PROJECT PATHS
# =============================================================================

DATA_ROOT = Path(r"E:\Quant_Lab\data")

RAW_DUKASCOPY_ROOT = DATA_ROOT / "raw" / "dukascopy_ticks"
PROCESSED_DUKASCOPY_ROOT = DATA_ROOT / "processed" / "dukascopy_ticks"
ANALYSIS_DUKASCOPY_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks"


# =============================================================================
# INITIAL RESEARCH SCOPE
# =============================================================================

SOURCE_NAME = "dukascopy"

INITIAL_SYMBOLS = list(certified_symbols())

INITIAL_TEST_DATES = [
    "2024-01-02",  # normal trading day, avoid weekend/holiday for first test
]


# =============================================================================
# EXPECTED RAW DUKASCOPY TICK COLUMNS
# =============================================================================

EXPECTED_RAW_COLUMNS = [
    "timestamp",
    "ask",
    "bid",
    "ask_volume",
    "bid_volume",
]


# =============================================================================
# TARGET BACQE NORMALISED TICK SCHEMA
# =============================================================================

BACQE_TICK_SCHEMA = {
    "timestamp_utc": "datetime64[ns, UTC]",
    "symbol": "string",
    "source": "string",
    "bid": "float64",
    "ask": "float64",
    "mid": "float64",
    "spread": "float64",
    "spread_points": "float64",
    "bid_volume": "float64",
    "ask_volume": "float64",
    "quote_volume": "float64",
    "normalisation_schema_version": "string",
    "symbol_metadata_version": "string",
    "raw_price_scale": "int64",
    "point_size": "float64",
    "pip_size": "float64",
    "coverage_status": "string",
}


# =============================================================================
# DERIVED COLUMN DEFINITIONS
# =============================================================================

DERIVED_COLUMNS = {
    "mid": "(bid + ask) / 2",
    "spread": "ask - bid",
    "spread_points": "spread converted into symbol-specific points",
    "quote_volume": "bid_volume + ask_volume",
}


# =============================================================================
# STORAGE DESIGN
# =============================================================================

RAW_STORAGE_PATTERN = (
    RAW_DUKASCOPY_ROOT
    / "symbol={symbol}"
    / "year={year}"
    / "month={month}"
    / "{symbol}_{date}_raw.csv"
)

PROCESSED_STORAGE_PATTERN = (
    PROCESSED_DUKASCOPY_ROOT
    / "symbol={symbol}"
    / "year={year}"
    / "month={month}"
    / "{symbol}_{date}_ticks.parquet"
)


# =============================================================================
# SYMBOL METADATA
# =============================================================================

SYMBOL_METADATA = registry_payload()


def print_schema_summary() -> None:
    """Print the current Dukascopy schema specification."""

    print("=" * 90)
    print("BACQE DUKASCOPY 01 - SCHEMA SPECIFICATION")
    print("=" * 90)

    print("\n[PATHS]")
    print(f"Raw root:       {RAW_DUKASCOPY_ROOT}")
    print(f"Processed root: {PROCESSED_DUKASCOPY_ROOT}")
    print(f"Analysis root:  {ANALYSIS_DUKASCOPY_ROOT}")

    print("\n[INITIAL SCOPE]")
    print(f"Source:  {SOURCE_NAME}")
    print(f"Symbols: {INITIAL_SYMBOLS}")
    print(f"Dates:   {INITIAL_TEST_DATES}")

    print("\n[EXPECTED RAW COLUMNS]")
    pprint(EXPECTED_RAW_COLUMNS)

    print("\n[BACQE NORMALISED TICK SCHEMA]")
    pprint(BACQE_TICK_SCHEMA)

    print("\n[DERIVED COLUMNS]")
    pprint(DERIVED_COLUMNS)

    print("\n[SYMBOL METADATA]")
    print(f"Metadata schema:      {SYMBOL_METADATA_SCHEMA_VERSION}")
    print(f"Normalisation schema: {NORMALISATION_SCHEMA_VERSION}")
    pprint(SYMBOL_METADATA)

    print("\n[STORAGE PATTERNS]")
    print(f"Raw:       {RAW_STORAGE_PATTERN}")
    print(f"Processed: {PROCESSED_STORAGE_PATTERN}")

    print("\n[DONE] Schema specification loaded successfully.")


if __name__ == "__main__":
    print_schema_summary()
