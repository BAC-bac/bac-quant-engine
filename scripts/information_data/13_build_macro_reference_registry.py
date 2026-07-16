from __future__ import annotations

import platform
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

REPORT_DIR = PROJECT_ROOT / "reports"
REPORT_CSV = REPORT_DIR / "macro_reference_registry_catalogue_latest.csv"
REPORT_TXT = REPORT_DIR / "macro_reference_registry_latest.txt"


COUNTRY_REGISTRY = [
    {
        "country_key": "united_states",
        "country_name": "United States",
        "iso2": "US",
        "iso3": "USA",
        "currency_code": "USD",
        "region": "north_america",
        "economic_area": "united_states",
        "is_currency_area": False,
        "is_active": True,
    },
    {
        "country_key": "united_kingdom",
        "country_name": "United Kingdom",
        "iso2": "GB",
        "iso3": "GBR",
        "currency_code": "GBP",
        "region": "europe",
        "economic_area": "united_kingdom",
        "is_currency_area": False,
        "is_active": True,
    },
    {
        "country_key": "euro_area",
        "country_name": "Euro Area",
        "iso2": "EU",
        "iso3": "EMU",
        "currency_code": "EUR",
        "region": "europe",
        "economic_area": "euro_area",
        "is_currency_area": True,
        "is_active": True,
    },
    {
        "country_key": "japan",
        "country_name": "Japan",
        "iso2": "JP",
        "iso3": "JPN",
        "currency_code": "JPY",
        "region": "asia_pacific",
        "economic_area": "japan",
        "is_currency_area": False,
        "is_active": True,
    },
    {
        "country_key": "switzerland",
        "country_name": "Switzerland",
        "iso2": "CH",
        "iso3": "CHE",
        "currency_code": "CHF",
        "region": "europe",
        "economic_area": "switzerland",
        "is_currency_area": False,
        "is_active": True,
    },
    {
        "country_key": "canada",
        "country_name": "Canada",
        "iso2": "CA",
        "iso3": "CAN",
        "currency_code": "CAD",
        "region": "north_america",
        "economic_area": "canada",
        "is_currency_area": False,
        "is_active": True,
    },
    {
        "country_key": "australia",
        "country_name": "Australia",
        "iso2": "AU",
        "iso3": "AUS",
        "currency_code": "AUD",
        "region": "asia_pacific",
        "economic_area": "australia",
        "is_currency_area": False,
        "is_active": True,
    },
    {
        "country_key": "new_zealand",
        "country_name": "New Zealand",
        "iso2": "NZ",
        "iso3": "NZL",
        "currency_code": "NZD",
        "region": "asia_pacific",
        "economic_area": "new_zealand",
        "is_currency_area": False,
        "is_active": True,
    },
    {
        "country_key": "china",
        "country_name": "China",
        "iso2": "CN",
        "iso3": "CHN",
        "currency_code": "CNY",
        "region": "asia_pacific",
        "economic_area": "china",
        "is_currency_area": False,
        "is_active": True,
    },
]


CURRENCY_REGISTRY = [
    {
        "currency_code": "USD",
        "currency_name": "US Dollar",
        "currency_symbol": "$",
        "country_key": "united_states",
        "central_bank_key": "federal_reserve",
        "is_g10": True,
        "is_reserve_currency": True,
        "is_bacqe_core": True,
    },
    {
        "currency_code": "EUR",
        "currency_name": "Euro",
        "currency_symbol": "€",
        "country_key": "euro_area",
        "central_bank_key": "european_central_bank",
        "is_g10": True,
        "is_reserve_currency": True,
        "is_bacqe_core": True,
    },
    {
        "currency_code": "GBP",
        "currency_name": "Pound Sterling",
        "currency_symbol": "£",
        "country_key": "united_kingdom",
        "central_bank_key": "bank_of_england",
        "is_g10": True,
        "is_reserve_currency": True,
        "is_bacqe_core": True,
    },
    {
        "currency_code": "JPY",
        "currency_name": "Japanese Yen",
        "currency_symbol": "¥",
        "country_key": "japan",
        "central_bank_key": "bank_of_japan",
        "is_g10": True,
        "is_reserve_currency": True,
        "is_bacqe_core": True,
    },
    {
        "currency_code": "CHF",
        "currency_name": "Swiss Franc",
        "currency_symbol": "CHF",
        "country_key": "switzerland",
        "central_bank_key": "swiss_national_bank",
        "is_g10": True,
        "is_reserve_currency": True,
        "is_bacqe_core": False,
    },
    {
        "currency_code": "CAD",
        "currency_name": "Canadian Dollar",
        "currency_symbol": "C$",
        "country_key": "canada",
        "central_bank_key": "bank_of_canada",
        "is_g10": True,
        "is_reserve_currency": False,
        "is_bacqe_core": False,
    },
    {
        "currency_code": "AUD",
        "currency_name": "Australian Dollar",
        "currency_symbol": "A$",
        "country_key": "australia",
        "central_bank_key": "reserve_bank_of_australia",
        "is_g10": True,
        "is_reserve_currency": False,
        "is_bacqe_core": False,
    },
    {
        "currency_code": "NZD",
        "currency_name": "New Zealand Dollar",
        "currency_symbol": "NZ$",
        "country_key": "new_zealand",
        "central_bank_key": "reserve_bank_of_new_zealand",
        "is_g10": True,
        "is_reserve_currency": False,
        "is_bacqe_core": False,
    },
    {
        "currency_code": "CNY",
        "currency_name": "Chinese Yuan",
        "currency_symbol": "¥",
        "country_key": "china",
        "central_bank_key": "peoples_bank_of_china",
        "is_g10": False,
        "is_reserve_currency": True,
        "is_bacqe_core": False,
    },
]


CENTRAL_BANK_REGISTRY = [
    {
        "central_bank_key": "federal_reserve",
        "central_bank_name": "Federal Reserve System",
        "short_name": "Federal Reserve",
        "currency_code": "USD",
        "country_key": "united_states",
        "policy_rate_series_id": "FEDFUNDS",
        "policy_rate_dataset": "fred_macro_series",
        "policy_rate_field": "value",
        "source_name": "FRED",
        "source_status": "active",
    },
    {
        "central_bank_key": "bank_of_england",
        "central_bank_name": "Bank of England",
        "short_name": "BoE",
        "currency_code": "GBP",
        "country_key": "united_kingdom",
        "policy_rate_series_id": "official_bank_rate",
        "policy_rate_dataset": "boe_bank_rate",
        "policy_rate_field": "bank_rate_percent",
        "source_name": "Bank of England",
        "source_status": "active",
    },
    {
        "central_bank_key": "european_central_bank",
        "central_bank_name": "European Central Bank",
        "short_name": "ECB",
        "currency_code": "EUR",
        "country_key": "euro_area",
        "policy_rate_series_id": None,
        "policy_rate_dataset": None,
        "policy_rate_field": None,
        "source_name": "European Central Bank",
        "source_status": "planned",
    },
    {
        "central_bank_key": "bank_of_japan",
        "central_bank_name": "Bank of Japan",
        "short_name": "BoJ",
        "currency_code": "JPY",
        "country_key": "japan",
        "policy_rate_series_id": None,
        "policy_rate_dataset": None,
        "policy_rate_field": None,
        "source_name": "Bank of Japan",
        "source_status": "planned",
    },
    {
        "central_bank_key": "swiss_national_bank",
        "central_bank_name": "Swiss National Bank",
        "short_name": "SNB",
        "currency_code": "CHF",
        "country_key": "switzerland",
        "policy_rate_series_id": None,
        "policy_rate_dataset": None,
        "policy_rate_field": None,
        "source_name": "Swiss National Bank",
        "source_status": "planned",
    },
    {
        "central_bank_key": "bank_of_canada",
        "central_bank_name": "Bank of Canada",
        "short_name": "BoC",
        "currency_code": "CAD",
        "country_key": "canada",
        "policy_rate_series_id": None,
        "policy_rate_dataset": None,
        "policy_rate_field": None,
        "source_name": "Bank of Canada",
        "source_status": "planned",
    },
    {
        "central_bank_key": "reserve_bank_of_australia",
        "central_bank_name": "Reserve Bank of Australia",
        "short_name": "RBA",
        "currency_code": "AUD",
        "country_key": "australia",
        "policy_rate_series_id": None,
        "policy_rate_dataset": None,
        "policy_rate_field": None,
        "source_name": "Reserve Bank of Australia",
        "source_status": "planned",
    },
    {
        "central_bank_key": "reserve_bank_of_new_zealand",
        "central_bank_name": "Reserve Bank of New Zealand",
        "short_name": "RBNZ",
        "currency_code": "NZD",
        "country_key": "new_zealand",
        "policy_rate_series_id": None,
        "policy_rate_dataset": None,
        "policy_rate_field": None,
        "source_name": "Reserve Bank of New Zealand",
        "source_status": "planned",
    },
    {
        "central_bank_key": "peoples_bank_of_china",
        "central_bank_name": "People's Bank of China",
        "short_name": "PBoC",
        "currency_code": "CNY",
        "country_key": "china",
        "policy_rate_series_id": None,
        "policy_rate_dataset": None,
        "policy_rate_field": None,
        "source_name": "People's Bank of China",
        "source_status": "planned",
    },
]


FX_PAIR_REGISTRY = [
    {
        "pair": "EURUSD",
        "base_currency": "EUR",
        "quote_currency": "USD",
        "pair_type": "major",
        "is_bacqe_core": True,
        "macro_enabled": True,
    },
    {
        "pair": "GBPUSD",
        "base_currency": "GBP",
        "quote_currency": "USD",
        "pair_type": "major",
        "is_bacqe_core": True,
        "macro_enabled": True,
    },
    {
        "pair": "USDJPY",
        "base_currency": "USD",
        "quote_currency": "JPY",
        "pair_type": "major",
        "is_bacqe_core": True,
        "macro_enabled": True,
    },
    {
        "pair": "EURGBP",
        "base_currency": "EUR",
        "quote_currency": "GBP",
        "pair_type": "cross",
        "is_bacqe_core": True,
        "macro_enabled": True,
    },
    {
        "pair": "GBPJPY",
        "base_currency": "GBP",
        "quote_currency": "JPY",
        "pair_type": "cross",
        "is_bacqe_core": True,
        "macro_enabled": True,
    },
    {
        "pair": "EURJPY",
        "base_currency": "EUR",
        "quote_currency": "JPY",
        "pair_type": "cross",
        "is_bacqe_core": True,
        "macro_enabled": True,
    },
    {
        "pair": "USDCHF",
        "base_currency": "USD",
        "quote_currency": "CHF",
        "pair_type": "major",
        "is_bacqe_core": False,
        "macro_enabled": True,
    },
    {
        "pair": "USDCAD",
        "base_currency": "USD",
        "quote_currency": "CAD",
        "pair_type": "major",
        "is_bacqe_core": False,
        "macro_enabled": True,
    },
    {
        "pair": "AUDUSD",
        "base_currency": "AUD",
        "quote_currency": "USD",
        "pair_type": "major",
        "is_bacqe_core": False,
        "macro_enabled": True,
    },
    {
        "pair": "NZDUSD",
        "base_currency": "NZD",
        "quote_currency": "USD",
        "pair_type": "major",
        "is_bacqe_core": False,
        "macro_enabled": True,
    },
]


CFTC_MARKET_MAPPING = [
    {
        "match_term": "EURO FX",
        "canonical_asset": "EUR",
        "currency_code": "EUR",
        "reference_pair": "EURUSD",
        "asset_class": "fx",
        "mapping_priority": 1,
    },
    {
        "match_term": "BRITISH POUND",
        "canonical_asset": "GBP",
        "currency_code": "GBP",
        "reference_pair": "GBPUSD",
        "asset_class": "fx",
        "mapping_priority": 1,
    },
    {
        "match_term": "JAPANESE YEN",
        "canonical_asset": "JPY",
        "currency_code": "JPY",
        "reference_pair": "USDJPY",
        "asset_class": "fx",
        "mapping_priority": 1,
    },
    {
        "match_term": "SWISS FRANC",
        "canonical_asset": "CHF",
        "currency_code": "CHF",
        "reference_pair": "USDCHF",
        "asset_class": "fx",
        "mapping_priority": 1,
    },
    {
        "match_term": "CANADIAN DOLLAR",
        "canonical_asset": "CAD",
        "currency_code": "CAD",
        "reference_pair": "USDCAD",
        "asset_class": "fx",
        "mapping_priority": 1,
    },
    {
        "match_term": "AUSTRALIAN DOLLAR",
        "canonical_asset": "AUD",
        "currency_code": "AUD",
        "reference_pair": "AUDUSD",
        "asset_class": "fx",
        "mapping_priority": 1,
    },
    {
        "match_term": "NEW ZEALAND DOLLAR",
        "canonical_asset": "NZD",
        "currency_code": "NZD",
        "reference_pair": "NZDUSD",
        "asset_class": "fx",
        "mapping_priority": 1,
    },
    {
        "match_term": "U.S. DOLLAR INDEX",
        "canonical_asset": "DXY",
        "currency_code": "USD",
        "reference_pair": None,
        "asset_class": "fx_index",
        "mapping_priority": 1,
    },
    {
        "match_term": "GOLD",
        "canonical_asset": "XAU",
        "currency_code": None,
        "reference_pair": "XAUUSD",
        "asset_class": "metal",
        "mapping_priority": 2,
    },
    {
        "match_term": "S&P 500",
        "canonical_asset": "SPX",
        "currency_code": "USD",
        "reference_pair": None,
        "asset_class": "equity_index",
        "mapping_priority": 2,
    },
]


DATASET_CADENCE_REGISTRY = [
    {
        "dataset": "cross_asset_macro_snapshots",
        "frequency": "daily",
        "expected_refresh_hours": 24,
        "warning_after_hours": 36,
        "stale_after_hours": 72,
        "enabled": True,
    },
    {
        "dataset": "economic_calendar_snapshots",
        "frequency": "daily",
        "expected_refresh_hours": 24,
        "warning_after_hours": 36,
        "stale_after_hours": 72,
        "enabled": False,
    },
    {
        "dataset": "financial_headline_snapshots",
        "frequency": "hourly",
        "expected_refresh_hours": 1,
        "warning_after_hours": 3,
        "stale_after_hours": 12,
        "enabled": True,
    },
    {
        "dataset": "fred_macro_series",
        "frequency": "daily",
        "expected_refresh_hours": 24,
        "warning_after_hours": 36,
        "stale_after_hours": 72,
        "enabled": True,
    },
    {
        "dataset": "boe_bank_rate",
        "frequency": "daily_check_event_driven",
        "expected_refresh_hours": 24,
        "warning_after_hours": 48,
        "stale_after_hours": 168,
        "enabled": True,
    },
    {
        "dataset": "us_treasury_average_interest_rates",
        "frequency": "monthly",
        "expected_refresh_hours": 744,
        "warning_after_hours": 840,
        "stale_after_hours": 1080,
        "enabled": True,
    },
    {
        "dataset": "cftc_cot_tff",
        "frequency": "weekly",
        "expected_refresh_hours": 168,
        "warning_after_hours": 216,
        "stale_after_hours": 336,
        "enabled": True,
    },
]


REGISTRY_DEFINITIONS = {
    "country_registry": {
        "rows": COUNTRY_REGISTRY,
        "primary_key": ["country_key"],
    },
    "currency_registry": {
        "rows": CURRENCY_REGISTRY,
        "primary_key": ["currency_code"],
    },
    "central_bank_registry": {
        "rows": CENTRAL_BANK_REGISTRY,
        "primary_key": ["central_bank_key"],
    },
    "fx_pair_registry": {
        "rows": FX_PAIR_REGISTRY,
        "primary_key": ["pair"],
    },
    "cftc_market_mapping": {
        "rows": CFTC_MARKET_MAPPING,
        "primary_key": ["match_term"],
    },
    "dataset_cadence_registry": {
        "rows": DATASET_CADENCE_REGISTRY,
        "primary_key": ["dataset"],
    },
}


def load_config() -> dict:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f"Could not find configuration file: {CONFIG_FILE}"
        )

    with CONFIG_FILE.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(
            f"Invalid YAML configuration: {CONFIG_FILE}"
        )

    return config


def select_existing_path(
    candidates: list[str | None],
) -> Path:
    valid_candidates = [
        candidate
        for candidate in candidates
        if candidate
    ]

    if not valid_candidates:
        raise ValueError(
            "No Data Lake path candidates were configured."
        )

    for candidate in valid_candidates:
        path = Path(candidate)

        if path.exists():
            return path

    raise FileNotFoundError(
        "None of the configured Data Lake paths exists: "
        + ", ".join(valid_candidates)
    )


def get_data_lake_root() -> Path:
    config = load_config()
    paths = config["data_lake_root"]

    if platform.system().lower() == "windows":
        return select_existing_path(
            [
                paths.get("windows_network"),
                paths.get("windows_local"),
                paths.get("windows"),
            ]
        )

    linux_path = paths.get("linux")

    if not linux_path:
        raise KeyError(
            "'data_lake_root.linux' is missing from config/paths.yaml"
        )

    resolved_path = Path(linux_path)

    if not resolved_path.exists():
        raise FileNotFoundError(
            f"Configured Linux Data Lake does not exist: {resolved_path}"
        )

    return resolved_path


def get_database_path(
    data_lake_root: Path,
) -> Path:
    return (
        data_lake_root
        / "data"
        / "curated"
        / "macro"
        / "master_database"
        / "macro_master_database_latest.sqlite"
    )


def get_registry_output_dir(
    data_lake_root: Path,
) -> Path:
    output_dir = (
        data_lake_root
        / "data"
        / "curated"
        / "macro"
        / "reference_registry"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    return output_dir


def validate_registry(
    registry_name: str,
    df: pd.DataFrame,
    primary_key: list[str],
) -> None:
    if df.empty:
        raise ValueError(
            f"{registry_name} contains no rows."
        )

    missing_key_columns = sorted(
        set(primary_key) - set(df.columns)
    )

    if missing_key_columns:
        raise ValueError(
            f"{registry_name} is missing primary-key columns: "
            f"{missing_key_columns}"
        )

    null_key_rows = df[
        df[primary_key].isna().any(axis=1)
    ]

    if not null_key_rows.empty:
        raise ValueError(
            f"{registry_name} contains null primary-key values."
        )

    duplicate_count = df.duplicated(
        subset=primary_key,
        keep=False,
    ).sum()

    if duplicate_count:
        raise ValueError(
            f"{registry_name} contains "
            f"{duplicate_count:,} duplicate primary-key rows."
        )


def validate_cross_registry_links(
    registry_frames: dict[str, pd.DataFrame],
) -> list[str]:
    errors: list[str] = []

    countries = registry_frames["country_registry"]
    currencies = registry_frames["currency_registry"]
    central_banks = registry_frames["central_bank_registry"]
    fx_pairs = registry_frames["fx_pair_registry"]

    country_keys = set(
        countries["country_key"]
    )

    currency_codes = set(
        currencies["currency_code"]
    )

    central_bank_keys = set(
        central_banks["central_bank_key"]
    )

    invalid_currency_countries = currencies[
        ~currencies["country_key"].isin(country_keys)
    ]

    if not invalid_currency_countries.empty:
        errors.append(
            "Currency registry contains unknown country keys: "
            f"{sorted(invalid_currency_countries['country_key'].unique())}"
        )

    invalid_currency_banks = currencies[
        ~currencies["central_bank_key"].isin(
            central_bank_keys
        )
    ]

    if not invalid_currency_banks.empty:
        errors.append(
            "Currency registry contains unknown central-bank keys: "
            f"{sorted(invalid_currency_banks['central_bank_key'].unique())}"
        )

    invalid_bank_currencies = central_banks[
        ~central_banks["currency_code"].isin(
            currency_codes
        )
    ]

    if not invalid_bank_currencies.empty:
        errors.append(
            "Central-bank registry contains unknown currencies: "
            f"{sorted(invalid_bank_currencies['currency_code'].unique())}"
        )

    invalid_base = fx_pairs[
        ~fx_pairs["base_currency"].isin(
            currency_codes
        )
    ]

    invalid_quote = fx_pairs[
        ~fx_pairs["quote_currency"].isin(
            currency_codes
        )
    ]

    if not invalid_base.empty:
        errors.append(
            "FX registry contains unknown base currencies: "
            f"{sorted(invalid_base['base_currency'].unique())}"
        )

    if not invalid_quote.empty:
        errors.append(
            "FX registry contains unknown quote currencies: "
            f"{sorted(invalid_quote['quote_currency'].unique())}"
        )

    return errors


def save_registry_outputs(
    registry_name: str,
    df: pd.DataFrame,
    output_dir: Path,
) -> tuple[Path, Path]:
    csv_path = output_dir / f"{registry_name}.csv"
    parquet_path = output_dir / f"{registry_name}.parquet"

    df.to_csv(
        csv_path,
        index=False,
    )

    df.to_parquet(
        parquet_path,
        index=False,
    )

    return csv_path, parquet_path


def write_registry_table(
    connection: sqlite3.Connection,
    registry_name: str,
    df: pd.DataFrame,
) -> None:
    df.to_sql(
        registry_name,
        connection,
        if_exists="replace",
        index=False,
    )


def create_reference_views(
    connection: sqlite3.Connection,
) -> None:
    connection.execute(
        "DROP VIEW IF EXISTS fx_pair_reference"
    )

    connection.execute(
        """
        CREATE VIEW fx_pair_reference AS
        SELECT
            fx.pair,
            fx.base_currency,
            base_currency.currency_name
                AS base_currency_name,
            base_currency.country_key
                AS base_country_key,
            base_country.country_name
                AS base_country_name,
            base_currency.central_bank_key
                AS base_central_bank_key,
            fx.quote_currency,
            quote_currency.currency_name
                AS quote_currency_name,
            quote_currency.country_key
                AS quote_country_key,
            quote_country.country_name
                AS quote_country_name,
            quote_currency.central_bank_key
                AS quote_central_bank_key,
            fx.pair_type,
            fx.is_bacqe_core,
            fx.macro_enabled
        FROM fx_pair_registry AS fx
        LEFT JOIN currency_registry AS base_currency
            ON fx.base_currency =
               base_currency.currency_code
        LEFT JOIN country_registry AS base_country
            ON base_currency.country_key =
               base_country.country_key
        LEFT JOIN currency_registry AS quote_currency
            ON fx.quote_currency =
               quote_currency.currency_code
        LEFT JOIN country_registry AS quote_country
            ON quote_currency.country_key =
               quote_country.country_key
        """
    )

    connection.execute(
        "DROP VIEW IF EXISTS currency_policy_rate_reference"
    )

    connection.execute(
        """
        CREATE VIEW currency_policy_rate_reference AS
        SELECT
            currency.currency_code,
            currency.currency_name,
            currency.country_key,
            country.country_name,
            currency.central_bank_key,
            bank.central_bank_name,
            bank.short_name AS central_bank_short_name,
            bank.policy_rate_series_id,
            bank.policy_rate_dataset,
            bank.policy_rate_field,
            bank.source_name,
            bank.source_status
        FROM currency_registry AS currency
        LEFT JOIN country_registry AS country
            ON currency.country_key =
               country.country_key
        LEFT JOIN central_bank_registry AS bank
            ON currency.central_bank_key =
               bank.central_bank_key
        """
    )


def write_reports(
    catalogue_df: pd.DataFrame,
    cross_registry_errors: list[str],
    database_path: Path,
    output_dir: Path,
    generated_utc: datetime,
) -> None:
    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    catalogue_df.to_csv(
        REPORT_CSV,
        index=False,
    )

    lines = [
        "=" * 100,
        "BACQE MACRO REFERENCE REGISTRY REPORT",
        "=" * 100,
        f"Generated UTC:      {generated_utc.isoformat()}",
        f"Database:           {database_path}",
        f"Registry output:    {output_dir}",
        "",
        f"Registries created: {len(catalogue_df)}",
        f"Total rows:         {catalogue_df['row_count'].sum():,}",
        f"Validation errors:  {len(cross_registry_errors)}",
        "",
        "-" * 100,
        "REGISTRY CATALOGUE",
        "-" * 100,
        catalogue_df.to_string(index=False),
    ]

    if cross_registry_errors:
        lines.extend(
            [
                "",
                "-" * 100,
                "CROSS-REGISTRY VALIDATION ERRORS",
                "-" * 100,
            ]
        )

        for error in cross_registry_errors:
            lines.append(f"- {error}")

    lines.extend(
        [
            "",
            "-" * 100,
            "REFERENCE VIEWS",
            "-" * 100,
            "fx_pair_reference",
            "currency_policy_rate_reference",
            "",
            "=" * 100,
            "OVERALL RESULT",
            "=" * 100,
        ]
    )

    if cross_registry_errors:
        lines.append(
            "FAILED — cross-registry relationships contain errors."
        )
    else:
        lines.append(
            "PASS — all reference registries and relationships validated."
        )

    REPORT_TXT.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    print("=" * 100)
    print(
        "BACQE INFORMATION DATA 13 - "
        "BUILD MACRO REFERENCE REGISTRY"
    )
    print("=" * 100)

    generated_utc = datetime.now(
        timezone.utc
    )

    data_lake_root = get_data_lake_root()

    database_path = get_database_path(
        data_lake_root
    )

    output_dir = get_registry_output_dir(
        data_lake_root
    )

    if not database_path.exists():
        raise FileNotFoundError(
            "Macro Master Database not found: "
            f"{database_path}. Run INFO-11 first."
        )

    print(f"Data lake root:  {data_lake_root}")
    print(f"Database:        {database_path}")
    print(f"Registry output: {output_dir}")
    print("-" * 100)

    registry_frames: dict[str, pd.DataFrame] = {}
    catalogue_rows: list[dict] = []

    for registry_name, definition in (
        REGISTRY_DEFINITIONS.items()
    ):
        df = pd.DataFrame(
            definition["rows"]
        )

        validate_registry(
            registry_name=registry_name,
            df=df,
            primary_key=definition["primary_key"],
        )

        registry_frames[registry_name] = df

        csv_path, parquet_path = (
            save_registry_outputs(
                registry_name=registry_name,
                df=df,
                output_dir=output_dir,
            )
        )

        catalogue_rows.append(
            {
                "registry_name": registry_name,
                "row_count": len(df),
                "column_count": len(df.columns),
                "primary_key": ",".join(
                    definition["primary_key"]
                ),
                "csv_path": str(csv_path),
                "parquet_path": str(parquet_path),
                "status": "validated",
            }
        )

        print(
            f"[REGISTRY] {registry_name:<30} "
            f"{len(df):>4,} rows"
        )

    cross_registry_errors = (
        validate_cross_registry_links(
            registry_frames
        )
    )

    catalogue_df = pd.DataFrame(
        catalogue_rows
    )

    with sqlite3.connect(
        database_path
    ) as connection:
        for registry_name, df in (
            registry_frames.items()
        ):
            write_registry_table(
                connection=connection,
                registry_name=registry_name,
                df=df,
            )

        create_reference_views(
            connection
        )

        registry_metadata = pd.DataFrame(
            [
                {
                    "registry_version": "1.0",
                    "generated_utc": (
                        generated_utc.isoformat()
                    ),
                    "registry_count": len(
                        registry_frames
                    ),
                    "total_rows": int(
                        catalogue_df[
                            "row_count"
                        ].sum()
                    ),
                    "validation_error_count": len(
                        cross_registry_errors
                    ),
                }
            ]
        )

        registry_metadata.to_sql(
            "reference_registry_metadata",
            connection,
            if_exists="replace",
            index=False,
        )

        connection.execute(
            "PRAGMA optimize;"
        )

        connection.commit()

    write_reports(
        catalogue_df=catalogue_df,
        cross_registry_errors=cross_registry_errors,
        database_path=database_path,
        output_dir=output_dir,
        generated_utc=generated_utc,
    )

    print()
    print("=" * 100)
    print("BACQE MACRO REFERENCE REGISTRY BUILD COMPLETE")
    print("=" * 100)
    print(
        f"Registries created: "
        f"{len(catalogue_df)}"
    )
    print(
        f"Total rows:         "
        f"{catalogue_df['row_count'].sum():,}"
    )
    print(
        f"Validation errors:  "
        f"{len(cross_registry_errors)}"
    )
    print(f"Catalogue CSV:      {REPORT_CSV}")
    print(f"Report TXT:         {REPORT_TXT}")

    print()
    print(
        catalogue_df[
            [
                "registry_name",
                "row_count",
                "column_count",
                "primary_key",
                "status",
            ]
        ].to_string(index=False)
    )

    if cross_registry_errors:
        print()
        for error in cross_registry_errors:
            print(f"[ERROR] {error}")

        print()
        print("Overall: FAILED")
        return 1

    print()
    print("Overall: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())