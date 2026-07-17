from __future__ import annotations

import platform
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

REPORT_DIR = PROJECT_ROOT / "reports"
REPORT_CSV = REPORT_DIR / "unified_currency_macro_snapshot_latest.csv"
REPORT_TXT = REPORT_DIR / "unified_currency_macro_snapshot_latest.txt"

OUTPUT_TABLE = "unified_currency_macro_snapshot"


# FRED series currently available in INFO-04.
# These are US observations and therefore map to USD.
FRED_US_FEATURE_MAP = {
    "FEDFUNDS": "policy_rate_fred",
    "DFF": "daily_policy_rate",
    "DGS2": "government_yield_2y",
    "DGS10": "government_yield_10y",
    "DGS30": "government_yield_30y",
    "T10Y2Y": "yield_curve_10y_2y",
    "T10Y3M": "yield_curve_10y_3m",
    "CPIAUCSL": "cpi_index",
    "CPILFESL": "core_cpi_index",
    "PCEPI": "pce_price_index",
    "PCEPILFE": "core_pce_price_index",
    "UNRATE": "unemployment_rate",
    "PAYEMS": "nonfarm_payrolls",
    "ICSA": "initial_jobless_claims",
    "GDP": "nominal_gdp",
    "GDPC1": "real_gdp",
    "INDPRO": "industrial_production",
    "RSAFS": "retail_sales",
    "M2SL": "money_supply_m2",
    "BAMLH0A0HYM2": "high_yield_spread",
    "USREC": "recession_indicator",
}


CROSS_ASSET_NAME_ALIASES = {
    "vix": ["vix", "^vix"],
    "sp500": ["sp500", "s&p 500", "^gspc"],
    "nasdaq": ["nasdaq", "^ixic"],
    "gold": ["gold_futures", "gold", "gc=f"],
    "wti": ["wti_crude", "wti", "cl=f"],
    "brent": ["brent_crude", "brent", "bz=f"],
    "bitcoin": ["bitcoin", "btc-usd"],
    "eurusd": ["eurusd", "eurusd=x"],
    "gbpusd": ["gbpusd", "gbpusd=x"],
    "usdjpy": ["usdjpy", "usdjpy=x"],
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


def get_output_dir(
    data_lake_root: Path,
) -> Path:
    output_dir = (
        data_lake_root
        / "data"
        / "curated"
        / "macro"
        / "currency_snapshot"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    return output_dir


def database_object_exists(
    connection: sqlite3.Connection,
    object_name: str,
) -> bool:
    count = connection.execute(
        """
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE name = ?
        """,
        [object_name],
    ).fetchone()[0]

    return count > 0


def read_sql_object(
    connection: sqlite3.Connection,
    object_name: str,
    required: bool = True,
) -> pd.DataFrame:
    if not database_object_exists(
        connection,
        object_name,
    ):
        if required:
            raise ValueError(
                f"Required database object is missing: {object_name}"
            )

        return pd.DataFrame()

    return pd.read_sql_query(
        f'SELECT * FROM "{object_name}"',
        connection,
    )


def normalise_boolean_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:
    output = df.copy()

    boolean_columns = [
        "is_g10",
        "is_reserve_currency",
        "is_bacqe_core",
        "is_active",
    ]

    for column in boolean_columns:
        if column in output.columns:
            output[column] = (
                pd.to_numeric(
                    output[column],
                    errors="coerce",
                )
                .fillna(0)
                .astype(bool)
            )

    return output


def build_currency_base(
    connection: sqlite3.Connection,
) -> pd.DataFrame:
    currency = read_sql_object(
        connection,
        "currency_registry",
    )

    country = read_sql_object(
        connection,
        "country_registry",
    )

    central_bank = read_sql_object(
        connection,
        "central_bank_registry",
    )

    base = currency.merge(
        country[
            [
                "country_key",
                "country_name",
                "iso2",
                "iso3",
                "region",
                "economic_area",
                "is_active",
            ]
        ],
        on="country_key",
        how="left",
        validate="many_to_one",
    )

    base = base.merge(
        central_bank[
            [
                "central_bank_key",
                "central_bank_name",
                "short_name",
                "policy_rate_series_id",
                "policy_rate_dataset",
                "policy_rate_field",
                "source_name",
                "source_status",
            ]
        ],
        on="central_bank_key",
        how="left",
        validate="many_to_one",
    )

    base = normalise_boolean_columns(base)

    return base


def parse_datetime_series(
    series: pd.Series,
) -> pd.Series:
    return pd.to_datetime(
        series,
        errors="coerce",
        utc=True,
    )


def add_fred_us_features(
    snapshot: pd.DataFrame,
    fred: pd.DataFrame,
) -> pd.DataFrame:
    output = snapshot.copy()

    if fred.empty:
        return output

    required_columns = {
        "series_id",
        "date",
        "value",
    }

    if not required_columns.issubset(fred.columns):
        print(
            "[WARN] FRED view does not contain the expected columns."
        )
        return output

    fred = fred.copy()
    fred["date"] = parse_datetime_series(
        fred["date"]
    )
    fred["value"] = pd.to_numeric(
        fred["value"],
        errors="coerce",
    )

    usd_mask = output["currency_code"] == "USD"

    for series_id, feature_name in (
        FRED_US_FEATURE_MAP.items()
    ):
        series_rows = fred[
            fred["series_id"] == series_id
        ].copy()

        value_column = feature_name
        date_column = f"{feature_name}_date"
        age_column = f"{feature_name}_age_days"

        output[value_column] = np.nan
        output[date_column] = pd.Series(
            pd.NaT,
            index=output.index,
            dtype="datetime64[ns, UTC]",
        )
        output[age_column] = np.nan

        if series_rows.empty:
            continue

        latest_row = (
            series_rows
            .sort_values("date")
            .iloc[-1]
        )

        observation_date = latest_row["date"]

        output.loc[
            usd_mask,
            value_column,
        ] = latest_row["value"]

        output.loc[
            usd_mask,
            date_column,
        ] = observation_date

        if pd.notna(observation_date):
            age_days = (
                pd.Timestamp.now(tz="UTC").normalize()
                - observation_date.normalize()
            ).days

            output.loc[
                usd_mask,
                age_column,
            ] = age_days

    return output


def add_boe_policy_rate(
    snapshot: pd.DataFrame,
    boe: pd.DataFrame,
) -> pd.DataFrame:
    output = snapshot.copy()

    if "policy_rate" not in output.columns:
        output["policy_rate"] = np.nan
        output["policy_rate_date"] = pd.Series(
            pd.NaT,
            index=output.index,
            dtype="datetime64[ns, UTC]",
        )
        output["policy_rate_source"] = pd.Series(
            pd.NA,
            index=output.index,
            dtype="string",
        )

    if boe.empty:
        return output

    required_columns = {
        "date",
        "bank_rate_percent",
    }

    if not required_columns.issubset(boe.columns):
        print(
            "[WARN] BoE view does not contain expected columns."
        )
        return output

    latest_row = boe.copy().iloc[-1]

    rate = pd.to_numeric(
        pd.Series(
            [latest_row["bank_rate_percent"]]
        ),
        errors="coerce",
    ).iloc[0]

    date = pd.to_datetime(
        latest_row["date"],
        errors="coerce",
        utc=True,
    )

    gbp_mask = output["currency_code"] == "GBP"

    output.loc[
        gbp_mask,
        "policy_rate",
    ] = rate

    output.loc[
        gbp_mask,
        "policy_rate_date",
    ] = date

    output.loc[
        gbp_mask,
        "policy_rate_source",
    ] = "Bank of England"

    return output


def add_us_policy_rate(
    snapshot: pd.DataFrame,
) -> pd.DataFrame:
    output = snapshot.copy()

    if "policy_rate" not in output.columns:
        output["policy_rate"] = np.nan
        output["policy_rate_date"] = pd.Series(
            pd.NaT,
            index=output.index,
            dtype="datetime64[ns, UTC]",
        )
        output["policy_rate_source"] = pd.Series(
            pd.NA,
            index=output.index,
            dtype="string",
        )

    usd_mask = output["currency_code"] == "USD"

    if "policy_rate_fred" in output.columns:
        output.loc[
            usd_mask,
            "policy_rate",
        ] = output.loc[
            usd_mask,
            "policy_rate_fred",
        ]

        output.loc[
            usd_mask,
            "policy_rate_date",
        ] = output.loc[
            usd_mask,
            "policy_rate_fred_date",
        ]

        output.loc[
            usd_mask,
            "policy_rate_source",
        ] = "FRED FEDFUNDS"

    return output


def calculate_policy_rate_freshness(
    snapshot: pd.DataFrame,
) -> pd.DataFrame:
    output = snapshot.copy()

    policy_dates = pd.to_datetime(
        output["policy_rate_date"],
        errors="coerce",
        utc=True,
    )

    now = pd.Timestamp.now(tz="UTC").normalize()

    output["policy_rate_age_days"] = (
        now - policy_dates.dt.normalize()
    ).dt.days

    output["policy_rate_available"] = (
        output["policy_rate"].notna()
    )

    return output


def choose_cftc_position_columns(
    cftc: pd.DataFrame,
) -> dict[str, str | None]:
    candidates = {
        "leveraged_net": [
            "lev_money_net_all",
        ],
        "leveraged_net_pct": [
            "lev_money_net_pct_open_interest",
        ],
        "asset_manager_net": [
            "asset_mgr_net_all",
        ],
        "asset_manager_net_pct": [
            "asset_mgr_net_pct_open_interest",
        ],
        "open_interest": [
            "open_interest_all",
        ],
    }

    selected: dict[str, str | None] = {}

    for output_name, column_candidates in (
        candidates.items()
    ):
        selected[output_name] = next(
            (
                column
                for column in column_candidates
                if column in cftc.columns
            ),
            None,
        )

    return selected


def add_cftc_features(
    snapshot: pd.DataFrame,
    cftc: pd.DataFrame,
    mappings: pd.DataFrame,
) -> pd.DataFrame:
    output = snapshot.copy()

    feature_columns = [
        "cftc_market_name",
        "cftc_report_date",
        "cftc_leveraged_net",
        "cftc_leveraged_net_pct",
        "cftc_asset_manager_net",
        "cftc_asset_manager_net_pct",
        "cftc_open_interest",
        "cftc_observation_age_days",
        "cftc_mapping_status",
    ]

    for column in feature_columns:
        if column in output.columns:
            continue

        if column == "cftc_report_date":
            output[column] = pd.Series(
                pd.NaT,
                index=output.index,
                dtype="datetime64[ns, UTC]",
            )
        elif column in {
            "cftc_market_name",
            "cftc_mapping_status",
        }:
            output[column] = pd.Series(
                pd.NA,
                index=output.index,
                dtype="string",
            )
        else:
            output[column] = np.nan

    if cftc.empty or mappings.empty:
        return output

    if "market_and_exchange_names" not in cftc.columns:
        print(
            "[WARN] CFTC view lacks market_and_exchange_names."
        )
        return output

    cftc = cftc.copy()

    report_date_column = (
        "report_date_as_yyyy_mm_dd"
    )

    cftc[report_date_column] = pd.to_datetime(
        cftc[report_date_column],
        errors="coerce",
        utc=True,
    )

    selected_columns = choose_cftc_position_columns(
        cftc
    )

    mappings = mappings.sort_values(
        "mapping_priority"
    )

    now = pd.Timestamp.now(tz="UTC").normalize()

    for _, mapping in mappings.iterrows():
        currency_code = mapping.get(
            "currency_code"
        )

        if pd.isna(currency_code):
            continue

        match_term = str(
            mapping["match_term"]
        ).upper()

        market_matches = cftc[
            cftc["market_and_exchange_names"]
            .astype(str)
            .str.upper()
            .str.contains(
                match_term,
                regex=False,
                na=False,
            )
        ].copy()

        if market_matches.empty:
            continue

        market_matches = market_matches.sort_values(
            report_date_column,
            ascending=False,
        )

        latest_row = market_matches.iloc[0]

        currency_mask = (
            output["currency_code"]
            == currency_code
        )

        if not currency_mask.any():
            continue

        report_date = latest_row[
            report_date_column
        ]

        output.loc[
            currency_mask,
            "cftc_market_name",
        ] = latest_row[
            "market_and_exchange_names"
        ]

        output.loc[
            currency_mask,
            "cftc_report_date",
        ] = report_date

        if pd.notna(report_date):
            output.loc[
                currency_mask,
                "cftc_observation_age_days",
            ] = (
                now - report_date.normalize()
            ).days

        mapping_pairs = {
            "cftc_leveraged_net": "leveraged_net",
            "cftc_leveraged_net_pct": "leveraged_net_pct",
            "cftc_asset_manager_net": "asset_manager_net",
            "cftc_asset_manager_net_pct": "asset_manager_net_pct",
            "cftc_open_interest": "open_interest",
        }

        for output_column, selected_key in (
            mapping_pairs.items()
        ):
            source_column = selected_columns[
                selected_key
            ]

            if source_column is None:
                continue

            output.loc[
                currency_mask,
                output_column,
            ] = latest_row[source_column]

        output.loc[
            currency_mask,
            "cftc_mapping_status",
        ] = "mapped"

    output["cftc_mapping_status"] = (
        output["cftc_mapping_status"]
        .fillna("unmapped")
    )

    return output


def detect_cross_asset_columns(
    cross_asset: pd.DataFrame,
) -> tuple[str | None, str | None]:
    name_candidates = [
        "asset_name",
        "series_name",
        "name",
        "asset",
        "label",
        "symbol",
        "ticker",
    ]

    value_candidates = [
        "close",
        "value",
        "price",
        "last",
        "last_price",
        "current_price",
    ]

    name_column = next(
        (
            column
            for column in name_candidates
            if column in cross_asset.columns
        ),
        None,
    )

    value_column = next(
        (
            column
            for column in value_candidates
            if column in cross_asset.columns
        ),
        None,
    )

    return name_column, value_column


def extract_cross_asset_value(
    cross_asset: pd.DataFrame,
    name_column: str,
    value_column: str,
    aliases: list[str],
) -> float | None:
    names = (
        cross_asset[name_column]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    for alias in aliases:
        matches = cross_asset[
            names == alias.lower()
        ]

        if matches.empty:
            matches = cross_asset[
                names.str.contains(
                    alias.lower(),
                    regex=False,
                    na=False,
                )
            ]

        if matches.empty:
            continue

        value = pd.to_numeric(
            matches.iloc[0][value_column],
            errors="coerce",
        )

        if pd.notna(value):
            return float(value)

    return None


def add_cross_asset_context(
    snapshot: pd.DataFrame,
    cross_asset: pd.DataFrame,
) -> pd.DataFrame:
    output = snapshot.copy()

    for feature_name in (
        CROSS_ASSET_NAME_ALIASES
    ):
        output[
            f"global_{feature_name}"
        ] = np.nan

    if cross_asset.empty:
        return output

    (
        name_column,
        value_column,
    ) = detect_cross_asset_columns(
        cross_asset
    )

    if name_column is None or value_column is None:
        print(
            "[WARN] Could not identify cross-asset "
            "name/value columns."
        )
        print(
            f"[INFO] Cross-asset columns: "
            f"{cross_asset.columns.tolist()}"
        )
        return output

    for feature_name, aliases in (
        CROSS_ASSET_NAME_ALIASES.items()
    ):
        value = extract_cross_asset_value(
            cross_asset=cross_asset,
            name_column=name_column,
            value_column=value_column,
            aliases=aliases,
        )

        if value is not None:
            output[
                f"global_{feature_name}"
            ] = value

    return output


def add_derived_features(
    snapshot: pd.DataFrame,
) -> pd.DataFrame:
    output = snapshot.copy()

    if {
        "government_yield_10y",
        "government_yield_2y",
    }.issubset(output.columns):
        output[
            "derived_yield_curve_10y_2y"
        ] = (
            output["government_yield_10y"]
            - output["government_yield_2y"]
        )

    if {
        "policy_rate",
        "inflation_rate",
    }.issubset(output.columns):
        output["real_policy_rate"] = (
            output["policy_rate"]
            - output["inflation_rate"]
        )
    else:
        output["real_policy_rate"] = np.nan

    return output


def add_coverage_metrics(
    snapshot: pd.DataFrame,
) -> pd.DataFrame:
    output = snapshot.copy()

    output["fred_macro_available"] = (
        output.get(
            "unemployment_rate",
            pd.Series(
                False,
                index=output.index,
            ),
        ).notna()
    )

    output["cftc_available"] = (
        output["cftc_mapping_status"]
        == "mapped"
    )

    core_fields = [
        "policy_rate",
        "cftc_leveraged_net",
    ]

    available_core_fields = [
        column
        for column in core_fields
        if column in output.columns
    ]

    output["available_core_factor_count"] = (
        output[
            available_core_fields
        ]
        .notna()
        .sum(axis=1)
    )

    output["expected_core_factor_count"] = len(
        available_core_fields
    )

    if available_core_fields:
        output["core_factor_coverage_pct"] = (
            output[
                "available_core_factor_count"
            ]
            / len(available_core_fields)
            * 100
        )
    else:
        output["core_factor_coverage_pct"] = 0.0

    output["macro_data_status"] = np.select(
        [
            output["core_factor_coverage_pct"]
            >= 100,
            output["core_factor_coverage_pct"]
            >= 50,
        ],
        [
            "strong_initial_coverage",
            "partial_coverage",
        ],
        default="limited_coverage",
    )

    return output


def validate_snapshot(
    snapshot: pd.DataFrame,
) -> None:
    required_columns = {
        "currency_code",
        "country_name",
        "central_bank_name",
        "macro_data_status",
    }

    missing_columns = required_columns.difference(
        snapshot.columns
    )

    if missing_columns:
        raise ValueError(
            "Unified currency snapshot is missing columns: "
            f"{sorted(missing_columns)}"
        )

    if snapshot.empty:
        raise ValueError(
            "Unified currency snapshot contains no rows."
        )

    duplicate_count = snapshot.duplicated(
        subset=["currency_code"],
        keep=False,
    ).sum()

    if duplicate_count:
        raise ValueError(
            "Unified currency snapshot contains "
            f"{duplicate_count:,} duplicate currency rows."
        )

    unknown_currencies = snapshot[
        snapshot["currency_code"].isna()
    ]

    if not unknown_currencies.empty:
        raise ValueError(
            "Unified currency snapshot contains null currencies."
        )


def make_sqlite_safe(
    df: pd.DataFrame,
) -> pd.DataFrame:
    output = df.copy()

    for column in output.columns:
        series = output[column]

        if pd.api.types.is_datetime64_any_dtype(series):
            output[column] = (
                pd.to_datetime(
                    series,
                    errors="coerce",
                    utc=True,
                )
                .dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                .where(series.notna(), None)
            )
            continue

        if pd.api.types.is_bool_dtype(series):
            output[column] = series.astype("Int64")
            continue

        if series.dtype == "object":
            output[column] = series.map(
                lambda value: (
                    value.isoformat()
                    if isinstance(
                        value,
                        (
                            pd.Timestamp,
                            datetime,
                        ),
                    )
                    else (
                        str(value)
                        if isinstance(
                            value,
                            (
                                list,
                                tuple,
                                dict,
                                set,
                            ),
                        )
                        else value
                    )
                )
            )

    return output


def save_outputs(
    snapshot: pd.DataFrame,
    connection: sqlite3.Connection,
    output_dir: Path,
    generated_utc: datetime,
) -> tuple[Path, Path, Path, Path]:
    timestamp = generated_utc.strftime(
        "%Y_%m_%d_%H%M%S"
    )

    dated_csv = (
        output_dir
        / f"unified_currency_macro_snapshot_{timestamp}.csv"
    )

    dated_parquet = (
        output_dir
        / f"unified_currency_macro_snapshot_{timestamp}.parquet"
    )

    latest_csv = (
        output_dir
        / "unified_currency_macro_snapshot_latest.csv"
    )

    latest_parquet = (
        output_dir
        / "unified_currency_macro_snapshot_latest.parquet"
    )

    snapshot.to_csv(
        dated_csv,
        index=False,
    )

    snapshot.to_parquet(
        dated_parquet,
        index=False,
    )

    snapshot.to_csv(
        latest_csv,
        index=False,
    )

    snapshot.to_parquet(
        latest_parquet,
        index=False,
    )

    sqlite_snapshot = make_sqlite_safe(
        snapshot
    )

    timestamp_cells = []

    for column in sqlite_snapshot.columns:
        for row_index, value in sqlite_snapshot[column].items():
            if isinstance(value, (pd.Timestamp, datetime)):
                timestamp_cells.append(
                    {
                        "row": row_index,
                        "column": column,
                        "value": repr(value),
                    }
                )

    if timestamp_cells:
        raise TypeError(
            "SQLite-unsafe timestamps remain: "
            f"{timestamp_cells[:10]}"
        )

    sqlite_snapshot.to_sql(
        OUTPUT_TABLE,
        connection,
        if_exists="replace",
        index=False,
    )

    return (
        dated_csv,
        dated_parquet,
        latest_csv,
        latest_parquet,
    )


def write_reports(
    snapshot: pd.DataFrame,
    database_path: Path,
    latest_csv: Path,
    latest_parquet: Path,
    generated_utc: datetime,
) -> None:
    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    snapshot.to_csv(
        REPORT_CSV,
        index=False,
    )

    coverage_summary = (
        snapshot["macro_data_status"]
        .value_counts()
        .rename_axis("status")
        .reset_index(name="currencies")
    )

    display_columns = [
        "currency_code",
        "country_name",
        "central_bank_name",
        "policy_rate",
        "policy_rate_date",
        "government_yield_2y",
        "government_yield_10y",
        "yield_curve_10y_2y",
        "unemployment_rate",
        "cftc_leveraged_net",
        "cftc_asset_manager_net",
        "core_factor_coverage_pct",
        "macro_data_status",
    ]

    existing_display_columns = [
        column
        for column in display_columns
        if column in snapshot.columns
    ]

    lines = [
        "=" * 110,
        "BACQE UNIFIED CURRENCY MACRO SNAPSHOT",
        "=" * 110,
        f"Generated UTC:     {generated_utc.isoformat()}",
        f"Database:          {database_path}",
        f"Latest CSV:        {latest_csv}",
        f"Latest parquet:    {latest_parquet}",
        "",
        f"Currencies:        {len(snapshot)}",
        f"Policy-rate data:  {snapshot['policy_rate_available'].sum()}",
        f"CFTC mappings:     {snapshot['cftc_available'].sum()}",
        "",
        "-" * 110,
        "COVERAGE SUMMARY",
        "-" * 110,
        coverage_summary.to_string(index=False),
        "",
        "-" * 110,
        "CURRENCY SNAPSHOT",
        "-" * 110,
        snapshot[
            existing_display_columns
        ].to_string(index=False),
        "",
        "=" * 110,
        "IMPORTANT INTERPRETATION",
        "=" * 110,
        (
            "Missing observations are preserved as missing evidence. "
            "They are not replaced with neutral scores or assumed values."
        ),
        (
            "At this stage, FRED macro and Treasury market-yield "
            "features primarily cover USD, while official BoE policy "
            "rate data covers GBP."
        ),
        "",
        "=" * 110,
        "OVERALL RESULT",
        "=" * 110,
        "PASS — unified currency-level macro snapshot created.",
    ]

    REPORT_TXT.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    print("=" * 100)
    print(
        "BACQE INFORMATION DATA 14 - "
        "BUILD UNIFIED CURRENCY MACRO SNAPSHOT"
    )
    print("=" * 100)

    generated_utc = datetime.now(
        timezone.utc
    )

    data_lake_root = get_data_lake_root()
    database_path = get_database_path(
        data_lake_root
    )
    output_dir = get_output_dir(
        data_lake_root
    )

    if not database_path.exists():
        raise FileNotFoundError(
            "Macro Master Database not found: "
            f"{database_path}. Run INFO-11 first."
        )

    print(f"Data lake root: {data_lake_root}")
    print(f"Database:       {database_path}")
    print(f"Output dir:     {output_dir}")
    print("-" * 100)

    with sqlite3.connect(
        database_path
    ) as connection:
        snapshot = build_currency_base(
            connection
        )

        fred = read_sql_object(
            connection,
            "latest_fred_series_values",
        )

        boe = read_sql_object(
            connection,
            "latest_boe_bank_rate",
        )

        cftc = read_sql_object(
            connection,
            "latest_cftc_positioning",
        )

        cftc_mapping = read_sql_object(
            connection,
            "cftc_market_mapping",
        )

        cross_asset = read_sql_object(
            connection,
            "latest_cross_asset_snapshot",
            required=False,
        )

        snapshot = add_fred_us_features(
            snapshot,
            fred,
        )

        snapshot = add_boe_policy_rate(
            snapshot,
            boe,
        )

        snapshot = add_us_policy_rate(
            snapshot
        )

        snapshot = calculate_policy_rate_freshness(
            snapshot
        )

        snapshot = add_cftc_features(
            snapshot=snapshot,
            cftc=cftc,
            mappings=cftc_mapping,
        )

        snapshot = add_cross_asset_context(
            snapshot=snapshot,
            cross_asset=cross_asset,
        )

        snapshot = add_derived_features(
            snapshot
        )

        snapshot = add_coverage_metrics(
            snapshot
        )

        snapshot["snapshot_generated_utc"] = (
            generated_utc.isoformat()
        )

        snapshot = snapshot.sort_values(
            [
                "is_bacqe_core",
                "is_g10",
                "currency_code",
            ],
            ascending=[
                False,
                False,
                True,
            ],
        ).reset_index(drop=True)

        validate_snapshot(
            snapshot
        )

        (
            dated_csv,
            dated_parquet,
            latest_csv,
            latest_parquet,
        ) = save_outputs(
            snapshot=snapshot,
            connection=connection,
            output_dir=output_dir,
            generated_utc=generated_utc,
        )

        connection.commit()

    write_reports(
        snapshot=snapshot,
        database_path=database_path,
        latest_csv=latest_csv,
        latest_parquet=latest_parquet,
        generated_utc=generated_utc,
    )

    print()
    print("=" * 100)
    print(
        "BACQE UNIFIED CURRENCY MACRO "
        "SNAPSHOT BUILD COMPLETE"
    )
    print("=" * 100)
    print(f"Currencies:          {len(snapshot)}")
    print(
        "Policy-rate data:    "
        f"{int(snapshot['policy_rate_available'].sum())}"
    )
    print(
        "CFTC mappings:       "
        f"{int(snapshot['cftc_available'].sum())}"
    )
    print(f"Dated CSV:          {dated_csv}")
    print(f"Dated parquet:      {dated_parquet}")
    print(f"Latest CSV:         {latest_csv}")
    print(f"Latest parquet:     {latest_parquet}")
    print(f"Report CSV:         {REPORT_CSV}")
    print(f"Report TXT:         {REPORT_TXT}")

    print()
    print(
        snapshot[
            [
                "currency_code",
                "country_name",
                "policy_rate",
                "policy_rate_date",
                "cftc_leveraged_net",
                "core_factor_coverage_pct",
                "macro_data_status",
            ]
        ].to_string(index=False)
    )

    print()
    print("Overall: PASS")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())