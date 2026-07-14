from __future__ import annotations

import platform
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

REPORT_DIR = PROJECT_ROOT / "reports"
REPORT_CSV = REPORT_DIR / "macro_master_database_catalogue_latest.csv"
REPORT_TXT = REPORT_DIR / "macro_master_database_report_latest.txt"


DATASETS = {
    "fred_macro_series": {
        "table_name": "fred_macro_series",
        "required": True,
        "description": "FRED macroeconomic and financial time series.",
        "date_candidates": ["date", "snapshot_date", "run_time_utc"],
    },
    "boe_bank_rate": {
        "table_name": "boe_bank_rate",
        "required": True,
        "description": "Official Bank of England Bank Rate history.",
        "date_candidates": ["date", "snapshot_date", "run_time_utc"],
    },
    "us_treasury_average_interest_rates": {
        "table_name": "us_treasury_average_interest_rates",
        "required": True,
        "description": "Average interest rates on US Treasury securities.",
        "date_candidates": ["record_date", "snapshot_date", "run_time_utc"],
    },
    "cftc_cot_tff": {
        "table_name": "cftc_cot_tff",
        "required": True,
        "description": "CFTC Traders in Financial Futures positioning.",
        "date_candidates": [
            "report_date_as_yyyy_mm_dd",
            "snapshot_date",
            "run_time_utc",
        ],
    },
    "cross_asset_macro_snapshots": {
        "table_name": "cross_asset_macro_snapshots",
        "required": True,
        "description": "Cross-asset market snapshot from yfinance.",
        "date_candidates": [
            "observation_date",
            "date",
            "snapshot_date",
            "run_time_utc",
        ],
    },
    "financial_headline_snapshots": {
        "table_name": "financial_headline_snapshots",
        "required": True,
        "description": "Financial and economic RSS headline snapshots.",
        "date_candidates": [
            "published_utc",
            "published",
            "snapshot_date",
            "run_time_utc",
        ],
    },
    "economic_calendar_snapshots": {
        "table_name": "economic_calendar_snapshots",
        "required": False,
        "description": "Economic calendar events and surprises.",
        "date_candidates": ["date", "snapshot_date", "run_time_utc"],
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


def get_raw_information_root(
    data_lake_root: Path,
) -> Path:
    return (
        data_lake_root
        / "data"
        / "raw"
        / "information_data"
    )


def get_output_paths(
    data_lake_root: Path,
    run_time_utc: datetime,
) -> tuple[Path, Path]:
    output_dir = (
        data_lake_root
        / "data"
        / "curated"
        / "macro"
        / "master_database"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    timestamp = run_time_utc.strftime(
        "%Y_%m_%d_%H%M%S"
    )

    dated_database = (
        output_dir
        / f"macro_master_database_{timestamp}.sqlite"
    )

    latest_database = (
        output_dir
        / "macro_master_database_latest.sqlite"
    )

    return dated_database, latest_database


def find_latest_parquet(
    dataset_root: Path,
) -> Path | None:
    if not dataset_root.exists():
        return None

    explicit_latest = list(
        dataset_root.rglob("*_latest.parquet")
    )

    if explicit_latest:
        return max(
            explicit_latest,
            key=lambda path: path.stat().st_mtime,
        )

    parquet_files = list(
        dataset_root.rglob("*.parquet")
    )

    if not parquet_files:
        return None

    return max(
        parquet_files,
        key=lambda path: path.stat().st_mtime,
    )


def make_sqlite_safe(
    df: pd.DataFrame,
) -> pd.DataFrame:
    output = df.copy()

    for column in output.columns:
        if pd.api.types.is_datetime64_any_dtype(
            output[column]
        ):
            output[column] = output[column].astype(
                "string"
            )

        elif output[column].dtype == "object":
            output[column] = output[column].map(
                lambda value: (
                    str(value)
                    if isinstance(
                        value,
                        (list, tuple, dict, set),
                    )
                    else value
                )
            )

    return output


def determine_date_coverage(
    df: pd.DataFrame,
    date_candidates: list[str],
) -> tuple[str | None, str | None, str | None]:
    for column in date_candidates:
        if column not in df.columns:
            continue

        parsed = pd.to_datetime(
            df[column],
            errors="coerce",
            utc=True,
        )

        valid = parsed.dropna()

        if valid.empty:
            continue

        min_date = valid.min().isoformat()
        max_date = valid.max().isoformat()

        return column, min_date, max_date

    return None, None, None


def write_dataset_table(
    connection: sqlite3.Connection,
    raw_information_root: Path,
    dataset_name: str,
    metadata: dict,
    run_time_utc: datetime,
) -> dict:
    dataset_root = (
        raw_information_root
        / dataset_name
    )

    latest_file = find_latest_parquet(
        dataset_root
    )

    result = {
        "dataset": dataset_name,
        "table_name": metadata["table_name"],
        "description": metadata["description"],
        "required": metadata["required"],
        "dataset_root": str(dataset_root),
        "dataset_root_exists": dataset_root.exists(),
        "latest_file": (
            str(latest_file)
            if latest_file
            else ""
        ),
        "latest_file_exists": latest_file is not None,
        "source_modified_utc": None,
        "rows": None,
        "columns": None,
        "date_column": None,
        "min_date": None,
        "max_date": None,
        "status": None,
        "error": None,
        "loaded_utc": run_time_utc.isoformat(),
    }

    if not dataset_root.exists():
        result["status"] = (
            "missing_optional_dataset"
            if not metadata["required"]
            else "missing_required_dataset"
        )
        return result

    if latest_file is None:
        result["status"] = (
            "missing_optional_latest_file"
            if not metadata["required"]
            else "missing_required_latest_file"
        )
        return result

    try:
        df = pd.read_parquet(
            latest_file
        )

        result["source_modified_utc"] = (
            datetime.fromtimestamp(
                latest_file.stat().st_mtime,
                tz=timezone.utc,
            ).isoformat()
        )

        result["rows"] = len(df)
        result["columns"] = len(df.columns)

        if df.empty:
            result["status"] = "empty_dataset"
            return result

        (
            date_column,
            min_date,
            max_date,
        ) = determine_date_coverage(
            df,
            metadata["date_candidates"],
        )

        result["date_column"] = date_column
        result["min_date"] = min_date
        result["max_date"] = max_date

        sqlite_df = make_sqlite_safe(
            df
        )

        sqlite_df.to_sql(
            metadata["table_name"],
            connection,
            if_exists="replace",
            index=False,
            chunksize=10_000,
        )

        result["status"] = "loaded"

    except Exception as exc:
        result["status"] = "load_failed"
        result["error"] = str(exc)

    return result


def create_database_metadata(
    connection: sqlite3.Connection,
    catalogue_df: pd.DataFrame,
    run_time_utc: datetime,
) -> None:
    catalogue_df.to_sql(
        "dataset_catalogue",
        connection,
        if_exists="replace",
        index=False,
    )

    database_metadata = pd.DataFrame(
        [
            {
                "database_name": "BACQE Macro Master Database",
                "database_version": "1.0",
                "generated_utc": run_time_utc.isoformat(),
                "dataset_count": len(catalogue_df),
                "loaded_dataset_count": int(
                    (
                        catalogue_df["status"]
                        == "loaded"
                    ).sum()
                ),
                "required_failure_count": int(
                    (
                        catalogue_df["status"]
                        .isin(
                            [
                                "missing_required_dataset",
                                "missing_required_latest_file",
                                "empty_dataset",
                                "load_failed",
                            ]
                        )
                    ).sum()
                ),
            }
        ]
    )

    database_metadata.to_sql(
        "database_metadata",
        connection,
        if_exists="replace",
        index=False,
    )


def write_reports(
    catalogue_df: pd.DataFrame,
    dated_database: Path,
    latest_database: Path,
    run_time_utc: datetime,
) -> None:
    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    catalogue_df.to_csv(
        REPORT_CSV,
        index=False,
    )

    loaded = catalogue_df[
        catalogue_df["status"] == "loaded"
    ]

    required_failures = catalogue_df[
        catalogue_df["required"]
        & (
            catalogue_df["status"]
            != "loaded"
        )
    ]

    optional_missing = catalogue_df[
        ~catalogue_df["required"]
        & (
            catalogue_df["status"]
            != "loaded"
        )
    ]

    lines = [
        "=" * 100,
        "BACQE MACRO MASTER DATABASE REPORT",
        "=" * 100,
        f"Generated UTC:       {run_time_utc.isoformat()}",
        f"Dated database:      {dated_database}",
        f"Latest database:     {latest_database}",
        "",
        f"Registered datasets: {len(catalogue_df)}",
        f"Loaded datasets:     {len(loaded)}",
        f"Required failures:   {len(required_failures)}",
        f"Optional missing:    {len(optional_missing)}",
        "",
        "-" * 100,
        "DATASET CATALOGUE",
        "-" * 100,
    ]

    display_columns = [
        "dataset",
        "table_name",
        "required",
        "status",
        "rows",
        "columns",
        "date_column",
        "min_date",
        "max_date",
    ]

    lines.append(
        catalogue_df[
            display_columns
        ].to_string(index=False)
    )

    if not required_failures.empty:
        lines.extend(
            [
                "",
                "-" * 100,
                "REQUIRED FAILURES",
                "-" * 100,
                required_failures[
                    [
                        "dataset",
                        "status",
                        "error",
                    ]
                ].to_string(index=False),
            ]
        )

    if not optional_missing.empty:
        lines.extend(
            [
                "",
                "-" * 100,
                "OPTIONAL DATASETS NOT LOADED",
                "-" * 100,
                optional_missing[
                    [
                        "dataset",
                        "status",
                        "error",
                    ]
                ].to_string(index=False),
            ]
        )

    lines.extend(
        [
            "",
            "=" * 100,
            "OVERALL RESULT",
            "=" * 100,
        ]
    )

    if required_failures.empty:
        lines.append(
            "PASS — all required datasets loaded successfully."
        )
    else:
        lines.append(
            "FAILED — one or more required datasets were not loaded."
        )

    REPORT_TXT.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    print("=" * 100)
    print("BACQE INFORMATION DATA 11 - BUILD MACRO MASTER DATABASE")
    print("=" * 100)

    run_time_utc = datetime.now(
        timezone.utc
    )

    data_lake_root = get_data_lake_root()
    raw_information_root = (
        get_raw_information_root(
            data_lake_root
        )
    )

    (
        dated_database,
        latest_database,
    ) = get_output_paths(
        data_lake_root,
        run_time_utc,
    )

    print(f"Data lake root:       {data_lake_root}")
    print(f"Raw information root: {raw_information_root}")
    print(f"Database output:      {dated_database}")
    print("-" * 100)

    catalogue_rows: list[dict] = []

    with sqlite3.connect(
        dated_database
    ) as connection:
        for dataset_name, metadata in DATASETS.items():
            print(
                f"[LOAD] {dataset_name:<40}",
                end="",
            )

            result = write_dataset_table(
                connection=connection,
                raw_information_root=raw_information_root,
                dataset_name=dataset_name,
                metadata=metadata,
                run_time_utc=run_time_utc,
            )

            catalogue_rows.append(
                result
            )

            print(
                f" {result['status']}"
            )

        catalogue_df = pd.DataFrame(
            catalogue_rows
        )

        create_database_metadata(
            connection=connection,
            catalogue_df=catalogue_df,
            run_time_utc=run_time_utc,
        )

        connection.execute(
            "PRAGMA optimize;"
        )

    shutil.copy2(
        dated_database,
        latest_database,
    )

    write_reports(
        catalogue_df=catalogue_df,
        dated_database=dated_database,
        latest_database=latest_database,
        run_time_utc=run_time_utc,
    )

    loaded_count = int(
        (
            catalogue_df["status"]
            == "loaded"
        ).sum()
    )

    required_failures = catalogue_df[
        catalogue_df["required"]
        & (
            catalogue_df["status"]
            != "loaded"
        )
    ]

    print()
    print("=" * 100)
    print("BACQE MACRO MASTER DATABASE BUILD COMPLETE")
    print("=" * 100)
    print(
        f"Datasets loaded:   "
        f"{loaded_count}/{len(catalogue_df)}"
    )
    print(
        f"Required failures: "
        f"{len(required_failures)}"
    )
    print(f"Dated database:    {dated_database}")
    print(f"Latest database:   {latest_database}")
    print(f"Catalogue CSV:     {REPORT_CSV}")
    print(f"Report TXT:        {REPORT_TXT}")

    print()
    print(
        catalogue_df[
            [
                "dataset",
                "table_name",
                "required",
                "status",
                "rows",
                "columns",
                "min_date",
                "max_date",
            ]
        ].to_string(index=False)
    )

    if not required_failures.empty:
        print()
        print("Overall: FAILED")
        return 1

    print()
    print("Overall: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())