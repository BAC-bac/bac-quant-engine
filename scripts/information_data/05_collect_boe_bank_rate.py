from __future__ import annotations

import os
import platform
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

SOURCE = "bank_of_england"
DATASET = "boe_bank_rate"
URL = "https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp"


def load_config() -> dict:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Could not find configuration file: {CONFIG_FILE}")

    with CONFIG_FILE.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(f"Invalid YAML configuration: {CONFIG_FILE}")

    return config


def select_existing_path(candidates: list[str | None]) -> Path:
    valid_candidates = [candidate for candidate in candidates if candidate]

    if not valid_candidates:
        raise ValueError("No Data Lake path candidates were configured.")

    for candidate in valid_candidates:
        path = Path(candidate)

        if path.exists():
            return path

    raise FileNotFoundError(
        "None of the configured Data Lake paths exists: "
        + ", ".join(valid_candidates)
    )


def get_data_lake_root() -> Path:
    env_path = os.getenv("DATA_LAKE_ROOT")

    if env_path:
        environment_root = Path(env_path)

        if environment_root.exists():
            return environment_root

        print(
            f"[WARN] DATA_LAKE_ROOT is set but does not exist: "
            f"{environment_root}"
        )

    config = load_config()

    if "data_lake_root" not in config:
        raise KeyError(
            f"'data_lake_root' is missing from configuration: {CONFIG_FILE}"
        )

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

    resolved_linux_path = Path(linux_path)

    if not resolved_linux_path.exists():
        raise FileNotFoundError(
            f"Configured Linux Data Lake does not exist: "
            f"{resolved_linux_path}"
        )

    return resolved_linux_path


def build_output_dir(
    data_lake_root: Path,
    run_time_utc: datetime,
) -> Path:
    return (
        data_lake_root
        / "data"
        / "raw"
        / "information_data"
        / DATASET
        / f"source={SOURCE}"
        / f"year={run_time_utc:%Y}"
        / f"month={run_time_utc:%m}"
    )


def fetch_bank_rate_tables() -> list[pd.DataFrame]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(compatible; BACQEResearchCollector/1.0)"
        )
    }

    response = requests.get(
        URL,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text))

    if not tables:
        raise RuntimeError(
            "The Bank of England page returned no readable HTML tables."
        )

    return tables


def find_bank_rate_table(
    tables: list[pd.DataFrame],
) -> pd.DataFrame:
    for table in tables:
        columns = [str(column).lower() for column in table.columns]
        joined_columns = " ".join(columns)

        contains_date = "date" in joined_columns
        contains_rate = (
            "rate" in joined_columns
            or "bank" in joined_columns
        )

        if contains_date and contains_rate:
            return table.copy()

    if tables:
        print(
            "[WARN] No table matched the expected date/rate schema; "
            "using the first HTML table."
        )
        return tables[0].copy()

    return pd.DataFrame()


def normalise_column_name(column: object) -> str:
    return (
        str(column)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("%", "percent")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "_")
    )


def normalise_bank_rate(
    df: pd.DataFrame,
    run_time_utc: datetime,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    output = df.copy()
    output.columns = [
        normalise_column_name(column)
        for column in output.columns
    ]

    date_candidates = [
        column
        for column in output.columns
        if "date" in column
    ]

    rate_candidates = [
        column
        for column in output.columns
        if "rate" in column
    ]

    if not date_candidates:
        raise ValueError(
            "Could not identify a date column in the BoE table. "
            f"Columns found: {output.columns.tolist()}"
        )

    if not rate_candidates:
        raise ValueError(
            "Could not identify a rate column in the BoE table. "
            f"Columns found: {output.columns.tolist()}"
        )

    date_column = date_candidates[0]
    rate_column = rate_candidates[0]

    output = output.rename(
        columns={
            date_column: "date",
            rate_column: "bank_rate_percent",
        }
    )

    raw_dates = output["date"].astype(str).str.strip()

    output["date"] = pd.to_datetime(
        raw_dates,
        format="%d %b %y",
        errors="coerce",
    )

    current_year = run_time_utc.year

    future_mask = output["date"].dt.year > current_year

    output.loc[future_mask, "date"] = (
            output.loc[future_mask, "date"] - pd.DateOffset(years=100)
    )

    output["bank_rate_percent"] = (
        output["bank_rate_percent"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
    )

    output["bank_rate_percent"] = pd.to_numeric(
        output["bank_rate_percent"],
        errors="coerce",
    )

    output = output.dropna(
        subset=["date", "bank_rate_percent"]
    ).copy()

    if output.empty:
        raise ValueError(
            "The BoE table was found, but no valid date/rate rows "
            "remained after normalisation."
        )

    output["run_time_utc"] = run_time_utc.isoformat()
    output["snapshot_date"] = run_time_utc.date().isoformat()
    output["source"] = SOURCE
    output["source_url"] = URL
    output["series_name"] = "official_bank_rate"

    preferred_columns = [
        "run_time_utc",
        "snapshot_date",
        "source",
        "series_name",
        "date",
        "bank_rate_percent",
        "source_url",
    ]

    output = output[preferred_columns]
    output = (
        output
        .drop_duplicates(
            subset=["series_name", "date"],
            keep="last",
        )
        .sort_values("date")
        .reset_index(drop=True)
    )

    return output


def validate_bank_rate(df: pd.DataFrame) -> None:
    required_columns = {
        "date",
        "bank_rate_percent",
        "source",
        "series_name",
    }

    current_date = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()

    future_dates = df[df["date"] > current_date]

    if not future_dates.empty:
        raise ValueError(
            "BoE output contains future observation dates: "
            f"{future_dates['date'].dt.strftime('%Y-%m-%d').tolist()[:10]}"
        )

    missing_columns = required_columns.difference(df.columns)

    if missing_columns:
        raise ValueError(
            f"BoE output is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError("BoE output contains no rows.")

    if df["date"].duplicated().any():
        raise ValueError(
            "BoE output contains duplicate observation dates."
        )

    invalid_rates = df[
        (df["bank_rate_percent"] < -5)
        | (df["bank_rate_percent"] > 30)
    ]

    if not invalid_rates.empty:
        raise ValueError(
            "BoE output contains implausible Bank Rate values: "
            f"{invalid_rates['bank_rate_percent'].tolist()}"
        )


def save_outputs(
    df: pd.DataFrame,
    output_dir: Path,
    run_time_utc: datetime,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = run_time_utc.strftime("%Y_%m_%d_%H%M%S")

    parquet_path = (
        output_dir
        / f"{DATASET}_{timestamp}.parquet"
    )
    csv_path = (
        output_dir
        / f"{DATASET}_{timestamp}.csv"
    )

    latest_parquet = output_dir / f"{DATASET}_latest.parquet"
    latest_csv = output_dir / f"{DATASET}_latest.csv"

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    df.to_parquet(latest_parquet, index=False)
    df.to_csv(latest_csv, index=False)

    latest_row = df.iloc[-1]

    print()
    print("[DONE] BoE Bank Rate history saved.")
    print(f"Rows:           {len(df):,}")
    print(f"Date range:     {df['date'].min().date()} -> {df['date'].max().date()}")
    print(f"Latest date:    {latest_row['date'].date()}")
    print(f"Latest rate:    {latest_row['bank_rate_percent']:.3f}%")
    print(f"Parquet:        {parquet_path}")
    print(f"CSV:            {csv_path}")
    print(f"Latest parquet: {latest_parquet}")
    print(f"Latest CSV:     {latest_csv}")


def main() -> None:
    print("=" * 90)
    print("BACQE INFORMATION DATA - BANK OF ENGLAND BANK RATE")
    print("=" * 90)

    run_time_utc = datetime.now(timezone.utc)
    data_lake_root = get_data_lake_root()
    output_dir = build_output_dir(
        data_lake_root,
        run_time_utc,
    )

    print(f"Data lake:  {data_lake_root}")
    print(f"Output dir: {output_dir}")
    print(f"Source URL: {URL}")
    print("-" * 90)

    tables = fetch_bank_rate_tables()

    print(f"HTML tables found: {len(tables):,}")

    raw_table = find_bank_rate_table(tables)
    df = normalise_bank_rate(
        raw_table,
        run_time_utc,
    )

    validate_bank_rate(df)
    save_outputs(
        df,
        output_dir,
        run_time_utc,
    )

    print("=" * 90)


if __name__ == "__main__":
    main()
