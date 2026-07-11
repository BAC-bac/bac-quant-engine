from __future__ import annotations

import os
import platform
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

SOURCE = "cftc"
DATASET = "cftc_cot_tff"

# Traders in Financial Futures — Futures Only.
URL_TEMPLATE = "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"

CURRENT_YEAR = datetime.now(timezone.utc).year
YEARS_TO_COLLECT = list(range(CURRENT_YEAR - 2, CURRENT_YEAR + 1))


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


def select_existing_path(candidates: list[str | None]) -> Path:
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
    env_path = os.getenv("DATA_LAKE_ROOT")

    if env_path:
        environment_root = Path(env_path)

        if environment_root.exists():
            return environment_root

        print(
            "[WARN] DATA_LAKE_ROOT is set but does not exist: "
            f"{environment_root}"
        )

    config = load_config()

    if "data_lake_root" not in config:
        raise KeyError(
            f"'data_lake_root' is missing from {CONFIG_FILE}"
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
            "Configured Linux Data Lake does not exist: "
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


def download_zip(
    year: int,
    session: requests.Session,
) -> bytes | None:
    url = URL_TEMPLATE.format(year=year)

    print(f"[FETCH] {year} -> {url}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(compatible; BACQECFTCCOTCollector/1.0)"
        )
    }

    try:
        response = session.get(
            url,
            headers=headers,
            timeout=60,
        )
    except requests.RequestException as exc:
        print(f"[WARN] {year} request failed: {exc}")
        return None

    if response.status_code == 404:
        print(
            f"[WARN] {year} annual archive is not available yet."
        )
        return None

    if response.status_code != 200:
        print(
            f"[WARN] {year} returned HTTP "
            f"{response.status_code}: {response.text[:200]}"
        )
        return None

    if not response.content:
        print(f"[WARN] {year} returned an empty response.")
        return None

    return response.content


def select_archive_member(
    members: list[str],
    year: int,
) -> str:
    usable_members = [
        member
        for member in members
        if not member.endswith("/")
        and Path(member).suffix.lower()
        in {".txt", ".csv"}
    ]

    if not usable_members:
        raise ValueError(
            f"No readable TXT/CSV member found in {year} archive. "
            f"Members: {members}"
        )

    preferred_members = [
        member
        for member in usable_members
        if "fut_fin" in member.lower()
        or "fin_fut" in member.lower()
    ]

    return (
        preferred_members[0]
        if preferred_members
        else usable_members[0]
    )


def read_cftc_zip(
    content: bytes,
    year: int,
) -> pd.DataFrame:
    try:
        with zipfile.ZipFile(BytesIO(content)) as archive:
            members = archive.namelist()

            if not members:
                raise ValueError(
                    f"CFTC archive for {year} is empty."
                )

            member = select_archive_member(
                members,
                year,
            )

            print(f"[INFO] Reading {member}")

            with archive.open(member) as file:
                df = pd.read_csv(
                    file,
                    low_memory=False,
                )

    except zipfile.BadZipFile as exc:
        raise ValueError(
            f"Invalid ZIP archive returned for {year}."
        ) from exc

    df["archive_year"] = year

    return df


def normalise_column_name(column: object) -> str:
    return (
        str(column)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("%", "pct")
        .replace(".", "")
        .replace("__", "_")
    )


def normalise_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:
    output = df.copy()

    output.columns = [
        normalise_column_name(column)
        for column in output.columns
    ]

    return output


def parse_report_dates(
    df: pd.DataFrame,
) -> pd.DataFrame:
    output = df.copy()

    date_candidates = [
        column
        for column in output.columns
        if "report_date" in column
        or column == "date"
    ]

    for column in date_candidates:
        output[column] = pd.to_datetime(
            output[column],
            errors="coerce",
        )

    return output


def convert_research_numeric_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:
    output = df.copy()

    numeric_patterns = (
        "open_interest",
        "positions_long",
        "positions_short",
        "positions_spread",
        "traders_long",
        "traders_short",
        "changes_in",
        "pct_of_oi",
    )

    for column in output.columns:
        if any(
            pattern in column
            for pattern in numeric_patterns
        ):
            output[column] = pd.to_numeric(
                output[column],
                errors="coerce",
            )

    return output


def collect_cftc_tff(
    run_time_utc: datetime,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    with requests.Session() as session:
        for year in YEARS_TO_COLLECT:
            content = download_zip(
                year,
                session,
            )

            if content is None:
                continue

            try:
                df = read_cftc_zip(
                    content,
                    year,
                )

                if df.empty:
                    print(
                        f"[WARN] {year} archive contained no rows."
                    )
                    continue

                df = normalise_columns(df)
                df = parse_report_dates(df)
                df = convert_research_numeric_columns(df)

                df["run_time_utc"] = run_time_utc.isoformat()
                df["snapshot_date"] = (
                    run_time_utc.date().isoformat()
                )
                df["source"] = SOURCE
                df["report_type"] = (
                    "traders_in_financial_futures_futures_only"
                )
                df["source_url"] = (
                    URL_TEMPLATE.format(year=year)
                )

                frames.append(df)

                print(
                    f"[OK] {year}: {len(df):,} rows, "
                    f"{len(df.columns):,} columns"
                )

            except Exception as exc:
                print(
                    f"[ERROR] Failed processing {year}: {exc}"
                )

    if not frames:
        return pd.DataFrame()

    output = pd.concat(
        frames,
        ignore_index=True,
    )

    preferred_columns = [
        "run_time_utc",
        "snapshot_date",
        "source",
        "report_type",
        "archive_year",
        "market_and_exchange_names",
        "cftc_contract_market_code",
        "cftc_market_code",
        "cftc_region_code",
        "cftc_commodity_code",
        "report_date_as_yyyy_mm_dd",
        "open_interest_all",
        "dealer_positions_long_all",
        "dealer_positions_short_all",
        "asset_mgr_positions_long_all",
        "asset_mgr_positions_short_all",
        "lev_money_positions_long_all",
        "lev_money_positions_short_all",
        "other_rept_positions_long_all",
        "other_rept_positions_short_all",
        "nonrept_positions_long_all",
        "nonrept_positions_short_all",
        "source_url",
    ]

    existing_columns = [
        column
        for column in preferred_columns
        if column in output.columns
    ]

    remaining_columns = [
        column
        for column in output.columns
        if column not in existing_columns
    ]

    output = output[
        existing_columns + remaining_columns
    ]

    sort_columns = [
        column
        for column in [
            "archive_year",
            "market_and_exchange_names",
            "report_date_as_yyyy_mm_dd",
        ]
        if column in output.columns
    ]

    if sort_columns:
        output = (
            output
            .sort_values(sort_columns)
            .reset_index(drop=True)
        )

    return output


def safe_divide(
    numerator: pd.Series,
    denominator: pd.Series,
) -> pd.Series:
    denominator = denominator.replace(0, pd.NA)

    return numerator.div(denominator)


def add_research_features(
    df: pd.DataFrame,
) -> pd.DataFrame:
    if df.empty:
        return df

    output = df.copy()

    open_interest_column = "open_interest_all"

    positioning_groups = {
        "dealer": (
            "dealer_positions_long_all",
            "dealer_positions_short_all",
        ),
        "asset_mgr": (
            "asset_mgr_positions_long_all",
            "asset_mgr_positions_short_all",
        ),
        "lev_money": (
            "lev_money_positions_long_all",
            "lev_money_positions_short_all",
        ),
        "other_rept": (
            "other_rept_positions_long_all",
            "other_rept_positions_short_all",
        ),
        "nonrept": (
            "nonrept_positions_long_all",
            "nonrept_positions_short_all",
        ),
    }

    for group_name, (
        long_column,
        short_column,
    ) in positioning_groups.items():
        if (
            long_column not in output.columns
            or short_column not in output.columns
        ):
            continue

        output[long_column] = pd.to_numeric(
            output[long_column],
            errors="coerce",
        )

        output[short_column] = pd.to_numeric(
            output[short_column],
            errors="coerce",
        )

        net_column = f"{group_name}_net_all"

        output[net_column] = (
            output[long_column]
            - output[short_column]
        )

        if open_interest_column in output.columns:
            output[open_interest_column] = pd.to_numeric(
                output[open_interest_column],
                errors="coerce",
            )

            output[f"{group_name}_net_pct_open_interest"] = (
                safe_divide(
                    output[net_column],
                    output[open_interest_column],
                )
                * 100
            )

    return output


def validate_cftc_data(
    df: pd.DataFrame,
) -> None:
    required_columns = {
        "archive_year",
        "market_and_exchange_names",
        "report_date_as_yyyy_mm_dd",
        "open_interest_all",
    }

    missing_columns = required_columns.difference(
        df.columns
    )

    if missing_columns:
        raise ValueError(
            "CFTC output is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError(
            "CFTC output contains no rows."
        )

    if df["report_date_as_yyyy_mm_dd"].isna().all():
        raise ValueError(
            "CFTC output contains no valid report dates."
        )

    current_date = (
        pd.Timestamp.now(tz="UTC")
        .tz_localize(None)
        .normalize()
    )

    future_rows = df[
        df["report_date_as_yyyy_mm_dd"]
        > current_date
    ]

    if not future_rows.empty:
        future_dates = (
            future_rows["report_date_as_yyyy_mm_dd"]
            .dt.strftime("%Y-%m-%d")
            .tolist()[:10]
        )

        raise ValueError(
            "CFTC output contains future report dates: "
            f"{future_dates}"
        )

    duplicate_columns = [
        "market_and_exchange_names",
        "report_date_as_yyyy_mm_dd",
        "cftc_contract_market_code",
    ]

    available_duplicate_columns = [
        column
        for column in duplicate_columns
        if column in df.columns
    ]

    if available_duplicate_columns:
        duplicate_count = df.duplicated(
            subset=available_duplicate_columns,
            keep=False,
        ).sum()

        if duplicate_count:
            print(
                "[WARN] CFTC output contains "
                f"{duplicate_count:,} duplicate-key rows."
            )

    invalid_open_interest = df[
        df["open_interest_all"] < 0
    ]

    if not invalid_open_interest.empty:
        raise ValueError(
            "CFTC output contains negative open interest."
        )


def save_outputs(
    df: pd.DataFrame,
    output_dir: Path,
    run_time_utc: datetime,
) -> None:
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    timestamp = run_time_utc.strftime(
        "%Y_%m_%d_%H%M%S"
    )

    parquet_path = (
        output_dir
        / f"{DATASET}_{timestamp}.parquet"
    )

    csv_path = (
        output_dir
        / f"{DATASET}_{timestamp}.csv"
    )

    latest_parquet = (
        output_dir
        / f"{DATASET}_latest.parquet"
    )

    latest_csv = (
        output_dir
        / f"{DATASET}_latest.csv"
    )

    df.to_parquet(
        parquet_path,
        index=False,
    )

    df.to_csv(
        csv_path,
        index=False,
    )

    df.to_parquet(
        latest_parquet,
        index=False,
    )

    df.to_csv(
        latest_csv,
        index=False,
    )

    print()
    print("[DONE] CFTC COT TFF data saved.")
    print(f"Rows:           {len(df):,}")
    print(f"Columns:        {len(df.columns):,}")
    print(
        "Archive years: "
        f"{sorted(df['archive_year'].dropna().unique().tolist())}"
    )

    if "market_and_exchange_names" in df.columns:
        print(
            "Markets:        "
            f"{df['market_and_exchange_names'].nunique():,}"
        )

    if "report_date_as_yyyy_mm_dd" in df.columns:
        print(
            "Date range:     "
            f"{df['report_date_as_yyyy_mm_dd'].min().date()} "
            f"-> "
            f"{df['report_date_as_yyyy_mm_dd'].max().date()}"
        )

    print(f"Parquet:        {parquet_path}")
    print(f"CSV:            {csv_path}")
    print(f"Latest parquet: {latest_parquet}")
    print(f"Latest CSV:     {latest_csv}")


def main() -> None:
    print("=" * 90)
    print(
        "BACQE INFORMATION DATA - "
        "CFTC COT TRADERS IN FINANCIAL FUTURES"
    )
    print("=" * 90)

    run_time_utc = datetime.now(timezone.utc)
    data_lake_root = get_data_lake_root()

    output_dir = build_output_dir(
        data_lake_root,
        run_time_utc,
    )

    print(f"Data lake:  {data_lake_root}")
    print(f"Output dir: {output_dir}")
    print(f"Years:      {YEARS_TO_COLLECT}")
    print("-" * 90)

    df = collect_cftc_tff(
        run_time_utc,
    )

    if df.empty:
        print(
            "[WARN] No CFTC COT data collected."
        )
        return

    df = add_research_features(df)

    validate_cftc_data(df)

    save_outputs(
        df,
        output_dir,
        run_time_utc,
    )

    print("=" * 90)


if __name__ == "__main__":
    main()