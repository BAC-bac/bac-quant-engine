"""
BACQE DATA REGISTRY - 02 Profile Datasets

Reads the file inventory created by 01_scan_data_lake.py and profiles supported
dataset files.

Input:
    E:/Quant_Lab/data/analysis/data_registry/data_lake_inventory_latest.csv

Output:
    E:/Quant_Lab/data/analysis/data_registry/dataset_profiles_latest.csv
    E:/Quant_Lab/data/analysis/data_registry/dataset_profiles_latest.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import hashlib
import sqlite3
import pandas as pd
import warnings


# =============================================================================
# CONFIG
# =============================================================================

DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
REGISTRY_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "data_registry"

INVENTORY_PATH = REGISTRY_DIR / "data_lake_inventory_latest.csv"

OUTPUT_CSV = REGISTRY_DIR / "dataset_profiles_latest.csv"
OUTPUT_PARQUET = REGISTRY_DIR / "dataset_profiles_latest.parquet"

SUPPORTED_EXTENSIONS = {".csv", ".parquet", ".db", ".sqlite"}

MAX_CSV_ROWS_FOR_PROFILE = 250_000
MAX_PARQUET_ROWS_FOR_PROFILE = 250_000


# =============================================================================
# HELPERS
# =============================================================================

def make_schema_hash(columns: list[str]) -> str:
    """
    Create a repeatable hash from column names.
    Useful for detecting schema changes.
    """
    joined = "|".join([str(col).strip().lower() for col in columns])
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def guess_date_column(columns: list[str]) -> str | None:
    """
    Guess likely date/time column from column names.
    """
    candidates = [
        "datetime",
        "timestamp",
        "time_msc_dt",
        "time",
        "date",
        "race_date",
        "event_date",
        "capture_time_utc",
        "modified_time_utc",
    ]

    lower_map = {str(col).lower(): col for col in columns}

    for candidate in candidates:
        if candidate in lower_map:
            return lower_map[candidate]

    for col in columns:
        col_lower = str(col).lower()
        if "date" in col_lower or "time" in col_lower:
            return col

    return None


def profile_dataframe(df: pd.DataFrame, file_path: Path, read_mode: str) -> dict:
    """
    Profile a pandas DataFrame.
    """

    columns = list(df.columns)
    date_col = guess_date_column(columns)

    min_date = None
    max_date = None

    if date_col is not None and date_col in df.columns:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                date_series = pd.to_datetime(df[date_col], errors="coerce", utc=True, )
            if date_series.notna().any():
                min_date = date_series.min().isoformat()
                max_date = date_series.max().isoformat()
        except Exception:
            min_date = None
            max_date = None

    total_cells = df.shape[0] * df.shape[1]

    if total_cells > 0:
        missing_value_pct = round((df.isna().sum().sum() / total_cells) * 100, 4)
    else:
        missing_value_pct = 0.0

    try:
        duplicate_row_count = int(df.duplicated().sum())
    except Exception:
        duplicate_row_count = None

    return {
        "file_path": str(file_path),
        "file_name": file_path.name,
        "extension": file_path.suffix.lower(),
        "read_status": "success",
        "read_mode": read_mode,
        "row_count_profiled": int(len(df)),
        "column_count": int(len(columns)),
        "columns": "|".join(map(str, columns)),
        "date_column_guess": date_col,
        "min_date": min_date,
        "max_date": max_date,
        "missing_value_pct": missing_value_pct,
        "duplicate_row_count_profiled": duplicate_row_count,
        "schema_hash": make_schema_hash(columns),
        "error_message": None,
    }


def profile_csv(file_path: Path) -> dict:
    """
    Profile a CSV file safely.
    """

    try:
        df = pd.read_csv(file_path, nrows=MAX_CSV_ROWS_FOR_PROFILE, low_memory=False)
        return profile_dataframe(df, file_path, read_mode=f"csv_nrows_{MAX_CSV_ROWS_FOR_PROFILE}")

    except UnicodeDecodeError:
        try:
            df = pd.read_csv(
                file_path,
                nrows=MAX_CSV_ROWS_FOR_PROFILE,
                low_memory=False,
                encoding="latin1",
            )
            return profile_dataframe(df, file_path, read_mode=f"csv_latin1_nrows_{MAX_CSV_ROWS_FOR_PROFILE}")
        except Exception as exc:
            return profile_error(file_path, exc)

    except Exception as exc:
        return profile_error(file_path, exc)


def profile_parquet(file_path: Path) -> dict:
    """
    Profile a Parquet file.
    """

    try:
        df = pd.read_parquet(file_path)

        if len(df) > MAX_PARQUET_ROWS_FOR_PROFILE:
            df = df.head(MAX_PARQUET_ROWS_FOR_PROFILE)
            read_mode = f"parquet_head_{MAX_PARQUET_ROWS_FOR_PROFILE}"
        else:
            read_mode = "parquet_full"

        return profile_dataframe(df, file_path, read_mode=read_mode)

    except Exception as exc:
        return profile_error(file_path, exc)


def profile_sqlite(file_path: Path) -> dict:
    """
    Profile SQLite / DB file by listing tables and row counts.
    """

    try:
        conn = sqlite3.connect(file_path)

        tables_df = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table';",
            conn,
        )

        tables = tables_df["name"].tolist()

        table_summaries = []
        total_rows = 0

        for table in tables:
            try:
                row_count = pd.read_sql_query(
                    f'SELECT COUNT(*) AS row_count FROM "{table}";',
                    conn,
                )["row_count"].iloc[0]

                total_rows += int(row_count)
                table_summaries.append(f"{table}:{row_count}")

            except Exception:
                table_summaries.append(f"{table}:ERROR")

        conn.close()

        return {
            "file_path": str(file_path),
            "file_name": file_path.name,
            "extension": file_path.suffix.lower(),
            "read_status": "success",
            "read_mode": "sqlite_table_summary",
            "row_count_profiled": int(total_rows),
            "column_count": None,
            "columns": "|".join(table_summaries),
            "date_column_guess": None,
            "min_date": None,
            "max_date": None,
            "missing_value_pct": None,
            "duplicate_row_count_profiled": None,
            "schema_hash": make_schema_hash(table_summaries),
            "error_message": None,
        }

    except Exception as exc:
        return profile_error(file_path, exc)


def profile_error(file_path: Path, exc: Exception) -> dict:
    """
    Return a failed profile record without crashing the script.
    """

    return {
        "file_path": str(file_path),
        "file_name": file_path.name,
        "extension": file_path.suffix.lower(),
        "read_status": "failed",
        "read_mode": None,
        "row_count_profiled": None,
        "column_count": None,
        "columns": None,
        "date_column_guess": None,
        "min_date": None,
        "max_date": None,
        "missing_value_pct": None,
        "duplicate_row_count_profiled": None,
        "schema_hash": None,
        "error_message": str(exc)[:500],
    }


def profile_file(file_path: Path) -> dict:
    """
    Route profiling by file extension.
    """

    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return profile_csv(file_path)

    if suffix == ".parquet":
        return profile_parquet(file_path)

    if suffix in {".db", ".sqlite"}:
        return profile_sqlite(file_path)

    return profile_error(file_path, ValueError(f"Unsupported extension: {suffix}"))


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=" * 90)
    print("BACQE DATA REGISTRY - 02 PROFILE DATASETS")
    print("=" * 90)

    if not INVENTORY_PATH.exists():
        raise FileNotFoundError(f"Inventory file not found: {INVENTORY_PATH}")

    inventory = pd.read_csv(INVENTORY_PATH)

    files_to_profile = inventory[
        inventory["extension"].isin(SUPPORTED_EXTENSIONS)
    ].copy()

    print(f"Inventory rows:      {len(inventory):,}")
    print(f"Files to profile:    {len(files_to_profile):,}")
    print("-" * 90)

    records = []

    for i, row in enumerate(files_to_profile.itertuples(index=False), start=1):
        file_path = Path(row.file_path)

        if not file_path.exists():
            records.append(profile_error(file_path, FileNotFoundError("File no longer exists")))
            continue

        profile = profile_file(file_path)

        profile["dataset_group"] = getattr(row, "dataset_group", None)
        profile["source_guess"] = getattr(row, "source_guess", None)
        profile["file_size_mb"] = getattr(row, "file_size_mb", None)
        profile["profile_time_utc"] = datetime.now(timezone.utc).isoformat()

        records.append(profile)

        if i % 250 == 0:
            successful = sum(1 for r in records if r["read_status"] == "success")
            failed = sum(1 for r in records if r["read_status"] == "failed")
            print(f"[INFO] Profiled {i:,}/{len(files_to_profile):,} | success={successful:,} | failed={failed:,}")

    profiles = pd.DataFrame(records)

    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)

    profiles.to_csv(OUTPUT_CSV, index=False)
    profiles.to_parquet(OUTPUT_PARQUET, index=False)

    print("-" * 90)
    print("[DONE] Dataset profiling complete.")
    print(f"Rows:       {len(profiles):,}")
    print(f"CSV:        {OUTPUT_CSV}")
    print(f"Parquet:    {OUTPUT_PARQUET}")
    print("-" * 90)

    print("\nRead status summary:")
    print(profiles["read_status"].value_counts(dropna=False).to_string())

    print("\nDataset group summary:")
    print(profiles["dataset_group"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()