"""
BACQE DATA REGISTRY - 01 Scan Data Lake

Scans the Quant_Lab data lake and creates a file-level inventory.

Output:
    E:/Quant_Lab/data/analysis/data_registry/data_lake_inventory_latest.csv
    E:/Quant_Lab/data/analysis/data_registry/data_lake_inventory_latest.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

OUTPUT_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "data_registry"

ALLOWED_EXTENSIONS = {
    ".csv",
    ".parquet",
    ".json",
    ".txt",
    ".db",
    ".sqlite",
    ".xlsx",
    ".xlsm",
    ".yaml",
    ".yml",
    ".log",
}


# =============================================================================
# HELPERS
# =============================================================================

def guess_dataset_group(file_path: Path) -> str:
    """
    Guess the broad BACQE dataset group based on the file path.
    """

    path_text = str(file_path).lower()

    if "tick" in path_text or "ticks" in path_text:
        return "tick_data"

    if "ohlcv" in path_text or "mt5_ohlcv" in path_text:
        return "ohlcv_data"

    if "regime" in path_text or "regimes" in path_text:
        return "regime_engine"

    if "macro" in path_text or "imf" in path_text or "ons" in path_text or "cftc" in path_text:
        return "macro_information"

    if "greyhound" in path_text or "rpg" in path_text:
        return "greyhound_data"

    if "betfair" in path_text:
        return "betfair_data"

    if "football" in path_text or "soccer" in path_text:
        return "football_data"

    if "sports" in path_text:
        return "sports_data"

    if "information_data" in path_text:
        return "information_data"

    if "analysis" in path_text:
        return "analysis_output"

    if "raw" in path_text:
        return "raw_data"

    if "processed" in path_text:
        return "processed_data"

    return "unknown"


def guess_source(file_path: Path) -> str:
    """
    Guess the likely source/provider based on the file path.
    """

    path_text = str(file_path).lower()

    if "ftmo" in path_text:
        return "FTMO"

    if "mt5" in path_text or "metatrader" in path_text:
        return "MetaTrader5"

    if "betfair" in path_text:
        return "Betfair"

    if "rpg" in path_text or "racingpost" in path_text:
        return "Racing Post"

    if "imf" in path_text:
        return "IMF"

    if "ons" in path_text:
        return "ONS"

    if "cftc" in path_text or "cot" in path_text:
        return "CFTC"

    return "unknown"


def extract_year_month(file_path: Path) -> tuple:
    """
    Try to extract year and month from folder names such as:
        year=2026
        month=05
        2026
        May 2026

    This is intentionally simple for v1.
    """

    year = None
    month = None

    for part in file_path.parts:
        part_lower = part.lower()

        if part_lower.startswith("year="):
            value = part_lower.replace("year=", "")
            if value.isdigit():
                year = int(value)

        if part_lower.startswith("month="):
            value = part_lower.replace("month=", "")
            if value.isdigit():
                month = int(value)

        if part.isdigit() and len(part) == 4:
            possible_year = int(part)
            if 2000 <= possible_year <= 2100:
                year = possible_year

    return year, month


def safe_file_stat(file_path: Path) -> dict | None:
    """
    Safely collect file metadata.
    Returns None if the file cannot be accessed.
    """

    try:
        stat = file_path.stat()

        modified_dt = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        created_dt = datetime.fromtimestamp(stat.st_ctime, tz=timezone.utc)

        year, month = extract_year_month(file_path)

        return {
            "file_path": str(file_path),
            "file_name": file_path.name,
            "extension": file_path.suffix.lower(),
            "parent_folder": str(file_path.parent),
            "dataset_group": guess_dataset_group(file_path),
            "source_guess": guess_source(file_path),
            "file_size_mb": round(stat.st_size / (1024 * 1024), 4),
            "created_time_utc": created_dt.isoformat(),
            "modified_time_utc": modified_dt.isoformat(),
            "year_guess": year,
            "month_guess": month,
        }

    except PermissionError:
        print(f"[WARN] Permission denied: {file_path}")
        return None

    except FileNotFoundError:
        print(f"[WARN] File disappeared during scan: {file_path}")
        return None

    except Exception as exc:
        print(f"[WARN] Could not read metadata for {file_path}: {exc}")
        return None


# =============================================================================
# MAIN SCANNER
# =============================================================================

def scan_data_lake(root_dir: Path) -> pd.DataFrame:
    """
    Recursively scan the data lake and return a file inventory DataFrame.
    """

    if not root_dir.exists():
        raise FileNotFoundError(f"Data lake root does not exist: {root_dir}")

    records = []

    print("=" * 90)
    print("BACQE DATA REGISTRY - 01 SCAN DATA LAKE")
    print("=" * 90)
    print(f"Scanning root: {root_dir}")
    print("-" * 90)

    file_counter = 0

    for file_path in root_dir.rglob("*"):
        if not file_path.is_file():
            continue

        if file_path.suffix.lower() not in ALLOWED_EXTENSIONS:
            continue

        metadata = safe_file_stat(file_path)

        if metadata is not None:
            records.append(metadata)
            file_counter += 1

        if file_counter > 0 and file_counter % 1000 == 0:
            print(f"[INFO] Files scanned: {file_counter:,}")

    df = pd.DataFrame(records)

    if df.empty:
        print("[WARN] No files found.")
        return df

    df["scan_time_utc"] = datetime.now(timezone.utc).isoformat()

    df = df.sort_values(
        by=["dataset_group", "source_guess", "modified_time_utc", "file_name"],
        ascending=[True, True, False, True],
    ).reset_index(drop=True)

    return df


def save_inventory(df: pd.DataFrame, output_dir: Path) -> None:
    """
    Save inventory outputs.
    """

    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "data_lake_inventory_latest.csv"
    parquet_path = output_dir / "data_lake_inventory_latest.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print("-" * 90)
    print("[DONE] Data lake inventory created.")
    print(f"Rows:        {len(df):,}")
    print(f"CSV:         {csv_path}")
    print(f"Parquet:     {parquet_path}")
    print("-" * 90)

    print("\nDataset group summary:")
    print(df["dataset_group"].value_counts(dropna=False).to_string())

    print("\nSource guess summary:")
    print(df["source_guess"].value_counts(dropna=False).to_string())


def main() -> None:
    df = scan_data_lake(DATA_LAKE_ROOT)
    save_inventory(df, OUTPUT_DIR)


if __name__ == "__main__":
    main()