"""
BACQE DATA REGISTRY - 03 Build Master Dataset Registry

Combines:
    01 data_lake_inventory_latest.csv
    02 dataset_profiles_latest.csv

Creates:
    dataset_registry_latest.csv
    dataset_registry_latest.parquet

This is the master BACQE data catalog.
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
REGISTRY_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "data_registry"

INVENTORY_PATH = REGISTRY_DIR / "data_lake_inventory_latest.csv"
PROFILES_PATH = REGISTRY_DIR / "dataset_profiles_latest.csv"

OUTPUT_CSV = REGISTRY_DIR / "dataset_registry_latest.csv"
OUTPUT_PARQUET = REGISTRY_DIR / "dataset_registry_latest.parquet"


# =============================================================================
# HELPERS
# =============================================================================

def safe_to_datetime(series: pd.Series) -> pd.Series:
    """
    Safely convert a Series to UTC datetimes.
    """
    return pd.to_datetime(series, errors="coerce", utc=True)


def derive_dataset_name(file_path: str, file_name: str, dataset_group: str) -> str:
    """
    Derive a practical dataset name from the file path.
    This is intentionally simple for v1.
    """

    path_lower = str(file_path).lower()
    stem = Path(str(file_name)).stem

    if dataset_group == "tick_data":
        parts = Path(str(file_path)).parts
        symbol = "unknown_symbol"

        for part in parts:
            if str(part).lower().startswith("symbol="):
                symbol = str(part).split("=", 1)[1]

        return f"tick_data_{symbol}"

    if dataset_group == "ohlcv_data":
        return f"ohlcv_{stem}"

    if dataset_group == "regime_engine":
        if "classified" in path_lower:
            return f"regime_classified_{stem}"
        if "forecast" in path_lower:
            return f"regime_forecast_{stem}"
        if "transition" in path_lower:
            return f"regime_transition_{stem}"
        return f"regime_{stem}"

    if dataset_group == "greyhound_data":
        if "betfair" in path_lower:
            return f"greyhound_betfair_{stem}"
        if "rpg" in path_lower or "racingpost" in path_lower:
            return f"greyhound_rpg_{stem}"
        return f"greyhound_{stem}"

    if dataset_group == "macro_information":
        return f"macro_{stem}"

    if dataset_group == "information_data":
        return f"information_{stem}"

    return stem


def calculate_freshness_days(row: pd.Series) -> int | None:
    """
    Prefer max_date from the actual dataset.
    Fall back to modified_time_utc from the file metadata.
    """

    now = pd.Timestamp.now(tz="UTC")

    max_date = pd.to_datetime(row.get("max_date"), errors="coerce", utc=True)

    if pd.notna(max_date):
        return int((now - max_date).days)

    modified_time = pd.to_datetime(row.get("modified_time_utc"), errors="coerce", utc=True)

    if pd.notna(modified_time):
        return int((now - modified_time).days)

    return None


def classify_freshness(days: int | None) -> str:
    """
    Convert freshness days into a broad label.
    """

    if days is None:
        return "unknown"

    if days <= 1:
        return "fresh"

    if days <= 7:
        return "recent"

    if days <= 30:
        return "acceptable"

    if days <= 90:
        return "aging"

    return "stale"


def calculate_quality_score(row: pd.Series) -> int:
    """
    Practical v1 quality score.

    Starts at 100 and deducts points for:
    - failed read
    - missing values
    - duplicate rows
    - stale data
    - unknown dataset grouping
    - no row count
    - no columns
    """

    score = 100

    read_status = row.get("read_status")
    missing_pct = row.get("missing_value_pct")
    duplicate_count = row.get("duplicate_row_count_profiled")
    freshness_days = row.get("freshness_days")
    dataset_group = row.get("dataset_group")
    row_count = row.get("row_count_profiled")
    column_count = row.get("column_count")

    if read_status != "success":
        score -= 40

    if pd.notna(missing_pct):
        if missing_pct > 25:
            score -= 20
        elif missing_pct > 10:
            score -= 10
        elif missing_pct > 5:
            score -= 5

    if pd.notna(duplicate_count):
        if duplicate_count > 10_000:
            score -= 20
        elif duplicate_count > 1_000:
            score -= 10
        elif duplicate_count > 100:
            score -= 5

    if pd.notna(freshness_days):
        if freshness_days > 365:
            score -= 15
        elif freshness_days > 90:
            score -= 10
        elif freshness_days > 30:
            score -= 5

    if dataset_group in {"unknown", None}:
        score -= 10

    if pd.isna(row_count):
        score -= 10
    elif row_count == 0:
        score -= 15

    if pd.isna(column_count):
        if str(row.get("extension")).lower() not in {".db", ".sqlite"}:
            score -= 10
    elif column_count == 0:
        score -= 15

    return max(0, min(100, int(score)))


def classify_quality(score: int) -> str:
    """
    Convert score into a readable label.
    """

    if score >= 90:
        return "excellent"

    if score >= 75:
        return "good"

    if score >= 60:
        return "usable"

    if score >= 40:
        return "warning"

    return "poor"


def classify_dataset_status(row: pd.Series) -> str:
    """
    Dataset status based on quality and read result.
    """

    if row.get("read_status") != "success":
        return "failed_read"

    score = row.get("quality_score", 0)
    freshness = row.get("freshness_category")

    if score >= 90:
        return "ready"

    if score >= 75:
        return "usable_with_caution"

    if freshness == "stale":
        return "stale_needs_review"

    if score >= 60:
        return "needs_review"

    return "poor_quality"


def is_analysis_ready(row: pd.Series) -> bool:
    """
    Whether a dataset is broadly ready to analyse.
    """

    if row.get("read_status") != "success":
        return False

    if row.get("quality_score", 0) < 60:
        return False

    if pd.isna(row.get("row_count_profiled")):
        return False

    if row.get("row_count_profiled") == 0:
        return False

    return True


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=" * 90)
    print("BACQE DATA REGISTRY - 03 BUILD MASTER DATASET REGISTRY")
    print("=" * 90)

    if not INVENTORY_PATH.exists():
        raise FileNotFoundError(f"Inventory not found: {INVENTORY_PATH}")

    if not PROFILES_PATH.exists():
        raise FileNotFoundError(f"Profiles not found: {PROFILES_PATH}")

    inventory = pd.read_csv(INVENTORY_PATH)
    profiles = pd.read_csv(PROFILES_PATH)

    print(f"Inventory rows: {len(inventory):,}")
    print(f"Profile rows:   {len(profiles):,}")
    print("-" * 90)

    merge_cols = ["file_path"]

    registry = inventory.merge(
        profiles,
        on=merge_cols,
        how="left",
        suffixes=("_inventory", "_profile"),
    )

    # Resolve duplicate/suffixed columns where needed
    for col in ["file_name", "extension", "dataset_group", "source_guess", "file_size_mb"]:
        inv_col = f"{col}_inventory"
        prof_col = f"{col}_profile"

        if inv_col in registry.columns:
            registry[col] = registry[inv_col]
        elif prof_col in registry.columns:
            registry[col] = registry[prof_col]

    registry["dataset_name"] = registry.apply(
        lambda row: derive_dataset_name(
            row.get("file_path"),
            row.get("file_name"),
            row.get("dataset_group"),
        ),
        axis=1,
    )

    registry["freshness_days"] = registry.apply(calculate_freshness_days, axis=1)
    registry["freshness_category"] = registry["freshness_days"].apply(classify_freshness)

    registry["quality_score"] = registry.apply(calculate_quality_score, axis=1)
    registry["quality_label"] = registry["quality_score"].apply(classify_quality)

    registry["dataset_status"] = registry.apply(classify_dataset_status, axis=1)
    registry["analysis_ready"] = registry.apply(is_analysis_ready, axis=1)

    registry["registry_build_time_utc"] = datetime.now(timezone.utc).isoformat()

    preferred_cols = [
        "dataset_name",
        "dataset_group",
        "source_guess",
        "dataset_status",
        "analysis_ready",
        "quality_score",
        "quality_label",
        "freshness_days",
        "freshness_category",
        "file_name",
        "extension",
        "file_size_mb",
        "row_count_profiled",
        "column_count",
        "date_column_guess",
        "min_date",
        "max_date",
        "missing_value_pct",
        "duplicate_row_count_profiled",
        "schema_hash",
        "read_status",
        "read_mode",
        "error_message",
        "file_path",
        "parent_folder",
        "modified_time_utc",
        "created_time_utc",
        "year_guess",
        "month_guess",
        "scan_time_utc",
        "profile_time_utc",
        "registry_build_time_utc",
    ]

    existing_preferred_cols = [col for col in preferred_cols if col in registry.columns]
    remaining_cols = [col for col in registry.columns if col not in existing_preferred_cols]

    registry = registry[existing_preferred_cols + remaining_cols]

    registry = registry.sort_values(
        by=["analysis_ready", "quality_score", "dataset_group", "dataset_name"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)

    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)

    registry.to_csv(OUTPUT_CSV, index=False)
    registry.to_parquet(OUTPUT_PARQUET, index=False)

    print("[DONE] Master dataset registry created.")
    print(f"Rows:      {len(registry):,}")
    print(f"CSV:       {OUTPUT_CSV}")
    print(f"Parquet:   {OUTPUT_PARQUET}")
    print("-" * 90)

    print("\nAnalysis readiness:")
    print(registry["analysis_ready"].value_counts(dropna=False).to_string())

    print("\nDataset status summary:")
    print(registry["dataset_status"].value_counts(dropna=False).to_string())

    print("\nQuality label summary:")
    print(registry["quality_label"].value_counts(dropna=False).to_string())

    print("\nDataset group summary:")
    print(registry["dataset_group"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()