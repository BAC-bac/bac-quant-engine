"""
09_profile_regime_pipeline_outputs.py
=====================================

BAC Quant Engine - Regime Engine
Stage 09: Profile Regime Engine pipeline outputs.

Purpose:
- Scan key Regime Engine data/output folders
- Measure file counts, file sizes and optional row counts
- Identify largest files, largest timeframes and likely bottlenecks
- Save profiling reports for performance optimisation planning

This script does not modify pipeline data.
It only reads metadata and writes analysis reports.
"""

from pathlib import Path
from datetime import datetime
import logging
import sys
import argparse

import pandas as pd


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

LOG_DIR = PROJECT_ROOT / "logs" / "regimes"

REPORT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_performance/FTMO")

RAW_DATA_ROOT = Path("E:/Quant_Lab/data/raw/fx/mt5_ohlcv/FTMO")
FEATURE_ROOT = Path("E:/Quant_Lab/data/processed/regimes/features/FTMO")
CLASSIFIED_ROOT = Path("E:/Quant_Lab/data/processed/regimes/classified/FTMO")

AUDIT_ROOT = Path("E:/Quant_Lab/data/analysis/regime_audits")
SUMMARY_ROOT = Path("E:/Quant_Lab/data/analysis/regime_summaries/FTMO")
DIAGNOSTICS_ROOT = Path("E:/Quant_Lab/data/analysis/regime_diagnostics/FTMO")
TRANSITIONS_ROOT = Path("E:/Quant_Lab/data/analysis/regime_transitions/FTMO")
FORECAST_ROOT = Path("E:/Quant_Lab/data/analysis/regime_forecasts/FTMO")
DASHBOARD_ROOT = Path("E:/Quant_Lab/data/analysis/regime_dashboard/FTMO")


LOG_DIR.mkdir(parents=True, exist_ok=True)
REPORT_ROOT.mkdir(parents=True, exist_ok=True)


# ============================================================
# TIMEFRAME GROUPS
# ============================================================

TIMEFRAME_GROUPS = {
    "small": ["M1", "M2", "M3", "M4", "M5", "M6", "M10", "M12", "M15"],
    "medium": ["M20", "M30", "H1", "H2", "H3", "H4"],
    "large": ["H6", "H8", "H12", "D1", "W1", "MN1"],
    "full": None,
}


TIMEFRAME_ORDER = {
    "M1": 1,
    "M2": 2,
    "M3": 3,
    "M4": 4,
    "M5": 5,
    "M6": 6,
    "M10": 10,
    "M12": 12,
    "M15": 15,
    "M20": 20,
    "M30": 30,
    "H1": 60,
    "H2": 120,
    "H3": 180,
    "H4": 240,
    "H6": 360,
    "H8": 480,
    "H12": 720,
    "D1": 1440,
    "W1": 10080,
    "MN1": 43200,
}


def get_allowed_timeframes(mode: str) -> set[str] | None:
    allowed_timeframes = TIMEFRAME_GROUPS.get(mode)

    if allowed_timeframes is None:
        return None

    return set(allowed_timeframes)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile BACQE Regime Engine pipeline outputs."
    )

    parser.add_argument(
        "--mode",
        choices=["full", "small", "medium", "large"],
        default="full",
        help="Choose which timeframe group to profile.",
    )

    parser.add_argument(
        "--row-counts",
        action="store_true",
        help="Read parquet files and count rows. Slower, but more informative.",
    )

    return parser.parse_args()


# ============================================================
# LOGGING
# ============================================================

log_path = LOG_DIR / f"profile_regime_pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


# ============================================================
# STAGE CONFIG
# ============================================================

STAGE_ROOTS = {
    "01_raw_ohlcv": RAW_DATA_ROOT,
    "02_features": FEATURE_ROOT,
    "03_classified_regimes": CLASSIFIED_ROOT,
    "01b_audits": AUDIT_ROOT,
    "03b_summaries": SUMMARY_ROOT,
    "03c_diagnostics": DIAGNOSTICS_ROOT,
    "04_transitions": TRANSITIONS_ROOT,
    "05_forecasts": FORECAST_ROOT,
    "06_dashboard": DASHBOARD_ROOT,
}


# ============================================================
# HELPERS
# ============================================================

def infer_timeframe_from_path(path: Path) -> str | None:
    """
    Most core data files are stored in timeframe folders:
    FTMO/M15/GBPUSD_M15.parquet

    Report files often have timeframe only as a column or mode suffix,
    so this may return None for report-level outputs.
    """
    parent_name = path.parent.name

    if parent_name in TIMEFRAME_ORDER:
        return parent_name

    stem_parts = path.stem.split("_")

    for part in stem_parts:
        if part in TIMEFRAME_ORDER:
            return part

    return None


def infer_symbol_from_path(path: Path, timeframe: str | None) -> str | None:
    """
    Attempts to infer symbol from filenames like:
    GBPUSD_M15.parquet
    GBPUSD_M15_features.parquet
    GBPUSD_M15_regimes.parquet
    """
    if timeframe is None:
        return None

    stem = path.stem

    suffixes = [
        f"_{timeframe}_features",
        f"_{timeframe}_regimes",
        f"_{timeframe}",
    ]

    for suffix in suffixes:
        if stem.endswith(suffix):
            return stem.replace(suffix, "")

    return None


def file_size_mb(path: Path) -> float:
    return round(path.stat().st_size / (1024 * 1024), 4)


def safe_row_count(path: Path, include_rows: bool) -> int | None:
    if not include_rows:
        return None

    if path.suffix.lower() != ".parquet":
        return None

    try:
        df = pd.read_parquet(path)
        return len(df)
    except Exception as exc:
        logger.warning(f"Could not read row count for {path}: {exc}")
        return None


def should_include_file(path: Path, allowed_timeframes: set[str] | None) -> bool:
    if allowed_timeframes is None:
        return True

    timeframe = infer_timeframe_from_path(path)

    if timeframe is None:
        return True

    return timeframe in allowed_timeframes


def scan_stage(
    stage_name: str,
    root: Path,
    allowed_timeframes: set[str] | None,
    include_rows: bool,
) -> list[dict]:
    if not root.exists():
        logger.warning(f"{stage_name}: root does not exist: {root}")
        return []

    files = sorted(
        [
            path for path in root.rglob("*")
            if path.is_file() and path.suffix.lower() in {".parquet", ".csv", ".log"}
        ]
    )

    files = [
        path for path in files
        if should_include_file(path, allowed_timeframes)
    ]

    logger.info(f"{stage_name}: discovered {len(files)} files after mode filter")

    rows = []

    for path in files:
        timeframe = infer_timeframe_from_path(path)
        symbol = infer_symbol_from_path(path, timeframe)

        rows.append(
            {
                "stage": stage_name,
                "path": str(path),
                "file_name": path.name,
                "suffix": path.suffix.lower(),
                "symbol": symbol,
                "timeframe": timeframe,
                "timeframe_rank": TIMEFRAME_ORDER.get(timeframe, 999999),
                "file_size_mb": file_size_mb(path),
                "rows": safe_row_count(path, include_rows),
                "modified_time": datetime.fromtimestamp(path.stat().st_mtime),
            }
        )

    return rows


def build_stage_summary(profile_df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        profile_df.groupby("stage")
        .agg(
            file_count=("file_name", "count"),
            total_size_mb=("file_size_mb", "sum"),
            avg_size_mb=("file_size_mb", "mean"),
            max_size_mb=("file_size_mb", "max"),
            total_rows=("rows", "sum"),
            latest_modified=("modified_time", "max"),
        )
        .reset_index()
    )

    summary["total_size_mb"] = summary["total_size_mb"].round(2)
    summary["avg_size_mb"] = summary["avg_size_mb"].round(4)
    summary["max_size_mb"] = summary["max_size_mb"].round(4)

    return summary.sort_values("total_size_mb", ascending=False)


def build_timeframe_summary(profile_df: pd.DataFrame) -> pd.DataFrame:
    tf_df = profile_df[profile_df["timeframe"].notna()].copy()

    if tf_df.empty:
        return pd.DataFrame()

    summary = (
        tf_df.groupby(["stage", "timeframe", "timeframe_rank"])
        .agg(
            file_count=("file_name", "count"),
            total_size_mb=("file_size_mb", "sum"),
            avg_size_mb=("file_size_mb", "mean"),
            max_size_mb=("file_size_mb", "max"),
            total_rows=("rows", "sum"),
            latest_modified=("modified_time", "max"),
        )
        .reset_index()
    )

    summary["total_size_mb"] = summary["total_size_mb"].round(2)
    summary["avg_size_mb"] = summary["avg_size_mb"].round(4)
    summary["max_size_mb"] = summary["max_size_mb"].round(4)

    return summary.sort_values(
        by=["stage", "timeframe_rank"],
        ascending=[True, True],
    )


def build_largest_files(profile_df: pd.DataFrame, n: int = 100) -> pd.DataFrame:
    return profile_df.sort_values("file_size_mb", ascending=False).head(n)


def save_report(df: pd.DataFrame, name: str, timestamp: str, mode: str) -> None:
    suffix = "" if mode == "full" else f"_{mode}"

    csv_path = REPORT_ROOT / f"{name}{suffix}_{timestamp}.csv"
    parquet_path = REPORT_ROOT / f"{name}{suffix}_{timestamp}.parquet"

    latest_csv = REPORT_ROOT / f"{name}{suffix}_latest.csv"
    latest_parquet = REPORT_ROOT / f"{name}{suffix}_latest.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    df.to_csv(latest_csv, index=False)
    df.to_parquet(latest_parquet, index=False)

    logger.info(f"Saved {name}: {latest_csv}")


# ============================================================
# MAIN
# ============================================================

def main(mode: str = "full", row_counts: bool = False) -> None:
    logger.info("=" * 80)
    logger.info("Starting BACQE Regime Engine output profiling")
    logger.info(f"Mode: {mode}")
    logger.info(f"Include row counts: {row_counts}")
    logger.info(f"Report root: {REPORT_ROOT}")
    logger.info("=" * 80)

    allowed_timeframes = get_allowed_timeframes(mode)

    if allowed_timeframes is not None:
        logger.info(f"Allowed timeframes: {sorted(allowed_timeframes)}")
    else:
        logger.info("Allowed timeframes: all")

    all_rows = []

    for stage_name, root in STAGE_ROOTS.items():
        logger.info("-" * 80)
        logger.info(f"Scanning stage: {stage_name}")
        logger.info(f"Root: {root}")

        stage_rows = scan_stage(
            stage_name=stage_name,
            root=root,
            allowed_timeframes=allowed_timeframes,
            include_rows=row_counts,
        )

        all_rows.extend(stage_rows)

    if not all_rows:
        logger.warning("No files found to profile")
        return

    profile_df = pd.DataFrame(all_rows)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    stage_summary = build_stage_summary(profile_df)
    timeframe_summary = build_timeframe_summary(profile_df)
    largest_files = build_largest_files(profile_df, n=100)

    save_report(profile_df, "regime_pipeline_file_profile", timestamp, mode)
    save_report(stage_summary, "regime_pipeline_stage_summary", timestamp, mode)

    if not timeframe_summary.empty:
        save_report(timeframe_summary, "regime_pipeline_timeframe_summary", timestamp, mode)

    save_report(largest_files, "regime_pipeline_largest_files", timestamp, mode)

    logger.info("=" * 80)
    logger.info("Profiling completed successfully")
    logger.info(f"Total files profiled: {len(profile_df)}")
    logger.info(f"Total size MB: {round(profile_df['file_size_mb'].sum(), 2)}")
    logger.info("=" * 80)

    logger.info("Stage summary:")
    logger.info(stage_summary.to_string(index=False))

    if not timeframe_summary.empty:
        logger.info("Top 20 largest stage/timeframe groups:")
        logger.info(
            timeframe_summary.sort_values(
                "total_size_mb",
                ascending=False,
            ).head(20).to_string(index=False)
        )

    logger.info("Top 20 largest files:")
    logger.info(
        largest_files[
            [
                "stage",
                "symbol",
                "timeframe",
                "file_size_mb",
                "rows",
                "file_name",
            ]
        ].head(20).to_string(index=False)
    )


if __name__ == "__main__":
    args = parse_args()
    main(mode=args.mode, row_counts=args.row_counts)