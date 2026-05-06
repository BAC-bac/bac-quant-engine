"""
01b_audit_market_data_lake.py
=============================

BAC Quant Engine - Regime Engine
Stage 01b: Audit parquet market data lake quality.

Purpose:
- Scan all OHLCV parquet files
- Analyse data coverage and quality
- Produce audit reports
- Detect weak/sparse datasets before indicators/regimes
"""

from pathlib import Path
from datetime import datetime, timezone
import logging
import sys

import pandas as pd


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_ROOT = Path("E:/Quant_Lab/data/raw/fx/mt5_ohlcv/FTMO")

REPORT_DIR = Path("E:/Quant_Lab/data/analysis/regime_audits")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = PROJECT_ROOT / "logs" / "regimes"
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

log_path = LOG_DIR / f"audit_market_data_lake_{datetime.now():%Y%m%d_%H%M%S}.log"

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
# SETTINGS
# ============================================================

MIN_ROWS_FOR_INDICATORS = 500
STALE_DAYS_WARNING = 7


# ============================================================
# AUDIT
# ============================================================

def audit_parquet_file(parquet_path: Path) -> dict:
    try:
        df = pd.read_parquet(parquet_path)

        if df.empty:
            return {
                "symbol": parquet_path.stem,
                "timeframe": parquet_path.parent.name,
                "rows": 0,
                "start_time": None,
                "end_time": None,
                "date_range_days": 0,
                "file_size_mb": round(parquet_path.stat().st_size / (1024 * 1024), 2),
                "is_empty": True,
                "is_stale": True,
                "indicator_ready": False,
                "error": None,
            }

        if "time" not in df.columns:
            raise ValueError("Missing 'time' column")

        df["time"] = pd.to_datetime(df["time"], utc=True)

        start_time = df["time"].min()
        end_time = df["time"].max()

        now_utc = pd.Timestamp.now(tz="UTC")

        stale_days = (now_utc - end_time).days

        return {
            "symbol": parquet_path.stem.replace(f"_{parquet_path.parent.name}", ""),
            "timeframe": parquet_path.parent.name,
            "rows": len(df),
            "start_time": start_time,
            "end_time": end_time,
            "date_range_days": (end_time - start_time).days,
            "file_size_mb": round(parquet_path.stat().st_size / (1024 * 1024), 2),
            "is_empty": False,
            "is_stale": stale_days > STALE_DAYS_WARNING,
            "indicator_ready": len(df) >= MIN_ROWS_FOR_INDICATORS,
            "error": None,
        }

    except Exception as exc:
        return {
            "symbol": parquet_path.stem,
            "timeframe": parquet_path.parent.name,
            "rows": None,
            "start_time": None,
            "end_time": None,
            "date_range_days": None,
            "file_size_mb": round(parquet_path.stat().st_size / (1024 * 1024), 2),
            "is_empty": None,
            "is_stale": None,
            "indicator_ready": False,
            "error": str(exc),
        }


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    logger.info("Starting market data lake audit")
    logger.info(f"Scanning root: {DATA_ROOT}")

    parquet_files = sorted(DATA_ROOT.rglob("*.parquet"))

    logger.info(f"Discovered {len(parquet_files)} parquet files")

    if not parquet_files:
        logger.warning("No parquet files found")
        return

    results = []

    for idx, parquet_path in enumerate(parquet_files, start=1):
        logger.info(f"[{idx}/{len(parquet_files)}] Auditing {parquet_path.name}")

        result = audit_parquet_file(parquet_path)
        results.append(result)

    audit_df = pd.DataFrame(results)

    audit_df = audit_df.sort_values(
        by=["indicator_ready", "rows"],
        ascending=[False, False],
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = REPORT_DIR / f"market_data_audit_{timestamp}.csv"
    parquet_path = REPORT_DIR / f"market_data_audit_{timestamp}.parquet"

    latest_csv = REPORT_DIR / "market_data_audit_latest.csv"
    latest_parquet = REPORT_DIR / "market_data_audit_latest.parquet"

    audit_df.to_csv(csv_path, index=False)
    audit_df.to_csv(latest_csv, index=False)

    audit_df.to_parquet(parquet_path, index=False)
    audit_df.to_parquet(latest_parquet, index=False)

    logger.info("Audit completed successfully")
    logger.info(f"Saved CSV report: {csv_path}")
    logger.info(f"Saved Parquet report: {parquet_path}")

    logger.info("Summary:")

    logger.info(f"Total files: {len(audit_df)}")

    logger.info(
        f"Indicator-ready files: "
        f"{audit_df['indicator_ready'].sum()}"
    )

    logger.info(
        f"Empty files: "
        f"{audit_df['is_empty'].fillna(False).sum()}"
    )

    logger.info(
        f"Files with errors: "
        f"{audit_df['error'].notna().sum()}"
    )

    logger.info(
        f"Stale files: "
        f"{audit_df['is_stale'].fillna(False).sum()}"
    )

    top_smallest = audit_df.nsmallest(10, "rows")[
        ["symbol", "timeframe", "rows"]
    ]

    logger.info("Smallest datasets:")
    logger.info(top_smallest.to_string(index=False))


if __name__ == "__main__":
    main()