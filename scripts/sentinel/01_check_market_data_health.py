"""
BACQE SENTINEL 01 - MARKET DATA HEALTH CHECK

Purpose:
    Scan the BACQE FTMO OHLCV parquet data lake and produce a health report.

Checks:
    - File exists and can be read
    - Required OHLCV columns exist
    - Timestamp column exists
    - Row count
    - Duplicate timestamps
    - Data freshness
    - Basic OHLC sanity checks
    - Time ordering

Outputs:
    E:/Quant_Lab/data/analysis/sentinel/market_data_health/market_data_health_latest.csv
    E:/Quant_Lab/data/analysis/sentinel/market_data_health/market_data_health_latest.json
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_LAKE = Path("E:/Quant_Lab")

MARKET_DATA_ROOT = DATA_LAKE / "data" / "raw" / "fx" / "mt5_ohlcv" / "FTMO"

OUTPUT_DIR = DATA_LAKE / "data" / "analysis" / "sentinel" / "market_data_health"

OUTPUT_CSV = OUTPUT_DIR / "market_data_health_latest.csv"
OUTPUT_JSON = OUTPUT_DIR / "market_data_health_latest.json"

REQUIRED_PRICE_COLUMNS = ["open", "high", "low", "close"]

OPTIONAL_COLUMNS = ["tick_volume", "real_volume", "spread", "volume"]

TIMESTAMP_CANDIDATES = [
    "time",
    "datetime",
    "timestamp",
    "date",
    "Time",
    "Datetime",
    "Timestamp",
    "Date",
]

FRESHNESS_LIMITS_HOURS = {
    "M1": 2,
    "M2": 3,
    "M3": 3,
    "M4": 4,
    "M5": 4,
    "M6": 4,
    "M10": 6,
    "M12": 6,
    "M15": 8,
    "M20": 10,
    "M30": 12,
    "H1": 24,
    "H2": 36,
    "H3": 48,
    "H4": 72,
    "H6": 96,
    "H8": 120,
    "H12": 168,
    "D1": 240,
    "W1": 24 * 14,
    "MN1": 24 * 45,
}


# =============================================================================
# HELPERS
# =============================================================================

def now_utc() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc))


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def find_timestamp_column(df: pd.DataFrame) -> str | None:
    for col in TIMESTAMP_CANDIDATES:
        if col in df.columns:
            return col
    return None


def infer_timeframe_from_path(path: Path) -> str:
    for part in path.parts:
        if part in FRESHNESS_LIMITS_HOURS:
            return part
    return "UNKNOWN"


def infer_symbol_from_path(path: Path) -> str:
    stem = path.stem

    # Expected examples:
    # GBPUSD_M15.parquet
    # EURUSD_H1.parquet
    if "_" in stem:
        return stem.split("_")[0]

    return stem


def score_health(status: str, issue_count: int) -> int:
    if status == "CRITICAL":
        return 0
    if status == "WARNING":
        return max(25, 100 - issue_count * 15)
    return 100


def classify_status(issues: list[str], critical_issues: list[str]) -> str:
    if critical_issues:
        return "CRITICAL"
    if issues:
        return "WARNING"
    return "OK"


def safe_read_parquet(path: Path) -> pd.DataFrame | None:
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


# =============================================================================
# FILE CHECK
# =============================================================================

def check_market_file(path: Path) -> dict:
    checked_at = now_utc()

    symbol = infer_symbol_from_path(path)
    timeframe = infer_timeframe_from_path(path)

    issues: list[str] = []
    critical_issues: list[str] = []

    result = {
        "checked_at_utc": checked_at.isoformat(),
        "symbol": symbol,
        "timeframe": timeframe,
        "file_path": str(path),
        "file_name": path.name,
        "file_size_mb": round(path.stat().st_size / (1024 * 1024), 3),
        "rows": 0,
        "columns": None,
        "timestamp_column": None,
        "first_timestamp": None,
        "last_timestamp": None,
        "age_hours": None,
        "duplicate_timestamps": None,
        "is_monotonic_increasing": None,
        "missing_required_columns": None,
        "available_volume_columns": None,
        "ohlc_invalid_rows": None,
        "status": None,
        "health_score": None,
        "issues": None,
        "critical_issues": None,
    }

    df = safe_read_parquet(path)

    if df is None:
        critical_issues.append("Could not read parquet file")
        status = classify_status(issues, critical_issues)
        result["status"] = status
        result["health_score"] = score_health(status, len(issues) + len(critical_issues))
        result["issues"] = "; ".join(issues)
        result["critical_issues"] = "; ".join(critical_issues)
        return result

    result["rows"] = len(df)
    result["columns"] = ", ".join(df.columns.astype(str))

    if df.empty:
        critical_issues.append("File is empty")
        status = classify_status(issues, critical_issues)
        result["status"] = status
        result["health_score"] = score_health(status, len(issues) + len(critical_issues))
        result["issues"] = "; ".join(issues)
        result["critical_issues"] = "; ".join(critical_issues)
        return result

    missing_required = [col for col in REQUIRED_PRICE_COLUMNS if col not in df.columns]
    result["missing_required_columns"] = ", ".join(missing_required)

    if missing_required:
        critical_issues.append(f"Missing required OHLC columns: {missing_required}")

    available_volume_cols = [col for col in OPTIONAL_COLUMNS if col in df.columns]
    result["available_volume_columns"] = ", ".join(available_volume_cols)

    if not available_volume_cols:
        issues.append("No recognised volume/spread column found")

    timestamp_col = find_timestamp_column(df)
    result["timestamp_column"] = timestamp_col

    if timestamp_col is None:
        critical_issues.append("No timestamp column found")
    else:
        try:
            timestamps = pd.to_datetime(df[timestamp_col], errors="coerce", utc=True)
            valid_timestamps = timestamps.dropna()

            if valid_timestamps.empty:
                critical_issues.append("Timestamp column exists but contains no valid timestamps")
            else:
                first_ts = valid_timestamps.min()
                last_ts = valid_timestamps.max()

                result["first_timestamp"] = first_ts.isoformat()
                result["last_timestamp"] = last_ts.isoformat()

                age_hours = (checked_at - last_ts).total_seconds() / 3600
                result["age_hours"] = round(age_hours, 2)

                freshness_limit = FRESHNESS_LIMITS_HOURS.get(timeframe)

                if freshness_limit is None:
                    issues.append("Unknown timeframe; freshness rule not applied")
                elif age_hours > freshness_limit:
                    issues.append(
                        f"Stale data: age {age_hours:.2f}h exceeds limit {freshness_limit}h"
                    )

                duplicate_count = int(timestamps.duplicated().sum())
                result["duplicate_timestamps"] = duplicate_count

                if duplicate_count > 0:
                    issues.append(f"Duplicate timestamps found: {duplicate_count}")

                is_monotonic = bool(valid_timestamps.is_monotonic_increasing)
                result["is_monotonic_increasing"] = is_monotonic

                if not is_monotonic:
                    issues.append("Timestamps are not monotonic increasing")

        except Exception as exc:
            critical_issues.append(f"Timestamp validation failed: {exc}")

    if not missing_required:
        try:
            o = pd.to_numeric(df["open"], errors="coerce")
            h = pd.to_numeric(df["high"], errors="coerce")
            l = pd.to_numeric(df["low"], errors="coerce")
            c = pd.to_numeric(df["close"], errors="coerce")

            invalid_ohlc = (
                (h < l)
                | (o > h)
                | (o < l)
                | (c > h)
                | (c < l)
                | o.isna()
                | h.isna()
                | l.isna()
                | c.isna()
            )

            invalid_count = int(invalid_ohlc.sum())
            result["ohlc_invalid_rows"] = invalid_count

            if invalid_count > 0:
                issues.append(f"Invalid OHLC rows found: {invalid_count}")

        except Exception as exc:
            critical_issues.append(f"OHLC validation failed: {exc}")

    status = classify_status(issues, critical_issues)

    result["status"] = status
    result["health_score"] = score_health(status, len(issues) + len(critical_issues))
    result["issues"] = "; ".join(issues)
    result["critical_issues"] = "; ".join(critical_issues)

    return result


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    ensure_output_dir()

    print("=" * 90)
    print("BACQE SENTINEL 01 - MARKET DATA HEALTH CHECK")
    print("=" * 90)
    print(f"Market data root: {MARKET_DATA_ROOT}")
    print(f"Output directory: {OUTPUT_DIR}")
    print("-" * 90)

    parquet_files = sorted(MARKET_DATA_ROOT.rglob("*.parquet"))

    if not parquet_files:
        print("[WARN] No parquet files found.")
        return

    print(f"[INFO] Found {len(parquet_files):,} parquet files.")

    records = []

    for i, path in enumerate(parquet_files, start=1):
        print(f"[CHECK] {i:,}/{len(parquet_files):,} -> {path.name}")
        records.append(check_market_file(path))

    health_df = pd.DataFrame(records)

    health_df = health_df.sort_values(
        by=["status", "timeframe", "symbol", "file_name"],
        ascending=[True, True, True, True],
    )

    health_df.to_csv(OUTPUT_CSV, index=False)

    summary = {
        "checked_at_utc": now_utc().isoformat(),
        "market_data_root": str(MARKET_DATA_ROOT),
        "files_checked": int(len(health_df)),
        "ok_files": int((health_df["status"] == "OK").sum()),
        "warning_files": int((health_df["status"] == "WARNING").sum()),
        "critical_files": int((health_df["status"] == "CRITICAL").sum()),
        "average_health_score": round(float(health_df["health_score"].mean()), 2),
        "outputs": {
            "csv": str(OUTPUT_CSV),
            "json": str(OUTPUT_JSON),
        },
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(
            {
                "summary": summary,
                "records": health_df.to_dict(orient="records"),
            },
            f,
            indent=4,
        )

    print("-" * 90)
    print("[DONE] Market data health check complete.")
    print(f"Files checked:       {summary['files_checked']:,}")
    print(f"OK files:            {summary['ok_files']:,}")
    print(f"Warning files:       {summary['warning_files']:,}")
    print(f"Critical files:      {summary['critical_files']:,}")
    print(f"Average health:      {summary['average_health_score']}")
    print(f"CSV output:          {OUTPUT_CSV}")
    print(f"JSON output:         {OUTPUT_JSON}")
    print("=" * 90)


if __name__ == "__main__":
    main()