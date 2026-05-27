"""
BACQE SENTINEL 02 - FEATURE PIPELINE HEALTH CHECK

Purpose:
    Audit BACQE regime feature parquet outputs.

Checks:
    - Feature files exist
    - Files can be read
    - Required regime feature columns exist
    - Row counts
    - Duplicate timestamps
    - Data freshness
    - NaN ratios
    - Infinite values
    - Feature pipeline status

Outputs:
    E:/Quant_Lab/data/analysis/sentinel/feature_pipeline_health/feature_pipeline_health_latest.csv
    E:/Quant_Lab/data/analysis/sentinel/feature_pipeline_health/feature_pipeline_health_latest.json
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

DATA_LAKE = Path("E:/Quant_Lab")

FEATURE_ROOT = DATA_LAKE / "data" / "processed" / "regimes" / "features" / "FTMO"

OUTPUT_DIR = DATA_LAKE / "data" / "analysis" / "sentinel" / "feature_pipeline_health"

OUTPUT_CSV = OUTPUT_DIR / "feature_pipeline_health_latest.csv"
OUTPUT_JSON = OUTPUT_DIR / "feature_pipeline_health_latest.json"

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

CORE_FEATURE_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
]

IMPORTANT_FEATURE_HINTS = [
    "atr",
    "rsi",
    "bb",
    "ema",
    "sma",
    "trend",
    "volatility",
    "momentum",
]

FRESHNESS_LIMITS_HOURS = {
    "M1": 3,
    "M2": 4,
    "M3": 4,
    "M4": 5,
    "M5": 6,
    "M6": 6,
    "M10": 8,
    "M12": 8,
    "M15": 12,
    "M20": 14,
    "M30": 18,
    "H1": 36,
    "H2": 48,
    "H3": 60,
    "H4": 84,
    "H6": 120,
    "H8": 144,
    "H12": 192,
    "D1": 288,
    "W1": 24 * 21,
    "MN1": 24 * 60,
}


# =============================================================================
# HELPERS
# =============================================================================

def now_utc() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc))


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def safe_read_parquet(path: Path) -> pd.DataFrame | None:
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def find_timestamp_column(df: pd.DataFrame) -> str | None:
    for col in TIMESTAMP_CANDIDATES:
        if col in df.columns:
            return col
    return None


def infer_timeframe_from_path(path: Path) -> str:
    for part in path.parts:
        if part in FRESHNESS_LIMITS_HOURS:
            return part

    stem_parts = path.stem.split("_")
    for part in stem_parts:
        if part in FRESHNESS_LIMITS_HOURS:
            return part

    return "UNKNOWN"


def infer_symbol_from_path(path: Path) -> str:
    stem = path.stem

    for tf in FRESHNESS_LIMITS_HOURS:
        suffix = f"_{tf}"
        if stem.endswith(suffix):
            return stem.replace(suffix, "")

    if "_" in stem:
        return stem.split("_")[0]

    return stem


def classify_status(issues: list[str], critical_issues: list[str]) -> str:
    if critical_issues:
        return "CRITICAL"
    if issues:
        return "WARNING"
    return "OK"


def score_health(status: str, issue_count: int, nan_ratio: float | None) -> int:
    if status == "CRITICAL":
        return 0

    score = 100 - issue_count * 12

    if nan_ratio is not None:
        if nan_ratio > 0.50:
            score -= 40
        elif nan_ratio > 0.25:
            score -= 25
        elif nan_ratio > 0.10:
            score -= 10

    if status == "WARNING":
        score = min(score, 85)

    return int(max(0, min(100, score)))


def detect_feature_columns(columns: list[str]) -> list[str]:
    lower_map = {col: col.lower() for col in columns}
    detected = []

    for original, lower in lower_map.items():
        if any(hint in lower for hint in IMPORTANT_FEATURE_HINTS):
            detected.append(original)

    return detected


def calculate_nan_ratio(df: pd.DataFrame) -> float:
    if df.empty:
        return 1.0

    total_cells = df.shape[0] * df.shape[1]

    if total_cells == 0:
        return 1.0

    return float(df.isna().sum().sum() / total_cells)


def calculate_inf_count(df: pd.DataFrame) -> int:
    numeric_df = df.select_dtypes(include=[np.number])

    if numeric_df.empty:
        return 0

    return int(np.isinf(numeric_df.to_numpy()).sum())


# =============================================================================
# FILE CHECK
# =============================================================================

def check_feature_file(path: Path) -> dict:
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
        "columns_count": 0,
        "timestamp_column": None,
        "first_timestamp": None,
        "last_timestamp": None,
        "age_hours": None,
        "duplicate_timestamps": None,
        "is_monotonic_increasing": None,
        "missing_core_columns": None,
        "detected_feature_columns_count": 0,
        "detected_feature_columns_sample": None,
        "nan_ratio": None,
        "inf_count": None,
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
        result["health_score"] = score_health(status, len(issues) + len(critical_issues), None)
        result["issues"] = "; ".join(issues)
        result["critical_issues"] = "; ".join(critical_issues)
        return result

    result["rows"] = len(df)
    result["columns_count"] = len(df.columns)

    if df.empty:
        critical_issues.append("Feature file is empty")
        status = classify_status(issues, critical_issues)
        result["status"] = status
        result["health_score"] = score_health(status, len(issues) + len(critical_issues), 1.0)
        result["issues"] = "; ".join(issues)
        result["critical_issues"] = "; ".join(critical_issues)
        return result

    missing_core = [col for col in CORE_FEATURE_COLUMNS if col not in df.columns]
    result["missing_core_columns"] = ", ".join(missing_core)

    if missing_core:
        issues.append(f"Missing core OHLC columns: {missing_core}")

    detected_features = detect_feature_columns(list(df.columns))
    result["detected_feature_columns_count"] = len(detected_features)
    result["detected_feature_columns_sample"] = ", ".join(detected_features[:20])

    if len(detected_features) == 0:
        issues.append("No obvious derived feature columns detected")

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
                        f"Stale feature data: age {age_hours:.2f}h exceeds limit {freshness_limit}h"
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

    try:
        nan_ratio = calculate_nan_ratio(df)
        result["nan_ratio"] = round(nan_ratio, 6)

        if nan_ratio > 0.50:
            critical_issues.append(f"Extreme NaN ratio detected: {nan_ratio:.2%}")
        elif nan_ratio > 0.25:
            issues.append(f"High NaN ratio detected: {nan_ratio:.2%}")
        elif nan_ratio > 0.10:
            issues.append(f"Moderate NaN ratio detected: {nan_ratio:.2%}")

    except Exception as exc:
        critical_issues.append(f"NaN validation failed: {exc}")
        nan_ratio = None

    try:
        inf_count = calculate_inf_count(df)
        result["inf_count"] = inf_count

        if inf_count > 0:
            issues.append(f"Infinite numeric values detected: {inf_count}")

    except Exception as exc:
        critical_issues.append(f"Infinite value validation failed: {exc}")

    status = classify_status(issues, critical_issues)

    result["status"] = status
    result["health_score"] = score_health(status, len(issues) + len(critical_issues), result["nan_ratio"])
    result["issues"] = "; ".join(issues)
    result["critical_issues"] = "; ".join(critical_issues)

    return result


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    ensure_output_dir()

    print("=" * 90)
    print("BACQE SENTINEL 02 - FEATURE PIPELINE HEALTH CHECK")
    print("=" * 90)
    print(f"Feature root:      {FEATURE_ROOT}")
    print(f"Output directory:  {OUTPUT_DIR}")
    print("-" * 90)

    parquet_files = sorted(FEATURE_ROOT.rglob("*.parquet"))

    if not parquet_files:
        print("[WARN] No feature parquet files found.")
        return

    print(f"[INFO] Found {len(parquet_files):,} feature parquet files.")

    records = []

    for i, path in enumerate(parquet_files, start=1):
        print(f"[CHECK] {i:,}/{len(parquet_files):,} -> {path.name}")
        records.append(check_feature_file(path))

    health_df = pd.DataFrame(records)

    health_df = health_df.sort_values(
        by=["status", "timeframe", "symbol", "file_name"],
        ascending=[True, True, True, True],
    )

    health_df.to_csv(OUTPUT_CSV, index=False)

    summary = {
        "checked_at_utc": now_utc().isoformat(),
        "feature_root": str(FEATURE_ROOT),
        "files_checked": int(len(health_df)),
        "ok_files": int((health_df["status"] == "OK").sum()),
        "warning_files": int((health_df["status"] == "WARNING").sum()),
        "critical_files": int((health_df["status"] == "CRITICAL").sum()),
        "average_health_score": round(float(health_df["health_score"].mean()), 2),
        "average_nan_ratio": round(float(health_df["nan_ratio"].fillna(0).mean()), 6),
        "total_inf_values": int(health_df["inf_count"].fillna(0).sum()),
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
    print("[DONE] Feature pipeline health check complete.")
    print(f"Files checked:       {summary['files_checked']:,}")
    print(f"OK files:            {summary['ok_files']:,}")
    print(f"Warning files:       {summary['warning_files']:,}")
    print(f"Critical files:      {summary['critical_files']:,}")
    print(f"Average health:      {summary['average_health_score']}")
    print(f"Average NaN ratio:   {summary['average_nan_ratio']}")
    print(f"Total inf values:    {summary['total_inf_values']:,}")
    print(f"CSV output:          {OUTPUT_CSV}")
    print(f"JSON output:         {OUTPUT_JSON}")
    print("=" * 90)


if __name__ == "__main__":
    main()