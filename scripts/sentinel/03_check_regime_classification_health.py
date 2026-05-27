"""
BACQE SENTINEL 03 - REGIME CLASSIFICATION HEALTH CHECK

Purpose:
    Audit BACQE classified regime parquet outputs.

Checks:
    - Classified regime files exist
    - Files can be read
    - Timestamp freshness
    - Duplicate timestamps
    - Required regime columns
    - Regime confidence quality
    - Excessive neutral/unknown states
    - NaN ratios
    - Infinite values
    - Regime distribution balance

Outputs:
    E:/Quant_Lab/data/analysis/sentinel/regime_classification_health/regime_classification_health_latest.csv
    E:/Quant_Lab/data/analysis/sentinel/regime_classification_health/regime_classification_health_latest.json
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


DATA_LAKE = Path("E:/Quant_Lab")

CLASSIFIED_ROOT = DATA_LAKE / "data" / "processed" / "regimes" / "classified" / "FTMO"

OUTPUT_DIR = DATA_LAKE / "data" / "analysis" / "sentinel" / "regime_classification_health"

OUTPUT_CSV = OUTPUT_DIR / "regime_classification_health_latest.csv"
OUTPUT_JSON = OUTPUT_DIR / "regime_classification_health_latest.json"

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

REQUIRED_REGIME_COLUMNS = [
    "composite_regime",
    "trend_state",
    "volatility_state",
    "momentum_state",
    "trend_strength_state",
    "regime_confidence",
]

REGIME_LABEL_COLUMNS = [
    "composite_regime",
    "trend_state",
    "volatility_state",
    "momentum_state",
    "trend_strength_state",
]

NEUTRAL_LIKE_VALUES = [
    "neutral",
    "range",
    "unknown",
    "mixed",
    "sideways",
    "none",
    "nan",
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
        suffix = f"_{tf}_classified"
        if stem.endswith(suffix):
            return stem.replace(suffix, "")

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


def score_health(status: str, issue_count: int, confidence_mean: float | None, neutral_ratio: float | None) -> int:
    if status == "CRITICAL":
        return 0

    score = 100 - issue_count * 12

    if confidence_mean is not None:
        if confidence_mean < 0.20:
            score -= 35
        elif confidence_mean < 0.40:
            score -= 20
        elif confidence_mean < 0.55:
            score -= 10

    if neutral_ratio is not None:
        if neutral_ratio > 0.90:
            score -= 30
        elif neutral_ratio > 0.75:
            score -= 20
        elif neutral_ratio > 0.60:
            score -= 10

    if status == "WARNING":
        score = min(score, 85)

    return int(max(0, min(100, score)))


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


def calculate_neutral_ratio(series: pd.Series) -> float:
    cleaned = series.astype(str).str.lower().str.strip()

    if cleaned.empty:
        return 1.0

    neutral_mask = cleaned.apply(
        lambda value: any(neutral_value in value for neutral_value in NEUTRAL_LIKE_VALUES)
    )

    return float(neutral_mask.mean())


def check_regime_file(path: Path) -> dict:
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
        "missing_required_regime_columns": None,
        "available_regime_columns": None,
        "composite_regime_unique_count": None,
        "top_composite_regime": None,
        "top_composite_regime_ratio": None,
        "neutral_like_ratio": None,
        "regime_confidence_mean": None,
        "regime_confidence_min": None,
        "regime_confidence_max": None,
        "low_confidence_ratio": None,
        "nan_ratio": None,
        "inf_count": None,
        "status": None,
        "health_score": None,
        "issues": None,
        "critical_issues": None,
    }

    df = safe_read_parquet(path)

    if df is None:
        critical_issues.append("Could not read classified regime parquet file")
        status = classify_status(issues, critical_issues)
        result["status"] = status
        result["health_score"] = score_health(status, len(issues) + len(critical_issues), None, None)
        result["issues"] = "; ".join(issues)
        result["critical_issues"] = "; ".join(critical_issues)
        return result

    result["rows"] = len(df)
    result["columns_count"] = len(df.columns)

    if df.empty:
        critical_issues.append("Classified regime file is empty")
        status = classify_status(issues, critical_issues)
        result["status"] = status
        result["health_score"] = score_health(status, len(issues) + len(critical_issues), None, None)
        result["issues"] = "; ".join(issues)
        result["critical_issues"] = "; ".join(critical_issues)
        return result

    missing_required = [col for col in REQUIRED_REGIME_COLUMNS if col not in df.columns]
    available_regime_columns = [col for col in REGIME_LABEL_COLUMNS if col in df.columns]

    result["missing_required_regime_columns"] = ", ".join(missing_required)
    result["available_regime_columns"] = ", ".join(available_regime_columns)

    if missing_required:
        critical_issues.append(f"Missing required regime columns: {missing_required}")

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
                        f"Stale classified regime data: age {age_hours:.2f}h exceeds limit {freshness_limit}h"
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

    if "composite_regime" in df.columns:
        try:
            composite = df["composite_regime"].astype(str).str.lower().str.strip()
            value_counts = composite.value_counts(dropna=False)

            result["composite_regime_unique_count"] = int(value_counts.shape[0])

            if not value_counts.empty:
                top_label = str(value_counts.index[0])
                top_ratio = float(value_counts.iloc[0] / len(df))

                result["top_composite_regime"] = top_label
                result["top_composite_regime_ratio"] = round(top_ratio, 6)

                if top_ratio > 0.95:
                    issues.append(f"Composite regime dominated by one label: {top_label} at {top_ratio:.2%}")
                elif top_ratio > 0.85:
                    issues.append(f"High composite regime concentration: {top_label} at {top_ratio:.2%}")

            neutral_ratio = calculate_neutral_ratio(df["composite_regime"])
            result["neutral_like_ratio"] = round(neutral_ratio, 6)

            if neutral_ratio > 0.90:
                issues.append(f"Extreme neutral/unknown-like regime ratio: {neutral_ratio:.2%}")
            elif neutral_ratio > 0.75:
                issues.append(f"High neutral/unknown-like regime ratio: {neutral_ratio:.2%}")
            elif neutral_ratio > 0.60:
                issues.append(f"Moderate neutral/unknown-like regime ratio: {neutral_ratio:.2%}")

        except Exception as exc:
            issues.append(f"Composite regime distribution validation failed: {exc}")

    if "regime_confidence" in df.columns:
        try:
            confidence = pd.to_numeric(df["regime_confidence"], errors="coerce")
            confidence_valid = confidence.dropna()

            if confidence_valid.empty:
                critical_issues.append("regime_confidence exists but contains no valid numeric values")
            else:
                confidence_mean = float(confidence_valid.mean())
                confidence_min = float(confidence_valid.min())
                confidence_max = float(confidence_valid.max())
                low_conf_ratio = float((confidence_valid < 0.40).mean())

                result["regime_confidence_mean"] = round(confidence_mean, 6)
                result["regime_confidence_min"] = round(confidence_min, 6)
                result["regime_confidence_max"] = round(confidence_max, 6)
                result["low_confidence_ratio"] = round(low_conf_ratio, 6)

                if confidence_min < 0:
                    issues.append("Negative regime confidence values detected")

                if confidence_max > 1:
                    issues.append("Regime confidence values greater than 1 detected")

                if confidence_mean < 0.20:
                    issues.append(f"Very low average regime confidence: {confidence_mean:.2%}")
                elif confidence_mean < 0.40:
                    issues.append(f"Low average regime confidence: {confidence_mean:.2%}")
                elif confidence_mean < 0.55:
                    issues.append(f"Moderate average regime confidence: {confidence_mean:.2%}")

                if low_conf_ratio > 0.75:
                    issues.append(f"High low-confidence regime ratio: {low_conf_ratio:.2%}")

        except Exception as exc:
            critical_issues.append(f"Regime confidence validation failed: {exc}")

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

    try:
        inf_count = calculate_inf_count(df)
        result["inf_count"] = inf_count

        if inf_count > 0:
            issues.append(f"Infinite numeric values detected: {inf_count}")

    except Exception as exc:
        critical_issues.append(f"Infinite value validation failed: {exc}")

    status = classify_status(issues, critical_issues)

    result["status"] = status
    result["health_score"] = score_health(
        status=status,
        issue_count=len(issues) + len(critical_issues),
        confidence_mean=result["regime_confidence_mean"],
        neutral_ratio=result["neutral_like_ratio"],
    )
    result["issues"] = "; ".join(issues)
    result["critical_issues"] = "; ".join(critical_issues)

    return result


def main() -> None:
    ensure_output_dir()

    print("=" * 90)
    print("BACQE SENTINEL 03 - REGIME CLASSIFICATION HEALTH CHECK")
    print("=" * 90)
    print(f"Classified root:    {CLASSIFIED_ROOT}")
    print(f"Output directory:   {OUTPUT_DIR}")
    print("-" * 90)

    parquet_files = sorted(CLASSIFIED_ROOT.rglob("*.parquet"))

    if not parquet_files:
        print("[WARN] No classified regime parquet files found.")
        return

    print(f"[INFO] Found {len(parquet_files):,} classified regime parquet files.")

    records = []

    for i, path in enumerate(parquet_files, start=1):
        print(f"[CHECK] {i:,}/{len(parquet_files):,} -> {path.name}")
        records.append(check_regime_file(path))

    health_df = pd.DataFrame(records)

    health_df = health_df.sort_values(
        by=["status", "health_score", "timeframe", "symbol", "file_name"],
        ascending=[True, True, True, True, True],
    )

    health_df.to_csv(OUTPUT_CSV, index=False)

    summary = {
        "checked_at_utc": now_utc().isoformat(),
        "classified_root": str(CLASSIFIED_ROOT),
        "files_checked": int(len(health_df)),
        "ok_files": int((health_df["status"] == "OK").sum()),
        "warning_files": int((health_df["status"] == "WARNING").sum()),
        "critical_files": int((health_df["status"] == "CRITICAL").sum()),
        "average_health_score": round(float(health_df["health_score"].mean()), 2),
        "average_regime_confidence": round(float(health_df["regime_confidence_mean"].fillna(0).mean()), 6),
        "average_neutral_like_ratio": round(float(health_df["neutral_like_ratio"].fillna(0).mean()), 6),
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
    print("[DONE] Regime classification health check complete.")
    print(f"Files checked:              {summary['files_checked']:,}")
    print(f"OK files:                   {summary['ok_files']:,}")
    print(f"Warning files:              {summary['warning_files']:,}")
    print(f"Critical files:             {summary['critical_files']:,}")
    print(f"Average health:             {summary['average_health_score']}")
    print(f"Average regime confidence:  {summary['average_regime_confidence']}")
    print(f"Average neutral ratio:      {summary['average_neutral_like_ratio']}")
    print(f"Total inf values:           {summary['total_inf_values']:,}")
    print(f"CSV output:                 {OUTPUT_CSV}")
    print(f"JSON output:                {OUTPUT_JSON}")
    print("=" * 90)


if __name__ == "__main__":
    main()