"""
BACQE SENTINEL 04 - REGIME FORECAST HEALTH CHECK

Purpose:
    Audit BACQE regime forecast, transition, and strategy-router outputs.

Checks:
    - Forecast / transition / router files exist
    - Files can be read
    - Missing values
    - Infinite values
    - Strict probability columns stay within 0-1
    - Soft confidence / likelihood style columns stay within sensible bounds
    - Transition probability row sums are sensible
    - Forecast confidence health
    - Stale forecast outputs

Outputs:
    E:/Quant_Lab/data/analysis/sentinel/regime_forecast_health/regime_forecast_health_latest.csv
    E:/Quant_Lab/data/analysis/sentinel/regime_forecast_health/regime_forecast_health_latest.json
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


DATA_LAKE = Path("E:/Quant_Lab")

FORECAST_ROOTS = [
    DATA_LAKE / "data" / "analysis" / "regime_forecasts",
    DATA_LAKE / "data" / "analysis" / "regime_transitions",
    DATA_LAKE / "data" / "analysis" / "strategy_router_decisions",
]

OUTPUT_DIR = DATA_LAKE / "data" / "analysis" / "sentinel" / "regime_forecast_health"

OUTPUT_CSV = OUTPUT_DIR / "regime_forecast_health_latest.csv"
OUTPUT_JSON = OUTPUT_DIR / "regime_forecast_health_latest.json"

MAX_FILE_AGE_HOURS = 24 * 7


STRICT_PROBABILITY_HINTS = [
    "probability",
    "_probability",
    "transition_probability",
    "next_regime_probability",
]

SOFT_SCORE_HINTS = [
    "confidence",
    "likelihood",
    "persistence",
]

EXCLUDED_NUMERIC_HINTS = [
    "count",
    "score",
    "strength",
    "rank",
    "index",
    "signal",
    "row",
    "id",
    "weight",
    "from_",
    "to_",
    "count",
    "total",
    "frequency",
    "freq",
]

TIMESTAMP_CANDIDATES = [
    "time",
    "datetime",
    "timestamp",
    "date",
    "created_at",
    "forecast_time",
    "checked_at_utc",
    "Time",
    "Datetime",
    "Timestamp",
    "Date",
]


def now_utc() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc))


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def safe_read_file(path: Path) -> pd.DataFrame | None:
    try:
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)

        if path.suffix.lower() == ".csv":
            return pd.read_csv(path)

        if path.suffix.lower() == ".json":
            raw = pd.read_json(path)

            if isinstance(raw, pd.Series):
                return raw.to_frame().T

            return raw

    except Exception:
        return None

    return None


def find_timestamp_column(df: pd.DataFrame) -> str | None:
    for col in TIMESTAMP_CANDIDATES:
        if col in df.columns:
            return col

    return None


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


def is_excluded_numeric_column(col: str) -> bool:
    lower = str(col).lower()

    return any(excluded in lower for excluded in EXCLUDED_NUMERIC_HINTS)


def detect_strict_probability_columns(df: pd.DataFrame) -> list[str]:
    allowed_exact_names = [
        "transition_probability",
        "next_regime_probability",
        "forecast_probability",
        "probability",
    ]

    detected = []

    for col in df.columns:
        lower = str(col).lower()

        if lower in allowed_exact_names:
            if pd.api.types.is_numeric_dtype(df[col]):
                detected.append(col)

    return detected


def detect_soft_score_columns(df: pd.DataFrame) -> list[str]:
    detected = []

    strict_cols = set(detect_strict_probability_columns(df))

    for col in df.columns:
        lower = str(col).lower()

        if col in strict_cols:
            continue

        if is_excluded_numeric_column(lower):
            continue

        if any(hint in lower for hint in SOFT_SCORE_HINTS):
            if pd.api.types.is_numeric_dtype(df[col]):
                detected.append(col)

    return detected


def classify_status(issues: list[str], critical_issues: list[str]) -> str:
    if critical_issues:
        return "CRITICAL"

    if issues:
        return "WARNING"

    return "OK"


def score_health(
    status: str,
    issue_count: int,
    nan_ratio: float | None,
    invalid_strict_probability_count: int | None,
    invalid_soft_score_count: int | None,
) -> int:
    if status == "CRITICAL":
        return 0

    score = 100 - issue_count * 10

    if nan_ratio is not None:
        if nan_ratio > 0.50:
            score -= 40
        elif nan_ratio > 0.25:
            score -= 25
        elif nan_ratio > 0.10:
            score -= 10

    if invalid_strict_probability_count is not None:
        if invalid_strict_probability_count > 100:
            score -= 30
        elif invalid_strict_probability_count > 10:
            score -= 20
        elif invalid_strict_probability_count > 0:
            score -= 10

    if invalid_soft_score_count is not None:
        if invalid_soft_score_count > 100:
            score -= 15
        elif invalid_soft_score_count > 10:
            score -= 8
        elif invalid_soft_score_count > 0:
            score -= 4

    if status == "WARNING":
        score = min(score, 90)

    return int(max(0, min(100, score)))


def infer_output_type(path: Path) -> str:
    lower = str(path).lower()

    if "forecast" in lower:
        return "forecast"

    if "transition" in lower:
        return "transition"

    if "router" in lower or "decision" in lower:
        return "router_decision"

    return "unknown"


def check_forecast_file(path: Path) -> dict:
    checked_at = now_utc()

    issues: list[str] = []
    critical_issues: list[str] = []

    file_modified_ts = pd.Timestamp(
        datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    )

    result = {
        "checked_at_utc": checked_at.isoformat(),
        "output_type": infer_output_type(path),
        "file_path": str(path),
        "file_name": path.name,
        "file_extension": path.suffix.lower(),
        "file_size_mb": round(path.stat().st_size / (1024 * 1024), 3),
        "file_modified_utc": file_modified_ts.isoformat(),
        "file_age_hours": round((checked_at - file_modified_ts).total_seconds() / 3600, 2),
        "rows": 0,
        "columns_count": 0,
        "timestamp_column": None,
        "first_timestamp": None,
        "last_timestamp": None,
        "data_age_hours": None,
        "strict_probability_columns_count": 0,
        "strict_probability_columns_sample": None,
        "soft_score_columns_count": 0,
        "soft_score_columns_sample": None,
        "invalid_strict_probability_count": 0,
        "invalid_soft_score_count": 0,
        "strict_probability_row_sum_min": None,
        "strict_probability_row_sum_max": None,
        "strict_probability_row_sum_mean": None,
        "nan_ratio": None,
        "inf_count": None,
        "status": None,
        "health_score": None,
        "issues": None,
        "critical_issues": None,
    }

    if result["file_age_hours"] > MAX_FILE_AGE_HOURS:
        issues.append(
            f"Forecast/transition/router file is stale by modified time: {result['file_age_hours']}h"
        )

    df = safe_read_file(path)

    if df is None:
        critical_issues.append("Could not read forecast/transition/router file")

        status = classify_status(issues, critical_issues)

        result["status"] = status
        result["health_score"] = score_health(
            status=status,
            issue_count=len(issues) + len(critical_issues),
            nan_ratio=None,
            invalid_strict_probability_count=None,
            invalid_soft_score_count=None,
        )
        result["issues"] = "; ".join(issues)
        result["critical_issues"] = "; ".join(critical_issues)

        return result

    result["rows"] = len(df)
    result["columns_count"] = len(df.columns)

    if df.empty:
        critical_issues.append("Forecast/transition/router file is empty")

        status = classify_status(issues, critical_issues)

        result["status"] = status
        result["health_score"] = score_health(
            status=status,
            issue_count=len(issues) + len(critical_issues),
            nan_ratio=1.0,
            invalid_strict_probability_count=None,
            invalid_soft_score_count=None,
        )
        result["issues"] = "; ".join(issues)
        result["critical_issues"] = "; ".join(critical_issues)

        return result

    timestamp_col = find_timestamp_column(df)
    result["timestamp_column"] = timestamp_col

    if timestamp_col is not None:
        try:
            timestamps = pd.to_datetime(df[timestamp_col], errors="coerce", utc=True)
            valid_timestamps = timestamps.dropna()

            if not valid_timestamps.empty:
                first_ts = valid_timestamps.min()
                last_ts = valid_timestamps.max()

                result["first_timestamp"] = first_ts.isoformat()
                result["last_timestamp"] = last_ts.isoformat()
                result["data_age_hours"] = round(
                    (checked_at - last_ts).total_seconds() / 3600,
                    2,
                )

                if result["data_age_hours"] > MAX_FILE_AGE_HOURS:
                    issues.append(
                        f"Forecast/transition/router data timestamp appears stale: {result['data_age_hours']}h"
                    )

        except Exception as exc:
            issues.append(f"Timestamp parsing failed: {exc}")

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

    strict_probability_cols = detect_strict_probability_columns(df)
    soft_score_cols = detect_soft_score_columns(df)

    result["strict_probability_columns_count"] = len(strict_probability_cols)
    result["strict_probability_columns_sample"] = ", ".join(strict_probability_cols[:20])

    result["soft_score_columns_count"] = len(soft_score_cols)
    result["soft_score_columns_sample"] = ", ".join(soft_score_cols[:20])

    if not strict_probability_cols and result["output_type"] in ["forecast", "transition"]:
        issues.append("No strict probability columns detected")

    if strict_probability_cols:
        try:
            probability_df = df[strict_probability_cols].apply(pd.to_numeric, errors="coerce")

            invalid_probability_mask = (probability_df < 0) | (probability_df > 1)
            invalid_probability_count = int(invalid_probability_mask.sum().sum())

            result["invalid_strict_probability_count"] = invalid_probability_count

            if invalid_probability_count > 0:
                issues.append(
                    f"Strict probability values outside 0-1 range detected: {invalid_probability_count}"
                )

            row_sums = probability_df.fillna(0).sum(axis=1)

            result["strict_probability_row_sum_min"] = round(float(row_sums.min()), 6)
            result["strict_probability_row_sum_max"] = round(float(row_sums.max()), 6)
            result["strict_probability_row_sum_mean"] = round(float(row_sums.mean()), 6)

            if result["output_type"] == "transition":
                if result["strict_probability_row_sum_mean"] < 0.50:
                    issues.append("Transition strict probability row sums appear unusually low")
                elif result["strict_probability_row_sum_mean"] > 2.50:
                    issues.append("Transition strict probability row sums appear unusually high")

        except Exception as exc:
            issues.append(f"Strict probability validation failed: {exc}")

    if soft_score_cols:
        try:
            soft_score_df = df[soft_score_cols].apply(pd.to_numeric, errors="coerce")

            invalid_soft_score_mask = (soft_score_df < 0) | (soft_score_df > 100)
            invalid_soft_score_count = int(invalid_soft_score_mask.sum().sum())

            result["invalid_soft_score_count"] = invalid_soft_score_count

            if invalid_soft_score_count > 0:
                issues.append(
                    f"Soft confidence/likelihood values outside 0-100 range detected: {invalid_soft_score_count}"
                )

        except Exception as exc:
            issues.append(f"Soft score validation failed: {exc}")

    status = classify_status(issues, critical_issues)

    result["status"] = status
    result["health_score"] = score_health(
        status=status,
        issue_count=len(issues) + len(critical_issues),
        nan_ratio=result["nan_ratio"],
        invalid_strict_probability_count=result["invalid_strict_probability_count"],
        invalid_soft_score_count=result["invalid_soft_score_count"],
    )
    result["issues"] = "; ".join(issues)
    result["critical_issues"] = "; ".join(critical_issues)

    return result


def main() -> None:
    ensure_output_dir()

    print("=" * 90)
    print("BACQE SENTINEL 04 - REGIME FORECAST HEALTH CHECK")
    print("=" * 90)

    files = []

    for root in FORECAST_ROOTS:
        print(f"Scanning root: {root}")

        if root.exists():
            files.extend(root.rglob("*latest*.parquet"))
            files.extend(root.rglob("*latest*.csv"))
            files.extend(root.rglob("*latest*.json"))
        else:
            print(f"[WARN] Root does not exist: {root}")

    files = sorted(set(files))

    print("-" * 90)

    if not files:
        print("[WARN] No forecast / transition / router files found.")
        return

    print(f"[INFO] Found {len(files):,} forecast/transition/router files.")

    records = []

    for i, path in enumerate(files, start=1):
        print(f"[CHECK] {i:,}/{len(files):,} -> {path.name}")
        records.append(check_forecast_file(path))

    health_df = pd.DataFrame(records)

    health_df = health_df.sort_values(
        by=["status", "health_score", "output_type", "file_name"],
        ascending=[True, True, True, True],
    )

    health_df.to_csv(OUTPUT_CSV, index=False)

    summary = {
        "checked_at_utc": now_utc().isoformat(),
        "roots_checked": [str(root) for root in FORECAST_ROOTS],
        "files_checked": int(len(health_df)),
        "ok_files": int((health_df["status"] == "OK").sum()),
        "warning_files": int((health_df["status"] == "WARNING").sum()),
        "critical_files": int((health_df["status"] == "CRITICAL").sum()),
        "average_health_score": round(float(health_df["health_score"].mean()), 2),
        "average_nan_ratio": round(float(health_df["nan_ratio"].fillna(0).mean()), 6),
        "total_inf_values": int(health_df["inf_count"].fillna(0).sum()),
        "total_invalid_strict_probability_values": int(
            health_df["invalid_strict_probability_count"].fillna(0).sum()
        ),
        "total_invalid_soft_score_values": int(
            health_df["invalid_soft_score_count"].fillna(0).sum()
        ),
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
    print("[DONE] Regime forecast health check complete.")
    print(f"Files checked:                         {summary['files_checked']:,}")
    print(f"OK files:                              {summary['ok_files']:,}")
    print(f"Warning files:                         {summary['warning_files']:,}")
    print(f"Critical files:                        {summary['critical_files']:,}")
    print(f"Average health:                        {summary['average_health_score']}")
    print(f"Average NaN ratio:                     {summary['average_nan_ratio']}")
    print(f"Total inf values:                      {summary['total_inf_values']:,}")
    print(
        f"Invalid strict probability values:     "
        f"{summary['total_invalid_strict_probability_values']:,}"
    )
    print(
        f"Invalid soft score values:             "
        f"{summary['total_invalid_soft_score_values']:,}"
    )
    print(f"CSV output:                            {OUTPUT_CSV}")
    print(f"JSON output:                           {OUTPUT_JSON}")
    print("=" * 90)


if __name__ == "__main__":
    main()