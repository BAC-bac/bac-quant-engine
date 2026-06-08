"""
BACQE SENTINEL 05 - SENTINEL SUMMARY REPORT

Purpose:
    Build one top-level Sentinel summary from the health-check outputs created by
    Sentinel scripts 01-04.

Inputs:
    E:/Quant_Lab/data/analysis/sentinel/**/**/*_latest.csv
    E:/Quant_Lab/data/analysis/sentinel/**/**/*_latest.json

Outputs:
    E:/Quant_Lab/data/analysis/sentinel/sentinel_summary/sentinel_summary_latest.csv
    E:/Quant_Lab/data/analysis/sentinel/sentinel_summary/sentinel_summary_latest.json
    E:/Quant_Lab/data/analysis/sentinel/sentinel_summary/sentinel_summary_latest.txt
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


DATA_LAKE = Path("E:/Quant_Lab")

SENTINEL_ROOT = DATA_LAKE / "data" / "analysis" / "sentinel"
OUTPUT_DIR = SENTINEL_ROOT / "sentinel_summary"

OUTPUT_CSV = OUTPUT_DIR / "sentinel_summary_latest.csv"
OUTPUT_JSON = OUTPUT_DIR / "sentinel_summary_latest.json"
OUTPUT_TXT = OUTPUT_DIR / "sentinel_summary_latest.txt"

EXCLUDE_DIR_NAMES = {
    "sentinel_summary",
    "sentinel_suite",
    "data_lake_freshness",
}


def now_utc() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc))


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def safe_read_csv(path: Path) -> pd.DataFrame | None:
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def infer_check_name(path: Path) -> str:
    parent_name = path.parent.name

    name_map = {
        "regime_classification_health": "regime_classification_health",
        "regime_forecast_health": "regime_forecast_health",
        "market_data_health": "market_data_health",
        "feature_health": "feature_health",
    }

    return name_map.get(parent_name, parent_name)


def classify_summary_status(
    critical_files: int,
    warning_files: int,
    average_health_score: float,
) -> str:
    if critical_files > 0:
        return "CRITICAL"

    if average_health_score < 50:
        return "CRITICAL"

    if warning_files > 0:
        return "WARNING"

    if average_health_score < 80:
        return "WARNING"

    return "OK"


def calculate_check_summary(path: Path) -> dict:
    checked_at = now_utc()
    check_name = infer_check_name(path)

    result = {
        "checked_at_utc": checked_at.isoformat(),
        "check_name": check_name,
        "source_file": str(path),
        "source_file_name": path.name,
        "source_modified_utc": datetime.fromtimestamp(
            path.stat().st_mtime,
            timezone.utc,
        ).isoformat(),
        "rows": 0,
        "ok_files": 0,
        "warning_files": 0,
        "critical_files": 0,
        "average_health_score": None,
        "minimum_health_score": None,
        "maximum_health_score": None,
        "average_nan_ratio": None,
        "total_inf_values": None,
        "total_invalid_probability_values": None,
        "total_invalid_strict_probability_values": None,
        "total_invalid_soft_score_values": None,
        "summary_status": "UNKNOWN",
        "summary_message": "",
    }

    df = safe_read_csv(path)

    if df is None:
        result["summary_status"] = "CRITICAL"
        result["summary_message"] = "Could not read Sentinel health CSV."
        return result

    if df.empty:
        result["summary_status"] = "CRITICAL"
        result["summary_message"] = "Sentinel health CSV is empty."
        return result

    result["rows"] = int(len(df))

    if "status" in df.columns:
        result["ok_files"] = int((df["status"] == "OK").sum())
        result["warning_files"] = int((df["status"] == "WARNING").sum())
        result["critical_files"] = int((df["status"] == "CRITICAL").sum())

    if "health_score" in df.columns:
        health_scores = pd.to_numeric(df["health_score"], errors="coerce").dropna()

        if not health_scores.empty:
            result["average_health_score"] = round(float(health_scores.mean()), 2)
            result["minimum_health_score"] = round(float(health_scores.min()), 2)
            result["maximum_health_score"] = round(float(health_scores.max()), 2)

    if "nan_ratio" in df.columns:
        nan_ratios = pd.to_numeric(df["nan_ratio"], errors="coerce").dropna()

        if not nan_ratios.empty:
            result["average_nan_ratio"] = round(float(nan_ratios.mean()), 6)

    if "inf_count" in df.columns:
        result["total_inf_values"] = int(
            pd.to_numeric(df["inf_count"], errors="coerce").fillna(0).sum()
        )

    if "invalid_probability_count" in df.columns:
        result["total_invalid_probability_values"] = int(
            pd.to_numeric(df["invalid_probability_count"], errors="coerce")
            .fillna(0)
            .sum()
        )

    if "invalid_strict_probability_count" in df.columns:
        result["total_invalid_strict_probability_values"] = int(
            pd.to_numeric(df["invalid_strict_probability_count"], errors="coerce")
            .fillna(0)
            .sum()
        )

    if "invalid_soft_score_count" in df.columns:
        result["total_invalid_soft_score_values"] = int(
            pd.to_numeric(df["invalid_soft_score_count"], errors="coerce")
            .fillna(0)
            .sum()
        )

    average_health_score = result["average_health_score"]

    if average_health_score is None:
        average_health_score = 0.0

    result["summary_status"] = classify_summary_status(
        critical_files=result["critical_files"],
        warning_files=result["warning_files"],
        average_health_score=average_health_score,
    )

    result["summary_message"] = (
        f"{check_name}: {result['summary_status']} | "
        f"rows={result['rows']:,}, "
        f"ok={result['ok_files']:,}, "
        f"warnings={result['warning_files']:,}, "
        f"critical={result['critical_files']:,}, "
        f"avg_health={result['average_health_score']}"
    )

    return result


def discover_latest_health_csvs() -> list[Path]:
    files = []

    for path in SENTINEL_ROOT.rglob("*latest.csv"):
        if any(part in EXCLUDE_DIR_NAMES for part in path.parts):
            continue

        files.append(path)

    return sorted(set(files))


def build_text_report(summary_df: pd.DataFrame, overall_summary: dict) -> str:
    lines = []

    lines.append("=" * 90)
    lines.append("BACQE SENTINEL 05 - SENTINEL SUMMARY REPORT")
    lines.append("=" * 90)
    lines.append(f"Checked at UTC:       {overall_summary['checked_at_utc']}")
    lines.append(f"Sentinel root:         {SENTINEL_ROOT}")
    lines.append(f"Checks discovered:     {overall_summary['checks_discovered']:,}")
    lines.append(f"Overall status:        {overall_summary['overall_status']}")
    lines.append(f"Average health score:  {overall_summary['average_health_score']}")
    lines.append(f"OK checks:             {overall_summary['ok_checks']:,}")
    lines.append(f"Warning checks:        {overall_summary['warning_checks']:,}")
    lines.append(f"Critical checks:       {overall_summary['critical_checks']:,}")
    lines.append("-" * 90)

    if summary_df.empty:
        lines.append("[WARN] No Sentinel health check files found.")
    else:
        for _, row in summary_df.iterrows():
            lines.append(
                f"{row['check_name']}: "
                f"{row['summary_status']} | "
                f"rows={row['rows']:,} | "
                f"ok={row['ok_files']:,} | "
                f"warnings={row['warning_files']:,} | "
                f"critical={row['critical_files']:,} | "
                f"avg_health={row['average_health_score']}"
            )

    lines.append("-" * 90)
    lines.append(f"CSV output:            {OUTPUT_CSV}")
    lines.append(f"JSON output:           {OUTPUT_JSON}")
    lines.append(f"TXT output:            {OUTPUT_TXT}")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    ensure_output_dir()

    print("=" * 90)
    print("BACQE SENTINEL 05 - SENTINEL SUMMARY REPORT")
    print("=" * 90)

    latest_csvs = discover_latest_health_csvs()

    print(f"[INFO] Sentinel latest CSV files discovered: {len(latest_csvs):,}")

    records = []

    for i, path in enumerate(latest_csvs, start=1):
        print(f"[READ] {i:,}/{len(latest_csvs):,} -> {path}")
        records.append(calculate_check_summary(path))

    summary_df = pd.DataFrame(records)

    if not summary_df.empty:
        summary_df = summary_df.sort_values(
            by=["summary_status", "average_health_score", "check_name"],
            ascending=[True, True, True],
        )

    if summary_df.empty:
        overall_status = "UNKNOWN"
        average_health_score = None
        ok_checks = 0
        warning_checks = 0
        critical_checks = 0
    else:
        ok_checks = int((summary_df["summary_status"] == "OK").sum())
        warning_checks = int((summary_df["summary_status"] == "WARNING").sum())
        critical_checks = int((summary_df["summary_status"] == "CRITICAL").sum())

        average_health_score = round(
            float(
                pd.to_numeric(summary_df["average_health_score"], errors="coerce")
                .dropna()
                .mean()
            ),
            2,
        )

        if critical_checks > 0:
            overall_status = "CRITICAL"
        elif warning_checks > 0:
            overall_status = "WARNING"
        else:
            overall_status = "OK"

    overall_summary = {
        "checked_at_utc": now_utc().isoformat(),
        "sentinel_root": str(SENTINEL_ROOT),
        "checks_discovered": int(len(summary_df)),
        "overall_status": overall_status,
        "average_health_score": average_health_score,
        "ok_checks": ok_checks,
        "warning_checks": warning_checks,
        "critical_checks": critical_checks,
        "outputs": {
            "csv": str(OUTPUT_CSV),
            "json": str(OUTPUT_JSON),
            "txt": str(OUTPUT_TXT),
        },
    }

    summary_df.to_csv(OUTPUT_CSV, index=False)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(
            {
                "overall_summary": overall_summary,
                "checks": summary_df.to_dict(orient="records"),
            },
            f,
            indent=4,
        )

    text_report = build_text_report(summary_df, overall_summary)

    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write(text_report)

    print("-" * 90)
    print(text_report)
    print("=" * 90)


if __name__ == "__main__":
    main()