"""
BACQE TICK RESEARCH - 18 Analyse Intraday Tick Structure

Analyses microstructure regimes, returns, volatility, activity and imbalance
by intraday session.

Input:
    E:/Quant_Lab/data/processed/tick_research/microstructure_regimes/GBPUSD_microstructure_regimes_latest.parquet

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/intraday_tick_structure_latest.csv
    E:/Quant_Lab/data/analysis/tick_research/intraday_tick_structure_latest.parquet
    E:/Quant_Lab/reports/tick_research/intraday_structure/intraday_tick_structure_report_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOL = "GBPUSD"

INPUT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "microstructure_regimes"
    / f"{SYMBOL}_microstructure_regimes_latest.parquet"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "intraday_structure"


def classify_session(hour_utc: int) -> str:
    """
    Broad FX session classification using UTC.

    These are deliberately broad v1 research buckets.
    """
    if 0 <= hour_utc < 7:
        return "asia_overnight"

    if 7 <= hour_utc < 12:
        return "london_morning"

    if 12 <= hour_utc < 16:
        return "london_new_york_overlap"

    if 16 <= hour_utc < 21:
        return "new_york_afternoon"

    return "late_us_rollover"


def add_session_fields(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    data["bar_start_time"] = pd.to_datetime(data["bar_start_time"], errors="coerce", utc=True)
    data["bar_end_time"] = pd.to_datetime(data["bar_end_time"], errors="coerce", utc=True)

    data = data.dropna(subset=["bar_start_time"]).copy()

    data["date_utc"] = data["bar_start_time"].dt.date.astype(str)
    data["hour_utc"] = data["bar_start_time"].dt.hour
    data["session_utc"] = data["hour_utc"].apply(classify_session)

    data["return"] = pd.to_numeric(data["return"], errors="coerce")
    data["abs_return"] = data["return"].abs()
    data["range"] = pd.to_numeric(data["range"], errors="coerce")
    data["duration_seconds"] = pd.to_numeric(data["duration_seconds"], errors="coerce")
    data["tick_count"] = pd.to_numeric(data["tick_count"], errors="coerce")

    if "imbalance_ratio" in data.columns:
        data["imbalance_ratio"] = pd.to_numeric(data["imbalance_ratio"], errors="coerce")
        data["abs_imbalance_ratio"] = data["imbalance_ratio"].abs()
    else:
        data["imbalance_ratio"] = np.nan
        data["abs_imbalance_ratio"] = np.nan

    return data


def build_session_summary(data: pd.DataFrame) -> pd.DataFrame:
    summary = (
        data.groupby(["bar_type", "bar_family", "session_utc"], dropna=False)
        .agg(
            bars=("bar_type", "count"),
            unique_days=("date_utc", "nunique"),
            avg_return=("return", "mean"),
            avg_abs_return=("abs_return", "mean"),
            return_std=("return", "std"),
            avg_range=("range", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            median_duration_seconds=("duration_seconds", "median"),
            avg_tick_count=("tick_count", "mean"),
            median_tick_count=("tick_count", "median"),
            avg_abs_imbalance_ratio=("abs_imbalance_ratio", "mean"),
            directional_imbalance_pct=(
                "microstructure_regime",
                lambda s: (s.astype(str).str.contains("directional_imbalance")).mean() * 100,
            ),
            volatility_expansion_pct=(
                "microstructure_regime",
                lambda s: (s.astype(str).str.contains("volatility_expansion")).mean() * 100,
            ),
            compressed_low_vol_pct=(
                "microstructure_regime",
                lambda s: (s.astype(str) == "compressed_low_vol").mean() * 100,
            ),
            normal_activity_pct=(
                "microstructure_regime",
                lambda s: (s.astype(str) == "normal_activity").mean() * 100,
            ),
        )
        .reset_index()
    )

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    session_order = {
        "asia_overnight": 1,
        "london_morning": 2,
        "london_new_york_overlap": 3,
        "new_york_afternoon": 4,
        "late_us_rollover": 5,
    }

    summary["session_order"] = summary["session_utc"].map(session_order).fillna(999)

    summary["analysis_time_utc"] = datetime.now(timezone.utc).isoformat()

    summary = summary.sort_values(["bar_type", "session_order"]).drop(columns=["session_order"])

    return summary.reset_index(drop=True)


def build_hourly_summary(data: pd.DataFrame) -> pd.DataFrame:
    summary = (
        data.groupby(["bar_type", "bar_family", "hour_utc"], dropna=False)
        .agg(
            bars=("bar_type", "count"),
            avg_abs_return=("abs_return", "mean"),
            return_std=("return", "std"),
            avg_range=("range", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            avg_tick_count=("tick_count", "mean"),
            directional_imbalance_pct=(
                "microstructure_regime",
                lambda s: (s.astype(str).str.contains("directional_imbalance")).mean() * 100,
            ),
            volatility_expansion_pct=(
                "microstructure_regime",
                lambda s: (s.astype(str).str.contains("volatility_expansion")).mean() * 100,
            ),
        )
        .reset_index()
    )

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["analysis_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary.sort_values(["bar_type", "hour_utc"]).reset_index(drop=True)


def build_report(session_summary: pd.DataFrame, hourly_summary: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    key_cols = [
        "bar_type",
        "session_utc",
        "bars",
        "unique_days",
        "avg_abs_return",
        "return_std",
        "avg_range",
        "avg_duration_seconds",
        "avg_tick_count",
        "directional_imbalance_pct",
        "volatility_expansion_pct",
        "compressed_low_vol_pct",
        "normal_activity_pct",
    ]

    available_key_cols = [col for col in key_cols if col in session_summary.columns]

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - INTRADAY TICK STRUCTURE REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Symbol:          {SYMBOL}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append("-" * 90)
    lines.append("")
    lines.append("SESSION SUMMARY")
    lines.append("-" * 90)
    lines.append(session_summary[available_key_cols].to_string(index=False))
    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("Sessions are broad UTC buckets for research diagnostics.")
    lines.append("London/New York overlap should often show stronger activity and volatility.")
    lines.append("Directional imbalance percentage is only meaningful for imbalance bar types.")
    lines.append("Fixed tick bars mainly show activity/volatility structure rather than directional pressure.")
    lines.append("This is diagnostic research, not a trading signal.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 18 ANALYSE INTRADAY TICK STRUCTURE")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Microstructure regimes file not found: {INPUT_PATH}")

    regimes = pd.read_parquet(INPUT_PATH)

    print(f"Rows loaded: {len(regimes):,}")

    data = add_session_fields(regimes)

    session_summary = build_session_summary(data)
    hourly_summary = build_hourly_summary(data)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    session_csv = OUTPUT_ANALYSIS_DIR / "intraday_tick_structure_latest.csv"
    session_parquet = OUTPUT_ANALYSIS_DIR / "intraday_tick_structure_latest.parquet"

    hourly_csv = OUTPUT_ANALYSIS_DIR / "intraday_tick_structure_hourly_latest.csv"
    hourly_parquet = OUTPUT_ANALYSIS_DIR / "intraday_tick_structure_hourly_latest.parquet"

    report_path = OUTPUT_REPORT_DIR / "intraday_tick_structure_report_latest.txt"

    session_summary.to_csv(session_csv, index=False)
    session_summary.to_parquet(session_parquet, index=False)

    hourly_summary.to_csv(hourly_csv, index=False)
    hourly_summary.to_parquet(hourly_parquet, index=False)

    report = build_report(session_summary, hourly_summary)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Intraday tick structure analysis created.")
    print(f"Session CSV:     {session_csv}")
    print(f"Session Parquet: {session_parquet}")
    print(f"Hourly CSV:      {hourly_csv}")
    print(f"Hourly Parquet:  {hourly_parquet}")
    print(f"Report:          {report_path}")
    print("-" * 90)

    display_cols = [
        "bar_type",
        "session_utc",
        "bars",
        "avg_abs_return",
        "return_std",
        "avg_range",
        "avg_duration_seconds",
        "avg_tick_count",
        "directional_imbalance_pct",
        "volatility_expansion_pct",
    ]

    available_display_cols = [col for col in display_cols if col in session_summary.columns]

    print(session_summary[available_display_cols].to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()