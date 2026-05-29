"""
BACQE TICK RESEARCH - 18 Analyse Intraday Tick Structure - Multi Symbol

Analyses microstructure regimes, returns, volatility, activity and imbalance
by intraday session.

Inputs:
    E:/Quant_Lab/data/processed/tick_research/microstructure_regimes/symbol=<SYMBOL>/

Outputs:
    Per-symbol:
        E:/Quant_Lab/data/analysis/tick_research/intraday_structure/symbol=<SYMBOL>/
        E:/Quant_Lab/reports/tick_research/intraday_structure/symbol=<SYMBOL>/

    Master:
        E:/Quant_Lab/data/analysis/tick_research/intraday_structure/_master/
        E:/Quant_Lab/reports/tick_research/intraday_structure/_master/
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "EURGBP",
    "GBPJPY",
    "XAUUSD",
]

INPUT_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "microstructure_regimes"
)

OUTPUT_ANALYSIS_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "intraday_structure"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "intraday_structure"
)


def classify_session(hour_utc: int) -> str:
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
        data.groupby(["symbol", "bar_type", "bar_family", "session_utc"], dropna=False)
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

    return (
        summary.sort_values(["symbol", "bar_type", "session_order"])
        .drop(columns=["session_order"])
        .reset_index(drop=True)
    )


def build_hourly_summary(data: pd.DataFrame) -> pd.DataFrame:
    summary = (
        data.groupby(["symbol", "bar_type", "bar_family", "hour_utc"], dropna=False)
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

    return summary.sort_values(["symbol", "bar_type", "hour_utc"]).reset_index(drop=True)


def build_report(symbol: str, session_summary: pd.DataFrame, input_path: Path) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    key_cols = [
        "symbol",
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

    return "\n".join(
        [
            "=" * 90,
            f"BACQE TICK RESEARCH - INTRADAY TICK STRUCTURE REPORT - {symbol}",
            "=" * 90,
            f"Report time UTC: {now_utc}",
            f"Input:           {input_path}",
            "-" * 90,
            "",
            "SESSION SUMMARY",
            "-" * 90,
            session_summary[available_key_cols].to_string(index=False),
            "",
            "INTERPRETATION NOTES",
            "-" * 90,
            "Sessions are broad UTC buckets for research diagnostics.",
            "London/New York overlap should often show stronger activity and volatility.",
            "Directional imbalance percentage is mainly meaningful for imbalance bar types.",
            "This is diagnostic research, not a trading signal.",
            "=" * 90,
        ]
    )


def process_symbol(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    input_path = (
        INPUT_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_regimes_latest.parquet"
    )

    if not input_path.exists():
        print(f"[WARN] {symbol}: regime file not found: {input_path}")
        return pd.DataFrame(), pd.DataFrame()

    regimes = pd.read_parquet(input_path)
    print(f"[INFO] {symbol}: rows loaded: {len(regimes):,}")

    data = add_session_fields(regimes)

    session_summary = build_session_summary(data)
    hourly_summary = build_hourly_summary(data)

    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    session_csv = analysis_dir / f"{symbol}_intraday_tick_structure_session_latest.csv"
    session_parquet = analysis_dir / f"{symbol}_intraday_tick_structure_session_latest.parquet"

    hourly_csv = analysis_dir / f"{symbol}_intraday_tick_structure_hourly_latest.csv"
    hourly_parquet = analysis_dir / f"{symbol}_intraday_tick_structure_hourly_latest.parquet"

    report_path = report_dir / f"{symbol}_intraday_tick_structure_report_latest.txt"

    session_summary.to_csv(session_csv, index=False)
    session_summary.to_parquet(session_parquet, index=False)

    hourly_summary.to_csv(hourly_csv, index=False)
    hourly_summary.to_parquet(hourly_parquet, index=False)

    report = build_report(symbol, session_summary, input_path)
    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: session CSV: {session_csv}")
    print(f"[DONE] {symbol}: hourly CSV:  {hourly_csv}")
    print(f"[DONE] {symbol}: report:      {report_path}")

    return session_summary, hourly_summary


def save_master_outputs(
    all_sessions: list[pd.DataFrame],
    all_hourly: list[pd.DataFrame],
) -> None:
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_session = pd.concat(all_sessions, ignore_index=True)
    master_hourly = pd.concat(all_hourly, ignore_index=True)

    session_csv = master_analysis_dir / "master_intraday_tick_structure_session_latest.csv"
    session_parquet = master_analysis_dir / "master_intraday_tick_structure_session_latest.parquet"

    hourly_csv = master_analysis_dir / "master_intraday_tick_structure_hourly_latest.csv"
    hourly_parquet = master_analysis_dir / "master_intraday_tick_structure_hourly_latest.parquet"

    master_session.to_csv(session_csv, index=False)
    master_session.to_parquet(session_parquet, index=False)

    master_hourly.to_csv(hourly_csv, index=False)
    master_hourly.to_parquet(hourly_parquet, index=False)

    report_path = master_report_dir / "master_intraday_tick_structure_report_latest.txt"

    display_cols = [
        "symbol",
        "bar_type",
        "session_utc",
        "bars",
        "avg_abs_return",
        "return_std",
        "avg_range",
        "directional_imbalance_pct",
        "volatility_expansion_pct",
    ]

    available_cols = [col for col in display_cols if col in master_session.columns]

    report_path.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - MASTER INTRADAY STRUCTURE REPORT",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                "-" * 90,
                master_session[available_cols].to_string(index=False),
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    print("-" * 90)
    print("[DONE] Master intraday outputs created.")
    print(f"Master session CSV: {session_csv}")
    print(f"Master hourly CSV:  {hourly_csv}")
    print(f"Master report:      {report_path}")


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 18 ANALYSE INTRADAY TICK STRUCTURE - MULTI SYMBOL")
    print("=" * 90)
    print(f"Input root:           {INPUT_ROOT}")
    print(f"Output analysis root: {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output report root:   {OUTPUT_REPORT_ROOT}")
    print(f"Symbols:              {SYMBOLS}")
    print("-" * 90)

    all_sessions = []
    all_hourly = []

    for symbol in SYMBOLS:
        session_summary, hourly_summary = process_symbol(symbol)

        if not session_summary.empty:
            all_sessions.append(session_summary)

        if not hourly_summary.empty:
            all_hourly.append(hourly_summary)

    if not all_sessions or not all_hourly:
        print("[WARN] No intraday structure summaries created.")
        return

    save_master_outputs(all_sessions, all_hourly)

    print("-" * 90)
    print("[COMPLETE] Multi-symbol intraday tick structure analysis complete.")
    print(f"Symbols analysed: {len(all_sessions)}")
    print("=" * 90)


if __name__ == "__main__":
    main()