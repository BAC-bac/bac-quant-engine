"""
BACQE TICK RESEARCH - 21 Validate Feature Store Quality - Multi Symbol

Validates the multi-symbol microstructure feature store before modelling.

Checks:
    - row/column counts
    - missing values
    - infinite values
    - target balance
    - feature/target separation
    - likely leakage columns
    - bar type sample sizes
    - symbol-level feature store health

Inputs:
    E:/Quant_Lab/data/processed/tick_research/feature_store/symbol=<SYMBOL>/

Outputs:
    Per-symbol:
        E:/Quant_Lab/data/analysis/tick_research/feature_store_quality/symbol=<SYMBOL>/
        E:/Quant_Lab/reports/tick_research/feature_store_quality/symbol=<SYMBOL>/

    Master:
        E:/Quant_Lab/data/analysis/tick_research/feature_store_quality/_master/
        E:/Quant_Lab/reports/tick_research/feature_store_quality/_master/
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
    / "feature_store"
)

OUTPUT_ANALYSIS_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "feature_store_quality"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "feature_store_quality"
)

MIN_HEALTHY_ROWS = 5_000


def classify_column(col: str) -> str:
    if col.startswith("target_"):
        return "target"

    if col.startswith("future_"):
        return "future_label"

    if col in {
        "bar_start_time",
        "bar_end_time",
        "date_utc",
        "feature_store_build_time_utc",
        "build_time_utc",
        "regime_build_time_utc",
        "summary_time_utc",
        "analysis_time_utc",
    }:
        return "metadata_time"

    if col in {
        "symbol",
        "broker",
        "bar_type",
        "bar_family",
        "bar_parameter",
        "microstructure_regime",
        "session_utc",
    }:
        return "categorical_feature"

    if col in {
        "open",
        "high",
        "low",
        "close",
        "return",
        "log_return",
        "direction",
    }:
        return "current_bar_feature"

    if col.startswith("rolling_"):
        return "rolling_feature"

    if col.startswith("is_") or col.startswith("direction_is_"):
        return "binary_feature"

    return "numeric_or_other_feature"


def build_column_quality(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    records = []
    total_rows = len(df)

    for col in df.columns:
        series = df[col]

        missing_count = int(series.isna().sum())
        missing_pct = (missing_count / total_rows) * 100 if total_rows else np.nan

        infinite_count = 0
        if pd.api.types.is_numeric_dtype(series):
            numeric_series = pd.to_numeric(series, errors="coerce")
            infinite_count = int(np.isinf(numeric_series).sum())

        unique_count = int(series.nunique(dropna=True))

        records.append(
            {
                "symbol": symbol,
                "column": col,
                "column_type": classify_column(col),
                "dtype": str(series.dtype),
                "missing_count": missing_count,
                "missing_pct": round(missing_pct, 6),
                "infinite_count": infinite_count,
                "unique_count": unique_count,
            }
        )

    quality = pd.DataFrame(records)

    return quality.sort_values(
        ["column_type", "missing_pct"],
        ascending=[True, False],
    ).reset_index(drop=True)


def build_target_balance(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    target_cols = [col for col in df.columns if col.startswith("target_")]
    records = []

    for col in target_cols:
        series = pd.to_numeric(df[col], errors="coerce")

        positive_count = int((series == 1).sum())
        negative_or_zero_count = int((series == 0).sum())
        rows = len(series)

        records.append(
            {
                "symbol": symbol,
                "target_column": col,
                "rows": rows,
                "missing_count": int(series.isna().sum()),
                "positive_count": positive_count,
                "negative_or_zero_count": negative_or_zero_count,
                "positive_pct": round((positive_count / rows) * 100, 6) if rows else np.nan,
            }
        )

    return pd.DataFrame(records)


def build_bar_type_summary(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    summary = (
        df.groupby(["symbol", "bar_type", "bar_family"], dropna=False)
        .agg(
            rows=("bar_type", "count"),
            first_bar_time=("bar_start_time", "min"),
            last_bar_time=("bar_start_time", "max"),
            target_up_h1_pct=("target_up_h1", "mean"),
            target_down_h1_pct=("target_down_h1", "mean"),
            target_direction_persist_h1_pct=("target_direction_persist_h1", "mean"),
            target_direction_flip_h1_pct=("target_direction_flip_h1", "mean"),
        )
        .reset_index()
    )

    pct_cols = [
        "target_up_h1_pct",
        "target_down_h1_pct",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
    ]

    for col in pct_cols:
        if col in summary.columns:
            summary[col] = (summary[col] * 100).round(6)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary.sort_values("rows", ascending=False).reset_index(drop=True)


def identify_likely_leakage_columns(df: pd.DataFrame) -> list[str]:
    leakage_keywords = [
        "future_",
        "target_",
        "next_",
    ]

    leakage_cols = []

    for col in df.columns:
        lower = col.lower()
        if any(keyword in lower for keyword in leakage_keywords):
            leakage_cols.append(col)

    return leakage_cols


def build_symbol_health(
    df: pd.DataFrame,
    symbol: str,
    column_quality: pd.DataFrame,
    target_balance: pd.DataFrame,
) -> pd.DataFrame:
    total_rows = len(df)
    total_cols = len(df.columns)
    total_cells = total_rows * total_cols

    missing_cells = int(df.isna().sum().sum())
    missing_pct = (missing_cells / total_cells) * 100 if total_cells else np.nan

    numeric_df = df.select_dtypes(include=[np.number])
    infinite_count = int(np.isinf(numeric_df).sum().sum()) if not numeric_df.empty else 0

    worst_missing_pct = (
        column_quality["missing_pct"].max()
        if not column_quality.empty
        else np.nan
    )

    target_positive_pcts = (
        pd.to_numeric(target_balance["positive_pct"], errors="coerce")
        if not target_balance.empty and "positive_pct" in target_balance.columns
        else pd.Series(dtype="float64")
    )

    avg_target_balance_distance = (
        (target_positive_pcts - 50).abs().mean()
        if not target_positive_pcts.empty
        else np.nan
    )

    missing_score = max(0, 100 - missing_pct) if not pd.isna(missing_pct) else 0
    infinite_score = 100 if infinite_count == 0 else 0

    target_balance_score = (
        max(0, 100 - (avg_target_balance_distance * 2))
        if not pd.isna(avg_target_balance_distance)
        else 50
    )

    sample_size_score = min(100, (total_rows / MIN_HEALTHY_ROWS) * 100)

    feature_store_health_score = round(
        missing_score * 0.40
        + infinite_score * 0.20
        + target_balance_score * 0.20
        + sample_size_score * 0.20,
        4,
    )

    if feature_store_health_score >= 90:
        health_label = "excellent"
    elif feature_store_health_score >= 75:
        health_label = "good"
    elif feature_store_health_score >= 60:
        health_label = "usable_with_caution"
    else:
        health_label = "needs_review"

    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "rows": total_rows,
                "columns": total_cols,
                "missing_cells": missing_cells,
                "missing_pct": round(missing_pct, 6),
                "worst_missing_pct": round(worst_missing_pct, 6)
                if not pd.isna(worst_missing_pct)
                else np.nan,
                "infinite_count": infinite_count,
                "avg_target_balance_distance": round(avg_target_balance_distance, 6)
                if not pd.isna(avg_target_balance_distance)
                else np.nan,
                "missing_score": round(missing_score, 4),
                "infinite_score": round(infinite_score, 4),
                "target_balance_score": round(target_balance_score, 4),
                "sample_size_score": round(sample_size_score, 4),
                "feature_store_health_score": feature_store_health_score,
                "health_label": health_label,
                "analysis_time_utc": datetime.now(timezone.utc).isoformat(),
            }
        ]
    )


def build_report(
    symbol: str,
    input_path: Path,
    df: pd.DataFrame,
    column_quality: pd.DataFrame,
    target_balance: pd.DataFrame,
    bar_summary: pd.DataFrame,
    leakage_cols: list[str],
    health: pd.DataFrame,
) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    total_rows = len(df)
    total_cols = len(df.columns)

    numeric_df = df.select_dtypes(include=[np.number])
    inf_count = int(np.isinf(numeric_df).sum().sum()) if not numeric_df.empty else 0

    missing_cells = int(df.isna().sum().sum())
    total_cells = total_rows * total_cols
    missing_pct = (missing_cells / total_cells) * 100 if total_cells else np.nan

    worst_missing = column_quality.sort_values(
        "missing_pct",
        ascending=False,
    ).head(20)

    lines = []
    lines.append("=" * 90)
    lines.append(f"BACQE TICK RESEARCH - FEATURE STORE QUALITY REPORT - {symbol}")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {input_path}")
    lines.append("-" * 90)
    lines.append(f"Rows:            {total_rows:,}")
    lines.append(f"Columns:         {total_cols:,}")
    lines.append(f"Missing cells:   {missing_cells:,}")
    lines.append(f"Missing pct:     {missing_pct:.6f}%")
    lines.append(f"Infinite values: {inf_count:,}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("SYMBOL HEALTH")
    lines.append("-" * 90)
    lines.append(health.to_string(index=False))

    lines.append("")
    lines.append("BAR TYPE SUMMARY")
    lines.append("-" * 90)
    lines.append(bar_summary.to_string(index=False))

    lines.append("")
    lines.append("TARGET BALANCE")
    lines.append("-" * 90)
    lines.append(target_balance.to_string(index=False))

    lines.append("")
    lines.append("LIKELY LEAKAGE / LABEL COLUMNS")
    lines.append("-" * 90)
    for col in leakage_cols:
        lines.append(col)

    lines.append("")
    lines.append("WORST MISSING COLUMNS")
    lines.append("-" * 90)
    lines.append(worst_missing.to_string(index=False))

    lines.append("")
    lines.append("MODELLING NOTES")
    lines.append("-" * 90)
    lines.append("Do not use future_* or target_* columns as model features.")
    lines.append("Use target_* columns only as labels.")
    lines.append("Use future_* columns for diagnostics, not as training features.")
    lines.append("Categorical columns need encoding before sklearn models.")
    lines.append("This validation step is required before baseline modelling.")
    lines.append("=" * 90)

    return "\n".join(lines)


def process_symbol(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    input_path = (
        INPUT_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_feature_store_latest.parquet"
    )

    if not input_path.exists():
        print(f"[WARN] {symbol}: feature store not found: {input_path}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df = pd.read_parquet(input_path)

    print(f"[INFO] {symbol}: rows loaded:    {len(df):,}")
    print(f"[INFO] {symbol}: columns loaded: {len(df.columns):,}")

    column_quality = build_column_quality(df, symbol)
    target_balance = build_target_balance(df, symbol)
    bar_summary = build_bar_type_summary(df, symbol)
    leakage_cols = identify_likely_leakage_columns(df)
    health = build_symbol_health(df, symbol, column_quality, target_balance)

    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    column_quality_csv = analysis_dir / f"{symbol}_feature_store_column_quality_latest.csv"
    target_balance_csv = analysis_dir / f"{symbol}_feature_store_target_balance_latest.csv"
    bar_summary_csv = analysis_dir / f"{symbol}_feature_store_bar_type_summary_latest.csv"
    health_csv = analysis_dir / f"{symbol}_feature_store_health_latest.csv"
    report_path = report_dir / f"{symbol}_feature_store_quality_report_latest.txt"

    column_quality.to_csv(column_quality_csv, index=False)
    target_balance.to_csv(target_balance_csv, index=False)
    bar_summary.to_csv(bar_summary_csv, index=False)
    health.to_csv(health_csv, index=False)

    report = build_report(
        symbol=symbol,
        input_path=input_path,
        df=df,
        column_quality=column_quality,
        target_balance=target_balance,
        bar_summary=bar_summary,
        leakage_cols=leakage_cols,
        health=health,
    )

    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: column quality: {column_quality_csv}")
    print(f"[DONE] {symbol}: target balance: {target_balance_csv}")
    print(f"[DONE] {symbol}: bar summary:    {bar_summary_csv}")
    print(f"[DONE] {symbol}: health:         {health_csv}")
    print(f"[DONE] {symbol}: report:         {report_path}")

    return column_quality, target_balance, bar_summary, health


def save_master_outputs(
    column_quality_frames: list[pd.DataFrame],
    target_balance_frames: list[pd.DataFrame],
    bar_summary_frames: list[pd.DataFrame],
    health_frames: list[pd.DataFrame],
) -> None:
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_column_quality = pd.concat(column_quality_frames, ignore_index=True)
    master_target_balance = pd.concat(target_balance_frames, ignore_index=True)
    master_bar_summary = pd.concat(bar_summary_frames, ignore_index=True)
    master_health = pd.concat(health_frames, ignore_index=True)

    column_quality_csv = master_analysis_dir / "master_feature_store_column_quality_latest.csv"
    target_balance_csv = master_analysis_dir / "master_feature_store_target_balance_latest.csv"
    bar_summary_csv = master_analysis_dir / "master_feature_store_bar_type_summary_latest.csv"
    health_csv = master_analysis_dir / "master_feature_store_health_latest.csv"

    master_column_quality.to_csv(column_quality_csv, index=False)
    master_target_balance.to_csv(target_balance_csv, index=False)
    master_bar_summary.to_csv(bar_summary_csv, index=False)
    master_health.to_csv(health_csv, index=False)

    report_path = master_report_dir / "master_feature_store_quality_report_latest.txt"

    worst_missing = master_column_quality.sort_values(
        "missing_pct",
        ascending=False,
    ).head(30)

    report_path.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - MASTER FEATURE STORE QUALITY REPORT",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                "-" * 90,
                "",
                "SYMBOL HEALTH",
                "-" * 90,
                master_health.sort_values(
                    "feature_store_health_score",
                    ascending=False,
                ).to_string(index=False),
                "",
                "MASTER BAR TYPE SUMMARY",
                "-" * 90,
                master_bar_summary.to_string(index=False),
                "",
                "WORST MISSING COLUMNS",
                "-" * 90,
                worst_missing.to_string(index=False),
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    print("-" * 90)
    print("[DONE] Master feature store quality validation created.")
    print(f"Master column quality: {column_quality_csv}")
    print(f"Master target balance: {target_balance_csv}")
    print(f"Master bar summary:    {bar_summary_csv}")
    print(f"Master health:         {health_csv}")
    print(f"Master report:         {report_path}")

    print("-" * 90)
    print("SYMBOL HEALTH")
    print(master_health.to_string(index=False))


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 21 VALIDATE FEATURE STORE QUALITY - MULTI SYMBOL")
    print("=" * 90)
    print(f"Input root:           {INPUT_ROOT}")
    print(f"Output analysis root: {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output report root:   {OUTPUT_REPORT_ROOT}")
    print(f"Symbols:              {SYMBOLS}")
    print("-" * 90)

    column_quality_frames = []
    target_balance_frames = []
    bar_summary_frames = []
    health_frames = []

    for symbol in SYMBOLS:
        column_quality, target_balance, bar_summary, health = process_symbol(symbol)

        if not column_quality.empty:
            column_quality_frames.append(column_quality)

        if not target_balance.empty:
            target_balance_frames.append(target_balance)

        if not bar_summary.empty:
            bar_summary_frames.append(bar_summary)

        if not health.empty:
            health_frames.append(health)

    if not health_frames:
        print("[WARN] No feature store quality outputs created.")
        return

    save_master_outputs(
        column_quality_frames=column_quality_frames,
        target_balance_frames=target_balance_frames,
        bar_summary_frames=bar_summary_frames,
        health_frames=health_frames,
    )

    print("-" * 90)
    print("[COMPLETE] Multi-symbol feature store quality validation complete.")
    print(f"Symbols analysed: {len(health_frames)}")
    print("=" * 90)


if __name__ == "__main__":
    main()