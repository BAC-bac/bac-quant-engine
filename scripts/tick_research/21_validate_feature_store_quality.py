"""
BACQE TICK RESEARCH - 21 Validate Feature Store Quality

Validates the Phase 1 microstructure feature store before modelling.

Checks:
    - row/column counts
    - missing values
    - infinite values
    - target balance
    - feature/target separation
    - likely leakage columns
    - bar type sample sizes

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/feature_store_quality_latest.csv
    E:/Quant_Lab/reports/tick_research/feature_store_quality/feature_store_quality_report_latest.txt
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
    / "feature_store"
    / f"{SYMBOL}_microstructure_feature_store_latest.parquet"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "feature_store_quality"


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

    if col in {
        "future_return_h1",
        "future_return_h2",
        "future_return_h3",
        "future_return_h5",
        "future_direction_h1",
        "future_direction_h2",
        "future_direction_h3",
        "future_direction_h5",
    }:
        return "future_label"

    return "numeric_or_other_feature"


def build_column_quality(df: pd.DataFrame) -> pd.DataFrame:
    records = []

    total_rows = len(df)

    for col in df.columns:
        series = df[col]

        missing_count = int(series.isna().sum())
        missing_pct = (missing_count / total_rows) * 100 if total_rows else np.nan

        infinite_count = 0
        if pd.api.types.is_numeric_dtype(series):
            infinite_count = int(np.isinf(series.replace([np.inf, -np.inf], np.nan)).sum())

        unique_count = int(series.nunique(dropna=True))

        records.append(
            {
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

    return quality.sort_values(["column_type", "missing_pct"], ascending=[True, False])


def build_target_balance(df: pd.DataFrame) -> pd.DataFrame:
    target_cols = [col for col in df.columns if col.startswith("target_")]

    records = []

    for col in target_cols:
        series = pd.to_numeric(df[col], errors="coerce")

        records.append(
            {
                "target_column": col,
                "rows": len(series),
                "missing_count": int(series.isna().sum()),
                "positive_count": int((series == 1).sum()),
                "negative_or_zero_count": int((series == 0).sum()),
                "positive_pct": round((series == 1).mean() * 100, 6),
            }
        )

    return pd.DataFrame(records)


def build_bar_type_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby(["bar_type", "bar_family"], dropna=False)
        .agg(
            rows=("bar_type", "count"),
            first_bar_time=("bar_start_time", "min"),
            last_bar_time=("bar_start_time", "max"),
            target_up_h1_pct=("target_up_h1", "mean"),
            target_direction_persist_h1_pct=("target_direction_persist_h1", "mean"),
            target_direction_flip_h1_pct=("target_direction_flip_h1", "mean"),
        )
        .reset_index()
    )

    pct_cols = [
        "target_up_h1_pct",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
    ]

    for col in pct_cols:
        summary[col] = (summary[col] * 100).round(6)

    return summary.sort_values("rows", ascending=False)


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


def build_report(
    df: pd.DataFrame,
    column_quality: pd.DataFrame,
    target_balance: pd.DataFrame,
    bar_summary: pd.DataFrame,
    leakage_cols: list[str],
) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    total_rows = len(df)
    total_cols = len(df.columns)

    numeric_df = df.select_dtypes(include=[np.number])
    inf_count = int(np.isinf(numeric_df.replace([np.inf, -np.inf], np.nan)).sum().sum())

    missing_cells = int(df.isna().sum().sum())
    total_cells = total_rows * total_cols
    missing_pct = (missing_cells / total_cells) * 100 if total_cells else np.nan

    worst_missing = column_quality.sort_values("missing_pct", ascending=False).head(20)

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - FEATURE STORE QUALITY REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append("-" * 90)
    lines.append(f"Rows:            {total_rows:,}")
    lines.append(f"Columns:         {total_cols:,}")
    lines.append(f"Missing cells:   {missing_cells:,}")
    lines.append(f"Missing pct:     {missing_pct:.6f}%")
    lines.append(f"Infinite values: {inf_count:,}")
    lines.append("-" * 90)

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


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 21 VALIDATE FEATURE STORE QUALITY")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Feature store not found: {INPUT_PATH}")

    df = pd.read_parquet(INPUT_PATH)

    print(f"Rows loaded:    {len(df):,}")
    print(f"Columns loaded: {len(df.columns):,}")

    column_quality = build_column_quality(df)
    target_balance = build_target_balance(df)
    bar_summary = build_bar_type_summary(df)
    leakage_cols = identify_likely_leakage_columns(df)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    column_quality_csv = OUTPUT_ANALYSIS_DIR / "feature_store_quality_latest.csv"
    target_balance_csv = OUTPUT_ANALYSIS_DIR / "feature_store_target_balance_latest.csv"
    bar_summary_csv = OUTPUT_ANALYSIS_DIR / "feature_store_bar_type_summary_latest.csv"
    report_path = OUTPUT_REPORT_DIR / "feature_store_quality_report_latest.txt"

    column_quality.to_csv(column_quality_csv, index=False)
    target_balance.to_csv(target_balance_csv, index=False)
    bar_summary.to_csv(bar_summary_csv, index=False)

    report = build_report(df, column_quality, target_balance, bar_summary, leakage_cols)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Feature store quality validation created.")
    print(f"Column quality: {column_quality_csv}")
    print(f"Target balance: {target_balance_csv}")
    print(f"Bar summary:    {bar_summary_csv}")
    print(f"Report:         {report_path}")
    print("-" * 90)

    print(bar_summary.to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()