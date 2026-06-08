"""
BACQE DUKASCOPY 11 - AUDIT MONTH OUTPUTS

Purpose:
    Audit January 2024 EURUSD Dukascopy outputs across:
        - processed daily ticks
        - fixed tick bars
        - tick imbalance bars

This script verifies:
    - expected daily files exist
    - row counts are readable
    - missing days are recorded
    - one audit CSV is saved
"""

from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd


DATA_ROOT = Path(r"E:\Quant_Lab\data")

TICK_ROOT = DATA_ROOT / "processed" / "dukascopy_ticks"
TICK_BAR_ROOT = DATA_ROOT / "processed" / "dukascopy_tick_bars"
TIB_ROOT = DATA_ROOT / "processed" / "dukascopy_tick_imbalance_bars"
REPORT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "month_audits"

SYMBOL = "EURUSD"
START_DATE = "2024-01-01"
END_DATE = "2024-03-31"

TICK_SIZES = [100, 250, 500, 1000]
IMBALANCE_THRESHOLDS = [25, 50, 100]


def date_range(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def processed_tick_path(symbol: str, dt: datetime) -> Path:
    return (
        TICK_ROOT
        / f"symbol={symbol}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_ticks.parquet"
    )


def tick_bar_path(symbol: str, dt: datetime, tick_size: int) -> Path:
    return (
        TICK_BAR_ROOT
        / f"symbol={symbol}"
        / f"tick_size={tick_size}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_tick_bars_{tick_size}.parquet"
    )


def tib_path(symbol: str, dt: datetime, threshold: int) -> Path:
    return (
        TIB_ROOT
        / f"symbol={symbol}"
        / f"threshold={threshold}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_tib_threshold_{threshold}.parquet"
    )


def safe_parquet_row_count(path: Path) -> tuple[str, int, str]:
    """
    Safely read a parquet file and return status, rows, error.
    """

    if not path.exists():
        return "missing", 0, ""

    try:
        df = pd.read_parquet(path)
        return "ok", len(df), ""
    except Exception as exc:
        return "read_error", 0, repr(exc)


def audit_day(dt: datetime) -> list[dict]:
    rows = []

    tick_path = processed_tick_path(SYMBOL, dt)
    status, count, error = safe_parquet_row_count(tick_path)

    rows.append({
        "date": dt.strftime("%Y-%m-%d"),
        "dataset_type": "ticks",
        "variant": "daily_ticks",
        "status": status,
        "rows": count,
        "path": str(tick_path),
        "error": error,
    })

    for tick_size in TICK_SIZES:
        path = tick_bar_path(SYMBOL, dt, tick_size)
        status, count, error = safe_parquet_row_count(path)

        rows.append({
            "date": dt.strftime("%Y-%m-%d"),
            "dataset_type": "fixed_tick_bars",
            "variant": f"tick_size={tick_size}",
            "status": status,
            "rows": count,
            "path": str(path),
            "error": error,
        })

    for threshold in IMBALANCE_THRESHOLDS:
        path = tib_path(SYMBOL, dt, threshold)
        status, count, error = safe_parquet_row_count(path)

        rows.append({
            "date": dt.strftime("%Y-%m-%d"),
            "dataset_type": "tick_imbalance_bars",
            "variant": f"threshold={threshold}",
            "status": status,
            "rows": count,
            "path": str(path),
            "error": error,
        })

    return rows


def main() -> None:
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    print("=" * 90)
    print("BACQE DUKASCOPY 11 - AUDIT MONTH OUTPUTS")
    print("=" * 90)
    print(f"Symbol:     {SYMBOL}")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print("-" * 90)

    rows = []

    for dt in date_range(start, end):
        day_rows = audit_day(dt)
        rows.extend(day_rows)

        day_df = pd.DataFrame(day_rows)

        tick_status = day_df.loc[
            day_df["dataset_type"] == "ticks", "status"
        ].iloc[0]

        ok_count = (day_df["status"] == "ok").sum()
        missing_count = (day_df["status"] == "missing").sum()
        error_count = (day_df["status"] == "read_error").sum()

        print(
            f"[{dt.strftime('%Y-%m-%d')}] "
            f"tick_status={tick_status:<10} | "
            f"ok={ok_count:>2} | "
            f"missing={missing_count:>2} | "
            f"errors={error_count:>2}"
        )

    audit_df = pd.DataFrame(rows)

    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    audit_path = (
        REPORT_ROOT
        / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_month_output_audit.csv"
    )

    audit_df.to_csv(audit_path, index=False)

    print("-" * 90)
    print("[SUMMARY BY DATASET TYPE]")
    summary = (
        audit_df
        .groupby(["dataset_type", "status"])
        .agg(
            files=("status", "size"),
            rows=("rows", "sum"),
        )
        .reset_index()
    )
    print(summary.to_string(index=False))

    print("-" * 90)
    print("[SUMMARY BY VARIANT]")
    variant_summary = (
        audit_df
        .groupby(["dataset_type", "variant", "status"])
        .agg(
            files=("status", "size"),
            rows=("rows", "sum"),
        )
        .reset_index()
    )
    print(variant_summary.to_string(index=False))

    print("-" * 90)
    print(f"Audit report: {audit_path}")
    print("[DONE] Dukascopy month output audit complete.")


if __name__ == "__main__":
    main()