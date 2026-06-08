"""
BACQE DUKASCOPY 16 - DIAGNOSE REPLAY SIGNAL PRESSURE

Purpose:
    Diagnose why all threshold pairs in Script 15 produced the same trade count.

Focus:
    - buy_pressure distribution
    - sell_pressure distribution
    - threshold sensitivity
    - long/short overlap
    - realistic threshold candidates
"""

from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np


DATA_ROOT = Path(r"E:\Quant_Lab\data")

TIB_ROOT = DATA_ROOT / "processed" / "dukascopy_tick_imbalance_bars"
OUTPUT_ROOT = DATA_ROOT / "analysis" / "dukascopy_ticks" / "candidate_replay_diagnostics"

SYMBOL = "EURUSD"
START_DATE = "2024-01-01"
END_DATE = "2024-03-31"

IMBALANCE_THRESHOLDS = [25, 50, 100]

REQUIRED_WEEKDAYS = ["Friday", "Thursday", "Tuesday"]
REQUIRED_SESSIONS = [
    "asia_late_overnight",
    "london_mid_morning",
    "pre_new_york",
]

TEST_THRESHOLDS = [
    0.50,
    0.51,
    0.52,
    0.53,
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
]


def date_range(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def tib_path(symbol: str, dt: datetime, threshold: int) -> Path:
    return (
        TIB_ROOT
        / f"symbol={symbol}"
        / f"threshold={threshold}"
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"{symbol}_{dt.strftime('%Y-%m-%d')}_tib_threshold_{threshold}.parquet"
    )


def classify_session(timestamp) -> str:
    hour = pd.Timestamp(timestamp).hour

    if 0 <= hour <= 5:
        return "asia_late_overnight"
    if 8 <= hour <= 11:
        return "london_mid_morning"
    if 12 <= hour <= 13:
        return "pre_new_york"
    if 14 <= hour <= 16:
        return "new_york_open"
    if 17 <= hour <= 20:
        return "new_york_afternoon"
    if 21 <= hour <= 23:
        return "rollover_late"

    return "other"


def load_tibs() -> pd.DataFrame:
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    dfs = []

    for dt in date_range(start, end):
        for threshold in IMBALANCE_THRESHOLDS:
            path = tib_path(SYMBOL, dt, threshold)

            if not path.exists():
                continue

            df = pd.read_parquet(path)

            if df.empty:
                continue

            df = df.copy()
            df["date"] = dt.strftime("%Y-%m-%d")
            df["weekday"] = pd.to_datetime(df["timestamp_start"]).dt.day_name()
            df["session"] = df["timestamp_start"].apply(classify_session)

            df["buy_pressure"] = df["buy_ticks"] / df["tick_count"]
            df["sell_pressure"] = df["sell_ticks"] / df["tick_count"]
            df["dominant_pressure"] = df[["buy_pressure", "sell_pressure"]].max(axis=1)
            df["pressure_edge"] = (df["buy_pressure"] - df["sell_pressure"]).abs()

            dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def build_distribution_report(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for tib_threshold, group in df.groupby("imbalance_threshold"):
        for col in ["buy_pressure", "sell_pressure", "dominant_pressure", "pressure_edge"]:
            desc = group[col].describe(percentiles=[
                0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99
            ])

            row = {
                "imbalance_threshold": tib_threshold,
                "metric": col,
                "count": desc.get("count"),
                "mean": desc.get("mean"),
                "std": desc.get("std"),
                "min": desc.get("min"),
                "p01": desc.get("1%"),
                "p05": desc.get("5%"),
                "p10": desc.get("10%"),
                "p25": desc.get("25%"),
                "p50": desc.get("50%"),
                "p75": desc.get("75%"),
                "p90": desc.get("90%"),
                "p95": desc.get("95%"),
                "p99": desc.get("99%"),
                "max": desc.get("max"),
            }
            rows.append(row)

    return pd.DataFrame(rows)


def build_threshold_sensitivity_report(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for tib_threshold, group in df.groupby("imbalance_threshold"):
        for test_threshold in TEST_THRESHOLDS:
            long_mask = group["buy_pressure"] >= test_threshold
            short_mask = group["sell_pressure"] >= test_threshold
            either_mask = long_mask | short_mask
            both_mask = long_mask & short_mask

            rows.append({
                "imbalance_threshold": tib_threshold,
                "test_pressure_threshold": test_threshold,
                "total_rows": len(group),
                "long_signals": int(long_mask.sum()),
                "short_signals": int(short_mask.sum()),
                "either_signals": int(either_mask.sum()),
                "both_signals": int(both_mask.sum()),
                "either_signal_rate": float(either_mask.mean()) if len(group) else np.nan,
                "both_signal_rate": float(both_mask.mean()) if len(group) else np.nan,
            })

    return pd.DataFrame(rows)


def build_pair_sensitivity_report(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    pairs = [
        (0.51, 0.49),
        (0.52, 0.48),
        (0.53, 0.47),
        (0.55, 0.45),
        (0.60, 0.40),
        (0.60, 0.60),
        (0.65, 0.65),
        (0.70, 0.70),
        (0.75, 0.75),
        (0.80, 0.80),
    ]

    for tib_threshold, group in df.groupby("imbalance_threshold"):
        for buy_th, sell_th in pairs:
            long_mask = group["buy_pressure"] >= buy_th
            short_mask = group["sell_pressure"] >= sell_th
            either_mask = long_mask | short_mask
            both_mask = long_mask & short_mask

            rows.append({
                "imbalance_threshold": tib_threshold,
                "buy_threshold": buy_th,
                "sell_threshold": sell_th,
                "threshold_pair": f"{buy_th:.2f}_{sell_th:.2f}",
                "total_rows": len(group),
                "long_signals": int(long_mask.sum()),
                "short_signals": int(short_mask.sum()),
                "either_signals": int(either_mask.sum()),
                "both_signals": int(both_mask.sum()),
                "either_signal_rate": float(either_mask.mean()) if len(group) else np.nan,
                "both_signal_rate": float(both_mask.mean()) if len(group) else np.nan,
            })

    return pd.DataFrame(rows)


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 16 - DIAGNOSE REPLAY SIGNAL PRESSURE")
    print("=" * 90)

    df = load_tibs()

    if df.empty:
        print("[ERROR] No TIB data loaded.")
        return

    print(f"Loaded TIB rows: {len(df):,}")

    filtered = df[
        df["weekday"].isin(REQUIRED_WEEKDAYS)
        & df["session"].isin(REQUIRED_SESSIONS)
    ].copy()

    print(f"Filtered rows:   {len(filtered):,}")
    print("-" * 90)

    print("[FILTERED ROWS BY TIB THRESHOLD]")
    print(filtered["imbalance_threshold"].value_counts().sort_index().to_string())

    print("\n[BUY PRESSURE SUMMARY]")
    print(filtered["buy_pressure"].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9]).to_string())

    print("\n[SELL PRESSURE SUMMARY]")
    print(filtered["sell_pressure"].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9]).to_string())

    distribution_report = build_distribution_report(filtered)
    threshold_sensitivity = build_threshold_sensitivity_report(filtered)
    pair_sensitivity = build_pair_sensitivity_report(filtered)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    distribution_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_pressure_distribution.csv"
    threshold_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_threshold_sensitivity.csv"
    pair_path = OUTPUT_ROOT / f"{SYMBOL}_{START_DATE}_to_{END_DATE}_pair_sensitivity.csv"

    distribution_report.to_csv(distribution_path, index=False)
    threshold_sensitivity.to_csv(threshold_path, index=False)
    pair_sensitivity.to_csv(pair_path, index=False)

    print("\n[PAIR SENSITIVITY PREVIEW]")
    print(pair_sensitivity.head(30).to_string(index=False))

    print("-" * 90)
    print("[OUTPUTS]")
    print(f"Distribution:          {distribution_path}")
    print(f"Threshold sensitivity: {threshold_path}")
    print(f"Pair sensitivity:      {pair_path}")
    print("[DONE] Signal pressure diagnostics complete.")


if __name__ == "__main__":
    main()