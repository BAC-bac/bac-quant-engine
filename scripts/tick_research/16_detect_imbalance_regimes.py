"""
BACQE TICK RESEARCH - 16 Detect Imbalance Regimes

Creates simple microstructure regime labels from fixed tick bars and tick imbalance bars.

Outputs:
    E:/Quant_Lab/data/processed/tick_research/microstructure_regimes/GBPUSD_microstructure_regimes_latest.parquet
    E:/Quant_Lab/data/analysis/tick_research/microstructure_regime_summary_latest.csv
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOL = "GBPUSD"
BROKER = "FTMO"

TICK_BAR_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_bars" / f"symbol={SYMBOL}"
IMBALANCE_BAR_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_imbalance_bars" / f"symbol={SYMBOL}"

OUTPUT_REGIME_DIR = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "microstructure_regimes"
OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"

TICK_SIZES = [100, 250, 500, 1000]
IMBALANCE_THRESHOLDS = [25, 50]

ROLLING_WINDOW = 25


def load_tick_bars(tick_size: int) -> pd.DataFrame:
    path = TICK_BAR_ROOT / f"tick_size={tick_size}" / f"{SYMBOL}_tick_bars_{tick_size}_latest.parquet"
    bars = pd.read_parquet(path)
    bars["bar_family"] = "fixed_tick"
    bars["bar_type"] = f"tick_{tick_size}"
    bars["bar_parameter"] = str(tick_size)
    return bars


def load_imbalance_bars(threshold: int) -> pd.DataFrame:
    path = IMBALANCE_BAR_ROOT / f"imbalance_threshold={threshold}" / f"{SYMBOL}_tick_imbalance_bars_{threshold}_latest.parquet"
    bars = pd.read_parquet(path)
    bars["bar_family"] = "tick_imbalance"
    bars["bar_type"] = f"imbalance_{threshold}"
    bars["bar_parameter"] = str(threshold)
    return bars


def add_regime_features(bars: pd.DataFrame) -> pd.DataFrame:
    df = bars.copy()

    df["return"] = pd.to_numeric(df["return"], errors="coerce")
    df["abs_return"] = df["return"].abs()
    df["squared_return"] = df["return"] ** 2

    df["rolling_vol"] = df["return"].rolling(ROLLING_WINDOW).std()
    df["rolling_abs_return"] = df["abs_return"].rolling(ROLLING_WINDOW).mean()
    df["rolling_range"] = df["range"].rolling(ROLLING_WINDOW).mean()
    df["rolling_duration"] = df["duration_seconds"].rolling(ROLLING_WINDOW).mean()
    df["rolling_tick_count"] = df["tick_count"].rolling(ROLLING_WINDOW).mean()

    if "imbalance_ratio" not in df.columns:
        df["imbalance_ratio"] = np.nan

    df["abs_imbalance_ratio"] = pd.to_numeric(df["imbalance_ratio"], errors="coerce").abs()

    for col in [
        "rolling_vol",
        "rolling_abs_return",
        "rolling_range",
        "rolling_duration",
        "rolling_tick_count",
    ]:
        mean = df[col].mean()
        std = df[col].std()

        if pd.isna(std) or std == 0:
            df[f"{col}_z"] = 0.0
        else:
            df[f"{col}_z"] = (df[col] - mean) / std

    return df


def label_microstructure_regime(df: pd.DataFrame) -> pd.DataFrame:
    labelled = df.copy()

    labelled["microstructure_regime"] = "normal_activity"

    vol_z = labelled["rolling_vol_z"]
    range_z = labelled["rolling_range_z"]
    duration_z = labelled["rolling_duration_z"]
    tick_z = labelled["rolling_tick_count_z"]

    labelled.loc[
        (vol_z <= -0.75) & (range_z <= -0.75),
        "microstructure_regime",
    ] = "compressed_low_vol"

    labelled.loc[
        (vol_z >= 1.0) | (range_z >= 1.0),
        "microstructure_regime",
    ] = "volatility_expansion"

    labelled.loc[
        (duration_z >= 1.0) & (tick_z >= 1.0),
        "microstructure_regime",
    ] = "slow_heavy_activity"

    labelled.loc[
        (duration_z <= -0.75) & (tick_z <= -0.25),
        "microstructure_regime",
    ] = "fast_activity"

    if labelled["abs_imbalance_ratio"].notna().any():
        labelled.loc[
            labelled["abs_imbalance_ratio"] >= 0.08,
            "microstructure_regime",
        ] = "directional_imbalance"

        labelled.loc[
            (labelled["abs_imbalance_ratio"] >= 0.08)
            & ((vol_z >= 1.0) | (range_z >= 1.0)),
            "microstructure_regime",
        ] = "volatile_directional_imbalance"

    labelled["regime_build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return labelled


def summarise_regimes(regimes: pd.DataFrame) -> pd.DataFrame:
    summary = (
        regimes.groupby(["bar_type", "bar_family", "microstructure_regime"], dropna=False)
        .agg(
            bars=("microstructure_regime", "count"),
            avg_return=("return", "mean"),
            avg_abs_return=("abs_return", "mean"),
            avg_range=("range", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            avg_tick_count=("tick_count", "mean"),
        )
        .reset_index()
    )

    totals = regimes.groupby("bar_type")["microstructure_regime"].count().to_dict()

    summary["regime_pct"] = summary.apply(
        lambda row: (row["bars"] / totals.get(row["bar_type"], np.nan)) * 100,
        axis=1,
    )

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary.sort_values(["bar_type", "regime_pct"], ascending=[True, False])


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 16 DETECT IMBALANCE REGIMES")
    print("=" * 90)

    frames = []

    for tick_size in TICK_SIZES:
        bars = load_tick_bars(tick_size)
        features = add_regime_features(bars)
        labelled = label_microstructure_regime(features)
        frames.append(labelled)
        print(f"[DONE] Labelled tick bars: {tick_size} | bars={len(labelled):,}")

    for threshold in IMBALANCE_THRESHOLDS:
        bars = load_imbalance_bars(threshold)
        features = add_regime_features(bars)
        labelled = label_microstructure_regime(features)
        frames.append(labelled)
        print(f"[DONE] Labelled imbalance bars: {threshold} | bars={len(labelled):,}")

    regimes = pd.concat(frames, ignore_index=True)
    summary = summarise_regimes(regimes)

    OUTPUT_REGIME_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    regime_parquet = OUTPUT_REGIME_DIR / f"{SYMBOL}_microstructure_regimes_latest.parquet"
    regime_csv = OUTPUT_REGIME_DIR / f"{SYMBOL}_microstructure_regimes_latest.csv"
    summary_csv = OUTPUT_ANALYSIS_DIR / "microstructure_regime_summary_latest.csv"
    summary_parquet = OUTPUT_ANALYSIS_DIR / "microstructure_regime_summary_latest.parquet"

    regimes.to_parquet(regime_parquet, index=False)
    regimes.to_csv(regime_csv, index=False)

    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    print("-" * 90)
    print("[DONE] Microstructure regimes created.")
    print(f"Regime parquet:  {regime_parquet}")
    print(f"Regime CSV:      {regime_csv}")
    print(f"Summary CSV:     {summary_csv}")
    print(f"Summary parquet: {summary_parquet}")
    print("-" * 90)

    print(summary.to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()