"""
BACQE TICK RESEARCH - 16 Detect Imbalance Regimes - Multi Symbol

Creates simple microstructure regime labels from fixed tick bars and tick imbalance bars.

Outputs:
    Per-symbol:
        E:/Quant_Lab/data/processed/tick_research/microstructure_regimes/symbol=<SYMBOL>/
        E:/Quant_Lab/data/analysis/tick_research/microstructure_regimes/symbol=<SYMBOL>/

    Master:
        E:/Quant_Lab/data/analysis/tick_research/microstructure_regimes/_master/
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

BROKER = "FTMO"

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "EURGBP",
    "GBPJPY",
    "XAUUSD",
]

TICK_SIZES = [100, 250, 500, 1000]
IMBALANCE_THRESHOLDS = [25, 50, 100, 200]

ROLLING_WINDOW = 25

TICK_BAR_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_bars"
IMBALANCE_BAR_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_imbalance_bars"

OUTPUT_REGIME_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "microstructure_regimes"
OUTPUT_ANALYSIS_ROOT = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research" / "microstructure_regimes"

BAR_ORDER = {
    "tick_100": 1,
    "tick_250": 2,
    "tick_500": 3,
    "tick_1000": 4,
    "imbalance_25": 5,
    "imbalance_50": 6,
    "imbalance_100": 7,
    "imbalance_200": 8,
}


def normalise_bar_columns(bars: pd.DataFrame) -> pd.DataFrame:
    bars = bars.copy()

    if "bar_start_time" not in bars.columns and "start_time" in bars.columns:
        bars["bar_start_time"] = bars["start_time"]

    if "bar_end_time" not in bars.columns and "end_time" in bars.columns:
        bars["bar_end_time"] = bars["end_time"]

    if "open" not in bars.columns and "open_mid" in bars.columns:
        bars["open"] = bars["open_mid"]

    if "high" not in bars.columns and "high_mid" in bars.columns:
        bars["high"] = bars["high_mid"]

    if "low" not in bars.columns and "low_mid" in bars.columns:
        bars["low"] = bars["low_mid"]

    if "close" not in bars.columns and "close_mid" in bars.columns:
        bars["close"] = bars["close_mid"]

    if "avg_spread" not in bars.columns and "mean_spread" in bars.columns:
        bars["avg_spread"] = bars["mean_spread"]

    if "range" not in bars.columns and {"high", "low"}.issubset(bars.columns):
        bars["range"] = bars["high"] - bars["low"]

    if "return" not in bars.columns and "close" in bars.columns:
        bars["return"] = bars["close"].pct_change()

    if "log_return" not in bars.columns and "close" in bars.columns:
        bars["log_return"] = np.log(bars["close"] / bars["close"].shift(1))

    if "duration_seconds" not in bars.columns:
        if {"bar_start_time", "bar_end_time"}.issubset(bars.columns):
            start = pd.to_datetime(bars["bar_start_time"], errors="coerce", utc=True)
            end = pd.to_datetime(bars["bar_end_time"], errors="coerce", utc=True)
            bars["duration_seconds"] = (end - start).dt.total_seconds()

    return bars


def load_tick_bars(symbol: str, tick_size: int) -> pd.DataFrame:
    path = (
        TICK_BAR_ROOT
        / f"symbol={symbol}"
        / f"tick_size={tick_size}"
        / f"{symbol}_tick_bars_{tick_size}_latest.parquet"
    )

    if not path.exists():
        print(f"[WARN] {symbol}: tick bar file not found: {path}")
        return pd.DataFrame()

    bars = pd.read_parquet(path)

    bars["symbol"] = symbol
    bars["broker"] = BROKER
    bars["bar_family"] = "fixed_tick"
    bars["bar_type"] = f"tick_{tick_size}"
    bars["bar_parameter"] = str(tick_size)

    return normalise_bar_columns(bars)


def load_imbalance_bars(symbol: str, threshold: int) -> pd.DataFrame:
    path = (
        IMBALANCE_BAR_ROOT
        / f"symbol={symbol}"
        / f"imbalance_threshold={threshold}"
        / f"{symbol}_tick_imbalance_bars_{threshold}_latest.parquet"
    )

    if not path.exists():
        print(f"[WARN] {symbol}: imbalance bar file not found: {path}")
        return pd.DataFrame()

    bars = pd.read_parquet(path)

    bars["symbol"] = symbol
    bars["broker"] = BROKER
    bars["bar_family"] = "tick_imbalance"
    bars["bar_type"] = f"imbalance_{threshold}"
    bars["bar_parameter"] = str(threshold)

    return normalise_bar_columns(bars)


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

    df["abs_imbalance_ratio"] = pd.to_numeric(
        df["imbalance_ratio"],
        errors="coerce",
    ).abs()

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
        regimes.groupby(
            ["symbol", "bar_type", "bar_family", "microstructure_regime"],
            dropna=False,
        )
        .agg(
            bars=("microstructure_regime", "count"),
            avg_return=("return", "mean"),
            avg_abs_return=("abs_return", "mean"),
            avg_range=("range", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            avg_tick_count=("tick_count", "mean"),
            avg_rolling_vol=("rolling_vol", "mean"),
            avg_abs_imbalance_ratio=("abs_imbalance_ratio", "mean"),
        )
        .reset_index()
    )

    totals = (
        regimes.groupby(["symbol", "bar_type"])["microstructure_regime"]
        .count()
        .to_dict()
    )

    summary["regime_pct"] = summary.apply(
        lambda row: (
            row["bars"] / totals.get((row["symbol"], row["bar_type"]), np.nan)
        )
        * 100,
        axis=1,
    )

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()
    summary["sort_order"] = summary["bar_type"].map(BAR_ORDER).fillna(999)

    summary = summary.sort_values(
        ["symbol", "sort_order", "regime_pct"],
        ascending=[True, True, False],
    ).drop(columns=["sort_order"]).reset_index(drop=True)

    return summary


def build_symbol_regimes(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    frames = []

    for tick_size in TICK_SIZES:
        bars = load_tick_bars(symbol, tick_size)

        if bars.empty:
            continue

        features = add_regime_features(bars)
        labelled = label_microstructure_regime(features)
        frames.append(labelled)

        print(f"[DONE] {symbol}: labelled tick bars {tick_size} | bars={len(labelled):,}")

    for threshold in IMBALANCE_THRESHOLDS:
        bars = load_imbalance_bars(symbol, threshold)

        if bars.empty:
            continue

        features = add_regime_features(bars)
        labelled = label_microstructure_regime(features)
        frames.append(labelled)

        print(f"[DONE] {symbol}: labelled imbalance bars {threshold} | bars={len(labelled):,}")

    if not frames:
        print(f"[WARN] {symbol}: no regime frames created.")
        return pd.DataFrame(), pd.DataFrame()

    regimes = pd.concat(frames, ignore_index=True)
    summary = summarise_regimes(regimes)

    regime_dir = OUTPUT_REGIME_ROOT / f"symbol={symbol}"
    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"

    regime_dir.mkdir(parents=True, exist_ok=True)
    analysis_dir.mkdir(parents=True, exist_ok=True)

    regime_parquet = regime_dir / f"{symbol}_microstructure_regimes_latest.parquet"
    regime_csv = regime_dir / f"{symbol}_microstructure_regimes_latest.csv"
    summary_csv = analysis_dir / f"{symbol}_microstructure_regime_summary_latest.csv"
    summary_parquet = analysis_dir / f"{symbol}_microstructure_regime_summary_latest.parquet"

    regimes.to_parquet(regime_parquet, index=False)
    regimes.to_csv(regime_csv, index=False)

    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    print(f"[DONE] {symbol}: regime parquet: {regime_parquet}")
    print(f"[DONE] {symbol}: regime CSV:     {regime_csv}")
    print(f"[DONE] {symbol}: summary CSV:    {summary_csv}")

    return regimes, summary


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 16 DETECT IMBALANCE REGIMES - MULTI SYMBOL")
    print("=" * 90)
    print(f"Broker:              {BROKER}")
    print(f"Tick bar root:        {TICK_BAR_ROOT}")
    print(f"Imbalance bar root:   {IMBALANCE_BAR_ROOT}")
    print(f"Output regime root:   {OUTPUT_REGIME_ROOT}")
    print(f"Output analysis root: {OUTPUT_ANALYSIS_ROOT}")
    print(f"Symbols:              {SYMBOLS}")
    print("-" * 90)

    all_regimes = []
    all_summaries = []

    for symbol in SYMBOLS:
        regimes, summary = build_symbol_regimes(symbol)

        if not regimes.empty:
            all_regimes.append(regimes)

        if not summary.empty:
            all_summaries.append(summary)

    if not all_regimes:
        print("[WARN] No microstructure regimes created.")
        return

    master_regimes = pd.concat(all_regimes, ignore_index=True)
    master_summary = pd.concat(all_summaries, ignore_index=True)

    master_regime_dir = OUTPUT_REGIME_ROOT / "_master"
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"

    master_regime_dir.mkdir(parents=True, exist_ok=True)
    master_analysis_dir.mkdir(parents=True, exist_ok=True)

    master_regime_parquet = master_regime_dir / "master_microstructure_regimes_latest.parquet"
    master_regime_csv = master_regime_dir / "master_microstructure_regimes_latest.csv"
    master_summary_csv = master_analysis_dir / "master_microstructure_regime_summary_latest.csv"
    master_summary_parquet = master_analysis_dir / "master_microstructure_regime_summary_latest.parquet"

    master_regimes.to_parquet(master_regime_parquet, index=False)
    master_regimes.to_csv(master_regime_csv, index=False)

    master_summary.to_csv(master_summary_csv, index=False)
    master_summary.to_parquet(master_summary_parquet, index=False)

    print("-" * 90)
    print("[DONE] Multi-symbol microstructure regimes created.")
    print(f"Master regime parquet:  {master_regime_parquet}")
    print(f"Master regime CSV:      {master_regime_csv}")
    print(f"Master summary CSV:     {master_summary_csv}")
    print(f"Master summary parquet: {master_summary_parquet}")
    print("-" * 90)

    display_cols = [
        "symbol",
        "bar_type",
        "bar_family",
        "microstructure_regime",
        "bars",
        "regime_pct",
        "avg_abs_return",
        "avg_range",
        "avg_duration_seconds",
        "avg_tick_count",
    ]

    available_cols = [col for col in display_cols if col in master_summary.columns]

    print(master_summary[available_cols].to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()