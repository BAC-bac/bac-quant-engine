"""
BACQE TICK RESEARCH - 20 Build Microstructure Feature Store - Multi Symbol

Builds reusable microstructure feature stores from labelled bar/regime data.

Inputs:
    E:/Quant_Lab/data/processed/tick_research/microstructure_regimes/symbol=<SYMBOL>/

Outputs:
    Per-symbol:
        E:/Quant_Lab/data/processed/tick_research/feature_store/symbol=<SYMBOL>/
        E:/Quant_Lab/data/analysis/tick_research/feature_store/symbol=<SYMBOL>/

    Master:
        E:/Quant_Lab/data/processed/tick_research/feature_store/_master/
        E:/Quant_Lab/data/analysis/tick_research/feature_store/_master/
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

OUTPUT_FEATURE_ROOT = (
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
    / "feature_store"
)

ROLLING_WINDOWS = [5, 10, 25, 50]
FORWARD_HORIZONS = [1, 2, 3, 5]


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


def prepare_base_features(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    data["bar_start_time"] = pd.to_datetime(
        data["bar_start_time"],
        errors="coerce",
        utc=True,
    )
    data["bar_end_time"] = pd.to_datetime(
        data["bar_end_time"],
        errors="coerce",
        utc=True,
    )

    data = data.dropna(subset=["bar_start_time"]).copy()

    if "symbol" not in data.columns:
        raise ValueError("Input data is missing required 'symbol' column.")

    data = data.sort_values(
        ["symbol", "bar_type", "bar_start_time"]
    ).reset_index(drop=True)

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "return",
        "log_return",
        "range",
        "avg_spread",
        "max_spread",
        "min_spread",
        "duration_seconds",
        "tick_count",
        "imbalance_ratio",
        "imbalance_sum",
        "imbalance_abs",
        "rolling_vol",
        "rolling_abs_return",
        "rolling_range",
        "rolling_duration",
        "rolling_tick_count",
        "abs_imbalance_ratio",
    ]

    for col in numeric_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    if "imbalance_ratio" not in data.columns:
        data["imbalance_ratio"] = np.nan

    if "imbalance_abs" not in data.columns:
        data["imbalance_abs"] = np.nan

    if "avg_spread" not in data.columns:
        data["avg_spread"] = np.nan

    data["return"] = pd.to_numeric(data["return"], errors="coerce")
    data["range"] = pd.to_numeric(data["range"], errors="coerce")
    data["close"] = pd.to_numeric(data["close"], errors="coerce")
    data["tick_count"] = pd.to_numeric(data["tick_count"], errors="coerce")
    data["duration_seconds"] = pd.to_numeric(data["duration_seconds"], errors="coerce")

    data["abs_return"] = data["return"].abs()
    data["squared_return"] = data["return"] ** 2
    data["range_pct"] = data["range"] / data["close"].replace(0, np.nan)
    data["spread_to_range"] = data["avg_spread"] / data["range"].replace(0, np.nan)
    data["ticks_per_second"] = data["tick_count"] / data["duration_seconds"].replace(0, np.nan)
    data["range_per_tick"] = data["range"] / data["tick_count"].replace(0, np.nan)
    data["volatility_per_tick"] = data["abs_return"] / data["tick_count"].replace(0, np.nan)

    data["hour_utc"] = data["bar_start_time"].dt.hour
    data["day_of_week"] = data["bar_start_time"].dt.dayofweek
    data["date_utc"] = data["bar_start_time"].dt.date.astype(str)
    data["session_utc"] = data["hour_utc"].apply(classify_session)

    data["is_london_morning"] = (data["session_utc"] == "london_morning").astype(int)
    data["is_london_ny_overlap"] = (data["session_utc"] == "london_new_york_overlap").astype(int)
    data["is_ny_afternoon"] = (data["session_utc"] == "new_york_afternoon").astype(int)
    data["is_rollover"] = (data["session_utc"] == "late_us_rollover").astype(int)

    data["direction"] = 0
    data.loc[data["return"] > 0, "direction"] = 1
    data.loc[data["return"] < 0, "direction"] = -1

    data["direction_is_up"] = (data["direction"] == 1).astype(int)
    data["direction_is_down"] = (data["direction"] == -1).astype(int)
    data["direction_is_flat"] = (data["direction"] == 0).astype(int)

    return data


def add_rolling_features(data: pd.DataFrame) -> pd.DataFrame:
    feature_store = data.copy()

    grouped = feature_store.groupby(["symbol", "bar_type"], group_keys=False)

    for window in ROLLING_WINDOWS:
        feature_store[f"rolling_return_mean_{window}"] = grouped["return"].transform(
            lambda s: s.rolling(window).mean()
        )
        feature_store[f"rolling_return_std_{window}"] = grouped["return"].transform(
            lambda s: s.rolling(window).std()
        )
        feature_store[f"rolling_abs_return_mean_{window}"] = grouped["abs_return"].transform(
            lambda s: s.rolling(window).mean()
        )
        feature_store[f"rolling_range_mean_{window}"] = grouped["range"].transform(
            lambda s: s.rolling(window).mean()
        )
        feature_store[f"rolling_duration_mean_{window}"] = grouped["duration_seconds"].transform(
            lambda s: s.rolling(window).mean()
        )
        feature_store[f"rolling_tick_count_mean_{window}"] = grouped["tick_count"].transform(
            lambda s: s.rolling(window).mean()
        )
        feature_store[f"rolling_direction_mean_{window}"] = grouped["direction"].transform(
            lambda s: s.rolling(window).mean()
        )
        feature_store[f"rolling_up_pct_{window}"] = grouped["direction_is_up"].transform(
            lambda s: s.rolling(window).mean()
        )
        feature_store[f"rolling_down_pct_{window}"] = grouped["direction_is_down"].transform(
            lambda s: s.rolling(window).mean()
        )
        feature_store[f"rolling_abs_imbalance_mean_{window}"] = grouped["imbalance_ratio"].transform(
            lambda s: s.abs().rolling(window).mean()
        )

    return feature_store


def add_forward_labels(data: pd.DataFrame) -> pd.DataFrame:
    labelled = data.copy()
    grouped = labelled.groupby(["symbol", "bar_type"], group_keys=False)

    for horizon in FORWARD_HORIZONS:
        labelled[f"future_return_h{horizon}"] = grouped["return"].shift(-horizon)
        labelled[f"future_abs_return_h{horizon}"] = labelled[f"future_return_h{horizon}"].abs()

        labelled[f"future_direction_h{horizon}"] = 0
        labelled.loc[labelled[f"future_return_h{horizon}"] > 0, f"future_direction_h{horizon}"] = 1
        labelled.loc[labelled[f"future_return_h{horizon}"] < 0, f"future_direction_h{horizon}"] = -1

        labelled[f"target_up_h{horizon}"] = (
            labelled[f"future_direction_h{horizon}"] == 1
        ).astype(int)

        labelled[f"target_down_h{horizon}"] = (
            labelled[f"future_direction_h{horizon}"] == -1
        ).astype(int)

        labelled[f"target_direction_persist_h{horizon}"] = (
            (labelled["direction"] != 0)
            & (labelled[f"future_direction_h{horizon}"] == labelled["direction"])
        ).astype(int)

        labelled[f"target_direction_flip_h{horizon}"] = (
            (labelled["direction"] != 0)
            & (labelled[f"future_direction_h{horizon}"] == -labelled["direction"])
        ).astype(int)

    return labelled


def build_summary(feature_store: pd.DataFrame) -> pd.DataFrame:
    summary = (
        feature_store.groupby(["symbol", "bar_type", "bar_family"], dropna=False)
        .agg(
            rows=("bar_type", "count"),
            first_bar_time=("bar_start_time", "min"),
            last_bar_time=("bar_start_time", "max"),
            avg_return=("return", "mean"),
            avg_abs_return=("abs_return", "mean"),
            return_std=("return", "std"),
            avg_range=("range", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            avg_tick_count=("tick_count", "mean"),
        )
        .reset_index()
    )

    feature_cols = [
        col for col in feature_store.columns
        if col.startswith("rolling_")
        or col.startswith("future_")
        or col.startswith("target_")
    ]

    null_rows = []

    for (symbol, bar_type), group in feature_store.groupby(
        ["symbol", "bar_type"],
        dropna=False,
    ):
        total_cells = len(group) * len(feature_cols)

        null_pct = (
            group[feature_cols].isna().sum().sum() / total_cells * 100
            if total_cells > 0
            else np.nan
        )

        null_rows.append(
            {
                "symbol": symbol,
                "bar_type": bar_type,
                "feature_null_pct": null_pct,
            }
        )

    null_df = pd.DataFrame(null_rows)

    summary = summary.merge(
        null_df,
        on=["symbol", "bar_type"],
        how="left",
    )

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary.sort_values(
        ["symbol", "rows"],
        ascending=[True, False],
    ).reset_index(drop=True)


def process_symbol(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    input_path = (
        INPUT_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_regimes_latest.parquet"
    )

    if not input_path.exists():
        print(f"[WARN] {symbol}: input file not found: {input_path}")
        return pd.DataFrame(), pd.DataFrame()

    regimes = pd.read_parquet(input_path)

    print(f"[INFO] {symbol}: rows loaded: {len(regimes):,}")

    base = prepare_base_features(regimes)
    features = add_rolling_features(base)
    feature_store = add_forward_labels(features)

    feature_store["feature_store_build_time_utc"] = datetime.now(timezone.utc).isoformat()

    summary = build_summary(feature_store)

    feature_dir = OUTPUT_FEATURE_ROOT / f"symbol={symbol}"
    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"

    feature_dir.mkdir(parents=True, exist_ok=True)
    analysis_dir.mkdir(parents=True, exist_ok=True)

    feature_parquet = feature_dir / f"{symbol}_microstructure_feature_store_latest.parquet"
    feature_csv = feature_dir / f"{symbol}_microstructure_feature_store_latest.csv"

    summary_csv = analysis_dir / f"{symbol}_microstructure_feature_store_summary_latest.csv"
    summary_parquet = analysis_dir / f"{symbol}_microstructure_feature_store_summary_latest.parquet"

    feature_store.to_parquet(feature_parquet, index=False)
    feature_store.to_csv(feature_csv, index=False)

    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    print(f"[DONE] {symbol}: feature parquet: {feature_parquet}")
    print(f"[DONE] {symbol}: feature CSV:     {feature_csv}")
    print(f"[DONE] {symbol}: summary CSV:     {summary_csv}")
    print(f"[INFO] {symbol}: feature rows:    {len(feature_store):,}")
    print(f"[INFO] {symbol}: feature columns: {len(feature_store.columns):,}")

    return feature_store, summary


def save_master_outputs(
    feature_stores: list[pd.DataFrame],
    summaries: list[pd.DataFrame],
) -> None:
    master_feature_dir = OUTPUT_FEATURE_ROOT / "_master"
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"

    master_feature_dir.mkdir(parents=True, exist_ok=True)
    master_analysis_dir.mkdir(parents=True, exist_ok=True)

    master_feature_store = pd.concat(feature_stores, ignore_index=True)
    master_summary = pd.concat(summaries, ignore_index=True)

    master_feature_parquet = master_feature_dir / "master_microstructure_feature_store_latest.parquet"
    master_feature_csv = master_feature_dir / "master_microstructure_feature_store_latest.csv"

    master_summary_csv = master_analysis_dir / "master_microstructure_feature_store_summary_latest.csv"
    master_summary_parquet = master_analysis_dir / "master_microstructure_feature_store_summary_latest.parquet"

    master_feature_store.to_parquet(master_feature_parquet, index=False)
    master_feature_store.to_csv(master_feature_csv, index=False)

    master_summary.to_csv(master_summary_csv, index=False)
    master_summary.to_parquet(master_summary_parquet, index=False)

    print("-" * 90)
    print("[DONE] Master microstructure feature store created.")
    print(f"Master feature parquet: {master_feature_parquet}")
    print(f"Master feature CSV:     {master_feature_csv}")
    print(f"Master summary CSV:     {master_summary_csv}")
    print(f"Master rows:            {len(master_feature_store):,}")
    print(f"Master columns:         {len(master_feature_store.columns):,}")


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 20 BUILD MICROSTRUCTURE FEATURE STORE - MULTI SYMBOL")
    print("=" * 90)
    print(f"Input root:           {INPUT_ROOT}")
    print(f"Output feature root:  {OUTPUT_FEATURE_ROOT}")
    print(f"Output analysis root: {OUTPUT_ANALYSIS_ROOT}")
    print(f"Symbols:              {SYMBOLS}")
    print("-" * 90)

    feature_stores = []
    summaries = []

    for symbol in SYMBOLS:
        feature_store, summary = process_symbol(symbol)

        if not feature_store.empty:
            feature_stores.append(feature_store)

        if not summary.empty:
            summaries.append(summary)

    if not feature_stores:
        print("[WARN] No feature stores created.")
        return

    save_master_outputs(feature_stores, summaries)

    display_cols = [
        "symbol",
        "bar_type",
        "bar_family",
        "rows",
        "avg_abs_return",
        "return_std",
        "avg_range",
        "avg_duration_seconds",
        "avg_tick_count",
        "feature_null_pct",
    ]

    master_summary = pd.concat(summaries, ignore_index=True)
    available_cols = [col for col in display_cols if col in master_summary.columns]

    print("-" * 90)
    print(master_summary[available_cols].to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()