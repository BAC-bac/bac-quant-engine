"""
BACQE TICK RESEARCH - 20 Build Microstructure Feature Store

Builds a reusable microstructure feature store from labelled bar/regime data.

Input:
    E:/Quant_Lab/data/processed/tick_research/microstructure_regimes/GBPUSD_microstructure_regimes_latest.parquet

Outputs:
    E:/Quant_Lab/data/processed/tick_research/feature_store/GBPUSD_microstructure_feature_store_latest.parquet
    E:/Quant_Lab/data/analysis/tick_research/microstructure_feature_store_summary_latest.csv
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

OUTPUT_FEATURE_DIR = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "feature_store"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"

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

    data["bar_start_time"] = pd.to_datetime(data["bar_start_time"], errors="coerce", utc=True)
    data["bar_end_time"] = pd.to_datetime(data["bar_end_time"], errors="coerce", utc=True)

    data = data.dropna(subset=["bar_start_time"]).copy()
    data = data.sort_values(["bar_type", "bar_start_time"]).reset_index(drop=True)

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
    ]

    for col in numeric_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    if "imbalance_ratio" not in data.columns:
        data["imbalance_ratio"] = np.nan

    if "imbalance_abs" not in data.columns:
        data["imbalance_abs"] = np.nan

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

    data["direction"] = pd.to_numeric(data["direction"], errors="coerce").fillna(0).astype(int)
    data["direction_is_up"] = (data["direction"] == 1).astype(int)
    data["direction_is_down"] = (data["direction"] == -1).astype(int)
    data["direction_is_flat"] = (data["direction"] == 0).astype(int)

    return data


def add_rolling_features(data: pd.DataFrame) -> pd.DataFrame:
    feature_store = data.copy()

    grouped = feature_store.groupby("bar_type", group_keys=False)

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

        if "imbalance_ratio" in feature_store.columns:
            feature_store[f"rolling_abs_imbalance_mean_{window}"] = grouped["imbalance_ratio"].transform(
                lambda s: s.abs().rolling(window).mean()
            )

    return feature_store


def add_forward_labels(data: pd.DataFrame) -> pd.DataFrame:
    labelled = data.copy()
    grouped = labelled.groupby("bar_type", group_keys=False)

    for horizon in FORWARD_HORIZONS:
        labelled[f"future_return_h{horizon}"] = grouped["return"].shift(-horizon)
        labelled[f"future_abs_return_h{horizon}"] = labelled[f"future_return_h{horizon}"].abs()

        labelled[f"future_direction_h{horizon}"] = 0
        labelled.loc[labelled[f"future_return_h{horizon}"] > 0, f"future_direction_h{horizon}"] = 1
        labelled.loc[labelled[f"future_return_h{horizon}"] < 0, f"future_direction_h{horizon}"] = -1

        labelled[f"target_up_h{horizon}"] = (labelled[f"future_direction_h{horizon}"] == 1).astype(int)
        labelled[f"target_down_h{horizon}"] = (labelled[f"future_direction_h{horizon}"] == -1).astype(int)

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
        feature_store.groupby(["bar_type", "bar_family"], dropna=False)
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
            feature_null_pct=("return", lambda s: np.nan),
        )
        .reset_index()
    )

    feature_cols = [
        col for col in feature_store.columns
        if col.startswith("rolling_") or col.startswith("future_") or col.startswith("target_")
    ]

    null_rows = []

    for bar_type, group in feature_store.groupby("bar_type", dropna=False):
        total_cells = len(group) * len(feature_cols)
        null_pct = (
            group[feature_cols].isna().sum().sum() / total_cells * 100
            if total_cells > 0
            else np.nan
        )
        null_rows.append({"bar_type": bar_type, "feature_null_pct": null_pct})

    null_df = pd.DataFrame(null_rows)

    summary = summary.drop(columns=["feature_null_pct"]).merge(null_df, on="bar_type", how="left")

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary.sort_values("rows", ascending=False).reset_index(drop=True)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 20 BUILD MICROSTRUCTURE FEATURE STORE")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    regimes = pd.read_parquet(INPUT_PATH)

    print(f"Rows loaded: {len(regimes):,}")

    base = prepare_base_features(regimes)
    features = add_rolling_features(base)
    feature_store = add_forward_labels(features)

    feature_store["feature_store_build_time_utc"] = datetime.now(timezone.utc).isoformat()

    summary = build_summary(feature_store)

    OUTPUT_FEATURE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    feature_parquet = OUTPUT_FEATURE_DIR / f"{SYMBOL}_microstructure_feature_store_latest.parquet"
    feature_csv = OUTPUT_FEATURE_DIR / f"{SYMBOL}_microstructure_feature_store_latest.csv"

    summary_csv = OUTPUT_ANALYSIS_DIR / "microstructure_feature_store_summary_latest.csv"
    summary_parquet = OUTPUT_ANALYSIS_DIR / "microstructure_feature_store_summary_latest.parquet"

    feature_store.to_parquet(feature_parquet, index=False)
    feature_store.to_csv(feature_csv, index=False)

    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    print("[DONE] Microstructure feature store created.")
    print(f"Feature Parquet: {feature_parquet}")
    print(f"Feature CSV:     {feature_csv}")
    print(f"Summary CSV:     {summary_csv}")
    print(f"Summary Parquet: {summary_parquet}")
    print("-" * 90)

    print(f"Feature store rows:    {len(feature_store):,}")
    print(f"Feature store columns: {len(feature_store.columns):,}")
    print("-" * 90)

    display_cols = [
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

    print(summary[display_cols].to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()