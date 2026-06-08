"""
BACQE MICROSTRUCTURE 08 - BUILD MICROSTRUCTURE FEATURES

Purpose:
    Build feature-enhanced datasets from validated microstructure bars.

Inputs:
    E:/Quant_Lab/data/processed/microstructure/
        tick_bars/
        volume_bars/
        tick_imbalance_bars/

Outputs:
    E:/Quant_Lab/data/features/microstructure/
        symbol=GBPUSD/
            bar_type=tick_bars/
                parameter=tick_size_100/
                    microstructure_features.parquet
"""

from pathlib import Path
from datetime import datetime, timezone
import yaml
import pandas as pd
import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"


def print_header(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_analysis_dir(micro_cfg: dict) -> Path:
    return Path(
        micro_cfg["output"].get(
            "analysis_dir",
            "E:/Quant_Lab/data/analysis/microstructure",
        )
    )


def get_features_dir(micro_cfg: dict) -> Path:
    return Path(
        micro_cfg["output"].get(
            "features_dir",
            "E:/Quant_Lab/data/features/microstructure",
        )
    )


def find_inventory_file(micro_cfg: dict) -> Path:
    analysis_dir = get_analysis_dir(micro_cfg)
    return analysis_dir / "inventory" / "microstructure_inventory_latest.csv"


def safe_zscore(series: pd.Series, window: int) -> pd.Series:
    rolling_mean = series.rolling(window=window, min_periods=max(3, window // 3)).mean()
    rolling_std = series.rolling(window=window, min_periods=max(3, window // 3)).std()

    z = (series - rolling_mean) / rolling_std
    z = z.replace([np.inf, -np.inf], np.nan)

    return z


def add_base_features(df: pd.DataFrame, bar_type: str, parameter_name: str, parameter_value: int) -> pd.DataFrame:
    df = df.copy()

    df["bar_type"] = bar_type
    df["parameter_name"] = parameter_name
    df["parameter_value"] = parameter_value
    df["feature_created_at_utc"] = datetime.now(timezone.utc).isoformat()

    df["start_time"] = pd.to_datetime(df["start_time"], utc=True, errors="coerce")
    df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")

    df = df.sort_values("end_time").reset_index(drop=True)

    if "return_mid" not in df.columns:
        df["return_mid"] = df["close_mid"].pct_change()

    df["log_return_mid"] = np.log(df["close_mid"] / df["close_mid"].shift(1))
    df["abs_return_mid"] = df["return_mid"].abs()

    df["forward_return_1"] = df["close_mid"].shift(-1) / df["close_mid"] - 1
    df["forward_return_3"] = df["close_mid"].shift(-3) / df["close_mid"] - 1
    df["forward_return_5"] = df["close_mid"].shift(-5) / df["close_mid"] - 1

    df["range_mid_abs"] = df["high_mid"] - df["low_mid"]
    df["range_mid_pct"] = df["range_mid_abs"] / df["close_mid"]

    # ------------------------------------------------------------------
    # Spread feature compatibility layer
    # ------------------------------------------------------------------
    # These columns are required by downstream microstructure scripts,
    # especially Signal Factory and Signal Context Review.
    # Some bar builders may already provide avg_spread / max_spread,
    # while others provide bid/ask OHLC columns. We derive a consistent
    # naming layer here.

    if "open_ask" in df.columns and "open_bid" in df.columns:
        df["open_spread"] = df["open_ask"] - df["open_bid"]

    if "high_ask" in df.columns and "high_bid" in df.columns:
        df["high_spread"] = df["high_ask"] - df["high_bid"]

    if "low_ask" in df.columns and "low_bid" in df.columns:
        df["low_spread"] = df["low_ask"] - df["low_bid"]

    if "close_ask" in df.columns and "close_bid" in df.columns:
        df["close_spread"] = df["close_ask"] - df["close_bid"]

    spread_ohlc_cols = [
        col for col in [
            "open_spread",
            "high_spread",
            "low_spread",
            "close_spread",
        ]
        if col in df.columns
    ]

    if spread_ohlc_cols:
        df["spread_mean"] = df[spread_ohlc_cols].mean(axis=1)
        df["spread_min"] = df[spread_ohlc_cols].min(axis=1)
        df["spread_max"] = df[spread_ohlc_cols].max(axis=1)
        df["spread_range"] = df["spread_max"] - df["spread_min"]

    if "close_spread" in df.columns and "close_mid" in df.columns:
        df["spread_pct_of_mid"] = df["close_spread"] / df["close_mid"].replace(0, np.nan)

    if "duration_seconds" in df.columns:
        df["duration_seconds"] = pd.to_numeric(df["duration_seconds"], errors="coerce")
        df["bars_per_hour"] = 3600 / df["duration_seconds"].replace(0, np.nan)

        if "tick_count" in df.columns:
            df["ticks_per_second"] = df["tick_count"] / df["duration_seconds"].replace(0, np.nan)

    if "avg_spread" in df.columns:
        df["spread_change"] = df["avg_spread"].diff()
        df["spread_pct_of_price"] = df["avg_spread"] / df["close_mid"]

    if "max_spread" in df.columns and "avg_spread" in df.columns:
        df["max_to_avg_spread_ratio"] = df["max_spread"] / df["avg_spread"].replace(0, np.nan)

    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    windows = [3, 5, 10, 20]

    for window in windows:
        if "return_mid" in df.columns:
            df[f"return_mean_{window}"] = df["return_mid"].rolling(window, min_periods=max(2, window // 2)).mean()
            df[f"return_std_{window}"] = df["return_mid"].rolling(window, min_periods=max(2, window // 2)).std()
            df[f"realized_vol_{window}"] = (
                df["return_mid"]
                .rolling(window, min_periods=max(2, window // 2))
                .std()
                * np.sqrt(window)
            )

        if "abs_return_mid" in df.columns:
            df[f"abs_return_mean_{window}"] = df["abs_return_mid"].rolling(window, min_periods=max(2, window // 2)).mean()

        if "range_mid_pct" in df.columns:
            df[f"range_pct_mean_{window}"] = df["range_mid_pct"].rolling(window, min_periods=max(2, window // 2)).mean()
            df[f"range_pct_std_{window}"] = df["range_mid_pct"].rolling(window, min_periods=max(2, window // 2)).std()

        if "avg_spread" in df.columns:
            df[f"avg_spread_mean_{window}"] = df["avg_spread"].rolling(window, min_periods=max(2, window // 2)).mean()
            df[f"avg_spread_zscore_{window}"] = safe_zscore(df["avg_spread"], window)

        if "close_spread" in df.columns:
            df[f"spread_mean_{window}"] = df["close_spread"].rolling(
                window,
                min_periods=max(2, window // 2),
            ).mean()

            df[f"spread_zscore_{window}"] = safe_zscore(df["close_spread"], window)

        if "spread_range" in df.columns:
            df[f"spread_range_mean_{window}"] = df["spread_range"].rolling(
                window,
                min_periods=max(2, window // 2),
            ).mean()

        if "tick_count" in df.columns:
            df[f"tick_count_mean_{window}"] = df["tick_count"].rolling(window, min_periods=max(2, window // 2)).mean()
            df[f"tick_count_zscore_{window}"] = safe_zscore(df["tick_count"], window)

        if "duration_seconds" in df.columns:
            df[f"duration_mean_{window}"] = df["duration_seconds"].rolling(window, min_periods=max(2, window // 2)).mean()
            df[f"duration_zscore_{window}"] = safe_zscore(df["duration_seconds"], window)

    return df


def add_tick_imbalance_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required = {"signed_tick_sum", "uptick_count", "downtick_count", "tick_count"}

    if not required.issubset(df.columns):
        return df

    df["imbalance_ratio"] = df["signed_tick_sum"] / df["tick_count"].replace(0, np.nan)
    df["abs_imbalance_ratio"] = df["imbalance_ratio"].abs()

    df["uptick_pct"] = df["uptick_count"] / df["tick_count"].replace(0, np.nan)
    df["downtick_pct"] = df["downtick_count"] / df["tick_count"].replace(0, np.nan)

    df["uptick_minus_downtick_pct"] = df["uptick_pct"] - df["downtick_pct"]

    windows = [3, 5, 10, 20]

    for window in windows:
        df[f"imbalance_ratio_mean_{window}"] = (
            df["imbalance_ratio"]
            .rolling(window, min_periods=max(2, window // 2))
            .mean()
        )

        df[f"abs_imbalance_ratio_mean_{window}"] = (
            df["abs_imbalance_ratio"]
            .rolling(window, min_periods=max(2, window // 2))
            .mean()
        )

        df[f"imbalance_ratio_zscore_{window}"] = safe_zscore(df["imbalance_ratio"], window)

    return df


def clean_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.replace([np.inf, -np.inf], np.nan)

    object_cols = df.select_dtypes(include=["object"]).columns
    for col in object_cols:
        if col not in ["symbol", "bar_type", "parameter_name", "volume_mode"]:
            df[col] = df[col].astype(str)

    return df


def build_features_for_file(record: pd.Series, features_dir: Path) -> dict:
    file_path = Path(record["file_path"])
    bar_type = record["bar_type"]
    symbol = record["symbol"]
    parameter_name = record["parameter_name"]
    parameter_value = int(record["parameter_value"])

    result = {
        "symbol": symbol,
        "bar_type": bar_type,
        "parameter_name": parameter_name,
        "parameter_value": parameter_value,
        "input_file": str(file_path),
        "output_file": None,
        "status": "unknown",
        "input_rows": 0,
        "output_rows": 0,
        "feature_columns": 0,
        "error": None,
    }

    if not file_path.exists():
        result["status"] = "missing_input"
        result["error"] = "Input file does not exist."
        return result

    try:
        df = pd.read_parquet(file_path)
    except Exception as exc:
        result["status"] = "failed_read"
        result["error"] = str(exc)
        return result

    result["input_rows"] = len(df)

    if df.empty:
        result["status"] = "empty_input"
        result["error"] = "Input file is empty."
        return result

    try:
        features = add_base_features(
            df=df,
            bar_type=bar_type,
            parameter_name=parameter_name,
            parameter_value=parameter_value,
        )

        features = add_rolling_features(features)

        if bar_type == "tick_imbalance_bars":
            features = add_tick_imbalance_features(features)

        features = clean_feature_frame(features)

    except Exception as exc:
        result["status"] = "failed_feature_build"
        result["error"] = str(exc)
        return result

    parameter_slug = f"{parameter_name}_{parameter_value}"

    save_dir = (
        features_dir
        / f"symbol={symbol}"
        / f"bar_type={bar_type}"
        / f"parameter={parameter_slug}"
    )
    save_dir.mkdir(parents=True, exist_ok=True)

    save_path = save_dir / "microstructure_features.parquet"

    try:
        features.to_parquet(save_path, index=False)
    except Exception as exc:
        result["status"] = "failed_write"
        result["error"] = str(exc)
        return result

    result["output_file"] = str(save_path)
    result["status"] = "ok"
    result["output_rows"] = len(features)
    result["feature_columns"] = len(features.columns)

    return result


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 08 - BUILD MICROSTRUCTURE FEATURES")

    config = load_config()
    micro_cfg = config["microstructure"]

    inventory_file = find_inventory_file(micro_cfg)
    features_dir = get_features_dir(micro_cfg)
    analysis_dir = get_analysis_dir(micro_cfg)

    report_dir = analysis_dir / "feature_build"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Config:         {CONFIG_PATH}")
    print(f"Inventory:      {inventory_file}")
    print(f"Features dir:   {features_dir}")
    print(f"Report dir:     {report_dir}")
    print("-" * 90)

    if not inventory_file.exists():
        raise FileNotFoundError(
            f"Missing inventory file: {inventory_file}. "
            "Run 07_generate_microstructure_inventory.py first."
        )

    inventory_df = pd.read_csv(inventory_file)

    usable_statuses = {"ready", "usable_sparse"}
    build_df = inventory_df[inventory_df["dataset_status"].isin(usable_statuses)].copy()

    print(f"Inventory datasets: {len(inventory_df)}")
    print(f"Build candidates:   {len(build_df)}")
    print("-" * 90)

    results = []

    for _, record in build_df.iterrows():
        result = build_features_for_file(record, features_dir)
        results.append(result)

        print(
            f"[BUILD] {result['symbol']:<8} "
            f"{result['bar_type']:<22} "
            f"{result['parameter_name']}={result['parameter_value']:<5} "
            f"status={result['status']:<18} "
            f"rows={result['output_rows']:,} "
            f"cols={result['feature_columns']}"
        )

        if result["error"]:
            print(f"        error={result['error']}")

    results_df = pd.DataFrame(results)

    csv_path = report_dir / "microstructure_feature_build_latest.csv"
    json_path = report_dir / "microstructure_feature_build_latest.json"

    results_df.to_csv(csv_path, index=False)
    results_df.to_json(json_path, orient="records", indent=2)

    status_counts = results_df["status"].value_counts(dropna=False).to_dict()

    print("-" * 90)
    print("[DONE] Microstructure feature build complete.")
    print(f"Files attempted: {len(results_df)}")
    print(f"Status counts:   {status_counts}")
    print(f"CSV output:      {csv_path}")
    print(f"JSON output:     {json_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()