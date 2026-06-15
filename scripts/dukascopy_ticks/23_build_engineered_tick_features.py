"""
BACQE DUKASCOPY 23 - BUILD ENGINEERED TICK FEATURES

Purpose:
    Convert processed Dukascopy tick files into engineered feature datasets.

Examples:
    python scripts\dukascopy_ticks\23_build_engineered_tick_features.py --symbol EURUSD
    python scripts\dukascopy_ticks\23_build_engineered_tick_features.py --symbol GBPUSD

Input:
    E:\\Quant_Lab\\data\\processed\\dukascopy_ticks\\symbol=<SYMBOL>\\year=...\\month=...

Output:
    E:\\Quant_Lab\\data\\processed\\dukascopy_engineered_features\\symbol=<SYMBOL>\\year=...\\month=...
"""

from pathlib import Path
import argparse

import numpy as np
import pandas as pd


# =============================================================================
# CONFIG
# =============================================================================

DEFAULT_SYMBOL = "EURUSD"
DEFAULT_QUANT_LAB = Path(r"E:\Quant_Lab")

ROLLING_WINDOWS = [5, 10, 25, 50, 100, 250]
MIN_ROWS = 500


# =============================================================================
# PATH HELPERS
# =============================================================================

def build_input_root(symbol: str, quant_lab: Path = DEFAULT_QUANT_LAB) -> Path:
    return quant_lab / "data" / "processed" / "dukascopy_ticks" / f"symbol={symbol}"


def build_output_root(symbol: str, quant_lab: Path = DEFAULT_QUANT_LAB) -> Path:
    return quant_lab / "data" / "processed" / "dukascopy_engineered_features" / f"symbol={symbol}"


def build_report_root(quant_lab: Path = DEFAULT_QUANT_LAB) -> Path:
    return quant_lab / "data" / "analysis" / "dukascopy_feature_engineering"


def ensure_dirs(output_root: Path, report_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)


def discover_input_files(input_root: Path) -> list[Path]:
    if not input_root.exists():
        print(f"[MISSING INPUT ROOT] {input_root}")
        return []

    return sorted(input_root.rglob("*.parquet"))


def get_output_path(input_path: Path, output_root: Path) -> Path:
    year = None
    month = None

    for part in input_path.parts:
        if part.startswith("year="):
            year = part
        if part.startswith("month="):
            month = part

    if year is None or month is None:
        output_root.mkdir(parents=True, exist_ok=True)
        return output_root / input_path.name.replace("_ticks.parquet", "_engineered_features.parquet")

    output_dir = output_root / year / month
    output_dir.mkdir(parents=True, exist_ok=True)

    output_name = input_path.name.replace("_ticks.parquet", "_engineered_features.parquet")
    return output_dir / output_name


# =============================================================================
# HELPERS
# =============================================================================

def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


# =============================================================================
# FEATURE ENGINEERING
# =============================================================================

def add_basic_tick_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["mid_return_1"] = df["mid"].pct_change()
    df["mid_return_5"] = df["mid"].pct_change(5)
    df["mid_return_10"] = df["mid"].pct_change(10)
    df["mid_return_25"] = df["mid"].pct_change(25)

    df["mid_diff_1"] = df["mid"].diff()
    df["bid_diff_1"] = df["bid"].diff()
    df["ask_diff_1"] = df["ask"].diff()

    df["spread_change"] = df["spread"].diff()
    df["spread_pct_change"] = df["spread"].pct_change()

    df["tick_direction"] = np.sign(df["mid_diff_1"]).fillna(0)
    df["up_tick"] = (df["tick_direction"] > 0).astype(int)
    df["down_tick"] = (df["tick_direction"] < 0).astype(int)
    df["flat_tick"] = (df["tick_direction"] == 0).astype(int)

    df["signed_tick"] = df["tick_direction"]
    df["absolute_mid_move"] = df["mid_diff_1"].abs()

    df["price_acceleration"] = df["mid_diff_1"].diff()

    return df


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for window in ROLLING_WINDOWS:
        df[f"rolling_return_mean_{window}"] = df["mid_return_1"].rolling(window).mean()
        df[f"rolling_return_std_{window}"] = df["mid_return_1"].rolling(window).std()

        df[f"rolling_spread_mean_{window}"] = df["spread"].rolling(window).mean()
        df[f"rolling_spread_std_{window}"] = df["spread"].rolling(window).std()

        df[f"rolling_up_ticks_{window}"] = df["up_tick"].rolling(window).sum()
        df[f"rolling_down_ticks_{window}"] = df["down_tick"].rolling(window).sum()
        df[f"rolling_flat_ticks_{window}"] = df["flat_tick"].rolling(window).sum()

        df[f"tick_imbalance_{window}"] = (
            df[f"rolling_up_ticks_{window}"] - df[f"rolling_down_ticks_{window}"]
        )

        df[f"tick_imbalance_ratio_{window}"] = df[f"tick_imbalance_{window}"] / window

        df[f"buy_pressure_{window}"] = df[f"rolling_up_ticks_{window}"] / window
        df[f"sell_pressure_{window}"] = df[f"rolling_down_ticks_{window}"] / window

        denominator = (
            df[f"rolling_up_ticks_{window}"] + df[f"rolling_down_ticks_{window}"]
        ).replace(0, np.nan)

        df[f"order_flow_ratio_{window}"] = (
            df[f"rolling_up_ticks_{window}"] - df[f"rolling_down_ticks_{window}"]
        ) / denominator

        df[f"rolling_abs_move_mean_{window}"] = df["absolute_mid_move"].rolling(window).mean()
        df[f"rolling_abs_move_sum_{window}"] = df["absolute_mid_move"].rolling(window).sum()

    return df


def add_zscore_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for window in ROLLING_WINDOWS:
        spread_mean = df["spread"].rolling(window).mean()
        spread_std = df["spread"].rolling(window).std().replace(0, np.nan)

        df[f"spread_zscore_{window}"] = (df["spread"] - spread_mean) / spread_std

        vol_mean = df[f"rolling_return_std_{window}"].rolling(window).mean()
        vol_std = df[f"rolling_return_std_{window}"].rolling(window).std().replace(0, np.nan)

        df[f"volatility_zscore_{window}"] = (
            df[f"rolling_return_std_{window}"] - vol_mean
        ) / vol_std

    return df


def add_forward_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for window in [1, 3, 5, 10, 20, 50]:
        df[f"future_return_{window}"] = df["mid"].shift(-window) / df["mid"] - 1

    return df


def clean_features(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace([np.inf, -np.inf], np.nan)


def process_file(path: Path, output_root: Path) -> dict:
    df = pd.read_parquet(path)

    if len(df) < MIN_ROWS:
        return {
            "input_file": str(path),
            "output_file": None,
            "status": "skipped_too_few_rows",
            "rows_in": len(df),
            "rows_out": 0,
            "features": 0,
        }

    required_cols = {"timestamp_utc", "bid", "ask", "mid", "spread"}
    missing_cols = required_cols - set(df.columns)

    if missing_cols:
        return {
            "input_file": str(path),
            "output_file": None,
            "status": f"missing_columns_{sorted(missing_cols)}",
            "rows_in": len(df),
            "rows_out": 0,
            "features": 0,
        }

    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    df = add_basic_tick_features(df)
    df = add_rolling_features(df)
    df = add_zscore_features(df)
    df = add_forward_returns(df)
    df = clean_features(df)

    output_path = get_output_path(path, output_root)
    df.to_parquet(output_path, index=False)

    feature_cols = [
        col for col in df.columns
        if col not in ["timestamp_utc", "bid", "ask", "mid", "spread"]
    ]

    return {
        "input_file": str(path),
        "output_file": str(output_path),
        "status": "ok",
        "rows_in": len(df),
        "rows_out": len(df),
        "features": len(feature_cols),
    }


# =============================================================================
# RUNNER
# =============================================================================

def run_feature_engineering(
    symbol: str = DEFAULT_SYMBOL,
    quant_lab: Path = DEFAULT_QUANT_LAB,
) -> Path | None:
    symbol = symbol.upper().strip()

    input_root = build_input_root(symbol=symbol, quant_lab=quant_lab)
    output_root = build_output_root(symbol=symbol, quant_lab=quant_lab)
    report_root = build_report_root(quant_lab=quant_lab)

    banner("BACQE DUKASCOPY 23 - BUILD ENGINEERED TICK FEATURES")

    ensure_dirs(output_root=output_root, report_root=report_root)

    print(f"Symbol:      {symbol}")
    print(f"Input root:  {input_root}")
    print(f"Output root: {output_root}")
    print(f"Report root: {report_root}")
    print("-" * 90)

    files = discover_input_files(input_root=input_root)

    print(f"Input files discovered: {len(files)}")
    print("-" * 90)

    if not files:
        print("[STOP] No input files found.")
        return None

    results = []

    for i, path in enumerate(files, start=1):
        print(f"[{i}/{len(files)}] {path}")

        try:
            result = process_file(path=path, output_root=output_root)
            results.append(result)

            print(
                f"    status={result['status']} "
                f"rows={result['rows_in']:,} "
                f"features={result['features']}"
            )

        except Exception as e:
            results.append({
                "input_file": str(path),
                "output_file": None,
                "status": "error",
                "error": str(e),
                "rows_in": None,
                "rows_out": None,
                "features": None,
            })

            print(f"    [ERROR] {e}")

    results_df = pd.DataFrame(results)

    report_path = report_root / f"{symbol}_dukascopy_feature_engineering_latest.csv"
    results_df.to_csv(report_path, index=False)

    ok_count = (results_df["status"] == "ok").sum()
    skipped_count = results_df["status"].astype(str).str.startswith("skipped").sum()
    error_count = (results_df["status"] == "error").sum()

    print("=" * 90)
    print("[DONE] Engineered feature build complete.")
    print(f"Files processed OK: {ok_count}")
    print(f"Files skipped:       {skipped_count}")
    print(f"Files errored:       {error_count}")
    print(f"Report:              {report_path}")
    print(f"Output root:         {output_root}")
    print("=" * 90)

    return report_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build engineered tick features from processed Dukascopy ticks."
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_feature_engineering(symbol=args.symbol)


if __name__ == "__main__":
    main()
