"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 01 - BUILD EXTENDED HORIZON TARGETS

Purpose:
    Build longer forward-return targets for Dukascopy engineered feature files.

Pilot:
    EURJPY

Default horizons:
    2500, 5000, 10000, 20000 ticks
"""

from pathlib import Path
import argparse
import pandas as pd
import numpy as np


DEFAULT_SYMBOL = "EURJPY"
DEFAULT_HORIZONS = [2500, 5000, 10000, 20000]

BASE_DIR = Path("E:/Quant_Lab")

INPUT_ROOT = BASE_DIR / "data" / "processed" / "dukascopy_engineered_features"
OUTPUT_ROOT = BASE_DIR / "data" / "processed" / "dukascopy_extended_horizon_features"
REPORT_ROOT = BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "target_build"


PRICE_COLUMN_CANDIDATES = [
    "close",
    "mid",
    "mid_price",
    "price",
    "last",
]


def print_header(symbol: str, horizons: list[int]) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 01 - BUILD EXTENDED HORIZON TARGETS")
    print("=" * 90)
    print(f"Symbol:       {symbol}")
    print(f"Horizons:     {horizons}")
    print(f"Input root:   {INPUT_ROOT}")
    print(f"Output root:  {OUTPUT_ROOT}")
    print(f"Report root:  {REPORT_ROOT}")
    print("-" * 90)


def find_parquet_files(symbol: str) -> list[Path]:
    symbol_root = INPUT_ROOT / f"symbol={symbol}"

    if not symbol_root.exists():
        raise FileNotFoundError(f"Input symbol folder not found: {symbol_root}")

    files = sorted(symbol_root.rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No parquet files found under: {symbol_root}")

    return files


def detect_price_column(columns: list[str]) -> str:
    lower_map = {col.lower(): col for col in columns}

    for candidate in PRICE_COLUMN_CANDIDATES:
        if candidate in lower_map:
            return lower_map[candidate]

    raise ValueError(
        "Could not detect price column. "
        f"Tried: {PRICE_COLUMN_CANDIDATES}. "
        f"Available columns: {columns[:30]}"
    )


def read_price_series(path: Path, price_col: str) -> pd.Series:
    try:
        return pd.read_parquet(path, columns=[price_col])[price_col]
    except Exception:
        df = pd.read_parquet(path)
        if price_col not in df.columns:
            raise ValueError(f"Price column '{price_col}' missing from {path}")
        return df[price_col]


def build_extended_targets_for_file(
    current_path: Path,
    future_paths: list[Path],
    horizons: list[int],
    price_col: str,
) -> tuple[pd.DataFrame, dict]:
    current_df = pd.read_parquet(current_path)

    current_prices = current_df[price_col].reset_index(drop=True)
    price_blocks = [current_prices]

    rows_available = len(current_prices)
    max_horizon = max(horizons)

    for future_path in future_paths:
        if rows_available > max_horizon:
            break

        future_prices = read_price_series(future_path, price_col).reset_index(drop=True)
        price_blocks.append(future_prices)
        rows_available += len(future_prices)

    combined_prices = pd.concat(price_blocks, ignore_index=True)

    report_row = {
        "file": str(current_path),
        "rows": len(current_df),
        "price_col": price_col,
        "rows_available_for_forward_calc": len(combined_prices),
    }

    for horizon in horizons:
        target_col = f"future_return_{horizon}"

        future_price = combined_prices.shift(-horizon)
        future_return = (future_price / combined_prices) - 1.0

        current_df[target_col] = future_return.iloc[: len(current_df)].to_numpy()

        valid_count = current_df[target_col].notna().sum()
        report_row[f"{target_col}_valid"] = int(valid_count)
        report_row[f"{target_col}_missing"] = int(len(current_df) - valid_count)

    return current_df, report_row


def output_path_for(symbol: str, input_path: Path) -> Path:
    symbol_input_root = INPUT_ROOT / f"symbol={symbol}"
    symbol_output_root = OUTPUT_ROOT / f"symbol={symbol}"

    relative_path = input_path.relative_to(symbol_input_root)
    return symbol_output_root / relative_path


def main(symbol: str, horizons: list[int]) -> None:
    print_header(symbol, horizons)

    files = find_parquet_files(symbol)

    first_df = pd.read_parquet(files[0])
    price_col = detect_price_column(list(first_df.columns))

    print(f"Files found:  {len(files)}")
    print(f"Price column: {price_col}")
    print("-" * 90)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    report_rows = []

    for idx, current_path in enumerate(files, start=1):
        future_paths = files[idx:]

        try:
            output_df, report_row = build_extended_targets_for_file(
                current_path=current_path,
                future_paths=future_paths,
                horizons=horizons,
                price_col=price_col,
            )

            out_path = output_path_for(symbol, current_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)

            output_df.to_parquet(out_path, index=False)

            report_row["status"] = "ok"
            report_row["output_file"] = str(out_path)

            print(
                f"[OK] {idx:>4}/{len(files)} "
                f"rows={len(output_df):>7} "
                f"saved={out_path.name}"
            )

        except Exception as exc:
            report_row = {
                "file": str(current_path),
                "status": "error",
                "error": str(exc),
            }

            print(f"[ERROR] {idx:>4}/{len(files)} {current_path.name} :: {exc}")

        report_rows.append(report_row)

    report_df = pd.DataFrame(report_rows)

    report_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_target_build_latest.csv"
    report_df.to_csv(report_path, index=False)

    print("-" * 90)
    print("[DONE] Extended horizon target build complete")
    print(f"Report saved: {report_path}")
    print(f"Output folder: {OUTPUT_ROOT / f'symbol={symbol}'}")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--symbol",
        default=DEFAULT_SYMBOL,
        help="Symbol to process, e.g. EURJPY",
    )

    parser.add_argument(
        "--horizons",
        nargs="+",
        type=int,
        default=DEFAULT_HORIZONS,
        help="Forward horizons in ticks",
    )

    args = parser.parse_args()

    main(
        symbol=args.symbol.upper(),
        horizons=args.horizons,
    )