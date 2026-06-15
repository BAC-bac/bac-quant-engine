"""
BACQE DUKASCOPY 30 - HORIZON EXPANSION ENGINE

Purpose:
    Add longer forward return horizons to engineered Dukascopy feature datasets.

Refactor note:
    This script can run standalone with CLI arguments and also exposes
    run_horizon_expansion() for orchestration scripts.

Example:
    python scripts/dukascopy_ticks/30_horizon_expansion_engine.py --symbol GBPUSD
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

HORIZONS = [1, 3, 5, 10, 20, 25, 50, 100, 250, 500, 1000]
MIN_ROWS = 1_500


def get_input_root(symbol: str) -> Path:
    symbol = symbol.upper().strip()
    return (
        QUANT_LAB
        / "data"
        / "processed"
        / "dukascopy_engineered_features"
        / f"symbol={symbol}"
    )


def get_output_root(symbol: str) -> Path:
    symbol = symbol.upper().strip()
    return (
        QUANT_LAB
        / "data"
        / "processed"
        / "dukascopy_horizon_features"
        / f"symbol={symbol}"
    )


def get_report_root(symbol: str) -> Path:
    symbol = symbol.upper().strip()
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_horizon_expansion"
        / f"symbol={symbol}"
    )


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs(output_root: Path, report_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)


def discover_files(input_root: Path) -> list[Path]:
    return sorted(input_root.rglob("*.parquet")) if input_root.exists() else []


def get_output_path(input_path: Path, output_root: Path) -> Path:
    year = None
    month = None

    for part in input_path.parts:
        if part.startswith("year="):
            year = part
        elif part.startswith("month="):
            month = part

    output_name = input_path.name.replace(
        "_engineered_features.parquet",
        "_horizon_features.parquet",
    )

    if year and month:
        output_dir = output_root / year / month
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / output_name

    output_root.mkdir(parents=True, exist_ok=True)
    return output_root / output_name


def add_forward_horizons(df: pd.DataFrame, horizons: list[int] | None = None) -> pd.DataFrame:
    df = df.copy()
    horizons = horizons or HORIZONS

    for h in horizons:
        df[f"future_return_{h}"] = df["mid"].shift(-h) / df["mid"] - 1

    return df


def process_file(path: Path, output_root: Path, horizons: list[int] | None = None) -> dict:
    horizons = horizons or HORIZONS
    df = pd.read_parquet(path)

    if len(df) < MIN_ROWS:
        return {
            "input_file": str(path),
            "output_file": None,
            "status": "skipped_too_few_rows",
            "rows_in": len(df),
            "rows_out": 0,
            "horizons_added": 0,
        }

    required = {"timestamp_utc", "mid"}
    missing = required - set(df.columns)

    if missing:
        return {
            "input_file": str(path),
            "output_file": None,
            "status": f"missing_columns_{sorted(missing)}",
            "rows_in": len(df),
            "rows_out": 0,
            "horizons_added": 0,
        }

    df = df.sort_values("timestamp_utc").reset_index(drop=True)
    df = add_forward_horizons(df, horizons=horizons)
    df = df.replace([np.inf, -np.inf], np.nan)

    output_path = get_output_path(path, output_root)
    df.to_parquet(output_path, index=False)

    return {
        "input_file": str(path),
        "output_file": str(output_path),
        "status": "ok",
        "rows_in": len(df),
        "rows_out": len(df),
        "horizons_added": len(horizons),
    }


def run_horizon_expansion(
    symbol: str = DEFAULT_SYMBOL,
    horizons: list[int] | None = None,
) -> tuple[Path, Path]:
    symbol = symbol.upper().strip()
    horizons = horizons or HORIZONS

    input_root = get_input_root(symbol)
    output_root = get_output_root(symbol)
    report_root = get_report_root(symbol)

    banner("BACQE DUKASCOPY 30 - HORIZON EXPANSION ENGINE")
    ensure_dirs(output_root, report_root)

    print(f"Symbol:      {symbol}")
    print(f"Input root:  {input_root}")
    print(f"Output root: {output_root}")
    print(f"Report root: {report_root}")
    print(f"Horizons:    {horizons}")
    print("-" * 90)

    files = discover_files(input_root)

    print(f"Input files discovered: {len(files)}")
    print("-" * 90)

    if not files:
        print("[STOP] No input files found.")
        return report_root / f"{symbol}_horizon_expansion_latest.csv", report_root / f"{symbol}_horizon_expansion_report_latest.txt"

    results = []

    for i, path in enumerate(files, start=1):
        print(f"[{i}/{len(files)}] {path}")

        try:
            result = process_file(path, output_root=output_root, horizons=horizons)
            results.append(result)

            print(
                f"    status={result['status']} "
                f"rows={result['rows_in']:,} "
                f"horizons={result['horizons_added']}"
            )

        except Exception as e:
            results.append({
                "input_file": str(path),
                "output_file": None,
                "status": "error",
                "error": str(e),
                "rows_in": None,
                "rows_out": None,
                "horizons_added": None,
            })

            print(f"    [ERROR] {e}")

    results_df = pd.DataFrame(results)

    report_csv = report_root / f"{symbol}_horizon_expansion_latest.csv"
    report_txt = report_root / f"{symbol}_horizon_expansion_report_latest.txt"

    results_df.to_csv(report_csv, index=False)

    ok_count = (results_df["status"] == "ok").sum()
    skipped_count = results_df["status"].str.startswith("skipped").sum()
    error_count = (results_df["status"] == "error").sum()

    with open(report_txt, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY HORIZON EXPANSION REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Input files: {len(files)}\n")
        f.write(f"Files OK: {ok_count}\n")
        f.write(f"Files skipped: {skipped_count}\n")
        f.write(f"Files errored: {error_count}\n")
        f.write(f"Horizons added: {horizons}\n\n")
        f.write(f"Input root: {input_root}\n")
        f.write(f"Output root: {output_root}\n")
        f.write(f"CSV report: {report_csv}\n")

    print("=" * 90)
    print("[DONE] Horizon expansion complete.")
    print(f"Files OK:      {ok_count}")
    print(f"Files skipped: {skipped_count}")
    print(f"Files errored: {error_count}")
    print(f"CSV report:    {report_csv}")
    print(f"TXT report:    {report_txt}")
    print(f"Output root:   {output_root}")
    print("=" * 90)

    return report_csv, report_txt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add forward-return horizons to Dukascopy engineered feature files."
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_horizon_expansion(symbol=args.symbol)


if __name__ == "__main__":
    main()
