"""
BACQE DUKASCOPY 30 - HORIZON EXPANSION ENGINE

Purpose:
    Add longer forward return horizons to engineered Dukascopy feature datasets.

Input:
    E:\\Quant_Lab\\data\\processed\\dukascopy_engineered_features\\symbol=EURUSD

Output:
    E:\\Quant_Lab\\data\\processed\\dukascopy_horizon_features\\symbol=EURUSD
"""

from pathlib import Path
import numpy as np
import pandas as pd


SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

INPUT_ROOT = (
    QUANT_LAB / "data" / "processed" / "dukascopy_engineered_features" / f"symbol={SYMBOL}"
)

OUTPUT_ROOT = (
    QUANT_LAB / "data" / "processed" / "dukascopy_horizon_features" / f"symbol={SYMBOL}"
)

REPORT_ROOT = QUANT_LAB / "data" / "analysis" / "dukascopy_horizon_expansion"

HORIZONS = [1, 3, 5, 10, 20, 25, 50, 100, 250, 500, 1000]

MIN_ROWS = 1_500


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)


def discover_files() -> list[Path]:
    return sorted(INPUT_ROOT.rglob("*.parquet")) if INPUT_ROOT.exists() else []


def get_output_path(input_path: Path) -> Path:
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
        output_dir = OUTPUT_ROOT / year / month
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / output_name

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    return OUTPUT_ROOT / output_name


def add_forward_horizons(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for h in HORIZONS:
        df[f"future_return_{h}"] = df["mid"].shift(-h) / df["mid"] - 1

    return df


def process_file(path: Path) -> dict:
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
    df = add_forward_horizons(df)
    df = df.replace([np.inf, -np.inf], np.nan)

    output_path = get_output_path(path)
    df.to_parquet(output_path, index=False)

    return {
        "input_file": str(path),
        "output_file": str(output_path),
        "status": "ok",
        "rows_in": len(df),
        "rows_out": len(df),
        "horizons_added": len(HORIZONS),
    }


def main() -> None:
    banner("BACQE DUKASCOPY 30 - HORIZON EXPANSION ENGINE")

    ensure_dirs()

    print(f"Symbol:      {SYMBOL}")
    print(f"Input root:  {INPUT_ROOT}")
    print(f"Output root: {OUTPUT_ROOT}")
    print(f"Report root: {REPORT_ROOT}")
    print(f"Horizons:    {HORIZONS}")
    print("-" * 90)

    files = discover_files()

    print(f"Input files discovered: {len(files)}")
    print("-" * 90)

    if not files:
        print("[STOP] No input files found.")
        return

    results = []

    for i, path in enumerate(files, start=1):
        print(f"[{i}/{len(files)}] {path}")

        try:
            result = process_file(path)
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

    report_csv = REPORT_ROOT / "horizon_expansion_latest.csv"
    report_txt = REPORT_ROOT / "horizon_expansion_report_latest.txt"

    results_df.to_csv(report_csv, index=False)

    ok_count = (results_df["status"] == "ok").sum()
    skipped_count = results_df["status"].str.startswith("skipped").sum()
    error_count = (results_df["status"] == "error").sum()

    with open(report_txt, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY HORIZON EXPANSION REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {SYMBOL}\n")
        f.write(f"Input files: {len(files)}\n")
        f.write(f"Files OK: {ok_count}\n")
        f.write(f"Files skipped: {skipped_count}\n")
        f.write(f"Files errored: {error_count}\n")
        f.write(f"Horizons added: {HORIZONS}\n\n")
        f.write(f"Output root: {OUTPUT_ROOT}\n")
        f.write(f"CSV report: {report_csv}\n")

    print("=" * 90)
    print("[DONE] Horizon expansion complete.")
    print(f"Files OK:      {ok_count}")
    print(f"Files skipped: {skipped_count}")
    print(f"Files errored: {error_count}")
    print(f"CSV report:    {report_csv}")
    print(f"TXT report:    {report_txt}")
    print(f"Output root:   {OUTPUT_ROOT}")
    print("=" * 90)


if __name__ == "__main__":
    main()