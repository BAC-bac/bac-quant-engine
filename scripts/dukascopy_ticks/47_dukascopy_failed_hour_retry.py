"""
BACQE DUKASCOPY 47 - FAILED HOUR RETRY

Purpose:
    Retry failed Dukascopy hourly downloads safely.

Safety:
    - Infers symbol from report filename.
    - Excludes Saturdays.
    - Excludes Sundays by default.
    - Filters by symbol.
    - Limits retry attempts by default.
"""

from pathlib import Path
import argparse
import importlib.util
import pandas as pd
import yaml


CONFIG_PATH = Path("config/dukascopy_research.yaml")


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["dukascopy_research"]


def import_download_module():
    script_path = Path("scripts/dukascopy_ticks/07_download_dukascopy_date_range.py")

    spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def infer_symbol_from_report_name(filename: str) -> str:
    return filename.split("_")[0].upper().strip()


def load_failed_hours(report_dir: Path) -> pd.DataFrame:
    files = sorted(report_dir.glob("*download_report.csv"))

    frames = []

    for file in files:
        try:
            df = pd.read_csv(file)
            df["source_report"] = file.name
            df["symbol"] = infer_symbol_from_report_name(file.name)
            frames.append(df)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    required = {"symbol", "date", "hour", "status", "source_report"}
    missing = required - set(combined.columns)

    if missing:
        print(f"[STOP] Missing columns in reports: {sorted(missing)}")
        return pd.DataFrame()

    failed = combined[
        combined["status"].astype(str).str.lower().eq("failed")
    ].copy()

    failed["date"] = pd.to_datetime(failed["date"], errors="coerce")
    failed = failed.dropna(subset=["date"])

    failed["weekday"] = failed["date"].dt.weekday
    failed["day_name"] = failed["date"].dt.day_name()

    failed["date"] = failed["date"].dt.strftime("%Y-%m-%d")
    failed["hour"] = pd.to_numeric(failed["hour"], errors="coerce").astype("Int64")

    failed = failed.dropna(subset=["hour"])

    return failed


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--symbol", type=str, default="GBPUSD")
    parser.add_argument("--max-retries", type=int, default=50)
    parser.add_argument("--include-sundays", action="store_true")
    parser.add_argument("--include-saturdays", action="store_true")

    args = parser.parse_args()

    banner("BACQE DUKASCOPY 47 - FAILED HOUR RETRY")

    cfg = load_config()

    analysis_root = Path(cfg["paths"]["analysis_root"])
    report_dir = analysis_root / "dukascopy_ticks" / "download_reports"

    print(f"Report dir:         {report_dir}")
    print(f"Symbol filter:      {args.symbol}")
    print(f"Max retries:        {args.max_retries}")
    print(f"Include Sundays:    {args.include_sundays}")
    print(f"Include Saturdays:  {args.include_saturdays}")
    print("-" * 90)

    failed = load_failed_hours(report_dir)

    if failed.empty:
        print("[PASS] No failed hourly downloads detected.")
        return

    failed = failed[failed["symbol"] == args.symbol.upper().strip()].copy()

    if not args.include_saturdays:
        failed = failed[failed["weekday"] != 5].copy()

    if not args.include_sundays:
        failed = failed[failed["weekday"] != 6].copy()

    # Exclude typical Friday late-market-close hours.
    failed = failed[~((failed["weekday"] == 4) & (failed["hour"].astype(int).isin([22, 23])))].copy()

    # Exclude typical Friday late-market-close/session-boundary hours.
    failed = failed[~((failed["weekday"] == 4) & (failed["hour"].astype(int).isin([21, 22, 23])))].copy()

    failed = failed.sort_values(["symbol", "date", "hour"])
    failed = failed.drop_duplicates(subset=["symbol", "date", "hour"])

    if args.max_retries > 0:
        failed = failed.head(args.max_retries).copy()

    if failed.empty:
        print("[PASS] No failed rows matched retry filters.")
        return

    print(f"Filtered failed rows: {len(failed):,}")
    print(failed[["symbol", "date", "day_name", "hour", "status", "source_report"]].head(40).to_string(index=False))
    print("-" * 90)

    module = import_download_module()

    if not hasattr(module, "download_one_hour"):
        print("[STOP] Script 07 does not expose download_one_hour().")
        return

    retry_rows = []

    for i, row in enumerate(failed.itertuples(index=False), start=1):
        print(f"[RETRY {i}/{len(failed)}] {row.symbol} {row.date} hour={int(row.hour):02d}")

        try:
            result = module.download_one_hour(
                symbol=row.symbol,
                date=row.date,
                hour=int(row.hour),
            )

            retry_rows.append({
                "symbol": row.symbol,
                "date": row.date,
                "hour": int(row.hour),
                "retry_status": result["status"],
                "bytes": result["bytes"],
                "path": result["path"],
                "error": result["error"],
            })

            print(f"    status={result['status']} bytes={result['bytes']}")

        except Exception as exc:
            retry_rows.append({
                "symbol": row.symbol,
                "date": row.date,
                "hour": int(row.hour),
                "retry_status": "error",
                "bytes": 0,
                "path": "",
                "error": repr(exc),
            })

            print(f"    error={exc}")

    output_root = analysis_root / "dukascopy_failed_hour_retry"
    output_root.mkdir(parents=True, exist_ok=True)

    output_path = output_root / "failed_hour_retry_latest.csv"
    pd.DataFrame(retry_rows).to_csv(output_path, index=False)

    print("=" * 90)
    print("[DONE] Failed-hour retry complete.")
    print(f"Retry rows: {len(retry_rows):,}")
    print(f"Output:     {output_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()