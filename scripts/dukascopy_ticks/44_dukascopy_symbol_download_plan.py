"""
BACQE DUKASCOPY 44 - SYMBOL DOWNLOAD PLAN

Purpose:
    Build a safe, controlled Dukascopy download plan from Script 43 inventory.

Outputs:
    - full missing processed tick plan
    - symbol-specific plan
    - limited batch plan for first download run
"""

from pathlib import Path
import argparse
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


def ensure_dirs(analysis_root: Path) -> Path:
    output_root = analysis_root / "dukascopy_download_plan"

    for folder in [
        output_root,
        output_root / "plans",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)

    return output_root


def get_inventory_missing_path(cfg: dict) -> Path:
    analysis_root = Path(cfg["paths"]["analysis_root"])

    return (
        analysis_root
        / "dukascopy_symbol_inventory"
        / "missing_dates"
        / "dukascopy_missing_dates_latest.csv"
    )


def load_missing_dates(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing inventory output: {path}. Run Script 43 first."
        )

    return pd.read_csv(path)


def build_plan(
    missing: pd.DataFrame,
    symbol: str | None,
    max_dates: int | None,
    include_existing_symbols: bool,
) -> pd.DataFrame:
    df = missing.copy()

    df = df[df["missing_stage"] == "processed_ticks"].copy()

    if symbol:
        df = df[df["symbol"] == symbol].copy()

    if not include_existing_symbols:
        # Protect EURUSD by default because it already has most data complete.
        df = df[df["symbol"] != "EURUSD"].copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values(["symbol", "date"])

    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    df["plan_status"] = "planned"
    df["download_stage"] = "processed_ticks"
    df["priority"] = df.groupby("symbol").cumcount() + 1

    if max_dates is not None and max_dates > 0:
        df = (
            df.groupby("symbol", group_keys=False)
            .head(max_dates)
            .copy()
        )

    return df[
        [
            "symbol",
            "date",
            "missing_stage",
            "download_stage",
            "priority",
            "plan_status",
        ]
    ]


def summarise_plan(plan: pd.DataFrame) -> pd.DataFrame:
    if plan.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "planned_dates",
                "start_date",
                "end_date",
            ]
        )

    return (
        plan.groupby("symbol", as_index=False)
        .agg(
            planned_dates=("date", "count"),
            start_date=("date", "min"),
            end_date=("date", "max"),
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--symbol",
        type=str,
        default="GBPUSD",
        help="Symbol to build plan for. Default: GBPUSD",
    )

    parser.add_argument(
        "--max-dates",
        type=int,
        default=31,
        help="Maximum dates per symbol for the batch plan. Default: 31",
    )

    parser.add_argument(
        "--include-existing-symbols",
        action="store_true",
        help="Allow planning for symbols such as EURUSD that already have data.",
    )

    args = parser.parse_args()

    banner("BACQE DUKASCOPY 44 - SYMBOL DOWNLOAD PLAN")

    cfg = load_config()
    analysis_root = Path(cfg["paths"]["analysis_root"])
    output_root = ensure_dirs(analysis_root)

    missing_path = get_inventory_missing_path(cfg)

    print(f"Config:       {CONFIG_PATH}")
    print(f"Missing file: {missing_path}")
    print(f"Output root:  {output_root}")
    print(f"Symbol:       {args.symbol}")
    print(f"Max dates:    {args.max_dates}")
    print("-" * 90)

    missing = load_missing_dates(missing_path)

    full_plan = build_plan(
        missing=missing,
        symbol=args.symbol,
        max_dates=None,
        include_existing_symbols=args.include_existing_symbols,
    )

    batch_plan = build_plan(
        missing=missing,
        symbol=args.symbol,
        max_dates=args.max_dates,
        include_existing_symbols=args.include_existing_symbols,
    )

    full_summary = summarise_plan(full_plan)
    batch_summary = summarise_plan(batch_plan)

    full_plan_path = output_root / "plans" / "dukascopy_full_download_plan_latest.csv"
    batch_plan_path = output_root / "plans" / "dukascopy_batch_download_plan_latest.csv"
    full_summary_path = output_root / "plans" / "dukascopy_full_download_plan_summary_latest.csv"
    batch_summary_path = output_root / "plans" / "dukascopy_batch_download_plan_summary_latest.csv"
    report_path = output_root / "reports" / "dukascopy_download_plan_report_latest.txt"

    full_plan.to_csv(full_plan_path, index=False)
    batch_plan.to_csv(batch_plan_path, index=False)
    full_summary.to_csv(full_summary_path, index=False)
    batch_summary.to_csv(batch_summary_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY DOWNLOAD PLAN REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Config: {CONFIG_PATH}\n")
        f.write(f"Missing input: {missing_path}\n")
        f.write(f"Symbol filter: {args.symbol}\n")
        f.write(f"Max dates per batch: {args.max_dates}\n")
        f.write(f"Include existing symbols: {args.include_existing_symbols}\n\n")

        f.write("Full Plan Summary\n")
        f.write("-" * 80 + "\n")
        f.write(full_summary.to_string(index=False))

        f.write("\n\nBatch Plan Summary\n")
        f.write("-" * 80 + "\n")
        f.write(batch_summary.to_string(index=False))

        f.write("\n\nBatch Plan Preview\n")
        f.write("-" * 80 + "\n")
        f.write(batch_plan.head(40).to_string(index=False))

        f.write("\n\nOutputs:\n")
        f.write(f"Full plan: {full_plan_path}\n")
        f.write(f"Batch plan: {batch_plan_path}\n")
        f.write(f"Full summary: {full_summary_path}\n")
        f.write(f"Batch summary: {batch_summary_path}\n")

    print("FULL PLAN SUMMARY")
    print("-" * 90)
    print(full_summary.to_string(index=False))

    print()
    print("BATCH PLAN SUMMARY")
    print("-" * 90)
    print(batch_summary.to_string(index=False))

    print("=" * 90)
    print("[DONE] Download plan complete.")
    print(f"Full plan:  {full_plan_path}")
    print(f"Batch plan: {batch_plan_path}")
    print(f"Report:     {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()