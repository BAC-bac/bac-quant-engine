"""
BACQE DUKASCOPY 46 - TRADING DAY DOWNLOAD PLAN

Purpose:
    Build cleaner Dukascopy download batches by excluding non-trading days.
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


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--symbol", type=str, default="GBPUSD")
    parser.add_argument("--max-dates", type=int, default=31)
    parser.add_argument("--include-sundays", action="store_true")

    args = parser.parse_args()

    banner("BACQE DUKASCOPY 46 - TRADING DAY DOWNLOAD PLAN")

    cfg = load_config()
    analysis_root = Path(cfg["paths"]["analysis_root"])

    missing_path = (
        analysis_root
        / "dukascopy_symbol_inventory"
        / "missing_dates"
        / "dukascopy_missing_dates_latest.csv"
    )

    output_root = analysis_root / "dukascopy_download_plan"

    for folder in [output_root / "plans", output_root / "reports"]:
        folder.mkdir(parents=True, exist_ok=True)

    if not missing_path.exists():
        print(f"[STOP] Missing inventory file: {missing_path}")
        return

    missing = pd.read_csv(missing_path)

    plan = missing[
        (missing["symbol"] == args.symbol)
        & (missing["missing_stage"] == "processed_ticks")
    ].copy()

    plan["date"] = pd.to_datetime(plan["date"], errors="coerce")
    plan = plan.dropna(subset=["date"])

    plan["weekday"] = plan["date"].dt.weekday
    plan["day_name"] = plan["date"].dt.day_name()

    # Exclude Saturdays always.
    plan = plan[plan["weekday"] != 5].copy()

    # Exclude Sundays unless explicitly requested.
    if not args.include_sundays:
        plan = plan[plan["weekday"] != 6].copy()

    plan = plan.sort_values(["symbol", "date"])

    if args.max_dates > 0:
        plan = plan.head(args.max_dates).copy()

    plan["date"] = plan["date"].dt.strftime("%Y-%m-%d")
    plan["download_stage"] = "processed_ticks"
    plan["plan_status"] = "planned"
    plan["priority"] = range(1, len(plan) + 1)

    plan = plan[
        [
            "symbol",
            "date",
            "day_name",
            "missing_stage",
            "download_stage",
            "priority",
            "plan_status",
        ]
    ]

    batch_plan_path = output_root / "plans" / "dukascopy_batch_download_plan_latest.csv"
    report_path = output_root / "reports" / "dukascopy_trading_day_plan_report_latest.txt"

    plan.to_csv(batch_plan_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY TRADING DAY DOWNLOAD PLAN REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {args.symbol}\n")
        f.write(f"Max dates: {args.max_dates}\n")
        f.write(f"Include Sundays: {args.include_sundays}\n")
        f.write(f"Planned rows: {len(plan):,}\n\n")
        f.write("Plan Preview\n")
        f.write("-" * 80 + "\n")
        f.write(plan.head(50).to_string(index=False))
        f.write("\n\nOutputs:\n")
        f.write(f"Batch plan: {batch_plan_path}\n")

    print(f"Symbol:           {args.symbol}")
    print(f"Max dates:        {args.max_dates}")
    print(f"Include Sundays:  {args.include_sundays}")
    print(f"Planned rows:     {len(plan):,}")
    print("-" * 90)
    print(plan.head(40).to_string(index=False))
    print("=" * 90)
    print("[DONE] Trading-day download plan complete.")
    print(f"Batch plan: {batch_plan_path}")
    print(f"Report:     {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()