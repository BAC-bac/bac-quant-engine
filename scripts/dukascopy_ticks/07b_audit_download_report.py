from pathlib import Path
import argparse
import pandas as pd

DEFAULT_SYMBOL = "EURUSD"
DEFAULT_START_DATE = "2024-01-01"
DEFAULT_END_DATE = "2024-01-31"

DOWNLOAD_REPORT_ROOT = (
    Path(r"E:\Quant_Lab\data\analysis\dukascopy_ticks\download_reports")
)


def build_report_path(
    symbol: str,
    start_date: str,
    end_date: str,
) -> Path:
    return (
        DOWNLOAD_REPORT_ROOT
        / f"{symbol}_{start_date}_to_{end_date}_download_report.csv"
    )

def run_download_report_audit(
    symbol: str = DEFAULT_SYMBOL,
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
) -> None:
    symbol = symbol.upper().strip()

    report_path = build_report_path(symbol, start_date, end_date, )

    print("=" * 90)
    print("BACQE DUKASCOPY 07B - AUDIT DOWNLOAD REPORT")
    print("=" * 90)

    if not report_path.exists():
        print(f"[STOP] Missing report: {report_path}")
        return

    df = pd.read_csv(report_path)

    df["date"] = pd.to_datetime(df["date"])
    df["weekday"] = df["date"].dt.day_name()

    print("\n[STATUS COUNTS]")
    print(df["status"].value_counts(dropna=False))

    print("\n[STATUS BY WEEKDAY]")
    print(pd.crosstab(df["weekday"], df["status"]))

    print("\n[FAILED BY DATE]")
    failed_by_date = (
        df[df["status"] == "failed"]
        .groupby(["date", "weekday"])
        .size()
        .reset_index(name="failed_hours")
    )
    print(failed_by_date.to_string(index=False))

    print("\n[ERROR COUNTS]")
    print(df[df["status"] == "failed"]["error"].value_counts(dropna=False).head(20))

    print("\n[DONE] Download report audit complete.")

    print(f"\nReport: {report_path}")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit a Dukascopy download report."
    )

    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_download_report_audit(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=args.end_date,
    )


if __name__ == "__main__":
    main()