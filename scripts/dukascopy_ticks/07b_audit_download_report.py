from pathlib import Path
import pandas as pd

REPORT_PATH = Path(
    r"E:\Quant_Lab\data\analysis\dukascopy_ticks\download_reports"
    r"\EURUSD_2024-01-01_to_2024-01-31_download_report.csv"
)

def main():
    print("=" * 90)
    print("BACQE DUKASCOPY 07B - AUDIT DOWNLOAD REPORT")
    print("=" * 90)

    df = pd.read_csv(REPORT_PATH)

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

if __name__ == "__main__":
    main()