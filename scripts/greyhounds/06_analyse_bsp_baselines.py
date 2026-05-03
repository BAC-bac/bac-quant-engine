from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def add_roi_columns(report: pd.DataFrame) -> pd.DataFrame:
    report = report.copy()

    report["back_roi"] = report["back_profit_1pt"] / report["bets"]
    report["lay_roi"] = report["lay_profit_1pt"] / report["bets"]
    report["strike_rate"] = report["winners"] / report["bets"]
    report["avg_bsp"] = report["avg_bsp"].round(3)
    report["median_bsp"] = report["median_bsp"].round(3)
    report["back_roi"] = report["back_roi"].round(4)
    report["lay_roi"] = report["lay_roi"].round(4)
    report["strike_rate"] = report["strike_rate"].round(4)

    return report


def build_group_report(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    report = (
        df.groupby(group_cols, dropna=False, observed=False)
        .agg(
            bets=("event_id", "size"),
            races=("event_id", "nunique"),
            winners=("is_winner", "sum"),
            avg_bsp=("bsp", "mean"),
            median_bsp=("bsp", "median"),
            back_profit_1pt=("back_profit_1pt", "sum"),
            lay_profit_1pt=("lay_profit_1pt", "sum"),
        )
        .reset_index()
    )

    report = add_roi_columns(report)
    report = report.sort_values("back_roi", ascending=False)

    return report


def add_bsp_bands(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    bins = [1.01, 2, 3, 5, 8, 13, 21, 34, 55, 1000]
    labels = [
        "01_1.01_to_2.00",
        "02_2.00_to_3.00",
        "03_3.00_to_5.00",
        "04_5.00_to_8.00",
        "05_8.00_to_13.00",
        "06_13.00_to_21.00",
        "07_21.00_to_34.00",
        "08_34.00_to_55.00",
        "09_55.00_plus",
    ]

    df["bsp_band"] = pd.cut(
        df["bsp"],
        bins=bins,
        labels=labels,
        include_lowest=True,
        right=False,
    )

    return df


def save_report(report: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(path, index=False)
    print(f"Saved: {path}")


def main() -> None:
    print("Starting BSP baseline analysis...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    curated_dir = Path(greyhound_paths["curated"])
    reports_dir = Path(greyhound_paths["reports"]) / "bsp_baselines"

    reports_dir.mkdir(parents=True, exist_ok=True)

    input_path = curated_dir / "greyhound_results_uk_analysis_base.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    print(f"Rows loaded: {len(df):,}")
    print(f"Races loaded: {df['event_id'].nunique():,}")

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df["year"] = df["event_dt"].dt.year
    df["month"] = df["event_dt"].dt.month
    df["year_month"] = df["event_dt"].dt.to_period("M").astype(str)

    df = add_bsp_bands(df)

    overall = build_group_report(df.assign(overall="all"), ["overall"])
    trap_roi = build_group_report(df, ["trap"])
    track_roi = build_group_report(df, ["track_key"])
    track_trap_roi = build_group_report(df, ["track_key", "trap"])
    bsp_band_roi = build_group_report(df, ["bsp_band"])
    yearly_roi = build_group_report(df, ["year"])
    monthly_roi = build_group_report(df, ["year_month"])
    track_year_roi = build_group_report(df, ["track_key", "year"])
    trap_bsp_band_roi = build_group_report(df, ["trap", "bsp_band"])
    track_bsp_band_roi = build_group_report(df, ["track_key", "bsp_band"])

    save_report(overall, reports_dir / "overall_roi.csv")
    save_report(trap_roi, reports_dir / "trap_roi.csv")
    save_report(track_roi, reports_dir / "track_roi.csv")
    save_report(track_trap_roi, reports_dir / "track_trap_roi.csv")
    save_report(bsp_band_roi, reports_dir / "bsp_band_roi.csv")
    save_report(yearly_roi, reports_dir / "yearly_roi.csv")
    save_report(monthly_roi, reports_dir / "monthly_roi.csv")
    save_report(track_year_roi, reports_dir / "track_year_roi.csv")
    save_report(trap_bsp_band_roi, reports_dir / "trap_bsp_band_roi.csv")
    save_report(track_bsp_band_roi, reports_dir / "track_bsp_band_roi.csv")

    print("\nBaseline analysis complete.")

    print("\nOverall:")
    print(overall)

    print("\nTop 10 track/trap by back ROI with at least 2,000 bets:")
    print(track_trap_roi[track_trap_roi["bets"] >= 2000].head(10))

    print("\nTop 10 BSP bands by back ROI:")
    print(bsp_band_roi.head(10))


if __name__ == "__main__":
    main()