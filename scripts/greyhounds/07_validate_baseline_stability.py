from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


MIN_BETS_YEAR = 200
MIN_BETS_MONTH = 30


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def add_roi_columns(report: pd.DataFrame) -> pd.DataFrame:
    report = report.copy()

    report["back_roi"] = report["back_profit_1pt"] / report["bets"]
    report["lay_roi"] = report["lay_profit_1pt"] / report["bets"]
    report["strike_rate"] = report["winners"] / report["bets"]

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


def create_stability_summary(report: pd.DataFrame, candidate_cols: list[str], period_col: str, min_bets: int) -> pd.DataFrame:
    valid = report[report["bets"] >= min_bets].copy()

    summary = (
        valid.groupby(candidate_cols, dropna=False, observed=False)
        .agg(
            periods_tested=(period_col, "nunique"),
            total_bets=("bets", "sum"),
            total_profit=("back_profit_1pt", "sum"),
            avg_period_roi=("back_roi", "mean"),
            median_period_roi=("back_roi", "median"),
            min_period_roi=("back_roi", "min"),
            max_period_roi=("back_roi", "max"),
            positive_periods=("back_roi", lambda x: (x > 0).sum()),
            negative_periods=("back_roi", lambda x: (x < 0).sum()),
        )
        .reset_index()
    )

    summary["overall_roi"] = summary["total_profit"] / summary["total_bets"]
    summary["positive_period_rate"] = summary["positive_periods"] / summary["periods_tested"]

    summary = summary.sort_values(
        ["overall_roi", "positive_period_rate", "total_bets"],
        ascending=[False, False, False],
    )

    return summary


def save_report(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Saved: {path}")


def main() -> None:
    print("Starting baseline stability validation...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    curated_dir = Path(greyhound_paths["curated"])
    reports_dir = Path(greyhound_paths["reports"]) / "stability"

    reports_dir.mkdir(parents=True, exist_ok=True)

    input_path = curated_dir / "greyhound_results_uk_analysis_base.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    print(f"Rows loaded: {len(df):,}")
    print(f"Races loaded: {df['event_id'].nunique():,}")

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df["year"] = df["event_dt"].dt.year
    df["year_month"] = df["event_dt"].dt.to_period("M").astype(str)

    df = add_bsp_bands(df)

    track_trap_year = build_group_report(df, ["track_key", "trap", "year"])
    track_trap_month = build_group_report(df, ["track_key", "trap", "year_month"])

    bsp_band_year = build_group_report(df, ["bsp_band", "year"])
    bsp_band_month = build_group_report(df, ["bsp_band", "year_month"])

    trap_year = build_group_report(df, ["trap", "year"])
    trap_month = build_group_report(df, ["trap", "year_month"])

    track_year = build_group_report(df, ["track_key", "year"])
    track_month = build_group_report(df, ["track_key", "year_month"])

    track_trap_year_summary = create_stability_summary(
        track_trap_year,
        ["track_key", "trap"],
        "year",
        MIN_BETS_YEAR,
    )

    track_trap_month_summary = create_stability_summary(
        track_trap_month,
        ["track_key", "trap"],
        "year_month",
        MIN_BETS_MONTH,
    )

    bsp_band_year_summary = create_stability_summary(
        bsp_band_year,
        ["bsp_band"],
        "year",
        MIN_BETS_YEAR,
    )

    bsp_band_month_summary = create_stability_summary(
        bsp_band_month,
        ["bsp_band"],
        "year_month",
        MIN_BETS_MONTH,
    )

    trap_year_summary = create_stability_summary(
        trap_year,
        ["trap"],
        "year",
        MIN_BETS_YEAR,
    )

    trap_month_summary = create_stability_summary(
        trap_month,
        ["trap"],
        "year_month",
        MIN_BETS_MONTH,
    )

    track_year_summary = create_stability_summary(
        track_year,
        ["track_key"],
        "year",
        MIN_BETS_YEAR,
    )

    track_month_summary = create_stability_summary(
        track_month,
        ["track_key"],
        "year_month",
        MIN_BETS_MONTH,
    )

    save_report(track_trap_year, reports_dir / "track_trap_year_detail.csv")
    save_report(track_trap_month, reports_dir / "track_trap_month_detail.csv")
    save_report(track_trap_year_summary, reports_dir / "track_trap_year_summary.csv")
    save_report(track_trap_month_summary, reports_dir / "track_trap_month_summary.csv")

    save_report(bsp_band_year, reports_dir / "bsp_band_year_detail.csv")
    save_report(bsp_band_month, reports_dir / "bsp_band_month_detail.csv")
    save_report(bsp_band_year_summary, reports_dir / "bsp_band_year_summary.csv")
    save_report(bsp_band_month_summary, reports_dir / "bsp_band_month_summary.csv")

    save_report(trap_year_summary, reports_dir / "trap_year_summary.csv")
    save_report(trap_month_summary, reports_dir / "trap_month_summary.csv")
    save_report(track_year_summary, reports_dir / "track_year_summary.csv")
    save_report(track_month_summary, reports_dir / "track_month_summary.csv")

    print("\nStability validation complete.")

    print("\nTop 15 track/trap yearly stability:")
    print(track_trap_year_summary.head(15).round(4))

    print("\nTop 15 track/trap monthly stability:")
    print(track_trap_month_summary.head(15).round(4))

    print("\nBSP band yearly stability:")
    print(bsp_band_year_summary.round(4))

    print("\nBSP band monthly stability:")
    print(bsp_band_month_summary.round(4))


if __name__ == "__main__":
    main()