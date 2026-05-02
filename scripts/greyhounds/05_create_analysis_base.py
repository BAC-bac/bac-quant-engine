from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def main() -> None:
    print("Starting UK greyhound analysis base creation...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    curated_dir = Path(greyhound_paths["curated"])
    reports_dir = Path(greyhound_paths["reports"])

    reports_dir.mkdir(parents=True, exist_ok=True)

    input_path = curated_dir / "greyhound_results_uk.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    print(f"Rows loaded: {len(df):,}")

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df["race_date"] = df["event_dt"].dt.date
    df["year"] = df["event_dt"].dt.year
    df["month"] = df["event_dt"].dt.month
    df["day_of_week"] = df["event_dt"].dt.day_name()
    df["hour"] = df["event_dt"].dt.hour

    df["trap"] = pd.to_numeric(df["trap"], errors="coerce")
    df["bsp"] = pd.to_numeric(df["bsp"], errors="coerce")

    before_filter = len(df)

    df = df.dropna(subset=["event_id", "event_dt", "track_key", "trap", "bsp"]).copy()
    df = df[df["trap"].between(1, 6)].copy()
    df = df[df["bsp"] > 1.01].copy()

    after_filter = len(df)

    print(f"Rows before analysis filter: {before_filter:,}")
    print(f"Rows after analysis filter: {after_filter:,}")
    print(f"Rows removed: {before_filter - after_filter:,}")

    df["trap"] = df["trap"].astype(int)
    df["is_winner"] = df["is_winner"].astype(bool)

    df["implied_probability"] = 1 / df["bsp"]

    # Backing every runner at BSP with 1 point stake.
    # Winner profit = BSP - 1
    # Loser loss = -1
    df["back_profit_1pt"] = df.apply(
        lambda row: row["bsp"] - 1 if row["is_winner"] else -1,
        axis=1,
    )

    # Laying every runner at BSP with 1 point liability style approximation:
    # Winner loses BSP - 1
    # Loser wins 1
    df["lay_profit_1pt"] = df.apply(
        lambda row: -(row["bsp"] - 1) if row["is_winner"] else 1,
        axis=1,
    )

    # Event-level runner count after filtering
    runner_counts = (
        df.groupby("event_id")
        .size()
        .rename("runner_count")
        .reset_index()
    )

    df = df.merge(runner_counts, on="event_id", how="left")

    output_cols = [
        "event_id",
        "event_dt",
        "race_date",
        "year",
        "month",
        "day_of_week",
        "hour",
        "track_key",
        "race_time",
        "trap",
        "dog_clean",
        "selection_id",
        "selection_name",
        "bsp",
        "implied_probability",
        "is_winner",
        "back_profit_1pt",
        "lay_profit_1pt",
        "runner_count",
        "source_file",
    ]

    available_cols = [col for col in output_cols if col in df.columns]
    analysis_df = df[available_cols].copy()

    output_path = curated_dir / "greyhound_results_uk_analysis_base.parquet"
    sample_path = curated_dir / "greyhound_results_uk_analysis_base_sample.csv"

    analysis_df.to_parquet(output_path, index=False)
    analysis_df.head(10000).to_csv(sample_path, index=False)

    summary = pd.DataFrame(
        [
            {
                "rows": len(analysis_df),
                "unique_events": analysis_df["event_id"].nunique(),
                "unique_tracks": analysis_df["track_key"].nunique(),
                "date_min": analysis_df["event_dt"].min(),
                "date_max": analysis_df["event_dt"].max(),
                "winner_rows": int(analysis_df["is_winner"].sum()),
                "winner_rate": analysis_df["is_winner"].mean(),
                "avg_bsp": analysis_df["bsp"].mean(),
                "median_bsp": analysis_df["bsp"].median(),
                "total_back_profit_1pt": analysis_df["back_profit_1pt"].sum(),
                "back_roi_1pt": analysis_df["back_profit_1pt"].sum() / len(analysis_df),
                "total_lay_profit_1pt": analysis_df["lay_profit_1pt"].sum(),
                "lay_roi_1pt": analysis_df["lay_profit_1pt"].sum() / len(analysis_df),
            }
        ]
    )

    summary_path = reports_dir / "analysis_base_summary.csv"
    summary.to_csv(summary_path, index=False)

    print("\nAnalysis base creation complete.")
    print(f"Saved analysis base to: {output_path}")
    print(f"Saved sample to: {sample_path}")
    print(f"Saved summary to: {summary_path}")

    print("\nSummary:")
    print(summary)


if __name__ == "__main__":
    main()