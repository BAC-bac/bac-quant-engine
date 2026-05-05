from pathlib import Path
import numpy as np
import pandas as pd

INPUT_PATH = Path("/mnt/quant_lab/curated/results_curated.parquet")
OUTPUT_PATH = Path("/mnt/quant_lab/curated/race_features.parquet")
SUMMARY_PATH = Path("/mnt/quant_lab/analysis/race_feature_summary.csv")


def safe_implied_prob(series: pd.Series) -> pd.Series:
    series = pd.to_numeric(series, errors="coerce")
    return np.where(series > 0, 1.0 / series, np.nan)


def add_buckets(df: pd.DataFrame) -> pd.DataFrame:
    df["bsp_bucket"] = pd.cut(
        df["bsp"],
        bins=[0, 2, 3, 4, 6, 10, 20, 50, 100, np.inf],
        labels=["0-2", "2-3", "3-4", "4-6", "6-10", "10-20", "20-50", "50-100", "100+"],
        right=False,
    )

    df["field_size_bucket"] = pd.cut(
        df["field_size"],
        bins=[0, 4, 6, 8, 10, 20],
        labels=["1-4", "5-6", "7-8", "9-10", "11+"],
        right=True,
    )

    df["rank_bucket"] = pd.cut(
        df["rank_bsp"],
        bins=[0, 1, 2, 3, 4, 6, 20],
        labels=["1", "2", "3", "4", "5-6", "7+"],
        right=True,
    )

    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Numeric coercion
    for col in ["bsp", "ppwap", "morningwap", "pptradedvol", "iptradedvol", "win_flag"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Implied probabilities
    df["implied_prob_bsp"] = safe_implied_prob(df["bsp"])
    df["implied_prob_ppwap"] = safe_implied_prob(df["ppwap"])
    df["implied_prob_morningwap"] = safe_implied_prob(df["morningwap"])

    # Group by race
    race_group = df.groupby("event_id", dropna=False)

    # Field size
    df["field_size"] = race_group["selection_id"].transform("count")

    # Market overround
    df["market_overround_bsp"] = race_group["implied_prob_bsp"].transform("sum")
    df["market_overround_ppwap"] = race_group["implied_prob_ppwap"].transform("sum")
    df["market_overround_morningwap"] = race_group["implied_prob_morningwap"].transform("sum")

    # Ranks
    df["rank_bsp"] = race_group["bsp"].rank(method="first", ascending=True)
    df["rank_ppwap"] = race_group["ppwap"].rank(method="first", ascending=True)
    df["rank_morningwap"] = race_group["morningwap"].rank(method="first", ascending=True)

    # Favourite flags
    df["favourite_flag"] = (df["rank_bsp"] == 1).astype(int)
    df["second_fav_flag"] = (df["rank_bsp"] == 2).astype(int)
    df["third_fav_flag"] = (df["rank_bsp"] == 3).astype(int)

    # Relative BSP features
    df["race_avg_bsp"] = race_group["bsp"].transform("mean")
    df["race_min_bsp"] = race_group["bsp"].transform("min")
    df["race_max_bsp"] = race_group["bsp"].transform("max")

    df["bsp_vs_race_avg"] = df["bsp"] / df["race_avg_bsp"]
    df["bsp_vs_race_min"] = df["bsp"] / df["race_min_bsp"]
    df["bsp_gap_from_fav"] = df["bsp"] - df["race_min_bsp"]

    # Probability share within market
    df["prob_share_bsp"] = df["implied_prob_bsp"] / df["market_overround_bsp"]

    # Traded volume relative to race
    if "pptradedvol" in df.columns:
        df["race_total_pptradedvol"] = race_group["pptradedvol"].transform("sum")
        df["pptradedvol_share"] = df["pptradedvol"] / df["race_total_pptradedvol"]

    # Win sanity check
    df["race_winner_count"] = race_group["win_flag"].transform("sum")

    # Add simple buckets
    df = add_buckets(df)

    return df


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = pd.DataFrame({
        "metric": [
            "rows",
            "races",
            "date_min",
            "date_max",
            "avg_field_size",
            "avg_overround_bsp",
            "avg_bsp",
            "win_rate",
            "favourite_win_rate",
        ],
        "value": [
            len(df),
            df["event_id"].nunique(),
            str(df["race_date"].min()),
            str(df["race_date"].max()),
            df["field_size"].mean(),
            df["market_overround_bsp"].mean(),
            df["bsp"].mean(),
            df["win_flag"].mean(),
            df.loc[df["favourite_flag"] == 1, "win_flag"].mean(),
        ]
    })
    return summary


def main() -> None:
    df = pd.read_parquet(INPUT_PATH)

    # Keep only mapped tracks for structured analysis first
    df = df[df["track_key"].notna()].copy()

    # Optional: keep only rows with core fields
    df = df[
        df["event_id"].notna() &
        df["selection_id"].notna() &
        df["bsp"].notna()
    ].copy()

    features = build_features(df)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)

    features.to_parquet(OUTPUT_PATH, index=False)

    summary = build_summary(features)
    summary.to_csv(SUMMARY_PATH, index=False)

    print(f"Saved race features: {OUTPUT_PATH}")
    print(f"Saved summary: {SUMMARY_PATH}")
    print(f"Rows: {len(features):,}")
    print(f"Races: {features['event_id'].nunique():,}")
    print(f"Date range: {features['race_date'].min()} -> {features['race_date'].max()}")
    print(f"Average field size: {features['field_size'].mean():.2f}")
    print(f"Average BSP overround: {features['market_overround_bsp'].mean():.4f}")

    print("\nSample rows:")
    print(
        features[
            [
                "race_date", "event_id", "track_key", "dog_clean", "bsp",
                "rank_bsp", "favourite_flag", "field_size",
                "market_overround_bsp", "prob_share_bsp", "win_flag"
            ]
        ].head(10).to_string(index=False)
    )


if __name__ == "__main__":
    main()