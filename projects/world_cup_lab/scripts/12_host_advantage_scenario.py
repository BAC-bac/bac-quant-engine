"""
================================================================================
WORLD CUP LAB
SCRIPT 12 - HOST ADVANTAGE SCENARIO BUILDER
================================================================================

Purpose:
    Create alternative team ratings files with Elo-style host-nation boosts
    for Mexico, United States, and Canada.

Inputs:
    data/world_cup_2026/team_ratings_real_elo.csv

Outputs:
    data/world_cup_2026/team_ratings_host_boost_25.csv
    data/world_cup_2026/team_ratings_host_boost_50.csv
    data/world_cup_2026/team_ratings_host_boost_75.csv
================================================================================
"""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"

INPUT_PATH = WC_DIR / "team_ratings_real_elo.csv"

HOST_TEAMS = [
    "Mexico",
    "United States",
    "Canada",
]

HOST_BOOSTS = [
    25,
    50,
    75,
]


def load_base_ratings():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_PATH}")

    return pd.read_csv(INPUT_PATH)


def recalculate_strength_features(df):
    ratings = df.copy()

    mean_elo = ratings["elo_adjusted"].mean()
    ratings["elo_diff_from_average"] = ratings["elo_adjusted"] - mean_elo

    ratings["attack_strength"] = 1.0 + ratings["elo_diff_from_average"] / 3000
    ratings["defence_strength"] = 1.0 - ratings["elo_diff_from_average"] / 3500

    ratings["attack_strength"] = ratings["attack_strength"].clip(0.80, 1.25)
    ratings["defence_strength"] = ratings["defence_strength"].clip(0.80, 1.25)

    ratings["rating_rank"] = (
        ratings["elo_adjusted"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    ratings = ratings.sort_values(["rating_rank", "team"]).reset_index(drop=True)

    return ratings


def create_host_boost_file(base_ratings, boost):
    df = base_ratings.copy()

    df["host_nation"] = df["team"].isin(HOST_TEAMS)
    df["host_boost_elo"] = 0
    df.loc[df["host_nation"], "host_boost_elo"] = boost

    df["elo_original"] = df["elo"]
    df["elo_adjusted"] = df["elo_original"] + df["host_boost_elo"]

    df = recalculate_strength_features(df)

    # Keep an "elo" column for compatibility with Script 09.
    df["elo"] = df["elo_adjusted"]

    output_path = WC_DIR / f"team_ratings_host_boost_{boost}.csv"
    df.to_csv(output_path, index=False)

    return output_path, df


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 12 - HOST ADVANTAGE SCENARIO BUILDER")
    print("=" * 80)

    base_ratings = load_base_ratings()

    print(f"Base ratings loaded: {len(base_ratings):,}")
    print(f"Host teams: {', '.join(HOST_TEAMS)}")
    print("-" * 80)

    for boost in HOST_BOOSTS:
        output_path, scenario = create_host_boost_file(base_ratings, boost)

        print(f"Host boost scenario: +{boost} Elo")
        print(f"Saved: {output_path}")

        print(
            scenario[
                scenario["team"].isin(HOST_TEAMS)
            ][
                [
                    "team",
                    "elo_original",
                    "host_boost_elo",
                    "elo_adjusted",
                    "rating_rank",
                    "attack_strength",
                    "defence_strength",
                ]
            ].to_string(index=False)
        )

        print("-" * 80)

    print("=" * 80)


if __name__ == "__main__":
    main()
