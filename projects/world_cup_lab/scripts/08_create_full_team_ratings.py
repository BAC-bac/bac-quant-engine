"""
================================================================================
WORLD CUP LAB
SCRIPT 08 - CREATE FULL TEAM RATINGS
================================================================================

Purpose:
    Create Elo-style ratings for all 48 teams in the structured World Cup dataset.

Inputs:
    data/world_cup_2026/teams.csv

Outputs:
    data/world_cup_2026/team_ratings_full.csv
================================================================================
"""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"

TEAMS_PATH = WC_DIR / "teams.csv"
OUTPUT_PATH = WC_DIR / "team_ratings_full.csv"


MANUAL_ELO_RATINGS = {
    "Spain": 2155,
    "Argentina": 2113,
    "France": 2062,
    "England": 2020,
    "Brazil": 1988,
    "Portugal": 1986,
    "Colombia": 1982,
    "Netherlands": 1948,
    "Germany": 1932,
    "Belgium": 1908,
    "Croatia": 1895,
    "Uruguay": 1888,
    "Morocco": 1865,
    "Switzerland": 1845,
    "Austria": 1835,
    "Ecuador": 1820,
    "Mexico": 1800,
    "United States": 1780,
    "Senegal": 1775,
    "Canada": 1765,
    "Japan": 1755,
    "Norway": 1745,
    "South Korea": 1725,
    "Australia": 1705,
    "Scotland": 1695,
    "Turkey": 1685,
    "Ivory Coast": 1675,
    "Czechia": 1665,
    "Egypt": 1660,
    "South Africa": 1650,
    "Iran": 1645,
    "Sweden": 1640,
    "Qatar": 1625,
    "Tunisia": 1615,
    "Algeria": 1605,
    "Ghana": 1595,
    "Saudi Arabia": 1585,
    "New Zealand": 1575,
    "Bosnia and Herzegovina": 1565,
    "Panama": 1555,
    "Paraguay": 1545,
    "Uzbekistan": 1535,
    "Cape Verde": 1525,
    "Haiti": 1515,
    "Jordan": 1505,
    "Iraq": 1495,
    "DR Congo": 1485,
    "Curacao": 1475,
}


def load_teams():
    return pd.read_csv(TEAMS_PATH)


def assign_ratings(teams_df):
    df = teams_df.copy()

    df["elo"] = df["team"].map(MANUAL_ELO_RATINGS)

    missing = df[df["elo"].isna()]["team"].tolist()

    if missing:
        raise ValueError(f"Missing Elo ratings for teams: {missing}")

    df["elo"] = df["elo"].astype(int)

    mean_elo = df["elo"].mean()

    df["elo_diff_from_average"] = df["elo"] - mean_elo

    df["attack_strength"] = 1.0 + (df["elo_diff_from_average"] / 1000)
    df["defence_strength"] = 1.0 - (df["elo_diff_from_average"] / 1200)

    df["attack_strength"] = df["attack_strength"].clip(lower=0.70, upper=1.40)
    df["defence_strength"] = df["defence_strength"].clip(lower=0.70, upper=1.40)

    df["rating_rank"] = (
        df["elo"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    df = df.sort_values(["rating_rank", "team"]).reset_index(drop=True)

    return df


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 08 - CREATE FULL TEAM RATINGS")
    print("=" * 80)

    teams = load_teams()
    ratings = assign_ratings(teams)

    ratings.to_csv(OUTPUT_PATH, index=False)

    print(f"Teams loaded:   {len(teams):,}")
    print(f"Ratings saved:  {len(ratings):,} -> {OUTPUT_PATH}")

    print("-" * 80)
    print("Top 15 teams:")
    print(
        ratings[
            [
                "rating_rank",
                "team",
                "group",
                "elo",
                "attack_strength",
                "defence_strength",
            ]
        ].head(15)
    )

    print("-" * 80)
    print("Bottom 10 teams:")
    print(
        ratings[
            [
                "rating_rank",
                "team",
                "group",
                "elo",
                "attack_strength",
                "defence_strength",
            ]
        ].tail(10)
    )

    print("=" * 80)


if __name__ == "__main__":
    main()
