"""
================================================================================
WORLD CUP LAB
SCRIPT 05 - SIMULATE FIXTURES
================================================================================

Purpose:
    Simulate each fixture many times using Poisson-generated scorelines.

Inputs:
    data/processed/team_ratings.csv
    data/fixtures/world_cup_fixtures.csv

Outputs:
    outputs/fixture_simulation_summary.csv
================================================================================
"""

from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson


PROJECT_ROOT = Path(__file__).resolve().parents[1]

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
FIXTURES_DIR = PROJECT_ROOT / "data" / "fixtures"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RATINGS_PATH = PROCESSED_DIR / "team_ratings.csv"
FIXTURES_PATH = FIXTURES_DIR / "world_cup_fixtures.csv"
OUTPUT_PATH = OUTPUT_DIR / "fixture_simulation_summary.csv"

N_SIMULATIONS = 10_000
RANDOM_SEED = 42


def load_ratings():
    return pd.read_csv(RATINGS_PATH).set_index("team")


def load_fixtures():
    return pd.read_csv(FIXTURES_PATH)


def expected_goals(ratings, home_team, away_team, base_goals=1.35):
    home_attack = ratings.loc[home_team, "attack_strength"]
    home_defence = ratings.loc[home_team, "defence_strength"]

    away_attack = ratings.loc[away_team, "attack_strength"]
    away_defence = ratings.loc[away_team, "defence_strength"]

    home_xg = base_goals * home_attack * away_defence
    away_xg = base_goals * away_attack * home_defence

    return home_xg, away_xg


def simulate_single_fixture(ratings, fixture, n_simulations=N_SIMULATIONS):
    fixture_id = fixture["fixture_id"]
    date = fixture["date"]
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]

    home_xg, away_xg = expected_goals(
        ratings=ratings,
        home_team=home_team,
        away_team=away_team,
    )

    home_goals = poisson.rvs(home_xg, size=n_simulations)
    away_goals = poisson.rvs(away_xg, size=n_simulations)

    simulation_df = pd.DataFrame(
        {
            "home_goals": home_goals,
            "away_goals": away_goals,
        }
    )

    simulation_df["total_goals"] = (
        simulation_df["home_goals"] + simulation_df["away_goals"]
    )

    simulation_df["scoreline"] = (
        simulation_df["home_goals"].astype(str)
        + "-"
        + simulation_df["away_goals"].astype(str)
    )

    simulation_df["result"] = np.select(
        [
            simulation_df["home_goals"] > simulation_df["away_goals"],
            simulation_df["home_goals"] == simulation_df["away_goals"],
            simulation_df["home_goals"] < simulation_df["away_goals"],
        ],
        [
            "home_win",
            "draw",
            "away_win",
        ],
        default="unknown",
    )

    home_win_prob = (simulation_df["result"] == "home_win").mean()
    draw_prob = (simulation_df["result"] == "draw").mean()
    away_win_prob = (simulation_df["result"] == "away_win").mean()

    over_15_prob = (simulation_df["total_goals"] > 1.5).mean()
    over_25_prob = (simulation_df["total_goals"] > 2.5).mean()
    over_35_prob = (simulation_df["total_goals"] > 3.5).mean()

    btts_prob = (
        (simulation_df["home_goals"] > 0)
        & (simulation_df["away_goals"] > 0)
    ).mean()

    most_likely_score = (
        simulation_df["scoreline"]
        .value_counts(normalize=True)
        .reset_index()
        .iloc[0]
    )

    return {
        "fixture_id": fixture_id,
        "date": date,
        "home_team": home_team,
        "away_team": away_team,
        "home_xg": home_xg,
        "away_xg": away_xg,
        "home_win_prob": home_win_prob,
        "draw_prob": draw_prob,
        "away_win_prob": away_win_prob,
        "over_15_prob": over_15_prob,
        "over_25_prob": over_25_prob,
        "over_35_prob": over_35_prob,
        "btts_prob": btts_prob,
        "most_likely_score": most_likely_score["scoreline"],
        "most_likely_score_prob": most_likely_score["proportion"],
    }


def main():
    np.random.seed(RANDOM_SEED)

    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 05 - SIMULATE FIXTURES")
    print("=" * 80)

    ratings = load_ratings()
    fixtures = load_fixtures()

    print(f"Ratings loaded:      {len(ratings):,}")
    print(f"Fixtures loaded:     {len(fixtures):,}")
    print(f"Simulations/match:   {N_SIMULATIONS:,}")
    print("-" * 80)

    results = []

    for _, fixture in fixtures.iterrows():
        home_team = fixture["home_team"]
        away_team = fixture["away_team"]

        if home_team not in ratings.index:
            raise ValueError(f"Missing home team in ratings file: {home_team}")

        if away_team not in ratings.index:
            raise ValueError(f"Missing away team in ratings file: {away_team}")

        result = simulate_single_fixture(ratings, fixture)
        results.append(result)

        print(
            f"{home_team} v {away_team} | "
            f"H {result['home_win_prob']:.2%} | "
            f"D {result['draw_prob']:.2%} | "
            f"A {result['away_win_prob']:.2%} | "
            f"O2.5 {result['over_25_prob']:.2%} | "
            f"BTTS {result['btts_prob']:.2%} | "
            f"MLS {result['most_likely_score']}"
        )

    results_df = pd.DataFrame(results)

    for col in [
        "home_win_prob",
        "draw_prob",
        "away_win_prob",
        "over_15_prob",
        "over_25_prob",
        "over_35_prob",
        "btts_prob",
        "most_likely_score_prob",
    ]:
        results_df[f"{col}_pct"] = results_df[col] * 100

    results_df.to_csv(OUTPUT_PATH, index=False)

    print("-" * 80)
    print(f"Output saved: {OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
