"""
================================================================================
WORLD CUP LAB
SCRIPT 06 - SIMULATE TOURNAMENT
================================================================================

Purpose:
    Run a simplified tournament simulation using:
    - Elo-derived expected goals
    - Poisson match simulation
    - Group tables
    - Knockout rounds

Inputs:
    data/processed/team_ratings.csv
    data/fixtures/world_cup_fixtures.csv

Outputs:
    outputs/tournament_simulation_summary.csv
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
OUTPUT_PATH = OUTPUT_DIR / "tournament_simulation_summary.csv"

N_TOURNAMENT_SIMULATIONS = 10_000
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


def simulate_match(ratings, home_team, away_team, allow_draw=True):
    home_xg, away_xg = expected_goals(ratings, home_team, away_team)

    home_goals = poisson.rvs(home_xg)
    away_goals = poisson.rvs(away_xg)

    if allow_draw:
        return home_goals, away_goals

    while home_goals == away_goals:
        home_goals = poisson.rvs(home_xg)
        away_goals = poisson.rvs(away_xg)

    return home_goals, away_goals


def initialise_group_table(teams):
    return {
        team: {
            "team": team,
            "played": 0,
            "wins": 0,
            "draws": 0,
            "losses": 0,
            "goals_for": 0,
            "goals_against": 0,
            "goal_difference": 0,
            "points": 0,
        }
        for team in teams
    }


def update_group_table(table, home_team, away_team, home_goals, away_goals):
    table[home_team]["played"] += 1
    table[away_team]["played"] += 1

    table[home_team]["goals_for"] += home_goals
    table[home_team]["goals_against"] += away_goals

    table[away_team]["goals_for"] += away_goals
    table[away_team]["goals_against"] += home_goals

    if home_goals > away_goals:
        table[home_team]["wins"] += 1
        table[away_team]["losses"] += 1
        table[home_team]["points"] += 3

    elif home_goals < away_goals:
        table[away_team]["wins"] += 1
        table[home_team]["losses"] += 1
        table[away_team]["points"] += 3

    else:
        table[home_team]["draws"] += 1
        table[away_team]["draws"] += 1
        table[home_team]["points"] += 1
        table[away_team]["points"] += 1

    for team in [home_team, away_team]:
        table[team]["goal_difference"] = (
            table[team]["goals_for"] - table[team]["goals_against"]
        )


def rank_group_table(table):
    df = pd.DataFrame(table.values())

    df = df.sort_values(
        by=["points", "goal_difference", "goals_for"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    df["group_position"] = df.index + 1

    return df


def simulate_group_stage(ratings, fixtures):
    all_group_winners = []
    all_group_runners_up = []

    for group_name, group_fixtures in fixtures.groupby("group"):
        teams = sorted(
            set(group_fixtures["home_team"]).union(set(group_fixtures["away_team"]))
        )

        table = initialise_group_table(teams)

        for _, fixture in group_fixtures.iterrows():
            home_team = fixture["home_team"]
            away_team = fixture["away_team"]

            home_goals, away_goals = simulate_match(
                ratings=ratings,
                home_team=home_team,
                away_team=away_team,
                allow_draw=True,
            )

            update_group_table(
                table=table,
                home_team=home_team,
                away_team=away_team,
                home_goals=home_goals,
                away_goals=away_goals,
            )

        ranked_table = rank_group_table(table)

        all_group_winners.append(ranked_table.iloc[0]["team"])

        if len(ranked_table) > 1:
            all_group_runners_up.append(ranked_table.iloc[1]["team"])

    qualifiers = all_group_winners + all_group_runners_up

    return qualifiers


def simulate_knockout_round(ratings, teams):
    shuffled_teams = teams.copy()
    np.random.shuffle(shuffled_teams)

    winners = []

    for i in range(0, len(shuffled_teams), 2):
        team_a = shuffled_teams[i]
        team_b = shuffled_teams[i + 1]

        goals_a, goals_b = simulate_match(
            ratings=ratings,
            home_team=team_a,
            away_team=team_b,
            allow_draw=False,
        )

        if goals_a > goals_b:
            winners.append(team_a)
        else:
            winners.append(team_b)

    return winners


def simulate_tournament_once(ratings, fixtures):
    qualifiers = simulate_group_stage(ratings, fixtures)

    # Safety check: this simplified model needs an even number of qualifiers.
    if len(qualifiers) % 2 != 0:
        qualifiers = qualifiers[:-1]

    current_round = qualifiers

    while len(current_round) > 1:
        if len(current_round) % 2 != 0:
            np.random.shuffle(current_round)
            current_round = current_round[:-1]

        current_round = simulate_knockout_round(ratings, current_round)

    return current_round[0]


def main():
    np.random.seed(RANDOM_SEED)

    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 06 - SIMULATE TOURNAMENT")
    print("=" * 80)

    ratings = load_ratings()
    fixtures = load_fixtures()

    print(f"Ratings loaded:                {len(ratings):,}")
    print(f"Fixtures loaded:               {len(fixtures):,}")
    print(f"Tournament simulations:        {N_TOURNAMENT_SIMULATIONS:,}")
    print("-" * 80)

    winners = []

    for i in range(N_TOURNAMENT_SIMULATIONS):
        winner = simulate_tournament_once(ratings, fixtures)
        winners.append(winner)

        if (i + 1) % 1000 == 0:
            print(f"Completed simulations: {i + 1:,}")

    summary = (
        pd.Series(winners)
        .value_counts(normalize=True)
        .reset_index()
    )

    summary.columns = ["team", "winner_probability"]
    summary["winner_probability_pct"] = summary["winner_probability"] * 100

    summary = summary.sort_values(
        by="winner_probability",
        ascending=False,
    ).reset_index(drop=True)

    summary.to_csv(OUTPUT_PATH, index=False)

    print("-" * 80)
    print("TOURNAMENT WINNER PROBABILITIES")
    print("-" * 80)
    print(summary.head(20))
    print("-" * 80)
    print(f"Output saved: {OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
