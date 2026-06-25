"""
================================================================================
WORLD CUP LAB
SCRIPT 09 - SIMULATE FULL TOURNAMENT
================================================================================

Purpose:
    Simulate the full 48-team tournament structure using:
    - 12 groups of 4
    - 72 group-stage fixtures
    - Top 2 teams from each group qualify
    - Best 8 third-placed teams also qualify
    - 32-team knockout stage

Inputs:
    data/world_cup_2026/team_ratings_full.csv
    data/world_cup_2026/fixtures.csv

Outputs:
    outputs/full_tournament_simulation_summary.csv
================================================================================
"""

from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RATINGS_PATH = WC_DIR / "team_ratings_live_adjusted.csv"
FIXTURES_PATH = WC_DIR / "fixtures.csv"
OUTPUT_PATH = OUTPUT_DIR / "full_tournament_simulation_summary.csv"

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


def initialise_table(teams):
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


def update_table(table, home_team, away_team, home_goals, away_goals):
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


def rank_table(table):
    df = pd.DataFrame(table.values())

    # Random tie-breaker prevents identical teams always being ordered alphabetically.
    df["tie_breaker"] = np.random.random(len(df))

    df = df.sort_values(
        by=["points", "goal_difference", "goals_for", "tie_breaker"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    df["group_position"] = df.index + 1

    return df


def simulate_group_stage(ratings, fixtures):
    group_tables = []

    for group_name, group_fixtures in fixtures.groupby("group"):
        teams = sorted(
            set(group_fixtures["home_team"]).union(set(group_fixtures["away_team"]))
        )

        table = initialise_table(teams)

        for _, fixture in group_fixtures.iterrows():
            home_team = fixture["home_team"]
            away_team = fixture["away_team"]

            home_goals, away_goals = simulate_match(
                ratings=ratings,
                home_team=home_team,
                away_team=away_team,
                allow_draw=True,
            )

            update_table(
                table=table,
                home_team=home_team,
                away_team=away_team,
                home_goals=home_goals,
                away_goals=away_goals,
            )

        ranked = rank_table(table)
        ranked["group"] = group_name
        group_tables.append(ranked)

    return pd.concat(group_tables, ignore_index=True)


def select_qualifiers(group_table):
    top_two = group_table[group_table["group_position"] <= 2].copy()

    third_placed = group_table[group_table["group_position"] == 3].copy()

    best_thirds = third_placed.sort_values(
        by=["points", "goal_difference", "goals_for", "tie_breaker"],
        ascending=[False, False, False, False],
    ).head(8)

    qualifiers = pd.concat([top_two, best_thirds], ignore_index=True)

    return qualifiers["team"].tolist()


def simulate_knockout_round(ratings, teams):
    teams = teams.copy()
    np.random.shuffle(teams)

    winners = []

    for i in range(0, len(teams), 2):
        team_a = teams[i]
        team_b = teams[i + 1]

        goals_a, goals_b = simulate_match(
            ratings=ratings,
            home_team=team_a,
            away_team=team_b,
            allow_draw=False,
        )

        winner = team_a if goals_a > goals_b else team_b
        winners.append(winner)

    return winners


def simulate_tournament_once(ratings, fixtures):
    stage_reached = {}

    group_table = simulate_group_stage(ratings, fixtures)
    qualifiers = select_qualifiers(group_table)

    for team in qualifiers:
        stage_reached[team] = "round_of_32"

    round_of_16 = simulate_knockout_round(ratings, qualifiers)
    for team in round_of_16:
        stage_reached[team] = "round_of_16"

    quarter_finalists = simulate_knockout_round(ratings, round_of_16)
    for team in quarter_finalists:
        stage_reached[team] = "quarter_final"

    semi_finalists = simulate_knockout_round(ratings, quarter_finalists)
    for team in semi_finalists:
        stage_reached[team] = "semi_final"

    finalists = simulate_knockout_round(ratings, semi_finalists)
    for team in finalists:
        stage_reached[team] = "final"

    champion = simulate_knockout_round(ratings, finalists)[0]
    stage_reached[champion] = "winner"

    return group_table, qualifiers, quarter_finalists, semi_finalists, finalists, champion


def main():
    np.random.seed(RANDOM_SEED)

    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 09 - SIMULATE FULL TOURNAMENT")
    print("=" * 80)

    ratings = load_ratings()
    fixtures = load_fixtures()

    all_teams = ratings.index.tolist()

    summary = {
        team: {
            "team": team,
            "group_winner_count": 0,
            "qualification_count": 0,
            "quarter_final_count": 0,
            "semi_final_count": 0,
            "final_count": 0,
            "winner_count": 0,
        }
        for team in all_teams
    }

    print(f"Teams loaded:           {len(ratings):,}")
    print(f"Fixtures loaded:        {len(fixtures):,}")
    print(f"Tournament simulations: {N_SIMULATIONS:,}")
    print("-" * 80)

    for simulation_number in range(1, N_SIMULATIONS + 1):
        (
            group_table,
            qualifiers,
            quarter_finalists,
            semi_finalists,
            finalists,
            champion,
        ) = simulate_tournament_once(ratings, fixtures)

        group_winners = group_table[group_table["group_position"] == 1]["team"].tolist()

        for team in group_winners:
            summary[team]["group_winner_count"] += 1

        for team in qualifiers:
            summary[team]["qualification_count"] += 1

        for team in quarter_finalists:
            summary[team]["quarter_final_count"] += 1

        for team in semi_finalists:
            summary[team]["semi_final_count"] += 1

        for team in finalists:
            summary[team]["final_count"] += 1

        summary[champion]["winner_count"] += 1

        if simulation_number % 1000 == 0:
            print(f"Completed simulations: {simulation_number:,}")

    results = pd.DataFrame(summary.values())

    count_cols = [
        "group_winner_count",
        "qualification_count",
        "quarter_final_count",
        "semi_final_count",
        "final_count",
        "winner_count",
    ]

    for col in count_cols:
        prob_col = col.replace("_count", "_probability")
        pct_col = col.replace("_count", "_probability_pct")

        results[prob_col] = results[col] / N_SIMULATIONS
        results[pct_col] = results[prob_col] * 100

    results = results.sort_values(
        by="winner_probability",
        ascending=False,
    ).reset_index(drop=True)

    results.to_csv(OUTPUT_PATH, index=False)

    print("-" * 80)
    print("TOP 20 WINNER PROBABILITIES")
    print("-" * 80)
    print(
        results[
            [
                "team",
                "qualification_probability_pct",
                "quarter_final_probability_pct",
                "semi_final_probability_pct",
                "final_probability_pct",
                "winner_probability_pct",
            ]
        ].head(20)
    )

    print("-" * 80)
    print(f"Output saved: {OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
