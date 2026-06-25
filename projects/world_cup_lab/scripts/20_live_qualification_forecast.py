"""
================================================================================
WORLD CUP LAB
SCRIPT 20 - LIVE QUALIFICATION FORECAST
================================================================================

Purpose:
    Recalculate qualification probabilities from the current group state by
    simulating only the remaining group fixtures.

Inputs:
    data/world_cup_2026/fixtures.csv
    data/world_cup_2026/actual_results.csv
    data/world_cup_2026/team_ratings_live_adjusted.csv

Outputs:
    outputs/live_qualification_forecast.csv
    outputs/live_qualification_forecast_summary.txt
================================================================================
"""

from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

FIXTURES_PATH = WC_DIR / "fixtures.csv"
ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
RATINGS_PATH = WC_DIR / "team_ratings_live_adjusted.csv"

FORECAST_OUTPUT_PATH = OUTPUT_DIR / "live_qualification_forecast.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "live_qualification_forecast_summary.txt"

N_SIMULATIONS = 10_000
RANDOM_SEED = 42


def load_data():
    fixtures = pd.read_csv(FIXTURES_PATH)
    actuals = pd.read_csv(ACTUAL_RESULTS_PATH)
    ratings = pd.read_csv(RATINGS_PATH).set_index("team")

    return fixtures, actuals, ratings


def expected_goals(ratings, home_team, away_team, base_goals=1.35):
    home_attack = ratings.loc[home_team, "attack_strength"]
    home_defence = ratings.loc[home_team, "defence_strength"]

    away_attack = ratings.loc[away_team, "attack_strength"]
    away_defence = ratings.loc[away_team, "defence_strength"]

    home_xg = base_goals * home_attack * away_defence
    away_xg = base_goals * away_attack * home_defence

    return home_xg, away_xg


def simulate_match(ratings, home_team, away_team):
    home_xg, away_xg = expected_goals(ratings, home_team, away_team)

    home_goals = poisson.rvs(home_xg)
    away_goals = poisson.rvs(away_xg)

    return home_goals, away_goals


def initialise_group_tables(fixtures):
    tables = {}

    for group, group_fixtures in fixtures.groupby("group"):
        teams = sorted(
            set(group_fixtures["home_team"]).union(set(group_fixtures["away_team"]))
        )

        tables[group] = {
            team: {
                "group": group,
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

    return tables


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


def apply_actual_results(tables, actuals):
    for _, result in actuals.iterrows():
        group = result["group"] if "group" in actuals.columns else None

        home_team = result["home_team"]
        away_team = result["away_team"]

        if group is None:
            raise ValueError(
                "actual_results.csv must include group column. "
                "Run Script 13 again first."
            )

        update_table(
            table=tables[group],
            home_team=home_team,
            away_team=away_team,
            home_goals=result["home_goals"],
            away_goals=result["away_goals"],
        )

    return tables


def rank_group_table(table):
    df = pd.DataFrame(table.values())

    df["tie_breaker"] = np.random.random(len(df))

    df = df.sort_values(
        by=["points", "goal_difference", "goals_for", "tie_breaker"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    df["group_position"] = df.index + 1

    return df


def get_remaining_fixtures(fixtures, actuals):
    completed_ids = set(actuals["fixture_id"].tolist())
    remaining = fixtures[~fixtures["fixture_id"].isin(completed_ids)].copy()

    return remaining


def simulate_remaining_group_stage(fixtures, actuals, ratings):
    tables = initialise_group_tables(fixtures)
    tables = apply_actual_results(tables, actuals)

    remaining = get_remaining_fixtures(fixtures, actuals)

    for _, fixture in remaining.iterrows():
        group = fixture["group"]
        home_team = fixture["home_team"]
        away_team = fixture["away_team"]

        home_goals, away_goals = simulate_match(
            ratings=ratings,
            home_team=home_team,
            away_team=away_team,
        )

        update_table(
            table=tables[group],
            home_team=home_team,
            away_team=away_team,
            home_goals=home_goals,
            away_goals=away_goals,
        )

    ranked_tables = []

    for group, table in tables.items():
        ranked = rank_group_table(table)
        ranked_tables.append(ranked)

    full_table = pd.concat(ranked_tables, ignore_index=True)

    return full_table


def select_qualifiers(full_table):
    top_two = full_table[full_table["group_position"] <= 2].copy()

    third_placed = full_table[full_table["group_position"] == 3].copy()

    best_thirds = third_placed.sort_values(
        by=["points", "goal_difference", "goals_for", "tie_breaker"],
        ascending=[False, False, False, False],
    ).head(8)

    qualifiers = pd.concat([top_two, best_thirds], ignore_index=True)

    return qualifiers


def run_simulations(fixtures, actuals, ratings):
    teams = sorted(set(fixtures["home_team"]).union(set(fixtures["away_team"])))

    summary = {
        team: {
            "team": team,
            "qualification_count": 0,
            "group_winner_count": 0,
            "top_two_count": 0,
            "third_place_qualifier_count": 0,
            "average_points": 0.0,
            "average_goal_difference": 0.0,
            "average_group_position": 0.0,
        }
        for team in teams
    }

    for simulation in range(1, N_SIMULATIONS + 1):
        full_table = simulate_remaining_group_stage(
            fixtures=fixtures,
            actuals=actuals,
            ratings=ratings,
        )

        qualifiers = select_qualifiers(full_table)
        qualifier_teams = set(qualifiers["team"].tolist())

        group_winners = set(
            full_table.loc[full_table["group_position"] == 1, "team"].tolist()
        )

        top_two = set(
            full_table.loc[full_table["group_position"] <= 2, "team"].tolist()
        )

        third_place_qualifiers = set(
            qualifiers.loc[qualifiers["group_position"] == 3, "team"].tolist()
        )

        for _, row in full_table.iterrows():
            team = row["team"]

            summary[team]["average_points"] += row["points"]
            summary[team]["average_goal_difference"] += row["goal_difference"]
            summary[team]["average_group_position"] += row["group_position"]

            if team in qualifier_teams:
                summary[team]["qualification_count"] += 1

            if team in group_winners:
                summary[team]["group_winner_count"] += 1

            if team in top_two:
                summary[team]["top_two_count"] += 1

            if team in third_place_qualifiers:
                summary[team]["third_place_qualifier_count"] += 1

        if simulation % 1000 == 0:
            print(f"Completed simulations: {simulation:,}")

    results = pd.DataFrame(summary.values())

    for col in [
        "qualification_count",
        "group_winner_count",
        "top_two_count",
        "third_place_qualifier_count",
    ]:
        prob_col = col.replace("_count", "_probability")
        pct_col = prob_col + "_pct"

        results[prob_col] = results[col] / N_SIMULATIONS
        results[pct_col] = results[prob_col] * 100

    for col in [
        "average_points",
        "average_goal_difference",
        "average_group_position",
    ]:
        results[col] = results[col] / N_SIMULATIONS

    results = results.sort_values(
        by=["qualification_probability", "group_winner_probability"],
        ascending=[False, False],
    ).reset_index(drop=True)

    return results


def build_summary(results):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 20 LIVE QUALIFICATION FORECAST")
    lines.append("=" * 80)
    lines.append(f"Simulations: {N_SIMULATIONS:,}")
    lines.append(f"Teams forecast: {len(results):,}")

    lines.append("-" * 80)
    lines.append("TOP 20 QUALIFICATION PROBABILITIES")
    lines.append("-" * 80)

    lines.append(
        results[
            [
                "team",
                "qualification_probability_pct",
                "group_winner_probability_pct",
                "top_two_probability_pct",
                "third_place_qualifier_probability_pct",
                "average_points",
                "average_group_position",
            ]
        ].head(20).to_string(index=False)
    )

    lines.append("-" * 80)
    lines.append("HOST NATIONS")
    lines.append("-" * 80)

    hosts = results[results["team"].isin(["Mexico", "United States", "Canada"])]

    lines.append(
        hosts[
            [
                "team",
                "qualification_probability_pct",
                "group_winner_probability_pct",
                "top_two_probability_pct",
                "average_points",
                "average_group_position",
            ]
        ].to_string(index=False)
    )

    lines.append("-" * 80)
    lines.append("DANGER ZONE - LOWEST QUALIFICATION PROBABILITIES")
    lines.append("-" * 80)

    lines.append(
        results.sort_values("qualification_probability_pct", ascending=True)[
            [
                "team",
                "qualification_probability_pct",
                "group_winner_probability_pct",
                "average_points",
                "average_group_position",
            ]
        ].head(15).to_string(index=False)
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    np.random.seed(RANDOM_SEED)

    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 20 - LIVE QUALIFICATION FORECAST")
    print("=" * 80)

    fixtures, actuals, ratings = load_data()

    if "group" not in actuals.columns:
        fixtures_group_lookup = fixtures[["fixture_id", "group"]]
        actuals = actuals.merge(fixtures_group_lookup, on="fixture_id", how="left")

    print(f"Fixtures loaded:    {len(fixtures):,}")
    print(f"Actuals loaded:     {len(actuals):,}")
    print(f"Ratings loaded:     {len(ratings):,}")
    print(f"Simulations:        {N_SIMULATIONS:,}")
    print("-" * 80)

    results = run_simulations(
        fixtures=fixtures,
        actuals=actuals,
        ratings=ratings,
    )

    results.to_csv(FORECAST_OUTPUT_PATH, index=False)

    summary = build_summary(results)
    SUMMARY_OUTPUT_PATH.write_text(summary, encoding="utf-8")

    print(summary)

    print("-" * 80)
    print(f"Live qualification forecast saved: {FORECAST_OUTPUT_PATH}")
    print(f"Summary saved:                     {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
