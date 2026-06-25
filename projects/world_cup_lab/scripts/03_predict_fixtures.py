"""
================================================================================
WORLD CUP LAB
SCRIPT 03 - PREDICT FIXTURES
================================================================================

Purpose:
    Load a fixture list, predict match probabilities and expected scorelines
    for every fixture, then save a batch prediction report.

Inputs:
    data/processed/team_ratings.csv
    data/fixtures/world_cup_fixtures.csv

Outputs:
    outputs/fixture_predictions.csv
================================================================================
"""

from pathlib import Path

import pandas as pd
from scipy.stats import poisson


PROJECT_ROOT = Path(__file__).resolve().parents[1]

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
FIXTURES_DIR = PROJECT_ROOT / "data" / "fixtures"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RATINGS_PATH = PROCESSED_DIR / "team_ratings.csv"
FIXTURES_PATH = FIXTURES_DIR / "world_cup_fixtures.csv"
OUTPUT_PATH = OUTPUT_DIR / "fixture_predictions.csv"


def load_ratings():
    ratings = pd.read_csv(RATINGS_PATH)
    return ratings.set_index("team")


def load_fixtures():
    return pd.read_csv(FIXTURES_PATH)


def elo_win_probability(elo_a, elo_b):
    return 1 / (1 + 10 ** ((elo_b - elo_a) / 400))


def match_probabilities(ratings, home_team, away_team, draw_rate=0.26):
    elo_home = ratings.loc[home_team, "elo"]
    elo_away = ratings.loc[away_team, "elo"]

    raw_home = elo_win_probability(elo_home, elo_away)
    raw_away = 1 - raw_home

    home_win_prob = raw_home * (1 - draw_rate)
    away_win_prob = raw_away * (1 - draw_rate)
    draw_prob = draw_rate

    return home_win_prob, draw_prob, away_win_prob


def expected_goals(ratings, home_team, away_team, base_goals=1.35):
    home_attack = ratings.loc[home_team, "attack_strength"]
    home_defence = ratings.loc[home_team, "defence_strength"]

    away_attack = ratings.loc[away_team, "attack_strength"]
    away_defence = ratings.loc[away_team, "defence_strength"]

    home_xg = base_goals * home_attack * away_defence
    away_xg = base_goals * away_attack * home_defence

    return home_xg, away_xg


def create_score_matrix(home_team, away_team, home_xg, away_xg, max_goals=6):
    rows = []

    for home_goals in range(max_goals + 1):
        for away_goals in range(max_goals + 1):
            probability = poisson.pmf(home_goals, home_xg) * poisson.pmf(away_goals, away_xg)

            rows.append({
                "home_team": home_team,
                "away_team": away_team,
                "home_goals": home_goals,
                "away_goals": away_goals,
                "scoreline": f"{home_goals}-{away_goals}",
                "score_probability": probability,
            })

    scores = pd.DataFrame(rows)

    scores["score_probability"] = (
        scores["score_probability"] / scores["score_probability"].sum()
    )

    return scores.sort_values("score_probability", ascending=False)


def predict_single_fixture(ratings, fixture):
    fixture_id = fixture["fixture_id"]
    date = fixture["date"]
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]

    home_win_prob, draw_prob, away_win_prob = match_probabilities(
        ratings=ratings,
        home_team=home_team,
        away_team=away_team,
    )

    home_xg, away_xg = expected_goals(
        ratings=ratings,
        home_team=home_team,
        away_team=away_team,
    )

    score_probs = create_score_matrix(
        home_team=home_team,
        away_team=away_team,
        home_xg=home_xg,
        away_xg=away_xg,
    )

    most_likely_score = score_probs.iloc[0]

    return {
        "fixture_id": fixture_id,
        "date": date,
        "home_team": home_team,
        "away_team": away_team,
        "home_win_prob": home_win_prob,
        "draw_prob": draw_prob,
        "away_win_prob": away_win_prob,
        "home_xg": home_xg,
        "away_xg": away_xg,
        "most_likely_score": most_likely_score["scoreline"],
        "most_likely_score_prob": most_likely_score["score_probability"],
    }


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 03 - PREDICT FIXTURES")
    print("=" * 80)

    ratings = load_ratings()
    fixtures = load_fixtures()

    print(f"Ratings loaded:  {len(ratings):,}")
    print(f"Fixtures loaded: {len(fixtures):,}")
    print("-" * 80)

    predictions = []

    for _, fixture in fixtures.iterrows():
        home_team = fixture["home_team"]
        away_team = fixture["away_team"]

        if home_team not in ratings.index:
            raise ValueError(f"Missing home team in ratings file: {home_team}")

        if away_team not in ratings.index:
            raise ValueError(f"Missing away team in ratings file: {away_team}")

        prediction = predict_single_fixture(ratings, fixture)
        predictions.append(prediction)

        print(
            f"{home_team} v {away_team} | "
            f"H {prediction['home_win_prob']:.2%} | "
            f"D {prediction['draw_prob']:.2%} | "
            f"A {prediction['away_win_prob']:.2%} | "
            f"xG {prediction['home_xg']:.2f}-{prediction['away_xg']:.2f} | "
            f"MLS {prediction['most_likely_score']}"
        )

    predictions_df = pd.DataFrame(predictions)

    percent_cols = [
        "home_win_prob",
        "draw_prob",
        "away_win_prob",
        "most_likely_score_prob",
    ]

    for col in percent_cols:
        predictions_df[f"{col}_pct"] = predictions_df[col] * 100

    predictions_df.to_csv(OUTPUT_PATH, index=False)

    print("-" * 80)
    print(f"Output saved: {OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
