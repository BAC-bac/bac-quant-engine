"""
================================================================================
WORLD CUP LAB
SCRIPT 04 - PREDICT FIXTURES USING POISSON RESULT PROBABILITIES
================================================================================

Purpose:
    Predict fixture probabilities using the Poisson score matrix itself.

    This improves Script 03 because home/draw/away probabilities are now derived
    directly from expected goals and scoreline probabilities.

Inputs:
    data/processed/team_ratings.csv
    data/fixtures/world_cup_fixtures.csv

Outputs:
    outputs/fixture_predictions_poisson.csv
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
OUTPUT_PATH = OUTPUT_DIR / "fixture_predictions_poisson.csv"


def load_ratings():
    ratings = pd.read_csv(RATINGS_PATH)
    return ratings.set_index("team")


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


def create_score_matrix(home_team, away_team, home_xg, away_xg, max_goals=8):
    rows = []

    for home_goals in range(max_goals + 1):
        for away_goals in range(max_goals + 1):
            score_probability = poisson.pmf(home_goals, home_xg) * poisson.pmf(
                away_goals,
                away_xg,
            )

            if home_goals > away_goals:
                result = "home_win"
            elif home_goals == away_goals:
                result = "draw"
            else:
                result = "away_win"

            rows.append(
                {
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_goals": home_goals,
                    "away_goals": away_goals,
                    "scoreline": f"{home_goals}-{away_goals}",
                    "result": result,
                    "score_probability": score_probability,
                }
            )

    scores = pd.DataFrame(rows)

    scores["score_probability"] = (
        scores["score_probability"] / scores["score_probability"].sum()
    )

    return scores.sort_values("score_probability", ascending=False)


def derive_result_probabilities(score_probs):
    home_win_prob = score_probs.loc[
        score_probs["result"] == "home_win",
        "score_probability",
    ].sum()

    draw_prob = score_probs.loc[
        score_probs["result"] == "draw",
        "score_probability",
    ].sum()

    away_win_prob = score_probs.loc[
        score_probs["result"] == "away_win",
        "score_probability",
    ].sum()

    return home_win_prob, draw_prob, away_win_prob


def predict_single_fixture(ratings, fixture):
    fixture_id = fixture["fixture_id"]
    date = fixture["date"]
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]

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

    home_win_prob, draw_prob, away_win_prob = derive_result_probabilities(
        score_probs
    )

    most_likely_score = score_probs.iloc[0]

    over_15_prob = score_probs.loc[
        score_probs["home_goals"] + score_probs["away_goals"] > 1.5,
        "score_probability",
    ].sum()

    over_25_prob = score_probs.loc[
        score_probs["home_goals"] + score_probs["away_goals"] > 2.5,
        "score_probability",
    ].sum()

    btts_prob = score_probs.loc[
        (score_probs["home_goals"] > 0) & (score_probs["away_goals"] > 0),
        "score_probability",
    ].sum()

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
        "most_likely_score": most_likely_score["scoreline"],
        "most_likely_score_prob": most_likely_score["score_probability"],
        "over_15_prob": over_15_prob,
        "over_25_prob": over_25_prob,
        "btts_prob": btts_prob,
    }


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 04 - POISSON FIXTURE PREDICTIONS")
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
            f"MLS {prediction['most_likely_score']} | "
            f"O2.5 {prediction['over_25_prob']:.2%} | "
            f"BTTS {prediction['btts_prob']:.2%}"
        )

    predictions_df = pd.DataFrame(predictions)

    for col in [
        "home_win_prob",
        "draw_prob",
        "away_win_prob",
        "most_likely_score_prob",
        "over_15_prob",
        "over_25_prob",
        "btts_prob",
    ]:
        predictions_df[f"{col}_pct"] = predictions_df[col] * 100

    predictions_df.to_csv(OUTPUT_PATH, index=False)

    print("-" * 80)
    print(f"Output saved: {OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
