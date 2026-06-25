"""
================================================================================
WORLD CUP LAB
SCRIPT 14 - PREDICTION ACCURACY TRACKER
================================================================================

Purpose:
    Compare actual World Cup results against model predictions.

Inputs:
    data/world_cup_2026/actual_results.csv
    data/world_cup_2026/fixtures.csv
    data/world_cup_2026/team_ratings_host_boost_50.csv

Outputs:
    outputs/prediction_accuracy_review.csv
    outputs/prediction_accuracy_summary.txt
================================================================================
"""

from pathlib import Path

import pandas as pd
from scipy.stats import poisson


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
FIXTURES_PATH = WC_DIR / "fixtures.csv"
RATINGS_PATH = WC_DIR / "team_ratings_host_boost_50.csv"

REVIEW_OUTPUT_PATH = OUTPUT_DIR / "prediction_accuracy_review.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "prediction_accuracy_summary.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_data():
    actuals = pd.read_csv(ACTUAL_RESULTS_PATH)
    fixtures = pd.read_csv(FIXTURES_PATH)
    ratings = pd.read_csv(RATINGS_PATH).set_index("team")

    return actuals, fixtures, ratings


def expected_goals(ratings, home_team, away_team, base_goals=1.35):
    home_attack = ratings.loc[home_team, "attack_strength"]
    home_defence = ratings.loc[home_team, "defence_strength"]

    away_attack = ratings.loc[away_team, "attack_strength"]
    away_defence = ratings.loc[away_team, "defence_strength"]

    home_xg = base_goals * home_attack * away_defence
    away_xg = base_goals * away_attack * home_defence

    return home_xg, away_xg


def score_matrix(home_xg, away_xg, max_goals=8):
    rows = []

    for home_goals in range(max_goals + 1):
        for away_goals in range(max_goals + 1):
            probability = poisson.pmf(home_goals, home_xg) * poisson.pmf(
                away_goals,
                away_xg,
            )

            if home_goals > away_goals:
                result = "home_win"
            elif home_goals < away_goals:
                result = "away_win"
            else:
                result = "draw"

            rows.append(
                {
                    "home_goals": home_goals,
                    "away_goals": away_goals,
                    "scoreline": f"{home_goals}-{away_goals}",
                    "result": result,
                    "probability": probability,
                }
            )

    df = pd.DataFrame(rows)
    df["probability"] = df["probability"] / df["probability"].sum()

    return df


def prediction_for_fixture(ratings, home_team, away_team):
    home_xg, away_xg = expected_goals(ratings, home_team, away_team)
    scores = score_matrix(home_xg, away_xg)

    home_win_prob = scores.loc[scores["result"] == "home_win", "probability"].sum()
    draw_prob = scores.loc[scores["result"] == "draw", "probability"].sum()
    away_win_prob = scores.loc[scores["result"] == "away_win", "probability"].sum()

    most_likely_score = scores.sort_values(
        "probability",
        ascending=False,
    ).iloc[0]

    predicted_result = max(
        {
            "home_win": home_win_prob,
            "draw": draw_prob,
            "away_win": away_win_prob,
        },
        key={
            "home_win": home_win_prob,
            "draw": draw_prob,
            "away_win": away_win_prob,
        }.get,
    )

    return {
        "home_xg": home_xg,
        "away_xg": away_xg,
        "home_win_prob": home_win_prob,
        "draw_prob": draw_prob,
        "away_win_prob": away_win_prob,
        "predicted_result": predicted_result,
        "most_likely_score": most_likely_score["scoreline"],
        "most_likely_score_prob": most_likely_score["probability"],
    }


def build_review(actuals, fixtures, ratings):
    rows = []

    for _, actual in actuals.iterrows():
        fixture_id = actual["fixture_id"]
        home_team = actual["home_team"]
        away_team = actual["away_team"]

        fixture_match = fixtures[fixtures["fixture_id"] == fixture_id]

        group = fixture_match.iloc[0]["group"] if not fixture_match.empty else None

        prediction = prediction_for_fixture(
            ratings=ratings,
            home_team=home_team,
            away_team=away_team,
        )

        actual_result = actual["actual_result"]

        result_correct = prediction["predicted_result"] == actual_result

        actual_scoreline = f"{actual['home_goals']}-{actual['away_goals']}"
        scoreline_correct = prediction["most_likely_score"] == actual_scoreline

        actual_result_probability = prediction[f"{actual_result}_prob"]

        rows.append(
            {
                "fixture_id": fixture_id,
                "group": group,
                "home_team": home_team,
                "away_team": away_team,
                "home_goals": actual["home_goals"],
                "away_goals": actual["away_goals"],
                "actual_scoreline": actual_scoreline,
                "actual_result": actual_result,
                "home_xg": prediction["home_xg"],
                "away_xg": prediction["away_xg"],
                "home_win_prob": prediction["home_win_prob"],
                "draw_prob": prediction["draw_prob"],
                "away_win_prob": prediction["away_win_prob"],
                "predicted_result": prediction["predicted_result"],
                "result_correct": result_correct,
                "most_likely_score": prediction["most_likely_score"],
                "most_likely_score_prob": prediction["most_likely_score_prob"],
                "scoreline_correct": scoreline_correct,
                "actual_result_probability": actual_result_probability,
                "host_team_involved": actual.get("host_team_involved", None),
                "host_team": actual.get("host_team", None),
                "observer_notes": actual.get("observer_notes", ""),
            }
        )

    review = pd.DataFrame(rows)

    for col in [
        "home_win_prob",
        "draw_prob",
        "away_win_prob",
        "most_likely_score_prob",
        "actual_result_probability",
    ]:
        review[f"{col}_pct"] = review[col] * 100

    return review


def build_summary(review):
    total_matches = len(review)
    result_accuracy = review["result_correct"].mean()
    scoreline_accuracy = review["scoreline_correct"].mean()
    avg_actual_result_probability = review["actual_result_probability"].mean()

    host_matches = review[review["host_team_involved"] == True]

    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 14 PREDICTION ACCURACY SUMMARY")
    lines.append("=" * 80)
    lines.append(f"Matches reviewed: {total_matches}")
    lines.append(f"Result accuracy: {result_accuracy:.2%}")
    lines.append(f"Exact scoreline accuracy: {scoreline_accuracy:.2%}")
    lines.append(
        f"Average probability assigned to actual result: "
        f"{avg_actual_result_probability:.2%}"
    )

    lines.append("-" * 80)
    lines.append("MATCH REVIEW")
    lines.append("-" * 80)

    for _, row in review.iterrows():
        lines.append(
            f"{row['home_team']} {row['home_goals']}-{row['away_goals']} "
            f"{row['away_team']} | "
            f"Predicted: {row['predicted_result']} | "
            f"Actual: {row['actual_result']} | "
            f"Correct: {row['result_correct']} | "
            f"Actual result probability: {row['actual_result_probability_pct']:.2f}%"
        )

    lines.append("-" * 80)

    if not host_matches.empty:
        host_accuracy = host_matches["result_correct"].mean()
        host_avg_actual_prob = host_matches["actual_result_probability"].mean()

        lines.append("HOST TEAM MATCHES")
        lines.append("-" * 80)
        lines.append(f"Host matches reviewed: {len(host_matches)}")
        lines.append(f"Host match result accuracy: {host_accuracy:.2%}")
        lines.append(
            f"Average probability assigned to host match actual result: "
            f"{host_avg_actual_prob:.2%}"
        )

        for _, row in host_matches.iterrows():
            lines.append(
                f"{row['host_team']} involved | "
                f"{row['home_team']} {row['home_goals']}-{row['away_goals']} "
                f"{row['away_team']} | "
                f"Actual result probability: {row['actual_result_probability_pct']:.2f}%"
            )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 14 - PREDICTION ACCURACY TRACKER")
    print("=" * 80)

    actuals, fixtures, ratings = load_data()

    review = build_review(
        actuals=actuals,
        fixtures=fixtures,
        ratings=ratings,
    )

    summary = build_summary(review)

    review.to_csv(REVIEW_OUTPUT_PATH, index=False)
    SUMMARY_OUTPUT_PATH.write_text(summary, encoding="utf-8")

    print(summary)
    print("-" * 80)
    print(f"Review saved:  {REVIEW_OUTPUT_PATH}")
    print(f"Summary saved: {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
