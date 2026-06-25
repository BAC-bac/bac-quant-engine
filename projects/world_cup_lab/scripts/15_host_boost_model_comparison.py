"""
================================================================================
WORLD CUP LAB
SCRIPT 15 - HOST BOOST MODEL COMPARISON
================================================================================

Purpose:
    Compare different host advantage rating scenarios against actual results.

Models compared:
    - Real Elo baseline
    - Host boost +25
    - Host boost +50
    - Host boost +75

Inputs:
    data/world_cup_2026/actual_results.csv
    data/world_cup_2026/fixtures.csv
    data/world_cup_2026/team_ratings_real_elo.csv
    data/world_cup_2026/team_ratings_host_boost_25.csv
    data/world_cup_2026/team_ratings_host_boost_50.csv
    data/world_cup_2026/team_ratings_host_boost_75.csv

Outputs:
    outputs/host_boost_model_comparison.csv
    outputs/host_boost_model_comparison_summary.txt
================================================================================
"""

from pathlib import Path

import pandas as pd
from scipy.stats import poisson


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
FIXTURES_PATH = WC_DIR / "fixtures.csv"

MODEL_FILES = {
    "real_elo_baseline": WC_DIR / "team_ratings_real_elo.csv",
    "host_boost_25": WC_DIR / "team_ratings_host_boost_25.csv",
    "host_boost_50": WC_DIR / "team_ratings_host_boost_50.csv",
    "host_boost_75": WC_DIR / "team_ratings_host_boost_75.csv",
}

REVIEW_OUTPUT_PATH = OUTPUT_DIR / "host_boost_model_comparison.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "host_boost_model_comparison_summary.txt"


def load_actual_results():
    return pd.read_csv(ACTUAL_RESULTS_PATH)


def load_fixtures():
    return pd.read_csv(FIXTURES_PATH)


def load_model_ratings(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing model ratings file: {path}")

    return pd.read_csv(path).set_index("team")


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


def predict_fixture(ratings, home_team, away_team):
    home_xg, away_xg = expected_goals(ratings, home_team, away_team)
    scores = score_matrix(home_xg, away_xg)

    home_win_prob = scores.loc[scores["result"] == "home_win", "probability"].sum()
    draw_prob = scores.loc[scores["result"] == "draw", "probability"].sum()
    away_win_prob = scores.loc[scores["result"] == "away_win", "probability"].sum()

    result_probs = {
        "home_win": home_win_prob,
        "draw": draw_prob,
        "away_win": away_win_prob,
    }

    predicted_result = max(result_probs, key=result_probs.get)

    most_likely_score = scores.sort_values(
        "probability",
        ascending=False,
    ).iloc[0]

    return {
        "home_xg": home_xg,
        "away_xg": away_xg,
        "home_win_prob": home_win_prob,
        "draw_prob": draw_prob,
        "away_win_prob": away_win_prob,
        "predicted_result": predicted_result,
        "predicted_result_probability": result_probs[predicted_result],
        "most_likely_score": most_likely_score["scoreline"],
        "most_likely_score_prob": most_likely_score["probability"],
        "result_probs": result_probs,
    }


def evaluate_model(model_name, ratings, actuals, fixtures):
    rows = []

    for _, actual in actuals.iterrows():
        fixture_id = actual["fixture_id"]
        home_team = actual["home_team"]
        away_team = actual["away_team"]
        actual_result = actual["actual_result"]

        fixture_match = fixtures[fixtures["fixture_id"] == fixture_id]
        group = fixture_match.iloc[0]["group"] if not fixture_match.empty else None

        prediction = predict_fixture(
            ratings=ratings,
            home_team=home_team,
            away_team=away_team,
        )

        actual_result_probability = prediction["result_probs"][actual_result]
        result_correct = prediction["predicted_result"] == actual_result

        actual_scoreline = f"{actual['home_goals']}-{actual['away_goals']}"
        scoreline_correct = prediction["most_likely_score"] == actual_scoreline

        rows.append(
            {
                "model_name": model_name,
                "fixture_id": fixture_id,
                "group": group,
                "home_team": home_team,
                "away_team": away_team,
                "home_goals": actual["home_goals"],
                "away_goals": actual["away_goals"],
                "actual_result": actual_result,
                "actual_scoreline": actual_scoreline,
                "home_xg": prediction["home_xg"],
                "away_xg": prediction["away_xg"],
                "home_win_prob": prediction["home_win_prob"],
                "draw_prob": prediction["draw_prob"],
                "away_win_prob": prediction["away_win_prob"],
                "predicted_result": prediction["predicted_result"],
                "predicted_result_probability": prediction[
                    "predicted_result_probability"
                ],
                "actual_result_probability": actual_result_probability,
                "result_correct": result_correct,
                "most_likely_score": prediction["most_likely_score"],
                "most_likely_score_prob": prediction["most_likely_score_prob"],
                "scoreline_correct": scoreline_correct,
                "host_team_involved": actual.get("host_team_involved", None),
                "host_team": actual.get("host_team", None),
            }
        )

    return pd.DataFrame(rows)


def build_summary(review):
    model_summary = (
        review.groupby("model_name")
        .agg(
            matches=("fixture_id", "count"),
            result_accuracy=("result_correct", "mean"),
            exact_score_accuracy=("scoreline_correct", "mean"),
            avg_actual_result_probability=("actual_result_probability", "mean"),
            avg_predicted_result_probability=("predicted_result_probability", "mean"),
        )
        .reset_index()
    )

    for col in [
        "result_accuracy",
        "exact_score_accuracy",
        "avg_actual_result_probability",
        "avg_predicted_result_probability",
    ]:
        model_summary[f"{col}_pct"] = model_summary[col] * 100

    model_summary = model_summary.sort_values(
        by=["avg_actual_result_probability", "result_accuracy"],
        ascending=[False, False],
    ).reset_index(drop=True)

    return model_summary


def build_text_summary(model_summary, review):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 15 HOST BOOST MODEL COMPARISON")
    lines.append("=" * 80)

    lines.append("MODEL SUMMARY")
    lines.append("-" * 80)

    lines.append(
        model_summary[
            [
                "model_name",
                "matches",
                "result_accuracy_pct",
                "exact_score_accuracy_pct",
                "avg_actual_result_probability_pct",
                "avg_predicted_result_probability_pct",
            ]
        ].to_string(index=False)
    )

    lines.append("-" * 80)
    best = model_summary.iloc[0]

    lines.append(
        f"Best model by average probability assigned to actual result: "
        f"{best['model_name']}"
    )
    lines.append(
        f"Average actual result probability: "
        f"{best['avg_actual_result_probability_pct']:.2f}%"
    )
    lines.append(
        f"Result accuracy: {best['result_accuracy_pct']:.2f}%"
    )

    lines.append("-" * 80)
    lines.append("MATCH-BY-MATCH REVIEW")
    lines.append("-" * 80)

    for fixture_id, group in review.groupby("fixture_id"):
        first = group.iloc[0]

        lines.append(
            f"{first['home_team']} {first['home_goals']}-{first['away_goals']} "
            f"{first['away_team']} | Actual: {first['actual_result']}"
        )

        for _, row in group.iterrows():
            lines.append(
                f"  {row['model_name']}: "
                f"pred={row['predicted_result']} | "
                f"actual_prob={row['actual_result_probability']:.2%} | "
                f"correct={row['result_correct']}"
            )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 15 - HOST BOOST MODEL COMPARISON")
    print("=" * 80)

    actuals = load_actual_results()
    fixtures = load_fixtures()

    all_reviews = []

    for model_name, path in MODEL_FILES.items():
        print(f"Evaluating model: {model_name}")
        ratings = load_model_ratings(path)

        model_review = evaluate_model(
            model_name=model_name,
            ratings=ratings,
            actuals=actuals,
            fixtures=fixtures,
        )

        all_reviews.append(model_review)

    review = pd.concat(all_reviews, ignore_index=True)

    for col in [
        "home_win_prob",
        "draw_prob",
        "away_win_prob",
        "predicted_result_probability",
        "actual_result_probability",
        "most_likely_score_prob",
    ]:
        review[f"{col}_pct"] = review[col] * 100

    model_summary = build_summary(review)
    text_summary = build_text_summary(model_summary, review)

    review.to_csv(REVIEW_OUTPUT_PATH, index=False)
    SUMMARY_OUTPUT_PATH.write_text(text_summary, encoding="utf-8")

    print(text_summary)

    print("-" * 80)
    print(f"Review saved:  {REVIEW_OUTPUT_PATH}")
    print(f"Summary saved: {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
