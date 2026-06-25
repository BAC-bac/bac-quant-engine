"""
================================================================================
WORLD CUP LAB
SCRIPT 29 - RECALIBRATED LIVE PREDICTION GENERATOR
================================================================================

Purpose:
    Generate improved live predictions using wider team-strength separation,
    dynamic base goals, and form/rating-change adjustments.

Inputs:
    data/world_cup_2026/fixtures.csv
    data/world_cup_2026/actual_results.csv
    data/world_cup_2026/team_ratings_live_adjusted.csv
    outputs/team_rating_adjustment_review.csv
    outputs/prematch_intelligence_ranked.csv

Outputs:
    outputs/recalibrated_live_fixture_predictions.csv
    outputs/recalibrated_live_fixture_predictions_report.txt
================================================================================
"""

from pathlib import Path

import pandas as pd
from scipy.stats import poisson


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

FIXTURES_PATH = WC_DIR / "fixtures.csv"
ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
LIVE_RATINGS_PATH = WC_DIR / "team_ratings_live_adjusted.csv"
RATING_REVIEW_PATH = OUTPUT_DIR / "team_rating_adjustment_review.csv"
PREMATCH_PATH = OUTPUT_DIR / "prematch_intelligence_ranked.csv"

PREDICTIONS_OUTPUT_PATH = OUTPUT_DIR / "recalibrated_live_fixture_predictions.csv"
REPORT_OUTPUT_PATH = OUTPUT_DIR / "recalibrated_live_fixture_predictions_report.txt"

MAX_GOALS = 8
BASE_GOALS = 1.42
STRENGTH_MULTIPLIER = 1.65
FORM_MULTIPLIER = 0.006


def safe_read_csv(path):
    if path.exists():
        return pd.read_csv(path)
    print(f"[WARNING] Missing file: {path}")
    return pd.DataFrame()


def load_data():
    fixtures = safe_read_csv(FIXTURES_PATH)
    actuals = safe_read_csv(ACTUAL_RESULTS_PATH)
    ratings = safe_read_csv(LIVE_RATINGS_PATH)
    rating_review = safe_read_csv(RATING_REVIEW_PATH)
    prematch = safe_read_csv(PREMATCH_PATH)

    return fixtures, actuals, ratings, rating_review, prematch


def prepare_ratings(ratings, rating_review):
    ratings = ratings.copy()

    if "elo_live" in ratings.columns:
        ratings["rating_for_model"] = ratings["elo_live"]
    elif "elo" in ratings.columns:
        ratings["rating_for_model"] = ratings["elo"]
    else:
        raise ValueError("Ratings file must contain either elo_live or elo column.")

    if not rating_review.empty and "rating_change" in rating_review.columns:
        ratings = ratings.merge(
            rating_review[["team", "rating_change"]],
            on="team",
            how="left",
            suffixes=("", "_review"),
        )
    else:
        ratings["rating_change"] = 0

    ratings["rating_change"] = ratings["rating_change"].fillna(0)

    mean_rating = ratings["rating_for_model"].mean()

    ratings["rating_edge"] = ratings["rating_for_model"] - mean_rating

    ratings["attack_model"] = 1 + (ratings["rating_edge"] / 3000) * STRENGTH_MULTIPLIER
    ratings["defence_model"] = 1 - (ratings["rating_edge"] / 3500) * STRENGTH_MULTIPLIER

    ratings["form_attack_boost"] = 1 + ratings["rating_change"] * FORM_MULTIPLIER
    ratings["form_defence_boost"] = 1 - ratings["rating_change"] * FORM_MULTIPLIER

    ratings["attack_model"] = ratings["attack_model"] * ratings["form_attack_boost"]
    ratings["defence_model"] = ratings["defence_model"] * ratings["form_defence_boost"]

    ratings["attack_model"] = ratings["attack_model"].clip(0.70, 1.45)
    ratings["defence_model"] = ratings["defence_model"].clip(0.65, 1.45)

    return ratings


def dynamic_base_goals(home_team, away_team, ratings_indexed):
    home_change = abs(ratings_indexed.loc[home_team, "rating_change"])
    away_change = abs(ratings_indexed.loc[away_team, "rating_change"])

    form_activity = home_change + away_change

    boost = min(form_activity * 0.003, 0.18)

    return BASE_GOALS + boost


def expected_goals(ratings_indexed, home_team, away_team):
    base_goals = dynamic_base_goals(home_team, away_team, ratings_indexed)

    home_attack = ratings_indexed.loc[home_team, "attack_model"]
    home_defence = ratings_indexed.loc[home_team, "defence_model"]

    away_attack = ratings_indexed.loc[away_team, "attack_model"]
    away_defence = ratings_indexed.loc[away_team, "defence_model"]

    home_xg = base_goals * home_attack * away_defence
    away_xg = base_goals * away_attack * home_defence

    return home_xg, away_xg, base_goals


def calculate_score_matrix(home_xg, away_xg):
    rows = []

    for home_goals in range(MAX_GOALS + 1):
        for away_goals in range(MAX_GOALS + 1):
            probability = poisson.pmf(home_goals, home_xg) * poisson.pmf(
                away_goals, away_xg
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

    return pd.DataFrame(rows)


def predict_fixture(ratings_indexed, fixture):
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]

    home_xg, away_xg, base_goals = expected_goals(
        ratings_indexed=ratings_indexed,
        home_team=home_team,
        away_team=away_team,
    )

    score_matrix = calculate_score_matrix(home_xg, away_xg)

    home_win_prob = score_matrix.loc[
        score_matrix["result"] == "home_win", "probability"
    ].sum()

    draw_prob = score_matrix.loc[
        score_matrix["result"] == "draw", "probability"
    ].sum()

    away_win_prob = score_matrix.loc[
        score_matrix["result"] == "away_win", "probability"
    ].sum()

    most_likely = score_matrix.sort_values(
        by="probability",
        ascending=False,
    ).iloc[0]

    predicted_result = max(
        [
            ("home_win", home_win_prob),
            ("draw", draw_prob),
            ("away_win", away_win_prob),
        ],
        key=lambda item: item[1],
    )[0]

    return {
        "fixture_id": fixture["fixture_id"],
        "group": fixture["group"],
        "group_match_number": fixture.get("group_match_number", None),
        "home_team": home_team,
        "away_team": away_team,
        "base_goals": base_goals,
        "home_xg": home_xg,
        "away_xg": away_xg,
        "home_win_prob": home_win_prob,
        "draw_prob": draw_prob,
        "away_win_prob": away_win_prob,
        "predicted_result": predicted_result,
        "most_likely_score": most_likely["scoreline"],
        "most_likely_score_prob": most_likely["probability"],
        "home_rating_change": ratings_indexed.loc[home_team, "rating_change"],
        "away_rating_change": ratings_indexed.loc[away_team, "rating_change"],
    }


def build_predictions(fixtures, actuals, ratings_model):
    completed_ids = set(actuals["fixture_id"].tolist())
    remaining = fixtures[~fixtures["fixture_id"].isin(completed_ids)].copy()

    ratings_indexed = ratings_model.set_index("team")

    rows = []

    for _, fixture in remaining.iterrows():
        rows.append(predict_fixture(ratings_indexed, fixture))

    predictions = pd.DataFrame(rows)

    for col in [
        "home_win_prob",
        "draw_prob",
        "away_win_prob",
        "most_likely_score_prob",
    ]:
        predictions[f"{col}_pct"] = predictions[col] * 100

    predictions["confidence_pct"] = predictions[
        ["home_win_prob", "draw_prob", "away_win_prob"]
    ].max(axis=1) * 100

    predictions["xg_edge"] = predictions["home_xg"] - predictions["away_xg"]
    predictions["abs_xg_edge"] = predictions["xg_edge"].abs()

    return predictions


def merge_prematch_context(predictions, prematch):
    if prematch.empty:
        return predictions

    cols = [
        "fixture_id",
        "prematch_intelligence_score",
        "fixture_tags",
        "rank",
    ]

    existing = [col for col in cols if col in prematch.columns]

    return predictions.merge(
        prematch[existing],
        on="fixture_id",
        how="left",
    )


def build_report(predictions):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 29 RECALIBRATED LIVE PREDICTION GENERATOR")
    lines.append("=" * 80)

    lines.append(f"Remaining fixtures predicted: {len(predictions):,}")
    lines.append(f"Base goals setting: {BASE_GOALS}")
    lines.append(f"Strength multiplier: {STRENGTH_MULTIPLIER}")
    lines.append(f"Form multiplier: {FORM_MULTIPLIER}")

    one_one_rate = (predictions["most_likely_score"] == "1-1").mean() * 100
    draw_rate = (predictions["draw_prob"] >= 0.25).mean() * 100

    lines.append(f"1-1 most-likely-score rate: {one_one_rate:.2f}%")
    lines.append(f"Draw-heavy fixture rate:    {draw_rate:.2f}%")

    lines.append("")
    lines.append("=" * 80)
    lines.append("TOP 15 BY PRE-MATCH INTELLIGENCE")
    lines.append("=" * 80)

    if "prematch_intelligence_score" in predictions.columns:
        top = predictions.sort_values(
            by="prematch_intelligence_score",
            ascending=False,
        ).head(15)
    else:
        top = predictions.head(15)

    for _, row in top.iterrows():
        lines.append(
            f"{row['home_team']} v {row['away_team']} | Group {row['group']} | "
            f"Prediction: {row['predicted_result']} | "
            f"H {row['home_win_prob_pct']:.2f}% / D {row['draw_prob_pct']:.2f}% / "
            f"A {row['away_win_prob_pct']:.2f}% | "
            f"xG {row['home_xg']:.2f}-{row['away_xg']:.2f} | "
            f"MLS {row['most_likely_score']} ({row['most_likely_score_prob_pct']:.2f}%)"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("HIGHEST CONFIDENCE RECALIBRATED PREDICTIONS")
    lines.append("=" * 80)

    confident = predictions.sort_values(
        by="confidence_pct",
        ascending=False,
    ).head(15)

    for _, row in confident.iterrows():
        lines.append(
            f"{row['home_team']} v {row['away_team']} | "
            f"{row['predicted_result']} confidence {row['confidence_pct']:.2f}% | "
            f"xG {row['home_xg']:.2f}-{row['away_xg']:.2f} | "
            f"MLS {row['most_likely_score']}"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("SCORELINE DIVERSITY CHECK")
    lines.append("=" * 80)

    scoreline_counts = (
        predictions["most_likely_score"]
        .value_counts()
        .head(12)
        .reset_index()
    )
    scoreline_counts.columns = ["most_likely_score", "fixtures"]

    lines.append(scoreline_counts.to_string(index=False))

    lines.append("")
    lines.append("=" * 80)
    lines.append("QUANT'S RECALIBRATION VERDICT")
    lines.append("=" * 80)

    if one_one_rate < 60:
        lines.append(
            "The recalibrated engine has reduced the previous 1-1 clustering problem."
        )
    else:
        lines.append(
            "The recalibrated engine still has too much 1-1 clustering and needs further widening."
        )

    if draw_rate < 70:
        lines.append(
            "Draw heaviness has improved compared with the previous live prediction layer."
        )
    else:
        lines.append(
            "The model remains draw-heavy and may still need stronger team separation."
        )

    lines.append(
        "This script should be compared against Script 27 and future actual results to decide "
        "whether recalibration improves predictive usefulness."
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 29 - RECALIBRATED LIVE PREDICTION GENERATOR")
    print("=" * 80)

    fixtures, actuals, ratings, rating_review, prematch = load_data()

    if fixtures.empty or ratings.empty:
        print("[STOP] Missing fixtures or ratings.")
        return

    ratings_model = prepare_ratings(ratings, rating_review)

    predictions = build_predictions(
        fixtures=fixtures,
        actuals=actuals,
        ratings_model=ratings_model,
    )

    predictions = merge_prematch_context(predictions, prematch)

    predictions.to_csv(PREDICTIONS_OUTPUT_PATH, index=False)

    report = build_report(predictions)
    REPORT_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)

    print("-" * 80)
    print(f"Recalibrated predictions saved: {PREDICTIONS_OUTPUT_PATH}")
    print(f"Report saved:                   {REPORT_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
