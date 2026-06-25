"""
================================================================================
WORLD CUP LAB
SCRIPT 17 - TEAM RATING ADJUSTMENT ENGINE
================================================================================

Purpose:
    Adjust pre-tournament Elo ratings using actual World Cup results so far.

Inputs:
    data/world_cup_2026/team_ratings_host_boost_50.csv
    data/world_cup_2026/actual_results.csv
    outputs/prediction_accuracy_review.csv

Outputs:
    data/world_cup_2026/team_ratings_live_adjusted.csv
    outputs/team_rating_adjustment_review.csv
    outputs/team_rating_adjustment_summary.txt
================================================================================
"""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

BASE_RATINGS_PATH = WC_DIR / "team_ratings_host_boost_50.csv"
ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
PREDICTION_REVIEW_PATH = OUTPUT_DIR / "prediction_accuracy_review.csv"

LIVE_RATINGS_OUTPUT_PATH = WC_DIR / "team_ratings_live_adjusted.csv"
REVIEW_OUTPUT_PATH = OUTPUT_DIR / "team_rating_adjustment_review.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "team_rating_adjustment_summary.txt"

K_FACTOR = 35


def load_data():
    ratings = pd.read_csv(BASE_RATINGS_PATH)
    actuals = pd.read_csv(ACTUAL_RESULTS_PATH)
    predictions = pd.read_csv(PREDICTION_REVIEW_PATH)

    return ratings, actuals, predictions


def actual_score_from_result(row, team):
    if row["home_goals"] == row["away_goals"]:
        return 0.5

    if team == row["home_team"]:
        return 1.0 if row["home_goals"] > row["away_goals"] else 0.0

    if team == row["away_team"]:
        return 1.0 if row["away_goals"] > row["home_goals"] else 0.0

    raise ValueError(f"Team {team} not found in fixture row.")


def expected_score_from_prediction(row, team):
    if team == row["home_team"]:
        return row["home_win_prob"] + 0.5 * row["draw_prob"]

    if team == row["away_team"]:
        return row["away_win_prob"] + 0.5 * row["draw_prob"]

    raise ValueError(f"Team {team} not found in prediction row.")


def build_team_adjustments(predictions):
    adjustments = {}

    for _, row in predictions.iterrows():
        for team in [row["home_team"], row["away_team"]]:
            actual_score = actual_score_from_result(row, team)
            expected_score = expected_score_from_prediction(row, team)

            rating_change = K_FACTOR * (actual_score - expected_score)

            if team not in adjustments:
                adjustments[team] = {
                    "team": team,
                    "matches_played": 0,
                    "rating_change": 0.0,
                    "actual_score_total": 0.0,
                    "expected_score_total": 0.0,
                    "goals_for": 0,
                    "goals_against": 0,
                }

            adjustments[team]["matches_played"] += 1
            adjustments[team]["rating_change"] += rating_change
            adjustments[team]["actual_score_total"] += actual_score
            adjustments[team]["expected_score_total"] += expected_score

            if team == row["home_team"]:
                adjustments[team]["goals_for"] += row["home_goals"]
                adjustments[team]["goals_against"] += row["away_goals"]
            else:
                adjustments[team]["goals_for"] += row["away_goals"]
                adjustments[team]["goals_against"] += row["home_goals"]

    adjustment_df = pd.DataFrame(adjustments.values())

    adjustment_df["goal_difference"] = (
        adjustment_df["goals_for"] - adjustment_df["goals_against"]
    )

    adjustment_df["performance_vs_expected"] = (
        adjustment_df["actual_score_total"] - adjustment_df["expected_score_total"]
    )

    adjustment_df["performance_vs_expected_per_match"] = (
        adjustment_df["performance_vs_expected"] / adjustment_df["matches_played"]
    )

    return adjustment_df


def apply_adjustments(base_ratings, adjustments):
    ratings = base_ratings.copy()

    if "elo_adjusted" in ratings.columns:
        ratings["elo_base"] = ratings["elo_adjusted"]
    else:
        ratings["elo_base"] = ratings["elo"]

    ratings = ratings.merge(
        adjustments[
            [
                "team",
                "matches_played",
                "rating_change",
                "actual_score_total",
                "expected_score_total",
                "performance_vs_expected",
                "performance_vs_expected_per_match",
                "goals_for",
                "goals_against",
                "goal_difference",
            ]
        ],
        on="team",
        how="left",
    )

    fill_zero_cols = [
        "matches_played",
        "rating_change",
        "actual_score_total",
        "expected_score_total",
        "performance_vs_expected",
        "performance_vs_expected_per_match",
        "goals_for",
        "goals_against",
        "goal_difference",
    ]

    for col in fill_zero_cols:
        ratings[col] = ratings[col].fillna(0)

    ratings["elo_live"] = ratings["elo_base"] + ratings["rating_change"]

    mean_elo = ratings["elo_live"].mean()
    ratings["elo_diff_from_average"] = ratings["elo_live"] - mean_elo

    ratings["attack_strength"] = 1.0 + ratings["elo_diff_from_average"] / 3000
    ratings["defence_strength"] = 1.0 - ratings["elo_diff_from_average"] / 3500

    ratings["attack_strength"] = ratings["attack_strength"].clip(0.80, 1.25)
    ratings["defence_strength"] = ratings["defence_strength"].clip(0.80, 1.25)

    ratings["rating_rank_live"] = (
        ratings["elo_live"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    ratings["rating_rank_base"] = (
        ratings["elo_base"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    ratings["rank_change"] = ratings["rating_rank_base"] - ratings["rating_rank_live"]

    # Keep Script 09 compatibility
    ratings["elo"] = ratings["elo_live"]

    ratings = ratings.sort_values(
        by=["rating_rank_live", "team"],
        ascending=[True, True],
    ).reset_index(drop=True)

    return ratings


def build_summary(live_ratings):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 17 TEAM RATING ADJUSTMENT ENGINE")
    lines.append("=" * 80)
    lines.append(f"K factor: {K_FACTOR}")
    lines.append(f"Teams rated: {len(live_ratings):,}")

    played = live_ratings[live_ratings["matches_played"] > 0].copy()

    lines.append("-" * 80)
    lines.append("BIGGEST POSITIVE RATING MOVES")
    lines.append("-" * 80)

    positive = played.sort_values("rating_change", ascending=False).head(10)

    lines.append(
        positive[
            [
                "team",
                "matches_played",
                "elo_base",
                "elo_live",
                "rating_change",
                "rank_change",
                "goal_difference",
                "performance_vs_expected_per_match",
            ]
        ].to_string(index=False)
    )

    lines.append("-" * 80)
    lines.append("BIGGEST NEGATIVE RATING MOVES")
    lines.append("-" * 80)

    negative = played.sort_values("rating_change", ascending=True).head(10)

    lines.append(
        negative[
            [
                "team",
                "matches_played",
                "elo_base",
                "elo_live",
                "rating_change",
                "rank_change",
                "goal_difference",
                "performance_vs_expected_per_match",
            ]
        ].to_string(index=False)
    )

    lines.append("-" * 80)
    lines.append("TOP 20 LIVE RATINGS")
    lines.append("-" * 80)

    lines.append(
        live_ratings[
            [
                "rating_rank_live",
                "team",
                "group",
                "elo_base",
                "elo_live",
                "rating_change",
                "matches_played",
                "rank_change",
            ]
        ].head(20).to_string(index=False)
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 17 - TEAM RATING ADJUSTMENT ENGINE")
    print("=" * 80)

    base_ratings, actuals, predictions = load_data()

    adjustments = build_team_adjustments(predictions)
    live_ratings = apply_adjustments(base_ratings, adjustments)

    summary = build_summary(live_ratings)

    adjustments.to_csv(REVIEW_OUTPUT_PATH, index=False)
    live_ratings.to_csv(LIVE_RATINGS_OUTPUT_PATH, index=False)
    SUMMARY_OUTPUT_PATH.write_text(summary, encoding="utf-8")

    print(summary)

    print("-" * 80)
    print(f"Live ratings saved:       {LIVE_RATINGS_OUTPUT_PATH}")
    print(f"Adjustment review saved:  {REVIEW_OUTPUT_PATH}")
    print(f"Summary saved:            {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
