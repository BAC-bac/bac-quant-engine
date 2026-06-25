"""
================================================================================
WORLD CUP LAB
SCRIPT 16 - TEAM WATCHLIST
================================================================================

Purpose:
    Build a watchlist of teams that may be outperforming or underperforming
    early model expectations.

Inputs:
    data/world_cup_2026/actual_results.csv
    outputs/prediction_accuracy_review.csv

Outputs:
    outputs/team_watchlist.csv
    outputs/team_watchlist_summary.txt
================================================================================
"""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
PREDICTION_REVIEW_PATH = OUTPUT_DIR / "prediction_accuracy_review.csv"

WATCHLIST_OUTPUT_PATH = OUTPUT_DIR / "team_watchlist.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "team_watchlist_summary.txt"


def load_data():
    actuals = pd.read_csv(ACTUAL_RESULTS_PATH)
    predictions = pd.read_csv(PREDICTION_REVIEW_PATH)

    return actuals, predictions


def team_points_from_match(row):
    home_team = row["home_team"]
    away_team = row["away_team"]

    home_goals = row["home_goals"]
    away_goals = row["away_goals"]

    if home_goals > away_goals:
        return {
            home_team: 3,
            away_team: 0,
        }

    if home_goals < away_goals:
        return {
            home_team: 0,
            away_team: 3,
        }

    return {
        home_team: 1,
        away_team: 1,
    }


def expected_points_from_prediction(row):
    home_team = row["home_team"]
    away_team = row["away_team"]

    home_expected_points = (
        row["home_win_prob"] * 3
        + row["draw_prob"] * 1
    )

    away_expected_points = (
        row["away_win_prob"] * 3
        + row["draw_prob"] * 1
    )

    return {
        home_team: home_expected_points,
        away_team: away_expected_points,
    }


def build_team_watchlist(predictions):
    team_rows = {}

    for _, row in predictions.iterrows():
        actual_points = team_points_from_match(row)
        expected_points = expected_points_from_prediction(row)

        for team in [row["home_team"], row["away_team"]]:
            if team not in team_rows:
                team_rows[team] = {
                    "team": team,
                    "matches_played": 0,
                    "actual_points": 0,
                    "expected_points": 0.0,
                    "goals_for": 0,
                    "goals_against": 0,
                    "host_matches": 0,
                    "positive_observation_count": 0,
                    "negative_observation_count": 0,
                }

            team_rows[team]["matches_played"] += 1
            team_rows[team]["actual_points"] += actual_points[team]
            team_rows[team]["expected_points"] += expected_points[team]

            if team == row["home_team"]:
                team_rows[team]["goals_for"] += row["home_goals"]
                team_rows[team]["goals_against"] += row["away_goals"]
            else:
                team_rows[team]["goals_for"] += row["away_goals"]
                team_rows[team]["goals_against"] += row["home_goals"]

            if row.get("host_team_involved", False) and row.get("host_team") == team:
                team_rows[team]["host_matches"] += 1

    watchlist = pd.DataFrame(team_rows.values())

    watchlist["goal_difference"] = (
        watchlist["goals_for"] - watchlist["goals_against"]
    )

    watchlist["points_vs_expected"] = (
        watchlist["actual_points"] - watchlist["expected_points"]
    )

    watchlist["points_vs_expected_per_match"] = (
        watchlist["points_vs_expected"] / watchlist["matches_played"]
    )

    watchlist["actual_points_per_match"] = (
        watchlist["actual_points"] / watchlist["matches_played"]
    )

    watchlist["expected_points_per_match"] = (
        watchlist["expected_points"] / watchlist["matches_played"]
    )

    watchlist["watchlist_flag"] = "neutral"

    watchlist.loc[
        watchlist["points_vs_expected_per_match"] >= 0.50,
        "watchlist_flag",
    ] = "positive_watch"

    watchlist.loc[
        watchlist["points_vs_expected_per_match"] <= -0.50,
        "watchlist_flag",
    ] = "negative_watch"

    watchlist.loc[
        (watchlist["host_matches"] > 0)
        & (watchlist["watchlist_flag"] == "positive_watch"),
        "watchlist_flag",
    ] = "host_positive_watch"

    watchlist = watchlist.sort_values(
        by=["points_vs_expected_per_match", "goal_difference"],
        ascending=[False, False],
    ).reset_index(drop=True)

    return watchlist


def build_summary(watchlist):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 16 TEAM WATCHLIST")
    lines.append("=" * 80)

    lines.append(f"Teams reviewed: {len(watchlist):,}")

    lines.append("-" * 80)
    lines.append("POSITIVE WATCHLIST")
    lines.append("-" * 80)

    positive = watchlist[
        watchlist["watchlist_flag"].isin(
            ["positive_watch", "host_positive_watch"]
        )
    ]

    if positive.empty:
        lines.append("No positive watchlist teams yet.")
    else:
        lines.append(
            positive[
                [
                    "team",
                    "matches_played",
                    "actual_points",
                    "expected_points",
                    "points_vs_expected",
                    "goal_difference",
                    "watchlist_flag",
                ]
            ].to_string(index=False)
        )

    lines.append("-" * 80)
    lines.append("NEGATIVE WATCHLIST")
    lines.append("-" * 80)

    negative = watchlist[watchlist["watchlist_flag"] == "negative_watch"]

    if negative.empty:
        lines.append("No negative watchlist teams yet.")
    else:
        lines.append(
            negative[
                [
                    "team",
                    "matches_played",
                    "actual_points",
                    "expected_points",
                    "points_vs_expected",
                    "goal_difference",
                    "watchlist_flag",
                ]
            ].to_string(index=False)
        )

    lines.append("-" * 80)
    lines.append("FULL WATCHLIST TABLE")
    lines.append("-" * 80)

    lines.append(
        watchlist[
            [
                "team",
                "matches_played",
                "actual_points",
                "expected_points",
                "points_vs_expected",
                "actual_points_per_match",
                "expected_points_per_match",
                "goal_difference",
                "host_matches",
                "watchlist_flag",
            ]
        ].to_string(index=False)
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 16 - TEAM WATCHLIST")
    print("=" * 80)

    _, predictions = load_data()

    watchlist = build_team_watchlist(predictions)
    summary = build_summary(watchlist)

    watchlist.to_csv(WATCHLIST_OUTPUT_PATH, index=False)
    SUMMARY_OUTPUT_PATH.write_text(summary, encoding="utf-8")

    print(summary)

    print("-" * 80)
    print(f"Watchlist saved: {WATCHLIST_OUTPUT_PATH}")
    print(f"Summary saved:   {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
