"""
================================================================================
WORLD CUP LAB
SCRIPT 21 - DAILY COMMAND CENTRE
================================================================================

Purpose:
    Produce a single daily World Cup Lab command-centre report by combining:

    - Actual results
    - Prediction accuracy
    - Host boost comparison
    - Team watchlist
    - Rating adjustments
    - Group dashboard
    - Live qualification forecast

Inputs:
    data/world_cup_2026/actual_results.csv
    outputs/prediction_accuracy_summary.txt
    outputs/host_boost_model_comparison_summary.txt
    outputs/team_watchlist.csv
    outputs/team_rating_adjustment_review.csv
    outputs/group_stage_dashboard.csv
    outputs/live_qualification_forecast.csv

Outputs:
    outputs/daily_command_centre_report.txt
    outputs/daily_command_centre_snapshot.csv
================================================================================
"""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
PREDICTION_SUMMARY_PATH = OUTPUT_DIR / "prediction_accuracy_summary.txt"
HOST_COMPARISON_SUMMARY_PATH = OUTPUT_DIR / "host_boost_model_comparison_summary.txt"
WATCHLIST_PATH = OUTPUT_DIR / "team_watchlist.csv"
RATING_ADJUSTMENT_PATH = OUTPUT_DIR / "team_rating_adjustment_review.csv"
GROUP_DASHBOARD_PATH = OUTPUT_DIR / "group_stage_dashboard.csv"
LIVE_QUALIFICATION_PATH = OUTPUT_DIR / "live_qualification_forecast.csv"

REPORT_OUTPUT_PATH = OUTPUT_DIR / "daily_command_centre_report.txt"
SNAPSHOT_OUTPUT_PATH = OUTPUT_DIR / "daily_command_centre_snapshot.csv"


HOST_TEAMS = ["Mexico", "United States", "Canada"]


def safe_read_csv(path):
    if path.exists():
        return pd.read_csv(path)

    print(f"[WARNING] Missing CSV: {path}")
    return pd.DataFrame()


def safe_read_text(path):
    if path.exists():
        return path.read_text(encoding="utf-8")

    print(f"[WARNING] Missing text file: {path}")
    return ""


def load_data():
    actuals = safe_read_csv(ACTUAL_RESULTS_PATH)
    watchlist = safe_read_csv(WATCHLIST_PATH)
    rating_adjustments = safe_read_csv(RATING_ADJUSTMENT_PATH)
    group_dashboard = safe_read_csv(GROUP_DASHBOARD_PATH)
    live_qualification = safe_read_csv(LIVE_QUALIFICATION_PATH)

    prediction_summary = safe_read_text(PREDICTION_SUMMARY_PATH)
    host_summary = safe_read_text(HOST_COMPARISON_SUMMARY_PATH)

    return {
        "actuals": actuals,
        "watchlist": watchlist,
        "rating_adjustments": rating_adjustments,
        "group_dashboard": group_dashboard,
        "live_qualification": live_qualification,
        "prediction_summary": prediction_summary,
        "host_summary": host_summary,
    }


def build_latest_results_section(actuals):
    lines = []

    lines.append("=" * 80)
    lines.append("LATEST RESULTS TRACKED")
    lines.append("=" * 80)

    if actuals.empty:
        lines.append("No actual results available.")
        return lines

    lines.append(f"Matches tracked: {len(actuals):,}")
    lines.append("-" * 80)

    display = actuals[
        [
            "fixture_id",
            "home_team",
            "away_team",
            "home_goals",
            "away_goals",
            "actual_result",
            "host_team_involved",
            "host_team",
        ]
    ].copy()

    lines.append(display.to_string(index=False))

    return lines


def build_live_qualification_section(live_qualification):
    lines = []

    lines.append("=" * 80)
    lines.append("LIVE QUALIFICATION FORECAST")
    lines.append("=" * 80)

    if live_qualification.empty:
        lines.append("No live qualification forecast available.")
        return lines

    top = live_qualification.sort_values(
        "qualification_probability_pct",
        ascending=False,
    ).head(15)

    lines.append("Top 15 qualification probabilities:")
    lines.append("-" * 80)

    lines.append(
        top[
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
    lines.append("Danger zone:")

    danger = live_qualification.sort_values(
        "qualification_probability_pct",
        ascending=True,
    ).head(10)

    lines.append(
        danger[
            [
                "team",
                "qualification_probability_pct",
                "group_winner_probability_pct",
                "average_points",
                "average_group_position",
            ]
        ].to_string(index=False)
    )

    return lines


def build_host_nation_section(live_qualification, group_dashboard):
    lines = []

    lines.append("=" * 80)
    lines.append("HOST NATION STATUS")
    lines.append("=" * 80)

    if live_qualification.empty:
        lines.append("No live qualification forecast available.")
        return lines

    host_forecast = live_qualification[
        live_qualification["team"].isin(HOST_TEAMS)
    ].copy()

    lines.append("Host qualification forecast:")
    lines.append("-" * 80)

    lines.append(
        host_forecast[
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

    if not group_dashboard.empty:
        lines.append("-" * 80)
        lines.append("Host current group position:")

        host_group = group_dashboard[
            group_dashboard["team"].isin(HOST_TEAMS)
        ].copy()

        lines.append(
            host_group[
                [
                    "team",
                    "group",
                    "group_position",
                    "played",
                    "points",
                    "goal_difference",
                    "watchlist_flag",
                ]
            ].to_string(index=False)
        )

    return lines


def build_watchlist_section(watchlist):
    lines = []

    lines.append("=" * 80)
    lines.append("TEAM WATCHLIST")
    lines.append("=" * 80)

    if watchlist.empty:
        lines.append("No watchlist available.")
        return lines

    positive = watchlist[
        watchlist["watchlist_flag"].isin(
            ["positive_watch", "host_positive_watch"]
        )
    ].sort_values("points_vs_expected", ascending=False)

    negative = watchlist[
        watchlist["watchlist_flag"] == "negative_watch"
    ].sort_values("points_vs_expected", ascending=True)

    lines.append("Positive watch:")
    lines.append("-" * 80)

    if positive.empty:
        lines.append("No positive watch teams.")
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
            ].head(12).to_string(index=False)
        )

    lines.append("-" * 80)
    lines.append("Negative watch:")

    if negative.empty:
        lines.append("No negative watch teams.")
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
            ].head(12).to_string(index=False)
        )

    return lines


def build_rating_adjustment_section(rating_adjustments):
    lines = []

    lines.append("=" * 80)
    lines.append("LIVE RATING ADJUSTMENTS")
    lines.append("=" * 80)

    if rating_adjustments.empty:
        lines.append("No rating adjustment review available.")
        return lines

    positive = rating_adjustments.sort_values(
        "rating_change",
        ascending=False,
    ).head(10)

    negative = rating_adjustments.sort_values(
        "rating_change",
        ascending=True,
    ).head(10)

    lines.append("Biggest positive rating moves:")
    lines.append("-" * 80)

    lines.append(
        positive[
            [
                "team",
                "matches_played",
                "rating_change",
                "actual_score_total",
                "expected_score_total",
                "goal_difference",
            ]
        ].to_string(index=False)
    )

    lines.append("-" * 80)
    lines.append("Biggest negative rating moves:")

    lines.append(
        negative[
            [
                "team",
                "matches_played",
                "rating_change",
                "actual_score_total",
                "expected_score_total",
                "goal_difference",
            ]
        ].to_string(index=False)
    )

    return lines


def build_group_leaders_section(group_dashboard):
    lines = []

    lines.append("=" * 80)
    lines.append("CURRENT GROUP LEADERS")
    lines.append("=" * 80)

    if group_dashboard.empty:
        lines.append("No group dashboard available.")
        return lines

    leaders = group_dashboard[group_dashboard["group_position"] == 1].copy()

    lines.append(
        leaders[
            [
                "group",
                "team",
                "played",
                "points",
                "goal_difference",
                "watchlist_flag",
            ]
        ].to_string(index=False)
    )

    return lines


def build_snapshot(live_qualification, watchlist, group_dashboard):
    if live_qualification.empty:
        return pd.DataFrame()

    snapshot = live_qualification.copy()

    if not watchlist.empty:
        watch_cols = [
            "team",
            "watchlist_flag",
            "points_vs_expected",
            "actual_points",
            "expected_points",
        ]

        snapshot = snapshot.merge(
            watchlist[[col for col in watch_cols if col in watchlist.columns]],
            on="team",
            how="left",
        )

    if not group_dashboard.empty:
        group_cols = [
            "team",
            "group",
            "group_position",
            "played",
            "points",
            "goal_difference",
        ]

        snapshot = snapshot.merge(
            group_dashboard[
                [col for col in group_cols if col in group_dashboard.columns]
            ],
            on="team",
            how="left",
            suffixes=("", "_current"),
        )

    snapshot = snapshot.sort_values(
        "qualification_probability_pct",
        ascending=False,
    ).reset_index(drop=True)

    return snapshot


def build_report(data):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - DAILY COMMAND CENTRE")
    lines.append("=" * 80)

    lines.append("")
    lines.extend(build_latest_results_section(data["actuals"]))
    lines.append("")
    lines.extend(build_live_qualification_section(data["live_qualification"]))
    lines.append("")
    lines.extend(build_host_nation_section(
        data["live_qualification"],
        data["group_dashboard"],
    ))
    lines.append("")
    lines.extend(build_watchlist_section(data["watchlist"]))
    lines.append("")
    lines.extend(build_rating_adjustment_section(data["rating_adjustments"]))
    lines.append("")
    lines.extend(build_group_leaders_section(data["group_dashboard"]))

    lines.append("")
    lines.append("=" * 80)
    lines.append("MODEL SUMMARY FILES")
    lines.append("=" * 80)
    lines.append(f"Prediction accuracy summary: {PREDICTION_SUMMARY_PATH}")
    lines.append(f"Host comparison summary:     {HOST_COMPARISON_SUMMARY_PATH}")
    lines.append(f"Daily report output:         {REPORT_OUTPUT_PATH}")
    lines.append(f"Daily snapshot output:       {SNAPSHOT_OUTPUT_PATH}")
    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 21 - DAILY COMMAND CENTRE")
    print("=" * 80)

    data = load_data()

    report = build_report(data)
    snapshot = build_snapshot(
        live_qualification=data["live_qualification"],
        watchlist=data["watchlist"],
        group_dashboard=data["group_dashboard"],
    )

    REPORT_OUTPUT_PATH.write_text(report, encoding="utf-8")

    if not snapshot.empty:
        snapshot.to_csv(SNAPSHOT_OUTPUT_PATH, index=False)

    print(report)

    print("-" * 80)
    print(f"Daily report saved:   {REPORT_OUTPUT_PATH}")
    print(f"Daily snapshot saved: {SNAPSHOT_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
