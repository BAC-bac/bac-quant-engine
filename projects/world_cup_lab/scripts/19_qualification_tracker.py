"""
================================================================================
WORLD CUP LAB
SCRIPT 19 - QUALIFICATION TRACKER
================================================================================

Purpose:
    Analyse current group-stage position and estimate qualification status.

Inputs:
    outputs/group_stage_dashboard.csv
    data/world_cup_2026/fixtures.csv
    data/world_cup_2026/actual_results.csv
    outputs/world_cup_report.csv

Outputs:
    outputs/qualification_tracker.csv
    outputs/qualification_tracker_summary.txt
================================================================================
"""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

GROUP_DASHBOARD_PATH = OUTPUT_DIR / "group_stage_dashboard.csv"
FIXTURES_PATH = WC_DIR / "fixtures.csv"
ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
REPORT_PATH = OUTPUT_DIR / "world_cup_report.csv"

QUALIFICATION_OUTPUT_PATH = OUTPUT_DIR / "qualification_tracker.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "qualification_tracker_summary.txt"


def load_data():
    group_dashboard = pd.read_csv(GROUP_DASHBOARD_PATH)
    fixtures = pd.read_csv(FIXTURES_PATH)
    actuals = pd.read_csv(ACTUAL_RESULTS_PATH)
    report = pd.read_csv(REPORT_PATH) if REPORT_PATH.exists() else pd.DataFrame()

    return group_dashboard, fixtures, actuals, report


def count_remaining_fixtures(group_dashboard, fixtures, actuals):
    completed_ids = set(actuals["fixture_id"].tolist())

    remaining = fixtures[~fixtures["fixture_id"].isin(completed_ids)].copy()

    remaining_counts = []

    for team in group_dashboard["team"].unique():
        team_remaining = remaining[
            (remaining["home_team"] == team)
            | (remaining["away_team"] == team)
        ]

        remaining_counts.append(
            {
                "team": team,
                "remaining_matches": len(team_remaining),
            }
        )

    return pd.DataFrame(remaining_counts)


def estimate_status(row):
    points = row["points"]
    played = row["played"]
    remaining = row["remaining_matches"]
    qualification_prob = row.get("qualification_probability_pct", None)

    max_possible_points = points + remaining * 3

    if played == 0:
        return "not_started"

    if points >= 6:
        return "strong_position"

    if points >= 4:
        return "on_track"

    if points == 3:
        if qualification_prob is not None and qualification_prob >= 70:
            return "good_position"
        return "competitive_position"

    if points == 2:
        return "needs_win"

    if points == 1:
        if max_possible_points >= 7:
            return "still_alive"
        return "danger_zone"

    if points == 0:
        if remaining >= 2:
            return "danger_zone"
        return "near_elimination"

    return "unknown"


def estimate_required_points(row):
    points = row["points"]
    remaining = row["remaining_matches"]

    target_good = 5
    target_strong = 6

    points_needed_for_good = max(target_good - points, 0)
    points_needed_for_strong = max(target_strong - points, 0)

    max_possible_points = points + remaining * 3

    return pd.Series(
        {
            "max_possible_points": max_possible_points,
            "points_needed_for_5": points_needed_for_good,
            "points_needed_for_6": points_needed_for_strong,
        }
    )


def build_tracker(group_dashboard, fixtures, actuals, report):
    tracker = group_dashboard.copy()

    remaining_counts = count_remaining_fixtures(
        group_dashboard=group_dashboard,
        fixtures=fixtures,
        actuals=actuals,
    )

    tracker = tracker.merge(
        remaining_counts,
        on="team",
        how="left",
    )

    if "qualification_probability_pct" not in tracker.columns and not report.empty:
        tracker = tracker.merge(
            report[
                [
                    "team",
                    "qualification_probability_pct",
                    "winner_probability_pct",
                ]
            ],
            on="team",
            how="left",
        )

    required = tracker.apply(estimate_required_points, axis=1)
    tracker = pd.concat([tracker, required], axis=1)

    tracker["qualification_status"] = tracker.apply(estimate_status, axis=1)

    tracker["points_per_match"] = tracker["points"] / tracker["played"].replace(0, pd.NA)
    tracker["points_per_match"] = tracker["points_per_match"].fillna(0)

    tracker = tracker.sort_values(
        by=[
            "group",
            "points",
            "goal_difference",
            "goals_for",
            "qualification_probability_pct",
        ],
        ascending=[True, False, False, False, False],
    ).reset_index(drop=True)

    tracker["group_position_live"] = tracker.groupby("group").cumcount() + 1

    return tracker


def build_summary(tracker):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 19 QUALIFICATION TRACKER")
    lines.append("=" * 80)

    lines.append(f"Teams tracked: {len(tracker):,}")
    lines.append(f"Groups tracked: {tracker['group'].nunique():,}")

    lines.append("-" * 80)
    lines.append("GROUP QUALIFICATION SNAPSHOT")
    lines.append("-" * 80)

    for group, group_df in tracker.groupby("group"):
        lines.append(f"\nGROUP {group}")
        lines.append("-" * 40)

        lines.append(
            group_df[
                [
                    "group_position_live",
                    "team",
                    "played",
                    "points",
                    "goal_difference",
                    "remaining_matches",
                    "max_possible_points",
                    "points_needed_for_5",
                    "points_needed_for_6",
                    "qualification_probability_pct",
                    "qualification_status",
                ]
            ].to_string(index=False)
        )

    lines.append("-" * 80)
    lines.append("STRONGEST CURRENT POSITIONS")
    lines.append("-" * 80)

    strong = tracker[
        tracker["qualification_status"].isin(
            ["strong_position", "good_position", "on_track"]
        )
    ].sort_values(
        by=["points", "goal_difference", "qualification_probability_pct"],
        ascending=[False, False, False],
    )

    if strong.empty:
        lines.append("No strong positions yet.")
    else:
        lines.append(
            strong[
                [
                    "team",
                    "group",
                    "played",
                    "points",
                    "goal_difference",
                    "remaining_matches",
                    "qualification_probability_pct",
                    "qualification_status",
                ]
            ].head(20).to_string(index=False)
        )

    lines.append("-" * 80)
    lines.append("DANGER ZONE")
    lines.append("-" * 80)

    danger = tracker[
        tracker["qualification_status"].isin(
            ["danger_zone", "near_elimination", "needs_win"]
        )
    ].sort_values(
        by=["points", "max_possible_points", "goal_difference"],
        ascending=[True, True, True],
    )

    if danger.empty:
        lines.append("No teams in danger zone yet.")
    else:
        lines.append(
            danger[
                [
                    "team",
                    "group",
                    "played",
                    "points",
                    "goal_difference",
                    "remaining_matches",
                    "max_possible_points",
                    "qualification_probability_pct",
                    "qualification_status",
                ]
            ].head(20).to_string(index=False)
        )

    lines.append("-" * 80)
    lines.append("HOST NATION POSITIONS")
    lines.append("-" * 80)

    hosts = tracker[tracker["team"].isin(["Mexico", "United States", "Canada"])]

    lines.append(
        hosts[
            [
                "team",
                "group",
                "group_position_live",
                "played",
                "points",
                "goal_difference",
                "remaining_matches",
                "qualification_probability_pct",
                "qualification_status",
            ]
        ].to_string(index=False)
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 19 - QUALIFICATION TRACKER")
    print("=" * 80)

    group_dashboard, fixtures, actuals, report = load_data()

    tracker = build_tracker(
        group_dashboard=group_dashboard,
        fixtures=fixtures,
        actuals=actuals,
        report=report,
    )

    tracker.to_csv(QUALIFICATION_OUTPUT_PATH, index=False)

    summary = build_summary(tracker)
    SUMMARY_OUTPUT_PATH.write_text(summary, encoding="utf-8")

    print(summary)

    print("-" * 80)
    print(f"Qualification tracker saved: {QUALIFICATION_OUTPUT_PATH}")
    print(f"Summary saved:               {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
