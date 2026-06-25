"""
================================================================================
WORLD CUP LAB
SCRIPT 18 - GROUP STAGE DASHBOARD
================================================================================

Purpose:
    Build a current group-stage dashboard using actual results so far.

Inputs:
    data/world_cup_2026/fixtures.csv
    data/world_cup_2026/actual_results.csv
    outputs/team_watchlist.csv
    outputs/world_cup_report.csv

Outputs:
    outputs/group_stage_dashboard.csv
    outputs/group_stage_dashboard_summary.txt
================================================================================
"""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

FIXTURES_PATH = WC_DIR / "fixtures.csv"
ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
WATCHLIST_PATH = OUTPUT_DIR / "team_watchlist.csv"
REPORT_PATH = OUTPUT_DIR / "world_cup_report.csv"

DASHBOARD_OUTPUT_PATH = OUTPUT_DIR / "group_stage_dashboard.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "group_stage_dashboard_summary.txt"


def load_data():
    fixtures = pd.read_csv(FIXTURES_PATH)
    actuals = pd.read_csv(ACTUAL_RESULTS_PATH)

    watchlist = pd.read_csv(WATCHLIST_PATH) if WATCHLIST_PATH.exists() else pd.DataFrame()
    report = pd.read_csv(REPORT_PATH) if REPORT_PATH.exists() else pd.DataFrame()

    return fixtures, actuals, watchlist, report


def initialise_group_table(fixtures):
    teams = []

    for _, row in fixtures.iterrows():
        teams.append({"group": row["group"], "team": row["home_team"]})
        teams.append({"group": row["group"], "team": row["away_team"]})

    teams_df = pd.DataFrame(teams).drop_duplicates()

    table = teams_df.copy()

    table["played"] = 0
    table["wins"] = 0
    table["draws"] = 0
    table["losses"] = 0
    table["goals_for"] = 0
    table["goals_against"] = 0
    table["goal_difference"] = 0
    table["points"] = 0

    return table


def apply_actual_result(table, result):
    home_team = result["home_team"]
    away_team = result["away_team"]
    home_goals = result["home_goals"]
    away_goals = result["away_goals"]

    home_mask = table["team"] == home_team
    away_mask = table["team"] == away_team

    table.loc[home_mask, "played"] += 1
    table.loc[away_mask, "played"] += 1

    table.loc[home_mask, "goals_for"] += home_goals
    table.loc[home_mask, "goals_against"] += away_goals

    table.loc[away_mask, "goals_for"] += away_goals
    table.loc[away_mask, "goals_against"] += home_goals

    if home_goals > away_goals:
        table.loc[home_mask, "wins"] += 1
        table.loc[away_mask, "losses"] += 1
        table.loc[home_mask, "points"] += 3

    elif home_goals < away_goals:
        table.loc[away_mask, "wins"] += 1
        table.loc[home_mask, "losses"] += 1
        table.loc[away_mask, "points"] += 3

    else:
        table.loc[home_mask, "draws"] += 1
        table.loc[away_mask, "draws"] += 1
        table.loc[home_mask, "points"] += 1
        table.loc[away_mask, "points"] += 1

    table["goal_difference"] = table["goals_for"] - table["goals_against"]

    return table


def build_group_table(fixtures, actuals):
    table = initialise_group_table(fixtures)

    for _, result in actuals.iterrows():
        table = apply_actual_result(table, result)

    table = table.sort_values(
        by=["group", "points", "goal_difference", "goals_for"],
        ascending=[True, False, False, False],
    ).reset_index(drop=True)

    table["group_position"] = (
        table.groupby("group").cumcount() + 1
    )

    return table


def add_model_context(group_table, watchlist, report):
    dashboard = group_table.copy()

    if not watchlist.empty:
        watch_cols = [
            "team",
            "expected_points",
            "points_vs_expected",
            "watchlist_flag",
        ]

        existing_cols = [col for col in watch_cols if col in watchlist.columns]

        dashboard = dashboard.merge(
            watchlist[existing_cols],
            on="team",
            how="left",
        )

    if not report.empty:
        report_cols = [
            "team",
            "qualification_probability_pct",
            "winner_probability_pct",
        ]

        existing_cols = [col for col in report_cols if col in report.columns]

        dashboard = dashboard.merge(
            report[existing_cols],
            on="team",
            how="left",
        )

    dashboard["expected_points"] = dashboard.get("expected_points", pd.Series()).fillna(0)
    dashboard["points_vs_expected"] = dashboard.get("points_vs_expected", pd.Series()).fillna(0)
    dashboard["watchlist_flag"] = dashboard.get("watchlist_flag", pd.Series()).fillna("not_played")

    return dashboard


def build_summary(dashboard):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 18 GROUP STAGE DASHBOARD")
    lines.append("=" * 80)

    lines.append(f"Teams tracked: {len(dashboard):,}")
    lines.append(f"Groups tracked: {dashboard['group'].nunique():,}")

    lines.append("-" * 80)
    lines.append("CURRENT GROUP TABLES")
    lines.append("-" * 80)

    for group, group_df in dashboard.groupby("group"):
        lines.append(f"\nGROUP {group}")
        lines.append("-" * 40)

        lines.append(
            group_df[
                [
                    "group_position",
                    "team",
                    "played",
                    "wins",
                    "draws",
                    "losses",
                    "goals_for",
                    "goals_against",
                    "goal_difference",
                    "points",
                    "watchlist_flag",
                ]
            ].to_string(index=False)
        )

    lines.append("-" * 80)
    lines.append("CURRENT GROUP LEADERS")
    lines.append("-" * 80)

    leaders = dashboard[dashboard["group_position"] == 1]

    lines.append(
        leaders[
            [
                "group",
                "team",
                "points",
                "goal_difference",
                "played",
                "watchlist_flag",
            ]
        ].to_string(index=False)
    )

    lines.append("-" * 80)
    lines.append("BIGGEST POINTS OVER EXPECTATION")
    lines.append("-" * 80)

    over = dashboard.sort_values("points_vs_expected", ascending=False).head(10)

    lines.append(
        over[
            [
                "team",
                "group",
                "points",
                "expected_points",
                "points_vs_expected",
                "watchlist_flag",
            ]
        ].to_string(index=False)
    )

    lines.append("-" * 80)
    lines.append("BIGGEST POINTS UNDER EXPECTATION")
    lines.append("-" * 80)

    under = dashboard.sort_values("points_vs_expected", ascending=True).head(10)

    lines.append(
        under[
            [
                "team",
                "group",
                "points",
                "expected_points",
                "points_vs_expected",
                "watchlist_flag",
            ]
        ].to_string(index=False)
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 18 - GROUP STAGE DASHBOARD")
    print("=" * 80)

    fixtures, actuals, watchlist, report = load_data()

    group_table = build_group_table(fixtures, actuals)
    dashboard = add_model_context(group_table, watchlist, report)

    dashboard.to_csv(DASHBOARD_OUTPUT_PATH, index=False)

    summary = build_summary(dashboard)
    SUMMARY_OUTPUT_PATH.write_text(summary, encoding="utf-8")

    print(summary)

    print("-" * 80)
    print(f"Dashboard saved: {DASHBOARD_OUTPUT_PATH}")
    print(f"Summary saved:   {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
