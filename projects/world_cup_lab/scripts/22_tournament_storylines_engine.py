"""
================================================================================
WORLD CUP LAB
SCRIPT 22 - TOURNAMENT STORYLINES ENGINE
================================================================================

Purpose:
    Convert World Cup Lab model outputs into a written analyst-style briefing.

Inputs:
    data/world_cup_2026/actual_results.csv
    outputs/team_watchlist.csv
    outputs/team_rating_adjustment_review.csv
    outputs/live_qualification_forecast.csv
    outputs/daily_command_centre_snapshot.csv

Outputs:
    outputs/tournament_storylines_report.txt
    outputs/tournament_storylines_ranked.csv
================================================================================
"""

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
WATCHLIST_PATH = OUTPUT_DIR / "team_watchlist.csv"
RATING_ADJUSTMENT_PATH = OUTPUT_DIR / "team_rating_adjustment_review.csv"
LIVE_QUALIFICATION_PATH = OUTPUT_DIR / "live_qualification_forecast.csv"
SNAPSHOT_PATH = OUTPUT_DIR / "daily_command_centre_snapshot.csv"

REPORT_OUTPUT_PATH = OUTPUT_DIR / "tournament_storylines_report.txt"
RANKED_OUTPUT_PATH = OUTPUT_DIR / "tournament_storylines_ranked.csv"

HOST_TEAMS = ["Mexico", "United States", "Canada"]


def safe_read_csv(path):
    if path.exists():
        return pd.read_csv(path)
    print(f"[WARNING] Missing file: {path}")
    return pd.DataFrame()


def load_data():
    return {
        "actuals": safe_read_csv(ACTUAL_RESULTS_PATH),
        "watchlist": safe_read_csv(WATCHLIST_PATH),
        "ratings": safe_read_csv(RATING_ADJUSTMENT_PATH),
        "qualification": safe_read_csv(LIVE_QUALIFICATION_PATH),
        "snapshot": safe_read_csv(SNAPSHOT_PATH),
    }


def build_storyline_scores(data):
    qualification = data["qualification"].copy()
    watchlist = data["watchlist"].copy()
    ratings = data["ratings"].copy()

    if qualification.empty:
        return pd.DataFrame()

    stories = qualification.copy()

    if not watchlist.empty:
        watch_cols = [
            "team",
            "watchlist_flag",
            "points_vs_expected",
            "goal_difference",
            "actual_points",
            "expected_points",
        ]
        stories = stories.merge(
            watchlist[[c for c in watch_cols if c in watchlist.columns]],
            on="team",
            how="left",
        )

    if not ratings.empty:
        rating_cols = [
            "team",
            "rating_change",
            "matches_played",
            "performance_vs_expected_per_match",
        ]
        stories = stories.merge(
            ratings[[c for c in rating_cols if c in ratings.columns]],
            on="team",
            how="left",
        )

    for col in [
        "points_vs_expected",
        "goal_difference",
        "actual_points",
        "expected_points",
        "rating_change",
        "matches_played",
        "performance_vs_expected_per_match",
    ]:
        if col in stories.columns:
            stories[col] = stories[col].fillna(0)

    if "watchlist_flag" not in stories.columns:
        stories["watchlist_flag"] = "not_played"
    else:
        stories["watchlist_flag"] = stories["watchlist_flag"].fillna("not_played")

    stories["host_team"] = stories["team"].isin(HOST_TEAMS)

    stories["positive_story_score"] = (
        stories.get("qualification_probability_pct", 0) * 0.03
        + stories.get("group_winner_probability_pct", 0) * 0.03
        + stories.get("points_vs_expected", 0) * 2.0
        + stories.get("rating_change", 0) * 0.15
        + stories.get("goal_difference", 0) * 0.5
    )

    stories["negative_story_score"] = (
        -stories.get("points_vs_expected", 0) * 2.0
        + -stories.get("rating_change", 0) * 0.15
        + -stories.get("goal_difference", 0) * 0.5
    )

    stories.loc[stories["positive_story_score"] < 0, "positive_story_score"] = 0
    stories.loc[stories["negative_story_score"] < 0, "negative_story_score"] = 0

    stories = stories.sort_values(
        by="positive_story_score",
        ascending=False,
    ).reset_index(drop=True)

    return stories


def sentence_for_positive_team(row):
    team = row["team"]
    q = row.get("qualification_probability_pct", 0)
    gw = row.get("group_winner_probability_pct", 0)
    rc = row.get("rating_change", 0)
    pve = row.get("points_vs_expected", 0)

    return (
        f"{team} are one of the tournament's strongest positive stories so far. "
        f"They now have a {q:.2f}% qualification probability, a {gw:.2f}% group-winner probability, "
        f"a rating adjustment of {rc:+.2f}, and they are {pve:+.2f} points versus expectation."
    )


def sentence_for_negative_team(row):
    team = row["team"]
    q = row.get("qualification_probability_pct", 0)
    rc = row.get("rating_change", 0)
    pve = row.get("points_vs_expected", 0)

    return (
        f"{team} are currently one of the main negative watch teams. "
        f"Their qualification probability sits at {q:.2f}%, with a rating adjustment of {rc:+.2f} "
        f"and {pve:+.2f} points versus expectation."
    )


def build_report(data, stories):
    actuals = data["actuals"]
    qualification = data["qualification"]

    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 22 TOURNAMENT STORYLINES ENGINE")
    lines.append("=" * 80)

    lines.append(f"Matches tracked: {len(actuals):,}")
    lines.append(f"Teams analysed:  {len(stories):,}")
    lines.append("")

    lines.append("=" * 80)
    lines.append("HEADLINE STORY")
    lines.append("=" * 80)

    if not stories.empty:
        top_story = stories.iloc[0]
        lines.append(sentence_for_positive_team(top_story))
    else:
        lines.append("No storyline data available yet.")

    lines.append("")

    lines.append("=" * 80)
    lines.append("BIGGEST POSITIVE STORYLINES")
    lines.append("=" * 80)

    positive = stories.sort_values("positive_story_score", ascending=False).head(10)

    for _, row in positive.iterrows():
        lines.append(f"- {sentence_for_positive_team(row)}")

    lines.append("")

    lines.append("=" * 80)
    lines.append("BIGGEST NEGATIVE STORYLINES")
    lines.append("=" * 80)

    negative = stories.sort_values("negative_story_score", ascending=False).head(10)

    for _, row in negative.iterrows():
        if row["negative_story_score"] > 0:
            lines.append(f"- {sentence_for_negative_team(row)}")

    lines.append("")

    lines.append("=" * 80)
    lines.append("HOST NATION OUTLOOK")
    lines.append("=" * 80)

    hosts = stories[stories["team"].isin(HOST_TEAMS)].copy()

    if hosts.empty:
        lines.append("No host-nation data available.")
    else:
        for _, row in hosts.sort_values("qualification_probability_pct", ascending=False).iterrows():
            lines.append(
                f"- {row['team']}: qualification {row['qualification_probability_pct']:.2f}%, "
                f"group winner {row['group_winner_probability_pct']:.2f}%, "
                f"rating change {row.get('rating_change', 0):+.2f}."
            )

    lines.append("")

    lines.append("=" * 80)
    lines.append("DARK HORSE BOARD")
    lines.append("=" * 80)

    dark_horses = stories[
        (stories["qualification_probability_pct"] >= 65)
        & (stories["qualification_probability_pct"] <= 95)
        & (~stories["team"].isin(["Spain", "Argentina", "France", "England", "Brazil", "Portugal"]))
    ].sort_values("positive_story_score", ascending=False).head(12)

    if dark_horses.empty:
        lines.append("No dark horse candidates identified yet.")
    else:
        for _, row in dark_horses.iterrows():
            lines.append(
                f"- {row['team']}: qualification {row['qualification_probability_pct']:.2f}%, "
                f"group winner {row['group_winner_probability_pct']:.2f}%, "
                f"rating change {row.get('rating_change', 0):+.2f}."
            )

    lines.append("")

    lines.append("=" * 80)
    lines.append("DANGER ZONE")
    lines.append("=" * 80)

    danger = qualification.sort_values("qualification_probability_pct", ascending=True).head(10)

    for _, row in danger.iterrows():
        lines.append(
            f"- {row['team']}: qualification probability {row['qualification_probability_pct']:.2f}%, "
            f"average points {row['average_points']:.2f}, "
            f"average group position {row['average_group_position']:.2f}."
        )

    lines.append("")

    lines.append("=" * 80)
    lines.append("QUANT'S VERDICT")
    lines.append("=" * 80)

    lines.append(
        "The tournament model is now moving beyond static pre-tournament forecasting. "
        "Actual results are feeding into live ratings, qualification simulations, watchlists, "
        "and storyline generation. The strongest early signals are coming from teams that have "
        "both beaten expectation and improved their live qualification outlook."
    )

    if not hosts.empty:
        lines.append(
            "The host-nation effect remains a live hypothesis, with Mexico and the United States "
            "both showing strong qualification positions after their opening matches."
        )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 22 - TOURNAMENT STORYLINES ENGINE")
    print("=" * 80)

    data = load_data()
    stories = build_storyline_scores(data)

    if not stories.empty:
        stories.to_csv(RANKED_OUTPUT_PATH, index=False)

    report = build_report(data, stories)
    REPORT_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)
    print("-" * 80)
    print(f"Storylines report saved: {REPORT_OUTPUT_PATH}")
    print(f"Ranked storylines saved: {RANKED_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
