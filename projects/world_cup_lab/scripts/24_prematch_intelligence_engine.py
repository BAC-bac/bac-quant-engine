"""
================================================================================
WORLD CUP LAB
SCRIPT 24 - PRE-MATCH INTELLIGENCE ENGINE
================================================================================

Purpose:
    Identify the most important upcoming World Cup fixtures based on:

    - Remaining fixtures
    - Current group position
    - Qualification probability
    - Watchlist status
    - Rating changes
    - Potential storyline impact

Inputs:
    data/world_cup_2026/fixtures.csv
    data/world_cup_2026/actual_results.csv
    outputs/qualification_tracker.csv
    outputs/team_watchlist.csv
    outputs/team_rating_adjustment_review.csv
    outputs/live_qualification_forecast.csv

Outputs:
    outputs/prematch_intelligence_ranked.csv
    outputs/prematch_intelligence_report.txt
================================================================================
"""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

FIXTURES_PATH = WC_DIR / "fixtures.csv"
ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
QUALIFICATION_PATH = OUTPUT_DIR / "qualification_tracker.csv"
WATCHLIST_PATH = OUTPUT_DIR / "team_watchlist.csv"
RATING_ADJUSTMENT_PATH = OUTPUT_DIR / "team_rating_adjustment_review.csv"
LIVE_QUALIFICATION_PATH = OUTPUT_DIR / "live_qualification_forecast.csv"

RANKED_OUTPUT_PATH = OUTPUT_DIR / "prematch_intelligence_ranked.csv"
REPORT_OUTPUT_PATH = OUTPUT_DIR / "prematch_intelligence_report.txt"

HOST_TEAMS = ["Mexico", "United States", "Canada"]


def safe_read_csv(path):
    if path.exists():
        return pd.read_csv(path)

    print(f"[WARNING] Missing file: {path}")
    return pd.DataFrame()


def load_data():
    fixtures = safe_read_csv(FIXTURES_PATH)
    actuals = safe_read_csv(ACTUAL_RESULTS_PATH)
    qualification = safe_read_csv(QUALIFICATION_PATH)
    watchlist = safe_read_csv(WATCHLIST_PATH)
    ratings = safe_read_csv(RATING_ADJUSTMENT_PATH)
    live_qualification = safe_read_csv(LIVE_QUALIFICATION_PATH)

    return fixtures, actuals, qualification, watchlist, ratings, live_qualification


def build_team_context(qualification, watchlist, ratings, live_qualification):
    teams = qualification.copy()

    if teams.empty and not live_qualification.empty:
        teams = live_qualification.copy()

    if teams.empty:
        return pd.DataFrame()

    if not watchlist.empty:
        watch_cols = [
            "team",
            "watchlist_flag",
            "points_vs_expected",
            "actual_points",
            "expected_points",
            "goal_difference",
        ]

        teams = teams.merge(
            watchlist[[col for col in watch_cols if col in watchlist.columns]],
            on="team",
            how="left",
            suffixes=("", "_watch"),
        )

    if not ratings.empty:
        rating_cols = [
            "team",
            "rating_change",
            "performance_vs_expected_per_match",
        ]

        teams = teams.merge(
            ratings[[col for col in rating_cols if col in ratings.columns]],
            on="team",
            how="left",
        )

    for col in [
        "qualification_probability_pct",
        "group_winner_probability_pct",
        "points",
        "goal_difference",
        "points_vs_expected",
        "rating_change",
        "performance_vs_expected_per_match",
    ]:
        if col in teams.columns:
            teams[col] = teams[col].fillna(0)

    if "watchlist_flag" not in teams.columns:
        teams["watchlist_flag"] = "not_played"
    else:
        teams["watchlist_flag"] = teams["watchlist_flag"].fillna("not_played")

    return teams


def get_team_row(team_context, team):
    rows = team_context[team_context["team"] == team]

    if rows.empty:
        return {}

    return rows.iloc[0].to_dict()


def team_pressure_score(row):
    qualification = row.get("qualification_probability_pct", 0)
    points = row.get("points", 0)
    status = row.get("qualification_status", "")

    score = 0

    if qualification < 50:
        score += 4
    elif qualification < 65:
        score += 3
    elif qualification < 75:
        score += 2
    else:
        score += 1

    if points == 0:
        score += 3
    elif points == 1:
        score += 2
    elif points == 3:
        score += 1

    if status in ["danger_zone", "near_elimination", "needs_win"]:
        score += 4
    elif status in ["still_alive", "competitive_position"]:
        score += 2
    elif status in ["good_position", "strong_position"]:
        score += 1

    return score


def team_story_score(row):
    score = 0

    flag = row.get("watchlist_flag", "not_played")
    rating_change = row.get("rating_change", 0)
    pve = row.get("points_vs_expected", 0)

    if flag in ["positive_watch", "host_positive_watch"]:
        score += 4
    elif flag == "negative_watch":
        score += 3

    score += min(abs(rating_change) / 5, 5)
    score += min(abs(pve) * 2, 5)

    return score


def fixture_intelligence_score(home, away):
    home_pressure = team_pressure_score(home)
    away_pressure = team_pressure_score(away)

    home_story = team_story_score(home)
    away_story = team_story_score(away)

    qualification_gap = abs(
        home.get("qualification_probability_pct", 0)
        - away.get("qualification_probability_pct", 0)
    )

    closeness_score = max(0, 5 - qualification_gap / 10)

    host_score = 0
    if home.get("team") in HOST_TEAMS or away.get("team") in HOST_TEAMS:
        host_score = 3

    contender_clash_score = 0
    if (
        home.get("qualification_probability_pct", 0) >= 70
        and away.get("qualification_probability_pct", 0) >= 70
    ):
        contender_clash_score = 4

    danger_clash_score = 0
    if (
        home.get("qualification_probability_pct", 0) < 60
        or away.get("qualification_probability_pct", 0) < 60
    ):
        danger_clash_score = 3

    total = (
        home_pressure
        + away_pressure
        + home_story
        + away_story
        + closeness_score
        + host_score
        + contender_clash_score
        + danger_clash_score
    )

    return total


def classify_fixture(home, away):
    labels = []

    home_team = home.get("team")
    away_team = away.get("team")

    if home_team in HOST_TEAMS or away_team in HOST_TEAMS:
        labels.append("host_nation_watch")

    if (
        home.get("qualification_probability_pct", 0) >= 70
        and away.get("qualification_probability_pct", 0) >= 70
    ):
        labels.append("high_quality_clash")

    if (
        home.get("watchlist_flag") in ["positive_watch", "host_positive_watch"]
        or away.get("watchlist_flag") in ["positive_watch", "host_positive_watch"]
    ):
        labels.append("positive_watch_team_involved")

    if (
        home.get("watchlist_flag") == "negative_watch"
        or away.get("watchlist_flag") == "negative_watch"
    ):
        labels.append("pressure_team_involved")

    if (
        home.get("qualification_probability_pct", 0) < 55
        or away.get("qualification_probability_pct", 0) < 55
    ):
        labels.append("danger_zone_match")

    if not labels:
        labels.append("standard_fixture")

    return ", ".join(labels)


def build_fixture_summary(row):
    home = row["home_team"]
    away = row["away_team"]

    home_q = row["home_qualification_probability_pct"]
    away_q = row["away_qualification_probability_pct"]

    home_flag = row["home_watchlist_flag"]
    away_flag = row["away_watchlist_flag"]

    return (
        f"{home} v {away}: {home} qualification {home_q:.2f}%, "
        f"{away} qualification {away_q:.2f}%. "
        f"Watchlist: {home}={home_flag}, {away}={away_flag}. "
        f"Tags: {row['fixture_tags']}."
    )


def build_prematch_rankings(fixtures, actuals, team_context):
    completed_ids = set(actuals["fixture_id"].tolist())

    remaining = fixtures[~fixtures["fixture_id"].isin(completed_ids)].copy()

    rows = []

    for _, fixture in remaining.iterrows():
        home_team = fixture["home_team"]
        away_team = fixture["away_team"]

        home = get_team_row(team_context, home_team)
        away = get_team_row(team_context, away_team)

        if not home:
            home = {"team": home_team}
        if not away:
            away = {"team": away_team}

        score = fixture_intelligence_score(home, away)
        tags = classify_fixture(home, away)

        rows.append(
            {
                "fixture_id": fixture["fixture_id"],
                "group": fixture["group"],
                "group_match_number": fixture.get("group_match_number", None),
                "home_team": home_team,
                "away_team": away_team,
                "home_qualification_probability_pct": home.get("qualification_probability_pct", 0),
                "away_qualification_probability_pct": away.get("qualification_probability_pct", 0),
                "home_group_winner_probability_pct": home.get("group_winner_probability_pct", 0),
                "away_group_winner_probability_pct": away.get("group_winner_probability_pct", 0),
                "home_points": home.get("points", 0),
                "away_points": away.get("points", 0),
                "home_goal_difference": home.get("goal_difference", 0),
                "away_goal_difference": away.get("goal_difference", 0),
                "home_watchlist_flag": home.get("watchlist_flag", "not_played"),
                "away_watchlist_flag": away.get("watchlist_flag", "not_played"),
                "home_rating_change": home.get("rating_change", 0),
                "away_rating_change": away.get("rating_change", 0),
                "home_qualification_status": home.get("qualification_status", "unknown"),
                "away_qualification_status": away.get("qualification_status", "unknown"),
                "fixture_tags": tags,
                "prematch_intelligence_score": score,
            }
        )

    ranked = pd.DataFrame(rows)

    ranked = ranked.sort_values(
        by="prematch_intelligence_score",
        ascending=False,
    ).reset_index(drop=True)

    ranked["rank"] = ranked.index + 1

    ranked["fixture_summary"] = ranked.apply(build_fixture_summary, axis=1)

    return ranked


def build_report(ranked):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 24 PRE-MATCH INTELLIGENCE ENGINE")
    lines.append("=" * 80)

    lines.append(f"Upcoming fixtures analysed: {len(ranked):,}")

    lines.append("")
    lines.append("=" * 80)
    lines.append("TOP 15 UPCOMING FIXTURES BY INTELLIGENCE SCORE")
    lines.append("=" * 80)

    top = ranked.head(15)

    for _, row in top.iterrows():
        lines.append(
            f"{int(row['rank'])}. {row['home_team']} v {row['away_team']} "
            f"| Group {row['group']} | Score {row['prematch_intelligence_score']:.2f}"
        )
        lines.append(f"   {row['fixture_summary']}")

    lines.append("")
    lines.append("=" * 80)
    lines.append("HOST NATION WATCH")
    lines.append("=" * 80)

    host_matches = ranked[
        ranked["fixture_tags"].str.contains("host_nation_watch", na=False)
    ].head(10)

    if host_matches.empty:
        lines.append("No remaining host-nation fixtures identified.")
    else:
        for _, row in host_matches.iterrows():
            lines.append(
                f"- {row['home_team']} v {row['away_team']} "
                f"| Group {row['group']} | Score {row['prematch_intelligence_score']:.2f}"
            )

    lines.append("")
    lines.append("=" * 80)
    lines.append("DANGER ZONE MATCHES")
    lines.append("=" * 80)

    danger = ranked[
        ranked["fixture_tags"].str.contains("danger_zone_match", na=False)
    ].head(10)

    if danger.empty:
        lines.append("No danger-zone fixtures identified.")
    else:
        for _, row in danger.iterrows():
            lines.append(
                f"- {row['home_team']} v {row['away_team']} "
                f"| {row['fixture_summary']}"
            )

    lines.append("")
    lines.append("=" * 80)
    lines.append("QUANT'S PRE-MATCH VERDICT")
    lines.append("=" * 80)

    if not ranked.empty:
        top_row = ranked.iloc[0]
        lines.append(
            f"The highest-priority upcoming fixture is {top_row['home_team']} v {top_row['away_team']}. "
            f"It combines qualification pressure, watchlist relevance, and potential storyline impact."
        )

    lines.append(
        "These rankings should guide which matches deserve closer attention, manual notes, "
        "and post-match model review."
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 24 - PRE-MATCH INTELLIGENCE ENGINE")
    print("=" * 80)

    fixtures, actuals, qualification, watchlist, ratings, live_qualification = load_data()

    team_context = build_team_context(
        qualification=qualification,
        watchlist=watchlist,
        ratings=ratings,
        live_qualification=live_qualification,
    )

    ranked = build_prematch_rankings(
        fixtures=fixtures,
        actuals=actuals,
        team_context=team_context,
    )

    ranked.to_csv(RANKED_OUTPUT_PATH, index=False)

    report = build_report(ranked)
    REPORT_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)

    print("-" * 80)
    print(f"Pre-match intelligence saved: {RANKED_OUTPUT_PATH}")
    print(f"Report saved:                 {REPORT_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
