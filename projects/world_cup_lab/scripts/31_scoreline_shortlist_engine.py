"""
================================================================================
WORLD CUP LAB
SCRIPT 31 - SCORELINE SHORTLIST ENGINE
================================================================================

Purpose:
    Generate a top-N scoreline shortlist for each remaining fixture using the
    recalibrated live model.

    This avoids over-relying on a single most-likely scoreline, especially when
    many scorelines have very similar probabilities.

Inputs:
    data/world_cup_2026/fixtures.csv
    data/world_cup_2026/actual_results.csv
    data/world_cup_2026/team_ratings_live_adjusted.csv
    outputs/team_rating_adjustment_review.csv
    outputs/prematch_intelligence_ranked.csv
    outputs/recalibrated_live_fixture_predictions.csv

Outputs:
    outputs/scoreline_shortlist.csv
    outputs/scoreline_shortlist_report.txt
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
RECALIBRATED_PATH = OUTPUT_DIR / "recalibrated_live_fixture_predictions.csv"

SHORTLIST_OUTPUT_PATH = OUTPUT_DIR / "scoreline_shortlist.csv"
REPORT_OUTPUT_PATH = OUTPUT_DIR / "scoreline_shortlist_report.txt"

MAX_GOALS = 8
TOP_N = 5

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
    recalibrated = safe_read_csv(RECALIBRATED_PATH)

    return fixtures, actuals, ratings, rating_review, prematch, recalibrated


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


def scoreline_matrix(home_xg, away_xg):
    rows = []

    for home_goals in range(MAX_GOALS + 1):
        for away_goals in range(MAX_GOALS + 1):
            prob = poisson.pmf(home_goals, home_xg) * poisson.pmf(away_goals, away_xg)

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
                    "scoreline_probability": prob,
                    "scoreline_probability_pct": prob * 100,
                }
            )

    matrix = pd.DataFrame(rows)

    matrix["scoreline_rank"] = matrix["scoreline_probability"].rank(
        method="first",
        ascending=False,
    ).astype(int)

    return matrix.sort_values("scoreline_probability", ascending=False)


def build_shortlist(fixtures, actuals, ratings_model, prematch, recalibrated):
    completed_ids = set(actuals["fixture_id"].tolist())
    remaining = fixtures[~fixtures["fixture_id"].isin(completed_ids)].copy()

    ratings_indexed = ratings_model.set_index("team")

    prematch_context = {}
    if not prematch.empty:
        prematch_context = prematch.set_index("fixture_id").to_dict("index")

    recal_context = {}
    if not recalibrated.empty:
        recal_context = recalibrated.set_index("fixture_id").to_dict("index")

    rows = []

    for _, fixture in remaining.iterrows():
        fixture_id = fixture["fixture_id"]
        home_team = fixture["home_team"]
        away_team = fixture["away_team"]

        home_xg, away_xg, base_goals = expected_goals(
            ratings_indexed=ratings_indexed,
            home_team=home_team,
            away_team=away_team,
        )

        matrix = scoreline_matrix(home_xg, away_xg).head(TOP_N)

        prematch_row = prematch_context.get(fixture_id, {})
        recal_row = recal_context.get(fixture_id, {})

        cumulative_top_n_prob = matrix["scoreline_probability"].sum()

        for _, score_row in matrix.iterrows():
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "group": fixture["group"],
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_xg": home_xg,
                    "away_xg": away_xg,
                    "base_goals": base_goals,
                    "scoreline_rank": score_row["scoreline_rank"],
                    "scoreline": score_row["scoreline"],
                    "scoreline_result": score_row["result"],
                    "scoreline_probability": score_row["scoreline_probability"],
                    "scoreline_probability_pct": score_row["scoreline_probability_pct"],
                    "top_n_probability_sum_pct": cumulative_top_n_prob * 100,
                    "recalibrated_predicted_result": recal_row.get("predicted_result"),
                    "recalibrated_most_likely_score": recal_row.get("most_likely_score"),
                    "recalibrated_confidence_pct": recal_row.get("confidence_pct"),
                    "prematch_intelligence_score": prematch_row.get("prematch_intelligence_score"),
                    "fixture_tags": prematch_row.get("fixture_tags"),
                }
            )

    return pd.DataFrame(rows)


def build_fixture_level_summary(shortlist):
    if shortlist.empty:
        return pd.DataFrame()

    top1 = shortlist[shortlist["scoreline_rank"] == 1].copy()

    diversity = (
        shortlist.groupby("fixture_id")
        .agg(
            top_5_probability_sum_pct=("scoreline_probability_pct", "sum"),
            top_scoreline_probability_pct=("scoreline_probability_pct", "max"),
            scoreline_options=("scoreline", lambda x: ", ".join(x.astype(str))),
        )
        .reset_index()
    )

    summary = top1.merge(
        diversity,
        on="fixture_id",
        how="left",
        suffixes=("", "_summary"),
    )

    summary["scoreline_uncertainty_pct"] = (
        summary["top_5_probability_sum_pct"]
        - summary["top_scoreline_probability_pct"]
    )

    return summary.sort_values(
        by=["prematch_intelligence_score", "top_scoreline_probability_pct"],
        ascending=[False, False],
    )


def build_report(shortlist):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 31 SCORELINE SHORTLIST ENGINE")
    lines.append("=" * 80)

    fixtures_count = shortlist["fixture_id"].nunique() if not shortlist.empty else 0
    lines.append(f"Fixtures analysed: {fixtures_count:,}")
    lines.append(f"Top scorelines per fixture: {TOP_N}")
    lines.append("")

    if shortlist.empty:
        lines.append("No scoreline shortlist data available.")
        return "\n".join(lines)

    fixture_summary = build_fixture_level_summary(shortlist)

    one_one_top_rate = (fixture_summary["scoreline"] == "1-1").mean() * 100
    lines.append(f"Top-ranked 1-1 scoreline rate: {one_one_top_rate:.2f}%")
    lines.append("")

    lines.append("=" * 80)
    lines.append("TOP PRE-MATCH FIXTURES WITH SCORELINE SHORTLISTS")
    lines.append("=" * 80)

    for _, fixture in fixture_summary.head(12).iterrows():
        fixture_id = fixture["fixture_id"]
        subset = shortlist[shortlist["fixture_id"] == fixture_id].sort_values("scoreline_rank")

        lines.append("")
        lines.append(
            f"{fixture['home_team']} v {fixture['away_team']} | Group {fixture['group']} | "
            f"xG {fixture['home_xg']:.2f}-{fixture['away_xg']:.2f}"
        )
        lines.append(
            f"Tags: {fixture.get('fixture_tags', '')} | "
            f"Prematch score: {fixture.get('prematch_intelligence_score', 0):.2f}"
        )

        for _, row in subset.iterrows():
            lines.append(
                f"  {int(row['scoreline_rank'])}. {row['scoreline']} "
                f"({row['scoreline_probability_pct']:.2f}%) "
                f"[{row['scoreline_result']}]"
            )

        lines.append(
            f"  Top-{TOP_N} probability mass: {fixture['top_5_probability_sum_pct']:.2f}%"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("MOST CONCENTRATED SCORELINE FIXTURES")
    lines.append("=" * 80)

    concentrated = fixture_summary.sort_values(
        "top_scoreline_probability_pct",
        ascending=False,
    ).head(10)

    for _, row in concentrated.iterrows():
        lines.append(
            f"{row['home_team']} v {row['away_team']} | "
            f"Top score {row['scoreline']} at {row['top_scoreline_probability_pct']:.2f}% | "
            f"Top-5 mass {row['top_5_probability_sum_pct']:.2f}%"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("QUANT'S SCORELINE VERDICT")
    lines.append("=" * 80)

    lines.append(
        "The scoreline shortlist is more informative than relying on a single most-likely score. "
        "Football scoreline probabilities are naturally spread across many small cells, so the top "
        "five outcomes usually give a better picture of the match shape."
    )

    if one_one_top_rate > 60:
        lines.append(
            "The model still frequently ranks 1-1 as the top individual cell, but the shortlist now "
            "shows whether nearby alternatives such as 1-2, 0-2, 2-1, or 2-0 are almost as likely."
        )
    else:
        lines.append(
            "The shortlist shows improved scoreline diversity, reducing dependence on the single 1-1 cell."
        )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 31 - SCORELINE SHORTLIST ENGINE")
    print("=" * 80)

    fixtures, actuals, ratings, rating_review, prematch, recalibrated = load_data()

    if fixtures.empty or ratings.empty:
        print("[STOP] Missing fixtures or ratings.")
        return

    ratings_model = prepare_ratings(ratings, rating_review)

    shortlist = build_shortlist(
        fixtures=fixtures,
        actuals=actuals,
        ratings_model=ratings_model,
        prematch=prematch,
        recalibrated=recalibrated,
    )

    shortlist.to_csv(SHORTLIST_OUTPUT_PATH, index=False)

    report = build_report(shortlist)
    REPORT_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)

    print("-" * 80)
    print(f"Scoreline shortlist saved: {SHORTLIST_OUTPUT_PATH}")
    print(f"Report saved:              {REPORT_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
