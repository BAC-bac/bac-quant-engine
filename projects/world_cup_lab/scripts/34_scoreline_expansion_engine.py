"""
================================================================================
WORLD CUP LAB
SCRIPT 34 - SCORELINE EXPANSION ENGINE
================================================================================

Purpose:
    Create a more expressive scoreline model that better allows dominant teams
    to produce wider winning margins.

Inputs:
    data/world_cup_2026/fixtures.csv
    data/world_cup_2026/actual_results.csv
    data/world_cup_2026/team_ratings_live_adjusted.csv
    outputs/team_rating_adjustment_review.csv
    outputs/prematch_intelligence_ranked.csv

Outputs:
    outputs/expanded_scoreline_predictions.csv
    outputs/expanded_scoreline_predictions_report.txt
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

EXPANDED_OUTPUT_PATH = OUTPUT_DIR / "expanded_scoreline_predictions.csv"
REPORT_OUTPUT_PATH = OUTPUT_DIR / "expanded_scoreline_predictions_report.txt"

MAX_GOALS = 10

BASE_GOALS = 1.48
STRENGTH_MULTIPLIER = 2.25
FORM_MULTIPLIER = 0.010
BLOWOUT_EDGE_THRESHOLD = 180
BLOWOUT_XG_BOOST = 0.35


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

    ratings["attack_model"] = 1 + (ratings["rating_edge"] / 2600) * STRENGTH_MULTIPLIER
    ratings["defence_model"] = 1 - (ratings["rating_edge"] / 3100) * STRENGTH_MULTIPLIER

    ratings["form_attack_boost"] = 1 + ratings["rating_change"] * FORM_MULTIPLIER
    ratings["form_defence_boost"] = 1 - ratings["rating_change"] * FORM_MULTIPLIER

    ratings["attack_model"] = ratings["attack_model"] * ratings["form_attack_boost"]
    ratings["defence_model"] = ratings["defence_model"] * ratings["form_defence_boost"]

    ratings["attack_model"] = ratings["attack_model"].clip(0.55, 1.75)
    ratings["defence_model"] = ratings["defence_model"].clip(0.45, 1.65)

    return ratings


def dynamic_base_goals(home_team, away_team, ratings_indexed):
    home_change = abs(ratings_indexed.loc[home_team, "rating_change"])
    away_change = abs(ratings_indexed.loc[away_team, "rating_change"])

    form_activity = home_change + away_change

    form_boost = min(form_activity * 0.005, 0.30)

    rating_gap = abs(
        ratings_indexed.loc[home_team, "rating_for_model"]
        - ratings_indexed.loc[away_team, "rating_for_model"]
    )

    gap_boost = min(rating_gap / 1000, 0.35)

    return BASE_GOALS + form_boost + gap_boost


def expected_goals(ratings_indexed, home_team, away_team):
    base_goals = dynamic_base_goals(home_team, away_team, ratings_indexed)

    home_attack = ratings_indexed.loc[home_team, "attack_model"]
    home_defence = ratings_indexed.loc[home_team, "defence_model"]

    away_attack = ratings_indexed.loc[away_team, "attack_model"]
    away_defence = ratings_indexed.loc[away_team, "defence_model"]

    home_rating = ratings_indexed.loc[home_team, "rating_for_model"]
    away_rating = ratings_indexed.loc[away_team, "rating_for_model"]

    rating_gap = home_rating - away_rating

    home_xg = base_goals * home_attack * away_defence
    away_xg = base_goals * away_attack * home_defence

    if rating_gap >= BLOWOUT_EDGE_THRESHOLD:
        home_xg += BLOWOUT_XG_BOOST
        away_xg *= 0.85
    elif rating_gap <= -BLOWOUT_EDGE_THRESHOLD:
        away_xg += BLOWOUT_XG_BOOST
        home_xg *= 0.85

    home_xg = max(home_xg, 0.20)
    away_xg = max(away_xg, 0.20)

    return home_xg, away_xg, base_goals, rating_gap


def scoreline_matrix(home_xg, away_xg):
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

            margin = abs(home_goals - away_goals)

            rows.append(
                {
                    "home_goals": home_goals,
                    "away_goals": away_goals,
                    "scoreline": f"{home_goals}-{away_goals}",
                    "result": result,
                    "margin": margin,
                    "total_goals": home_goals + away_goals,
                    "probability": probability,
                    "probability_pct": probability * 100,
                }
            )

    matrix = pd.DataFrame(rows)

    matrix["scoreline_rank"] = matrix["probability"].rank(
        method="first",
        ascending=False,
    ).astype(int)

    return matrix.sort_values("probability", ascending=False)


def predict_fixture(fixture, ratings_indexed, prematch_context):
    fixture_id = fixture["fixture_id"]
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]

    home_xg, away_xg, base_goals, rating_gap = expected_goals(
        ratings_indexed=ratings_indexed,
        home_team=home_team,
        away_team=away_team,
    )

    matrix = scoreline_matrix(home_xg, away_xg)

    home_win_prob = matrix.loc[matrix["result"] == "home_win", "probability"].sum()
    draw_prob = matrix.loc[matrix["result"] == "draw", "probability"].sum()
    away_win_prob = matrix.loc[matrix["result"] == "away_win", "probability"].sum()

    margin_2_plus_prob = matrix.loc[matrix["margin"] >= 2, "probability"].sum()
    margin_3_plus_prob = matrix.loc[matrix["margin"] >= 3, "probability"].sum()
    over_25_prob = matrix.loc[matrix["total_goals"] >= 3, "probability"].sum()
    over_35_prob = matrix.loc[matrix["total_goals"] >= 4, "probability"].sum()

    top_scorelines = matrix.head(8).copy()

    prematch_row = prematch_context.get(fixture_id, {})

    rows = []

    for _, row in top_scorelines.iterrows():
        rows.append(
            {
                "fixture_id": fixture_id,
                "group": fixture["group"],
                "home_team": home_team,
                "away_team": away_team,
                "home_xg": home_xg,
                "away_xg": away_xg,
                "base_goals": base_goals,
                "rating_gap_home_minus_away": rating_gap,
                "home_win_prob": home_win_prob,
                "draw_prob": draw_prob,
                "away_win_prob": away_win_prob,
                "home_win_prob_pct": home_win_prob * 100,
                "draw_prob_pct": draw_prob * 100,
                "away_win_prob_pct": away_win_prob * 100,
                "margin_2_plus_prob_pct": margin_2_plus_prob * 100,
                "margin_3_plus_prob_pct": margin_3_plus_prob * 100,
                "over_25_prob_pct": over_25_prob * 100,
                "over_35_prob_pct": over_35_prob * 100,
                "scoreline_rank": row["scoreline_rank"],
                "scoreline": row["scoreline"],
                "scoreline_result": row["result"],
                "scoreline_probability_pct": row["probability_pct"],
                "scoreline_margin": row["margin"],
                "scoreline_total_goals": row["total_goals"],
                "prematch_intelligence_score": prematch_row.get("prematch_intelligence_score"),
                "fixture_tags": prematch_row.get("fixture_tags"),
            }
        )

    return rows


def build_expanded_predictions(fixtures, actuals, ratings_model, prematch):
    completed_ids = set(actuals["fixture_id"].tolist())
    remaining = fixtures[~fixtures["fixture_id"].isin(completed_ids)].copy()

    ratings_indexed = ratings_model.set_index("team")

    prematch_context = {}
    if not prematch.empty:
        prematch_context = prematch.set_index("fixture_id").to_dict("index")

    rows = []

    for _, fixture in remaining.iterrows():
        rows.extend(
            predict_fixture(
                fixture=fixture,
                ratings_indexed=ratings_indexed,
                prematch_context=prematch_context,
            )
        )

    return pd.DataFrame(rows)


def build_fixture_summary(predictions):
    top1 = predictions[predictions["scoreline_rank"] == 1].copy()

    summary = (
        predictions.groupby("fixture_id")
        .agg(
            top_8_probability_mass_pct=("scoreline_probability_pct", "sum"),
            top_scoreline=("scoreline", "first"),
            top_scoreline_probability_pct=("scoreline_probability_pct", "first"),
            scoreline_options=("scoreline", lambda x: ", ".join(x.astype(str))),
            home_team=("home_team", "first"),
            away_team=("away_team", "first"),
            group=("group", "first"),
            home_xg=("home_xg", "first"),
            away_xg=("away_xg", "first"),
            home_win_prob_pct=("home_win_prob_pct", "first"),
            draw_prob_pct=("draw_prob_pct", "first"),
            away_win_prob_pct=("away_win_prob_pct", "first"),
            margin_2_plus_prob_pct=("margin_2_plus_prob_pct", "first"),
            margin_3_plus_prob_pct=("margin_3_plus_prob_pct", "first"),
            over_25_prob_pct=("over_25_prob_pct", "first"),
            over_35_prob_pct=("over_35_prob_pct", "first"),
            prematch_intelligence_score=("prematch_intelligence_score", "first"),
            fixture_tags=("fixture_tags", "first"),
        )
        .reset_index()
    )

    return summary.sort_values(
        by=["prematch_intelligence_score", "margin_2_plus_prob_pct"],
        ascending=[False, False],
    )


def build_report(predictions):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 34 SCORELINE EXPANSION ENGINE")
    lines.append("=" * 80)

    fixtures_count = predictions["fixture_id"].nunique() if not predictions.empty else 0
    lines.append(f"Fixtures analysed: {fixtures_count:,}")
    lines.append(f"Top scorelines retained per fixture: 8")
    lines.append(f"Base goals: {BASE_GOALS}")
    lines.append(f"Strength multiplier: {STRENGTH_MULTIPLIER}")
    lines.append(f"Form multiplier: {FORM_MULTIPLIER}")

    summary = build_fixture_summary(predictions)

    one_one_rate = (summary["top_scoreline"] == "1-1").mean() * 100
    lines.append(f"Top-ranked 1-1 scoreline rate: {one_one_rate:.2f}%")

    lines.append("")
    lines.append("=" * 80)
    lines.append("TOP PRE-MATCH FIXTURES - EXPANDED SCORELINE VIEW")
    lines.append("=" * 80)

    for _, fixture in summary.head(12).iterrows():
        subset = predictions[predictions["fixture_id"] == fixture["fixture_id"]].sort_values(
            "scoreline_rank"
        )

        lines.append("")
        lines.append(
            f"{fixture['home_team']} v {fixture['away_team']} | Group {fixture['group']} | "
            f"xG {fixture['home_xg']:.2f}-{fixture['away_xg']:.2f}"
        )
        lines.append(
            f"H {fixture['home_win_prob_pct']:.2f}% / "
            f"D {fixture['draw_prob_pct']:.2f}% / "
            f"A {fixture['away_win_prob_pct']:.2f}% | "
            f"Margin 2+ {fixture['margin_2_plus_prob_pct']:.2f}% | "
            f"Margin 3+ {fixture['margin_3_plus_prob_pct']:.2f}%"
        )

        for _, row in subset.iterrows():
            lines.append(
                f"  {int(row['scoreline_rank'])}. {row['scoreline']} "
                f"({row['scoreline_probability_pct']:.2f}%) [{row['scoreline_result']}]"
            )

    lines.append("")
    lines.append("=" * 80)
    lines.append("BLOWOUT WATCH")
    lines.append("=" * 80)

    blowout = summary.sort_values(
        by="margin_3_plus_prob_pct",
        ascending=False,
    ).head(12)

    for _, row in blowout.iterrows():
        lines.append(
            f"{row['home_team']} v {row['away_team']} | "
            f"Margin 3+ {row['margin_3_plus_prob_pct']:.2f}% | "
            f"Margin 2+ {row['margin_2_plus_prob_pct']:.2f}% | "
            f"Top scores: {row['scoreline_options']}"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("QUANT'S EXPANSION VERDICT")
    lines.append("=" * 80)

    lines.append(
        "This expanded scoreline engine is designed to correct the compressed-scoreline issue "
        "found in previous scripts. It increases team-strength separation, raises base goal levels "
        "for mismatches, and tracks margin probabilities directly."
    )

    if one_one_rate < 60:
        lines.append(
            "The 1-1 clustering problem has materially improved."
        )
    else:
        lines.append(
            "The model is still producing frequent 1-1 top cells, but margin probabilities now provide "
            "a better way to identify potential dominant wins."
        )

    lines.append(
        "The key test will be whether margin probabilities better identify future blowouts than the old "
        "single-scoreline model."
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 34 - SCORELINE EXPANSION ENGINE")
    print("=" * 80)

    fixtures, actuals, ratings, rating_review, prematch = load_data()

    if fixtures.empty or ratings.empty:
        print("[STOP] Missing fixtures or ratings.")
        return

    ratings_model = prepare_ratings(ratings, rating_review)

    predictions = build_expanded_predictions(
        fixtures=fixtures,
        actuals=actuals,
        ratings_model=ratings_model,
        prematch=prematch,
    )

    predictions.to_csv(EXPANDED_OUTPUT_PATH, index=False)

    report = build_report(predictions)
    REPORT_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)

    print("-" * 80)
    print(f"Expanded scoreline predictions saved: {EXPANDED_OUTPUT_PATH}")
    print(f"Report saved:                         {REPORT_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
