"""
================================================================================
WORLD CUP LAB
SCRIPT 27 - LIVE PREDICTION GENERATOR
================================================================================

Purpose:
    Generate live-updated predictions for remaining World Cup group fixtures
    using live-adjusted team ratings.

Inputs:
    data/world_cup_2026/fixtures.csv
    data/world_cup_2026/actual_results.csv
    data/world_cup_2026/team_ratings_live_adjusted.csv
    outputs/prematch_intelligence_ranked.csv

Outputs:
    outputs/live_fixture_predictions.csv
    outputs/live_fixture_predictions_report.txt
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
PREMATCH_INTELLIGENCE_PATH = OUTPUT_DIR / "prematch_intelligence_ranked.csv"

PREDICTIONS_OUTPUT_PATH = OUTPUT_DIR / "live_fixture_predictions.csv"
REPORT_OUTPUT_PATH = OUTPUT_DIR / "live_fixture_predictions_report.txt"

MAX_GOALS = 7


def safe_read_csv(path):
    if path.exists():
        return pd.read_csv(path)
    print(f"[WARNING] Missing file: {path}")
    return pd.DataFrame()


def load_data():
    fixtures = safe_read_csv(FIXTURES_PATH)
    actuals = safe_read_csv(ACTUAL_RESULTS_PATH)
    ratings = safe_read_csv(LIVE_RATINGS_PATH)
    prematch = safe_read_csv(PREMATCH_INTELLIGENCE_PATH)

    return fixtures, actuals, ratings, prematch


def expected_goals(ratings, home_team, away_team, base_goals=1.35):
    ratings_indexed = ratings.set_index("team")

    home_attack = ratings_indexed.loc[home_team, "attack_strength"]
    home_defence = ratings_indexed.loc[home_team, "defence_strength"]

    away_attack = ratings_indexed.loc[away_team, "attack_strength"]
    away_defence = ratings_indexed.loc[away_team, "defence_strength"]

    home_xg = base_goals * home_attack * away_defence
    away_xg = base_goals * away_attack * home_defence

    return home_xg, away_xg


def calculate_score_matrix(home_xg, away_xg):
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
                    "probability": prob,
                }
            )

    return pd.DataFrame(rows)


def predict_fixture(ratings, fixture):
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]

    home_xg, away_xg = expected_goals(
        ratings=ratings,
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
        "probability",
        ascending=False,
    ).iloc[0]

    predicted_result = max(
        [
            ("home_win", home_win_prob),
            ("draw", draw_prob),
            ("away_win", away_win_prob),
        ],
        key=lambda x: x[1],
    )[0]

    return {
        "fixture_id": fixture["fixture_id"],
        "group": fixture["group"],
        "group_match_number": fixture.get("group_match_number", None),
        "home_team": home_team,
        "away_team": away_team,
        "home_xg": home_xg,
        "away_xg": away_xg,
        "home_win_prob": home_win_prob,
        "draw_prob": draw_prob,
        "away_win_prob": away_win_prob,
        "predicted_result": predicted_result,
        "most_likely_score": most_likely["scoreline"],
        "most_likely_score_prob": most_likely["probability"],
    }


def build_live_predictions(fixtures, actuals, ratings):
    completed_ids = set(actuals["fixture_id"].tolist())

    remaining = fixtures[~fixtures["fixture_id"].isin(completed_ids)].copy()

    rows = []

    for _, fixture in remaining.iterrows():
        rows.append(predict_fixture(ratings, fixture))

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

    return predictions


def merge_prematch_context(predictions, prematch):
    if prematch.empty:
        return predictions

    context_cols = [
        "fixture_id",
        "prematch_intelligence_score",
        "fixture_tags",
        "rank",
    ]

    existing_cols = [col for col in context_cols if col in prematch.columns]

    return predictions.merge(
        prematch[existing_cols],
        on="fixture_id",
        how="left",
    )


def build_report(predictions):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 27 LIVE PREDICTION GENERATOR")
    lines.append("=" * 80)

    lines.append(f"Remaining fixtures predicted: {len(predictions):,}")

    lines.append("")
    lines.append("=" * 80)
    lines.append("TOP 15 BY PRE-MATCH INTELLIGENCE")
    lines.append("=" * 80)

    if "prematch_intelligence_score" in predictions.columns:
        top = predictions.sort_values(
            "prematch_intelligence_score",
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
            f"MLS {row['most_likely_score']} ({row['most_likely_score_prob_pct']:.2f}%)"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("HIGHEST CONFIDENCE LIVE PREDICTIONS")
    lines.append("=" * 80)

    confident = predictions.sort_values(
        "confidence_pct",
        ascending=False,
    ).head(15)

    for _, row in confident.iterrows():
        lines.append(
            f"{row['home_team']} v {row['away_team']} | "
            f"{row['predicted_result']} confidence {row['confidence_pct']:.2f}% | "
            f"MLS {row['most_likely_score']}"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("DRAW WATCH")
    lines.append("=" * 80)

    draw_watch = predictions.sort_values(
        "draw_prob",
        ascending=False,
    ).head(10)

    for _, row in draw_watch.iterrows():
        lines.append(
            f"{row['home_team']} v {row['away_team']} | Draw {row['draw_prob_pct']:.2f}% | "
            f"H {row['home_win_prob_pct']:.2f}% / A {row['away_win_prob_pct']:.2f}%"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("QUANT'S LIVE PREDICTION VERDICT")
    lines.append("=" * 80)

    lines.append(
        "These predictions use live-adjusted ratings rather than only the original "
        "pre-tournament Elo assumptions. They should therefore respond to early tournament "
        "form, rating upgrades, and rating downgrades."
    )

    lines.append(
        "The most useful matches to study are not necessarily the highest-confidence picks, "
        "but the fixtures where live prediction, pre-match intelligence, and watchlist context "
        "all point to a meaningful tournament story."
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 27 - LIVE PREDICTION GENERATOR")
    print("=" * 80)

    fixtures, actuals, ratings, prematch = load_data()

    if fixtures.empty or ratings.empty:
        print("[STOP] Missing fixtures or live ratings.")
        return

    predictions = build_live_predictions(
        fixtures=fixtures,
        actuals=actuals,
        ratings=ratings,
    )

    predictions = merge_prematch_context(predictions, prematch)

    predictions.to_csv(PREDICTIONS_OUTPUT_PATH, index=False)

    report = build_report(predictions)
    REPORT_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)

    print("-" * 80)
    print(f"Live predictions saved: {PREDICTIONS_OUTPUT_PATH}")
    print(f"Report saved:           {REPORT_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
