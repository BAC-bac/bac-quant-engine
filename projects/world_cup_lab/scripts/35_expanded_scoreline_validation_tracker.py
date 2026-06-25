"""
================================================================================
WORLD CUP LAB
SCRIPT 35 - EXPANDED SCORELINE VALIDATION TRACKER
================================================================================

Purpose:
    Validate Script 34 expanded scoreline predictions against actual results.

Inputs:
    outputs/expanded_scoreline_predictions.csv
    data/world_cup_2026/actual_results.csv

Outputs:
    outputs/expanded_scoreline_validation_tracker.csv
    outputs/expanded_scoreline_validation_summary.txt
================================================================================
"""

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

EXPANDED_PATH = OUTPUT_DIR / "expanded_scoreline_predictions.csv"
ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"

VALIDATION_OUTPUT_PATH = OUTPUT_DIR / "expanded_scoreline_validation_tracker.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "expanded_scoreline_validation_summary.txt"


def safe_read_csv(path):
    if path.exists():
        return pd.read_csv(path)
    print(f"[WARNING] Missing file: {path}")
    return pd.DataFrame()


def classify_result(home_goals, away_goals):
    if home_goals > away_goals:
        return "home_win"
    if home_goals < away_goals:
        return "away_win"
    return "draw"


def build_fixture_level_predictions(expanded):
    fixture_summary = (
        expanded.groupby(["fixture_id", "group", "home_team", "away_team"], dropna=False)
        .agg(
            home_xg=("home_xg", "first"),
            away_xg=("away_xg", "first"),
            home_win_prob_pct=("home_win_prob_pct", "first"),
            draw_prob_pct=("draw_prob_pct", "first"),
            away_win_prob_pct=("away_win_prob_pct", "first"),
            margin_2_plus_prob_pct=("margin_2_plus_prob_pct", "first"),
            margin_3_plus_prob_pct=("margin_3_plus_prob_pct", "first"),
            over_25_prob_pct=("over_25_prob_pct", "first"),
            over_35_prob_pct=("over_35_prob_pct", "first"),
            top_scoreline=("scoreline", "first"),
            top_scoreline_probability_pct=("scoreline_probability_pct", "first"),
            scoreline_options=("scoreline", lambda x: ", ".join(x.astype(str))),
            top_8_probability_mass_pct=("scoreline_probability_pct", "sum"),
            prematch_intelligence_score=("prematch_intelligence_score", "first"),
            fixture_tags=("fixture_tags", "first"),
        )
        .reset_index()
    )

    fixture_summary["predicted_result"] = fixture_summary[
        ["home_win_prob_pct", "draw_prob_pct", "away_win_prob_pct"]
    ].idxmax(axis=1)

    fixture_summary["predicted_result"] = fixture_summary["predicted_result"].map(
        {
            "home_win_prob_pct": "home_win",
            "draw_prob_pct": "draw",
            "away_win_prob_pct": "away_win",
        }
    )

    return fixture_summary


def validate_expanded_predictions(expanded, actuals):
    predictions = build_fixture_level_predictions(expanded)

    actuals = actuals.copy()

    if "actual_result" not in actuals.columns:
        actuals["actual_result"] = actuals.apply(
            lambda row: classify_result(row["home_goals"], row["away_goals"]),
            axis=1,
        )

    actuals["actual_scoreline"] = (
        actuals["home_goals"].astype(str) + "-" + actuals["away_goals"].astype(str)
    )

    actuals["actual_margin"] = (actuals["home_goals"] - actuals["away_goals"]).abs()
    actuals["actual_total_goals"] = actuals["home_goals"] + actuals["away_goals"]
    actuals["actual_margin_2_plus"] = actuals["actual_margin"] >= 2
    actuals["actual_margin_3_plus"] = actuals["actual_margin"] >= 3
    actuals["actual_over_25"] = actuals["actual_total_goals"] >= 3
    actuals["actual_over_35"] = actuals["actual_total_goals"] >= 4

    validation = predictions.merge(
        actuals[
            [
                "fixture_id",
                "home_goals",
                "away_goals",
                "actual_result",
                "actual_scoreline",
                "actual_margin",
                "actual_total_goals",
                "actual_margin_2_plus",
                "actual_margin_3_plus",
                "actual_over_25",
                "actual_over_35",
                "observer_notes",
            ]
        ],
        on="fixture_id",
        how="inner",
    )

    validation["result_correct"] = (
        validation["predicted_result"] == validation["actual_result"]
    )

    validation["top_scoreline_correct"] = (
        validation["top_scoreline"] == validation["actual_scoreline"]
    )

    validation["actual_in_top_8"] = validation.apply(
        lambda row: str(row["actual_scoreline"]) in str(row["scoreline_options"]).split(", "),
        axis=1,
    )

    validation["predicted_margin_2_plus_signal"] = (
        validation["margin_2_plus_prob_pct"] >= 50
    )

    validation["predicted_margin_3_plus_signal"] = (
        validation["margin_3_plus_prob_pct"] >= 35
    )

    validation["margin_2_plus_signal_correct"] = (
        validation["predicted_margin_2_plus_signal"] == validation["actual_margin_2_plus"]
    )

    validation["margin_3_plus_signal_correct"] = (
        validation["predicted_margin_3_plus_signal"] == validation["actual_margin_3_plus"]
    )

    validation["over_25_signal"] = validation["over_25_prob_pct"] >= 50
    validation["over_35_signal"] = validation["over_35_prob_pct"] >= 40

    validation["over_25_signal_correct"] = (
        validation["over_25_signal"] == validation["actual_over_25"]
    )

    validation["over_35_signal_correct"] = (
        validation["over_35_signal"] == validation["actual_over_35"]
    )

    return validation.sort_values("fixture_id")


def build_signal_summary(validation):
    if validation.empty:
        return pd.DataFrame()

    rows = []

    checks = [
        ("result_accuracy", "result_correct"),
        ("top_scoreline_accuracy", "top_scoreline_correct"),
        ("top_8_scoreline_hit_rate", "actual_in_top_8"),
        ("margin_2_plus_signal_accuracy", "margin_2_plus_signal_correct"),
        ("margin_3_plus_signal_accuracy", "margin_3_plus_signal_correct"),
        ("over_25_signal_accuracy", "over_25_signal_correct"),
        ("over_35_signal_accuracy", "over_35_signal_correct"),
    ]

    for name, col in checks:
        rows.append(
            {
                "metric": name,
                "matches": len(validation),
                "accuracy_pct": validation[col].mean() * 100,
                "correct": validation[col].sum(),
                "incorrect": len(validation) - validation[col].sum(),
            }
        )

    return pd.DataFrame(rows)


def build_blowout_summary(validation):
    if validation.empty:
        return pd.DataFrame()

    blowout_watch = validation[
        (validation["predicted_margin_2_plus_signal"])
        | (validation["predicted_margin_3_plus_signal"])
    ].copy()

    if blowout_watch.empty:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "blowout_watch_matches": len(blowout_watch),
                "actual_margin_2_plus_rate_pct": blowout_watch["actual_margin_2_plus"].mean() * 100,
                "actual_margin_3_plus_rate_pct": blowout_watch["actual_margin_3_plus"].mean() * 100,
                "avg_actual_margin": blowout_watch["actual_margin"].mean(),
                "avg_predicted_margin_2_plus_prob_pct": blowout_watch["margin_2_plus_prob_pct"].mean(),
                "avg_predicted_margin_3_plus_prob_pct": blowout_watch["margin_3_plus_prob_pct"].mean(),
            }
        ]
    )


def build_report(validation, signal_summary, blowout_summary):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 35 EXPANDED SCORELINE VALIDATION TRACKER")
    lines.append("=" * 80)

    lines.append(f"Validated fixtures: {len(validation):,}")

    if not validation.empty:
        lines.append(f"Result accuracy:          {validation['result_correct'].mean() * 100:.2f}%")
        lines.append(f"Top scoreline accuracy:   {validation['top_scoreline_correct'].mean() * 100:.2f}%")
        lines.append(f"Top-8 scoreline hit rate: {validation['actual_in_top_8'].mean() * 100:.2f}%")
        lines.append(f"Margin 2+ accuracy:       {validation['margin_2_plus_signal_correct'].mean() * 100:.2f}%")
        lines.append(f"Margin 3+ accuracy:       {validation['margin_3_plus_signal_correct'].mean() * 100:.2f}%")

    lines.append("")
    lines.append("=" * 80)
    lines.append("SIGNAL SUMMARY")
    lines.append("=" * 80)

    if signal_summary.empty:
        lines.append("No signal summary available.")
    else:
        lines.append(signal_summary.to_string(index=False))

    lines.append("")
    lines.append("=" * 80)
    lines.append("BLOWOUT WATCH SUMMARY")
    lines.append("=" * 80)

    if blowout_summary.empty:
        lines.append("No blowout-watch matches available yet.")
    else:
        lines.append(blowout_summary.to_string(index=False))

    lines.append("")
    lines.append("=" * 80)
    lines.append("MATCH-BY-MATCH VALIDATION")
    lines.append("=" * 80)

    if validation.empty:
        lines.append("No completed expanded-prediction fixtures available yet.")
    else:
        for _, row in validation.iterrows():
            lines.append(
                f"{row['home_team']} {row['home_goals']}-{row['away_goals']} {row['away_team']} | "
                f"Pred: {row['predicted_result']} | Actual: {row['actual_result']} | "
                f"Top score: {row['top_scoreline']} | Top-8 hit: {row['actual_in_top_8']} | "
                f"Margin2+ prob {row['margin_2_plus_prob_pct']:.2f}% actual {row['actual_margin_2_plus']} | "
                f"Margin3+ prob {row['margin_3_plus_prob_pct']:.2f}% actual {row['actual_margin_3_plus']}"
            )

    lines.append("")
    lines.append("=" * 80)
    lines.append("QUANT'S EXPANDED VALIDATION VERDICT")
    lines.append("=" * 80)

    lines.append(
        "This tracker tests whether the expanded scoreline engine improves practical forecasting. "
        "The key metrics are not only exact scoreline accuracy, but whether result direction, "
        "top-8 shortlist coverage, and margin signals behave sensibly."
    )

    lines.append(
        "If margin 2+ and margin 3+ signals validate over time, the expanded engine becomes a much "
        "better tournament-intelligence layer than the earlier compressed 1-1 model."
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 35 - EXPANDED SCORELINE VALIDATION TRACKER")
    print("=" * 80)

    expanded = safe_read_csv(EXPANDED_PATH)
    actuals = safe_read_csv(ACTUAL_RESULTS_PATH)

    if expanded.empty or actuals.empty:
        print("[STOP] Missing expanded predictions or actual results.")
        return

    validation = validate_expanded_predictions(expanded, actuals)

    validation.to_csv(VALIDATION_OUTPUT_PATH, index=False)

    signal_summary = build_signal_summary(validation)
    blowout_summary = build_blowout_summary(validation)

    report = build_report(validation, signal_summary, blowout_summary)
    SUMMARY_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)

    print("-" * 80)
    print(f"Expanded validation saved: {VALIDATION_OUTPUT_PATH}")
    print(f"Summary saved:             {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
