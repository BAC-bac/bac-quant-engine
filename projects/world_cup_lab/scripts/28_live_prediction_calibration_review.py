"""
================================================================================
WORLD CUP LAB
SCRIPT 28 - LIVE PREDICTION CALIBRATION REVIEW
================================================================================

Purpose:
    Review whether live-adjusted predictions look better calibrated than the
    original/pre-tournament style predictions, and identify model weaknesses.

Inputs:
    outputs/live_fixture_predictions.csv
    outputs/prematch_intelligence_ranked.csv
    outputs/model_edge_tracker.csv

Outputs:
    outputs/live_prediction_calibration_review.csv
    outputs/live_prediction_calibration_summary.txt
================================================================================
"""

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "outputs"

LIVE_PREDICTIONS_PATH = OUTPUT_DIR / "live_fixture_predictions.csv"
PREMATCH_PATH = OUTPUT_DIR / "prematch_intelligence_ranked.csv"
MODEL_EDGE_PATH = OUTPUT_DIR / "model_edge_tracker.csv"

CALIBRATION_OUTPUT_PATH = OUTPUT_DIR / "live_prediction_calibration_review.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "live_prediction_calibration_summary.txt"


def safe_read_csv(path):
    if path.exists():
        return pd.read_csv(path)
    print(f"[WARNING] Missing file: {path}")
    return pd.DataFrame()


def load_data():
    live_predictions = safe_read_csv(LIVE_PREDICTIONS_PATH)
    prematch = safe_read_csv(PREMATCH_PATH)
    model_edge = safe_read_csv(MODEL_EDGE_PATH)

    return live_predictions, prematch, model_edge


def build_calibration_table(live_predictions, prematch):
    df = live_predictions.copy()

    if not prematch.empty:
        extra_cols = [
            "fixture_id",
            "prematch_intelligence_score",
            "fixture_tags",
            "home_watchlist_flag",
            "away_watchlist_flag",
            "home_qualification_probability_pct",
            "away_qualification_probability_pct",
        ]

        existing = [c for c in extra_cols if c in prematch.columns]

        df = df.merge(
            prematch[existing],
            on="fixture_id",
            how="left",
            suffixes=("", "_prematch"),
        )

    df["result_probability_spread"] = (
        df[["home_win_prob", "draw_prob", "away_win_prob"]].max(axis=1)
        - df[["home_win_prob", "draw_prob", "away_win_prob"]].min(axis=1)
    )

    df["draw_heaviness_score"] = df["draw_prob"]

    df["is_draw_heavy"] = df["draw_prob"] >= 0.25
    df["is_low_confidence"] = df["confidence_pct"] < 40
    df["is_high_confidence"] = df["confidence_pct"] >= 45

    df["home_xg_edge"] = df["home_xg"] - df["away_xg"]
    df["abs_xg_edge"] = df["home_xg_edge"].abs()

    df["prediction_profile"] = "balanced"

    df.loc[df["is_draw_heavy"], "prediction_profile"] = "draw_heavy"
    df.loc[df["is_low_confidence"], "prediction_profile"] = "low_confidence"
    df.loc[df["is_high_confidence"], "prediction_profile"] = "higher_confidence"

    df["calibration_warning"] = ""

    df.loc[
        df["most_likely_score"] == "1-1",
        "calibration_warning",
    ] = "most_likely_score_1_1"

    df.loc[
        (df["most_likely_score"] == "1-1") & (df["confidence_pct"] >= 45),
        "calibration_warning",
    ] = "confident_but_scoreline_flat"

    df = df.sort_values(
        by=["prematch_intelligence_score", "confidence_pct"],
        ascending=[False, False],
    ).reset_index(drop=True)

    return df


def summarise_predictions(df):
    if df.empty:
        return {}

    summary = {
        "remaining_fixtures": len(df),
        "avg_home_win_prob_pct": df["home_win_prob"].mean() * 100,
        "avg_draw_prob_pct": df["draw_prob"].mean() * 100,
        "avg_away_win_prob_pct": df["away_win_prob"].mean() * 100,
        "avg_confidence_pct": df["confidence_pct"].mean(),
        "avg_result_probability_spread_pct": df["result_probability_spread"].mean() * 100,
        "draw_heavy_rate_pct": df["is_draw_heavy"].mean() * 100,
        "low_confidence_rate_pct": df["is_low_confidence"].mean() * 100,
        "high_confidence_rate_pct": df["is_high_confidence"].mean() * 100,
        "one_one_mls_rate_pct": (df["most_likely_score"] == "1-1").mean() * 100,
    }

    return summary


def build_profile_summary(df):
    if df.empty:
        return pd.DataFrame()

    return (
        df.groupby("prediction_profile", dropna=False)
        .agg(
            fixtures=("fixture_id", "count"),
            avg_confidence_pct=("confidence_pct", "mean"),
            avg_draw_prob_pct=("draw_prob_pct", "mean"),
            avg_home_xg=("home_xg", "mean"),
            avg_away_xg=("away_xg", "mean"),
            avg_abs_xg_edge=("abs_xg_edge", "mean"),
        )
        .reset_index()
    )


def build_warning_summary(df):
    if df.empty:
        return pd.DataFrame()

    return (
        df.groupby("calibration_warning", dropna=False)
        .agg(
            fixtures=("fixture_id", "count"),
            avg_confidence_pct=("confidence_pct", "mean"),
            avg_draw_prob_pct=("draw_prob_pct", "mean"),
        )
        .reset_index()
        .sort_values("fixtures", ascending=False)
    )


def build_summary_report(df, summary, profile_summary, warning_summary, model_edge):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 28 LIVE PREDICTION CALIBRATION REVIEW")
    lines.append("=" * 80)

    lines.append(f"Remaining fixtures reviewed: {summary.get('remaining_fixtures', 0):,}")
    lines.append(f"Average home win probability: {summary.get('avg_home_win_prob_pct', 0):.2f}%")
    lines.append(f"Average draw probability:     {summary.get('avg_draw_prob_pct', 0):.2f}%")
    lines.append(f"Average away win probability: {summary.get('avg_away_win_prob_pct', 0):.2f}%")
    lines.append(f"Average confidence:           {summary.get('avg_confidence_pct', 0):.2f}%")
    lines.append(f"Draw-heavy fixture rate:      {summary.get('draw_heavy_rate_pct', 0):.2f}%")
    lines.append(f"Low-confidence fixture rate:  {summary.get('low_confidence_rate_pct', 0):.2f}%")
    lines.append(f"High-confidence fixture rate: {summary.get('high_confidence_rate_pct', 0):.2f}%")
    lines.append(f"1-1 most-likely-score rate:   {summary.get('one_one_mls_rate_pct', 0):.2f}%")

    lines.append("-" * 80)
    lines.append("PREDICTION PROFILE SUMMARY")
    lines.append("-" * 80)

    if profile_summary.empty:
        lines.append("No profile summary available.")
    else:
        lines.append(profile_summary.to_string(index=False))

    lines.append("-" * 80)
    lines.append("CALIBRATION WARNING SUMMARY")
    lines.append("-" * 80)

    if warning_summary.empty:
        lines.append("No warning summary available.")
    else:
        lines.append(warning_summary.to_string(index=False))

    lines.append("-" * 80)
    lines.append("TOP 15 FIXTURES NEEDING CALIBRATION ATTENTION")
    lines.append("-" * 80)

    attention = df.sort_values(
        by=["calibration_warning", "prematch_intelligence_score", "confidence_pct"],
        ascending=[False, False, False],
    ).head(15)

    for _, row in attention.iterrows():
        lines.append(
            f"{row['home_team']} v {row['away_team']} | "
            f"Prediction {row['predicted_result']} | "
            f"H {row['home_win_prob_pct']:.2f}% / D {row['draw_prob_pct']:.2f}% / "
            f"A {row['away_win_prob_pct']:.2f}% | MLS {row['most_likely_score']} | "
            f"Warning: {row['calibration_warning']}"
        )

    lines.append("-" * 80)
    lines.append("MODEL EDGE CONTEXT")
    lines.append("-" * 80)

    if model_edge.empty:
        lines.append("No model edge tracker data available.")
    else:
        result_accuracy = model_edge["model_correct_int"].mean() * 100
        score_accuracy = model_edge["scoreline_correct_int"].mean() * 100
        upset_rate = model_edge["upset_flag"].mean() * 100

        lines.append(f"Historical result accuracy so far: {result_accuracy:.2f}%")
        lines.append(f"Historical scoreline accuracy:     {score_accuracy:.2f}%")
        lines.append(f"Historical upset rate:             {upset_rate:.2f}%")

    lines.append("-" * 80)
    lines.append("QUANT'S CALIBRATION VERDICT")
    lines.append("-" * 80)

    one_one_rate = summary.get("one_one_mls_rate_pct", 0)
    draw_rate = summary.get("draw_heavy_rate_pct", 0)

    if one_one_rate > 50:
        lines.append(
            "The live prediction engine is too concentrated around 1-1 scorelines. "
            "This suggests the current Poisson settings are too symmetrical or too compressed."
        )

    if draw_rate > 50:
        lines.append(
            "The model is also draw-heavy. That may be reasonable in some balanced fixtures, "
            "but across the full remaining schedule it suggests we should recalibrate base goals "
            "or widen team-strength differences."
        )

    lines.append(
        "The next improvement should be to make Script 27 more expressive by introducing "
        "dynamic base goals, wider attack/defence scaling, or a form/pressure adjustment."
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 28 - LIVE PREDICTION CALIBRATION REVIEW")
    print("=" * 80)

    live_predictions, prematch, model_edge = load_data()

    if live_predictions.empty:
        print("[STOP] No live predictions found. Run Script 27 first.")
        return

    calibration = build_calibration_table(live_predictions, prematch)
    summary = summarise_predictions(calibration)
    profile_summary = build_profile_summary(calibration)
    warning_summary = build_warning_summary(calibration)

    calibration.to_csv(CALIBRATION_OUTPUT_PATH, index=False)

    report = build_summary_report(
        df=calibration,
        summary=summary,
        profile_summary=profile_summary,
        warning_summary=warning_summary,
        model_edge=model_edge,
    )

    SUMMARY_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)

    print("-" * 80)
    print(f"Calibration review saved: {CALIBRATION_OUTPUT_PATH}")
    print(f"Summary saved:            {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
