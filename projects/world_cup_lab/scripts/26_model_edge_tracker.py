"""
================================================================================
WORLD CUP LAB
SCRIPT 26 - MODEL EDGE TRACKER
================================================================================

Purpose:
    Track whether the model is adding useful signal.

Inputs:
    outputs/prediction_accuracy_review.csv
    outputs/team_watchlist.csv
    outputs/team_rating_adjustment_review.csv
    data/world_cup_2026/actual_results.csv

Outputs:
    outputs/model_edge_tracker.csv
    outputs/model_edge_tracker_summary.txt
================================================================================
"""

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

PREDICTION_REVIEW_PATH = OUTPUT_DIR / "prediction_accuracy_review.csv"
WATCHLIST_PATH = OUTPUT_DIR / "team_watchlist.csv"
RATING_REVIEW_PATH = OUTPUT_DIR / "team_rating_adjustment_review.csv"
ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"

EDGE_OUTPUT_PATH = OUTPUT_DIR / "model_edge_tracker.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "model_edge_tracker_summary.txt"


def safe_read_csv(path):
    if path.exists():
        return pd.read_csv(path)
    print(f"[WARNING] Missing file: {path}")
    return pd.DataFrame()


def load_data():
    predictions = safe_read_csv(PREDICTION_REVIEW_PATH)
    watchlist = safe_read_csv(WATCHLIST_PATH)
    ratings = safe_read_csv(RATING_REVIEW_PATH)
    actuals = safe_read_csv(ACTUAL_RESULTS_PATH)

    return predictions, watchlist, ratings, actuals


def build_edge_table(predictions):
    edge = predictions.copy()

    edge["model_correct_int"] = edge["result_correct"].astype(int)
    edge["scoreline_correct_int"] = edge["scoreline_correct"].astype(int)

    edge["predicted_result_probability"] = edge[
        ["home_win_prob", "draw_prob", "away_win_prob"]
    ].max(axis=1)

    edge["actual_result_probability_bucket"] = pd.cut(
        edge["actual_result_probability"],
        bins=[0, 0.25, 0.35, 0.45, 0.55, 1.0],
        labels=["0-25%", "25-35%", "35-45%", "45-55%", "55%+"],
        include_lowest=True,
    )

    edge["predicted_result_probability_bucket"] = pd.cut(
        edge["predicted_result_probability"],
        bins=[0, 0.35, 0.40, 0.45, 0.50, 1.0],
        labels=["0-35%", "35-40%", "40-45%", "45-50%", "50%+"],
        include_lowest=True,
    )

    edge["upset_flag"] = edge["actual_result_probability"] < 0.35
    edge["strong_model_view"] = edge["predicted_result_probability"] >= 0.45

    return edge


def summarise_group(df, group_col):
    if df.empty or group_col not in df.columns:
        return pd.DataFrame()

    grouped = (
        df.groupby(group_col, dropna=False)
        .agg(
            matches=("fixture_id", "count"),
            result_accuracy_pct=("model_correct_int", lambda x: x.mean() * 100),
            exact_score_accuracy_pct=("scoreline_correct_int", lambda x: x.mean() * 100),
            avg_actual_result_probability_pct=("actual_result_probability", lambda x: x.mean() * 100),
            avg_predicted_result_probability_pct=("predicted_result_probability", lambda x: x.mean() * 100),
            upset_rate_pct=("upset_flag", lambda x: x.mean() * 100),
        )
        .reset_index()
    )

    return grouped


def build_watchlist_summary(watchlist):
    if watchlist.empty:
        return pd.DataFrame()

    summary = (
        watchlist.groupby("watchlist_flag", dropna=False)
        .agg(
            teams=("team", "count"),
            avg_actual_points=("actual_points", "mean"),
            avg_expected_points=("expected_points", "mean"),
            avg_points_vs_expected=("points_vs_expected", "mean"),
            avg_goal_difference=("goal_difference", "mean"),
        )
        .reset_index()
    )

    return summary


def build_rating_summary(ratings):
    if ratings.empty:
        return pd.DataFrame()

    played = ratings[ratings["matches_played"] > 0].copy()

    if played.empty:
        return pd.DataFrame()

    summary = pd.DataFrame(
        [
            {
                "teams_played": len(played),
                "avg_rating_change": played["rating_change"].mean(),
                "max_positive_rating_change": played["rating_change"].max(),
                "max_negative_rating_change": played["rating_change"].min(),
                "avg_performance_vs_expected_per_match": played[
                    "performance_vs_expected_per_match"
                ].mean(),
            }
        ]
    )

    return summary


def build_summary_report(edge, bucket_summary, watchlist_summary, rating_summary):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 26 MODEL EDGE TRACKER")
    lines.append("=" * 80)

    lines.append(f"Matches reviewed: {len(edge):,}")

    if not edge.empty:
        lines.append(f"Overall result accuracy: {edge['model_correct_int'].mean() * 100:.2f}%")
        lines.append(f"Exact scoreline accuracy: {edge['scoreline_correct_int'].mean() * 100:.2f}%")
        lines.append(
            f"Average probability assigned to actual result: "
            f"{edge['actual_result_probability'].mean() * 100:.2f}%"
        )
        lines.append(
            f"Average predicted-result confidence: "
            f"{edge['predicted_result_probability'].mean() * 100:.2f}%"
        )
        lines.append(f"Upset rate: {edge['upset_flag'].mean() * 100:.2f}%")

    lines.append("-" * 80)
    lines.append("PROBABILITY BUCKET PERFORMANCE")
    lines.append("-" * 80)

    if bucket_summary.empty:
        lines.append("No bucket summary available.")
    else:
        lines.append(bucket_summary.to_string(index=False))

    lines.append("-" * 80)
    lines.append("WATCHLIST EDGE SUMMARY")
    lines.append("-" * 80)

    if watchlist_summary.empty:
        lines.append("No watchlist summary available.")
    else:
        lines.append(watchlist_summary.to_string(index=False))

    lines.append("-" * 80)
    lines.append("RATING ADJUSTMENT SUMMARY")
    lines.append("-" * 80)

    if rating_summary.empty:
        lines.append("No rating summary available.")
    else:
        lines.append(rating_summary.to_string(index=False))

    lines.append("-" * 80)
    lines.append("QUANT'S MODEL VERDICT")
    lines.append("-" * 80)

    if not edge.empty:
        accuracy = edge["model_correct_int"].mean() * 100
        upset_rate = edge["upset_flag"].mean() * 100

        if accuracy >= 50:
            lines.append(
                "The model is currently showing useful directional signal, "
                "but the sample size remains small."
            )
        else:
            lines.append(
                "The model is currently below 50% result accuracy, but this does not "
                "automatically mean it is poor. The tournament has already produced "
                "several draw and upset results that are valuable for calibration."
            )

        if upset_rate >= 40:
            lines.append(
                "The high upset rate suggests the model may be overconfident in stronger-rated teams "
                "or underweighting tournament-specific factors such as pressure, travel, venue, "
                "rotation, and early-stage volatility."
            )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 26 - MODEL EDGE TRACKER")
    print("=" * 80)

    predictions, watchlist, ratings, actuals = load_data()

    if predictions.empty:
        print("[STOP] No prediction review available. Run Script 14 first.")
        return

    edge = build_edge_table(predictions)

    bucket_summary = summarise_group(edge, "predicted_result_probability_bucket")
    watchlist_summary = build_watchlist_summary(watchlist)
    rating_summary = build_rating_summary(ratings)

    edge.to_csv(EDGE_OUTPUT_PATH, index=False)

    summary = build_summary_report(
        edge=edge,
        bucket_summary=bucket_summary,
        watchlist_summary=watchlist_summary,
        rating_summary=rating_summary,
    )

    SUMMARY_OUTPUT_PATH.write_text(summary, encoding="utf-8")

    print(summary)

    print("-" * 80)
    print(f"Model edge table saved: {EDGE_OUTPUT_PATH}")
    print(f"Summary saved:          {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
