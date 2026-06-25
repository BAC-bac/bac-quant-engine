"""
================================================================================
WORLD CUP LAB
SCRIPT 30 - LIVE PREDICTION COMPARISON REPORT
================================================================================

Purpose:
    Compare original live predictions from Script 27 against recalibrated live
    predictions from Script 29.

Inputs:
    outputs/live_fixture_predictions.csv
    outputs/recalibrated_live_fixture_predictions.csv
    outputs/prematch_intelligence_ranked.csv

Outputs:
    outputs/live_prediction_comparison_report.csv
    outputs/live_prediction_comparison_summary.txt
================================================================================
"""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "outputs"

ORIGINAL_PATH = OUTPUT_DIR / "live_fixture_predictions.csv"
RECALIBRATED_PATH = OUTPUT_DIR / "recalibrated_live_fixture_predictions.csv"
PREMATCH_PATH = OUTPUT_DIR / "prematch_intelligence_ranked.csv"

COMPARISON_OUTPUT_PATH = OUTPUT_DIR / "live_prediction_comparison_report.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "live_prediction_comparison_summary.txt"


def safe_read_csv(path):
    if path.exists():
        return pd.read_csv(path)

    print(f"[WARNING] Missing file: {path}")
    return pd.DataFrame()


def load_data():
    original = safe_read_csv(ORIGINAL_PATH)
    recalibrated = safe_read_csv(RECALIBRATED_PATH)
    prematch = safe_read_csv(PREMATCH_PATH)

    return original, recalibrated, prematch


def prepare_prediction_frame(df, prefix):
    keep_cols = [
        "fixture_id",
        "group",
        "home_team",
        "away_team",
        "home_xg",
        "away_xg",
        "home_win_prob",
        "draw_prob",
        "away_win_prob",
        "predicted_result",
        "most_likely_score",
        "most_likely_score_prob",
        "confidence_pct",
    ]

    existing_cols = [col for col in keep_cols if col in df.columns]

    prepared = df[existing_cols].copy()

    rename_map = {
        col: f"{prefix}_{col}"
        for col in existing_cols
        if col not in ["fixture_id", "group", "home_team", "away_team"]
    }

    prepared = prepared.rename(columns=rename_map)

    return prepared


def build_comparison(original, recalibrated, prematch):
    original_prepared = prepare_prediction_frame(original, "original")
    recalibrated_prepared = prepare_prediction_frame(recalibrated, "recalibrated")

    comparison = original_prepared.merge(
        recalibrated_prepared,
        on=["fixture_id", "group", "home_team", "away_team"],
        how="inner",
    )

    if not prematch.empty:
        prematch_cols = [
            "fixture_id",
            "prematch_intelligence_score",
            "fixture_tags",
            "rank",
        ]

        existing = [col for col in prematch_cols if col in prematch.columns]

        comparison = comparison.merge(
            prematch[existing],
            on="fixture_id",
            how="left",
        )

    comparison["result_prediction_changed"] = (
        comparison["original_predicted_result"]
        != comparison["recalibrated_predicted_result"]
    )

    comparison["scoreline_prediction_changed"] = (
        comparison["original_most_likely_score"]
        != comparison["recalibrated_most_likely_score"]
    )

    comparison["confidence_change_pct"] = (
        comparison["recalibrated_confidence_pct"]
        - comparison["original_confidence_pct"]
    )

    comparison["draw_prob_change_pct"] = (
        comparison["recalibrated_draw_prob"]
        - comparison["original_draw_prob"]
    ) * 100

    comparison["home_win_prob_change_pct"] = (
        comparison["recalibrated_home_win_prob"]
        - comparison["original_home_win_prob"]
    ) * 100

    comparison["away_win_prob_change_pct"] = (
        comparison["recalibrated_away_win_prob"]
        - comparison["original_away_win_prob"]
    ) * 100

    comparison["original_xg_edge"] = (
        comparison["original_home_xg"] - comparison["original_away_xg"]
    )

    comparison["recalibrated_xg_edge"] = (
        comparison["recalibrated_home_xg"] - comparison["recalibrated_away_xg"]
    )

    comparison["xg_edge_change"] = (
        comparison["recalibrated_xg_edge"] - comparison["original_xg_edge"]
    )

    comparison["original_was_1_1"] = comparison["original_most_likely_score"] == "1-1"
    comparison["recalibrated_is_1_1"] = comparison["recalibrated_most_likely_score"] == "1-1"

    comparison["one_one_removed"] = (
        comparison["original_was_1_1"] & ~comparison["recalibrated_is_1_1"]
    )

    comparison["comparison_interest_score"] = (
        comparison["result_prediction_changed"].astype(int) * 5
        + comparison["scoreline_prediction_changed"].astype(int) * 3
        + comparison["one_one_removed"].astype(int) * 4
        + comparison["confidence_change_pct"].abs() * 0.25
        + comparison["draw_prob_change_pct"].abs() * 0.35
        + comparison["xg_edge_change"].abs() * 2
    )

    comparison = comparison.sort_values(
        by="comparison_interest_score",
        ascending=False,
    ).reset_index(drop=True)

    comparison["comparison_rank"] = comparison.index + 1

    return comparison


def summarise_comparison(comparison):
    if comparison.empty:
        return {}

    summary = {
        "fixtures_compared": len(comparison),
        "result_prediction_changed_pct": comparison["result_prediction_changed"].mean() * 100,
        "scoreline_prediction_changed_pct": comparison["scoreline_prediction_changed"].mean() * 100,
        "avg_confidence_change_pct": comparison["confidence_change_pct"].mean(),
        "avg_abs_confidence_change_pct": comparison["confidence_change_pct"].abs().mean(),
        "avg_draw_prob_change_pct": comparison["draw_prob_change_pct"].mean(),
        "original_1_1_rate_pct": comparison["original_was_1_1"].mean() * 100,
        "recalibrated_1_1_rate_pct": comparison["recalibrated_is_1_1"].mean() * 100,
        "one_one_removed_pct": comparison["one_one_removed"].mean() * 100,
    }

    return summary


def build_result_change_summary(comparison):
    if comparison.empty:
        return pd.DataFrame()

    changed = comparison[comparison["result_prediction_changed"]].copy()

    if changed.empty:
        return pd.DataFrame()

    return (
        changed.groupby(
            ["original_predicted_result", "recalibrated_predicted_result"],
            dropna=False,
        )
        .agg(fixtures=("fixture_id", "count"))
        .reset_index()
        .sort_values("fixtures", ascending=False)
    )


def build_scoreline_change_summary(comparison):
    if comparison.empty:
        return pd.DataFrame()

    changed = comparison[comparison["scoreline_prediction_changed"]].copy()

    if changed.empty:
        return pd.DataFrame()

    return (
        changed.groupby(
            ["original_most_likely_score", "recalibrated_most_likely_score"],
            dropna=False,
        )
        .agg(fixtures=("fixture_id", "count"))
        .reset_index()
        .sort_values("fixtures", ascending=False)
    )


def build_summary_report(
    comparison,
    summary,
    result_change_summary,
    scoreline_change_summary,
):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 30 LIVE PREDICTION COMPARISON REPORT")
    lines.append("=" * 80)

    lines.append(f"Fixtures compared: {summary.get('fixtures_compared', 0):,}")
    lines.append(
        f"Result prediction changed: "
        f"{summary.get('result_prediction_changed_pct', 0):.2f}%"
    )
    lines.append(
        f"Scoreline prediction changed: "
        f"{summary.get('scoreline_prediction_changed_pct', 0):.2f}%"
    )
    lines.append(
        f"Average confidence change: "
        f"{summary.get('avg_confidence_change_pct', 0):+.2f} percentage points"
    )
    lines.append(
        f"Average absolute confidence change: "
        f"{summary.get('avg_abs_confidence_change_pct', 0):.2f} percentage points"
    )
    lines.append(
        f"Average draw probability change: "
        f"{summary.get('avg_draw_prob_change_pct', 0):+.2f} percentage points"
    )
    lines.append(
        f"Original 1-1 MLS rate: "
        f"{summary.get('original_1_1_rate_pct', 0):.2f}%"
    )
    lines.append(
        f"Recalibrated 1-1 MLS rate: "
        f"{summary.get('recalibrated_1_1_rate_pct', 0):.2f}%"
    )
    lines.append(
        f"1-1 removed rate: "
        f"{summary.get('one_one_removed_pct', 0):.2f}%"
    )

    lines.append("-" * 80)
    lines.append("RESULT PREDICTION CHANGE SUMMARY")
    lines.append("-" * 80)

    if result_change_summary.empty:
        lines.append("No result prediction changes.")
    else:
        lines.append(result_change_summary.to_string(index=False))

    lines.append("-" * 80)
    lines.append("SCORELINE CHANGE SUMMARY")
    lines.append("-" * 80)

    if scoreline_change_summary.empty:
        lines.append("No scoreline prediction changes.")
    else:
        lines.append(scoreline_change_summary.to_string(index=False))

    lines.append("-" * 80)
    lines.append("TOP 20 FIXTURES MOST AFFECTED BY RECALIBRATION")
    lines.append("-" * 80)

    top = comparison.head(20)

    for _, row in top.iterrows():
        lines.append(
            f"{row['home_team']} v {row['away_team']} | Group {row['group']} | "
            f"Original: {row['original_predicted_result']} / "
            f"{row['original_most_likely_score']} / "
            f"{row['original_confidence_pct']:.2f}% | "
            f"Recalibrated: {row['recalibrated_predicted_result']} / "
            f"{row['recalibrated_most_likely_score']} / "
            f"{row['recalibrated_confidence_pct']:.2f}% | "
            f"Draw change {row['draw_prob_change_pct']:+.2f}pp"
        )

    lines.append("-" * 80)
    lines.append("QUANT'S COMPARISON VERDICT")
    lines.append("-" * 80)

    if summary.get("recalibrated_1_1_rate_pct", 0) < summary.get("original_1_1_rate_pct", 0):
        lines.append(
            "The recalibrated model improves scoreline diversity by reducing the 1-1 clustering problem."
        )
    else:
        lines.append(
            "The recalibrated model has not improved scoreline diversity enough."
        )

    if summary.get("avg_draw_prob_change_pct", 0) < 0:
        lines.append(
            "Draw probability has generally decreased, which addresses the draw-heavy issue identified in Script 28."
        )

    lines.append(
        "The next decision is whether to tune the recalibrated engine further or move toward "
        "a scoreline-shortlist approach rather than relying on a single most-likely score."
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 30 - LIVE PREDICTION COMPARISON REPORT")
    print("=" * 80)

    original, recalibrated, prematch = load_data()

    if original.empty or recalibrated.empty:
        print("[STOP] Missing original or recalibrated predictions.")
        return

    comparison = build_comparison(
        original=original,
        recalibrated=recalibrated,
        prematch=prematch,
    )

    comparison.to_csv(COMPARISON_OUTPUT_PATH, index=False)

    summary = summarise_comparison(comparison)
    result_change_summary = build_result_change_summary(comparison)
    scoreline_change_summary = build_scoreline_change_summary(comparison)

    report = build_summary_report(
        comparison=comparison,
        summary=summary,
        result_change_summary=result_change_summary,
        scoreline_change_summary=scoreline_change_summary,
    )

    SUMMARY_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)

    print("-" * 80)
    print(f"Comparison report saved: {COMPARISON_OUTPUT_PATH}")
    print(f"Summary saved:           {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
