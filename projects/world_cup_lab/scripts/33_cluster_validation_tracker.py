"""
================================================================================
WORLD CUP LAB
SCRIPT 33 - CLUSTER VALIDATION TRACKER
================================================================================

Purpose:
    Validate whether scoreline cluster classifications are proving useful once
    actual results are known.

Inputs:
    outputs/scoreline_cluster_classifier.csv
    data/world_cup_2026/actual_results.csv

Outputs:
    outputs/cluster_validation_tracker.csv
    outputs/cluster_validation_summary.txt
================================================================================
"""

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

CLUSTERS_PATH = OUTPUT_DIR / "scoreline_cluster_classifier.csv"
ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"

VALIDATION_OUTPUT_PATH = OUTPUT_DIR / "cluster_validation_tracker.csv"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "cluster_validation_summary.txt"


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


def expected_result_from_cluster(cluster_type):
    if cluster_type == "home_win_cluster":
        return "home_win"
    if cluster_type == "away_win_cluster":
        return "away_win"
    if cluster_type == "draw_cluster":
        return "draw"
    return "uncertain"


def validate_clusters(clusters, actuals):
    actuals = actuals.copy()

    if "actual_result" not in actuals.columns:
        actuals["actual_result"] = actuals.apply(
            lambda row: classify_result(row["home_goals"], row["away_goals"]),
            axis=1,
        )

    validation = clusters.merge(
        actuals[
            [
                "fixture_id",
                "home_goals",
                "away_goals",
                "actual_result",
                "observer_notes",
            ]
        ],
        on="fixture_id",
        how="inner",
    )

    validation["expected_cluster_result"] = validation["cluster_type"].apply(
        expected_result_from_cluster
    )

    validation["cluster_result_correct"] = (
        validation["expected_cluster_result"] == validation["actual_result"]
    )

    validation.loc[
        validation["expected_cluster_result"] == "uncertain",
        "cluster_result_correct",
    ] = pd.NA

    validation["actual_scoreline"] = (
        validation["home_goals"].astype(str) + "-" + validation["away_goals"].astype(str)
    )

    validation["top_scoreline_correct"] = (
        validation["top_scoreline"] == validation["actual_scoreline"]
    )

    validation["actual_in_scoreline_options"] = validation.apply(
        lambda row: str(row["actual_scoreline"]) in str(row["scoreline_options"]).split(", "),
        axis=1,
    )

    return validation


def build_cluster_summary(validation):
    if validation.empty:
        return pd.DataFrame()

    return (
        validation.groupby("cluster_type", dropna=False)
        .agg(
            matches=("fixture_id", "count"),
            correct_cluster_results=("cluster_result_correct", lambda x: x.fillna(False).sum()),
            cluster_result_accuracy_pct=("cluster_result_correct", lambda x: x.dropna().mean() * 100 if len(x.dropna()) else None),
            top_scoreline_accuracy_pct=("top_scoreline_correct", lambda x: x.mean() * 100),
            shortlist_hit_rate_pct=("actual_in_scoreline_options", lambda x: x.mean() * 100),
            avg_cluster_strength_pct=("cluster_strength_pct", "mean"),
            avg_cluster_gap_pct=("cluster_gap_pct", "mean"),
        )
        .reset_index()
        .sort_values("matches", ascending=False)
    )


def build_summary_report(validation, cluster_summary):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 33 CLUSTER VALIDATION TRACKER")
    lines.append("=" * 80)

    lines.append(f"Validated fixtures: {len(validation):,}")

    if not validation.empty:
        directional = validation.dropna(subset=["cluster_result_correct"])

        if not directional.empty:
            lines.append(
                f"Directional cluster accuracy: "
                f"{directional['cluster_result_correct'].mean() * 100:.2f}%"
            )

        lines.append(
            f"Top scoreline accuracy: "
            f"{validation['top_scoreline_correct'].mean() * 100:.2f}%"
        )
        lines.append(
            f"Top-5 shortlist hit rate: "
            f"{validation['actual_in_scoreline_options'].mean() * 100:.2f}%"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("CLUSTER TYPE PERFORMANCE")
    lines.append("=" * 80)

    if cluster_summary.empty:
        lines.append("No cluster summary available.")
    else:
        lines.append(cluster_summary.to_string(index=False))

    lines.append("")
    lines.append("=" * 80)
    lines.append("MATCH-BY-MATCH VALIDATION")
    lines.append("=" * 80)

    if validation.empty:
        lines.append("No completed cluster-classified fixtures available yet.")
    else:
        for _, row in validation.sort_values("fixture_id").iterrows():
            lines.append(
                f"{row['home_team']} {row['home_goals']}-{row['away_goals']} {row['away_team']} | "
                f"Cluster: {row['cluster_type']} | "
                f"Expected: {row['expected_cluster_result']} | "
                f"Actual: {row['actual_result']} | "
                f"Top score: {row['top_scoreline']} | "
                f"Actual in shortlist: {row['actual_in_scoreline_options']}"
            )

    lines.append("")
    lines.append("=" * 80)
    lines.append("QUANT'S CLUSTER VALIDATION VERDICT")
    lines.append("=" * 80)

    lines.append(
        "This tracker validates the match-shape layer rather than only the raw result prediction. "
        "The key metric is not just whether the top scoreline was correct, but whether the actual "
        "scoreline appeared inside the shortlist and whether the cluster direction matched the result."
    )

    lines.append(
        "As more results are added, this will show whether home-win, away-win, balanced, and volatile "
        "clusters carry useful predictive information."
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 33 - CLUSTER VALIDATION TRACKER")
    print("=" * 80)

    clusters = safe_read_csv(CLUSTERS_PATH)
    actuals = safe_read_csv(ACTUAL_RESULTS_PATH)

    if clusters.empty or actuals.empty:
        print("[STOP] Missing clusters or actual results.")
        return

    validation = validate_clusters(clusters, actuals)
    validation.to_csv(VALIDATION_OUTPUT_PATH, index=False)

    cluster_summary = build_cluster_summary(validation)

    report = build_summary_report(validation, cluster_summary)
    SUMMARY_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)

    print("-" * 80)
    print(f"Cluster validation saved: {VALIDATION_OUTPUT_PATH}")
    print(f"Summary saved:            {SUMMARY_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
