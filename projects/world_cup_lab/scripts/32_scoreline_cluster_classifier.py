"""
================================================================================
WORLD CUP LAB
SCRIPT 32 - SCORELINE CLUSTER CLASSIFIER
================================================================================

Purpose:
    Convert scoreline shortlists into fixture-level match-shape clusters.

Inputs:
    outputs/scoreline_shortlist.csv

Outputs:
    outputs/scoreline_cluster_classifier.csv
    outputs/scoreline_cluster_classifier_report.txt
================================================================================
"""

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "outputs"

SHORTLIST_PATH = OUTPUT_DIR / "scoreline_shortlist.csv"

CLUSTER_OUTPUT_PATH = OUTPUT_DIR / "scoreline_cluster_classifier.csv"
REPORT_OUTPUT_PATH = OUTPUT_DIR / "scoreline_cluster_classifier_report.txt"


def safe_read_csv(path):
    if path.exists():
        return pd.read_csv(path)

    print(f"[WARNING] Missing file: {path}")
    return pd.DataFrame()


def classify_cluster(row):
    home_mass = row["home_win_mass_pct"]
    draw_mass = row["draw_mass_pct"]
    away_mass = row["away_win_mass_pct"]

    top_mass = max(home_mass, draw_mass, away_mass)
    second_mass = sorted([home_mass, draw_mass, away_mass], reverse=True)[1]
    gap = top_mass - second_mass

    if top_mass < 18:
        return "volatile_cluster"

    if gap < 4:
        return "balanced_cluster"

    if draw_mass == top_mass:
        return "draw_cluster"

    if home_mass == top_mass:
        return "home_win_cluster"

    if away_mass == top_mass:
        return "away_win_cluster"

    return "unknown_cluster"


def build_cluster_table(shortlist):
    grouped = (
        shortlist.groupby(
            ["fixture_id", "group", "home_team", "away_team"],
            dropna=False,
        )
        .agg(
            home_xg=("home_xg", "first"),
            away_xg=("away_xg", "first"),
            prematch_intelligence_score=("prematch_intelligence_score", "first"),
            fixture_tags=("fixture_tags", "first"),
            top_5_probability_mass_pct=("scoreline_probability_pct", "sum"),
            top_scoreline=("scoreline", "first"),
            top_scoreline_probability_pct=("scoreline_probability_pct", "first"),
            top_scoreline_result=("scoreline_result", "first"),
            scoreline_options=("scoreline", lambda x: ", ".join(x.astype(str))),
        )
        .reset_index()
    )

    result_mass = (
        shortlist.groupby(
            ["fixture_id", "scoreline_result"],
            dropna=False,
        )["scoreline_probability_pct"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
    )

    for col in ["home_win", "draw", "away_win"]:
        if col not in result_mass.columns:
            result_mass[col] = 0

    result_mass = result_mass.rename(
        columns={
            "home_win": "home_win_mass_pct",
            "draw": "draw_mass_pct",
            "away_win": "away_win_mass_pct",
        }
    )

    clusters = grouped.merge(result_mass, on="fixture_id", how="left")

    clusters["cluster_type"] = clusters.apply(classify_cluster, axis=1)

    clusters["cluster_strength_pct"] = clusters[
        ["home_win_mass_pct", "draw_mass_pct", "away_win_mass_pct"]
    ].max(axis=1)

    clusters["cluster_gap_pct"] = clusters.apply(
        lambda row: max(
            row["home_win_mass_pct"],
            row["draw_mass_pct"],
            row["away_win_mass_pct"],
        )
        - sorted(
            [
                row["home_win_mass_pct"],
                row["draw_mass_pct"],
                row["away_win_mass_pct"],
            ],
            reverse=True,
        )[1],
        axis=1,
    )

    clusters = clusters.sort_values(
        by=["prematch_intelligence_score", "cluster_strength_pct"],
        ascending=[False, False],
    ).reset_index(drop=True)

    clusters["cluster_rank"] = clusters.index + 1

    return clusters


def build_report(clusters):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 32 SCORELINE CLUSTER CLASSIFIER")
    lines.append("=" * 80)

    lines.append(f"Fixtures classified: {len(clusters):,}")

    lines.append("")
    lines.append("=" * 80)
    lines.append("CLUSTER TYPE SUMMARY")
    lines.append("=" * 80)

    summary = (
        clusters.groupby("cluster_type")
        .agg(
            fixtures=("fixture_id", "count"),
            avg_cluster_strength_pct=("cluster_strength_pct", "mean"),
            avg_cluster_gap_pct=("cluster_gap_pct", "mean"),
            avg_top_5_mass_pct=("top_5_probability_mass_pct", "mean"),
        )
        .reset_index()
        .sort_values("fixtures", ascending=False)
    )

    lines.append(summary.to_string(index=False))

    lines.append("")
    lines.append("=" * 80)
    lines.append("TOP 15 FIXTURE CLUSTERS")
    lines.append("=" * 80)

    for _, row in clusters.head(15).iterrows():
        lines.append(
            f"{int(row['cluster_rank'])}. {row['home_team']} v {row['away_team']} "
            f"| Group {row['group']} | {row['cluster_type']} | "
            f"Strength {row['cluster_strength_pct']:.2f}% | Gap {row['cluster_gap_pct']:.2f}%"
        )
        lines.append(
            f"   xG {row['home_xg']:.2f}-{row['away_xg']:.2f} | "
            f"Top scores: {row['scoreline_options']}"
        )
        lines.append(
            f"   Mass: home {row['home_win_mass_pct']:.2f}% / "
            f"draw {row['draw_mass_pct']:.2f}% / "
            f"away {row['away_win_mass_pct']:.2f}%"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("STRONGEST HOME-WIN CLUSTERS")
    lines.append("=" * 80)

    home_clusters = clusters[clusters["cluster_type"] == "home_win_cluster"].head(10)

    if home_clusters.empty:
        lines.append("No strong home-win clusters found.")
    else:
        for _, row in home_clusters.iterrows():
            lines.append(
                f"- {row['home_team']} v {row['away_team']} | "
                f"home mass {row['home_win_mass_pct']:.2f}% | "
                f"Top scores: {row['scoreline_options']}"
            )

    lines.append("")
    lines.append("=" * 80)
    lines.append("STRONGEST AWAY-WIN CLUSTERS")
    lines.append("=" * 80)

    away_clusters = clusters[clusters["cluster_type"] == "away_win_cluster"].head(10)

    if away_clusters.empty:
        lines.append("No strong away-win clusters found.")
    else:
        for _, row in away_clusters.iterrows():
            lines.append(
                f"- {row['home_team']} v {row['away_team']} | "
                f"away mass {row['away_win_mass_pct']:.2f}% | "
                f"Top scores: {row['scoreline_options']}"
            )

    lines.append("")
    lines.append("=" * 80)
    lines.append("BALANCED / VOLATILE MATCHES")
    lines.append("=" * 80)

    balanced = clusters[
        clusters["cluster_type"].isin(["balanced_cluster", "volatile_cluster"])
    ].head(12)

    if balanced.empty:
        lines.append("No balanced or volatile matches found.")
    else:
        for _, row in balanced.iterrows():
            lines.append(
                f"- {row['home_team']} v {row['away_team']} | {row['cluster_type']} | "
                f"home {row['home_win_mass_pct']:.2f}% / "
                f"draw {row['draw_mass_pct']:.2f}% / "
                f"away {row['away_win_mass_pct']:.2f}%"
            )

    lines.append("")
    lines.append("=" * 80)
    lines.append("QUANT'S CLUSTER VERDICT")
    lines.append("=" * 80)

    lines.append(
        "The cluster classifier converts scoreline probabilities into match-shape signals. "
        "This is more useful than a single scoreline because it shows whether the model "
        "leans toward a home win, away win, draw, or genuinely balanced fixture."
    )

    lines.append(
        "The next practical use is to compare these cluster classifications against actual "
        "future results to learn which cluster types are most reliable."
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 32 - SCORELINE CLUSTER CLASSIFIER")
    print("=" * 80)

    shortlist = safe_read_csv(SHORTLIST_PATH)

    if shortlist.empty:
        print("[STOP] No scoreline shortlist found. Run Script 31 first.")
        return

    clusters = build_cluster_table(shortlist)

    clusters.to_csv(CLUSTER_OUTPUT_PATH, index=False)

    report = build_report(clusters)
    REPORT_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)

    print("-" * 80)
    print(f"Cluster classifier saved: {CLUSTER_OUTPUT_PATH}")
    print(f"Report saved:             {REPORT_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
