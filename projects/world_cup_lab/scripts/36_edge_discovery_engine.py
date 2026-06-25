"""
================================================================================
WORLD CUP LAB
SCRIPT 36 - EDGE DISCOVERY ENGINE
================================================================================

Purpose:
    Discover which World Cup Lab signals are showing predictive value.

Inputs:
    data/world_cup_2026/actual_results.csv
    outputs/cluster_validation_tracker.csv
    outputs/expanded_scoreline_validation_tracker.csv
    outputs/team_watchlist.csv

Outputs:
    outputs/world_cup_edge_discovery.csv
    outputs/world_cup_edge_discovery_report.txt
================================================================================
"""

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
CLUSTER_VALIDATION_PATH = OUTPUT_DIR / "cluster_validation_tracker.csv"
EXPANDED_VALIDATION_PATH = OUTPUT_DIR / "expanded_scoreline_validation_tracker.csv"
WATCHLIST_PATH = OUTPUT_DIR / "team_watchlist.csv"

EDGE_OUTPUT_PATH = OUTPUT_DIR / "world_cup_edge_discovery.csv"
REPORT_OUTPUT_PATH = OUTPUT_DIR / "world_cup_edge_discovery_report.txt"


def safe_read_csv(path):
    if path.exists():
        return pd.read_csv(path)
    print(f"[WARNING] Missing file: {path}")
    return pd.DataFrame()


def add_edge(rows, category, signal, matches, hits, notes):
    if matches == 0:
        accuracy = None
    else:
        accuracy = hits / matches * 100

    rows.append(
        {
            "category": category,
            "signal": signal,
            "matches": matches,
            "hits": hits,
            "misses": matches - hits,
            "accuracy_pct": accuracy,
            "notes": notes,
        }
    )


def build_cluster_edges(cluster_validation):
    rows = []

    if cluster_validation.empty:
        return rows

    directional = cluster_validation[
        cluster_validation["expected_cluster_result"] != "uncertain"
    ].copy()

    for cluster_type, group in directional.groupby("cluster_type"):
        add_edge(
            rows=rows,
            category="cluster",
            signal=cluster_type,
            matches=len(group),
            hits=int(group["cluster_result_correct"].sum()),
            notes="Directional cluster result matched actual result.",
        )

    volatile = cluster_validation[
        cluster_validation["cluster_type"] == "volatile_cluster"
    ].copy()

    if not volatile.empty:
        add_edge(
            rows=rows,
            category="cluster",
            signal="volatile_cluster_top5_hit",
            matches=len(volatile),
            hits=int(volatile["actual_in_scoreline_options"].sum()),
            notes="Volatile cluster actual scoreline appeared in top-5 shortlist.",
        )

    return rows


def build_expanded_edges(expanded_validation):
    rows = []

    if expanded_validation.empty:
        return rows

    add_edge(
        rows,
        "expanded_model",
        "expanded_result_prediction",
        len(expanded_validation),
        int(expanded_validation["result_correct"].sum()),
        "Expanded model predicted correct result direction.",
    )

    add_edge(
        rows,
        "expanded_model",
        "expanded_top8_scoreline_hit",
        len(expanded_validation),
        int(expanded_validation["actual_in_top_8"].sum()),
        "Actual scoreline appeared in expanded top-8 shortlist.",
    )

    margin_2_signals = expanded_validation[
        expanded_validation["predicted_margin_2_plus_signal"]
    ].copy()

    add_edge(
        rows,
        "margin",
        "margin_2_plus_signal",
        len(margin_2_signals),
        int(margin_2_signals["actual_margin_2_plus"].sum()) if not margin_2_signals.empty else 0,
        "Model signalled margin 2+ and actual result had margin 2+.",
    )

    margin_3_signals = expanded_validation[
        expanded_validation["predicted_margin_3_plus_signal"]
    ].copy()

    add_edge(
        rows,
        "margin",
        "margin_3_plus_signal",
        len(margin_3_signals),
        int(margin_3_signals["actual_margin_3_plus"].sum()) if not margin_3_signals.empty else 0,
        "Model signalled margin 3+ and actual result had margin 3+.",
    )

    high_margin_2 = expanded_validation[
        expanded_validation["margin_2_plus_prob_pct"] >= 60
    ].copy()

    add_edge(
        rows,
        "margin",
        "margin_2_plus_prob_ge_60",
        len(high_margin_2),
        int(high_margin_2["actual_margin_2_plus"].sum()) if not high_margin_2.empty else 0,
        "Margin 2+ probability >= 60%.",
    )

    high_margin_3 = expanded_validation[
        expanded_validation["margin_3_plus_prob_pct"] >= 40
    ].copy()

    add_edge(
        rows,
        "margin",
        "margin_3_plus_prob_ge_40",
        len(high_margin_3),
        int(high_margin_3["actual_margin_3_plus"].sum()) if not high_margin_3.empty else 0,
        "Margin 3+ probability >= 40%.",
    )

    over_25 = expanded_validation[expanded_validation["over_25_signal"]].copy()

    add_edge(
        rows,
        "goals",
        "over_25_signal",
        len(over_25),
        int(over_25["actual_over_25"].sum()) if not over_25.empty else 0,
        "Model signalled over 2.5 goals.",
    )

    over_35 = expanded_validation[expanded_validation["over_35_signal"]].copy()

    add_edge(
        rows,
        "goals",
        "over_35_signal",
        len(over_35),
        int(over_35["actual_over_35"].sum()) if not over_35.empty else 0,
        "Model signalled over 3.5 goals.",
    )

    return rows


def build_watchlist_edges(watchlist):
    rows = []

    if watchlist.empty:
        return rows

    for flag, group in watchlist.groupby("watchlist_flag", dropna=False):
        if "actual_points" not in group.columns:
            continue

        positive_points = group["actual_points"] >= 3

        add_edge(
            rows,
            "watchlist",
            str(flag),
            len(group),
            int(positive_points.sum()),
            "Watchlist team achieved 3 points from completed matches.",
        )

    return rows


def build_edge_table(cluster_validation, expanded_validation, watchlist):
    rows = []

    rows.extend(build_cluster_edges(cluster_validation))
    rows.extend(build_expanded_edges(expanded_validation))
    rows.extend(build_watchlist_edges(watchlist))

    edges = pd.DataFrame(rows)

    if edges.empty:
        return edges

    edges["sample_warning"] = edges["matches"].apply(
        lambda x: "small_sample" if x < 10 else "usable_sample"
    )

    edges = edges.sort_values(
        by=["accuracy_pct", "matches"],
        ascending=[False, False],
        na_position="last",
    ).reset_index(drop=True)

    edges["edge_rank"] = edges.index + 1

    return edges


def build_report(edges, cluster_validation, expanded_validation):
    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - SCRIPT 36 EDGE DISCOVERY ENGINE")
    lines.append("=" * 80)

    if edges.empty:
        lines.append("No edge data available yet.")
        return "\n".join(lines)

    lines.append(f"Signals tested: {len(edges):,}")

    lines.append("")
    lines.append("=" * 80)
    lines.append("BEST SIGNALS FOUND")
    lines.append("=" * 80)

    top_edges = edges.head(15)

    for _, row in top_edges.iterrows():
        acc = row["accuracy_pct"]
        acc_text = "N/A" if pd.isna(acc) else f"{acc:.2f}%"

        lines.append(
            f"{int(row['edge_rank'])}. [{row['category']}] {row['signal']} | "
            f"Matches {int(row['matches'])} | Hits {int(row['hits'])} | "
            f"Accuracy {acc_text} | {row['sample_warning']}"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("CLUSTER EDGE SUMMARY")
    lines.append("=" * 80)

    cluster_edges = edges[edges["category"] == "cluster"]

    if cluster_edges.empty:
        lines.append("No cluster edges available.")
    else:
        lines.append(cluster_edges.to_string(index=False))

    lines.append("")
    lines.append("=" * 80)
    lines.append("MARGIN / GOALS EDGE SUMMARY")
    lines.append("=" * 80)

    margin_edges = edges[edges["category"].isin(["margin", "goals"])]

    if margin_edges.empty:
        lines.append("No margin or goals edges available.")
    else:
        lines.append(margin_edges.to_string(index=False))

    lines.append("")
    lines.append("=" * 80)
    lines.append("WATCHLIST EDGE SUMMARY")
    lines.append("=" * 80)

    watch_edges = edges[edges["category"] == "watchlist"]

    if watch_edges.empty:
        lines.append("No watchlist edges available.")
    else:
        lines.append(watch_edges.to_string(index=False))

    lines.append("")
    lines.append("=" * 80)
    lines.append("CURRENT VALIDATION CONTEXT")
    lines.append("=" * 80)

    if not cluster_validation.empty:
        directional = cluster_validation[
            cluster_validation["expected_cluster_result"] != "uncertain"
        ]
        if not directional.empty:
            lines.append(
                f"Directional cluster accuracy: "
                f"{directional['cluster_result_correct'].mean() * 100:.2f}%"
            )

    if not expanded_validation.empty:
        lines.append(
            f"Expanded result accuracy: "
            f"{expanded_validation['result_correct'].mean() * 100:.2f}%"
        )
        lines.append(
            f"Expanded top-8 scoreline hit rate: "
            f"{expanded_validation['actual_in_top_8'].mean() * 100:.2f}%"
        )
        lines.append(
            f"Expanded margin 3+ signal accuracy: "
            f"{expanded_validation['margin_3_plus_signal_correct'].mean() * 100:.2f}%"
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("QUANT'S EDGE DISCOVERY VERDICT")
    lines.append("=" * 80)

    lines.append(
        "This engine shifts the World Cup Lab away from exact-score prediction and toward "
        "signal validation. The most useful signals are those that repeatedly identify "
        "match shape, dominance, margin risk, or team momentum."
    )

    lines.append(
        "Small samples should be treated carefully. Signals with fewer than 10 matches are "
        "interesting but not proven. As the tournament progresses, this report should reveal "
        "which model layers are genuinely useful."
    )

    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 36 - EDGE DISCOVERY ENGINE")
    print("=" * 80)

    cluster_validation = safe_read_csv(CLUSTER_VALIDATION_PATH)
    expanded_validation = safe_read_csv(EXPANDED_VALIDATION_PATH)
    watchlist = safe_read_csv(WATCHLIST_PATH)

    edges = build_edge_table(
        cluster_validation=cluster_validation,
        expanded_validation=expanded_validation,
        watchlist=watchlist,
    )

    edges.to_csv(EDGE_OUTPUT_PATH, index=False)

    report = build_report(
        edges=edges,
        cluster_validation=cluster_validation,
        expanded_validation=expanded_validation,
    )

    REPORT_OUTPUT_PATH.write_text(report, encoding="utf-8")

    print(report)

    print("-" * 80)
    print(f"Edge discovery saved: {EDGE_OUTPUT_PATH}")
    print(f"Report saved:         {REPORT_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
