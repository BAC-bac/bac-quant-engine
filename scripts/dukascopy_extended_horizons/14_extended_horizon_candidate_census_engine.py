from __future__ import annotations

"""BACQE EH14 - Candidate Census and Discovery Analytics Engine."""

import argparse
import hashlib
import json
import math
import re
from pathlib import Path

import numpy as np
import pandas as pd

ANALYSIS_ROOT = Path(r"E:\Quant_Lab\data\analysis\dukascopy_extended_horizons")
OUTPUT_NAME = "candidate_census"
ENGINE_VERSION = "1.2.0"
TIERS = {
    "tier_1_priority_candidate": 1,
    "tier_2_research_candidate": 2,
    "tier_3_watchlist_candidate": 3,
    "reject_or_hold": 4,
}
PRIORITY_TIERS = {"tier_1_priority_candidate", "tier_2_research_candidate"}
REQUIRED = {
    "test_symbol", "candidate_tier", "candidate_score",
    "research_confidence_score", "candidate_id", "candidate_layer",
    "transfer_status", "year_stability_status", "context_type",
    "context_value", "target", "feature", "threshold_quantile",
    "threshold_side", "files_tested", "total_trades",
    "net_total_return", "positive_year_rate", "min_year_return",
    "median_net_win_rate", "median_net_profit_factor",
}
NUMERIC = [
    "candidate_score", "research_confidence_score", "threshold_quantile",
    "files_tested", "total_trades", "net_total_return",
    "positive_year_rate", "min_year_return", "median_net_win_rate",
    "median_net_profit_factor",
]


def args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path)
    parser.add_argument("--analysis-root", type=Path, default=ANALYSIS_ROOT)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--base-symbol", default="EURJPY")
    parser.add_argument("--top-n", type=int, default=25)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def token(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_.-]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def find_registry(explicit: Path | None, root: Path) -> Path:
    if explicit:
        if not explicit.exists():
            raise FileNotFoundError(explicit)
        return explicit
    folder = root / "candidate_registry"
    paths = [p for p in folder.glob("*candidate_registry_latest.csv") if p.is_file()]
    if not paths:
        raise FileNotFoundError(
            f"No EH13 candidate registry found beneath {folder}. Use --registry."
        )
    return max(paths, key=lambda p: p.stat().st_mtime)


def load_registry(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, low_memory=False)
    missing = sorted(REQUIRED - set(frame.columns))
    if missing:
        raise ValueError("Missing required columns: " + ", ".join(missing))
    if frame.empty:
        raise ValueError("Candidate registry is empty.")
    for column in NUMERIC:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    # candidate_id identifies the underlying research configuration and may
    # legitimately recur across test symbols or research layers. Only fully
    # duplicated registry rows are invalid at this stage.
    duplicate_rows = int(frame.duplicated(keep=False).sum())
    if duplicate_rows:
        raise ValueError(
            f"Candidate registry contains {duplicate_rows} fully duplicated row(s)."
        )
    return frame


def parent_context(context_type: object, context_value: object) -> str:
    ctype, value = token(context_type), token(context_value)
    if ctype == "context_hour":
        match = re.search(r"(\d{1,2})", value)
        return f"hour_{int(match.group(1)):02d}" if match else value
    if "london_newyork_overlap" in value:
        return "london_newyork_overlap"
    return value or "unconditioned"


def feature_family(feature: object) -> str:
    value = token(feature)
    return re.sub(r"_\d+$", "", value) or "unknown_feature"


def family_key(row: pd.Series) -> str:
    parts = [
        token(row["target"]),
        feature_family(row["feature"]),
        token(row["threshold_side"]) or "unknown_side",
        token(row["context_type"]) or "unconditioned",
        parent_context(row["context_type"], row["context_value"]),
    ]
    digest = hashlib.sha1("||".join(parts).encode()).hexdigest()[:16]
    return f"family_{digest}"


def enrich(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()

    # A candidate can be replayed on several symbols and can appear in more
    # than one research layer. Preserve candidate_id as the shared scientific
    # identity, while candidate_record_id uniquely identifies each registry
    # observation. The occurrence suffix protects against legitimate repeated
    # observations that share symbol, layer and candidate configuration.
    identity_base = (
        out["test_symbol"].fillna("").astype(str).str.strip()
        + "::"
        + out["candidate_layer"].fillna("").astype(str).str.strip()
        + "::"
        + out["candidate_id"].fillna("").astype(str).str.strip()
    )
    occurrence = out.groupby(identity_base, sort=False).cumcount() + 1
    occurrence_total = out.groupby(identity_base, sort=False)["candidate_id"].transform("size")
    out["candidate_record_id"] = np.where(
        occurrence_total.eq(1),
        identity_base,
        identity_base + "::record_" + occurrence.astype(str),
    )

    out["feature_family"] = out["feature"].map(feature_family)
    out["parent_context"] = out.apply(
        lambda r: parent_context(r["context_type"], r["context_value"]), axis=1
    )
    out["edge_family_id"] = out.apply(family_key, axis=1)
    out["tier_rank"] = out["candidate_tier"].map(TIERS).fillna(99).astype(int)
    out["is_priority"] = out["candidate_tier"].isin(PRIORITY_TIERS)
    out["is_promoted"] = out["candidate_tier"].isin(PRIORITY_TIERS | {"tier_3_watchlist_candidate"})
    out["transfer_success"] = out["transfer_status"].astype(str).str.contains(
        r"transfer_pass|base_symbol", regex=True, na=False
    )
    out["year_stable"] = out["year_stability_status"].astype(str).str.contains(
        r"year_stable|base_symbol_not_cross_year_tested", regex=True, na=False
    )
    return out


def effective_count(counts: pd.Series) -> float:
    values = pd.to_numeric(counts, errors="coerce").dropna()
    values = values[values > 0]
    if values.empty:
        return 0.0
    shares = values / values.sum()
    return float(1.0 / np.square(shares).sum())


def valid_numeric(series: pd.Series) -> pd.Series:
    """Return finite numeric observations without emitting empty-slice warnings."""
    values = pd.to_numeric(series, errors="coerce")
    return values[np.isfinite(values)]


def safe_median(series: pd.Series) -> float:
    values = valid_numeric(series)
    return float(values.median()) if not values.empty else np.nan


def safe_max(series: pd.Series) -> float:
    values = valid_numeric(series)
    return float(values.max()) if not values.empty else np.nan


def safe_mean(series: pd.Series) -> float:
    values = valid_numeric(series)
    return float(values.mean()) if not values.empty else np.nan


def safe_sum(series: pd.Series) -> float:
    values = valid_numeric(series)
    return float(values.sum()) if not values.empty else np.nan


def diversity_metrics(counts: pd.Series) -> dict[str, float]:
    values = pd.to_numeric(counts, errors="coerce").dropna()
    values = values[values > 0]
    if values.empty:
        return {
            "shannon_entropy": 0.0,
            "normalised_shannon_entropy": 0.0,
            "simpson_diversity": 0.0,
            "gini_coefficient": 0.0,
        }
    shares = (values / values.sum()).to_numpy(dtype=float)
    entropy = float(-(shares * np.log(shares)).sum())
    normalised_entropy = entropy / math.log(len(shares)) if len(shares) > 1 else 0.0
    simpson = float(1.0 - np.square(shares).sum())
    sorted_values = np.sort(values.to_numpy(dtype=float))
    n = len(sorted_values)
    gini = float(
        (2.0 * np.dot(np.arange(1, n + 1), sorted_values) / (n * sorted_values.sum()))
        - (n + 1.0) / n
    ) if n else 0.0
    return {
        "shannon_entropy": entropy,
        "normalised_shannon_entropy": normalised_entropy,
        "simpson_diversity": simpson,
        "gini_coefficient": max(0.0, min(gini, 1.0)),
    }


def census(frame: pd.DataFrame) -> pd.DataFrame:
    dimensions = [
        "test_symbol", "candidate_tier", "feature", "feature_family", "target",
        "threshold_quantile", "threshold_side", "context_type", "context_value",
        "parent_context", "transfer_status", "year_stability_status",
        "candidate_layer", "edge_family_id",
    ]
    rows = []
    for dimension in dimensions:
        for value, group in frame.groupby(dimension, dropna=False, observed=False):
            rows.append({
                "dimension": dimension,
                "dimension_value": str(value),
                "candidate_count": len(group),
                "priority_candidate_count": int(group["is_priority"].sum()),
                "promoted_candidate_count": int(group["is_promoted"].sum()),
                "distinct_symbols": group["test_symbol"].nunique(),
                "distinct_families": group["edge_family_id"].nunique(),
                "total_trades": safe_sum(group["total_trades"]),
                "total_net_return": safe_sum(group["net_total_return"]),
                "median_candidate_score": safe_median(group["candidate_score"]),
                "median_win_rate": safe_median(group["median_net_win_rate"]),
                "median_profit_factor": safe_median(group["median_net_profit_factor"]),
                "median_positive_year_rate": safe_median(group["positive_year_rate"]),
            })
    return pd.DataFrame(rows).sort_values(
        ["dimension", "candidate_count", "median_candidate_score"],
        ascending=[True, False, False], kind="stable"
    ).reset_index(drop=True)


def classify_family(row: pd.Series) -> tuple[str, str]:
    if row.tier_1_count and row.symbol_count >= 3 and row.transfer_success_rate >= .75 and row.year_stable_rate >= .65:
        return "cross_symbol_priority_family", "Promote to replay, drawdown, pre-COVID and walk-forward validation."
    if row.tier_1_count and row.symbol_count >= 2 and row.transfer_success_rate >= .60:
        return "transferable_research_family", "Retain for deeper cross-symbol and temporal validation."
    if row.tier_1_count and row.symbol_count == 1:
        return "base_or_symbol_specific_family", "Test whether the edge is symbol-specific before promotion."
    if row.tier_2_count and row.transfer_success_rate >= .50:
        return "secondary_research_family", "Retain as a secondary research family."
    if row.family_concentration_risk == "high":
        return "parameter_variant_concentration", "Collapse neighbouring variants before more testing."
    return "hold_or_reject_family", "Hold unless transfer or year stability improves."


def family_registry(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for family_id, group in frame.groupby("edge_family_id", observed=False):
        tiers = group["candidate_tier"].value_counts()
        symbols = sorted(group["test_symbol"].dropna().astype(str).unique())
        count = len(group)
        largest_symbol_share = group["test_symbol"].value_counts(normalize=True).max()
        risk = "high" if count >= 10 or largest_symbol_share >= .80 else (
            "medium" if count >= 5 or largest_symbol_share >= .60 else "low"
        )
        first = group.iloc[0]
        rows.append({
            "edge_family_id": family_id,
            "target": first["target"],
            "feature_family": first["feature_family"],
            "threshold_side": first["threshold_side"],
            "context_type": first["context_type"],
            "parent_context": first["parent_context"],
            "candidate_count": count,
            "symbol_count": len(symbols),
            "symbols_present": ", ".join(symbols),
            "tier_1_count": int(tiers.get("tier_1_priority_candidate", 0)),
            "tier_2_count": int(tiers.get("tier_2_research_candidate", 0)),
            "tier_3_count": int(tiers.get("tier_3_watchlist_candidate", 0)),
            "reject_count": int(tiers.get("reject_or_hold", 0)),
            "largest_symbol_share": largest_symbol_share,
            "total_trades": safe_sum(group["total_trades"]),
            "total_net_return": safe_sum(group["net_total_return"]),
            "median_candidate_score": safe_median(group["candidate_score"]),
            "max_candidate_score": safe_max(group["candidate_score"]),
            "median_confidence_score": safe_median(group["research_confidence_score"]),
            "median_win_rate": safe_median(group["median_net_win_rate"]),
            "median_profit_factor": safe_median(group["median_net_profit_factor"]),
            "median_positive_year_rate": safe_median(group["positive_year_rate"]),
            "transfer_success_rate": safe_mean(group["transfer_success"]),
            "year_stable_rate": safe_mean(group["year_stable"]),
            "family_concentration_risk": risk,
            "family_population_class": (
                "orphan_family" if count == 1 else
                "small_family" if count <= 4 else
                "established_family" if count <= 9 else
                "dominant_family"
            ),
            "is_orphan_family": count == 1,
        })
    result = pd.DataFrame(rows)
    max_symbols = max(frame["test_symbol"].nunique(), 1)
    duplicate_penalty = np.log1p(result["candidate_count"] - 1) / math.log(11)
    result["family_independence_score"] = (
        55 * result["symbol_count"].div(max_symbols).clip(0, 1)
        + 45 * (1 - duplicate_penalty.clip(0, 1))
    ).clip(0, 100)
    decisions = result.apply(classify_family, axis=1)
    result["recommended_research_status"] = decisions.map(lambda x: x[0])
    result["recommended_next_step"] = decisions.map(lambda x: x[1])
    return result.sort_values(
        ["tier_1_count", "symbol_count", "median_candidate_score", "family_independence_score"],
        ascending=[False, False, False, False], kind="stable"
    ).reset_index(drop=True)


def transfer_matrix(frame: pd.DataFrame) -> pd.DataFrame:
    grouped = frame.groupby(["edge_family_id", "test_symbol"], observed=False).agg(
        candidate_count=("candidate_id", "count"),
        best_tier_rank=("tier_rank", "min"),
        best_candidate_score=("candidate_score", safe_max),
        median_candidate_score=("candidate_score", safe_median),
        total_trades=("total_trades", safe_sum),
        total_net_return=("net_total_return", safe_sum),
        median_win_rate=("median_net_win_rate", safe_median),
        median_profit_factor=("median_net_profit_factor", safe_median),
        median_positive_year_rate=("positive_year_rate", safe_median),
        transfer_success_rate=("transfer_success", safe_mean),
        year_stable_rate=("year_stable", safe_mean),
    ).reset_index()
    reverse = {v: k for k, v in TIERS.items()}
    grouped["best_candidate_tier"] = grouped["best_tier_rank"].map(reverse)
    wide = grouped.pivot(index="edge_family_id", columns="test_symbol", values="best_candidate_tier")
    wide.columns = [f"{c}_best_tier" for c in wide.columns]
    return grouped.merge(wide.reset_index(), on="edge_family_id", how="left")


def independence(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy().reset_index(drop=True)
    family_sizes = out.groupby("edge_family_id")["candidate_id"].transform("count")
    symbol_sizes = out.groupby(["edge_family_id", "test_symbol"])["candidate_id"].transform("count")
    family_penalty = (np.log1p(family_sizes - 1) / math.log(11)).clip(0, 1)
    symbol_penalty = (np.log1p(symbol_sizes - 1) / math.log(11)).clip(0, 1)
    out["family_candidate_count"] = family_sizes
    out["same_symbol_family_count"] = symbol_sizes
    out["candidate_independence_score"] = (
        100 * (1 - .65 * family_penalty - .35 * symbol_penalty)
    ).clip(0, 100)
    out["candidate_independence_class"] = pd.cut(
        out["candidate_independence_score"], [-1, 25, 50, 75, 101],
        labels=["very_low", "low", "moderate", "high"]
    ).astype(str)
    columns = [
        "candidate_record_id", "candidate_id", "edge_family_id", "test_symbol", "candidate_tier",
        "candidate_score", "research_confidence_score", "target", "feature",
        "threshold_quantile", "threshold_side", "context_type", "context_value",
        "parent_context", "family_candidate_count", "same_symbol_family_count",
        "candidate_independence_score", "candidate_independence_class",
    ]
    return out[columns].sort_values(
        ["candidate_independence_score", "candidate_score"], ascending=[False, False]
    ).reset_index(drop=True)


def concentration(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for level, subset in [("all_candidates", frame), ("priority_candidates", frame[frame.is_priority])]:
        if subset.empty:
            continue
        for dimension in ["test_symbol", "feature_family", "target", "context_type", "parent_context", "edge_family_id"]:
            counts = subset[dimension].value_counts(dropna=False)
            shares = counts / counts.sum()
            diversity = diversity_metrics(counts)
            rows.append({
                "analysis_level": level,
                "dimension": dimension,
                "candidate_count": len(subset),
                "distinct_values": len(counts),
                "largest_group_count": int(counts.iloc[0]),
                "largest_group_share": float(shares.iloc[0]),
                "top_5_share": float(shares.head(5).sum()),
                "herfindahl_index": float(np.square(shares).sum()),
                "effective_count": effective_count(counts),
                **diversity,
            })
    return pd.DataFrame(rows)


def summary(frame: pd.DataFrame, families: pd.DataFrame, base_symbol: str) -> pd.DataFrame:
    priority = frame[frame.is_priority]
    counts = priority["edge_family_id"].value_counts()
    symbol_counts = priority["test_symbol"].value_counts()
    context_counts = priority["parent_context"].value_counts()
    diversity = diversity_metrics(counts)
    orphan_count = int(families["is_orphan_family"].sum()) if not families.empty else 0
    metrics = [
        ("registry_rows", len(frame), "Total EH13 candidate rows."),
        ("tier_1_candidates", int((frame.candidate_tier == "tier_1_priority_candidate").sum()), "Highest-priority rows."),
        ("tier_2_candidates", int((frame.candidate_tier == "tier_2_research_candidate").sum()), "Secondary research rows."),
        ("tier_3_candidates", int((frame.candidate_tier == "tier_3_watchlist_candidate").sum()), "Watchlist rows."),
        ("reject_or_hold_candidates", int((frame.candidate_tier == "reject_or_hold").sum()), "Rows not currently promoted."),
        ("distinct_edge_families", len(families), "Normalised target-feature-context families."),
        ("priority_edge_families", priority.edge_family_id.nunique(), "Families represented in Tier 1 or Tier 2."),
        ("effective_priority_family_count", round(effective_count(counts), 4), "Diversity-adjusted priority family count."),
        ("largest_priority_family_share", round(float(counts.iloc[0] / counts.sum()), 4) if not counts.empty else 0, "Priority concentration in the largest family."),
        ("priority_family_shannon_entropy", round(diversity["shannon_entropy"], 4), "Raw Shannon diversity across priority families."),
        ("priority_family_normalised_entropy", round(diversity["normalised_shannon_entropy"], 4), "Priority-family diversity scaled from 0 to 1."),
        ("priority_family_simpson_diversity", round(diversity["simpson_diversity"], 4), "Probability that two priority rows belong to different families."),
        ("priority_family_gini", round(diversity["gini_coefficient"], 4), "Inequality of candidate counts across priority families."),
        ("orphan_edge_families", orphan_count, "Families represented by exactly one registry row."),
        ("orphan_family_share", round(orphan_count / len(families), 4) if len(families) else 0, "Share of all families represented by one row."),
        ("base_symbol", base_symbol.upper(), "Configured base discovery symbol."),
        ("dominant_priority_symbols", ", ".join(symbol_counts.head(3).index), "Symbols with the most Tier 1 and Tier 2 rows."),
        ("dominant_priority_contexts", ", ".join(context_counts.head(3).index), "Most common priority contexts."),
    ]
    return pd.DataFrame(metrics, columns=["metric", "value", "interpretation"])


def report_text(path: Path, frame: pd.DataFrame, families: pd.DataFrame, conc: pd.DataFrame, summ: pd.DataFrame, top_n: int) -> str:
    symbol = frame.groupby(["test_symbol", "candidate_tier"], observed=False).agg(
        candidates=("candidate_id", "count"), distinct_families=("edge_family_id", "nunique"),
        total_trades=("total_trades", safe_sum), total_net_return=("net_total_return", safe_sum),
        median_candidate_score=("candidate_score", safe_median),
        median_win_rate=("median_net_win_rate", safe_median),
        median_profit_factor=("median_net_profit_factor", safe_median),
        median_positive_year_rate=("positive_year_rate", safe_median),
    ).reset_index()
    width = 110
    return "\n".join([
        "BACQE DUKASCOPY EXTENDED HORIZONS",
        "EH14 - CANDIDATE CENSUS AND DISCOVERY ANALYTICS REPORT",
        "=" * width,
        f"Registry: {path}",
        f"Registry rows: {len(frame):,}",
        f"Distinct edge families: {len(families):,}",
        f"Priority edge families: {frame.loc[frame.is_priority, 'edge_family_id'].nunique():,}",
        "-" * width,
        "DISCOVERY SUMMARY",
        summ.to_string(index=False),
        "-" * width,
        "SYMBOL AND TIER CENSUS",
        symbol.to_string(index=False),
        "-" * width,
        "TOP EDGE FAMILIES",
        families.head(top_n).to_string(index=False),
        "-" * width,
        "PRIORITY CONCENTRATION",
        conc[conc.analysis_level == "priority_candidates"].to_string(index=False),
        "=" * width,
    ])


def run_live(registry_path: Path, output_dir: Path, base_symbol: str, top_n: int) -> None:
    frame = enrich(load_registry(registry_path))
    census_df = census(frame)
    families = family_registry(frame)
    members = frame.sort_values(["edge_family_id", "tier_rank", "candidate_score"], ascending=[True, True, False])
    transfer = transfer_matrix(frame)
    independence_df = independence(frame)
    concentration_df = concentration(frame)
    summary_df = summary(frame, families, base_symbol)
    text = report_text(registry_path, frame, families, concentration_df, summary_df, top_n)

    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "candidate_census_latest.csv": census_df,
        "candidate_family_registry_latest.csv": families,
        "candidate_family_members_latest.csv": members,
        "candidate_concentration_latest.csv": concentration_df,
        "candidate_transfer_matrix_latest.csv": transfer,
        "candidate_independence_scores_latest.csv": independence_df,
        "candidate_discovery_summary_latest.csv": summary_df,
    }
    for name, data in outputs.items():
        data.to_csv(output_dir / name, index=False)
    (output_dir / "candidate_census_report_latest.txt").write_text(text, encoding="utf-8")
    state = {
        "registry": str(registry_path), "registry_rows": len(frame),
        "distinct_edge_families": len(families),
        "priority_edge_families": int(frame.loc[frame.is_priority, "edge_family_id"].nunique()),
        "engine_version": ENGINE_VERSION,
        "status": "complete",
    }
    (output_dir / "candidate_census_state_latest.json").write_text(json.dumps(state, indent=2), encoding="utf-8")

    print("=" * 110)
    print(f"BACQE DUKASCOPY EXTENDED HORIZONS EH14 v{ENGINE_VERSION} - CANDIDATE CENSUS AND DISCOVERY ANALYTICS ENGINE")
    print("=" * 110)
    print(f"Registry rows:          {len(frame):,}")
    print(f"Priority candidates:    {int(frame.is_priority.sum()):,}")
    print(f"Distinct edge families: {len(families):,}")
    print(f"Priority edge families: {frame.loc[frame.is_priority, 'edge_family_id'].nunique():,}")
    print("-" * 110)
    print(summary_df.to_string(index=False))
    print("-" * 110)
    print(f"Family registry: {output_dir / 'candidate_family_registry_latest.csv'}")
    print(f"Report:          {output_dir / 'candidate_census_report_latest.txt'}")
    print("=" * 110)


def self_test() -> None:
    base = {
        "research_confidence_score": 90, "candidate_layer": "cross_symbol_transfer",
        "files_tested": 100, "total_trades": 1000, "positive_year_rate": 1.0,
        "min_year_return": 1.0, "median_net_win_rate": .60,
        "median_net_profit_factor": 2.0, "net_total_return": 100.0,
    }
    rows = [
        dict(base, test_symbol="EURJPY", candidate_tier="tier_1_priority_candidate", candidate_score=200,
             candidate_id="a", transfer_status="base_symbol", year_stability_status="base_symbol_not_cross_year_tested",
             context_type="context_hour", context_value="14", target="future_return_20000", feature="hour",
             threshold_quantile=.9, threshold_side="upper"),
        dict(base, test_symbol="USDJPY", candidate_tier="tier_1_priority_candidate", candidate_score=170,
             candidate_id="a", transfer_status="transfer_pass_primary", year_stability_status="year_stable_secondary",
             context_type="context_hour", context_value="14", target="future_return_20000", feature="hour",
             threshold_quantile=.8, threshold_side="upper"),
        dict(base, test_symbol="EURGBP", candidate_tier="reject_or_hold", candidate_score=-10,
             candidate_id="c", transfer_status="transfer_fail_or_watchlist", year_stability_status="year_unstable_or_fail",
             context_type="context_session", context_value="london_newyork_overlap", target="future_return_20000", feature="hour",
             threshold_quantile=.9, threshold_side="upper"),
    ]
    frame = enrich(pd.DataFrame(rows))
    missing_numeric_frame = frame.copy()
    missing_numeric_frame["median_net_win_rate"] = np.nan
    missing_numeric_frame["median_net_profit_factor"] = np.nan
    missing_numeric_frame["positive_year_rate"] = np.nan
    with np.errstate(all="raise"):
        missing_census = census(missing_numeric_frame)
        missing_families = family_registry(missing_numeric_frame)
        missing_transfer = transfer_matrix(missing_numeric_frame)
    family_df = family_registry(frame)
    concentration_df = concentration(frame)
    tests = {
        "shared candidate id allowed": frame.loc[0, "candidate_id"] == frame.loc[1, "candidate_id"],
        "record ids remain unique": frame["candidate_record_id"].is_unique,
        "family grouping": frame.loc[0, "edge_family_id"] == frame.loc[1, "edge_family_id"],
        "context family separation": frame.loc[0, "edge_family_id"] != frame.loc[2, "edge_family_id"],
        "census output": not census(frame).empty,
        "family registry": len(family_df) == 2,
        "orphan family detection": int(family_df["is_orphan_family"].sum()) == 1,
        "transfer matrix": not transfer_matrix(frame).empty,
        "all-NaN statistics safe": (
            not missing_census.empty and not missing_families.empty and not missing_transfer.empty
        ),
        "independence bounds": independence(frame).candidate_independence_score.between(0, 100).all(),
        "concentration output": not concentration_df.empty,
        "diversity metrics bounded": (
            concentration_df["normalised_shannon_entropy"].between(0, 1).all()
            and concentration_df["simpson_diversity"].between(0, 1).all()
            and concentration_df["gini_coefficient"].between(0, 1).all()
        ),
        "summary output": len(summary(frame, family_df, "EURJPY")) >= 16,
    }
    print("=" * 110)
    print("BACQE EH14 - SELF TEST")
    print("=" * 110)
    for name, passed in tests.items():
        print(f"[{'PASS' if passed else 'FAIL'}] {name}")
    print("-" * 110)
    print(f"Passed: {sum(tests.values())}/{len(tests)}")
    print("=" * 110)
    if not all(tests.values()):
        raise AssertionError("EH14 self-test failed.")


def main() -> None:
    options = args()
    if options.self_test:
        self_test()
        return
    registry_path = find_registry(options.registry, options.analysis_root)
    output_dir = options.output_dir or options.analysis_root / OUTPUT_NAME
    run_live(registry_path, output_dir, options.base_symbol, max(options.top_n, 1))


if __name__ == "__main__":
    main()