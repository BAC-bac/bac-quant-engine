"""
BACQE DUKASCOPY 24 - FEATURE AGGREGATION & STABILITY ENGINE
"""

from pathlib import Path
import numpy as np
import pandas as pd


SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

INPUT_PATH = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_feature_discovery"
    / "feature_scores"
    / "feature_scores_latest.csv"
)

OUTPUT_ROOT = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_feature_stability"
)

TOP_N = 100


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "aggregated_scores",
        OUTPUT_ROOT / "top_features",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def extract_date_from_dataset(dataset: str) -> pd.Timestamp:
    # Example: EURUSD_2023-12-25_engineered_features
    parts = dataset.split("_")
    for part in parts:
        try:
            return pd.to_datetime(part, errors="raise")
        except Exception:
            continue
    return pd.NaT


def classify_consistency(hit_rate: float, mean_abs_spearman: float) -> str:
    if hit_rate >= 0.70 and mean_abs_spearman >= 0.02:
        return "high_consistency"
    if hit_rate >= 0.60 and mean_abs_spearman >= 0.01:
        return "medium_consistency"
    if hit_rate >= 0.55:
        return "low_consistency"
    return "unstable"


def main() -> None:
    banner("BACQE DUKASCOPY 24 - FEATURE AGGREGATION & STABILITY ENGINE")

    ensure_dirs()

    print(f"Symbol:      {SYMBOL}")
    print(f"Input path:  {INPUT_PATH}")
    print(f"Output root: {OUTPUT_ROOT}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        print("[STOP] Missing feature scores file.")
        return

    df = pd.read_csv(INPUT_PATH)

    print(f"Loaded score rows: {len(df):,}")

    required_cols = {
        "dataset",
        "feature",
        "target",
        "spearman",
        "abs_spearman",
        "valid_rows",
    }

    missing = required_cols - set(df.columns)

    if missing:
        print(f"[STOP] Missing required columns: {sorted(missing)}")
        return

    df["dataset_date"] = df["dataset"].apply(extract_date_from_dataset)
    df["year"] = df["dataset_date"].dt.year
    df["month"] = df["dataset_date"].dt.to_period("M").astype(str)

    df = df.dropna(subset=["spearman", "abs_spearman", "dataset_date"])

    print(f"Usable score rows: {len(df):,}")

    # Direction consistency:
    # For each feature-target pair, determine dominant sign and how often it appears.
    grouped = df.groupby(["feature", "target"], as_index=False)

    agg = grouped.agg(
        observations=("spearman", "count"),
        days_tested=("dataset_date", "nunique"),
        years_tested=("year", "nunique"),
        months_tested=("month", "nunique"),
        mean_spearman=("spearman", "mean"),
        median_spearman=("spearman", "median"),
        std_spearman=("spearman", "std"),
        mean_abs_spearman=("abs_spearman", "mean"),
        median_abs_spearman=("abs_spearman", "median"),
        max_abs_spearman=("abs_spearman", "max"),
        min_abs_spearman=("abs_spearman", "min"),
        total_valid_rows=("valid_rows", "sum"),
        mean_valid_rows=("valid_rows", "mean"),
    )

    sign_stats = []

    for (feature, target), g in df.groupby(["feature", "target"]):
        positive_rate = (g["spearman"] > 0).mean()
        negative_rate = (g["spearman"] < 0).mean()

        dominant_direction = (
            "positive"
            if positive_rate >= negative_rate
            else "negative"
        )

        hit_rate = max(positive_rate, negative_rate)

        sign_stats.append({
            "feature": feature,
            "target": target,
            "positive_rate": positive_rate,
            "negative_rate": negative_rate,
            "dominant_direction": dominant_direction,
            "direction_hit_rate": hit_rate,
        })

    sign_df = pd.DataFrame(sign_stats)

    agg = agg.merge(sign_df, on=["feature", "target"], how="left")

    agg["stability_ratio"] = (
        agg["mean_abs_spearman"]
        / agg["std_spearman"].abs().replace(0, np.nan)
    )

    agg["consistency_label"] = agg.apply(
        lambda row: classify_consistency(
            row["direction_hit_rate"],
            row["mean_abs_spearman"],
        ),
        axis=1,
    )

    # Conservative score:
    # Prefer features that are consistent across many days/months/years,
    # have stable direction, and are not just one-day wonders.
    agg["coverage_score"] = (
        (agg["days_tested"] / agg["days_tested"].max()).fillna(0) * 0.50
        + (agg["months_tested"] / agg["months_tested"].max()).fillna(0) * 0.30
        + (agg["years_tested"] / agg["years_tested"].max()).fillna(0) * 0.20
    )

    agg["direction_score"] = agg["direction_hit_rate"].fillna(0)

    max_abs = agg["mean_abs_spearman"].max()

    if pd.notna(max_abs) and max_abs != 0:
        agg["effect_score"] = agg["mean_abs_spearman"] / max_abs
    else:
        agg["effect_score"] = 0

    agg["final_stability_score"] = (
        agg["effect_score"] * 0.45
        + agg["direction_score"] * 0.35
        + agg["coverage_score"] * 0.20
    )

    agg = agg.sort_values("final_stability_score", ascending=False)
    agg.insert(0, "rank", range(1, len(agg) + 1))

    top = agg.head(TOP_N)

    output_scores = OUTPUT_ROOT / "aggregated_scores" / "feature_stability_scores_latest.csv"
    output_top = OUTPUT_ROOT / "top_features" / "top_stable_features_latest.csv"
    output_report = OUTPUT_ROOT / "reports" / "feature_stability_report_latest.txt"

    agg.to_csv(output_scores, index=False)
    top.to_csv(output_top, index=False)

    with open(output_report, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY FEATURE AGGREGATION & STABILITY REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {SYMBOL}\n")
        f.write(f"Input rows: {len(df):,}\n")
        f.write(f"Feature-target pairs: {len(agg):,}\n")
        f.write(f"Top N: {TOP_N}\n\n")

        f.write("Top Stable Feature / Target Pairs\n")
        f.write("-" * 80 + "\n")

        f.write(
            top[
                [
                    "rank",
                    "feature",
                    "target",
                    "days_tested",
                    "months_tested",
                    "years_tested",
                    "mean_spearman",
                    "mean_abs_spearman",
                    "direction_hit_rate",
                    "dominant_direction",
                    "consistency_label",
                    "final_stability_score",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs:\n")
        f.write(f"Scores: {output_scores}\n")
        f.write(f"Top:    {output_top}\n")

    print("=" * 90)
    print("[DONE] Feature aggregation and stability complete.")
    print(f"Aggregated scores: {output_scores}")
    print(f"Top stable:        {output_top}")
    print(f"Report:            {output_report}")
    print("=" * 90)


if __name__ == "__main__":
    main()