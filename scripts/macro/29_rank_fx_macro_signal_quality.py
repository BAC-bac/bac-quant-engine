from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SUMMARY_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_backtest_v4_multi_horizon_d1_summary.csv"

OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_macro_signal_quality_rankings.csv"
PIVOT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_macro_signal_quality_pivot.csv"


KEY_HORIZONS = ["20d", "60d", "120d", "252d"]


def classify_signal_quality(row: pd.Series) -> str:
    positive_horizons = row["positive_key_horizons"]
    avg_win_rate = row["avg_key_win_rate"]
    avg_return = row["avg_key_signal_return"]
    consistency_score = row["consistency_score"]

    if positive_horizons >= 3 and avg_win_rate >= 0.52 and avg_return > 0:
        return "TRADEABLE_MACRO_SIGNAL"

    if positive_horizons >= 2 and avg_return > 0:
        return "WATCHLIST_ONLY"

    if consistency_score > 0 and avg_return > 0:
        return "WEAK_POSITIVE_BIAS"

    return "REJECTED_BY_HISTORY"


def main() -> None:
    summary = pd.read_csv(SUMMARY_FILE)

    print("\nLoaded D1 macro signal summary:")
    print(summary.to_string(index=False))

    key = summary[summary["horizon"].isin(KEY_HORIZONS)].copy()

    pair_quality = (
        key.groupby(["pair", "signal_v4", "direction"])
        .agg(
            key_horizons_tested=("horizon", "count"),
            positive_key_horizons=("avg_signal_return", lambda x: (x > 0).sum()),
            avg_key_win_rate=("win_rate", "mean"),
            avg_key_signal_return=("avg_signal_return", "mean"),
            median_key_signal_return=("median_signal_return", "mean"),
            total_key_signal_return=("total_signal_return", "sum"),
            best_key_signal_return=("best_signal_return", "max"),
            worst_key_signal_return=("worst_signal_return", "min"),
            avg_simple_score=("simple_score", "mean"),
        )
        .reset_index()
    )

    pair_quality["consistency_score"] = (
        pair_quality["positive_key_horizons"] / pair_quality["key_horizons_tested"]
    )

    pair_quality["quality_label"] = pair_quality.apply(classify_signal_quality, axis=1)

    pair_quality = pair_quality.sort_values(
        [
            "quality_label",
            "consistency_score",
            "avg_key_signal_return",
            "avg_key_win_rate",
        ],
        ascending=[True, False, False, False],
    )

    horizon_pivot = key.pivot_table(
        index=["pair", "signal_v4", "direction"],
        columns="horizon",
        values=["win_rate", "avg_signal_return", "total_signal_return"],
    )

    horizon_pivot.columns = [
        f"{metric}_{horizon}" for metric, horizon in horizon_pivot.columns
    ]

    horizon_pivot = horizon_pivot.reset_index()

    final = pair_quality.merge(
        horizon_pivot,
        on=["pair", "signal_v4", "direction"],
        how="left",
    )

    final.to_csv(OUTPUT_FILE, index=False)
    horizon_pivot.to_csv(PIVOT_FILE, index=False)

    print(f"\nSaved signal quality rankings to: {OUTPUT_FILE}")
    print(f"Saved signal quality pivot to:    {PIVOT_FILE}")

    print("\nSignal quality ranking:")
    print(final.to_string(index=False))


if __name__ == "__main__":
    main()