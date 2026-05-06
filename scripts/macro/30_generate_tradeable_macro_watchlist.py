from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SIGNALS_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_signals_v4.csv"
QUALITY_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_macro_signal_quality_rankings.csv"

OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_tradeable_macro_watchlist.csv"


def assign_action(quality_label: str, signal: str) -> str:
    if signal == "NEUTRAL":
        return "NO_TRADE"

    if quality_label == "TRADEABLE_MACRO_SIGNAL":
        return "TRADEABLE"

    if quality_label in ["WATCHLIST_ONLY", "WEAK_POSITIVE_BIAS"]:
        return "WATCHLIST"

    return "RESEARCH_ONLY"


def assign_position_bias(signal: str) -> str:
    if "BUY" in signal:
        return "LONG_BASE_SHORT_QUOTE"

    if "SELL" in signal:
        return "SHORT_BASE_LONG_QUOTE"

    return "FLAT"


def main() -> None:
    signals = pd.read_csv(SIGNALS_FILE)
    quality = pd.read_csv(QUALITY_FILE)

    merged = signals.merge(
        quality[
            [
                "pair",
                "quality_label",
                "consistency_score",
                "avg_key_win_rate",
                "avg_key_signal_return",
                "positive_key_horizons",
                "key_horizons_tested",
            ]
        ],
        on="pair",
        how="left",
    )

    merged["quality_label"] = merged["quality_label"].fillna("NOT_TESTED")
    merged["consistency_score"] = merged["consistency_score"].fillna(0)
    merged["avg_key_win_rate"] = merged["avg_key_win_rate"].fillna(0)
    merged["avg_key_signal_return"] = merged["avg_key_signal_return"].fillna(0)
    merged["positive_key_horizons"] = merged["positive_key_horizons"].fillna(0)
    merged["key_horizons_tested"] = merged["key_horizons_tested"].fillna(0)

    merged["position_bias"] = merged["signal_v4"].apply(assign_position_bias)

    merged["action"] = merged.apply(
        lambda row: assign_action(row["quality_label"], row["signal_v4"]),
        axis=1,
    )

    merged["macro_confidence_score"] = (
        merged["consistency_score"] * 0.40
        + merged["avg_key_win_rate"] * 0.35
        + (merged["avg_key_signal_return"].clip(lower=0) * 25) * 0.25
    )

    merged["macro_confidence_score"] = merged["macro_confidence_score"].round(4)

    output_cols = [
        "pair",
        "signal_v4",
        "position_bias",
        "score_diff",
        "macro_bias_v4",
        "quality_label",
        "action",
        "macro_confidence_score",
        "consistency_score",
        "avg_key_win_rate",
        "avg_key_signal_return",
        "positive_key_horizons",
        "key_horizons_tested",
        "base_country",
        "quote_country",
        "base_macro_score_v4",
        "quote_macro_score_v4",
    ]

    output = merged[output_cols].sort_values(
        ["action", "macro_confidence_score", "score_diff"],
        ascending=[True, False, False],
    )

    output.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved tradeable macro watchlist to: {OUTPUT_FILE}")

    print("\nTradeable macro watchlist:")
    print(output.to_string(index=False))


if __name__ == "__main__":
    main()