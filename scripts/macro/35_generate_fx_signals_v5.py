from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_macro_bias_v5_real_rates.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_signals_v5.csv"


def generate_signal(row: pd.Series) -> str:
    diff = row["score_diff"]

    if diff > 0.5:
        return "STRONG_BUY_BASE"
    if diff > 0.2:
        return "BUY_BASE"
    if diff < -0.5:
        return "STRONG_SELL_BASE"
    if diff < -0.2:
        return "SELL_BASE"

    return "NEUTRAL"


def main() -> None:
    df = pd.read_csv(INPUT_FILE)

    df["signal_v5"] = df.apply(generate_signal, axis=1)

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved FX signals v5 to: {OUTPUT_FILE}")

    print("\nGenerated FX signals v5:")
    print(
        df[
            [
                "pair",
                "base_country",
                "quote_country",
                "base_macro_score_v5",
                "quote_macro_score_v5",
                "score_diff",
                "macro_bias_v5",
                "signal_v5",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()