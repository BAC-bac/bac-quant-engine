from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_macro_bias_v4_rates.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_signals_v4.csv"


def generate_signal(row):
    diff = row["score_diff"]

    if diff > 0.5:
        return "STRONG_BUY_BASE"
    elif diff > 0.2:
        return "BUY_BASE"
    elif diff < -0.5:
        return "STRONG_SELL_BASE"
    elif diff < -0.2:
        return "SELL_BASE"
    else:
        return "NEUTRAL"


def main():
    df = pd.read_csv(INPUT_FILE)

    df["signal_v4"] = df.apply(generate_signal, axis=1)

    print("\nGenerated FX signals:")
    print(df[["pair", "score_diff", "signal_v4"]])

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved signals to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()