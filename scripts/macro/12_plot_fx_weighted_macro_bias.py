from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_weighted_macro_bias.csv"
CHART_DIR = PROJECT_ROOT / "macro_data" / "processed" / "charts"


def classify_direction_label(row: pd.Series) -> str:
    if row["macro_bias"] == "bullish_base":
        return f"Bullish {row['pair'][:3]}"
    elif row["macro_bias"] == "bearish_base":
        return f"Bearish {row['pair'][:3]}"
    return "Neutral"


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Could not find input file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    df["direction_label"] = df.apply(classify_direction_label, axis=1)

    df = df.sort_values("score_diff")

    CHART_DIR.mkdir(parents=True, exist_ok=True)
    output_path = CHART_DIR / "fx_weighted_macro_bias.png"

    plt.figure(figsize=(10, 5))

    plt.bar(df["pair"], df["score_diff"])

    plt.axhline(0, linewidth=1)
    plt.axhline(0.5, linestyle="--", linewidth=1, label="Bearish base threshold")
    plt.axhline(-0.5, linestyle="--", linewidth=1, label="Bullish base threshold")

    plt.title("Weighted Sovereign Macro FX Bias")
    plt.xlabel("FX Pair")
    plt.ylabel("Base Score - Quote Score")
    plt.grid(axis="y")
    plt.legend()

    for index, row in enumerate(df.itertuples()):
        plt.text(
            index,
            row.score_diff,
            row.macro_bias,
            ha="center",
            va="bottom" if row.score_diff >= 0 else "top",
            fontsize=8,
        )

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.show()

    print(f"Saved FX weighted macro bias chart to: {output_path}")
    print()
    print(df[["pair", "score_diff", "macro_bias", "direction_label"]].to_string(index=False))


if __name__ == "__main__":
    main()