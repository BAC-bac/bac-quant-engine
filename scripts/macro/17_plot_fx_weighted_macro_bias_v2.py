from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_weighted_macro_bias_v2.csv"
CHART_DIR = PROJECT_ROOT / "macro_data" / "processed" / "charts"


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Could not find input file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    df = df.sort_values("score_diff").reset_index(drop=True)

    CHART_DIR.mkdir(parents=True, exist_ok=True)
    output_path = CHART_DIR / "fx_weighted_macro_bias_v2.png"

    plt.figure(figsize=(11, 5))

    plt.bar(df["pair"], df["score_diff"])

    plt.axhline(0, linewidth=1)
    plt.axhline(0.5, linestyle="--", linewidth=1, label="Bearish base threshold")
    plt.axhline(-0.5, linestyle="--", linewidth=1, label="Bullish base threshold")

    plt.title("Weighted Sovereign Macro FX Bias v2")
    plt.xlabel("FX Pair")
    plt.ylabel("Base Score - Quote Score")
    plt.xticks(rotation=30, ha="right")
    plt.grid(axis="y")
    plt.legend()

    for index, row in enumerate(df.itertuples()):
        plt.text(
            index,
            row.score_diff,
            row.macro_bias_v2,
            ha="center",
            va="bottom" if row.score_diff >= 0 else "top",
            fontsize=8,
        )

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.show()

    print(f"Saved FX weighted macro bias v2 chart to: {output_path}")
    print()
    print(df[["pair", "score_diff", "macro_bias_v2"]].to_string(index=False))


if __name__ == "__main__":
    main()