from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_weighted_macro_bias_v3.csv"
CHART_DIR = PROJECT_ROOT / "macro_data" / "processed" / "charts"
CHART_DIR.mkdir(parents=True, exist_ok=True)


def load_data() -> pd.DataFrame:
    df = pd.read_csv(INPUT_FILE)
    print("\nLoaded FX weighted macro bias v3:")
    print(df)
    return df


def plot_score_diff(df: pd.DataFrame) -> None:
    df_plot = df.sort_values("score_diff")

    plt.figure(figsize=(10, 6))
    plt.barh(df_plot["pair"], df_plot["score_diff"])
    plt.axvline(0, linewidth=1)
    plt.title("FX Macro Bias v3 - Score Difference")
    plt.xlabel("Base Score - Quote Score")
    plt.ylabel("FX Pair")
    plt.tight_layout()

    output_path = CHART_DIR / "fx_macro_bias_v3_score_diff.png"
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"Saved chart: {output_path}")


def plot_base_vs_quote(df: pd.DataFrame) -> None:
    df_plot = df.sort_values("pair")

    x = range(len(df_plot))
    width = 0.35

    plt.figure(figsize=(10, 6))
    plt.bar([i - width / 2 for i in x], df_plot["base_score_v3"], width=width, label="Base country")
    plt.bar([i + width / 2 for i in x], df_plot["quote_score_v3"], width=width, label="Quote country")

    plt.xticks(list(x), df_plot["pair"])
    plt.title("FX Macro Bias v3 - Base vs Quote Scores")
    plt.xlabel("FX Pair")
    plt.ylabel("Macro Score v3")
    plt.legend()
    plt.tight_layout()

    output_path = CHART_DIR / "fx_macro_bias_v3_base_vs_quote.png"
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"Saved chart: {output_path}")


def plot_bias_counts(df: pd.DataFrame) -> None:
    bias_counts = df["macro_bias_v3"].value_counts()

    plt.figure(figsize=(8, 5))
    plt.bar(bias_counts.index, bias_counts.values)
    plt.title("FX Macro Bias v3 - Bias Classification Count")
    plt.xlabel("Macro Bias")
    plt.ylabel("Number of Pairs")
    plt.tight_layout()

    output_path = CHART_DIR / "fx_macro_bias_v3_bias_counts.png"
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"Saved chart: {output_path}")


def main() -> None:
    df = load_data()

    plot_score_diff(df)
    plot_base_vs_quote(df)
    plot_bias_counts(df)

    print("\nMacro v3 charts complete.")


if __name__ == "__main__":
    main()