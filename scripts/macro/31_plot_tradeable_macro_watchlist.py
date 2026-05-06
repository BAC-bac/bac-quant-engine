from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_tradeable_macro_watchlist.csv"

CHART_DIR = PROJECT_ROOT / "macro_data" / "processed" / "charts"
CHART_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_SCORE_DIFF = CHART_DIR / "fx_tradeable_macro_watchlist_score_diff.png"
OUTPUT_CONFIDENCE = CHART_DIR / "fx_tradeable_macro_watchlist_confidence.png"
OUTPUT_ACTION_COUNTS = CHART_DIR / "fx_tradeable_macro_watchlist_action_counts.png"


ACTION_ORDER = {
    "TRADEABLE": 0,
    "WATCHLIST": 1,
    "RESEARCH_ONLY": 2,
    "NO_TRADE": 3,
}


def load_watchlist() -> pd.DataFrame:
    df = pd.read_csv(INPUT_FILE)

    df["action_rank"] = df["action"].map(ACTION_ORDER).fillna(99)
    df = df.sort_values(
        ["action_rank", "macro_confidence_score", "score_diff"],
        ascending=[True, False, False],
    )

    print("\nLoaded tradeable macro watchlist:")
    print(df.to_string(index=False))

    return df


def plot_score_diff(df: pd.DataFrame) -> None:
    df_plot = df.sort_values("score_diff")

    plt.figure(figsize=(10, 6))
    plt.barh(df_plot["pair"], df_plot["score_diff"])
    plt.axvline(0, linewidth=1)
    plt.title("FX Tradeable Macro Watchlist - Score Difference")
    plt.xlabel("Base Macro Score v4 - Quote Macro Score v4")
    plt.ylabel("FX Pair")
    plt.tight_layout()
    plt.savefig(OUTPUT_SCORE_DIFF, dpi=150)
    plt.close()

    print(f"Saved chart: {OUTPUT_SCORE_DIFF}")


def plot_confidence(df: pd.DataFrame) -> None:
    df_plot = df.sort_values("macro_confidence_score")

    plt.figure(figsize=(10, 6))
    plt.barh(df_plot["pair"], df_plot["macro_confidence_score"])
    plt.title("FX Tradeable Macro Watchlist - Confidence Score")
    plt.xlabel("Macro Confidence Score")
    plt.ylabel("FX Pair")
    plt.tight_layout()
    plt.savefig(OUTPUT_CONFIDENCE, dpi=150)
    plt.close()

    print(f"Saved chart: {OUTPUT_CONFIDENCE}")


def plot_action_counts(df: pd.DataFrame) -> None:
    counts = df["action"].value_counts()

    ordered_actions = [action for action in ACTION_ORDER if action in counts.index]
    values = [counts[action] for action in ordered_actions]

    plt.figure(figsize=(8, 5))
    plt.bar(ordered_actions, values)
    plt.title("FX Tradeable Macro Watchlist - Action Counts")
    plt.xlabel("Action")
    plt.ylabel("Number of Pairs")
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(OUTPUT_ACTION_COUNTS, dpi=150)
    plt.close()

    print(f"Saved chart: {OUTPUT_ACTION_COUNTS}")


def main() -> None:
    df = load_watchlist()

    plot_score_diff(df)
    plot_confidence(df)
    plot_action_counts(df)

    print("\nTradeable macro watchlist charts complete.")


if __name__ == "__main__":
    main()