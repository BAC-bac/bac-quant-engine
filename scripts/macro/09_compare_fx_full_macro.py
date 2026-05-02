from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_full_scores.csv"


FX_PAIRS = {
    "GBPUSD": ("United Kingdom", "United States"),
    "EURUSD": ("Germany", "United States"),
    "USDJPY": ("United States", "Japan"),
    "EURGBP": ("Germany", "United Kingdom"),
}


def classify_bias(diff):
    if diff <= -2:
        return "bullish_base"
    elif diff >= 2:
        return "bearish_base"
    else:
        return "neutral"


def main():
    df = pd.read_csv(INPUT_FILE)

    latest_year = df["year"].max()
    latest = df[df["year"] == latest_year]

    results = []

    for pair, (base, quote) in FX_PAIRS.items():
        base_row = latest[latest["COUNTRY"] == base]
        quote_row = latest[latest["COUNTRY"] == quote]

        if base_row.empty or quote_row.empty:
            continue

        base_score = base_row.iloc[0]["sovereign_score"]
        quote_score = quote_row.iloc[0]["sovereign_score"]

        diff = base_score - quote_score
        bias = classify_bias(diff)

        results.append({
            "pair": pair,
            "base": base,
            "quote": quote,
            "base_score": base_score,
            "quote_score": quote_score,
            "score_diff": diff,
            "macro_bias": bias
        })

    result_df = pd.DataFrame(results)

    print(f"\nFull macro FX comparison ({latest_year}):")
    print(result_df.to_string(index=False))


if __name__ == "__main__":
    main()