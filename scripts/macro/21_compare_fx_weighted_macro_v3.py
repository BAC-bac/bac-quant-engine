from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_weighted_scores_v3.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_weighted_macro_bias_v3.csv"

SCORE_COL = "weighted_sovereign_score_v3"


FX_PAIRS = {
    "GBPUSD": ("United Kingdom", "United States"),
    "EURUSD": ("Germany", "United States"),
    "USDJPY": ("United States", "Japan"),
    "EURGBP": ("Germany", "United Kingdom"),
    "EURJPY": ("Germany", "Japan"),
    "GBPJPY": ("United Kingdom", "Japan"),
    "USDCNY_proxy": ("United States", "China, People's Republic of"),
}


def classify_bias(diff: float) -> str:
    if diff <= -0.5:
        return "bullish_base"
    elif diff >= 0.5:
        return "bearish_base"
    return "neutral"


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Could not find input file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    latest_year = df["year"].max()
    latest = df[df["year"] == latest_year].copy()

    results = []

    for pair, (base_country, quote_country) in FX_PAIRS.items():
        base_row = latest[latest["COUNTRY"] == base_country]
        quote_row = latest[latest["COUNTRY"] == quote_country]

        if base_row.empty or quote_row.empty:
            print(f"Skipping {pair}: missing country data")
            continue

        base_score = float(base_row.iloc[0][SCORE_COL])
        quote_score = float(quote_row.iloc[0][SCORE_COL])

        diff = round(base_score - quote_score, 2)

        results.append(
            {
                "pair": pair,
                "base_country": base_country,
                "quote_country": quote_country,
                "base_score_v3": base_score,
                "quote_score_v3": quote_score,
                "score_diff": diff,
                "macro_bias_v3": classify_bias(diff),
            }
        )

    result_df = pd.DataFrame(results)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved FX weighted macro bias v3 to: {OUTPUT_FILE}")
    print()
    print(f"Weighted macro FX comparison v3 ({latest_year}):")
    print(result_df.to_string(index=False))


if __name__ == "__main__":
    main()