from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_debt_scores.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_sovereign_debt_bias.csv"

FX_PAIRS = {
    "GBPUSD": ("United Kingdom", "United States"),
    "EURUSD_proxy": ("Germany", "United States"),
    "USDJPY": ("United States", "Japan"),
    "EURGBP_proxy": ("Germany", "United Kingdom"),
}


def classify_fx_bias(base_score: float, quote_score: float) -> str:
    score_diff = base_score - quote_score

    if score_diff <= -2:
        return "bullish_base"
    elif score_diff >= 2:
        return "bearish_base"
    else:
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

        base = base_row.iloc[0]
        quote = quote_row.iloc[0]

        base_score = base["sovereign_debt_score"]
        quote_score = quote["sovereign_debt_score"]
        score_diff = base_score - quote_score

        bias = classify_fx_bias(base_score, quote_score)

        results.append(
            {
                "pair": pair,
                "base_country": base_country,
                "quote_country": quote_country,
                "base_score": base_score,
                "quote_score": quote_score,
                "score_diff": score_diff,
                "macro_debt_bias": bias,
            }
        )

    result_df = pd.DataFrame(results)

    result_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved FX sovereign debt bias file to: {OUTPUT_FILE}")
    print()
    print(f"FX sovereign debt comparison for {latest_year}:")
    print(result_df.to_string(index=False))


if __name__ == "__main__":
    main()