from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_weighted_scores_v4_rates.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_macro_bias_v4_rates.csv"


FX_PAIRS = [
    ("GBPUSD", "United Kingdom", "United States"),
    ("EURUSD", "Germany", "United States"),
    ("USDJPY", "United States", "Japan"),
    ("EURGBP", "Germany", "United Kingdom"),
    ("EURJPY", "Germany", "Japan"),
    ("GBPJPY", "United Kingdom", "Japan"),
    ("USDCNY_proxy", "United States", "China, People's Republic of"),
]


def classify_bias(diff: float, threshold: float = 0.3) -> str:
    if diff >= threshold:
        return "bullish_base"
    elif diff <= -threshold:
        return "bearish_base"
    return "neutral"


def main() -> None:
    df = pd.read_csv(INPUT_FILE)
    df.columns = df.columns.str.lower()

    latest_year = df["year"].max()
    latest = df[df["year"] == latest_year].copy()

    rows = []

    for pair, base_country, quote_country in FX_PAIRS:
        base_row = latest[latest["country"] == base_country]
        quote_row = latest[latest["country"] == quote_country]

        if base_row.empty:
            print(f"[WARN] Missing base country: {base_country}")
            continue

        if quote_row.empty:
            print(f"[WARN] Missing quote country: {quote_country}")
            continue

        base_score = float(base_row["macro_score_v4"].iloc[0])
        quote_score = float(quote_row["macro_score_v4"].iloc[0])
        diff = base_score - quote_score

        rows.append(
            {
                "pair": pair,
                "base_country": base_country,
                "quote_country": quote_country,
                "base_macro_score_v4": round(base_score, 4),
                "quote_macro_score_v4": round(quote_score, 4),
                "score_diff": round(diff, 4),
                "macro_bias_v4": classify_bias(diff),
            }
        )

    output = pd.DataFrame(rows)
    output.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved FX macro bias v4 rates to: {OUTPUT_FILE}")
    print("\nFX macro comparison v4 with rates:")
    print(output.to_string(index=False))


if __name__ == "__main__":
    main()