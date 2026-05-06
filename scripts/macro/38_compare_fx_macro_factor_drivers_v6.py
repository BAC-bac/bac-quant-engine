from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_weighted_scores_v5_real_rates.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_macro_factor_comparison_v6.csv"


FX_PAIRS = {
    "GBPUSD": ("United Kingdom", "United States"),
    "EURUSD": ("Germany", "United States"),
    "USDJPY": ("United States", "Japan"),
    "EURGBP": ("Germany", "United Kingdom"),
    "EURJPY": ("Germany", "Japan"),
    "GBPJPY": ("United Kingdom", "Japan"),
    "AUDUSD": ("Australia", "United States"),
    "NZDUSD": ("New Zealand", "United States"),
    "USDCAD": ("United States", "Canada"),
    "USDCHF": ("United States", "Switzerland"),
}


FACTOR_COLUMNS = {
    "debt_score": "Debt / GDP",
    "growth_score": "GDP Growth",
    "inflation_score": "Inflation",
    "external_balance_score": "Current Account",
    "fiscal_balance_score": "Fiscal Balance",
    "rates_yields_score_1_5": "Nominal Rates & Yields",
    "real_rates_score_1_5": "Real Rates",
}


def classify_signal(score_diff: float) -> str:
    if score_diff >= 0.75:
        return "STRONG_BUY_BASE"
    if score_diff >= 0.25:
        return "BUY_BASE"
    if score_diff <= -0.75:
        return "STRONG_SELL_BASE"
    if score_diff <= -0.25:
        return "SELL_BASE"
    return "NEUTRAL"


def classify_conviction(abs_diff: float, alignment_score: float) -> str:
    if abs_diff >= 0.75 and alignment_score >= 0.65:
        return "high"
    if abs_diff >= 0.50 and alignment_score >= 0.50:
        return "medium"
    if abs_diff >= 0.25:
        return "low"
    return "neutral"


def get_latest_country_row(df: pd.DataFrame, country: str) -> pd.Series:
    country_df = df[df["country"] == country].copy()

    if country_df.empty:
        raise ValueError(f"No data found for country: {country}")

    latest_year = country_df["year"].max()
    latest_row = country_df[country_df["year"] == latest_year].iloc[0]

    return latest_row


def main() -> None:
    df = pd.read_csv(INPUT_FILE)

    required_cols = ["country", "year", "macro_score_v5"] + list(FACTOR_COLUMNS.keys())
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    results = []

    for pair, (base_country, quote_country) in FX_PAIRS.items():
        base = get_latest_country_row(df, base_country)
        quote = get_latest_country_row(df, quote_country)

        row = {
            "pair": pair,
            "base_country": base_country,
            "quote_country": quote_country,
            "base_year": int(base["year"]),
            "quote_year": int(quote["year"]),
            "base_macro_score_v5": base["macro_score_v5"],
            "quote_macro_score_v5": quote["macro_score_v5"],
        }

        row["macro_score_diff"] = row["base_macro_score_v5"] - row["quote_macro_score_v5"]
        row["abs_macro_score_diff"] = abs(row["macro_score_diff"])
        row["signal_v6"] = classify_signal(row["macro_score_diff"])

        factor_diffs = {}

        for col, readable_name in FACTOR_COLUMNS.items():
            base_value = base[col]
            quote_value = quote[col]
            diff = base_value - quote_value

            factor_diffs[col] = diff

            clean_name = (
                readable_name.lower()
                .replace(" / ", "_")
                .replace(" & ", "_")
                .replace(" ", "_")
            )

            row[f"base_{clean_name}"] = base_value
            row[f"quote_{clean_name}"] = quote_value
            row[f"{clean_name}_diff"] = diff

        dominant_factor_col = max(
            factor_diffs,
            key=lambda col: abs(factor_diffs[col]) if pd.notna(factor_diffs[col]) else -1,
        )

        dominant_factor_diff = factor_diffs[dominant_factor_col]

        row["dominant_factor"] = FACTOR_COLUMNS[dominant_factor_col]
        row["dominant_factor_diff"] = dominant_factor_diff

        if dominant_factor_diff > 0:
            row["dominant_factor_supports"] = "base"
        elif dominant_factor_diff < 0:
            row["dominant_factor_supports"] = "quote"
        else:
            row["dominant_factor_supports"] = "neutral"

        non_missing_diffs = [v for v in factor_diffs.values() if pd.notna(v)]

        if row["macro_score_diff"] > 0:
            aligned = [v for v in non_missing_diffs if v > 0]
        elif row["macro_score_diff"] < 0:
            aligned = [v for v in non_missing_diffs if v < 0]
        else:
            aligned = []

        row["factor_count"] = len(non_missing_diffs)
        row["aligned_factor_count"] = len(aligned)

        if row["factor_count"] > 0:
            row["factor_alignment_score"] = row["aligned_factor_count"] / row["factor_count"]
        else:
            row["factor_alignment_score"] = 0

        row["macro_conviction_v6"] = classify_conviction(
            abs_diff=row["abs_macro_score_diff"],
            alignment_score=row["factor_alignment_score"],
        )

        results.append(row)

    output = pd.DataFrame(results)

    output = output.sort_values(
        ["abs_macro_score_diff", "factor_alignment_score"],
        ascending=[False, False],
    )

    output.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved FX macro factor comparison v6 to: {OUTPUT_FILE}")

    print("\nFX macro factor comparison v6 preview:")
    print(
        output[
            [
                "pair",
                "base_country",
                "quote_country",
                "base_macro_score_v5",
                "quote_macro_score_v5",
                "macro_score_diff",
                "signal_v6",
                "dominant_factor",
                "dominant_factor_diff",
                "dominant_factor_supports",
                "factor_alignment_score",
                "macro_conviction_v6",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()