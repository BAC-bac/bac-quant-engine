from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_weighted_scores_v5_real_rates.csv"

OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_factor_decomposition_v6.csv"
LATEST_OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_factor_decomposition_v6_latest.csv"


FACTOR_COLUMNS = {
    "debt_score": "Debt / GDP",
    "growth_score": "GDP Growth",
    "inflation_score": "Inflation",
    "external_balance_score": "Current Account",
    "fiscal_balance_score": "Fiscal Balance",
    "rates_yields_score_1_5": "Nominal Rates & Yields",
    "real_rates_score_1_5": "Real Rates",
}


def classify_score(score: float) -> str:
    if pd.isna(score):
        return "missing"
    if score >= 4.25:
        return "very strong"
    if score >= 3.50:
        return "strong"
    if score >= 2.75:
        return "neutral-positive"
    if score >= 2.25:
        return "neutral"
    if score >= 1.50:
        return "weak"
    return "very weak"


def main() -> None:
    df = pd.read_csv(INPUT_FILE)

    required_cols = ["country", "year", "macro_score_v5"] + list(FACTOR_COLUMNS.keys())
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    factor_df = df[required_cols].copy()

    factor_df["macro_label_v6"] = factor_df["macro_score_v5"].apply(classify_score)

    factor_df["strongest_factor"] = factor_df[list(FACTOR_COLUMNS.keys())].idxmax(axis=1)
    factor_df["weakest_factor"] = factor_df[list(FACTOR_COLUMNS.keys())].idxmin(axis=1)

    factor_df["strongest_factor_name"] = factor_df["strongest_factor"].map(FACTOR_COLUMNS)
    factor_df["weakest_factor_name"] = factor_df["weakest_factor"].map(FACTOR_COLUMNS)

    factor_df["strongest_factor_score"] = factor_df.apply(lambda row: row[row["strongest_factor"]], axis=1)

    factor_df["weakest_factor_score"] = factor_df.apply(lambda row: row[row["weakest_factor"]], axis=1)

    factor_df["factor_spread"] = (
        factor_df["strongest_factor_score"] - factor_df["weakest_factor_score"]
    )

    latest_year = factor_df["year"].max()
    latest_df = factor_df[factor_df["year"] == latest_year].copy()

    factor_df.to_csv(OUTPUT_FILE, index=False)
    latest_df.to_csv(LATEST_OUTPUT_FILE, index=False)

    print(f"Saved full v6 factor decomposition to: {OUTPUT_FILE}")
    print(f"Saved latest v6 factor decomposition to: {LATEST_OUTPUT_FILE}")

    print("\nLatest macro factor decomposition preview:")
    print(
        latest_df[
            [
                "country",
                "year",
                "macro_score_v5",
                "macro_label_v6",
                "strongest_factor_name",
                "strongest_factor_score",
                "weakest_factor_name",
                "weakest_factor_score",
                "factor_spread",
            ]
        ]
        .sort_values("macro_score_v5", ascending=False)
        .head(25)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()