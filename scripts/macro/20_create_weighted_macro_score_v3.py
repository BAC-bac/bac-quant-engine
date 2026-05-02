from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features_with_fiscal_balance.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_weighted_scores_v3.csv"


WEIGHTS = {
    "debt_score": 0.35,
    "growth_score": 0.20,
    "inflation_score": 0.10,
    "external_balance_score": 0.20,
    "fiscal_balance_score": 0.15,
}


def score_debt(value: float) -> int:
    if value < 60:
        return 1
    elif value < 100:
        return 2
    elif value < 150:
        return 3
    return 4


def score_growth(value: float) -> int:
    if value > 4:
        return 1
    elif value > 2:
        return 2
    elif value > 0:
        return 3
    return 4


def score_inflation(value: float) -> int:
    if 1 <= value <= 3:
        return 1
    elif 0 <= value < 1 or 3 < value <= 6:
        return 2
    return 3


def create_weighted_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["debt_score"] = df["debt_to_gdp"].apply(score_debt)
    df["growth_score"] = df["gdp_growth"].apply(score_growth)
    df["inflation_score"] = df["inflation"].apply(score_inflation)

    df["weighted_sovereign_score_v3"] = (
        df["debt_score"] * WEIGHTS["debt_score"]
        + df["growth_score"] * WEIGHTS["growth_score"]
        + df["inflation_score"] * WEIGHTS["inflation_score"]
        + df["external_balance_score"] * WEIGHTS["external_balance_score"]
        + df["fiscal_balance_score"] * WEIGHTS["fiscal_balance_score"]
    ).round(2)

    return df


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Could not find input file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    scored_df = create_weighted_score(df)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    scored_df.to_csv(OUTPUT_FILE, index=False)

    latest_year = scored_df["year"].max()
    latest = scored_df[scored_df["year"] == latest_year].copy()

    selected = latest[
        latest["COUNTRY"].isin(
            [
                "United Kingdom",
                "United States",
                "Germany",
                "Japan",
                "France",
                "Italy",
                "China, People's Republic of",
            ]
        )
    ].copy()

    selected = selected.sort_values("weighted_sovereign_score_v3")

    print(f"Saved weighted sovereign scores v3 to: {OUTPUT_FILE}")
    print()
    print(f"Selected countries weighted sovereign score v3 ({latest_year}):")
    print(
        selected[
            [
                "COUNTRY",
                "debt_to_gdp",
                "gdp_growth",
                "inflation",
                "current_account_pct_gdp",
                "fiscal_balance_pct_gdp",
                "debt_score",
                "growth_score",
                "inflation_score",
                "external_balance_score",
                "fiscal_balance_score",
                "weighted_sovereign_score_v3",
            ]
        ].to_string(index=False)
    )

    print()
    print(f"Top strongest sovereign profiles v3 ({latest_year}):")
    print(
        latest.sort_values("weighted_sovereign_score_v3")
        [
            [
                "COUNTRY",
                "debt_to_gdp",
                "gdp_growth",
                "inflation",
                "current_account_pct_gdp",
                "fiscal_balance_pct_gdp",
                "weighted_sovereign_score_v3",
            ]
        ]
        .head(15)
        .to_string(index=False)
    )

    print()
    print(f"Top weakest sovereign profiles v3 ({latest_year}):")
    print(
        latest.sort_values("weighted_sovereign_score_v3")
        [
            [
                "COUNTRY",
                "debt_to_gdp",
                "gdp_growth",
                "inflation",
                "current_account_pct_gdp",
                "fiscal_balance_pct_gdp",
                "weighted_sovereign_score_v3",
            ]
        ]
        .tail(15)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()