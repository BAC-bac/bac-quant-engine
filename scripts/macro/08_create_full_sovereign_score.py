from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_full_scores.csv"


def score_debt(value):
    if value < 60:
        return 1
    elif value < 100:
        return 2
    elif value < 150:
        return 3
    else:
        return 4


def score_growth(value):
    if value > 4:
        return 1   # strong growth (good)
    elif value > 2:
        return 2
    elif value > 0:
        return 3
    else:
        return 4   # recession (bad)


def score_inflation(value):
    if 1 <= value <= 3:
        return 1   # ideal
    elif 0 <= value < 1 or 3 < value <= 6:
        return 2
    elif value > 6:
        return 3   # too high
    else:
        return 3   # deflation risk


def create_scores(df):
    df = df.copy()

    df["debt_score"] = df["debt_to_gdp"].apply(score_debt)
    df["growth_score"] = df["gdp_growth"].apply(score_growth)
    df["inflation_score"] = df["inflation"].apply(score_inflation)

    # Lower score = healthier
    df["sovereign_score"] = (
        df["debt_score"]
        + df["growth_score"]
        + df["inflation_score"]
    )

    return df


def main():
    df = pd.read_csv(INPUT_FILE)

    scored_df = create_scores(df)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    scored_df.to_csv(OUTPUT_FILE, index=False)

    latest_year = scored_df["year"].max()
    latest = scored_df[scored_df["year"] == latest_year].copy()

    latest = latest.sort_values("sovereign_score")

    print(f"\nTop strongest sovereign profiles ({latest_year}):")
    print(
        latest[
            ["COUNTRY", "debt_to_gdp", "gdp_growth", "inflation", "sovereign_score"]
        ].head(20).to_string(index=False)
    )

    print(f"\nTop weakest sovereign profiles ({latest_year}):")
    print(
        latest[
            ["COUNTRY", "debt_to_gdp", "gdp_growth", "inflation", "sovereign_score"]
        ].tail(20).to_string(index=False)
    )


if __name__ == "__main__":
    main()