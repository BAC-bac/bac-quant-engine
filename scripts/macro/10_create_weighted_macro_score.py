from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_full_scores.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_weighted_scores.csv"


WEIGHTS = {
    "debt_score": 0.50,
    "growth_score": 0.30,
    "inflation_score": 0.20,
}


def create_weighted_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["weighted_sovereign_score"] = (
        df["debt_score"] * WEIGHTS["debt_score"]
        + df["growth_score"] * WEIGHTS["growth_score"]
        + df["inflation_score"] * WEIGHTS["inflation_score"]
    )

    df["weighted_sovereign_score"] = df["weighted_sovereign_score"].round(2)

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

    latest = latest.sort_values("weighted_sovereign_score")

    print(f"Saved weighted sovereign scores to: {OUTPUT_FILE}")

    print(f"\nTop strongest weighted sovereign profiles ({latest_year}):")
    print(
        latest[
            [
                "COUNTRY",
                "debt_to_gdp",
                "gdp_growth",
                "inflation",
                "debt_score",
                "growth_score",
                "inflation_score",
                "weighted_sovereign_score",
            ]
        ]
        .head(20)
        .to_string(index=False)
    )

    print(f"\nTop weakest weighted sovereign profiles ({latest_year}):")
    print(
        latest[
            [
                "COUNTRY",
                "debt_to_gdp",
                "gdp_growth",
                "inflation",
                "debt_score",
                "growth_score",
                "inflation_score",
                "weighted_sovereign_score",
            ]
        ]
        .tail(20)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()