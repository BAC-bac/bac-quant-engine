from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "debt_to_gdp_features.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_debt_scores.csv"


DEBT_LEVEL_SCORE = {
    "low": 1,
    "moderate": 2,
    "high": 3,
    "extreme": 4,
}

DEBT_TREND_SCORE = {
    "falling": -1,
    "stable": 0,
    "rising": 1,
    "surging": 2,
    "unknown": 0,
}

STRESS_SCORE = {
    "low_stress": 1,
    "normal": 2,
    "watchlist": 3,
    "high_stress": 4,
    "very_high_stress": 5,
}


def create_sovereign_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["debt_level_score"] = df["debt_level"].map(DEBT_LEVEL_SCORE)
    df["debt_trend_score"] = df["debt_trend"].map(DEBT_TREND_SCORE)
    df["stress_score"] = df["sovereign_stress"].map(STRESS_SCORE)

    df["debt_acceleration"] = df["debt_change_1y"] - (df["debt_change_3y"] / 3)

    df["sovereign_debt_score"] = (
        df["debt_level_score"]
        + df["debt_trend_score"]
        + df["stress_score"]
    )

    df["sovereign_debt_score"] = df["sovereign_debt_score"].round(2)

    return df


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Could not find input file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    scored_df = create_sovereign_scores(df)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    scored_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved sovereign debt score file to: {OUTPUT_FILE}")
    print(f"Rows: {len(scored_df)}")

    latest_year = scored_df["year"].max()
    latest = scored_df[scored_df["year"] == latest_year].copy()

    latest = latest.sort_values("sovereign_debt_score", ascending=False)

    print()
    print(f"Top sovereign debt risk scores for {latest_year}:")
    print(
        latest[
            [
                "COUNTRY",
                "year",
                "debt_to_gdp",
                "debt_change_1y",
                "debt_change_3y",
                "debt_level",
                "debt_trend",
                "sovereign_stress",
                "sovereign_debt_score",
            ]
        ]
        .head(20)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()