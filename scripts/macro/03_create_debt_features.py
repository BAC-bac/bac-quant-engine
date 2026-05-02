from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "debt_to_gdp_long.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "debt_to_gdp_features.csv"


def classify_debt_level(value: float) -> str:
    if value < 60:
        return "low"
    elif value < 100:
        return "moderate"
    elif value < 150:
        return "high"
    else:
        return "extreme"


def classify_debt_trend(change_3y: float) -> str:
    if pd.isna(change_3y):
        return "unknown"
    elif change_3y <= -5:
        return "falling"
    elif change_3y < 5:
        return "stable"
    elif change_3y < 15:
        return "rising"
    else:
        return "surging"


def classify_sovereign_stress(row) -> str:
    level = row["debt_level"]
    trend = row["debt_trend"]

    if level == "extreme":
        return "very_high_stress"

    if level == "high" and trend in ["rising", "surging"]:
        return "high_stress"

    if level == "moderate" and trend == "surging":
        return "watchlist"

    if level == "low" and trend in ["stable", "falling"]:
        return "low_stress"

    return "normal"


def create_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["COUNTRY", "year"]).copy()

    df["debt_change_1y"] = (
        df.groupby("COUNTRY")["debt_to_gdp"]
        .diff(1)
    )

    df["debt_change_3y"] = (
        df.groupby("COUNTRY")["debt_to_gdp"]
        .diff(3)
    )

    df["debt_change_5y"] = (
        df.groupby("COUNTRY")["debt_to_gdp"]
        .diff(5)
    )

    df["debt_level"] = df["debt_to_gdp"].apply(classify_debt_level)

    df["debt_trend"] = df["debt_change_3y"].apply(classify_debt_trend)

    df["sovereign_stress"] = df.apply(classify_sovereign_stress, axis=1)

    return df


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Could not find input file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    exclude_terms = ["World", "Advanced Economies", "Emerging Market", "ASEAN", "Euro Area", "G20", "G7", ]

    df = df[~df["COUNTRY"].str.contains("|".join(exclude_terms), case=False, na=False)].copy().reset_index(drop=True)

    features_df = create_features(df)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    features_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved debt feature file to: {OUTPUT_FILE}")
    print(f"Rows: {len(features_df)}")
    print(features_df.head(20))


if __name__ == "__main__":
    main()