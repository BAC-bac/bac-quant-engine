from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features_with_current_account.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features_with_external_score.csv"


def score_current_account(value: float) -> int:
    if pd.isna(value):
        return 3
    elif value > 5:
        return 1
    elif value >= 0:
        return 2
    elif value >= -3:
        return 3
    elif value >= -6:
        return 4
    else:
        return 5


def label_current_account(value: float) -> str:
    if pd.isna(value):
        return "unknown"
    elif value > 5:
        return "very_strong_surplus"
    elif value >= 0:
        return "supportive_surplus"
    elif value >= -3:
        return "mild_deficit"
    elif value >= -6:
        return "weak_deficit"
    else:
        return "fragile_deficit"


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Could not find input file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    df["external_balance_score"] = df["current_account_pct_gdp"].apply(score_current_account)
    df["external_balance_label"] = df["current_account_pct_gdp"].apply(label_current_account)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    latest_year = df["year"].max()
    latest = df[df["year"] == latest_year].copy()

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

    selected = selected.sort_values("current_account_pct_gdp", ascending=False)

    print(f"Saved macro features with external balance score to: {OUTPUT_FILE}")
    print()
    print(f"Selected countries external balance snapshot ({latest_year}):")
    print(
        selected[
            [
                "COUNTRY",
                "current_account_pct_gdp",
                "external_balance_score",
                "external_balance_label",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()