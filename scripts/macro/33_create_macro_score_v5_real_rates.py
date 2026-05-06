from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_weighted_scores_v4_rates.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_weighted_scores_v5_real_rates.csv"


def min_max_score(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    series = pd.to_numeric(series, errors="coerce")

    min_value = series.min()
    max_value = series.max()

    if pd.isna(min_value) or pd.isna(max_value) or min_value == max_value:
        return pd.Series([0.5] * len(series), index=series.index)

    score = (series - min_value) / (max_value - min_value)

    if not higher_is_better:
        score = 1 - score

    return score


def main() -> None:
    df = pd.read_csv(INPUT_FILE)
    df.columns = df.columns.str.lower().str.strip()

    required_cols = [
        "country",
        "year",
        "inflation",
        "weighted_sovereign_score_v3",
        "policy_rate",
        "gov_2y_yield",
        "gov_10y_yield",
        "macro_score_v4",
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df["inflation"] = pd.to_numeric(df["inflation"], errors="coerce")
    df["policy_rate"] = pd.to_numeric(df["policy_rate"], errors="coerce")
    df["gov_2y_yield"] = pd.to_numeric(df["gov_2y_yield"], errors="coerce")
    df["gov_10y_yield"] = pd.to_numeric(df["gov_10y_yield"], errors="coerce")

    df["real_policy_rate"] = df["policy_rate"] - df["inflation"]
    df["real_2y_yield"] = df["gov_2y_yield"] - df["inflation"]
    df["real_10y_yield"] = df["gov_10y_yield"] - df["inflation"]

    df["real_policy_rate_score"] = min_max_score(df["real_policy_rate"], higher_is_better=True)
    df["real_2y_yield_score"] = min_max_score(df["real_2y_yield"], higher_is_better=True)
    df["real_10y_yield_score"] = min_max_score(df["real_10y_yield"], higher_is_better=True)

    df["real_rates_score"] = (
        df["real_policy_rate_score"] * 0.40
        + df["real_2y_yield_score"] * 0.30
        + df["real_10y_yield_score"] * 0.30
    )

    df["real_rates_score"] = df["real_rates_score"].fillna(0.5)
    df["real_rates_score_1_5"] = 1 + (df["real_rates_score"] * 4)

    df["macro_score_v5"] = (
        df["weighted_sovereign_score_v3"] * 0.60
        + df["rates_yields_score_1_5"] * 0.20
        + df["real_rates_score_1_5"] * 0.20
    )

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved macro v5 real-rates score to: {OUTPUT_FILE}")

    latest_year = df["year"].max()
    latest = df[df["year"] == latest_year].copy()

    display_cols = [
        "country",
        "weighted_sovereign_score_v3",
        "inflation",
        "policy_rate",
        "gov_2y_yield",
        "gov_10y_yield",
        "real_policy_rate",
        "real_2y_yield",
        "real_10y_yield",
        "rates_yields_score_1_5",
        "real_rates_score_1_5",
        "macro_score_v4",
        "macro_score_v5",
    ]

    print("\nLatest macro v5 real-rates preview:")
    print(latest[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()