from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

MACRO_SCORE_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_weighted_scores_v3.csv"
RATES_FILE = PROJECT_ROOT / "macro_data" / "processed" / "rates_yields_snapshot.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "sovereign_weighted_scores_v4_rates.csv"


def create_rates_template_if_missing() -> None:
    if RATES_FILE.exists():
        return

    template = pd.DataFrame(
        {
            "country": [
                "United Kingdom",
                "United States",
                "Germany",
                "Japan",
                "China, People's Republic of",
            ],
            "policy_rate": [None, None, None, None, None],
            "gov_2y_yield": [None, None, None, None, None],
            "gov_10y_yield": [None, None, None, None, None],
        }
    )

    template.to_csv(RATES_FILE, index=False)
    print(f"Created rates/yields template: {RATES_FILE}")


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
    create_rates_template_if_missing()

    macro_df = pd.read_csv(MACRO_SCORE_FILE)
    rates_df = pd.read_csv(RATES_FILE)

    macro_df.columns = macro_df.columns.str.lower()
    rates_df.columns = rates_df.columns.str.lower()

    print("\nLoaded macro scores:")
    print(macro_df.tail())

    print("\nLoaded rates/yields snapshot:")
    print(rates_df)

    df = macro_df.merge(
        rates_df,
        on="country",
        how="left",
    )

    df["yield_curve_10y_2y"] = df["gov_10y_yield"] - df["gov_2y_yield"]

    df["policy_rate_score"] = min_max_score(df["policy_rate"], higher_is_better=True)
    df["gov_2y_yield_score"] = min_max_score(df["gov_2y_yield"], higher_is_better=True)
    df["gov_10y_yield_score"] = min_max_score(df["gov_10y_yield"], higher_is_better=True)
    df["yield_curve_score"] = min_max_score(df["yield_curve_10y_2y"], higher_is_better=True)

    df["rates_yields_score"] = (
        df["policy_rate_score"] * 0.35
        + df["gov_2y_yield_score"] * 0.30
        + df["gov_10y_yield_score"] * 0.20
        + df["yield_curve_score"] * 0.15
    )
    df["rates_yields_score"] = df["rates_yields_score"].fillna(0.5)

    df["rates_yields_score_1_5"] = 1 + (df["rates_yields_score"] * 4)

    df["macro_score_v4"] = (df["weighted_sovereign_score_v3"] * 0.75 + df["rates_yields_score_1_5"] * 0.25)

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved macro scores with rates/yields to: {OUTPUT_FILE}")

    latest_year = df["year"].max() if "year" in df.columns else None

    if latest_year is not None:
        latest = df[df["year"] == latest_year].copy()
    else:
        latest = df.copy()

    display_cols = ["country", "weighted_sovereign_score_v3", "policy_rate", "gov_2y_yield", "gov_10y_yield",
        "yield_curve_10y_2y", "rates_yields_score", "rates_yields_score_1_5", "macro_score_v4", ]

    available_cols = [col for col in display_cols if col in latest.columns]

    print("\nLatest macro v4 rates/yields preview:")
    print(latest[available_cols].to_string(index=False))


if __name__ == "__main__":
    main()