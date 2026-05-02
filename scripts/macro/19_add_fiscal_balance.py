from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = PROJECT_ROOT / "macro_data" / "raw" / "imf_weo_2026_macro.csv"
INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features_with_external_score.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features_with_fiscal_balance.csv"


def extract_fiscal_balance(df: pd.DataFrame) -> pd.DataFrame:
    fiscal_df = df[df["SERIES_CODE"].str.contains("GGXCNL_NGDP", na=False)].copy()

    year_cols = [col for col in fiscal_df.columns if col.isdigit()]

    long_df = fiscal_df.melt(
        id_vars=["COUNTRY"],
        value_vars=year_cols,
        var_name="year",
        value_name="fiscal_balance_pct_gdp",
    )

    long_df["year"] = long_df["year"].astype(int)
    long_df["fiscal_balance_pct_gdp"] = pd.to_numeric(
        long_df["fiscal_balance_pct_gdp"],
        errors="coerce",
    )

    return long_df.dropna(subset=["fiscal_balance_pct_gdp"])


def score_fiscal_balance(value: float) -> int:
    if value > 1:
        return 1
    elif value >= 0:
        return 2
    elif value >= -3:
        return 3
    elif value >= -6:
        return 4
    return 5


def label_fiscal_balance(value: float) -> str:
    if value > 1:
        return "strong_surplus"
    elif value >= 0:
        return "balanced"
    elif value >= -3:
        return "mild_deficit"
    elif value >= -6:
        return "weak_deficit"
    return "severe_deficit"


def main() -> None:
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Could not find raw IMF file: {RAW_FILE}")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Could not find input file: {INPUT_FILE}")

    raw_df = pd.read_csv(RAW_FILE)
    macro_df = pd.read_csv(INPUT_FILE)

    fiscal_df = extract_fiscal_balance(raw_df)

    merged = macro_df.merge(
        fiscal_df,
        on=["COUNTRY", "year"],
        how="left",
    )

    merged["fiscal_balance_score"] = merged["fiscal_balance_pct_gdp"].apply(score_fiscal_balance)
    merged["fiscal_balance_label"] = merged["fiscal_balance_pct_gdp"].apply(label_fiscal_balance)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)

    latest_year = merged["year"].max()
    latest = merged[merged["year"] == latest_year].copy()

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

    selected = selected.sort_values("fiscal_balance_pct_gdp", ascending=False)

    print(f"Saved macro features with fiscal balance to: {OUTPUT_FILE}")
    print()
    print(f"Selected countries fiscal balance snapshot ({latest_year}):")
    print(
        selected[
            [
                "COUNTRY",
                "fiscal_balance_pct_gdp",
                "fiscal_balance_score",
                "fiscal_balance_label",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()