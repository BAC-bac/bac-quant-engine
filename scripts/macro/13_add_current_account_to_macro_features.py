from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = PROJECT_ROOT / "macro_data" / "raw" / "imf_weo_2026_macro.csv"
MACRO_FEATURES_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features_with_current_account.csv"


def extract_current_account(df: pd.DataFrame) -> pd.DataFrame:
    ca_df = df[df["SERIES_CODE"].str.contains("BCA_NGDPD", na=False)].copy()

    year_cols = [col for col in ca_df.columns if col.isdigit()]

    long_df = ca_df.melt(
        id_vars=["COUNTRY"],
        value_vars=year_cols,
        var_name="year",
        value_name="current_account_pct_gdp",
    )

    long_df["year"] = long_df["year"].astype(int)
    long_df["current_account_pct_gdp"] = pd.to_numeric(
        long_df["current_account_pct_gdp"],
        errors="coerce",
    )

    long_df = long_df.dropna(subset=["current_account_pct_gdp"])

    return long_df


def main() -> None:
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Could not find raw IMF file: {RAW_FILE}")

    if not MACRO_FEATURES_FILE.exists():
        raise FileNotFoundError(f"Could not find macro features file: {MACRO_FEATURES_FILE}")

    raw_df = pd.read_csv(RAW_FILE)
    macro_df = pd.read_csv(MACRO_FEATURES_FILE)

    current_account_df = extract_current_account(raw_df)

    merged = macro_df.merge(
        current_account_df,
        on=["COUNTRY", "year"],
        how="left",
    )

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved macro features with current account to: {OUTPUT_FILE}")
    print(f"Rows: {len(merged)}")
    print()
    print(
        merged[
            [
                "COUNTRY",
                "year",
                "debt_to_gdp",
                "gdp_growth",
                "inflation",
                "current_account_pct_gdp",
            ]
        ]
        .head(20)
        .to_string(index=False)
    )

    latest_year = merged["year"].max()
    latest = merged[merged["year"] == latest_year].copy()

    selected = latest[
        latest["COUNTRY"].isin(
            ["United Kingdom", "United States", "Germany", "Japan", "France", "Italy", "China, People's Republic of"]
        )
    ]

    print()
    print(f"Selected countries current account snapshot ({latest_year}):")
    print(
        selected[
            ["COUNTRY", "current_account_pct_gdp"]
        ].sort_values("current_account_pct_gdp", ascending=False).to_string(index=False)
    )


if __name__ == "__main__":
    main()