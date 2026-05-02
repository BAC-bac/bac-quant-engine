from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = PROJECT_ROOT / "macro_data" / "raw" / "imf_weo_2026_macro.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features.csv"


SERIES_MAP = {
    "GGXWDG_NGDP": "debt_to_gdp",
    "NGDP_RPCH": "gdp_growth",
    "PCPIPCH": "inflation",
}


def extract_series(df: pd.DataFrame, code: str, value_name: str) -> pd.DataFrame:
    series_df = df[df["SERIES_CODE"].str.contains(code, na=False)].copy()

    year_cols = [col for col in series_df.columns if col.isdigit()]

    long_df = series_df.melt(
        id_vars=["COUNTRY"],
        value_vars=year_cols,
        var_name="year",
        value_name=value_name,
    )

    long_df["year"] = long_df["year"].astype(int)
    long_df[value_name] = pd.to_numeric(long_df[value_name], errors="coerce")

    return long_df.dropna()


def main():
    df = pd.read_csv(RAW_FILE)

    exclude_terms = ["World", "Advanced", "Emerging", "Developing", "ASEAN", "Euro Area", "G20", "G7", "Latin America",
        "Middle East", "Africa", "Asia", ]

    df = df[
        ~df["COUNTRY"].str.contains("|".join(exclude_terms), case=False, na=False)
    ].copy()

    dataframes = []

    for code, name in SERIES_MAP.items():
        extracted = extract_series(df, code, name)
        dataframes.append(extracted)

    macro_df = dataframes[0]

    for df_part in dataframes[1:]:
        macro_df = macro_df.merge(df_part, on=["COUNTRY", "year"], how="inner")

    macro_df = macro_df.sort_values(["COUNTRY", "year"]).reset_index(drop=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    macro_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved macro features to: {OUTPUT_FILE}")
    print(macro_df.head(20))


if __name__ == "__main__":
    main()