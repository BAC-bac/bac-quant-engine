from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = PROJECT_ROOT / "macro_data" / "raw" / "imf_weo_2026_macro.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "debt_to_gdp_long.csv"


def load_imf_file(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Could not find file: {file_path}")

    return pd.read_csv(file_path)


def clean_debt_to_gdp(df: pd.DataFrame) -> pd.DataFrame:
    debt_df = df[df["SERIES_CODE"].str.contains("GGXWDG_NGDP", na=False)].copy()

    year_columns = [col for col in debt_df.columns if col.isdigit()]

    long_df = debt_df.melt(
        id_vars=["DATASET", "SERIES_CODE", "COUNTRY", "INDICATOR", "FREQUENCY"],
        value_vars=year_columns,
        var_name="year",
        value_name="debt_to_gdp",
    )

    long_df["year"] = long_df["year"].astype(int)
    long_df["debt_to_gdp"] = pd.to_numeric(long_df["debt_to_gdp"], errors="coerce")

    long_df = long_df.dropna(subset=["debt_to_gdp"])

    long_df = long_df.sort_values(["COUNTRY", "year"]).reset_index(drop=True)

    return long_df


def main() -> None:
    df = load_imf_file(RAW_FILE)

    debt_long = clean_debt_to_gdp(df)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    debt_long.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved cleaned debt-to-GDP data to: {OUTPUT_FILE}")
    print(f"Rows: {len(debt_long)}")
    print(debt_long.head())


if __name__ == "__main__":
    main()