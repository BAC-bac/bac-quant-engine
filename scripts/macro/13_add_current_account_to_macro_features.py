from pathlib import Path
import platform

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

MACRO_FEATURES_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features_with_current_account.csv"


def load_config() -> dict:
    with CONFIG_FILE.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def select_existing_path(candidates: list[str]) -> Path:
    for candidate in candidates:
        if candidate is None:
            continue
        path = Path(candidate)
        if path.exists():
            return path

    return Path(next(candidate for candidate in candidates if candidate is not None))


def get_data_lake_root(config: dict) -> Path:
    system = platform.system().lower()
    paths = config["data_lake_root"]

    if system == "windows":
        return select_existing_path(
            [
                paths.get("windows_network"),
                paths.get("windows_local"),
                paths.get("windows"),
            ]
        )

    return Path(paths["linux"])


def get_imf_weo_source_file(config: dict) -> Path:
    data_lake_root = get_data_lake_root(config)
    return data_lake_root / "data" / "raw" / "macro" / "imf_weo" / "imf_weo_2026_macro.csv"


def extract_current_account(df: pd.DataFrame) -> pd.DataFrame:
    ca_df = df[df["SERIES_CODE"].astype(str).str.contains("BCA_NGDPD", na=False)].copy()

    if ca_df.empty:
        raise ValueError("No BCA_NGDPD current account series found in IMF WEO file.")

    year_cols = [col for col in ca_df.columns if str(col).isdigit()]

    if not year_cols:
        raise ValueError("No year columns found for current account series.")

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

    return long_df.dropna(subset=["current_account_pct_gdp"])


def main() -> None:
    config = load_config()
    raw_file = get_imf_weo_source_file(config)

    print("=" * 90)
    print("BACQE MACRO 13 - ADD CURRENT ACCOUNT TO MACRO FEATURES")
    print("=" * 90)
    print(f"IMF WEO source: {raw_file}")
    print(f"Macro features input: {MACRO_FEATURES_FILE}")
    print(f"Output file: {OUTPUT_FILE}")
    print("-" * 90)

    if not raw_file.exists():
        raise FileNotFoundError(f"Could not find IMF WEO source file: {raw_file}")

    if not MACRO_FEATURES_FILE.exists():
        raise FileNotFoundError(f"Could not find macro features file: {MACRO_FEATURES_FILE}")

    raw_df = pd.read_csv(raw_file, low_memory=False)
    macro_df = pd.read_csv(MACRO_FEATURES_FILE)

    current_account_df = extract_current_account(raw_df)

    merged = macro_df.merge(
        current_account_df,
        on=["COUNTRY", "year"],
        how="left",
    )

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
    ]

    print(f"Saved macro features with current account to: {OUTPUT_FILE}")
    print(f"Rows: {len(merged):,}")
    print(f"Countries: {merged['COUNTRY'].nunique():,}")
    print(f"Year range: {merged['year'].min()} -> {merged['year'].max()}")
    print(f"Current account coverage: {merged['current_account_pct_gdp'].notna().mean():.2%}")
    print()
    print(merged.head(20).to_string(index=False))

    print()
    print(f"Selected countries current account snapshot ({latest_year}):")
    print(
        selected[
            ["COUNTRY", "current_account_pct_gdp"]
        ].sort_values("current_account_pct_gdp", ascending=False).to_string(index=False)
    )


if __name__ == "__main__":
    main()