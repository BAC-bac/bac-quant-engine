from pathlib import Path
import platform

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features.csv"


SERIES_MAP = {
    "GGXWDG_NGDP": "debt_to_gdp",
    "NGDP_RPCH": "gdp_growth",
    "PCPIPCH": "inflation",
}


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


def extract_series(df: pd.DataFrame, code: str, value_name: str) -> pd.DataFrame:
    series_df = df[df["SERIES_CODE"].astype(str).str.contains(code, na=False)].copy()

    if series_df.empty:
        raise ValueError(f"No IMF WEO rows found for series code: {code}")

    year_cols = [col for col in series_df.columns if str(col).isdigit()]

    if not year_cols:
        raise ValueError(f"No year columns found for series code: {code}")

    long_df = series_df.melt(
        id_vars=["COUNTRY"],
        value_vars=year_cols,
        var_name="year",
        value_name=value_name,
    )

    long_df["year"] = long_df["year"].astype(int)
    long_df[value_name] = pd.to_numeric(long_df[value_name], errors="coerce")

    return long_df.dropna(subset=[value_name])


def main() -> None:
    config = load_config()
    raw_file = get_imf_weo_source_file(config)

    if not raw_file.exists():
        raise FileNotFoundError(f"Could not find IMF WEO source file: {raw_file}")

    print("=" * 90)
    print("BACQE MACRO 07 - BUILD MACRO FEATURES")
    print("=" * 90)
    print(f"IMF WEO source: {raw_file}")
    print(f"Output file: {OUTPUT_FILE}")
    print("-" * 90)

    df = pd.read_csv(raw_file, low_memory=False)

    required_cols = ["SERIES_CODE", "COUNTRY"]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing required IMF columns: {missing_cols}")

    exclude_terms = [
        "World",
        "Advanced",
        "Emerging",
        "Developing",
        "ASEAN",
        "Euro Area",
        "G20",
        "G7",
        "Latin America",
        "Middle East",
        "Africa",
        "Asia",
    ]

    df = df[
        ~df["COUNTRY"].astype(str).str.contains("|".join(exclude_terms), case=False, na=False)
    ].copy()

    dataframes = []

    for code, name in SERIES_MAP.items():
        extracted = extract_series(df, code, name)
        dataframes.append(extracted)
        print(f"Extracted {name}: {len(extracted):,} rows")

    macro_df = dataframes[0]

    for df_part in dataframes[1:]:
        macro_df = macro_df.merge(df_part, on=["COUNTRY", "year"], how="inner")

    macro_df = macro_df.sort_values(["COUNTRY", "year"]).reset_index(drop=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    macro_df.to_csv(OUTPUT_FILE, index=False)

    print("-" * 90)
    print(f"Saved macro features to: {OUTPUT_FILE}")
    print(f"Rows: {len(macro_df):,}")
    print(f"Countries: {macro_df['COUNTRY'].nunique():,}")
    print(f"Year range: {macro_df['year'].min()} -> {macro_df['year'].max()}")
    print()
    print(macro_df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()