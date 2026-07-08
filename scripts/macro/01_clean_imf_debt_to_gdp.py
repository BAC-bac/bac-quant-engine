from pathlib import Path
import platform

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "debt_to_gdp_long.csv"


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


def load_imf_file(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Could not find IMF WEO source file: {file_path}")

    return pd.read_csv(file_path, low_memory=False)


def clean_debt_to_gdp(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["SERIES_CODE", "DATASET", "COUNTRY", "INDICATOR", "FREQUENCY"]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f"Missing required IMF columns: {missing_cols}")

    debt_df = df[df["SERIES_CODE"].astype(str).str.contains("GGXWDG_NGDP", na=False)].copy()

    if debt_df.empty:
        raise ValueError("No GGXWDG_NGDP debt-to-GDP series found in IMF WEO file.")

    year_columns = [col for col in debt_df.columns if str(col).isdigit()]

    if not year_columns:
        raise ValueError("No year columns found in IMF WEO file.")

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
    config = load_config()
    raw_file = get_imf_weo_source_file(config)

    print("=" * 90)
    print("BACQE MACRO 01 - CLEAN IMF DEBT TO GDP")
    print("=" * 90)
    print(f"IMF WEO source: {raw_file}")
    print(f"Output file: {OUTPUT_FILE}")
    print("-" * 90)

    df = load_imf_file(raw_file)
    debt_long = clean_debt_to_gdp(df)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    debt_long.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved cleaned debt-to-GDP data to: {OUTPUT_FILE}")
    print(f"Rows: {len(debt_long):,}")
    print(f"Countries: {debt_long['COUNTRY'].nunique():,}")
    print(f"Year range: {debt_long['year'].min()} -> {debt_long['year'].max()}")
    print()
    print(debt_long.head(20).to_string(index=False))


if __name__ == "__main__":
    main()