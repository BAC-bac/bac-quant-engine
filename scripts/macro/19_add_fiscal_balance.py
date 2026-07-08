from pathlib import Path
import platform

import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features_with_external_score.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "macro_features_with_fiscal_balance.csv"


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
    paths = config["data_lake_root"]
    if platform.system().lower() == "windows":
        return select_existing_path([
            paths.get("windows_network"),
            paths.get("windows_local"),
            paths.get("windows"),
        ])
    return Path(paths["linux"])


def get_imf_weo_source_file(config: dict) -> Path:
    return get_data_lake_root(config) / "data" / "raw" / "macro" / "imf_weo" / "imf_weo_2026_macro.csv"


def extract_fiscal_balance(df: pd.DataFrame) -> pd.DataFrame:
    fiscal_df = df[df["SERIES_CODE"].astype(str).str.contains("GGXCNL_NGDP", na=False)].copy()

    if fiscal_df.empty:
        raise ValueError("No GGXCNL_NGDP fiscal balance series found in IMF WEO file.")

    year_cols = [col for col in fiscal_df.columns if str(col).isdigit()]

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
    if pd.isna(value):
        return 3
    if value > 1:
        return 1
    if value >= 0:
        return 2
    if value >= -3:
        return 3
    if value >= -6:
        return 4
    return 5


def label_fiscal_balance(value: float) -> str:
    if pd.isna(value):
        return "unknown"
    if value > 1:
        return "strong_surplus"
    if value >= 0:
        return "balanced"
    if value >= -3:
        return "mild_deficit"
    if value >= -6:
        return "weak_deficit"
    return "severe_deficit"


def main() -> None:
    config = load_config()
    raw_file = get_imf_weo_source_file(config)

    print("=" * 90)
    print("BACQE MACRO 19 - ADD FISCAL BALANCE")
    print("=" * 90)
    print(f"IMF WEO source: {raw_file}")
    print(f"Input file: {INPUT_FILE}")
    print(f"Output file: {OUTPUT_FILE}")
    print("-" * 90)

    if not raw_file.exists():
        raise FileNotFoundError(f"Could not find IMF WEO source file: {raw_file}")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Could not find input file: {INPUT_FILE}")

    raw_df = pd.read_csv(raw_file, low_memory=False)
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
    print(f"Rows: {len(merged):,}")
    print(f"Fiscal balance coverage: {merged['fiscal_balance_pct_gdp'].notna().mean():.2%}")
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