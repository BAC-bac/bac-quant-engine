from pathlib import Path
import pandas as pd

RAW_BASE = Path("/mnt/quant_lab/raw/Greyhound Racing")
OUT_PATH = Path("/mnt/quant_lab/staging/betfair_greyhound_results_raw.parquet")

KEEP_COLS = [
    "event_id",
    "menu_hint",
    "event_name",
    "event_dt",
    "selection_id",
    "selection_name",
    "win_lose",
    "bsp",
    "ppwap",
    "morningwap",
    "ppmax",
    "ppmin",
    "ipmax",
    "ipmin",
    "morningtradedvol",
    "pptradedvol",
    "iptradedvol",
]

def find_result_files():
    # Only include daily Betfair greyhound result files under Results folders
    files = [
        f for f in RAW_BASE.rglob("dwbfgreyhoundwin*.csv")
        if "/Results/" in str(f).replace("\\", "/")
    ]
    return sorted(files)

def read_result_file(file: Path) -> pd.DataFrame | None:
    # Try normal parser first
    try:
        df = pd.read_csv(file, low_memory=False)
    except Exception:
        # Fallback for malformed rows
        try:
            df = pd.read_csv(
                file,
                engine="python",
                on_bad_lines="skip",
            )
        except Exception as e:
            print(f"[FAIL] {file}: {e}")
            return None

    df["source_file"] = str(file)

    existing_cols = [c for c in KEEP_COLS if c in df.columns]

    # Skip files that clearly are not in the expected schema
    if not existing_cols:
        print(f"[SKIP] No expected result columns in {file}")
        return None

    df = df[existing_cols + ["source_file"]].copy()
    return df

def load_all_results() -> pd.DataFrame:
    csv_files = find_result_files()
    if not csv_files:
        raise FileNotFoundError(f"No result CSV files found under {RAW_BASE}")

    dfs = []

    print(f"Found {len(csv_files):,} candidate result files")

    for i, file in enumerate(csv_files, start=1):
        df = read_result_file(file)
        if df is not None and not df.empty:
            dfs.append(df)

        if i % 250 == 0 or i == len(csv_files):
            print(f"Processed {i:,}/{len(csv_files):,} files")

    if not dfs:
        raise ValueError("No valid result files could be loaded.")

    combined = pd.concat(dfs, ignore_index=True)
    return combined

def main() -> None:
    combined = load_all_results()

    if "event_dt" in combined.columns:
        combined["event_dt"] = pd.to_datetime(
            combined["event_dt"],
            format="%d-%m-%Y %H:%M",
            errors="coerce"
        )
        combined["race_date"] = combined["event_dt"].dt.date
    else:
        combined["event_dt"] = pd.NaT
        combined["race_date"] = pd.NaT

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUT_PATH, index=False)

    print(f"\nSaved staging file: {OUT_PATH}")
    print(f"Rows: {len(combined):,}")
    print(f"Columns: {combined.columns.tolist()}")

    if "race_date" in combined.columns:
        print(f"Date range: {combined['race_date'].min()} -> {combined['race_date'].max()}")

    print("\nSample:")
    print(combined.head(5).to_string(index=False))

if __name__ == "__main__":
    main()
