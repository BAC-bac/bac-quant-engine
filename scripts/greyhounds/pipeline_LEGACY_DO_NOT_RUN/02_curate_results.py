from pathlib import Path
import pandas as pd
import re

STAGING_PATH = Path("/mnt/quant_lab/staging/betfair_greyhound_results_raw.parquet")
TRACK_MAP_PATH = Path("/mnt/quant_lab/reference/track_mapping_manual.csv")
OUT_PATH = Path("/mnt/quant_lab/curated/results_curated.parquet")
UNMAPPED_PATH = Path("/mnt/quant_lab/analysis/unmapped_results_tracks.csv")

def clean_dog_name(name: str) -> str | None:
    if pd.isna(name):
        return None
    name = str(name).strip().lower()
    name = re.sub(r"^\d+\.\s*", "", name)   # remove "4. "
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or None

def extract_track(menu_hint: str) -> str | None:
    if pd.isna(menu_hint):
        return None
    s = str(menu_hint).strip()
    s = re.sub(r"\s+\d{1,2}(st|nd|rd|th)\s+[A-Za-z]{3,}$", "", s).strip()
    return s or None

def build_track_mapping() -> dict:
    track_map = pd.read_csv(TRACK_MAP_PATH)

    raw_col = "results_track_raw"
    key_col = "track_key"

    if raw_col not in track_map.columns or key_col not in track_map.columns:
        raise KeyError(
            f"Expected columns '{raw_col}' and '{key_col}' in {TRACK_MAP_PATH}, "
            f"but found {track_map.columns.tolist()}"
        )

    mapping = dict(
        zip(
            track_map[raw_col].astype(str).str.strip().str.lower(),
            track_map[key_col].astype(str).str.strip().str.lower()
        )
    )
    return mapping

def main() -> None:
    df = pd.read_parquet(STAGING_PATH)
    track_mapping = build_track_mapping()

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce").dt.date

    df["track_raw"] = df["menu_hint"].apply(extract_track)
    df["track_raw_clean"] = df["track_raw"].astype("string").str.strip().str.lower()
    df["track_key"] = df["track_raw_clean"].map(track_mapping)

    df["dog_name_raw"] = df["selection_name"].astype("string").str.strip()
    df["dog_clean"] = df["dog_name_raw"].apply(clean_dog_name)

    df["bsp"] = pd.to_numeric(df["bsp"], errors="coerce")
    df["win_lose"] = pd.to_numeric(df["win_lose"], errors="coerce")
    df["win_flag"] = df["win_lose"].fillna(0).astype(int)

    curated = df[
        [
            "race_date",
            "event_dt",
            "event_id",
            "menu_hint",
            "event_name",
            "track_raw",
            "track_key",
            "selection_id",
            "dog_name_raw",
            "dog_clean",
            "win_lose",
            "win_flag",
            "bsp",
            "ppwap",
            "morningwap",
            "pptradedvol",
            "iptradedvol",
            "source_file",
        ]
    ].copy()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    curated.to_parquet(OUT_PATH, index=False)

    unmapped = (
        curated[curated["track_key"].isna()]
        .groupby("track_raw", dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values("rows", ascending=False)
    )

    UNMAPPED_PATH.parent.mkdir(parents=True, exist_ok=True)
    unmapped.to_csv(UNMAPPED_PATH, index=False)

    print(f"Saved curated results: {OUT_PATH}")
    print(f"Rows: {len(curated):,}")
    print(f"Date range: {curated['race_date'].min()} -> {curated['race_date'].max()}")
    print(f"Missing track_key rate: {curated['track_key'].isna().mean():.2%}")
    print(f"Missing dog_clean rate: {curated['dog_clean'].isna().mean():.2%}")
    print(f"Saved unmapped tracks report: {UNMAPPED_PATH}")

    print("\nTop unmapped tracks:")
    print(unmapped.head(25).to_string(index=False))

    print("\nSample curated rows:")
    print(curated.head(10).to_string(index=False))

if __name__ == "__main__":
    main()