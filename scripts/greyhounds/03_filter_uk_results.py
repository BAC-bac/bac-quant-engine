from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"
TRACKS_CONFIG = PROJECT_ROOT / "config" / "greyhound_tracks.yaml"


def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")

    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def normalise_text(value):
    if pd.isna(value):
        return None

    text = str(value).lower().strip()
    text = text.replace("\\", "/")
    text = text.replace("(uk)", "")
    text = text.replace("  ", " ")

    return text.strip()


def apply_aliases(text, aliases: dict):
    if text is None:
        return None

    for old, new in aliases.items():
        if old in text:
            text = text.replace(old, new)

    return text.strip()


def derive_track_key(row, uk_tracks: set, aliases: dict):
    track_clean = normalise_text(row.get("track_clean"))
    track_clean = apply_aliases(track_clean, aliases)

    if track_clean in uk_tracks:
        return track_clean

    menu_hint = normalise_text(row.get("menu_hint"))
    menu_hint = apply_aliases(menu_hint, aliases)

    if menu_hint:
        for track in uk_tracks:
            if track in menu_hint:
                return track

    return None


def fix_is_winner(value):
    if pd.isna(value):
        return False

    text = str(value).strip().lower()

    return text in {"1", "1.0", "winner", "win", "won", "true"}


def main() -> None:
    print("Starting UK greyhound results filter...")

    paths = load_yaml(PATHS_CONFIG)
    track_config = load_yaml(TRACKS_CONFIG)

    greyhound_paths = paths["greyhounds"]

    uk_tracks = set(track_config["uk_tracks"])
    aliases = track_config.get("aliases", {})

    curated_dir = Path(greyhound_paths["curated"])
    reports_dir = Path(greyhound_paths["reports"])

    reports_dir.mkdir(parents=True, exist_ok=True)

    input_path = curated_dir / "greyhound_results_curated.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    print(f"Loading curated results from: {input_path}")

    df = pd.read_parquet(input_path)

    print(f"Rows before UK filter: {len(df):,}")
    print(f"UK tracks in config: {len(uk_tracks):,}")
    print(f"Aliases in config: {len(aliases):,}")

    df["track_key"] = df.apply(
        lambda row: derive_track_key(row, uk_tracks, aliases),
        axis=1,
    )

    if "win_lose" in df.columns:
        df["is_winner"] = df["win_lose"].apply(fix_is_winner)

    uk_df = df[df["track_key"].notna()].copy()

    print(f"Rows after UK filter: {len(uk_df):,}")
    print(f"Unique UK tracks found: {uk_df['track_key'].nunique()}")

    uk_track_counts = (
        uk_df["track_key"]
        .value_counts()
        .reset_index()
    )
    uk_track_counts.columns = ["track_key", "row_count"]

    uk_track_counts_path = reports_dir / "uk_track_counts.csv"
    uk_track_counts.to_csv(uk_track_counts_path, index=False)

    unmapped = (
        df[df["track_key"].isna()]
        [["menu_hint", "track_clean"]]
        .drop_duplicates()
        .head(5000)
    )

    unmapped_path = reports_dir / "unmapped_greyhound_tracks_sample.csv"
    unmapped.to_csv(unmapped_path, index=False)

    output_parquet = curated_dir / "greyhound_results_uk.parquet"
    output_csv = curated_dir / "greyhound_results_uk_sample.csv"

    uk_df.to_parquet(output_parquet, index=False)
    uk_df.head(10000).to_csv(output_csv, index=False)

    print("\nUK filtering complete.")
    print(f"Saved UK parquet to: {output_parquet}")
    print(f"Saved UK sample CSV to: {output_csv}")
    print(f"Saved UK track counts to: {uk_track_counts_path}")
    print(f"Saved unmapped sample to: {unmapped_path}")

    print("\nUK track counts preview:")
    print(uk_track_counts.head(25))

    print("\nWinner check:")
    print(uk_df["is_winner"].value_counts(dropna=False).head())


if __name__ == "__main__":
    main()