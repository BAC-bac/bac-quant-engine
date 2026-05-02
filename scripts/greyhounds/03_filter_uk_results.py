from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


UK_TRACKS = {
    "central park",
    "crayford",
    "doncaster",
    "harlow",
    "henlow",
    "kilmarnock",
    "monmore",
    "newcastle",
    "nottingham",
    "oxford",
    "pelaw grange",
    "perry barr",
    "romford",
    "sheffield",
    "sunderland",
    "swindon",
    "towcester",
    "yarmouth",
    "hove",
    "brighton and hove",
}


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def normalise_text(value):
    if pd.isna(value):
        return None

    text = str(value).lower().strip()

    replacements = {
        "brighton & hove": "brighton and hove",
        "pelaw": "pelaw grange",
        "newcastle bags": "newcastle",
        "romford bags": "romford",
        "crayford bags": "crayford",
        "monmore green": "monmore",
        "sunderland bags": "sunderland",
        "sheffield bags": "sheffield",
        "nottingham bags": "nottingham",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    text = text.replace("(uk)", "")
    text = text.replace("  ", " ")
    text = text.strip()

    return text


def derive_track_key(row):
    track_clean = normalise_text(row.get("track_clean"))

    if track_clean in UK_TRACKS:
        return track_clean

    menu_hint = normalise_text(row.get("menu_hint"))

    if menu_hint:
        for track in UK_TRACKS:
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

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    curated_dir = Path(greyhound_paths["curated"])
    reports_dir = Path(greyhound_paths["reports"])

    reports_dir.mkdir(parents=True, exist_ok=True)

    input_path = curated_dir / "greyhound_results_curated.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    print(f"Loading curated results from: {input_path}")

    df = pd.read_parquet(input_path)

    print(f"Rows before UK filter: {len(df):,}")

    df["track_key"] = df.apply(derive_track_key, axis=1)

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
    print(uk_track_counts.head(20))

    print("\nWinner check:")
    print(uk_df["is_winner"].value_counts(dropna=False).head())


if __name__ == "__main__":
    main()