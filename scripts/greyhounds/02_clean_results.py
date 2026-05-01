from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


USEFUL_COLUMNS = [
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
    "source_file",
]


def load_paths() -> dict:
    if not PATHS_CONFIG.exists():
        raise FileNotFoundError(f"Missing config file: {PATHS_CONFIG}")

    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return df


def extract_trap(selection_name: str):
    if pd.isna(selection_name):
        return None

    text = str(selection_name).strip()

    if not text:
        return None

    first_part = text.split(".")[0].strip()

    if first_part.isdigit():
        return int(first_part)

    return None


def extract_dog_name(selection_name: str):
    if pd.isna(selection_name):
        return None

    text = str(selection_name).strip()

    if "." in text:
        return text.split(".", 1)[1].strip().lower()

    return text.lower()


def extract_track(menu_hint: str):
    if pd.isna(menu_hint):
        return None

    text = str(menu_hint).strip().lower()

    if not text:
        return None

    # Common Betfair-style format examples:
    # "Crayford 20:54"
    # "Romford 19:08"
    # "GB / Crayford / 20:54"
    parts = text.replace("\\", "/").split("/")
    last_part = parts[-1].strip()

    words = last_part.split()

    if len(words) >= 2 and ":" in words[-1]:
        return " ".join(words[:-1]).strip()

    return last_part.strip()


def extract_race_time(menu_hint: str):
    if pd.isna(menu_hint):
        return None

    text = str(menu_hint).strip()

    for part in text.replace("\\", "/").split("/"):
        words = part.strip().split()
        for word in words:
            if ":" in word:
                return word.strip()

    return None


def clean_results(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_column_names(df)

    available_columns = [col for col in USEFUL_COLUMNS if col in df.columns]
    df = df[available_columns].copy()

    if "event_dt" in df.columns:
        df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce", dayfirst=True)
        df["race_date"] = df["event_dt"].dt.date
        df["race_hour"] = df["event_dt"].dt.hour
        df["race_minute"] = df["event_dt"].dt.minute

    if "selection_name" in df.columns:
        df["trap"] = df["selection_name"].apply(extract_trap)
        df["dog_clean"] = df["selection_name"].apply(extract_dog_name)

    if "menu_hint" in df.columns:
        df["track_clean"] = df["menu_hint"].apply(extract_track)
        df["race_time"] = df["menu_hint"].apply(extract_race_time)

    numeric_columns = [
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

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "win_lose" in df.columns:
        df["win_lose"] = df["win_lose"].astype(str).str.strip().str.upper()
        df["is_winner"] = df["win_lose"].eq("WINNER")

    return df


def main() -> None:
    print("Starting Greyhound results cleaning...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    interim_dir = Path(greyhound_paths["interim"])
    curated_dir = Path(greyhound_paths["curated"])

    curated_dir.mkdir(parents=True, exist_ok=True)

    input_path = interim_dir / "greyhound_results_raw_combined.csv"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    print(f"Loading interim file: {input_path}")

    df = pd.read_csv(input_path, low_memory=False)

    print(f"Rows before cleaning: {len(df):,}")
    print(f"Columns before cleaning: {len(df.columns):,}")

    cleaned_df = clean_results(df)

    output_csv = curated_dir / "greyhound_results_curated.csv"
    output_parquet = curated_dir / "greyhound_results_curated.parquet"

    cleaned_df.to_csv(output_csv, index=False)
    cleaned_df.to_parquet(output_parquet, index=False)

    print("\nCleaning complete.")
    print(f"Rows after cleaning: {len(cleaned_df):,}")
    print(f"Columns after cleaning: {len(cleaned_df.columns):,}")
    print(f"CSV saved to: {output_csv}")
    print(f"Parquet saved to: {output_parquet}")

    print("\nPreview:")
    print(cleaned_df.head())


if __name__ == "__main__":
    main()