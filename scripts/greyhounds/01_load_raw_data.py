from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


def load_paths() -> dict:
    """Load project path settings from config/paths.yaml."""
    if not PATHS_CONFIG.exists():
        raise FileNotFoundError(f"Missing config file: {PATHS_CONFIG}")

    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def ensure_folder(path: Path) -> None:
    """Create folder if it does not already exist."""
    path.mkdir(parents=True, exist_ok=True)


def find_csv_files(folder_path: Path) -> list[Path]:
    """Find all CSV files recursively inside a folder."""
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder_path}")

    return sorted(folder_path.rglob("*.csv"))


def load_csv_files(folder_path: Path) -> pd.DataFrame:
    """Load all CSV files from a folder into one DataFrame."""
    csv_files = find_csv_files(folder_path)

    if not csv_files:
        print(f"No CSV files found in: {folder_path}")
        return pd.DataFrame()

    frames = []

    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path)
            df["source_file"] = str(file_path)
            frames.append(df)
            print(f"Loaded: {file_path.name} | rows: {len(df)}")
        except Exception as error:
            print(f"Failed to load {file_path}: {error}")

    if not frames:
        return pd.DataFrame()

    clean_frames = [frame.dropna(axis=1, how="all") for frame in frames if not frame.empty]

    if not clean_frames:
        return pd.DataFrame()

    return pd.concat(clean_frames, ignore_index=True)


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise column names."""
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return df


def main() -> None:
    print("Starting Greyhound raw results loader...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    raw_results_dir = Path(greyhound_paths["raw_results"])
    interim_dir = Path(greyhound_paths["interim"])

    ensure_folder(interim_dir)

    print(f"Reading raw results from: {raw_results_dir}")

    results_df = load_csv_files(raw_results_dir)

    if results_df.empty:
        print("No results data loaded. Check your raw_results path.")
        return

    results_df = clean_column_names(results_df)

    output_path = interim_dir / "greyhound_results_raw_combined.csv"
    results_df.to_csv(output_path, index=False)

    print("\nLoad complete.")
    print(f"Rows loaded: {len(results_df):,}")
    print(f"Columns: {len(results_df.columns)}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()