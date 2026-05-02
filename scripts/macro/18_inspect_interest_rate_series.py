from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = PROJECT_ROOT / "macro_data" / "raw" / "imf_weo_2026_macro.csv"


KEYWORDS = [
    "interest",
    "rate",
    "yield",
    "policy",
    "government bond",
    "treasury",
]


def main() -> None:
    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Could not find raw IMF file: {RAW_FILE}")

    df = pd.read_csv(RAW_FILE)

    search_df = df[
        df["INDICATOR"].str.contains(
            "|".join(KEYWORDS),
            case=False,
            na=False,
        )
    ].copy()

    cols = ["SERIES_CODE", "COUNTRY", "INDICATOR"]

    print("\nPossible interest-rate / yield series found:\n")

    if search_df.empty:
        print("No matching interest-rate series found in this IMF file.")
        return

    print(
        search_df[cols]
        .drop_duplicates()
        .sort_values(["INDICATOR", "COUNTRY"])
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()