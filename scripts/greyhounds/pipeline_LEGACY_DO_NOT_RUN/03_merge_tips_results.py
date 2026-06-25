from pathlib import Path
import pandas as pd

TIPS_PATH = Path("/mnt/quant_lab/greyhounds/curated/tips/tips_nb_trackkey_ready.csv")
RESULTS_PATH = Path("/mnt/quant_lab/curated/results_curated.parquet")

MERGED_PATH = Path("/mnt/quant_lab/analysis/tips_results_merged.parquet")
MATCHED_PATH = Path("/mnt/quant_lab/analysis/tips_results_matched.parquet")
UNMATCHED_PATH = Path("/mnt/quant_lab/analysis/tips_results_unmatched.parquet")

def main() -> None:
    tips = pd.read_csv(TIPS_PATH)
    results = pd.read_parquet(RESULTS_PATH)

    tips["race_date"] = pd.to_datetime(tips["race_date"], errors="coerce").dt.date
    results["race_date"] = pd.to_datetime(results["race_date"], errors="coerce").dt.date

    # Keep only fully keyed rows
    tips = tips[
        tips["race_date"].notna() &
        tips["track_key"].notna() &
        tips["dog_clean"].notna()
    ].copy()

    results = results[
        results["race_date"].notna() &
        results["track_key"].notna() &
        results["dog_clean"].notna()
    ].copy()

    # Focus on overlap era
    tips = tips[tips["race_date"] >= pd.to_datetime("2024-01-01").date()].copy()
    results = results[results["race_date"] >= pd.to_datetime("2024-01-01").date()].copy()

    merged = tips.merge(
        results,
        on=["race_date", "track_key", "dog_clean"],
        how="left",
        suffixes=("_tip", "_result"),
        indicator=True,
    )

    matched = merged[merged["_merge"] == "both"].copy()
    unmatched = merged[merged["_merge"] == "left_only"].copy()

    MERGED_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(MERGED_PATH, index=False)
    matched.to_parquet(MATCHED_PATH, index=False)
    unmatched.to_parquet(UNMATCHED_PATH, index=False)

    print("Merge outcome:")
    print(merged["_merge"].value_counts(dropna=False).to_string())

    print(f"\nMatched rows:   {len(matched):,}")
    print(f"Unmatched rows: {len(unmatched):,}")

    if len(merged) > 0:
        print(f"Match rate:     {len(matched) / len(merged):.2%}")

    print(f"\nSaved merged file:    {MERGED_PATH}")
    print(f"Saved matched file:   {MATCHED_PATH}")
    print(f"Saved unmatched file: {UNMATCHED_PATH}")

    if not unmatched.empty:
        print("\nTop unmatched track_keys:")
        print(
            unmatched["track_key"]
            .value_counts(dropna=False)
            .head(20)
            .to_string()
        )

        print("\nTop unmatched tracks:")
        print(
            unmatched["track"]
            .value_counts(dropna=False)
            .head(20)
            .to_string()
        )

if __name__ == "__main__":
    main()