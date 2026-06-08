from pathlib import Path
import pandas as pd

INPUT_PATH = Path(
    r"E:\Quant_Lab\data\analysis\microstructure\candidate_validation_review"
    r"\microstructure_candidate_validation_review_latest.csv"
)

OUTPUT_DIR = Path(
    r"E:\Quant_Lab\data\analysis\dukascopy_ticks\candidate_replay_prep"
)

SYMBOL = "EURUSD"


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 12 - INVENTORY SCRIPT 48 EURUSD CANDIDATES")
    print("=" * 90)

    if not INPUT_PATH.exists():
        print(f"[ERROR] Input file not found: {INPUT_PATH}")
        return

    df = pd.read_csv(INPUT_PATH)

    print(f"Loaded rows: {len(df):,}")
    print(f"Input:       {INPUT_PATH}")

    print("\n[COLUMNS]")
    for col in df.columns:
        print(f" - {col}")

    print("\n[HEAD]")
    print(df.head(10).to_string(index=False))

    symbol_cols = [c for c in df.columns if c.lower() in ["symbol", "symbols", "pair", "instrument"]]

    if not symbol_cols:
        print("\n[WARNING] Could not find obvious symbol column.")
        return

    symbol_col = symbol_cols[0]
    print(f"\nDetected symbol column: {symbol_col}")

    eurusd_df = df[df[symbol_col].astype(str).str.upper() == SYMBOL].copy()

    print(f"\nEURUSD rows: {len(eurusd_df):,}")

    print("\n[EURUSD STATUS-LIKE COLUMNS]")
    for col in eurusd_df.columns:
        if any(token in col.lower() for token in ["status", "decision", "action", "surviv", "pass", "keep", "valid"]):
            print(f"\nColumn: {col}")
            print(eurusd_df[col].value_counts(dropna=False).head(20))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    output_path = OUTPUT_DIR / "eurusd_candidate_inventory_from_validation_review.csv"
    eurusd_df.to_csv(output_path, index=False)

    print("\n[OUTPUT]")
    print(f"Saved EURUSD inventory: {output_path}")
    print("[DONE] Candidate inventory inspection complete.")


if __name__ == "__main__":
    main()