from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RATES_FILE = PROJECT_ROOT / "macro_data" / "processed" / "rates_yields_snapshot.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "rates_yields_snapshot_with_bonds.csv"


BOND_YIELD_UPDATES = {
    "United Kingdom": {
        "gov_2y_yield": 4.46,
        "gov_10y_yield": 5.06,
    },
    "United States": {
        "gov_2y_yield": None,
        "gov_10y_yield": None,
    },
    "Germany": {
        "gov_2y_yield": 2.62,
        "gov_10y_yield": 3.05,
    },
    "Japan": {
        "gov_2y_yield": 1.38,
        "gov_10y_yield": 2.49,
    },
    "China, People's Republic of": {
        "gov_2y_yield": None,
        "gov_10y_yield": None,
    },
}


def main() -> None:
    df = pd.read_csv(RATES_FILE)

    df.columns = df.columns.str.lower().str.strip()

    required_cols = ["country", "policy_rate", "gov_2y_yield", "gov_10y_yield"]

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    for country, updates in BOND_YIELD_UPDATES.items():
        mask = df["country"] == country

        if not mask.any():
            print(f"[WARN] Country not found in rates file: {country}")
            continue

        for col, value in updates.items():
            if value is not None:
                df.loc[mask, col] = value

    df["yield_curve_10y_2y"] = df["gov_10y_yield"] - df["gov_2y_yield"]

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSaved updated rates/yields snapshot to: {OUTPUT_FILE}")

    print("\nRates/yields snapshot preview:")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()