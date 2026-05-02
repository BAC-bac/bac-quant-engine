from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "debt_to_gdp_features.csv"
CHART_DIR = PROJECT_ROOT / "macro_data" / "processed" / "charts"

COUNTRIES = [
    "Japan",
    "Italy",
    "United States",
    "France",
    "United Kingdom",
    "China",
    "Germany",
]


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Could not find input file: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    latest_year = df["year"].max()

    latest = df[
        (df["year"] == latest_year)
        & (df["COUNTRY"].isin(COUNTRIES))
    ].copy()

    latest = latest.sort_values("debt_to_gdp", ascending=False)

    CHART_DIR.mkdir(parents=True, exist_ok=True)

    output_path = CHART_DIR / "latest_debt_stress_comparison.png"

    plt.figure(figsize=(11, 6))
    plt.bar(latest["COUNTRY"], latest["debt_to_gdp"])

    plt.axhline(60, linestyle="--", linewidth=1, label="60% reference")
    plt.axhline(100, linestyle="--", linewidth=1, label="100% reference")
    plt.axhline(150, linestyle="--", linewidth=1, label="150% reference")

    plt.title(f"Latest Government Debt-to-GDP Stress Comparison ({latest_year})")
    plt.xlabel("Country")
    plt.ylabel("Debt-to-GDP (%)")
    plt.xticks(rotation=45, ha="right")
    plt.grid(axis="y")
    plt.legend()

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.show()

    print(f"Chart saved to: {output_path}")
    print()
    print(
        latest[
            [
                "COUNTRY",
                "year",
                "debt_to_gdp",
                "debt_change_1y",
                "debt_change_3y",
                "debt_level",
                "debt_trend",
                "sovereign_stress",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()