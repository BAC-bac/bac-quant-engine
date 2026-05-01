from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "debt_to_gdp_long.csv"
CHART_DIR = PROJECT_ROOT / "macro_data" / "processed" / "charts"


COUNTRIES = [
    "United Kingdom",
    "United States",
    "Japan",
    "Germany",
    "France",
    "Italy",
    "China",
]


def main() -> None:
    df = pd.read_csv(INPUT_FILE)

    plot_df = df[df["COUNTRY"].isin(COUNTRIES)].copy()

    plt.figure(figsize=(12, 6))

    for country in COUNTRIES:
        country_df = plot_df[plot_df["COUNTRY"] == country]
        plt.plot(
            country_df["year"],
            country_df["debt_to_gdp"],
            marker="o",
            label=country,
        )

    plt.axhline(60, linestyle="--", linewidth=1, label="60% reference")
    plt.axhline(100, linestyle="--", linewidth=1, label="100% reference")

    plt.fill_between(df["year"], 100, 300, alpha=0.05)

    plt.title("General Government Gross Debt-to-GDP Ratio")
    plt.xlabel("Year")
    plt.ylabel("Debt-to-GDP (%)")
    plt.legend()
    plt.grid(True)

    CHART_DIR.mkdir(parents=True, exist_ok=True)

    output_path = CHART_DIR / "debt_to_gdp_selected_countries.png"
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.show()

    print(f"Chart saved to: {output_path}")


if __name__ == "__main__":
    main()