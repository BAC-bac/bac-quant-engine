from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def save_equity_chart(df: pd.DataFrame, system_name: str, output_dir: Path) -> None:
    system_df = df[df["system_name"] == system_name].copy()

    if system_df.empty:
        print(f"No data found for {system_name}")
        return

    system_df["race_date"] = pd.to_datetime(system_df["race_date"], errors="coerce")
    system_df = system_df.sort_values("race_date")

    plt.figure(figsize=(12, 6))
    plt.plot(system_df["race_date"], system_df["equity_points"])
    plt.title(f"{system_name} - Daily Equity Curve")
    plt.xlabel("Date")
    plt.ylabel("Profit / Loss Points")
    plt.grid(True)

    output_path = output_dir / f"{system_name.lower()}_equity_curve.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"Saved equity chart: {output_path}")


def save_drawdown_chart(df: pd.DataFrame, system_name: str, output_dir: Path) -> None:
    system_df = df[df["system_name"] == system_name].copy()

    if system_df.empty:
        print(f"No data found for {system_name}")
        return

    system_df["race_date"] = pd.to_datetime(system_df["race_date"], errors="coerce")
    system_df = system_df.sort_values("race_date")

    plt.figure(figsize=(12, 6))
    plt.plot(system_df["race_date"], system_df["drawdown_points"])
    plt.title(f"{system_name} - Drawdown Curve")
    plt.xlabel("Date")
    plt.ylabel("Drawdown Points")
    plt.grid(True)

    output_path = output_dir / f"{system_name.lower()}_drawdown_curve.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"Saved drawdown chart: {output_path}")


def save_combined_equity_chart(df: pd.DataFrame, output_dir: Path) -> None:
    plt.figure(figsize=(14, 7))

    for system_name in sorted(df["system_name"].unique()):
        system_df = df[df["system_name"] == system_name].copy()
        system_df["race_date"] = pd.to_datetime(system_df["race_date"], errors="coerce")
        system_df = system_df.sort_values("race_date")

        plt.plot(
            system_df["race_date"],
            system_df["equity_points"],
            label=system_name,
        )

    plt.title("Greyhound Candidate Systems - Daily Equity Curves")
    plt.xlabel("Date")
    plt.ylabel("Profit / Loss Points")
    plt.legend()
    plt.grid(True)

    output_path = output_dir / "combined_equity_curves.png"
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"Saved combined equity chart: {output_path}")


def main() -> None:
    print("Starting equity curve plotting...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    reports_dir = Path(greyhound_paths["reports"])
    equity_dir = reports_dir / "equity_curves"
    charts_dir = reports_dir / "charts"

    charts_dir.mkdir(parents=True, exist_ok=True)

    input_path = equity_dir / "system_equity_daily.csv"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing daily equity file: {input_path}")

    df = pd.read_csv(input_path)

    print(f"Rows loaded: {len(df):,}")
    print(f"Systems found: {df['system_name'].nunique()}")

    for system_name in sorted(df["system_name"].unique()):
        save_equity_chart(df, system_name, charts_dir)
        save_drawdown_chart(df, system_name, charts_dir)

    save_combined_equity_chart(df, charts_dir)

    print("\nChart export complete.")
    print(f"Charts saved to: {charts_dir}")


if __name__ == "__main__":
    main()