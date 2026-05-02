from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def save_report(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Saved: {path}")


def main() -> None:
    print("Starting UK greyhound results profiling...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    curated_dir = Path(greyhound_paths["curated"])
    reports_dir = Path(greyhound_paths["reports"])

    input_path = curated_dir / "greyhound_results_uk.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    print(f"Rows loaded: {len(df):,}")
    print(f"Columns loaded: {len(df.columns):,}")

    if "event_dt" in df.columns:
        df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
        df["race_date"] = df["event_dt"].dt.date
        df["year"] = df["event_dt"].dt.year

    summary = pd.DataFrame(
        [
            {
                "rows": len(df),
                "columns": len(df.columns),
                "unique_events": df["event_id"].nunique() if "event_id" in df.columns else None,
                "unique_tracks": df["track_key"].nunique() if "track_key" in df.columns else None,
                "date_min": df["event_dt"].min() if "event_dt" in df.columns else None,
                "date_max": df["event_dt"].max() if "event_dt" in df.columns else None,
                "winner_rows": int(df["is_winner"].sum()) if "is_winner" in df.columns else None,
                "winner_rate": df["is_winner"].mean() if "is_winner" in df.columns else None,
                "missing_bsp_rows": int(df["bsp"].isna().sum()) if "bsp" in df.columns else None,
                "missing_trap_rows": int(df["trap"].isna().sum()) if "trap" in df.columns else None,
            }
        ]
    )

    save_report(summary, reports_dir / "uk_results_summary.csv")

    if "track_key" in df.columns:
        track_report = (
            df.groupby("track_key", dropna=False)
            .agg(
                rows=("event_id", "size"),
                races=("event_id", "nunique"),
                winners=("is_winner", "sum"),
                winner_rate=("is_winner", "mean"),
                avg_bsp=("bsp", "mean"),
                median_bsp=("bsp", "median"),
            )
            .reset_index()
            .sort_values("rows", ascending=False)
        )

        save_report(track_report, reports_dir / "track_win_rates.csv")

    if "trap" in df.columns:
        trap_report = (
            df.groupby("trap", dropna=False)
            .agg(
                rows=("event_id", "size"),
                races=("event_id", "nunique"),
                winners=("is_winner", "sum"),
                winner_rate=("is_winner", "mean"),
                avg_bsp=("bsp", "mean"),
                median_bsp=("bsp", "median"),
            )
            .reset_index()
            .sort_values("trap")
        )

        save_report(trap_report, reports_dir / "trap_win_rates.csv")

    if "year" in df.columns:
        yearly_report = (
            df.groupby("year", dropna=False)
            .agg(
                rows=("event_id", "size"),
                races=("event_id", "nunique"),
                winners=("is_winner", "sum"),
                winner_rate=("is_winner", "mean"),
                unique_tracks=("track_key", "nunique"),
            )
            .reset_index()
            .sort_values("year")
        )

        save_report(yearly_report, reports_dir / "yearly_coverage.csv")

    if {"track_key", "trap"}.issubset(df.columns):
        track_trap_report = (
            df.groupby(["track_key", "trap"], dropna=False)
            .agg(
                rows=("event_id", "size"),
                races=("event_id", "nunique"),
                winners=("is_winner", "sum"),
                winner_rate=("is_winner", "mean"),
                avg_bsp=("bsp", "mean"),
            )
            .reset_index()
            .sort_values(["track_key", "trap"])
        )

        save_report(track_trap_report, reports_dir / "track_trap_win_rates.csv")

    print("\nProfiling complete.")
    print("\nSummary preview:")
    print(summary)


if __name__ == "__main__":
    main()