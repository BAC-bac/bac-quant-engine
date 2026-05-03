from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


CANDIDATE_SYSTEMS = {
    "BSP_8_TO_13": {
        "description": "Back all UK greyhounds with BSP between 8.00 and 13.00",
        "track_key": None,
        "trap": None,
        "bsp_min": 8.00,
        "bsp_max": 13.00,
    },
    "ROMFORD_TRAP_3": {
        "description": "Back Romford trap 3",
        "track_key": "romford",
        "trap": 3,
        "bsp_min": None,
        "bsp_max": None,
    },
    "HARLOW_TRAP_1": {
        "description": "Back Harlow trap 1",
        "track_key": "harlow",
        "trap": 1,
        "bsp_min": None,
        "bsp_max": None,
    },
    "HENLOW_TRAP_2": {
        "description": "Back Henlow trap 2",
        "track_key": "henlow",
        "trap": 2,
        "bsp_min": None,
        "bsp_max": None,
    },
    "TOWCESTER_TRAP_3": {
        "description": "Back Towcester trap 3",
        "track_key": "towcester",
        "trap": 3,
        "bsp_min": None,
        "bsp_max": None,
    },
    "ROMFORD_TRAP_3_BSP_8_TO_13": {
        "description": "Back Romford trap 3 only when BSP is between 8.00 and 13.00",
        "track_key": "romford",
        "trap": 3,
        "bsp_min": 8.00,
        "bsp_max": 13.00,
    },
    "HARLOW_TRAP_1_BSP_8_TO_13": {
        "description": "Back Harlow trap 1 only when BSP is between 8.00 and 13.00",
        "track_key": "harlow",
        "trap": 1,
        "bsp_min": 8.00,
        "bsp_max": 13.00,
    },
    "HENLOW_TRAP_2_BSP_8_TO_13": {
        "description": "Back Henlow trap 2 only when BSP is between 8.00 and 13.00",
        "track_key": "henlow",
        "trap": 2,
        "bsp_min": 8.00,
        "bsp_max": 13.00,
    },
    "TOWCESTER_TRAP_3_BSP_8_TO_13": {
        "description": "Back Towcester trap 3 only when BSP is between 8.00 and 13.00",
        "track_key": "towcester",
        "trap": 3,
        "bsp_min": 8.00,
        "bsp_max": 13.00,
    },
}


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def apply_candidate_filter(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    system_df = df.copy()

    if rules["track_key"] is not None:
        system_df = system_df[system_df["track_key"] == rules["track_key"]]

    if rules["trap"] is not None:
        system_df = system_df[system_df["trap"] == rules["trap"]]

    if rules["bsp_min"] is not None:
        system_df = system_df[system_df["bsp"] >= rules["bsp_min"]]

    if rules["bsp_max"] is not None:
        system_df = system_df[system_df["bsp"] < rules["bsp_max"]]

    return system_df.copy()


def calculate_max_drawdown(profit_series: pd.Series) -> float:
    if profit_series.empty:
        return 0.0

    equity = profit_series.cumsum()
    running_peak = equity.cummax()
    drawdown = equity - running_peak

    return float(drawdown.min())


def summarise_system(system_name: str, description: str, system_df: pd.DataFrame) -> dict:
    bets = len(system_df)

    if bets == 0:
        return {
            "system_name": system_name,
            "description": description,
            "bets": 0,
            "races": 0,
            "winners": 0,
            "strike_rate": 0.0,
            "avg_bsp": None,
            "median_bsp": None,
            "total_profit": 0.0,
            "roi": 0.0,
            "max_drawdown_points": 0.0,
            "profit_factor": None,
            "date_min": None,
            "date_max": None,
        }

    winners = int(system_df["is_winner"].sum())
    total_profit = float(system_df["back_profit_1pt"].sum())

    gross_profit = system_df.loc[system_df["back_profit_1pt"] > 0, "back_profit_1pt"].sum()
    gross_loss = abs(system_df.loc[system_df["back_profit_1pt"] < 0, "back_profit_1pt"].sum())

    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None

    return {
        "system_name": system_name,
        "description": description,
        "bets": bets,
        "races": system_df["event_id"].nunique(),
        "winners": winners,
        "strike_rate": winners / bets,
        "avg_bsp": system_df["bsp"].mean(),
        "median_bsp": system_df["bsp"].median(),
        "total_profit": total_profit,
        "roi": total_profit / bets,
        "max_drawdown_points": calculate_max_drawdown(system_df["back_profit_1pt"]),
        "profit_factor": profit_factor,
        "date_min": system_df["event_dt"].min(),
        "date_max": system_df["event_dt"].max(),
    }


def build_period_report(system_df: pd.DataFrame, system_name: str, period_col: str) -> pd.DataFrame:
    if system_df.empty:
        return pd.DataFrame()

    report = (
        system_df.groupby(period_col, dropna=False)
        .agg(
            bets=("event_id", "size"),
            races=("event_id", "nunique"),
            winners=("is_winner", "sum"),
            avg_bsp=("bsp", "mean"),
            median_bsp=("bsp", "median"),
            total_profit=("back_profit_1pt", "sum"),
        )
        .reset_index()
    )

    report["system_name"] = system_name
    report["strike_rate"] = report["winners"] / report["bets"]
    report["roi"] = report["total_profit"] / report["bets"]

    cols = ["system_name", period_col, "bets", "races", "winners", "strike_rate", "avg_bsp", "median_bsp", "total_profit", "roi"]

    return report[cols]


def main() -> None:
    print("Starting candidate system tests...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    curated_dir = Path(greyhound_paths["curated"])
    reports_dir = Path(greyhound_paths["reports"]) / "candidate_systems"

    reports_dir.mkdir(parents=True, exist_ok=True)

    input_path = curated_dir / "greyhound_results_uk_analysis_base.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df = df.sort_values("event_dt").reset_index(drop=True)

    df["year"] = df["event_dt"].dt.year
    df["year_month"] = df["event_dt"].dt.to_period("M").astype(str)

    print(f"Rows loaded: {len(df):,}")
    print(f"Races loaded: {df['event_id'].nunique():,}")

    summary_rows = []
    yearly_reports = []
    monthly_reports = []
    bet_logs = []

    for system_name, rules in CANDIDATE_SYSTEMS.items():
        description = rules["description"]

        system_df = apply_candidate_filter(df, rules)
        system_df = system_df.sort_values("event_dt").reset_index(drop=True)

        system_df["system_name"] = system_name
        system_df["system_description"] = description
        system_df["bet_number"] = range(1, len(system_df) + 1)
        system_df["equity_curve_points"] = system_df["back_profit_1pt"].cumsum()
        system_df["running_peak_points"] = system_df["equity_curve_points"].cummax()
        system_df["drawdown_points"] = system_df["equity_curve_points"] - system_df["running_peak_points"]

        summary_rows.append(summarise_system(system_name, description, system_df))
        yearly_reports.append(build_period_report(system_df, system_name, "year"))
        monthly_reports.append(build_period_report(system_df, system_name, "year_month"))

        bet_log_cols = [
            "system_name",
            "system_description",
            "bet_number",
            "event_id",
            "event_dt",
            "race_date",
            "year",
            "year_month",
            "track_key",
            "race_time",
            "trap",
            "dog_clean",
            "bsp",
            "is_winner",
            "back_profit_1pt",
            "equity_curve_points",
            "drawdown_points",
        ]

        available_cols = [col for col in bet_log_cols if col in system_df.columns]
        bet_logs.append(system_df[available_cols].copy())

        print(
            f"Tested {system_name}: "
            f"{len(system_df):,} bets | "
            f"profit {system_df['back_profit_1pt'].sum():.2f} pts | "
            f"ROI {(system_df['back_profit_1pt'].sum() / len(system_df)) if len(system_df) else 0:.4f}"
        )

    summary = pd.DataFrame(summary_rows)
    yearly = pd.concat(yearly_reports, ignore_index=True) if yearly_reports else pd.DataFrame()
    monthly = pd.concat(monthly_reports, ignore_index=True) if monthly_reports else pd.DataFrame()
    all_bet_logs = pd.concat(bet_logs, ignore_index=True) if bet_logs else pd.DataFrame()

    summary = summary.sort_values(["roi", "bets"], ascending=[False, False])

    summary_path = reports_dir / "candidate_system_summary.csv"
    yearly_path = reports_dir / "candidate_system_yearly.csv"
    monthly_path = reports_dir / "candidate_system_monthly.csv"
    bet_logs_path = reports_dir / "candidate_system_bet_logs.parquet"
    bet_logs_sample_path = reports_dir / "candidate_system_bet_logs_sample.csv"

    summary.to_csv(summary_path, index=False)
    yearly.to_csv(yearly_path, index=False)
    monthly.to_csv(monthly_path, index=False)
    all_bet_logs.to_parquet(bet_logs_path, index=False)
    all_bet_logs.head(10000).to_csv(bet_logs_sample_path, index=False)

    print("\nCandidate system testing complete.")
    print(f"Saved summary to: {summary_path}")
    print(f"Saved yearly report to: {yearly_path}")
    print(f"Saved monthly report to: {monthly_path}")
    print(f"Saved bet logs to: {bet_logs_path}")
    print(f"Saved bet log sample to: {bet_logs_sample_path}")

    print("\nSystem summary:")
    print(summary.round(4))


if __name__ == "__main__":
    main()