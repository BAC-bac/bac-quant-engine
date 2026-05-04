from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


SYSTEMS_TO_EXPORT = {
    "BSP_8_TO_13": {
        "track_key": None,
        "trap": None,
        "bsp_min": 8.00,
        "bsp_max": 13.00,
    },
    "ROMFORD_TRAP_3": {
        "track_key": "romford",
        "trap": 3,
        "bsp_min": None,
        "bsp_max": None,
    },
    "HARLOW_TRAP_1": {
        "track_key": "harlow",
        "trap": 1,
        "bsp_min": None,
        "bsp_max": None,
    },
    "HENLOW_TRAP_2": {
        "track_key": "henlow",
        "trap": 2,
        "bsp_min": None,
        "bsp_max": None,
    },
    "TOWCESTER_TRAP_3": {
        "track_key": "towcester",
        "trap": 3,
        "bsp_min": None,
        "bsp_max": None,
    },
}


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def apply_system_filter(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    system_df = df.copy()

    if rules["track_key"] is not None:
        system_df = system_df[system_df["track_key"] == rules["track_key"]]

    if rules["trap"] is not None:
        system_df = system_df[system_df["trap"] == rules["trap"]]

    if rules["bsp_min"] is not None:
        system_df = system_df[system_df["bsp"] >= rules["bsp_min"]]

    if rules["bsp_max"] is not None:
        system_df = system_df[system_df["bsp"] < rules["bsp_max"]]

    return system_df.sort_values("event_dt").reset_index(drop=True)


def add_equity_columns(system_df: pd.DataFrame, system_name: str) -> pd.DataFrame:
    system_df = system_df.copy()

    system_df["system_name"] = system_name
    system_df["bet_number"] = range(1, len(system_df) + 1)
    system_df["equity_points"] = system_df["back_profit_1pt"].cumsum()
    system_df["running_peak_points"] = system_df["equity_points"].cummax()
    system_df["drawdown_points"] = system_df["equity_points"] - system_df["running_peak_points"]
    system_df["is_new_equity_high"] = system_df["equity_points"].eq(system_df["running_peak_points"])

    return system_df


def build_daily_equity(system_df: pd.DataFrame, system_name: str) -> pd.DataFrame:
    if system_df.empty:
        return pd.DataFrame()

    daily = (
        system_df.groupby("race_date", dropna=False)
        .agg(
            bets=("event_id", "size"),
            winners=("is_winner", "sum"),
            daily_profit=("back_profit_1pt", "sum"),
        )
        .reset_index()
    )

    daily["system_name"] = system_name
    daily["race_date"] = pd.to_datetime(daily["race_date"], errors="coerce")
    daily = daily.sort_values("race_date").reset_index(drop=True)

    daily["equity_points"] = daily["daily_profit"].cumsum()
    daily["running_peak_points"] = daily["equity_points"].cummax()
    daily["drawdown_points"] = daily["equity_points"] - daily["running_peak_points"]
    daily["strike_rate"] = daily["winners"] / daily["bets"]

    return daily[
        [
            "system_name",
            "race_date",
            "bets",
            "winners",
            "strike_rate",
            "daily_profit",
            "equity_points",
            "running_peak_points",
            "drawdown_points",
        ]
    ]


def build_monthly_equity(system_df: pd.DataFrame, system_name: str) -> pd.DataFrame:
    if system_df.empty:
        return pd.DataFrame()

    monthly = (
        system_df.groupby("year_month", dropna=False)
        .agg(
            bets=("event_id", "size"),
            winners=("is_winner", "sum"),
            monthly_profit=("back_profit_1pt", "sum"),
        )
        .reset_index()
    )

    monthly["system_name"] = system_name
    monthly = monthly.sort_values("year_month").reset_index(drop=True)

    monthly["equity_points"] = monthly["monthly_profit"].cumsum()
    monthly["running_peak_points"] = monthly["equity_points"].cummax()
    monthly["drawdown_points"] = monthly["equity_points"] - monthly["running_peak_points"]
    monthly["strike_rate"] = monthly["winners"] / monthly["bets"]
    monthly["roi"] = monthly["monthly_profit"] / monthly["bets"]

    return monthly[
        [
            "system_name",
            "year_month",
            "bets",
            "winners",
            "strike_rate",
            "monthly_profit",
            "roi",
            "equity_points",
            "running_peak_points",
            "drawdown_points",
        ]
    ]


def build_system_summary(system_df: pd.DataFrame, system_name: str) -> dict:
    if system_df.empty:
        return {
            "system_name": system_name,
            "bets": 0,
            "winners": 0,
            "strike_rate": 0,
            "total_profit": 0,
            "roi": 0,
            "max_drawdown_points": 0,
            "final_equity_points": 0,
            "date_min": None,
            "date_max": None,
        }

    return {
        "system_name": system_name,
        "bets": len(system_df),
        "races": system_df["event_id"].nunique(),
        "winners": int(system_df["is_winner"].sum()),
        "strike_rate": system_df["is_winner"].mean(),
        "avg_bsp": system_df["bsp"].mean(),
        "median_bsp": system_df["bsp"].median(),
        "total_profit": system_df["back_profit_1pt"].sum(),
        "roi": system_df["back_profit_1pt"].sum() / len(system_df),
        "max_drawdown_points": system_df["drawdown_points"].min(),
        "final_equity_points": system_df["equity_points"].iloc[-1],
        "date_min": system_df["event_dt"].min(),
        "date_max": system_df["event_dt"].max(),
    }


def main() -> None:
    print("Starting equity curve exports...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    curated_dir = Path(greyhound_paths["curated"])
    reports_dir = Path(greyhound_paths["reports"]) / "equity_curves"

    reports_dir.mkdir(parents=True, exist_ok=True)

    input_path = curated_dir / "greyhound_results_uk_analysis_base.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df["year_month"] = df["event_dt"].dt.to_period("M").astype(str)

    df = df.sort_values("event_dt").reset_index(drop=True)

    bet_level_exports = []
    daily_exports = []
    monthly_exports = []
    summary_rows = []

    for system_name, rules in SYSTEMS_TO_EXPORT.items():
        system_df = apply_system_filter(df, rules)
        system_df = add_equity_columns(system_df, system_name)

        daily = build_daily_equity(system_df, system_name)
        monthly = build_monthly_equity(system_df, system_name)

        summary_rows.append(build_system_summary(system_df, system_name))

        bet_cols = [
            "system_name",
            "bet_number",
            "event_id",
            "event_dt",
            "race_date",
            "year_month",
            "track_key",
            "race_time",
            "trap",
            "dog_clean",
            "bsp",
            "is_winner",
            "back_profit_1pt",
            "equity_points",
            "running_peak_points",
            "drawdown_points",
            "is_new_equity_high",
        ]

        available_cols = [col for col in bet_cols if col in system_df.columns]
        bet_level_exports.append(system_df[available_cols])
        daily_exports.append(daily)
        monthly_exports.append(monthly)

        print(
            f"{system_name}: "
            f"{len(system_df):,} bets | "
            f"profit {system_df['back_profit_1pt'].sum():.2f} pts | "
            f"max DD {system_df['drawdown_points'].min():.2f} pts"
        )

    bet_level = pd.concat(bet_level_exports, ignore_index=True)
    daily_all = pd.concat(daily_exports, ignore_index=True)
    monthly_all = pd.concat(monthly_exports, ignore_index=True)
    summary = pd.DataFrame(summary_rows)

    bet_level_path = reports_dir / "system_equity_bet_level.parquet"
    bet_level_sample_path = reports_dir / "system_equity_bet_level_sample.csv"
    daily_path = reports_dir / "system_equity_daily.csv"
    monthly_path = reports_dir / "system_equity_monthly.csv"
    summary_path = reports_dir / "system_equity_summary.csv"

    bet_level.to_parquet(bet_level_path, index=False)
    bet_level.head(10000).to_csv(bet_level_sample_path, index=False)
    daily_all.to_csv(daily_path, index=False)
    monthly_all.to_csv(monthly_path, index=False)
    summary.to_csv(summary_path, index=False)

    print("\nEquity curve export complete.")
    print(f"Saved bet-level equity to: {bet_level_path}")
    print(f"Saved bet-level sample to: {bet_level_sample_path}")
    print(f"Saved daily equity to: {daily_path}")
    print(f"Saved monthly equity to: {monthly_path}")
    print(f"Saved summary to: {summary_path}")

    print("\nSummary:")
    print(summary.round(4))


if __name__ == "__main__":
    main()