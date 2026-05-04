from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


INITIAL_BANKROLL = 10_000.00

STAKING_MODELS = {
    "M01_FLAT_10": {
        "type": "flat",
        "flat_stake": 10.00,
        "stake_pct": None,
        "max_stake": None,
        "stop_dd_pct": None,
    },
    "M02_FLAT_25": {
        "type": "flat",
        "flat_stake": 25.00,
        "stake_pct": None,
        "max_stake": None,
        "stop_dd_pct": None,
    },
    "M03_PCT_0_25": {
        "type": "percentage",
        "flat_stake": None,
        "stake_pct": 0.0025,
        "max_stake": None,
        "stop_dd_pct": None,
    },
    "M04_PCT_0_25_STOP_30": {
        "type": "percentage",
        "flat_stake": None,
        "stake_pct": 0.0025,
        "max_stake": None,
        "stop_dd_pct": -0.30,
    },
    "M05_PCT_1_CAP_25": {
        "type": "percentage_capped",
        "flat_stake": None,
        "stake_pct": 0.01,
        "max_stake": 25.00,
        "stop_dd_pct": None,
    },
    "M06_PCT_1_CAP_50_STOP_30": {
        "type": "percentage_capped",
        "flat_stake": None,
        "stake_pct": 0.01,
        "max_stake": 50.00,
        "stop_dd_pct": -0.30,
    },
}


PORTFOLIOS_TO_TEST = [
    "P01_BSP_8_TO_13_ONLY",
    "P02_TRACK_TRAP_CORE",
    "P03_BSP_PLUS_TRACK_TRAP",
    "P04_CONSERVATIVE_CORE",
]


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def calculate_stake(bankroll: float, model: dict) -> float:
    if model["type"] == "flat":
        return min(model["flat_stake"], bankroll)

    if model["type"] == "percentage":
        return bankroll * model["stake_pct"]

    if model["type"] == "percentage_capped":
        return min(bankroll * model["stake_pct"], model["max_stake"])

    raise ValueError(f"Unknown staking model type: {model['type']}")


def simulate_bankroll(portfolio_df: pd.DataFrame, model_name: str, model: dict) -> pd.DataFrame:
    portfolio_df = portfolio_df.sort_values("event_dt").copy().reset_index(drop=True)

    bankroll = INITIAL_BANKROLL
    peak_bankroll = INITIAL_BANKROLL
    stopped = False

    rows = []

    for i, row in portfolio_df.iterrows():
        if stopped or bankroll <= 0:
            break

        stake = calculate_stake(bankroll, model)

        if stake <= 0:
            break

        profit_multiplier = row["portfolio_profit_1pt"]
        profit_cash = stake * profit_multiplier

        bankroll_before = bankroll
        bankroll_after = bankroll_before + profit_cash

        if bankroll_after < 0:
            bankroll_after = 0

        peak_bankroll = max(peak_bankroll, bankroll_after)

        drawdown_cash = bankroll_after - peak_bankroll
        drawdown_pct = drawdown_cash / peak_bankroll if peak_bankroll else 0

        stop_triggered = False

        if model["stop_dd_pct"] is not None and drawdown_pct <= model["stop_dd_pct"]:
            stop_triggered = True
            stopped = True

        rows.append(
            {
                "portfolio_name": row["portfolio_name"],
                "model_name": model_name,
                "bet_number": i + 1,
                "event_id": row["event_id"],
                "event_dt": row["event_dt"],
                "race_date": row["race_date"],
                "year_month": row["year_month"],
                "system_name": row["system_name"],
                "track_key": row["track_key"],
                "trap": row["trap"],
                "dog_clean": row["dog_clean"],
                "bsp": row["bsp"],
                "is_winner": row["is_winner"],
                "stake": stake,
                "profit_multiplier": profit_multiplier,
                "profit_cash": profit_cash,
                "bankroll_before": bankroll_before,
                "bankroll_after": bankroll_after,
                "peak_bankroll": peak_bankroll,
                "drawdown_cash": drawdown_cash,
                "drawdown_pct": drawdown_pct,
                "stop_triggered": stop_triggered,
            }
        )

        bankroll = bankroll_after

    return pd.DataFrame(rows)


def build_summary(sim_df: pd.DataFrame) -> dict:
    if sim_df.empty:
        return {}

    initial = INITIAL_BANKROLL
    final = sim_df["bankroll_after"].iloc[-1]
    profit = final - initial
    total_staked = sim_df["stake"].sum()

    return {
        "portfolio_name": sim_df["portfolio_name"].iloc[0],
        "model_name": sim_df["model_name"].iloc[0],
        "initial_bankroll": initial,
        "final_bankroll": final,
        "profit_cash": profit,
        "return_pct": profit / initial,
        "bets_taken": len(sim_df),
        "winners": int(sim_df["is_winner"].sum()),
        "strike_rate": sim_df["is_winner"].mean(),
        "total_staked": total_staked,
        "profit_on_turnover": profit / total_staked if total_staked else 0,
        "max_drawdown_cash": sim_df["drawdown_cash"].min(),
        "max_drawdown_pct": sim_df["drawdown_pct"].min(),
        "largest_stake": sim_df["stake"].max(),
        "smallest_stake": sim_df["stake"].min(),
        "stopped": bool(sim_df["stop_triggered"].any()),
        "date_min": sim_df["event_dt"].min(),
        "date_max": sim_df["event_dt"].max(),
    }


def build_daily(sim_df: pd.DataFrame) -> pd.DataFrame:
    daily = (
        sim_df.groupby(["portfolio_name", "model_name", "race_date"], dropna=False)
        .agg(
            bets=("event_id", "size"),
            winners=("is_winner", "sum"),
            daily_profit_cash=("profit_cash", "sum"),
            bankroll_after=("bankroll_after", "last"),
            peak_bankroll=("peak_bankroll", "max"),
            drawdown_cash=("drawdown_cash", "min"),
            drawdown_pct=("drawdown_pct", "min"),
        )
        .reset_index()
    )

    daily["strike_rate"] = daily["winners"] / daily["bets"]

    return daily


def build_monthly(sim_df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        sim_df.groupby(["portfolio_name", "model_name", "year_month"], dropna=False)
        .agg(
            bets=("event_id", "size"),
            winners=("is_winner", "sum"),
            monthly_profit_cash=("profit_cash", "sum"),
            bankroll_after=("bankroll_after", "last"),
            peak_bankroll=("peak_bankroll", "max"),
            drawdown_cash=("drawdown_cash", "min"),
            drawdown_pct=("drawdown_pct", "min"),
        )
        .reset_index()
    )

    monthly["strike_rate"] = monthly["winners"] / monthly["bets"]

    return monthly


def main() -> None:
    print("Starting risk-controlled capital engine...")
    print(f"Initial bankroll: £{INITIAL_BANKROLL:,.2f}")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    reports_dir = Path(greyhound_paths["reports"])
    portfolio_dir = reports_dir / "portfolios"
    output_dir = reports_dir / "capital_engine_risk_control"

    output_dir.mkdir(parents=True, exist_ok=True)

    input_path = portfolio_dir / "portfolio_bet_log.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df["year_month"] = df["event_dt"].dt.to_period("M").astype(str)

    simulations = []
    summary_rows = []
    daily_rows = []
    monthly_rows = []

    for portfolio_name in PORTFOLIOS_TO_TEST:
        portfolio_df = df[df["portfolio_name"] == portfolio_name].copy()

        if portfolio_df.empty:
            print(f"No data found for {portfolio_name}")
            continue

        for model_name, model in STAKING_MODELS.items():
            sim_df = simulate_bankroll(portfolio_df, model_name, model)

            if sim_df.empty:
                continue

            simulations.append(sim_df)
            summary_rows.append(build_summary(sim_df))
            daily_rows.append(build_daily(sim_df))
            monthly_rows.append(build_monthly(sim_df))

            final_bankroll = sim_df["bankroll_after"].iloc[-1]
            max_dd_pct = sim_df["drawdown_pct"].min()
            stopped = sim_df["stop_triggered"].any()

            print(
                f"{portfolio_name} | {model_name}: "
                f"final £{final_bankroll:,.2f} | "
                f"return {(final_bankroll / INITIAL_BANKROLL - 1):.2%} | "
                f"max DD {max_dd_pct:.2%} | "
                f"bets {len(sim_df):,} | "
                f"stopped {stopped}"
            )

    all_simulations = pd.concat(simulations, ignore_index=True)
    summary = pd.DataFrame(summary_rows)
    daily = pd.concat(daily_rows, ignore_index=True)
    monthly = pd.concat(monthly_rows, ignore_index=True)

    summary = summary.sort_values(
        ["return_pct", "max_drawdown_pct"],
        ascending=[False, False],
    )

    all_simulations.to_parquet(output_dir / "risk_control_bet_log.parquet", index=False)
    all_simulations.head(10000).to_csv(output_dir / "risk_control_bet_log_sample.csv", index=False)
    summary.to_csv(output_dir / "risk_control_summary.csv", index=False)
    daily.to_csv(output_dir / "risk_control_daily.csv", index=False)
    monthly.to_csv(output_dir / "risk_control_monthly.csv", index=False)

    print("\nRisk-controlled capital engine complete.")
    print(f"Saved outputs to: {output_dir}")

    print("\nTop results by return:")
    print(summary.head(20).round(4))

    print("\nBest controlled results with max drawdown better than -30%:")
    controlled = summary[summary["max_drawdown_pct"] > -0.30].copy()
    print(controlled.head(20).round(4))


if __name__ == "__main__":
    main()