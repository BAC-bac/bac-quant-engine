from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


INITIAL_BANKROLL = 10_000.00
STAKE_PERCENT = 0.01  # 1% of current bankroll per bet


PORTFOLIOS_TO_TEST = [
    "P01_BSP_8_TO_13_ONLY",
    "P02_TRACK_TRAP_CORE",
    "P03_BSP_PLUS_TRACK_TRAP",
    "P04_CONSERVATIVE_CORE",
]


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def simulate_bankroll(portfolio_df: pd.DataFrame) -> pd.DataFrame:
    portfolio_df = portfolio_df.sort_values("event_dt").copy().reset_index(drop=True)

    bankroll = INITIAL_BANKROLL
    peak_bankroll = INITIAL_BANKROLL

    rows = []

    for i, row in portfolio_df.iterrows():
        stake = bankroll * STAKE_PERCENT

        profit_multiplier = row["portfolio_profit_1pt"]
        profit_cash = stake * profit_multiplier

        bankroll_after = bankroll + profit_cash

        peak_bankroll = max(peak_bankroll, bankroll_after)
        drawdown_cash = bankroll_after - peak_bankroll
        drawdown_pct = drawdown_cash / peak_bankroll if peak_bankroll else 0

        rows.append(
            {
                "portfolio_name": row["portfolio_name"],
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
                "bankroll_before": bankroll,
                "bankroll_after": bankroll_after,
                "peak_bankroll": peak_bankroll,
                "drawdown_cash": drawdown_cash,
                "drawdown_pct": drawdown_pct,
            }
        )

        bankroll = bankroll_after

        if bankroll <= 0:
            break

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
        "initial_bankroll": initial,
        "final_bankroll": final,
        "profit_cash": profit,
        "return_pct": profit / initial,
        "bets": len(sim_df),
        "winners": int(sim_df["is_winner"].sum()),
        "strike_rate": sim_df["is_winner"].mean(),
        "total_staked": total_staked,
        "profit_on_turnover": profit / total_staked if total_staked else 0,
        "max_drawdown_cash": sim_df["drawdown_cash"].min(),
        "max_drawdown_pct": sim_df["drawdown_pct"].min(),
        "largest_stake": sim_df["stake"].max(),
        "smallest_stake": sim_df["stake"].min(),
        "date_min": sim_df["event_dt"].min(),
        "date_max": sim_df["event_dt"].max(),
    }


def build_daily_equity(sim_df: pd.DataFrame) -> pd.DataFrame:
    daily = (
        sim_df.groupby(["portfolio_name", "race_date"], dropna=False)
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


def build_monthly_equity(sim_df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        sim_df.groupby(["portfolio_name", "year_month"], dropna=False)
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
    print("Starting capital engine Option B...")
    print(f"Initial bankroll: £{INITIAL_BANKROLL:,.2f}")
    print(f"Stake model: {STAKE_PERCENT:.2%} of current bankroll per bet")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    reports_dir = Path(greyhound_paths["reports"])
    portfolio_dir = reports_dir / "portfolios"
    capital_dir = reports_dir / "capital_engine_option_b"

    capital_dir.mkdir(parents=True, exist_ok=True)

    input_path = portfolio_dir / "portfolio_bet_log.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df["year_month"] = df["event_dt"].dt.to_period("M").astype(str)

    all_sims = []
    summary_rows = []
    daily_rows = []
    monthly_rows = []

    for portfolio_name in PORTFOLIOS_TO_TEST:
        portfolio_df = df[df["portfolio_name"] == portfolio_name].copy()

        if portfolio_df.empty:
            print(f"No data found for {portfolio_name}")
            continue

        sim_df = simulate_bankroll(portfolio_df)

        all_sims.append(sim_df)
        summary_rows.append(build_summary(sim_df))
        daily_rows.append(build_daily_equity(sim_df))
        monthly_rows.append(build_monthly_equity(sim_df))

        final_bankroll = sim_df["bankroll_after"].iloc[-1]
        max_dd_pct = sim_df["drawdown_pct"].min()

        print(
            f"{portfolio_name}: "
            f"final £{final_bankroll:,.2f} | "
            f"return {(final_bankroll / INITIAL_BANKROLL - 1):.2%} | "
            f"max DD {max_dd_pct:.2%} | "
            f"bets {len(sim_df):,}"
        )

    all_simulations = pd.concat(all_sims, ignore_index=True)
    summary = pd.DataFrame(summary_rows)
    daily = pd.concat(daily_rows, ignore_index=True)
    monthly = pd.concat(monthly_rows, ignore_index=True)

    summary = summary.sort_values("return_pct", ascending=False)

    all_simulations.to_parquet(capital_dir / "capital_simulation_bet_log.parquet", index=False)
    all_simulations.head(10000).to_csv(capital_dir / "capital_simulation_bet_log_sample.csv", index=False)
    summary.to_csv(capital_dir / "capital_summary.csv", index=False)
    daily.to_csv(capital_dir / "capital_daily.csv", index=False)
    monthly.to_csv(capital_dir / "capital_monthly.csv", index=False)

    print("\nCapital engine Option B complete.")
    print(f"Saved outputs to: {capital_dir}")

    print("\nCapital summary:")
    print(summary.round(4))


if __name__ == "__main__":
    main()