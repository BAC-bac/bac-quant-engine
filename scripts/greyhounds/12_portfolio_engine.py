from pathlib import Path
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.yaml"


def load_paths() -> dict:
    with open(PATHS_CONFIG, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def calculate_max_drawdown(equity: pd.Series) -> float:
    peak = equity.cummax()
    drawdown = equity - peak
    return float(drawdown.min())


def build_portfolio_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for portfolio_name, group in df.groupby("portfolio_name"):
        group = group.sort_values("event_dt").copy()

        bets = len(group)
        total_profit = group["portfolio_profit_1pt"].sum()
        equity = group["portfolio_profit_1pt"].cumsum()

        rows.append(
            {
                "portfolio_name": portfolio_name,
                "bets": bets,
                "races": group["event_id"].nunique(),
                "systems": group["system_name"].nunique(),
                "winners": int(group["is_winner"].sum()),
                "strike_rate": group["is_winner"].mean(),
                "avg_bsp": group["bsp"].mean(),
                "median_bsp": group["bsp"].median(),
                "total_profit": total_profit,
                "roi": total_profit / bets if bets else 0,
                "max_drawdown_points": calculate_max_drawdown(equity),
                "final_equity_points": equity.iloc[-1] if not equity.empty else 0,
                "date_min": group["event_dt"].min(),
                "date_max": group["event_dt"].max(),
            }
        )

    return pd.DataFrame(rows).sort_values("roi", ascending=False)


def build_daily_portfolio(df: pd.DataFrame) -> pd.DataFrame:
    daily = (
        df.groupby(["portfolio_name", "race_date"], dropna=False)
        .agg(
            bets=("event_id", "size"),
            winners=("is_winner", "sum"),
            daily_profit=("portfolio_profit_1pt", "sum"),
        )
        .reset_index()
    )

    daily["race_date"] = pd.to_datetime(daily["race_date"], errors="coerce")
    daily = daily.sort_values(["portfolio_name", "race_date"])

    daily["equity_points"] = daily.groupby("portfolio_name")["daily_profit"].cumsum()
    daily["running_peak_points"] = daily.groupby("portfolio_name")["equity_points"].cummax()
    daily["drawdown_points"] = daily["equity_points"] - daily["running_peak_points"]
    daily["strike_rate"] = daily["winners"] / daily["bets"]

    return daily


def build_monthly_portfolio(df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        df.groupby(["portfolio_name", "year_month"], dropna=False)
        .agg(
            bets=("event_id", "size"),
            winners=("is_winner", "sum"),
            monthly_profit=("portfolio_profit_1pt", "sum"),
        )
        .reset_index()
    )

    monthly = monthly.sort_values(["portfolio_name", "year_month"])

    monthly["equity_points"] = monthly.groupby("portfolio_name")["monthly_profit"].cumsum()
    monthly["running_peak_points"] = monthly.groupby("portfolio_name")["equity_points"].cummax()
    monthly["drawdown_points"] = monthly["equity_points"] - monthly["running_peak_points"]
    monthly["strike_rate"] = monthly["winners"] / monthly["bets"]
    monthly["roi"] = monthly["monthly_profit"] / monthly["bets"]

    return monthly


def assign_portfolios(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    portfolios = []

    # Portfolio 1: broad structural edge only
    p1 = df[df["system_name"] == "BSP_8_TO_13"].copy()
    p1["portfolio_name"] = "P01_BSP_8_TO_13_ONLY"
    p1["portfolio_profit_1pt"] = p1["back_profit_1pt"]
    portfolios.append(p1)

    # Portfolio 2: strongest track/trap systems only
    p2 = df[
        df["system_name"].isin(
            [
                "ROMFORD_TRAP_3",
                "HARLOW_TRAP_1",
                "HENLOW_TRAP_2",
                "TOWCESTER_TRAP_3",
            ]
        )
    ].copy()
    p2["portfolio_name"] = "P02_TRACK_TRAP_CORE"
    p2["portfolio_profit_1pt"] = p2["back_profit_1pt"]
    portfolios.append(p2)

    # Portfolio 3: broad edge + track/trap core
    p3 = df[
        df["system_name"].isin(
            [
                "BSP_8_TO_13",
                "ROMFORD_TRAP_3",
                "HARLOW_TRAP_1",
                "HENLOW_TRAP_2",
                "TOWCESTER_TRAP_3",
            ]
        )
    ].copy()
    p3["portfolio_name"] = "P03_BSP_PLUS_TRACK_TRAP"
    p3["portfolio_profit_1pt"] = p3["back_profit_1pt"]
    portfolios.append(p3)

    # Portfolio 4: conservative core - remove smaller/fragile Henlow
    p4 = df[
        df["system_name"].isin(
            [
                "BSP_8_TO_13",
                "ROMFORD_TRAP_3",
                "HARLOW_TRAP_1",
            ]
        )
    ].copy()
    p4["portfolio_name"] = "P04_CONSERVATIVE_CORE"
    p4["portfolio_profit_1pt"] = p4["back_profit_1pt"]
    portfolios.append(p4)

    return pd.concat(portfolios, ignore_index=True)


def main() -> None:
    print("Starting greyhound portfolio engine...")

    paths = load_paths()
    greyhound_paths = paths["greyhounds"]

    reports_dir = Path(greyhound_paths["reports"])
    equity_dir = reports_dir / "equity_curves"
    portfolio_dir = reports_dir / "portfolios"

    portfolio_dir.mkdir(parents=True, exist_ok=True)

    input_path = equity_dir / "system_equity_bet_level.parquet"

    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")

    df = pd.read_parquet(input_path)

    df["event_dt"] = pd.to_datetime(df["event_dt"], errors="coerce")
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df["year_month"] = df["event_dt"].dt.to_period("M").astype(str)

    df = df.sort_values("event_dt").reset_index(drop=True)

    print(f"Rows loaded: {len(df):,}")
    print(f"Systems loaded: {df['system_name'].nunique()}")

    portfolio_bets = assign_portfolios(df)
    portfolio_bets = portfolio_bets.sort_values(["portfolio_name", "event_dt"]).reset_index(drop=True)

    portfolio_bets["portfolio_bet_number"] = (
        portfolio_bets.groupby("portfolio_name").cumcount() + 1
    )

    portfolio_bets["portfolio_equity_points"] = (
        portfolio_bets.groupby("portfolio_name")["portfolio_profit_1pt"].cumsum()
    )

    portfolio_bets["portfolio_running_peak_points"] = (
        portfolio_bets.groupby("portfolio_name")["portfolio_equity_points"].cummax()
    )

    portfolio_bets["portfolio_drawdown_points"] = (
        portfolio_bets["portfolio_equity_points"]
        - portfolio_bets["portfolio_running_peak_points"]
    )

    summary = build_portfolio_summary(portfolio_bets)
    daily = build_daily_portfolio(portfolio_bets)
    monthly = build_monthly_portfolio(portfolio_bets)

    bet_log_path = portfolio_dir / "portfolio_bet_log.parquet"
    bet_log_sample_path = portfolio_dir / "portfolio_bet_log_sample.csv"
    summary_path = portfolio_dir / "portfolio_summary.csv"
    daily_path = portfolio_dir / "portfolio_daily.csv"
    monthly_path = portfolio_dir / "portfolio_monthly.csv"

    portfolio_bets.to_parquet(bet_log_path, index=False)
    portfolio_bets.head(10000).to_csv(bet_log_sample_path, index=False)
    summary.to_csv(summary_path, index=False)
    daily.to_csv(daily_path, index=False)
    monthly.to_csv(monthly_path, index=False)

    print("\nPortfolio engine complete.")
    print(f"Saved bet log to: {bet_log_path}")
    print(f"Saved bet log sample to: {bet_log_sample_path}")
    print(f"Saved summary to: {summary_path}")
    print(f"Saved daily portfolio equity to: {daily_path}")
    print(f"Saved monthly portfolio equity to: {monthly_path}")

    print("\nPortfolio summary:")
    print(summary.round(4))


if __name__ == "__main__":
    main()