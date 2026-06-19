"""
BACQE DUKASCOPY 59 - OOS PASS TRADE PROFILE

Purpose:
    Deep profile the single Script 58 OOS-pass context:

        EURUSD
        mid_return_1 -> future_return_1000
        long
        session=asia
        day_of_week=Monday
        spread_regime=low_spread

Goal:
    Understand whether the validated edge is smooth, concentrated,
    drawdown-heavy, or dependent on a few abnormal periods.
"""

from pathlib import Path
import numpy as np
import pandas as pd


SYMBOL = "EURUSD"
FEATURE = "mid_return_1"
TARGET = "future_return_1000"
SIDE = "long"

CONTEXT_LABEL = "session=asia | day_of_week=Monday | spread_regime=low_spread"
COST_SCENARIO = "half_spread_plus_low_commission"

QUANT_LAB = Path(r"E:\Quant_Lab")

LEDGER_FILE = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_horizon_candidate_replay"
    / "trade_ledgers"
    / "candidate_replay_ledger_latest.parquet"
)

OUTPUT_ROOT = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_oos_pass_trade_profile"
    / f"symbol={SYMBOL}"
)

SPREAD_FRACTION = 0.5
COMMISSION_RETURN = 0.000005


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "tables",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def parse_context_label(label: str) -> dict:
    filters = {}

    for part in str(label).split("|"):
        part = part.strip()

        if "=" not in part:
            continue

        key, value = part.split("=", 1)
        filters[key.strip()] = value.strip()

    return filters


def evaluate_returns(returns: pd.Series) -> dict:
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    if returns.empty:
        return {
            "trade_count": 0,
            "win_rate": np.nan,
            "mean_return": np.nan,
            "median_return": np.nan,
            "total_return": np.nan,
            "profit_factor": np.nan,
            "max_drawdown_return": np.nan,
        }

    wins = returns[returns > 0]
    losses = returns[returns < 0]

    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())

    profit_factor = gross_profit / gross_loss if gross_loss != 0 else np.nan

    equity = returns.cumsum()
    drawdown = equity - equity.cummax()

    return {
        "trade_count": len(returns),
        "win_rate": (returns > 0).mean(),
        "mean_return": returns.mean(),
        "median_return": returns.median(),
        "total_return": returns.sum(),
        "profit_factor": profit_factor,
        "max_drawdown_return": drawdown.min(),
    }


def load_and_filter_ledger() -> pd.DataFrame:
    if not LEDGER_FILE.exists():
        raise FileNotFoundError(f"Missing ledger file: {LEDGER_FILE}")

    ledger = pd.read_parquet(LEDGER_FILE)

    df = ledger[
        (ledger["feature"].astype(str) == FEATURE)
        & (ledger["target"].astype(str) == TARGET)
        & (ledger["side"].astype(str) == SIDE)
    ].copy()

    filters = parse_context_label(CONTEXT_LABEL)

    for column, value in filters.items():
        if column not in df.columns:
            raise ValueError(f"Missing context column: {column}")

        df = df[df[column].astype(str) == str(value)].copy()

    if df.empty:
        return df

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["month"] = df["timestamp_utc"].dt.to_period("M").astype(str)
    df["date"] = df["timestamp_utc"].dt.date.astype(str)

    for col in [
        "signal_return",
        "target_return",
        "spread",
        "feature_value",
        "bid",
        "ask",
        "mid",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.replace([np.inf, -np.inf], np.nan)

    df = df.dropna(
        subset=[
            "timestamp_utc",
            "year",
            "month",
            "signal_return",
            "spread",
            "feature_value",
        ]
    )

    df["gross_signal_return"] = df["signal_return"]

    df["cost_return"] = (
        df["spread"] * SPREAD_FRACTION
        + COMMISSION_RETURN
    )

    df["net_signal_return"] = (
        df["gross_signal_return"]
        - df["cost_return"]
    )

    df["equity_curve"] = df["net_signal_return"].cumsum()
    df["running_peak"] = df["equity_curve"].cummax()
    df["drawdown_return"] = df["equity_curve"] - df["running_peak"]

    return df


def build_period_profile(
    df: pd.DataFrame,
    group_col: str,
) -> pd.DataFrame:
    rows = []

    for key, group in df.groupby(group_col):
        gross_stats = evaluate_returns(group["gross_signal_return"])
        net_stats = evaluate_returns(group["net_signal_return"])

        rows.append({
            group_col: key,
            "trade_count": len(group),
            "avg_spread": group["spread"].mean(),
            "avg_cost_return": group["cost_return"].mean(),
            "avg_feature_value": group["feature_value"].mean(),
            "avg_abs_feature_value": group["feature_value"].abs().mean(),
            "gross_win_rate": gross_stats["win_rate"],
            "gross_total_return": gross_stats["total_return"],
            "gross_profit_factor": gross_stats["profit_factor"],
            "net_win_rate": net_stats["win_rate"],
            "net_mean_return": net_stats["mean_return"],
            "net_total_return": net_stats["total_return"],
            "net_profit_factor": net_stats["profit_factor"],
            "net_max_drawdown_return": net_stats["max_drawdown_return"],
        })

    return pd.DataFrame(rows)


def build_hour_profile(df: pd.DataFrame) -> pd.DataFrame:
    return build_period_profile(df, "hour").sort_values("hour")


def build_drawdown_profile(df: pd.DataFrame) -> pd.DataFrame:
    drawdown = df[
        [
            "timestamp_utc",
            "year",
            "month",
            "date",
            "net_signal_return",
            "equity_curve",
            "running_peak",
            "drawdown_return",
            "spread",
            "feature_value",
        ]
    ].copy()

    drawdown = drawdown.sort_values("drawdown_return")

    return drawdown.head(1000)


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 59 - OOS PASS TRADE PROFILE")
    print("=" * 90)
    print(f"Symbol:        {SYMBOL}")
    print(f"Feature:       {FEATURE}")
    print(f"Target:        {TARGET}")
    print(f"Side:          {SIDE}")
    print(f"Context:       {CONTEXT_LABEL}")
    print(f"Cost scenario: {COST_SCENARIO}")
    print(f"Ledger:        {LEDGER_FILE}")
    print(f"Output root:   {OUTPUT_ROOT}")
    print("-" * 90)

    ensure_dirs()

    df = load_and_filter_ledger()

    print(f"Filtered trades: {len(df):,}")

    if df.empty:
        print("[STOP] No matching trades found.")
        return

    gross_stats = evaluate_returns(df["gross_signal_return"])
    net_stats = evaluate_returns(df["net_signal_return"])

    monthly = build_period_profile(df, "month").sort_values("month")
    yearly = build_period_profile(df, "year").sort_values("year")
    hourly = build_hour_profile(df)
    drawdown = build_drawdown_profile(df)

    trade_profile_path = OUTPUT_ROOT / "tables" / "trade_profile_latest.csv"
    monthly_path = OUTPUT_ROOT / "tables" / "monthly_profile_latest.csv"
    yearly_path = OUTPUT_ROOT / "tables" / "yearly_profile_latest.csv"
    hourly_path = OUTPUT_ROOT / "tables" / "hourly_profile_latest.csv"
    drawdown_path = OUTPUT_ROOT / "tables" / "drawdown_profile_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "oos_pass_trade_profile_report_latest.txt"

    df.to_csv(trade_profile_path, index=False)
    monthly.to_csv(monthly_path, index=False)
    yearly.to_csv(yearly_path, index=False)
    hourly.to_csv(hourly_path, index=False)
    drawdown.to_csv(drawdown_path, index=False)

    worst_months = monthly.sort_values("net_total_return").head(10)
    best_months = monthly.sort_values("net_total_return", ascending=False).head(10)

    positive_month_rate = (monthly["net_total_return"] > 0).mean()
    positive_year_rate = (yearly["net_total_return"] > 0).mean()

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY OOS PASS TRADE PROFILE REPORT\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Symbol: {SYMBOL}\n")
        f.write(f"Feature: {FEATURE}\n")
        f.write(f"Target: {TARGET}\n")
        f.write(f"Side: {SIDE}\n")
        f.write(f"Context: {CONTEXT_LABEL}\n")
        f.write(f"Cost scenario: {COST_SCENARIO}\n\n")

        f.write("OVERALL PERFORMANCE\n")
        f.write("-" * 80 + "\n")
        f.write(f"Trade count: {len(df):,}\n")
        f.write(f"Average spread: {df['spread'].mean():.8f}\n")
        f.write(f"Average cost return: {df['cost_return'].mean():.8f}\n")
        f.write(f"Gross win rate: {gross_stats['win_rate']:.4f}\n")
        f.write(f"Gross total return: {gross_stats['total_return']:.6f}\n")
        f.write(f"Gross profit factor: {gross_stats['profit_factor']:.6f}\n")
        f.write(f"Net win rate: {net_stats['win_rate']:.4f}\n")
        f.write(f"Net total return: {net_stats['total_return']:.6f}\n")
        f.write(f"Net profit factor: {net_stats['profit_factor']:.6f}\n")
        f.write(f"Net max drawdown return: {net_stats['max_drawdown_return']:.6f}\n")
        f.write(f"Positive month rate: {positive_month_rate:.4f}\n")
        f.write(f"Positive year rate: {positive_year_rate:.4f}\n\n")

        f.write("YEARLY PROFILE\n")
        f.write("-" * 80 + "\n")
        f.write(yearly.to_string(index=False))
        f.write("\n\n")

        f.write("WORST MONTHS\n")
        f.write("-" * 80 + "\n")
        f.write(worst_months.to_string(index=False))
        f.write("\n\n")

        f.write("BEST MONTHS\n")
        f.write("-" * 80 + "\n")
        f.write(best_months.to_string(index=False))
        f.write("\n\n")

        f.write("HOURLY PROFILE\n")
        f.write("-" * 80 + "\n")
        f.write(hourly.to_string(index=False))
        f.write("\n\n")

        f.write("Outputs\n")
        f.write("-" * 80 + "\n")
        f.write(f"Trade profile: {trade_profile_path}\n")
        f.write(f"Monthly: {monthly_path}\n")
        f.write(f"Yearly: {yearly_path}\n")
        f.write(f"Hourly: {hourly_path}\n")
        f.write(f"Drawdown: {drawdown_path}\n")

    print("=" * 90)
    print("[DONE] OOS pass trade profile complete.")
    print(f"Trade profile: {trade_profile_path}")
    print(f"Monthly:       {monthly_path}")
    print(f"Yearly:        {yearly_path}")
    print(f"Hourly:        {hourly_path}")
    print(f"Drawdown:      {drawdown_path}")
    print(f"Report:        {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()