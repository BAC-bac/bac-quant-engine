"""
BACQE DUKASCOPY 35 - HORIZON CONTEXT REPLAY

Purpose:
    Replay the best context-surviving horizon candidates from Script 34.
"""

from pathlib import Path
import numpy as np
import pandas as pd


SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

LEDGER_PATH = (
    QUANT_LAB / "data" / "analysis" / "dukascopy_horizon_candidate_replay"
    / "trade_ledgers" / "candidate_replay_ledger_latest.parquet"
)

CONTEXT_PATH = (
    QUANT_LAB / "data" / "analysis" / "dukascopy_horizon_context_optimizer"
    / "top_contexts" / "top_horizon_contexts_latest.csv"
)

OUTPUT_ROOT = QUANT_LAB / "data" / "analysis" / "dukascopy_horizon_context_replay"

TOP_N_CONTEXTS = 20

PIP_SIZE = 0.0001
SPREAD_MULTIPLIER = 0.5
COMMISSION_PIPS = 0.05

MIN_TRADES = 50_000


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "trade_ledgers",
        OUTPUT_ROOT / "equity_curves",
        OUTPUT_ROOT / "summaries",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


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


def add_costs(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["spread_cost_return"] = df["spread"] * SPREAD_MULTIPLIER
    df["commission_cost_return"] = COMMISSION_PIPS * PIP_SIZE
    df["total_cost_return"] = df["spread_cost_return"] + df["commission_cost_return"]
    df["net_signal_return"] = df["signal_return"] - df["total_cost_return"]

    return df


def parse_context_value(context_value: str) -> dict:
    """
    Example:
        session=asia | day_of_week=Monday
    """
    parts = str(context_value).split("|")
    result = {}

    for part in parts:
        part = part.strip()

        if "=" not in part:
            continue

        key, value = part.split("=", 1)
        result[key.strip()] = value.strip()

    return result


def apply_context_filter(df: pd.DataFrame, context_value: str) -> pd.DataFrame:
    filters = parse_context_value(context_value)
    temp = df.copy()

    for col, value in filters.items():
        if col not in temp.columns:
            return pd.DataFrame()

        temp = temp[temp[col].astype(str) == str(value)]

    return temp


def build_equity_curve(df: pd.DataFrame, replay_id: str) -> pd.DataFrame:
    temp = df.sort_values("timestamp_utc").copy()
    temp["equity"] = temp["net_signal_return"].cumsum()
    temp["running_max"] = temp["equity"].cummax()
    temp["drawdown"] = temp["equity"] - temp["running_max"]
    temp["replay_id"] = replay_id

    return temp[
        [
            "replay_id",
            "timestamp_utc",
            "equity",
            "running_max",
            "drawdown",
            "net_signal_return",
        ]
    ]


def main() -> None:
    banner("BACQE DUKASCOPY 35 - HORIZON CONTEXT REPLAY")

    ensure_dirs()

    print(f"Symbol:       {SYMBOL}")
    print(f"Ledger:       {LEDGER_PATH}")
    print(f"Contexts:     {CONTEXT_PATH}")
    print(f"Output root:  {OUTPUT_ROOT}")
    print("-" * 90)

    if not LEDGER_PATH.exists():
        print("[STOP] Missing horizon replay ledger.")
        return

    if not CONTEXT_PATH.exists():
        print("[STOP] Missing Script 34 context file.")
        return

    ledger = pd.read_parquet(LEDGER_PATH)
    contexts = pd.read_csv(CONTEXT_PATH)

    print(f"Loaded ledger rows:   {len(ledger):,}")
    print(f"Loaded context rows:  {len(contexts):,}")

    contexts = contexts[
        contexts["context_label"].isin(
            ["context_survivor", "strong_context_survivor", "fragile_context_survivor"]
        )
    ].copy()

    contexts = contexts.head(TOP_N_CONTEXTS)

    print(f"Contexts selected:    {len(contexts)}")
    print("-" * 90)

    ledger = ledger.replace([np.inf, -np.inf], np.nan)
    ledger = ledger.dropna(subset=["spread", "signal_return"])
    ledger = add_costs(ledger)

    replay_ledgers = []
    equity_curves = []
    summary_rows = []
    monthly_rows = []
    yearly_rows = []

    for idx, row in contexts.iterrows():
        feature = row["feature"]
        target = row["target"]
        side = row["side"]
        context_type = row["context_type"]
        context_value = row["context_value"]

        replay_id = f"{feature}__{target}__{side}__{context_type}__{context_value}"
        safe_replay_id = (
            replay_id
            .replace(" ", "_")
            .replace("|", "")
            .replace("=", "-")
            .replace(":", "")
            .replace("/", "_")
        )[:180]

        print(f"[REPLAY] {safe_replay_id}")

        temp = ledger[
            (ledger["feature"] == feature)
            & (ledger["target"] == target)
            & (ledger["side"] == side)
        ].copy()

        temp = apply_context_filter(temp, context_value)

        if len(temp) < MIN_TRADES:
            print(f"    skipped: trades={len(temp):,}")
            continue

        temp["replay_id"] = replay_id
        temp["context_type"] = context_type
        temp["context_value"] = context_value

        stats = evaluate_returns(temp["net_signal_return"])

        summary_rows.append({
            "replay_id": replay_id,
            "feature": feature,
            "target": target,
            "side": side,
            "context_type": context_type,
            "context_value": context_value,
            **stats,
            "positive_day_rate": (
                temp.groupby("dataset")["net_signal_return"].sum() > 0
            ).mean(),
            "positive_month_rate": (
                temp.groupby("month")["net_signal_return"].sum() > 0
            ).mean(),
            "positive_year_rate": (
                temp.groupby("year")["net_signal_return"].sum() > 0
            ).mean(),
        })

        monthly = (
            temp.groupby("month", as_index=False)
            .agg(
                net_total_return=("net_signal_return", "sum"),
                trades=("net_signal_return", "count"),
                win_rate=("net_signal_return", lambda x: (x > 0).mean()),
            )
        )
        monthly["replay_id"] = replay_id
        monthly_rows.append(monthly)

        yearly = (
            temp.groupby("year", as_index=False)
            .agg(
                net_total_return=("net_signal_return", "sum"),
                trades=("net_signal_return", "count"),
                win_rate=("net_signal_return", lambda x: (x > 0).mean()),
            )
        )
        yearly["replay_id"] = replay_id
        yearly_rows.append(yearly)

        equity_curves.append(build_equity_curve(temp, replay_id))

        keep_cols = [
            "replay_id",
            "timestamp_utc",
            "dataset",
            "year",
            "month",
            "day_of_week",
            "hour",
            "session",
            "spread_regime",
            "volatility_regime",
            "feature",
            "target",
            "side",
            "context_type",
            "context_value",
            "spread",
            "signal_return",
            "total_cost_return",
            "net_signal_return",
        ]

        replay_ledgers.append(temp[keep_cols])

    if not summary_rows:
        print("[STOP] No context replay results generated.")
        return

    summary = pd.DataFrame(summary_rows).sort_values("profit_factor", ascending=False)

    replay_ledger = pd.concat(replay_ledgers, ignore_index=True)
    equity = pd.concat(equity_curves, ignore_index=True)
    monthly_df = pd.concat(monthly_rows, ignore_index=True)
    yearly_df = pd.concat(yearly_rows, ignore_index=True)

    summary_path = OUTPUT_ROOT / "summaries" / "horizon_context_replay_summary_latest.csv"
    ledger_path = OUTPUT_ROOT / "trade_ledgers" / "horizon_context_replay_ledger_latest.parquet"
    ledger_sample_path = OUTPUT_ROOT / "trade_ledgers" / "horizon_context_replay_ledger_sample_latest.csv"
    equity_path = OUTPUT_ROOT / "equity_curves" / "horizon_context_equity_curves_latest.csv"
    monthly_path = OUTPUT_ROOT / "summaries" / "horizon_context_monthly_returns_latest.csv"
    yearly_path = OUTPUT_ROOT / "summaries" / "horizon_context_yearly_returns_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "horizon_context_replay_report_latest.txt"

    summary.to_csv(summary_path, index=False)
    replay_ledger.to_parquet(ledger_path, index=False)
    replay_ledger.head(250_000).to_csv(ledger_sample_path, index=False)
    equity.to_csv(equity_path, index=False)
    monthly_df.to_csv(monthly_path, index=False)
    yearly_df.to_csv(yearly_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY HORIZON CONTEXT REPLAY REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {SYMBOL}\n")
        f.write(f"Ledger rows loaded: {len(ledger):,}\n")
        f.write(f"Contexts selected: {len(contexts)}\n")
        f.write(f"Replay ledgers generated: {len(summary):,}\n")
        f.write(f"Replay trades generated: {len(replay_ledger):,}\n\n")

        f.write("Top Horizon Context Replay Candidates\n")
        f.write("-" * 80 + "\n")

        f.write(
            summary.head(30)[
                [
                    "feature",
                    "target",
                    "side",
                    "context_type",
                    "context_value",
                    "trade_count",
                    "win_rate",
                    "mean_return",
                    "total_return",
                    "profit_factor",
                    "positive_day_rate",
                    "positive_month_rate",
                    "positive_year_rate",
                    "max_drawdown_return",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs:\n")
        f.write(f"Summary: {summary_path}\n")
        f.write(f"Ledger parquet: {ledger_path}\n")
        f.write(f"Ledger CSV sample: {ledger_sample_path}\n")
        f.write(f"Equity curves: {equity_path}\n")
        f.write(f"Monthly returns: {monthly_path}\n")
        f.write(f"Yearly returns: {yearly_path}\n")

    print("=" * 90)
    print("[DONE] Horizon context replay complete.")
    print(f"Summary:        {summary_path}")
    print(f"Ledger parquet: {ledger_path}")
    print(f"Ledger sample:  {ledger_sample_path}")
    print(f"Equity curves:  {equity_path}")
    print(f"Monthly:        {monthly_path}")
    print(f"Yearly:         {yearly_path}")
    print(f"Report:         {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()