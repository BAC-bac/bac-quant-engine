"""
BACQE DUKASCOPY 40 - CONDITIONAL CONTEXT ENGINE

Purpose:
    Test whether weekend-gap conditioning improves the Monday Asia context edge.

Compares:
    Monday Asia only
    Monday Asia + small/medium/large gap
    Monday Asia + gap up/down
    Monday Asia + gap size + gap direction
"""

from pathlib import Path
import numpy as np
import pandas as pd


SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

CONTEXT_LEDGER = (
    QUANT_LAB / "data" / "analysis" / "dukascopy_horizon_context_replay"
    / "trade_ledgers" / "horizon_context_replay_ledger_latest.parquet"
)

DAILY_GAP_PATH = (
    QUANT_LAB / "data" / "analysis" / "dukascopy_weekend_gap_research"
    / "gap_tables" / "daily_open_gap_table_latest.csv"
)

OUTPUT_ROOT = QUANT_LAB / "data" / "analysis" / "dukascopy_conditional_context_engine"

MIN_TRADES = 5_000


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "conditional_results",
        OUTPUT_ROOT / "rankings",
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
        "total_return": returns.sum(),
        "profit_factor": profit_factor,
        "max_drawdown_return": drawdown.min(),
    }


def attach_gap_info(ledger: pd.DataFrame, gap: pd.DataFrame) -> pd.DataFrame:
    ledger = ledger.copy()
    gap = gap.copy()

    ledger["timestamp_utc"] = pd.to_datetime(ledger["timestamp_utc"], errors="coerce")
    ledger["trade_date"] = pd.to_datetime(ledger["timestamp_utc"].dt.date)

    gap["date"] = pd.to_datetime(gap["date"], errors="coerce")

    gap_cols = [
        "date",
        "open_gap_return",
        "abs_open_gap_return",
        "gap_direction",
        "monday_gap_size",
        "first_spread",
        "avg_spread",
    ]

    return ledger.merge(
        gap[gap_cols],
        left_on="trade_date",
        right_on="date",
        how="left",
    )


def build_condition_sets(df: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    conditions = []

    conditions.append(("monday_asia_all", df))

    for gap_size in ["small_gap", "medium_gap", "large_gap"]:
        conditions.append((
            f"monday_asia_{gap_size}",
            df[df["monday_gap_size"] == gap_size],
        ))

    for direction in ["gap_up", "gap_down"]:
        conditions.append((
            f"monday_asia_{direction}",
            df[df["gap_direction"] == direction],
        ))

    for gap_size in ["small_gap", "medium_gap", "large_gap"]:
        for direction in ["gap_up", "gap_down"]:
            conditions.append((
                f"monday_asia_{gap_size}_{direction}",
                df[
                    (df["monday_gap_size"] == gap_size)
                    & (df["gap_direction"] == direction)
                ],
            ))

    return conditions


def score_results(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df[df["trade_count"] >= MIN_TRADES].copy()

    if df.empty:
        return df

    df["pf_score"] = df["profit_factor"].clip(0, 2).fillna(0) / 2
    df["win_score"] = df["win_rate"].fillna(0)
    df["day_score"] = df["positive_day_rate"].fillna(0)
    df["month_score"] = df["positive_month_rate"].fillna(0)
    df["year_score"] = df["positive_year_rate"].fillna(0)

    df["return_score"] = df["mean_return"].clip(lower=0)
    max_return = df["return_score"].max()

    if pd.notna(max_return) and max_return != 0:
        df["return_score"] = df["return_score"] / max_return
    else:
        df["return_score"] = 0

    df["conditional_score"] = (
        df["pf_score"] * 0.30
        + df["win_score"] * 0.15
        + df["day_score"] * 0.15
        + df["month_score"] * 0.20
        + df["year_score"] * 0.10
        + df["return_score"] * 0.10
    )

    df["conditional_label"] = np.select(
        [
            (df["profit_factor"] >= 1.30) & (df["total_return"] > 0) & (df["positive_year_rate"] >= 0.67),
            (df["profit_factor"] >= 1.15) & (df["total_return"] > 0),
            (df["profit_factor"] >= 1.00) & (df["total_return"] > 0),
        ],
        [
            "strong_conditional_edge",
            "conditional_edge",
            "fragile_conditional_edge",
        ],
        default="reject",
    )

    df = df.sort_values("conditional_score", ascending=False)
    df.insert(0, "conditional_rank", range(1, len(df) + 1))

    return df


def main() -> None:
    banner("BACQE DUKASCOPY 40 - CONDITIONAL CONTEXT ENGINE")

    ensure_dirs()

    print(f"Symbol:         {SYMBOL}")
    print(f"Context ledger: {CONTEXT_LEDGER}")
    print(f"Daily gaps:     {DAILY_GAP_PATH}")
    print(f"Output root:    {OUTPUT_ROOT}")
    print("-" * 90)

    if not CONTEXT_LEDGER.exists():
        print("[STOP] Missing Script 35 context replay ledger.")
        return

    if not DAILY_GAP_PATH.exists():
        print("[STOP] Missing Script 39 daily gap table.")
        return

    ledger = pd.read_parquet(CONTEXT_LEDGER)
    gaps = pd.read_csv(DAILY_GAP_PATH)

    print(f"Loaded ledger rows: {len(ledger):,}")
    print(f"Loaded gap rows:    {len(gaps):,}")

    required = {
        "timestamp_utc",
        "replay_id",
        "feature",
        "target",
        "side",
        "context_value",
        "session",
        "day_of_week",
        "year",
        "month",
        "net_signal_return",
    }

    missing = required - set(ledger.columns)

    if missing:
        print(f"[STOP] Missing ledger columns: {sorted(missing)}")
        return

    monday_asia = ledger[
        (ledger["session"].astype(str) == "asia")
        & (ledger["day_of_week"].astype(str) == "Monday")
        & (ledger["feature"].astype(str) == "mid_return_1")
        & (ledger["target"].astype(str) == "future_return_1000")
        & (ledger["side"].astype(str) == "long")
    ].copy()

    print(f"Monday Asia candidate rows: {len(monday_asia):,}")

    if monday_asia.empty:
        print("[STOP] No Monday Asia candidate rows found.")
        return

    monday_asia = attach_gap_info(monday_asia, gaps)

    rows = []

    for condition_name, condition_df in build_condition_sets(monday_asia):
        if len(condition_df) < MIN_TRADES:
            continue

        stats = evaluate_returns(condition_df["net_signal_return"])

        rows.append({
            "condition_name": condition_name,
            "feature": "mid_return_1",
            "target": "future_return_1000",
            "side": "long",
            "trade_count": stats["trade_count"],
            "win_rate": stats["win_rate"],
            "mean_return": stats["mean_return"],
            "total_return": stats["total_return"],
            "profit_factor": stats["profit_factor"],
            "max_drawdown_return": stats["max_drawdown_return"],
            "avg_open_gap_return": condition_df["open_gap_return"].mean(),
            "avg_abs_open_gap_return": condition_df["abs_open_gap_return"].mean(),
            "avg_first_spread": condition_df["first_spread"].mean(),
            "avg_day_spread": condition_df["avg_spread"].mean(),
            "days_tested": condition_df["trade_date"].nunique(),
            "months_tested": condition_df["month"].nunique(),
            "years_tested": condition_df["year"].nunique(),
            "positive_day_rate": (
                condition_df.groupby("trade_date")["net_signal_return"].sum() > 0
            ).mean(),
            "positive_month_rate": (
                condition_df.groupby("month")["net_signal_return"].sum() > 0
            ).mean(),
            "positive_year_rate": (
                condition_df.groupby("year")["net_signal_return"].sum() > 0
            ).mean(),
        })

    results = pd.DataFrame(rows)

    if results.empty:
        print("[STOP] No conditional results generated.")
        return

    ranked = score_results(results)

    output_all = OUTPUT_ROOT / "conditional_results" / "conditional_context_results_latest.csv"
    output_ranked = OUTPUT_ROOT / "rankings" / "conditional_context_ranked_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "conditional_context_report_latest.txt"

    results.to_csv(output_all, index=False)
    ranked.to_csv(output_ranked, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY CONDITIONAL CONTEXT ENGINE REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {SYMBOL}\n")
        f.write(f"Monday Asia candidate rows: {len(monday_asia):,}\n")
        f.write(f"Conditional rows: {len(results):,}\n\n")

        f.write("Conditional Label Counts\n")
        f.write("-" * 80 + "\n")
        f.write(ranked["conditional_label"].value_counts().to_string())
        f.write("\n\n")

        f.write("Ranked Conditional Contexts\n")
        f.write("-" * 80 + "\n")
        f.write(
            ranked[
                [
                    "conditional_rank",
                    "condition_name",
                    "trade_count",
                    "win_rate",
                    "mean_return",
                    "total_return",
                    "profit_factor",
                    "max_drawdown_return",
                    "positive_day_rate",
                    "positive_month_rate",
                    "positive_year_rate",
                    "avg_open_gap_return",
                    "avg_abs_open_gap_return",
                    "conditional_label",
                    "conditional_score",
                ]
            ].to_string(index=False)
        )

        f.write("\n\nOutputs:\n")
        f.write(f"All:    {output_all}\n")
        f.write(f"Ranked: {output_ranked}\n")

    print("=" * 90)
    print("[DONE] Conditional context engine complete.")
    print(f"All:    {output_all}")
    print(f"Ranked: {output_ranked}")
    print(f"Report: {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()