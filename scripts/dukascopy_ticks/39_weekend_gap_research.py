"""
BACQE DUKASCOPY 39 - WEEKEND GAP RESEARCH

Purpose:
    Investigate whether Monday / Asia continuation effects are linked to
    weekend gap size, gap direction, and post-weekend information assimilation.
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURUSD"
QUANT_LAB = Path(r"E:\Quant_Lab")

def build_context_ledger(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_horizon_context_replay"
        / f"symbol={symbol}"
        / "trade_ledgers"
        / "horizon_context_replay_ledger_latest.parquet"
    )

def build_horizon_feature_root(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "processed"
        / "dukascopy_horizon_features"
        / f"symbol={symbol}"
    )

def build_output_root(symbol: str) -> Path:
    return (
        QUANT_LAB
        / "data"
        / "analysis"
        / "dukascopy_weekend_gap_research"
        / f"symbol={symbol}"
    )

TARGET_REPLAY_FILTER = "session=asia | day_of_week=Monday"
MIN_TRADES = 5_000


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def ensure_dirs(output_root: Path) -> None:
    for folder in [
        output_root,
        output_root / "gap_tables",
        output_root / "performance_tables",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def discover_horizon_files(horizon_feature_root: Path) -> list[Path]:
    return (
        sorted(horizon_feature_root.rglob("*.parquet"))
        if horizon_feature_root.exists()
        else []
    )


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


def build_daily_gap_table(
    horizon_feature_root: Path,
) -> pd.DataFrame:
    files = discover_horizon_files(horizon_feature_root)

    rows = []

    print(f"Horizon files discovered for gap table: {len(files)}")

    for i, path in enumerate(files, start=1):
        try:
            df = pd.read_parquet(path, columns=["timestamp_utc", "mid", "spread"])

            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce")
            df = df.dropna(subset=["timestamp_utc", "mid"])

            if df.empty:
                continue

            df = df.sort_values("timestamp_utc")

            first = df.iloc[0]
            last = df.iloc[-1]

            rows.append({
                "dataset": path.stem,
                "date": first["timestamp_utc"].date(),
                "first_timestamp": first["timestamp_utc"],
                "last_timestamp": last["timestamp_utc"],
                "first_mid": first["mid"],
                "last_mid": last["mid"],
                "first_spread": first["spread"],
                "avg_spread": df["spread"].mean(),
                "rows": len(df),
            })

        except Exception as e:
            print(f"[ERROR] {path.name}: {e}")

        if i % 100 == 0 or i == len(files):
            print(f"Processed {i}/{len(files)} files for gap table")

    daily = pd.DataFrame(rows)

    if daily.empty:
        return daily

    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date").reset_index(drop=True)

    daily["prev_last_mid"] = daily["last_mid"].shift(1)
    daily["prev_date"] = daily["date"].shift(1)

    daily["open_gap_return"] = daily["first_mid"] / daily["prev_last_mid"] - 1
    daily["abs_open_gap_return"] = daily["open_gap_return"].abs()

    daily["day_of_week"] = daily["date"].dt.day_name()
    daily["year"] = daily["date"].dt.year
    daily["month"] = daily["date"].dt.to_period("M").astype(str)

    daily["gap_direction"] = np.select(
        [
            daily["open_gap_return"] > 0,
            daily["open_gap_return"] < 0,
        ],
        [
            "gap_up",
            "gap_down",
        ],
        default="flat_gap",
    )

    monday_mask = daily["day_of_week"] == "Monday"

    if monday_mask.any():
        monday_gaps = daily.loc[monday_mask, "abs_open_gap_return"].dropna()

        if len(monday_gaps) >= 5:
            q1 = monday_gaps.quantile(0.33)
            q2 = monday_gaps.quantile(0.66)

            def classify_gap_size(x):
                if pd.isna(x):
                    return "unknown_gap"
                if x <= q1:
                    return "small_gap"
                if x <= q2:
                    return "medium_gap"
                return "large_gap"

            daily["monday_gap_size"] = daily["abs_open_gap_return"].apply(classify_gap_size)
        else:
            daily["monday_gap_size"] = "unknown_gap"
    else:
        daily["monday_gap_size"] = "unknown_gap"

    return daily


def attach_gap_info_to_ledger(ledger: pd.DataFrame, daily_gap: pd.DataFrame) -> pd.DataFrame:
    ledger = ledger.copy()

    ledger["timestamp_utc"] = pd.to_datetime(ledger["timestamp_utc"], errors="coerce")
    ledger["trade_date"] = ledger["timestamp_utc"].dt.date
    ledger["trade_date"] = pd.to_datetime(ledger["trade_date"])

    gap_cols = [
        "date",
        "open_gap_return",
        "abs_open_gap_return",
        "gap_direction",
        "monday_gap_size",
        "first_spread",
        "avg_spread",
    ]

    merged = ledger.merge(
        daily_gap[gap_cols],
        left_on="trade_date",
        right_on="date",
        how="left",
    )

    return merged


def analyse_gap_performance(ledger: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []

    for keys, group in ledger.groupby(group_cols):
        if not isinstance(keys, tuple):
            keys = (keys,)

        if len(group) < MIN_TRADES:
            continue

        row = {col: value for col, value in zip(group_cols, keys)}
        stats = evaluate_returns(group["net_signal_return"])

        row.update(stats)
        row["avg_open_gap_return"] = group["open_gap_return"].mean()
        row["avg_abs_open_gap_return"] = group["abs_open_gap_return"].mean()
        row["avg_first_spread"] = group["first_spread"].mean()
        row["avg_day_spread"] = group["avg_spread"].mean()
        row["days_tested"] = group["trade_date"].nunique()
        row["months_tested"] = group["month"].nunique() if "month" in group.columns else np.nan
        row["years_tested"] = group["year"].nunique() if "year" in group.columns else np.nan

        rows.append(row)

    return pd.DataFrame(rows)


def run_weekend_gap_research(
    symbol: str = DEFAULT_SYMBOL,
) -> None:
    symbol = symbol.upper().strip()

    context_ledger = build_context_ledger(symbol)
    horizon_feature_root = build_horizon_feature_root(symbol)
    output_root = build_output_root(symbol)

    banner("BACQE DUKASCOPY 39 - WEEKEND GAP RESEARCH")

    ensure_dirs(output_root)

    print(f"Symbol:          {symbol}")
    print(f"Context ledger:  {context_ledger}")
    print(f"Horizon root:    {horizon_feature_root}")
    print(f"Output root:     {output_root}")
    print("-" * 90)

    if not context_ledger.exists():
        print("[STOP] Missing Script 35 context replay ledger.")
        return

    daily_gap = build_daily_gap_table(horizon_feature_root)

    if daily_gap.empty:
        print("[STOP] Could not build daily gap table.")
        return

    ledger = pd.read_parquet(context_ledger)

    print(f"Loaded context replay ledger rows: {len(ledger):,}")

    required = {
        "timestamp_utc",
        "replay_id",
        "context_value",
        "net_signal_return",
        "day_of_week",
        "session",
        "year",
        "month",
    }

    missing = required - set(ledger.columns)

    if missing:
        print(f"[STOP] Missing ledger columns: {sorted(missing)}")
        return

    # Focus primarily on the core Monday Asia candidate, but keep full ledger for comparison outputs.
    monday_asia = ledger[
        ledger["context_value"].astype(str).str.contains("session=asia", case=False, na=False)
        & ledger["context_value"].astype(str).str.contains("day_of_week=Monday", case=False, na=False)
    ].copy()

    if monday_asia.empty:
        print("[WARNING] No exact Monday Asia context found. Falling back to session/day columns.")
        monday_asia = ledger[
            (ledger["session"].astype(str) == "asia")
            & (ledger["day_of_week"].astype(str) == "Monday")
        ].copy()

    print(f"Monday Asia ledger rows: {len(monday_asia):,}")

    monday_asia = attach_gap_info_to_ledger(monday_asia, daily_gap)

    monday_only_gaps = daily_gap[daily_gap["day_of_week"] == "Monday"].copy()

    # Performance by gap type
    by_gap_size = analyse_gap_performance(monday_asia, ["monday_gap_size"])
    by_gap_direction = analyse_gap_performance(monday_asia, ["gap_direction"])
    by_gap_size_direction = analyse_gap_performance(monday_asia, ["monday_gap_size", "gap_direction"])
    by_year_gap_size = analyse_gap_performance(monday_asia, ["year", "monday_gap_size"])

    # Save outputs
    daily_gap_path = output_root / "gap_tables" / "daily_open_gap_table_latest.csv"
    monday_gap_path = output_root / "gap_tables" / "monday_open_gap_table_latest.csv"
    by_gap_size_path = output_root / "performance_tables" / "monday_asia_by_gap_size_latest.csv"
    by_gap_direction_path = output_root / "performance_tables" / "monday_asia_by_gap_direction_latest.csv"
    by_gap_size_direction_path = output_root / "performance_tables" / "monday_asia_by_gap_size_direction_latest.csv"
    by_year_gap_size_path = output_root / "performance_tables" / "monday_asia_by_year_gap_size_latest.csv"
    report_path = output_root / "reports" / "weekend_gap_research_report_latest.txt"

    daily_gap.to_csv(daily_gap_path, index=False)
    monday_only_gaps.to_csv(monday_gap_path, index=False)
    by_gap_size.to_csv(by_gap_size_path, index=False)
    by_gap_direction.to_csv(by_gap_direction_path, index=False)
    by_gap_size_direction.to_csv(by_gap_size_direction_path, index=False)
    by_year_gap_size.to_csv(by_year_gap_size_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY WEEKEND GAP RESEARCH REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Daily gap rows: {len(daily_gap):,}\n")
        f.write(f"Monday gap rows: {len(monday_only_gaps):,}\n")
        f.write(f"Monday Asia replay rows: {len(monday_asia):,}\n\n")

        f.write("Monday Gap Summary\n")
        f.write("-" * 80 + "\n")
        f.write(
            monday_only_gaps[
                [
                    "date",
                    "open_gap_return",
                    "abs_open_gap_return",
                    "gap_direction",
                    "monday_gap_size",
                    "first_spread",
                    "avg_spread",
                    "rows",
                ]
            ].describe(include="all").to_string()
        )

        f.write("\n\nMonday Asia Performance by Gap Size\n")
        f.write("-" * 80 + "\n")
        f.write(by_gap_size.to_string(index=False))

        f.write("\n\nMonday Asia Performance by Gap Direction\n")
        f.write("-" * 80 + "\n")
        f.write(by_gap_direction.to_string(index=False))

        f.write("\n\nMonday Asia Performance by Gap Size + Direction\n")
        f.write("-" * 80 + "\n")
        f.write(by_gap_size_direction.to_string(index=False))

        f.write("\n\nMonday Asia Performance by Year + Gap Size\n")
        f.write("-" * 80 + "\n")
        f.write(by_year_gap_size.to_string(index=False))

        f.write("\n\nOutputs:\n")
        f.write(f"Daily gaps: {daily_gap_path}\n")
        f.write(f"Monday gaps: {monday_gap_path}\n")
        f.write(f"By gap size: {by_gap_size_path}\n")
        f.write(f"By gap direction: {by_gap_direction_path}\n")
        f.write(f"By gap size/direction: {by_gap_size_direction_path}\n")
        f.write(f"By year/gap size: {by_year_gap_size_path}\n")

    print("=" * 90)
    print("[DONE] Weekend gap research complete.")
    print(f"Report: {report_path}")
    print("=" * 90)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Dukascopy weekend gap research."
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_weekend_gap_research(
        symbol=args.symbol,
    )


if __name__ == "__main__":
    main()