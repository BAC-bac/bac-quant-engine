"""
BACQE DUKASCOPY 62 - REFINED CANDIDATE VALIDATION
"""

from pathlib import Path
import numpy as np
import pandas as pd


SYMBOL = "EURUSD"

QUANT_LAB = Path(r"E:\Quant_Lab")

INPUT_FILE = (
    QUANT_LAB / "data" / "analysis"
    / "dukascopy_oos_pass_trade_profile"
    / f"symbol={SYMBOL}" / "tables"
    / "trade_profile_latest.csv"
)

OUTPUT_ROOT = (
    QUANT_LAB / "data" / "analysis"
    / "dukascopy_refined_candidate_validation"
    / f"symbol={SYMBOL}"
)

EXCLUDED_HOURS = [3, 6]
MIN_TRADES_PER_YEAR = 5_000


def ensure_dirs() -> None:
    for folder in [
        OUTPUT_ROOT,
        OUTPUT_ROOT / "yearly_validation",
        OUTPUT_ROOT / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def evaluate_returns(returns: pd.Series) -> dict:
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    if returns.empty:
        return {
            "trade_count": 0,
            "win_rate": np.nan,
            "total_return": np.nan,
            "profit_factor": np.nan,
            "max_drawdown": np.nan,
        }

    wins = returns[returns > 0]
    losses = returns[returns < 0]

    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())

    profit_factor = gross_profit / gross_loss if gross_loss > 0 else np.nan

    equity = returns.cumsum()
    drawdown = equity - equity.cummax()

    return {
        "trade_count": len(returns),
        "win_rate": (returns > 0).mean(),
        "total_return": returns.sum(),
        "profit_factor": profit_factor,
        "max_drawdown": drawdown.min(),
    }


def classify_result(row: pd.Series) -> str:
    if (
        row["train_profit_factor"] >= 1.05
        and row["validation_profit_factor"] >= 1.05
        and row["oos_profit_factor"] >= 1.05
        and row["profitable_years"] == 3
    ):
        return "refined_oos_pass"

    if (
        row["validation_profit_factor"] >= 1.00
        and row["oos_profit_factor"] >= 1.00
        and row["profitable_years"] >= 2
    ):
        return "refined_watchlist"

    return "refined_reject"


def positive_month_rate(df: pd.DataFrame) -> float:
    monthly = df.groupby("month")["net_signal_return"].sum()

    if monthly.empty:
        return np.nan

    return (monthly > 0).mean()


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 62 - REFINED CANDIDATE VALIDATION")
    print("=" * 90)
    print(f"Input:          {INPUT_FILE}")
    print(f"Output:         {OUTPUT_ROOT}")
    print(f"Excluded hours: {EXCLUDED_HOURS}")
    print("-" * 90)

    ensure_dirs()

    if not INPUT_FILE.exists():
        print(f"[STOP] Missing input file: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)

    print(f"Base trades loaded: {len(df):,}")

    required = {"hour", "year", "month", "net_signal_return"}

    missing = required - set(df.columns)
    if missing:
        print(f"[STOP] Missing columns: {sorted(missing)}")
        return

    df["hour"] = pd.to_numeric(df["hour"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["net_signal_return"] = pd.to_numeric(df["net_signal_return"], errors="coerce")

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["hour", "year", "net_signal_return"])

    df["hour"] = df["hour"].astype(int)
    df["year"] = df["year"].astype(int)

    refined = df[~df["hour"].isin(EXCLUDED_HOURS)].copy()

    print(f"Refined trades: {len(refined):,}")

    rows = []

    result = {
        "symbol": SYMBOL,
        "excluded_hours": ",".join(str(h) for h in EXCLUDED_HOURS),
    }

    profitable_years = 0

    for year, prefix in [
        (2023, "train"),
        (2024, "validation"),
        (2025, "oos"),
    ]:
        year_df = refined[refined["year"] == year].copy()
        stats = evaluate_returns(year_df["net_signal_return"])

        result[f"{prefix}_trade_count"] = stats["trade_count"]
        result[f"{prefix}_win_rate"] = stats["win_rate"]
        result[f"{prefix}_total_return"] = stats["total_return"]
        result[f"{prefix}_profit_factor"] = stats["profit_factor"]
        result[f"{prefix}_max_drawdown"] = stats["max_drawdown"]
        result[f"{prefix}_positive_month_rate"] = positive_month_rate(year_df)

        if (
            stats["trade_count"] >= MIN_TRADES_PER_YEAR
            and stats["total_return"] > 0
        ):
            profitable_years += 1

        rows.append({
            "year": year,
            "phase": prefix,
            "trade_count": stats["trade_count"],
            "win_rate": stats["win_rate"],
            "total_return": stats["total_return"],
            "profit_factor": stats["profit_factor"],
            "max_drawdown": stats["max_drawdown"],
            "positive_month_rate": positive_month_rate(year_df),
        })

    result["profitable_years"] = profitable_years
    result["validation_label"] = classify_result(pd.Series(result))

    summary = pd.DataFrame([result])
    yearly = pd.DataFrame(rows)

    yearly_path = OUTPUT_ROOT / "yearly_validation" / "refined_candidate_yearly_latest.csv"
    summary_path = OUTPUT_ROOT / "yearly_validation" / "refined_candidate_summary_latest.csv"
    report_path = OUTPUT_ROOT / "reports" / "refined_candidate_validation_report_latest.txt"

    yearly.to_csv(yearly_path, index=False)
    summary.to_csv(summary_path, index=False)

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY REFINED CANDIDATE VALIDATION REPORT\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Symbol: {SYMBOL}\n")
        f.write(f"Excluded hours: {EXCLUDED_HOURS}\n")
        f.write(f"Base trades: {len(df):,}\n")
        f.write(f"Refined trades: {len(refined):,}\n")
        f.write(f"Validation label: {result['validation_label']}\n")
        f.write(f"Profitable years: {profitable_years}/3\n\n")

        f.write("YEARLY VALIDATION\n")
        f.write("-" * 80 + "\n")
        f.write(yearly.to_string(index=False))
        f.write("\n\n")

        f.write("SUMMARY\n")
        f.write("-" * 80 + "\n")
        f.write(summary.to_string(index=False))

    print("=" * 90)
    print("[DONE] Refined candidate validation complete.")
    print(f"Yearly:  {yearly_path}")
    print(f"Summary: {summary_path}")
    print(f"Report:  {report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()