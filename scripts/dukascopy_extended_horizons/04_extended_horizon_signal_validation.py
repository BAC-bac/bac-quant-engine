"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 04 - EXTENDED HORIZON SIGNAL VALIDATION

Purpose:
    Validate stable extended-horizon feature candidates by replaying them
    across the EURJPY extended horizon feature files.

Inputs:
    1. Extended horizon feature files from Script 01
    2. Stable candidates from Script 03

Outputs:
    Ranked validation results and report.
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURJPY"
DEFAULT_TOP_N = 75

BASE_DIR = Path("E:/Quant_Lab")

FEATURE_ROOT = BASE_DIR / "data" / "processed" / "dukascopy_extended_horizon_features"

STABILITY_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "feature_stability"
)

REPORT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "signal_validation"
)

QUANTILES = [0.10, 0.20, 0.25, 0.75, 0.80, 0.90]


def print_header(symbol: str, top_n: int) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 04 - EXTENDED HORIZON SIGNAL VALIDATION")
    print("=" * 90)
    print(f"Symbol:       {symbol}")
    print(f"Top N:        {top_n}")
    print(f"Feature root: {FEATURE_ROOT}")
    print(f"Report root:  {REPORT_ROOT}")
    print("-" * 90)


def find_feature_files(symbol: str) -> list[Path]:
    root = FEATURE_ROOT / f"symbol={symbol}"

    if not root.exists():
        raise FileNotFoundError(f"Missing feature folder: {root}")

    files = sorted(root.rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No parquet files found under: {root}")

    return files


def load_candidates(symbol: str, top_n: int) -> pd.DataFrame:
    path = STABILITY_ROOT / f"{symbol.lower()}_extended_horizon_stable_candidates_latest.csv"

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 03 stable candidates file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Stable candidates file is empty.")

    df = df.sort_values("stability_score", ascending=False).head(top_n).copy()

    required = ["target", "feature", "best_side", "stability_score", "stability_status"]
    missing = [col for col in required if col not in df.columns]

    if missing:
        raise ValueError(f"Stable candidates file missing required columns: {missing}")

    return df


def safe_profit_factor(returns: pd.Series) -> float:
    wins = returns[returns > 0].sum()
    losses = returns[returns < 0].sum()

    if losses == 0:
        return np.inf if wins > 0 else np.nan

    return float(wins / abs(losses))


def safe_expectancy(returns: pd.Series) -> float:
    if len(returns) == 0:
        return np.nan

    return float(returns.mean())


def validate_candidate_on_file(
    df: pd.DataFrame,
    feature: str,
    target: str,
    candidate_side: str,
    file_path: Path,
) -> list[dict]:

    if feature not in df.columns or target not in df.columns:
        return []

    data = df[[feature, target]].replace([np.inf, -np.inf], np.nan).dropna()

    if len(data) < 500:
        return []

    feature_series = data[feature]
    target_series = data[target]

    if feature_series.nunique(dropna=True) <= 1:
        return []

    thresholds = feature_series.quantile(QUANTILES).to_dict()

    rows = []

    for q, threshold in thresholds.items():

        if q < 0.5:
            signal_mask = feature_series <= threshold
            threshold_side = "lower"
        else:
            signal_mask = feature_series >= threshold
            threshold_side = "upper"

        signal_target_returns = target_series[signal_mask]

        if candidate_side == "short":
            trade_returns = -signal_target_returns
        else:
            trade_returns = signal_target_returns

        trade_returns = trade_returns.dropna()

        if len(trade_returns) < 100:
            continue

        rows.append(
            {
                "file": str(file_path),
                "filename": file_path.name,
                "feature": feature,
                "target": target,
                "candidate_side": candidate_side,
                "threshold_quantile": q,
                "threshold_side": threshold_side,
                "threshold_value": float(threshold),
                "trades": int(len(trade_returns)),
                "win_rate": float((trade_returns > 0).mean()),
                "avg_return": float(trade_returns.mean()),
                "median_return": float(trade_returns.median()),
                "total_return": float(trade_returns.sum()),
                "profit_factor": safe_profit_factor(trade_returns),
                "expectancy": safe_expectancy(trade_returns),
                "max_return": float(trade_returns.max()),
                "min_return": float(trade_returns.min()),
            }
        )

    return rows


def aggregate_validation(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw

    grouped = (
        raw.groupby(
            [
                "target",
                "feature",
                "candidate_side",
                "threshold_quantile",
                "threshold_side",
            ],
            dropna=False,
        )
        .agg(
            files_tested=("file", "nunique"),
            total_trades=("trades", "sum"),
            mean_win_rate=("win_rate", "mean"),
            median_win_rate=("win_rate", "median"),
            mean_avg_return=("avg_return", "mean"),
            median_avg_return=("avg_return", "median"),
            total_return=("total_return", "sum"),
            mean_profit_factor=("profit_factor", "mean"),
            median_profit_factor=("profit_factor", "median"),
            mean_expectancy=("expectancy", "mean"),
            median_expectancy=("expectancy", "median"),
            worst_file_return=("total_return", "min"),
            best_file_return=("total_return", "max"),
        )
        .reset_index()
    )

    grouped["validation_score"] = (
        (grouped["mean_win_rate"].fillna(0.5) - 0.5) * 100
        + grouped["mean_avg_return"].fillna(0) * 100000
        + np.log1p(grouped["total_trades"].fillna(0)) * 2
        + grouped["median_profit_factor"].replace([np.inf, -np.inf], np.nan).fillna(0)
    )

    grouped["validation_status"] = np.select(
        [
            (grouped["total_trades"] >= 10_000)
            & (grouped["median_win_rate"] > 0.52)
            & (grouped["median_avg_return"] > 0)
            & (grouped["median_profit_factor"] > 1.05),

            (grouped["total_trades"] >= 10_000)
            & (grouped["median_win_rate"] > 0.505)
            & (grouped["median_avg_return"] > 0),

            grouped["total_trades"] < 10_000,
        ],
        [
            "validation_pass_primary",
            "validation_pass_secondary",
            "insufficient_trades",
        ],
        default="validation_fail",
    )

    grouped = grouped.sort_values(
        by=[
            "validation_status",
            "validation_score",
            "median_win_rate",
            "median_avg_return",
            "median_profit_factor",
        ],
        ascending=[True, False, False, False, False],
    )

    return grouped


def write_outputs(symbol: str, raw: pd.DataFrame, ranked: pd.DataFrame) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    raw_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_signal_validation_raw_latest.csv"
    ranked_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_signal_validation_ranked_latest.csv"
    passed_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_signal_validation_passed_latest.csv"
    txt_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_signal_validation_report_latest.txt"

    raw.to_csv(raw_path, index=False)
    ranked.to_csv(ranked_path, index=False)

    passed = ranked[
        ranked["validation_status"].isin(
            ["validation_pass_primary", "validation_pass_secondary"]
        )
    ].copy()

    passed.to_csv(passed_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 04 - EXTENDED HORIZON SIGNAL VALIDATION REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Raw validation rows: {len(raw)}\n")
        f.write(f"Ranked validation rows: {len(ranked)}\n")
        f.write(f"Passed candidates: {len(passed)}\n\n")

        if not ranked.empty:
            f.write("STATUS COUNTS\n")
            f.write("-" * 90 + "\n")
            f.write(ranked["validation_status"].value_counts().to_string())
            f.write("\n\n")

            display_cols = [
                "target",
                "feature",
                "candidate_side",
                "threshold_quantile",
                "threshold_side",
                "validation_status",
                "validation_score",
                "total_trades",
                "median_win_rate",
                "median_avg_return",
                "median_profit_factor",
                "total_return",
                "files_tested",
            ]

            f.write("TOP 50 VALIDATED CANDIDATES\n")
            f.write("-" * 90 + "\n")
            f.write(ranked[display_cols].head(50).to_string(index=False))
        else:
            f.write("No ranked validation rows produced.\n")

    print(f"Raw validation:    {raw_path}")
    print(f"Ranked validation: {ranked_path}")
    print(f"Passed candidates: {passed_path}")
    print(f"Text report:       {txt_path}")


def main(symbol: str, top_n: int) -> None:
    print_header(symbol, top_n)

    files = find_feature_files(symbol)
    candidates = load_candidates(symbol, top_n)

    print(f"Feature files found: {len(files):,}")
    print(f"Candidates loaded:   {len(candidates):,}")
    print("-" * 90)

    all_rows = []

    candidate_records = candidates.to_dict("records")

    for file_idx, file_path in enumerate(files, start=1):
        try:
            needed_cols = sorted(
                set(
                    [row["feature"] for row in candidate_records]
                    + [row["target"] for row in candidate_records]
                )
            )

            df = pd.read_parquet(file_path, columns=needed_cols)

            file_rows = 0

            for candidate in candidate_records:
                rows = validate_candidate_on_file(
                    df=df,
                    feature=candidate["feature"],
                    target=candidate["target"],
                    candidate_side=candidate["best_side"],
                    file_path=file_path,
                )

                for row in rows:
                    row["stability_score"] = candidate["stability_score"]
                    row["stability_status"] = candidate["stability_status"]

                all_rows.extend(rows)
                file_rows += len(rows)

            print(
                f"[OK] {file_idx:>4}/{len(files)} "
                f"validation_rows={file_rows:>5} "
                f"file={file_path.name}"
            )

        except Exception as exc:
            print(f"[ERROR] {file_idx:>4}/{len(files)} {file_path.name} :: {exc}")

    raw = pd.DataFrame(all_rows)
    ranked = aggregate_validation(raw)

    print("-" * 90)
    print(f"Raw validation rows:    {len(raw):,}")
    print(f"Ranked validation rows: {len(ranked):,}")

    if not ranked.empty:
        print("Validation status counts:")
        print(ranked["validation_status"].value_counts())

    print("-" * 90)

    write_outputs(symbol, raw, ranked)

    print("-" * 90)
    print("[DONE] Extended horizon signal validation complete")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--symbol",
        default=DEFAULT_SYMBOL,
        help="Symbol to process, e.g. EURJPY",
    )

    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help="Number of stable candidates to validate",
    )

    args = parser.parse_args()

    main(
        symbol=args.symbol.upper(),
        top_n=args.top_n,
    )