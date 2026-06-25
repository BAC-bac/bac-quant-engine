"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 11 - CROSS SYMBOL TRANSFER ENGINE

Purpose:
    Test whether EURJPY's strongest extended-horizon regimes transfer to
    other processed Dukascopy symbols.

Base:
    EURJPY

Targets:
    EURUSD, GBPUSD, USDJPY by default
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_BASE_SYMBOL = "EURJPY"
DEFAULT_SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY"]
DEFAULT_TOP_N = 25

BASE_DIR = Path("E:/Quant_Lab")

FEATURE_ROOT = BASE_DIR / "data" / "processed" / "dukascopy_extended_horizon_features"

MC_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "monte_carlo_robustness"
)

REPORT_ROOT = (
    BASE_DIR
    / "data"
    / "analysis"
    / "dukascopy_extended_horizons"
    / "cross_symbol_transfer"
)


def print_header(base_symbol: str, symbols: list[str], top_n: int) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 11 - CROSS SYMBOL TRANSFER ENGINE")
    print("=" * 90)
    print(f"Base symbol:    {base_symbol}")
    print(f"Test symbols:   {symbols}")
    print(f"Top N regimes:  {top_n}")
    print(f"Feature root:   {FEATURE_ROOT}")
    print(f"MC root:        {MC_ROOT}")
    print(f"Report root:    {REPORT_ROOT}")
    print("-" * 90)


def find_feature_files(symbol: str) -> list[Path]:
    root = FEATURE_ROOT / f"symbol={symbol}"

    if not root.exists():
        raise FileNotFoundError(f"Missing feature folder: {root}")

    files = sorted(root.rglob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No parquet files found under: {root}")

    return files


def load_base_mc_regimes(base_symbol: str, top_n: int) -> pd.DataFrame:
    path = MC_ROOT / f"{base_symbol.lower()}_extended_horizon_monte_carlo_passed_latest.csv"

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 10 Monte Carlo passed file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Monte Carlo passed file is empty.")

    df = df.sort_values(
        by=[
            "robustness_score",
            "probability_profitable",
            "mc_total_return_p05",
            "net_total_return",
        ],
        ascending=[False, False, False, False],
    ).head(top_n).copy()

    required = [
        "regime_id",
        "context_type",
        "context_value",
        "target",
        "feature",
        "threshold_quantile",
        "threshold_side",
        "mc_status",
        "robustness_score",
        "probability_profitable",
    ]

    missing = [col for col in required if col not in df.columns]

    if missing:
        raise ValueError(f"Monte Carlo passed file missing required columns: {missing}")

    return df


def detect_spread_column(columns: list[str]) -> str | None:
    for col in ["spread", "spread_points", "rolling_spread_mean_5", "rolling_spread_mean_10"]:
        if col in columns:
            return col
    return None


def detect_volatility_column(columns: list[str]) -> str | None:
    for col in [
        "rolling_return_std_250",
        "rolling_return_std_100",
        "rolling_return_std_50",
        "rolling_abs_move_mean_250",
        "rolling_abs_move_mean_100",
    ]:
        if col in columns:
            return col
    return None


def parse_date_from_filename(path: Path) -> pd.Timestamp:
    extracted = pd.Series([path.name]).str.extract(r"(\d{4}[-_]\d{2}[-_]\d{2})")[0].iloc[0]

    if pd.isna(extracted):
        return pd.NaT

    return pd.to_datetime(str(extracted).replace("_", "-"), errors="coerce")


def assign_session(hour: pd.Series) -> pd.Series:
    hour = pd.to_numeric(hour, errors="coerce")

    conditions = [
        hour.between(0, 5, inclusive="both"),
        hour.between(6, 7, inclusive="both"),
        hour.between(8, 11, inclusive="both"),
        hour.between(12, 15, inclusive="both"),
        hour.between(16, 20, inclusive="both"),
        hour.between(21, 23, inclusive="both"),
    ]

    choices = [
        "asia_overnight",
        "pre_london",
        "london_morning",
        "london_newyork_overlap",
        "newyork_late",
        "late_session",
    ]

    return pd.Series(np.select(conditions, choices, default="unknown"), index=hour.index)


def add_context_columns(df: pd.DataFrame, spread_col: str | None, vol_col: str | None) -> pd.DataFrame:
    df = df.copy()

    if "hour" in df.columns:
        df["context_hour"] = pd.to_numeric(df["hour"], errors="coerce")
        df["context_session"] = assign_session(df["context_hour"])
    else:
        df["context_hour"] = np.nan
        df["context_session"] = "unknown"

    if spread_col and spread_col in df.columns:
        spread = pd.to_numeric(df[spread_col], errors="coerce")
        df["context_spread_regime"] = pd.qcut(
            spread.rank(method="first"),
            q=4,
            labels=["spread_q1_low", "spread_q2", "spread_q3", "spread_q4_high"],
            duplicates="drop",
        ).astype(str)
    else:
        df["context_spread_regime"] = "unknown"

    if vol_col and vol_col in df.columns:
        vol = pd.to_numeric(df[vol_col], errors="coerce")
        df["context_vol_regime"] = pd.qcut(
            vol.rank(method="first"),
            q=4,
            labels=["vol_q1_low", "vol_q2", "vol_q3", "vol_q4_high"],
            duplicates="drop",
        ).astype(str)
    else:
        df["context_vol_regime"] = "unknown"

    df["context_session_spread"] = (
        df["context_session"].astype(str)
        + "__"
        + df["context_spread_regime"].astype(str)
    )

    df["context_session_vol"] = (
        df["context_session"].astype(str)
        + "__"
        + df["context_vol_regime"].astype(str)
    )

    df["context_spread_vol"] = (
        df["context_spread_regime"].astype(str)
        + "__"
        + df["context_vol_regime"].astype(str)
    )

    return df


def context_match_mask(df: pd.DataFrame, context_type: str, context_value: str) -> pd.Series:
    if context_type not in df.columns:
        return pd.Series(False, index=df.index)

    series = df[context_type]

    if context_type == "context_hour":
        wanted = pd.to_numeric(pd.Series([context_value]), errors="coerce").iloc[0]
        return pd.to_numeric(series, errors="coerce") == wanted

    return series.astype(str) == str(context_value)


def profit_factor(returns: pd.Series) -> float:
    wins = returns[returns > 0].sum()
    losses = returns[returns < 0].sum()

    if losses == 0:
        return np.inf if wins > 0 else np.nan

    return float(wins / abs(losses))


def calculate_transfer_file_result(
    df: pd.DataFrame,
    regime: dict,
    spread_col: str | None,
    file_path: Path,
    test_symbol: str,
    base_symbol: str,
) -> dict | None:
    feature = regime["feature"]
    target = regime["target"]
    context_type = regime["context_type"]
    context_value = regime["context_value"]
    threshold_quantile = float(regime["threshold_quantile"])
    threshold_side = regime["threshold_side"]

    if feature not in df.columns or target not in df.columns:
        return None

    needed = [feature, target, context_type]

    if spread_col and spread_col in df.columns:
        needed.append(spread_col)

    needed = list(dict.fromkeys([col for col in needed if col in df.columns]))

    data = df.loc[:, needed].replace([np.inf, -np.inf], np.nan).dropna(subset=[feature, target])

    if len(data) < 500:
        return None

    feature_series = data[feature]

    if feature_series.nunique(dropna=True) <= 1:
        return None

    threshold = feature_series.quantile(threshold_quantile)

    if threshold_side == "lower":
        signal_mask = feature_series <= threshold
    else:
        signal_mask = feature_series >= threshold

    regime_mask = context_match_mask(data, context_type, context_value)
    final_mask = signal_mask & regime_mask

    signal_data = data.loc[final_mask].copy()

    if len(signal_data) == 0:
        return None

    gross_returns = signal_data[target]

    # Long-only transfer check to match current EURJPY regime research.
    if spread_col and spread_col in signal_data.columns:
        net_cost = signal_data[spread_col].abs() * 0.5
    else:
        net_cost = 0.0

    net_returns = gross_returns - net_cost

    file_date = parse_date_from_filename(file_path)

    return {
        "base_symbol": base_symbol,
        "test_symbol": test_symbol,
        "source_regime_id": regime["regime_id"],
        "source_mc_status": regime.get("mc_status", "unknown"),
        "source_robustness_score": regime.get("robustness_score", np.nan),
        "source_probability_profitable": regime.get("probability_profitable", np.nan),
        "file": str(file_path),
        "filename": file_path.name,
        "date": file_date,
        "year": file_date.year if pd.notna(file_date) else np.nan,
        "quarter": f"{file_date.year}Q{file_date.quarter}" if pd.notna(file_date) else "unknown",
        "month": file_date.strftime("%Y-%m") if pd.notna(file_date) else "unknown",
        "context_type": context_type,
        "context_value": context_value,
        "target": target,
        "feature": feature,
        "threshold_quantile": threshold_quantile,
        "threshold_side": threshold_side,
        "trades": int(len(net_returns)),
        "gross_total_return": float(gross_returns.sum()),
        "total_dynamic_cost": float(net_cost.sum()) if isinstance(net_cost, pd.Series) else float(net_cost),
        "net_total_return": float(net_returns.sum()),
        "net_mean_return": float(net_returns.mean()),
        "net_median_return": float(net_returns.median()),
        "net_win_rate": float((net_returns > 0).mean()),
        "net_profit_factor": profit_factor(net_returns),
    }


def aggregate_transfer(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw

    grouped = (
        raw.groupby(
            [
                "base_symbol",
                "test_symbol",
                "source_regime_id",
                "source_mc_status",
                "context_type",
                "context_value",
                "target",
                "feature",
                "threshold_quantile",
                "threshold_side",
            ],
            dropna=False,
        )
        .agg(
            files_tested=("file", "nunique"),
            total_trades=("trades", "sum"),
            gross_total_return=("gross_total_return", "sum"),
            total_dynamic_cost=("total_dynamic_cost", "sum"),
            net_total_return=("net_total_return", "sum"),
            mean_file_return=("net_total_return", "mean"),
            median_file_return=("net_total_return", "median"),
            positive_file_rate=("net_total_return", lambda x: float((x > 0).mean())),
            mean_net_win_rate=("net_win_rate", "mean"),
            median_net_win_rate=("net_win_rate", "median"),
            mean_net_profit_factor=("net_profit_factor", "mean"),
            median_net_profit_factor=("net_profit_factor", "median"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            source_robustness_score=("source_robustness_score", "mean"),
            source_probability_profitable=("source_probability_profitable", "mean"),
        )
        .reset_index()
    )

    grouped["transfer_score"] = (
        grouped["net_total_return"].fillna(0)
        + grouped["median_file_return"].fillna(0) * 100
        + (grouped["positive_file_rate"].fillna(0.5) - 0.5) * 100
        + (grouped["median_net_win_rate"].fillna(0.5) - 0.5) * 100
        + grouped["median_net_profit_factor"].replace([np.inf, -np.inf], np.nan).fillna(0) * 10
    )

    grouped["transfer_status"] = np.select(
        [
            (grouped["net_total_return"] > 0)
            & (grouped["positive_file_rate"] > 0.55)
            & (grouped["median_net_win_rate"] > 0.52)
            & (grouped["median_net_profit_factor"] > 1.10),

            (grouped["net_total_return"] > 0)
            & (grouped["positive_file_rate"] > 0.50)
            & (grouped["median_net_win_rate"] > 0.505),
        ],
        ["transfer_pass_primary", "transfer_pass_secondary"],
        default="transfer_fail_or_watchlist",
    )

    grouped = grouped.sort_values(
        by=[
            "test_symbol",
            "transfer_status",
            "transfer_score",
            "net_total_return",
            "positive_file_rate",
        ],
        ascending=[True, True, False, False, False],
    )

    return grouped


def aggregate_symbol_summary(ranked: pd.DataFrame) -> pd.DataFrame:
    if ranked.empty:
        return ranked

    summary = (
        ranked.groupby(["test_symbol", "transfer_status"], dropna=False)
        .agg(
            regimes=("source_regime_id", "count"),
            total_trades=("total_trades", "sum"),
            total_net_return=("net_total_return", "sum"),
            median_positive_file_rate=("positive_file_rate", "median"),
            median_net_win_rate=("median_net_win_rate", "median"),
            median_net_profit_factor=("median_net_profit_factor", "median"),
            median_transfer_score=("transfer_score", "median"),
        )
        .reset_index()
    )

    summary = summary.sort_values(
        by=["test_symbol", "transfer_status", "total_net_return"],
        ascending=[True, True, False],
    )

    return summary


def write_outputs(
    base_symbol: str,
    symbols: list[str],
    raw: pd.DataFrame,
    ranked: pd.DataFrame,
    summary: pd.DataFrame,
) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    symbol_suffix = "_".join([s.lower() for s in symbols])

    raw_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{symbol_suffix}_cross_symbol_transfer_raw_latest.csv"
    ranked_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{symbol_suffix}_cross_symbol_transfer_ranked_latest.csv"
    passed_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{symbol_suffix}_cross_symbol_transfer_passed_latest.csv"
    summary_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{symbol_suffix}_cross_symbol_transfer_summary_latest.csv"
    txt_path = REPORT_ROOT / f"{base_symbol.lower()}_to_{symbol_suffix}_cross_symbol_transfer_report_latest.txt"

    raw.to_csv(raw_path, index=False)
    ranked.to_csv(ranked_path, index=False)
    summary.to_csv(summary_path, index=False)

    if ranked.empty or "transfer_status" not in ranked.columns:
        passed = pd.DataFrame()
    else:
        passed = ranked[ranked["transfer_status"].isin(["transfer_pass_primary", "transfer_pass_secondary"])].copy()

    passed.to_csv(passed_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 11 - CROSS SYMBOL TRANSFER REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Base symbol: {base_symbol}\n")
        f.write(f"Test symbols: {symbols}\n")
        f.write(f"Raw rows: {len(raw)}\n")
        f.write(f"Ranked rows: {len(ranked)}\n")
        f.write(f"Transfer passes: {len(passed)}\n\n")

        if not ranked.empty:
            f.write("TRANSFER STATUS COUNTS\n")
            f.write("-" * 90 + "\n")
            f.write(ranked.groupby(["test_symbol", "transfer_status"]).size().to_string())
            f.write("\n\n")

            display_cols = [
                "test_symbol",
                "transfer_status",
                "source_regime_id",
                "context_type",
                "context_value",
                "target",
                "feature",
                "threshold_quantile",
                "threshold_side",
                "transfer_score",
                "files_tested",
                "total_trades",
                "net_total_return",
                "positive_file_rate",
                "median_net_win_rate",
                "median_net_profit_factor",
                "first_date",
                "last_date",
            ]

            f.write("TOP 75 TRANSFER RESULTS\n")
            f.write("-" * 90 + "\n")
            f.write(ranked[display_cols].head(75).to_string(index=False))
            f.write("\n\n")

            f.write("SYMBOL SUMMARY\n")
            f.write("-" * 90 + "\n")
            f.write(summary.to_string(index=False))

    print(f"Raw transfer:     {raw_path}")
    print(f"Ranked transfer:  {ranked_path}")
    print(f"Passed transfer:  {passed_path}")
    print(f"Summary:          {summary_path}")
    print(f"Text report:      {txt_path}")


def main(base_symbol: str, symbols: list[str], top_n: int) -> None:
    base_symbol = base_symbol.upper()
    symbols = [symbol.upper() for symbol in symbols if symbol.upper() != base_symbol]

    print_header(base_symbol, symbols, top_n)

    regimes = load_base_mc_regimes(base_symbol, top_n)
    regime_records = regimes.to_dict("records")

    print(f"Base regimes loaded: {len(regimes):,}")
    print("-" * 90)

    all_rows = []

    for symbol in symbols:
        try:
            files = find_feature_files(symbol)
        except Exception as exc:
            print(f"[SKIP] {symbol}: {exc}")
            continue

        print(f"[SYMBOL] {symbol} files={len(files):,}")

        for idx, file_path in enumerate(files, start=1):
            try:
                sample_df = pd.read_parquet(file_path)
                columns = sample_df.columns.tolist()

                spread_col = detect_spread_column(columns)
                vol_col = detect_volatility_column(columns)

                needed_cols = {"hour"}

                if spread_col:
                    needed_cols.add(spread_col)

                if vol_col:
                    needed_cols.add(vol_col)

                for regime in regime_records:
                    needed_cols.add(regime["feature"])
                    needed_cols.add(regime["target"])

                available_cols = [col for col in needed_cols if col in columns]

                df = pd.read_parquet(file_path, columns=available_cols)
                df = add_context_columns(df, spread_col=spread_col, vol_col=vol_col)

                file_rows = 0

                for regime in regime_records:
                    result = calculate_transfer_file_result(
                        df=df,
                        regime=regime,
                        spread_col=spread_col,
                        file_path=file_path,
                        test_symbol=symbol,
                        base_symbol=base_symbol,
                    )

                    if result is not None:
                        all_rows.append(result)
                        file_rows += 1

                print(
                    f"[OK] {symbol} {idx:>4}/{len(files)} "
                    f"transfer_results={file_rows:>4} "
                    f"spread_col={spread_col} "
                    f"vol_col={vol_col} "
                    f"file={file_path.name}"
                )

            except Exception as exc:
                print(f"[ERROR] {symbol} {idx:>4}/{len(files)} {file_path.name} :: {exc}")

    raw = pd.DataFrame(all_rows)
    ranked = aggregate_transfer(raw)
    summary = aggregate_symbol_summary(ranked)

    print("-" * 90)
    print(f"Raw transfer rows: {len(raw):,}")
    print(f"Ranked rows:       {len(ranked):,}")

    if not ranked.empty:
        print("Transfer status counts:")
        print(ranked.groupby(["test_symbol", "transfer_status"]).size())

    print("-" * 90)

    write_outputs(
        base_symbol=base_symbol,
        symbols=symbols,
        raw=raw,
        ranked=ranked,
        summary=summary,
    )

    print("-" * 90)
    print("[DONE] Cross symbol transfer engine complete")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--base-symbol", default=DEFAULT_BASE_SYMBOL)
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)

    args = parser.parse_args()

    main(
        base_symbol=args.base_symbol,
        symbols=args.symbols,
        top_n=args.top_n,
    )