"""
BACQE TICK RESEARCH - 14 Analyse Volatility Clustering - Multi Symbol

Compares volatility clustering across fixed tick bars and tick imbalance bars.

Outputs:
    Per-symbol:
        E:/Quant_Lab/data/analysis/tick_research/volatility_clustering/symbol=<SYMBOL>/
        E:/Quant_Lab/reports/tick_research/volatility_clustering/symbol=<SYMBOL>/

    Master:
        E:/Quant_Lab/data/analysis/tick_research/volatility_clustering/_master/
        E:/Quant_Lab/reports/tick_research/volatility_clustering/_master/
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

BROKER = "FTMO"

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "EURGBP",
    "GBPJPY",
    "XAUUSD",
]

TICK_SIZES = [100, 250, 500, 1000]
IMBALANCE_THRESHOLDS = [25, 50, 100, 200]
ROLLING_WINDOWS = [10, 25, 50]

TICK_BAR_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_bars"
IMBALANCE_BAR_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_imbalance_bars"

OUTPUT_ANALYSIS_ROOT = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research" / "volatility_clustering"
OUTPUT_REPORT_ROOT = DATA_LAKE_ROOT / "reports" / "tick_research" / "volatility_clustering"

BAR_ORDER = {
    "tick_100": 1,
    "tick_250": 2,
    "tick_500": 3,
    "tick_1000": 4,
    "imbalance_25": 5,
    "imbalance_50": 6,
    "imbalance_100": 7,
    "imbalance_200": 8,
}


def normalise_bar_columns(bars: pd.DataFrame) -> pd.DataFrame:
    bars = bars.copy()

    if "bar_start_time" not in bars.columns and "start_time" in bars.columns:
        bars["bar_start_time"] = bars["start_time"]

    if "bar_end_time" not in bars.columns and "end_time" in bars.columns:
        bars["bar_end_time"] = bars["end_time"]

    if "open" not in bars.columns and "open_mid" in bars.columns:
        bars["open"] = bars["open_mid"]

    if "high" not in bars.columns and "high_mid" in bars.columns:
        bars["high"] = bars["high_mid"]

    if "low" not in bars.columns and "low_mid" in bars.columns:
        bars["low"] = bars["low_mid"]

    if "close" not in bars.columns and "close_mid" in bars.columns:
        bars["close"] = bars["close_mid"]

    if "avg_spread" not in bars.columns and "mean_spread" in bars.columns:
        bars["avg_spread"] = bars["mean_spread"]

    if "range" not in bars.columns and {"high", "low"}.issubset(bars.columns):
        bars["range"] = bars["high"] - bars["low"]

    if "return" not in bars.columns and "close" in bars.columns:
        bars["return"] = bars["close"].pct_change()

    if "log_return" not in bars.columns and "close" in bars.columns:
        bars["log_return"] = np.log(bars["close"] / bars["close"].shift(1))

    if "duration_seconds" not in bars.columns:
        if {"bar_start_time", "bar_end_time"}.issubset(bars.columns):
            start = pd.to_datetime(bars["bar_start_time"], errors="coerce", utc=True)
            end = pd.to_datetime(bars["bar_end_time"], errors="coerce", utc=True)
            bars["duration_seconds"] = (end - start).dt.total_seconds()

    return bars


def safe_autocorr(series: pd.Series, lag: int = 1) -> float | None:
    clean = (
        pd.to_numeric(series, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )

    if len(clean) <= lag + 2:
        return None

    return clean.autocorr(lag=lag)


def load_tick_bars(symbol: str, tick_size: int) -> pd.DataFrame:
    path = (
        TICK_BAR_ROOT
        / f"symbol={symbol}"
        / f"tick_size={tick_size}"
        / f"{symbol}_tick_bars_{tick_size}_latest.parquet"
    )

    if not path.exists():
        print(f"[WARN] {symbol}: tick bar file not found: {path}")
        return pd.DataFrame()

    bars = pd.read_parquet(path)

    bars["symbol"] = symbol
    bars["broker"] = BROKER
    bars["bar_family"] = "fixed_tick"
    bars["bar_type"] = f"tick_{tick_size}"
    bars["bar_parameter"] = str(tick_size)

    return normalise_bar_columns(bars)


def load_imbalance_bars(symbol: str, threshold: int) -> pd.DataFrame:
    path = (
        IMBALANCE_BAR_ROOT
        / f"symbol={symbol}"
        / f"imbalance_threshold={threshold}"
        / f"{symbol}_tick_imbalance_bars_{threshold}_latest.parquet"
    )

    if not path.exists():
        print(f"[WARN] {symbol}: imbalance bar file not found: {path}")
        return pd.DataFrame()

    bars = pd.read_parquet(path)

    bars["symbol"] = symbol
    bars["broker"] = BROKER
    bars["bar_family"] = "tick_imbalance"
    bars["bar_type"] = f"imbalance_{threshold}"
    bars["bar_parameter"] = str(threshold)

    return normalise_bar_columns(bars)


def calculate_clustering_metrics(symbol: str, bars: pd.DataFrame) -> dict:
    bars = bars.copy()

    returns = pd.to_numeric(bars["return"], errors="coerce").replace([np.inf, -np.inf], np.nan)
    abs_returns = returns.abs()
    squared_returns = returns ** 2

    bar_count = len(bars)
    return_std = returns.std()

    metrics = {
        "symbol": symbol,
        "broker": BROKER,
        "bar_family": bars["bar_family"].iloc[0],
        "bar_type": bars["bar_type"].iloc[0],
        "bar_parameter": bars["bar_parameter"].iloc[0],
        "bar_count": bar_count,
        "first_bar_time": bars["bar_start_time"].min() if "bar_start_time" in bars.columns else None,
        "last_bar_time": bars["bar_end_time"].max() if "bar_end_time" in bars.columns else None,
        "return_std": return_std,
        "abs_return_mean": abs_returns.mean(),
        "squared_return_mean": squared_returns.mean(),
        "abs_return_autocorr_lag1": safe_autocorr(abs_returns, lag=1),
        "abs_return_autocorr_lag5": safe_autocorr(abs_returns, lag=5),
        "squared_return_autocorr_lag1": safe_autocorr(squared_returns, lag=1),
        "squared_return_autocorr_lag5": safe_autocorr(squared_returns, lag=5),
        "return_abs_to_std_ratio": (
            abs_returns.mean() / return_std
            if return_std is not None and not pd.isna(return_std) and return_std != 0
            else np.nan
        ),
        "analysis_time_utc": datetime.now(timezone.utc).isoformat(),
    }

    for window in ROLLING_WINDOWS:
        rolling_vol = returns.rolling(window=window).std()
        rolling_abs = abs_returns.rolling(window=window).mean()

        rolling_vol_mean = rolling_vol.mean()
        rolling_abs_mean = rolling_abs.mean()

        metrics[f"rolling_vol_{window}_mean"] = rolling_vol_mean
        metrics[f"rolling_vol_{window}_std"] = rolling_vol.std()
        metrics[f"rolling_vol_{window}_cv"] = (
            rolling_vol.std() / rolling_vol_mean
            if rolling_vol_mean is not None and not pd.isna(rolling_vol_mean) and rolling_vol_mean != 0
            else np.nan
        )

        metrics[f"rolling_abs_{window}_mean"] = rolling_abs_mean
        metrics[f"rolling_abs_{window}_std"] = rolling_abs.std()
        metrics[f"rolling_abs_{window}_cv"] = (
            rolling_abs.std() / rolling_abs_mean
            if rolling_abs_mean is not None and not pd.isna(rolling_abs_mean) and rolling_abs_mean != 0
            else np.nan
        )

        metrics[f"rolling_vol_{window}_autocorr_lag1"] = safe_autocorr(rolling_vol, lag=1)
        metrics[f"rolling_abs_{window}_autocorr_lag1"] = safe_autocorr(rolling_abs, lag=1)

    return metrics


def add_clustering_scores(analysis: pd.DataFrame, rank_scope: str) -> pd.DataFrame:
    scored = analysis.copy()

    score_cols = [
        "abs_return_autocorr_lag1",
        "squared_return_autocorr_lag1",
        "rolling_vol_25_autocorr_lag1",
        "rolling_abs_25_autocorr_lag1",
    ]

    for col in score_cols:
        scored[col] = pd.to_numeric(scored[col], errors="coerce")

    scored["volatility_clustering_score"] = (
        scored["abs_return_autocorr_lag1"].fillna(0) * 0.30
        + scored["squared_return_autocorr_lag1"].fillna(0) * 0.30
        + scored["rolling_vol_25_autocorr_lag1"].fillna(0) * 0.20
        + scored["rolling_abs_25_autocorr_lag1"].fillna(0) * 0.20
    ).round(8)

    scored["clustering_rank"] = (
        scored["volatility_clustering_score"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    scored["rank_scope"] = rank_scope

    scored["sort_order"] = scored["bar_type"].map(BAR_ORDER).fillna(999)

    scored = scored.sort_values(
        ["clustering_rank", "volatility_clustering_score", "sort_order"],
        ascending=[True, False, True],
    ).drop(columns=["sort_order"]).reset_index(drop=True)

    return scored


def build_report(analysis: pd.DataFrame, title: str, input_label: str) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()
    top = analysis.iloc[0]

    display_cols = [
        "clustering_rank",
        "symbol",
        "bar_type",
        "bar_family",
        "bar_count",
        "return_std",
        "abs_return_autocorr_lag1",
        "abs_return_autocorr_lag5",
        "squared_return_autocorr_lag1",
        "squared_return_autocorr_lag5",
        "rolling_vol_25_cv",
        "rolling_vol_25_autocorr_lag1",
        "rolling_abs_25_autocorr_lag1",
        "volatility_clustering_score",
    ]

    available_cols = [col for col in display_cols if col in analysis.columns]

    lines = []
    lines.append("=" * 90)
    lines.append(title)
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input source:     {input_label}")
    lines.append("-" * 90)
    lines.append(f"Strongest clustering symbol:   {top.get('symbol', 'UNKNOWN')}")
    lines.append(f"Strongest clustering bar type: {top['bar_type']}")
    lines.append(f"Clustering score:              {top['volatility_clustering_score']}")
    lines.append("-" * 90)
    lines.append("")
    lines.append("VOLATILITY CLUSTERING RANKING")
    lines.append("-" * 90)
    lines.append(analysis[available_cols].to_string(index=False))
    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("abs_return_autocorr_lag1 measures whether large absolute returns follow large absolute returns.")
    lines.append("squared_return_autocorr_lag1 is a classic volatility clustering proxy.")
    lines.append("rolling_vol_25_autocorr_lag1 measures persistence in rolling volatility.")
    lines.append("rolling_abs_25_autocorr_lag1 measures persistence in rolling absolute movement.")
    lines.append("Higher values suggest stronger volatility clustering.")
    lines.append("This report is diagnostic research, not a trading signal.")
    lines.append("=" * 90)

    return "\n".join(lines)


def save_analysis(
    analysis: pd.DataFrame,
    analysis_dir: Path,
    report_dir: Path,
    file_prefix: str,
    report_title: str,
    input_label: str,
) -> None:
    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    csv_path = analysis_dir / f"{file_prefix}_volatility_clustering_analysis_latest.csv"
    parquet_path = analysis_dir / f"{file_prefix}_volatility_clustering_analysis_latest.parquet"
    report_path = report_dir / f"{file_prefix}_volatility_clustering_report_latest.txt"

    analysis.to_csv(csv_path, index=False)
    analysis.to_parquet(parquet_path, index=False)

    report = build_report(
        analysis=analysis,
        title=report_title,
        input_label=input_label,
    )

    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] Saved analysis: {csv_path}")
    print(f"[DONE] Saved parquet:  {parquet_path}")
    print(f"[DONE] Saved report:   {report_path}")


def process_symbol(symbol: str) -> pd.DataFrame:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    records = []

    for tick_size in TICK_SIZES:
        bars = load_tick_bars(symbol, tick_size)

        if bars.empty:
            continue

        records.append(calculate_clustering_metrics(symbol, bars))
        print(f"[DONE] {symbol}: analysed tick bars {tick_size} | bars={len(bars):,}")

    for threshold in IMBALANCE_THRESHOLDS:
        bars = load_imbalance_bars(symbol, threshold)

        if bars.empty:
            continue

        records.append(calculate_clustering_metrics(symbol, bars))
        print(f"[DONE] {symbol}: analysed imbalance bars {threshold} | bars={len(bars):,}")

    if not records:
        print(f"[WARN] {symbol}: no volatility clustering records created.")
        return pd.DataFrame()

    analysis = pd.DataFrame(records)

    numeric_cols = analysis.select_dtypes(include=["float", "int"]).columns
    analysis[numeric_cols] = analysis[numeric_cols].round(8)

    analysis = add_clustering_scores(
        analysis=analysis,
        rank_scope=f"symbol={symbol}",
    )

    save_analysis(
        analysis=analysis,
        analysis_dir=OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}",
        report_dir=OUTPUT_REPORT_ROOT / f"symbol={symbol}",
        file_prefix=symbol,
        report_title=f"BACQE TICK RESEARCH - VOLATILITY CLUSTERING REPORT - {symbol}",
        input_label=f"Processed tick and imbalance bars for {symbol}",
    )

    display_cols = [
        "clustering_rank",
        "symbol",
        "bar_type",
        "bar_family",
        "bar_count",
        "abs_return_autocorr_lag1",
        "squared_return_autocorr_lag1",
        "rolling_vol_25_autocorr_lag1",
        "rolling_abs_25_autocorr_lag1",
        "volatility_clustering_score",
    ]

    print(analysis[display_cols].to_string(index=False))

    return analysis


def build_winner_summary(symbol_analyses: list[pd.DataFrame]) -> pd.DataFrame:
    winners = []

    for analysis in symbol_analyses:
        if analysis.empty:
            continue

        winners.append(analysis.iloc[0].copy())

    if not winners:
        return pd.DataFrame()

    winner_summary = pd.DataFrame(winners)

    keep_cols = [
        "symbol",
        "bar_type",
        "bar_family",
        "bar_count",
        "return_std",
        "abs_return_autocorr_lag1",
        "squared_return_autocorr_lag1",
        "rolling_vol_25_autocorr_lag1",
        "rolling_abs_25_autocorr_lag1",
        "volatility_clustering_score",
        "rank_scope",
    ]

    available_cols = [col for col in keep_cols if col in winner_summary.columns]

    return winner_summary[available_cols].sort_values(
        "volatility_clustering_score",
        ascending=False,
    ).reset_index(drop=True)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 14 ANALYSE VOLATILITY CLUSTERING - MULTI SYMBOL")
    print("=" * 90)
    print(f"Broker:               {BROKER}")
    print(f"Tick bar root:         {TICK_BAR_ROOT}")
    print(f"Imbalance bar root:    {IMBALANCE_BAR_ROOT}")
    print(f"Output analysis root:  {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output report root:    {OUTPUT_REPORT_ROOT}")
    print(f"Symbols:               {SYMBOLS}")
    print("-" * 90)

    symbol_analyses = []

    for symbol in SYMBOLS:
        analysis = process_symbol(symbol)

        if not analysis.empty:
            symbol_analyses.append(analysis)

    if not symbol_analyses:
        print("[WARN] No volatility clustering analyses created.")
        return

    master_input = pd.concat(symbol_analyses, ignore_index=True)

    master_analysis = add_clustering_scores(
        analysis=master_input,
        rank_scope="master_cross_symbol",
    )

    save_analysis(
        analysis=master_analysis,
        analysis_dir=OUTPUT_ANALYSIS_ROOT / "_master",
        report_dir=OUTPUT_REPORT_ROOT / "_master",
        file_prefix="master",
        report_title="BACQE TICK RESEARCH - VOLATILITY CLUSTERING REPORT - MASTER CROSS-SYMBOL",
        input_label="All processed multi-symbol tick and imbalance bars",
    )

    winner_summary = build_winner_summary(symbol_analyses)

    if not winner_summary.empty:
        winner_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
        winner_report_dir = OUTPUT_REPORT_ROOT / "_master"
        winner_analysis_dir.mkdir(parents=True, exist_ok=True)
        winner_report_dir.mkdir(parents=True, exist_ok=True)

        winner_csv = winner_analysis_dir / "symbol_winners_volatility_clustering_latest.csv"
        winner_parquet = winner_analysis_dir / "symbol_winners_volatility_clustering_latest.parquet"
        winner_txt = winner_report_dir / "symbol_winners_volatility_clustering_latest.txt"

        winner_summary.to_csv(winner_csv, index=False)
        winner_summary.to_parquet(winner_parquet, index=False)

        winner_txt.write_text(
            "\n".join(
                [
                    "=" * 90,
                    "BACQE TICK RESEARCH - SYMBOL WINNERS VOLATILITY CLUSTERING",
                    "=" * 90,
                    f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                    "-" * 90,
                    winner_summary.to_string(index=False),
                    "=" * 90,
                ]
            ),
            encoding="utf-8",
        )

        print("-" * 90)
        print("[DONE] Symbol winner summary created.")
        print(f"CSV:     {winner_csv}")
        print(f"Parquet: {winner_parquet}")
        print(f"Report:  {winner_txt}")

    print("-" * 90)
    print("[COMPLETE] Multi-symbol volatility clustering analysis complete.")
    print(f"Symbols analysed: {len(symbol_analyses)}")
    print(f"Master rows:      {len(master_analysis):,}")
    print("=" * 90)


if __name__ == "__main__":
    main()