"""
BACQE TICK RESEARCH - 13 Analyse Bar Efficiency - Multi Symbol

Compares fixed tick bars and tick imbalance bars using structural efficiency metrics.

Input:
    E:/Quant_Lab/data/analysis/tick_research/tick_vs_imbalance/
        tick_vs_imbalance_bar_comparison_latest.csv

Outputs:
    Per-symbol:
        E:/Quant_Lab/data/analysis/tick_research/bar_efficiency/symbol=<SYMBOL>/
        E:/Quant_Lab/reports/tick_research/bar_efficiency/symbol=<SYMBOL>/

    Master:
        E:/Quant_Lab/data/analysis/tick_research/bar_efficiency/_master/
        E:/Quant_Lab/reports/tick_research/bar_efficiency/_master/
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

INPUT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "tick_vs_imbalance"
    / "tick_vs_imbalance_bar_comparison_latest.csv"
)

OUTPUT_ANALYSIS_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "bar_efficiency"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "bar_efficiency"
)

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "EURGBP",
    "GBPJPY",
    "XAUUSD",
]


def min_max_score(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")

    min_value = values.min()
    max_value = values.max()

    if pd.isna(min_value) or pd.isna(max_value) or min_value == max_value:
        return pd.Series(50.0, index=series.index)

    score = ((values - min_value) / (max_value - min_value)) * 100

    if not higher_is_better:
        score = 100 - score

    return score.round(4)


def build_efficiency_scores(df: pd.DataFrame, rank_scope: str) -> pd.DataFrame:
    analysis = df.copy()

    numeric_cols = [
        "bar_count",
        "avg_duration_seconds",
        "median_duration_seconds",
        "avg_tick_count",
        "median_tick_count",
        "avg_range",
        "return_std",
        "return_kurtosis",
        "lag1_return_autocorr",
        "avg_imbalance_ratio",
        "positive_imbalance_pct",
        "negative_imbalance_pct",
    ]

    for col in numeric_cols:
        if col in analysis.columns:
            analysis[col] = pd.to_numeric(analysis[col], errors="coerce")

    analysis["abs_lag1_autocorr"] = analysis["lag1_return_autocorr"].abs()
    analysis["abs_imbalance_ratio"] = analysis["avg_imbalance_ratio"].abs()

    analysis["range_per_tick"] = analysis["avg_range"] / analysis["avg_tick_count"].replace(0, np.nan)
    analysis["volatility_per_tick"] = analysis["return_std"] / analysis["avg_tick_count"].replace(0, np.nan)
    analysis["range_per_second"] = analysis["avg_range"] / analysis["avg_duration_seconds"].replace(0, np.nan)
    analysis["volatility_per_second"] = analysis["return_std"] / analysis["avg_duration_seconds"].replace(0, np.nan)

    analysis["tail_efficiency_score"] = min_max_score(
        analysis["return_kurtosis"],
        higher_is_better=False,
    )

    analysis["noise_reduction_score"] = min_max_score(
        analysis["abs_lag1_autocorr"],
        higher_is_better=False,
    )

    analysis["information_density_score"] = min_max_score(
        analysis["range_per_tick"],
        higher_is_better=True,
    )

    analysis["volatility_density_score"] = min_max_score(
        analysis["volatility_per_tick"],
        higher_is_better=True,
    )

    analysis["sample_size_score"] = min_max_score(
        analysis["bar_count"],
        higher_is_better=True,
    )

    analysis["stability_score"] = min_max_score(
        analysis["return_std"],
        higher_is_better=False,
    )

    analysis["structural_efficiency_score"] = (
        analysis["tail_efficiency_score"] * 0.25
        + analysis["noise_reduction_score"] * 0.20
        + analysis["information_density_score"] * 0.20
        + analysis["volatility_density_score"] * 0.15
        + analysis["sample_size_score"] * 0.10
        + analysis["stability_score"] * 0.10
    ).round(4)

    analysis["efficiency_rank"] = (
        analysis["structural_efficiency_score"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    analysis["rank_scope"] = rank_scope
    analysis["analysis_time_utc"] = datetime.now(timezone.utc).isoformat()

    analysis = analysis.sort_values(
        ["efficiency_rank", "structural_efficiency_score"],
        ascending=[True, False],
    ).reset_index(drop=True)

    return analysis


def build_report(analysis: pd.DataFrame, title: str, input_path: Path) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()
    top = analysis.iloc[0]

    display_cols = [
        "efficiency_rank",
        "symbol",
        "bar_type",
        "bar_family",
        "bar_count",
        "avg_tick_count",
        "avg_duration_seconds",
        "return_std",
        "return_kurtosis",
        "lag1_return_autocorr",
        "range_per_tick",
        "volatility_per_tick",
        "tail_efficiency_score",
        "noise_reduction_score",
        "information_density_score",
        "volatility_density_score",
        "sample_size_score",
        "stability_score",
        "structural_efficiency_score",
    ]

    available_cols = [col for col in display_cols if col in analysis.columns]

    lines = []
    lines.append("=" * 90)
    lines.append(title)
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input file:       {input_path}")
    lines.append("-" * 90)
    lines.append(f"Best ranked symbol:   {top.get('symbol', 'UNKNOWN')}")
    lines.append(f"Best ranked bar type: {top['bar_type']}")
    lines.append(f"Best score:           {top['structural_efficiency_score']}")
    lines.append("-" * 90)
    lines.append("")
    lines.append("EFFICIENCY RANKING")
    lines.append("-" * 90)
    lines.append(analysis[available_cols].to_string(index=False))
    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("tail_efficiency_score rewards lower kurtosis.")
    lines.append("noise_reduction_score rewards lower absolute lag-1 autocorrelation.")
    lines.append("information_density_score rewards more price range per tick.")
    lines.append("volatility_density_score rewards more return movement per tick.")
    lines.append("sample_size_score rewards more observations.")
    lines.append("stability_score rewards lower return volatility.")
    lines.append("")
    lines.append("This is a v1 research score, not a trading signal.")
    lines.append("The score ranks sampling methods for structural research quality.")
    lines.append("=" * 90)

    return "\n".join(lines)


def save_analysis(
    analysis: pd.DataFrame,
    analysis_dir: Path,
    report_dir: Path,
    file_prefix: str,
    report_title: str,
) -> None:
    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    csv_path = analysis_dir / f"{file_prefix}_bar_efficiency_analysis_latest.csv"
    parquet_path = analysis_dir / f"{file_prefix}_bar_efficiency_analysis_latest.parquet"
    report_path = report_dir / f"{file_prefix}_bar_efficiency_report_latest.txt"

    analysis.to_csv(csv_path, index=False)
    analysis.to_parquet(parquet_path, index=False)

    report = build_report(
        analysis=analysis,
        title=report_title,
        input_path=INPUT_PATH,
    )

    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] Saved analysis: {csv_path}")
    print(f"[DONE] Saved parquet:  {parquet_path}")
    print(f"[DONE] Saved report:   {report_path}")


def process_symbol(symbol: str, df: pd.DataFrame) -> pd.DataFrame:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    symbol_df = df[df["symbol"] == symbol].copy()

    if symbol_df.empty:
        print(f"[WARN] No rows found for {symbol}. Skipping.")
        return pd.DataFrame()

    analysis = build_efficiency_scores(
        symbol_df,
        rank_scope=f"symbol={symbol}",
    )

    save_analysis(
        analysis=analysis,
        analysis_dir=OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}",
        report_dir=OUTPUT_REPORT_ROOT / f"symbol={symbol}",
        file_prefix=symbol,
        report_title=f"BACQE TICK RESEARCH - BAR EFFICIENCY REPORT - {symbol}",
    )

    display_cols = [
        "efficiency_rank",
        "symbol",
        "bar_type",
        "bar_family",
        "bar_count",
        "return_kurtosis",
        "lag1_return_autocorr",
        "range_per_tick",
        "volatility_per_tick",
        "structural_efficiency_score",
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
        "avg_tick_count",
        "avg_duration_seconds",
        "return_std",
        "return_kurtosis",
        "lag1_return_autocorr",
        "structural_efficiency_score",
        "rank_scope",
    ]

    available_cols = [col for col in keep_cols if col in winner_summary.columns]

    return winner_summary[available_cols].sort_values(
        "structural_efficiency_score",
        ascending=False,
    ).reset_index(drop=True)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 13 ANALYSE BAR EFFICIENCY - MULTI SYMBOL")
    print("=" * 90)
    print(f"Input:                {INPUT_PATH}")
    print(f"Output analysis root: {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output report root:   {OUTPUT_REPORT_ROOT}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input comparison file not found: {INPUT_PATH}")

    df = pd.read_csv(INPUT_PATH, low_memory=False)

    if "symbol" not in df.columns:
        raise ValueError("Input comparison file does not contain a 'symbol' column.")

    print(f"Rows loaded: {len(df):,}")
    print(f"Symbols:     {sorted(df['symbol'].dropna().unique().tolist())}")

    symbol_analyses = []

    for symbol in SYMBOLS:
        analysis = process_symbol(symbol, df)

        if not analysis.empty:
            symbol_analyses.append(analysis)

    if not symbol_analyses:
        print("[WARN] No symbol efficiency analyses created.")
        return

    master_input = pd.concat(symbol_analyses, ignore_index=True)

    master_analysis = build_efficiency_scores(
        df,
        rank_scope="master_cross_symbol",
    )

    save_analysis(
        analysis=master_analysis,
        analysis_dir=OUTPUT_ANALYSIS_ROOT / "_master",
        report_dir=OUTPUT_REPORT_ROOT / "_master",
        file_prefix="master",
        report_title="BACQE TICK RESEARCH - BAR EFFICIENCY REPORT - MASTER CROSS-SYMBOL",
    )

    winner_summary = build_winner_summary(symbol_analyses)

    if not winner_summary.empty:
        winner_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
        winner_report_dir = OUTPUT_REPORT_ROOT / "_master"
        winner_analysis_dir.mkdir(parents=True, exist_ok=True)
        winner_report_dir.mkdir(parents=True, exist_ok=True)

        winner_csv = winner_analysis_dir / "symbol_winners_bar_efficiency_latest.csv"
        winner_parquet = winner_analysis_dir / "symbol_winners_bar_efficiency_latest.parquet"
        winner_txt = winner_report_dir / "symbol_winners_bar_efficiency_latest.txt"

        winner_summary.to_csv(winner_csv, index=False)
        winner_summary.to_parquet(winner_parquet, index=False)
        winner_txt.write_text(
            "\n".join(
                [
                    "=" * 90,
                    "BACQE TICK RESEARCH - SYMBOL WINNERS BAR EFFICIENCY",
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
    print("[COMPLETE] Multi-symbol bar efficiency analysis complete.")
    print(f"Symbols analysed: {len(symbol_analyses)}")
    print(f"Master rows:      {len(master_analysis):,}")
    print("=" * 90)


if __name__ == "__main__":
    main()