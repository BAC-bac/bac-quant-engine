"""
BACQE TICK RESEARCH - 17 Compare Bar Predictability - Multi Symbol

Tests simple next-bar predictability across microstructure regimes.

Input:
    E:/Quant_Lab/data/processed/tick_research/microstructure_regimes/symbol=<SYMBOL>/

Outputs:
    Per-symbol:
        E:/Quant_Lab/data/analysis/tick_research/bar_predictability/symbol=<SYMBOL>/
        E:/Quant_Lab/reports/tick_research/bar_predictability/symbol=<SYMBOL>/

    Master:
        E:/Quant_Lab/data/analysis/tick_research/bar_predictability/_master/
        E:/Quant_Lab/reports/tick_research/bar_predictability/_master/
"""

from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "EURGBP",
    "GBPJPY",
    "XAUUSD",
]

INPUT_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "microstructure_regimes"
)

OUTPUT_ANALYSIS_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "bar_predictability"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "bar_predictability"
)

MIN_OBSERVATIONS = 30


def add_forward_labels(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    data["bar_start_time"] = pd.to_datetime(
        data["bar_start_time"],
        errors="coerce",
        utc=True,
    )

    data = data.sort_values(
        ["symbol", "bar_type", "bar_start_time"]
    ).reset_index(drop=True)

    data["next_return"] = data.groupby(
        ["symbol", "bar_type"]
    )["return"].shift(-1)

    data["next_abs_return"] = data["next_return"].abs()

    data["next_direction"] = 0
    data.loc[data["next_return"] > 0, "next_direction"] = 1
    data.loc[data["next_return"] < 0, "next_direction"] = -1

    if "direction" not in data.columns:
        data["direction"] = 0
        data.loc[data["return"] > 0, "direction"] = 1
        data.loc[data["return"] < 0, "direction"] = -1

    data["current_direction"] = (
        pd.to_numeric(data["direction"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    data["direction_persisted"] = (
        (data["current_direction"] != 0)
        & (data["next_direction"] == data["current_direction"])
    )

    data["direction_flipped"] = (
        (data["current_direction"] != 0)
        & (data["next_direction"] == -data["current_direction"])
    )

    data["next_positive"] = data["next_return"] > 0
    data["next_negative"] = data["next_return"] < 0

    return data


def summarise_predictability(labelled: pd.DataFrame) -> pd.DataFrame:
    clean = labelled.dropna(subset=["next_return"]).copy()

    group_cols = [
        "symbol",
        "bar_type",
        "bar_family",
        "microstructure_regime",
    ]

    summary = (
        clean.groupby(group_cols, dropna=False)
        .agg(
            observations=("next_return", "count"),
            current_avg_return=("return", "mean"),
            next_avg_return=("next_return", "mean"),
            next_median_return=("next_return", "median"),
            next_avg_abs_return=("next_abs_return", "mean"),
            next_return_std=("next_return", "std"),
            next_positive_pct=("next_positive", "mean"),
            next_negative_pct=("next_negative", "mean"),
            direction_persistence_pct=("direction_persisted", "mean"),
            direction_flip_pct=("direction_flipped", "mean"),
            avg_current_range=("range", "mean"),
            avg_current_duration_seconds=("duration_seconds", "mean"),
            avg_current_tick_count=("tick_count", "mean"),
        )
        .reset_index()
    )

    summary["next_positive_pct"] *= 100
    summary["next_negative_pct"] *= 100
    summary["direction_persistence_pct"] *= 100
    summary["direction_flip_pct"] *= 100

    summary["edge_proxy"] = (
        summary["next_avg_return"]
        / summary["next_return_std"].replace(0, np.nan)
    )

    summary["activity_adjusted_abs_return"] = (
        summary["next_avg_abs_return"]
        / summary["avg_current_duration_seconds"].replace(0, np.nan)
    )

    summary["sample_quality"] = "low_sample"
    summary.loc[summary["observations"] >= MIN_OBSERVATIONS, "sample_quality"] = "usable"
    summary.loc[summary["observations"] >= 100, "sample_quality"] = "stronger"

    summary["abs_edge_proxy"] = summary["edge_proxy"].abs()

    summary["predictability_score"] = (
        summary["abs_edge_proxy"].fillna(0) * 0.45
        + (summary["direction_persistence_pct"].fillna(0) / 100) * 0.25
        + (summary["activity_adjusted_abs_return"].fillna(0)) * 0.30
    )

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["analysis_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary.sort_values(
        ["symbol", "bar_type", "sample_quality", "observations"],
        ascending=[True, True, True, False],
    ).reset_index(drop=True)


def build_bar_level_summary(labelled: pd.DataFrame) -> pd.DataFrame:
    clean = labelled.dropna(subset=["next_return"]).copy()

    summary = (
        clean.groupby(["symbol", "bar_type", "bar_family"], dropna=False)
        .agg(
            observations=("next_return", "count"),
            next_avg_return=("next_return", "mean"),
            next_avg_abs_return=("next_abs_return", "mean"),
            next_return_std=("next_return", "std"),
            next_positive_pct=("next_positive", "mean"),
            direction_persistence_pct=("direction_persisted", "mean"),
            direction_flip_pct=("direction_flipped", "mean"),
        )
        .reset_index()
    )

    summary["next_positive_pct"] *= 100
    summary["direction_persistence_pct"] *= 100
    summary["direction_flip_pct"] *= 100

    summary["edge_proxy"] = (
        summary["next_avg_return"]
        / summary["next_return_std"].replace(0, np.nan)
    )

    summary["abs_edge_proxy"] = summary["edge_proxy"].abs()

    summary["predictability_score"] = (
        summary["abs_edge_proxy"].fillna(0) * 0.60
        + (summary["direction_persistence_pct"].fillna(0) / 100) * 0.40
    )

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    return summary.sort_values(
        ["symbol", "predictability_score"],
        ascending=[True, False],
    ).reset_index(drop=True)


def build_report(
    symbol: str,
    regime_summary: pd.DataFrame,
    bar_summary: pd.DataFrame,
    input_path: Path,
) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    usable = regime_summary[
        regime_summary["observations"] >= MIN_OBSERVATIONS
    ].copy()

    usable = usable.sort_values(
        "predictability_score",
        ascending=False,
        na_position="last",
    )

    display_cols = [
        "symbol",
        "bar_type",
        "bar_family",
        "microstructure_regime",
        "observations",
        "sample_quality",
        "next_avg_return",
        "next_avg_abs_return",
        "next_positive_pct",
        "direction_persistence_pct",
        "direction_flip_pct",
        "edge_proxy",
        "predictability_score",
    ]

    bar_cols = [
        "symbol",
        "bar_type",
        "bar_family",
        "observations",
        "next_avg_return",
        "next_avg_abs_return",
        "next_positive_pct",
        "direction_persistence_pct",
        "direction_flip_pct",
        "edge_proxy",
        "predictability_score",
    ]

    lines = []

    lines.append("=" * 90)
    lines.append(f"BACQE TICK RESEARCH - BAR PREDICTABILITY REPORT - {symbol}")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {input_path}")
    lines.append(f"Minimum obs:     {MIN_OBSERVATIONS}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("BAR-LEVEL PREDICTABILITY")
    lines.append("-" * 90)
    lines.append(bar_summary[bar_cols].to_string(index=False))

    lines.append("")
    lines.append("REGIME-LEVEL PREDICTABILITY - USABLE SAMPLES")
    lines.append("-" * 90)

    if usable.empty:
        lines.append("No regimes met the minimum observation threshold.")
    else:
        lines.append(usable[display_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This is diagnostic research, not a trading signal.")
    lines.append("edge_proxy is next_avg_return divided by next_return_std.")
    lines.append("predictability_score combines absolute edge proxy, direction persistence, and activity-adjusted movement.")
    lines.append("Positive edge_proxy suggests positive next-bar drift; negative edge_proxy suggests negative next-bar drift.")
    lines.append("Small datasets can produce unstable results; treat these as hypotheses.")
    lines.append("=" * 90)

    return "\n".join(lines)


def save_symbol_outputs(
    symbol: str,
    labelled: pd.DataFrame,
    regime_summary: pd.DataFrame,
    bar_summary: pd.DataFrame,
    input_path: Path,
) -> None:
    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    labelled_path = analysis_dir / f"{symbol}_bar_predictability_labelled_latest.parquet"
    regime_csv = analysis_dir / f"{symbol}_bar_predictability_regime_level_latest.csv"
    regime_parquet = analysis_dir / f"{symbol}_bar_predictability_regime_level_latest.parquet"
    bar_csv = analysis_dir / f"{symbol}_bar_predictability_bar_level_latest.csv"
    bar_parquet = analysis_dir / f"{symbol}_bar_predictability_bar_level_latest.parquet"
    report_path = report_dir / f"{symbol}_bar_predictability_report_latest.txt"

    labelled.to_parquet(labelled_path, index=False)
    regime_summary.to_csv(regime_csv, index=False)
    regime_summary.to_parquet(regime_parquet, index=False)
    bar_summary.to_csv(bar_csv, index=False)
    bar_summary.to_parquet(bar_parquet, index=False)

    report = build_report(
        symbol=symbol,
        regime_summary=regime_summary,
        bar_summary=bar_summary,
        input_path=input_path,
    )

    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: regime summary CSV: {regime_csv}")
    print(f"[DONE] {symbol}: bar summary CSV:    {bar_csv}")
    print(f"[DONE] {symbol}: report:             {report_path}")


def process_symbol(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    input_path = (
        INPUT_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_regimes_latest.parquet"
    )

    if not input_path.exists():
        print(f"[WARN] {symbol}: microstructure regimes file not found: {input_path}")
        return pd.DataFrame(), pd.DataFrame()

    regimes = pd.read_parquet(input_path)

    print(f"[INFO] {symbol}: rows loaded: {len(regimes):,}")

    labelled = add_forward_labels(regimes)
    regime_summary = summarise_predictability(labelled)
    bar_summary = build_bar_level_summary(labelled)

    save_symbol_outputs(
        symbol=symbol,
        labelled=labelled,
        regime_summary=regime_summary,
        bar_summary=bar_summary,
        input_path=input_path,
    )

    display_cols = [
        "symbol",
        "bar_type",
        "bar_family",
        "observations",
        "next_avg_return",
        "next_avg_abs_return",
        "next_positive_pct",
        "direction_persistence_pct",
        "direction_flip_pct",
        "edge_proxy",
        "predictability_score",
    ]

    print("BAR-LEVEL SUMMARY")
    print(bar_summary[display_cols].to_string(index=False))

    return regime_summary, bar_summary


def save_master_outputs(
    all_regime_summaries: list[pd.DataFrame],
    all_bar_summaries: list[pd.DataFrame],
) -> None:
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_regime = pd.concat(all_regime_summaries, ignore_index=True)
    master_bar = pd.concat(all_bar_summaries, ignore_index=True)

    master_regime_csv = master_analysis_dir / "master_bar_predictability_regime_level_latest.csv"
    master_regime_parquet = master_analysis_dir / "master_bar_predictability_regime_level_latest.parquet"
    master_bar_csv = master_analysis_dir / "master_bar_predictability_bar_level_latest.csv"
    master_bar_parquet = master_analysis_dir / "master_bar_predictability_bar_level_latest.parquet"

    master_regime.to_csv(master_regime_csv, index=False)
    master_regime.to_parquet(master_regime_parquet, index=False)

    master_bar.to_csv(master_bar_csv, index=False)
    master_bar.to_parquet(master_bar_parquet, index=False)

    winner_summary = (
        master_bar
        .sort_values(["symbol", "predictability_score"], ascending=[True, False])
        .groupby("symbol", as_index=False)
        .head(1)
        .sort_values("predictability_score", ascending=False)
        .reset_index(drop=True)
    )

    winner_csv = master_analysis_dir / "symbol_winners_bar_predictability_latest.csv"
    winner_parquet = master_analysis_dir / "symbol_winners_bar_predictability_latest.parquet"
    winner_report = master_report_dir / "symbol_winners_bar_predictability_latest.txt"

    winner_summary.to_csv(winner_csv, index=False)
    winner_summary.to_parquet(winner_parquet, index=False)

    winner_report.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - SYMBOL WINNERS BAR PREDICTABILITY",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                "-" * 90,
                winner_summary.to_string(index=False),
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    master_report_path = master_report_dir / "master_bar_predictability_report_latest.txt"

    top_regimes = (
        master_regime[master_regime["observations"] >= MIN_OBSERVATIONS]
        .sort_values("predictability_score", ascending=False)
        .head(30)
    )

    master_report_path.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - MASTER BAR PREDICTABILITY REPORT",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                f"Minimum obs:     {MIN_OBSERVATIONS}",
                "-" * 90,
                "",
                "SYMBOL WINNERS",
                "-" * 90,
                winner_summary.to_string(index=False),
                "",
                "TOP REGIME-LEVEL PREDICTABILITY ROWS",
                "-" * 90,
                top_regimes.to_string(index=False),
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    print("-" * 90)
    print("[DONE] Master predictability outputs created.")
    print(f"Master regime CSV: {master_regime_csv}")
    print(f"Master bar CSV:    {master_bar_csv}")
    print(f"Winner CSV:        {winner_csv}")
    print(f"Master report:     {master_report_path}")


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 17 COMPARE BAR PREDICTABILITY - MULTI SYMBOL")
    print("=" * 90)
    print(f"Input root:           {INPUT_ROOT}")
    print(f"Output analysis root: {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output report root:   {OUTPUT_REPORT_ROOT}")
    print(f"Minimum obs:          {MIN_OBSERVATIONS}")
    print(f"Symbols:              {SYMBOLS}")
    print("-" * 90)

    all_regime_summaries = []
    all_bar_summaries = []

    for symbol in SYMBOLS:
        regime_summary, bar_summary = process_symbol(symbol)

        if not regime_summary.empty:
            all_regime_summaries.append(regime_summary)

        if not bar_summary.empty:
            all_bar_summaries.append(bar_summary)

    if not all_regime_summaries or not all_bar_summaries:
        print("[WARN] No predictability summaries created.")
        return

    save_master_outputs(
        all_regime_summaries=all_regime_summaries,
        all_bar_summaries=all_bar_summaries,
    )

    print("-" * 90)
    print("[COMPLETE] Multi-symbol bar predictability analysis complete.")
    print(f"Symbols analysed: {len(all_bar_summaries)}")
    print("=" * 90)


if __name__ == "__main__":
    main()