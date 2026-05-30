"""
BACQE TICK RESEARCH - 19 Measure Directional Persistence - Multi Symbol

Measures microstructure momentum / directional persistence across bar types,
microstructure regimes, and sessions.

Inputs:
    E:/Quant_Lab/data/processed/tick_research/microstructure_regimes/symbol=<SYMBOL>/

Outputs:
    Per-symbol:
        E:/Quant_Lab/data/analysis/tick_research/directional_persistence/symbol=<SYMBOL>/
        E:/Quant_Lab/reports/tick_research/directional_persistence/symbol=<SYMBOL>/

    Master:
        E:/Quant_Lab/data/analysis/tick_research/directional_persistence/_master/
        E:/Quant_Lab/reports/tick_research/directional_persistence/_master/
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
    / "directional_persistence"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "directional_persistence"
)

HORIZONS = [1, 2, 3, 5]
MIN_OBSERVATIONS = 30


def classify_session(hour_utc: int) -> str:
    if 0 <= hour_utc < 7:
        return "asia_overnight"
    if 7 <= hour_utc < 12:
        return "london_morning"
    if 12 <= hour_utc < 16:
        return "london_new_york_overlap"
    if 16 <= hour_utc < 21:
        return "new_york_afternoon"
    return "late_us_rollover"


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    data["bar_start_time"] = pd.to_datetime(
        data["bar_start_time"],
        errors="coerce",
        utc=True,
    )

    data = data.dropna(subset=["bar_start_time"]).copy()

    if "symbol" not in data.columns:
        raise ValueError("Input data is missing required 'symbol' column.")

    data["hour_utc"] = data["bar_start_time"].dt.hour
    data["session_utc"] = data["hour_utc"].apply(classify_session)

    data["return"] = pd.to_numeric(data["return"], errors="coerce")

    data["direction"] = 0
    data.loc[data["return"] > 0, "direction"] = 1
    data.loc[data["return"] < 0, "direction"] = -1

    data = data.sort_values(
        ["symbol", "bar_type", "bar_start_time"]
    ).reset_index(drop=True)

    return data


def add_forward_direction_labels(data: pd.DataFrame) -> pd.DataFrame:
    labelled = data.copy()

    for horizon in HORIZONS:
        labelled[f"future_return_h{horizon}"] = (
            labelled.groupby(["symbol", "bar_type"])["return"].shift(-horizon)
        )

        labelled[f"future_direction_h{horizon}"] = 0
        labelled.loc[
            labelled[f"future_return_h{horizon}"] > 0,
            f"future_direction_h{horizon}",
        ] = 1
        labelled.loc[
            labelled[f"future_return_h{horizon}"] < 0,
            f"future_direction_h{horizon}",
        ] = -1

        labelled[f"persist_h{horizon}"] = (
            (labelled["direction"] != 0)
            & (labelled[f"future_direction_h{horizon}"] == labelled["direction"])
        )

        labelled[f"flip_h{horizon}"] = (
            (labelled["direction"] != 0)
            & (labelled[f"future_direction_h{horizon}"] == -labelled["direction"])
        )

        labelled[f"future_abs_return_h{horizon}"] = (
            labelled[f"future_return_h{horizon}"].abs()
        )

    return labelled


def summarise_group(
    labelled: pd.DataFrame,
    group_cols: list[str],
    level_name: str,
) -> pd.DataFrame:
    records = []

    grouped = labelled.groupby(group_cols, dropna=False)

    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)

        base = {
            "summary_level": level_name,
            "observations_total": len(group),
            "analysis_time_utc": datetime.now(timezone.utc).isoformat(),
        }

        for col, value in zip(group_cols, keys):
            base[col] = value

        current_directional = group[group["direction"] != 0].copy()
        base["directional_observations"] = len(current_directional)

        for horizon in HORIZONS:
            valid = current_directional.dropna(
                subset=[f"future_return_h{horizon}"]
            ).copy()

            obs = len(valid)
            base[f"obs_h{horizon}"] = obs

            if obs == 0:
                base[f"persist_pct_h{horizon}"] = np.nan
                base[f"flip_pct_h{horizon}"] = np.nan
                base[f"future_avg_return_h{horizon}"] = np.nan
                base[f"future_avg_abs_return_h{horizon}"] = np.nan
                base[f"persistence_edge_h{horizon}"] = np.nan
                continue

            base[f"persist_pct_h{horizon}"] = valid[f"persist_h{horizon}"].mean() * 100
            base[f"flip_pct_h{horizon}"] = valid[f"flip_h{horizon}"].mean() * 100
            base[f"future_avg_return_h{horizon}"] = valid[f"future_return_h{horizon}"].mean()
            base[f"future_avg_abs_return_h{horizon}"] = valid[
                f"future_abs_return_h{horizon}"
            ].mean()

            base[f"persistence_edge_h{horizon}"] = (
                base[f"persist_pct_h{horizon}"]
                - base[f"flip_pct_h{horizon}"]
            )

        records.append(base)

    summary = pd.DataFrame(records)

    for horizon in HORIZONS:
        summary[f"sample_quality_h{horizon}"] = "low_sample"
        summary.loc[
            summary[f"obs_h{horizon}"] >= MIN_OBSERVATIONS,
            f"sample_quality_h{horizon}",
        ] = "usable"
        summary.loc[
            summary[f"obs_h{horizon}"] >= 100,
            f"sample_quality_h{horizon}",
        ] = "stronger"

    summary["persistence_score"] = (
        summary["persistence_edge_h1"].fillna(0) * 0.40
        + summary["persistence_edge_h2"].fillna(0) * 0.30
        + summary["persistence_edge_h3"].fillna(0) * 0.20
        + summary["persistence_edge_h5"].fillna(0) * 0.10
    ).round(8)

    summary["abs_persistence_score"] = summary["persistence_score"].abs().round(8)

    summary["persistence_bias"] = "neutral"
    summary.loc[summary["persistence_score"] > 0, "persistence_bias"] = "momentum"
    summary.loc[summary["persistence_score"] < 0, "persistence_bias"] = "mean_reversion"

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    return summary


def build_report(symbol: str, summary: pd.DataFrame, input_path: Path) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    bar_level = summary[summary["summary_level"] == "bar_type"].copy()

    bar_display_cols = [
        "symbol",
        "bar_type",
        "bar_family",
        "directional_observations",
        "persist_pct_h1",
        "flip_pct_h1",
        "persistence_edge_h1",
        "persist_pct_h2",
        "flip_pct_h2",
        "persistence_edge_h2",
        "persist_pct_h3",
        "persistence_edge_h3",
        "persist_pct_h5",
        "persistence_edge_h5",
        "persistence_score",
        "persistence_bias",
    ]

    available_bar_cols = [
        col for col in bar_display_cols if col in bar_level.columns
    ]

    strongest = summary.copy()

    if "abs_persistence_score" in strongest.columns:
        strongest = strongest.sort_values(
            "abs_persistence_score",
            ascending=False,
            na_position="last",
        )

    strongest_cols = [
        "symbol",
        "summary_level",
        "bar_type",
        "bar_family",
        "microstructure_regime",
        "session_utc",
        "directional_observations",
        "persist_pct_h1",
        "flip_pct_h1",
        "persistence_edge_h1",
        "persistence_score",
        "persistence_bias",
        "sample_quality_h1",
    ]

    available_strong_cols = [
        col for col in strongest_cols if col in strongest.columns
    ]

    lines = []
    lines.append("=" * 90)
    lines.append(f"BACQE TICK RESEARCH - DIRECTIONAL PERSISTENCE REPORT - {symbol}")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {input_path}")
    lines.append(f"Horizons:        {HORIZONS}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("BAR-LEVEL DIRECTIONAL PERSISTENCE")
    lines.append("-" * 90)

    if bar_level.empty:
        lines.append("No bar-level rows found.")
    else:
        lines.append(bar_level[available_bar_cols].to_string(index=False))

    lines.append("")
    lines.append("STRONGEST ABSOLUTE PERSISTENCE SCORES")
    lines.append("-" * 90)
    lines.append(strongest[available_strong_cols].head(30).to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("persist_pct measures whether current direction continues after N bars.")
    lines.append("flip_pct measures whether current direction reverses after N bars.")
    lines.append("persistence_edge = persist_pct - flip_pct.")
    lines.append("Positive persistence_edge suggests microstructure momentum.")
    lines.append("Negative persistence_edge suggests mean-reversion / reversal pressure.")
    lines.append("persistence_score is weighted: 40% H1, 30% H2, 20% H3, 10% H5.")
    lines.append("Small samples can be misleading; use sample_quality fields.")
    lines.append("This is diagnostic research, not a trading signal.")
    lines.append("=" * 90)

    return "\n".join(lines)


def save_symbol_outputs(
    symbol: str,
    labelled: pd.DataFrame,
    summary: pd.DataFrame,
    input_path: Path,
) -> None:
    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    labelled_path = analysis_dir / f"{symbol}_directional_persistence_labelled_latest.parquet"
    summary_csv = analysis_dir / f"{symbol}_directional_persistence_latest.csv"
    summary_parquet = analysis_dir / f"{symbol}_directional_persistence_latest.parquet"
    report_path = report_dir / f"{symbol}_directional_persistence_report_latest.txt"

    labelled.to_parquet(labelled_path, index=False)
    summary.to_csv(summary_csv, index=False)
    summary.to_parquet(summary_parquet, index=False)

    report = build_report(
        symbol=symbol,
        summary=summary,
        input_path=input_path,
    )

    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: summary CSV: {summary_csv}")
    print(f"[DONE] {symbol}: report:      {report_path}")


def process_symbol(symbol: str) -> pd.DataFrame:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    input_path = (
        INPUT_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_regimes_latest.parquet"
    )

    if not input_path.exists():
        print(f"[WARN] {symbol}: microstructure regimes file not found: {input_path}")
        return pd.DataFrame()

    regimes = pd.read_parquet(input_path)

    print(f"[INFO] {symbol}: rows loaded: {len(regimes):,}")

    data = prepare_data(regimes)
    labelled = add_forward_direction_labels(data)

    summaries = []

    summaries.append(
        summarise_group(
            labelled,
            group_cols=["symbol", "bar_type", "bar_family"],
            level_name="bar_type",
        )
    )

    summaries.append(
        summarise_group(
            labelled,
            group_cols=[
                "symbol",
                "bar_type",
                "bar_family",
                "microstructure_regime",
            ],
            level_name="bar_type_regime",
        )
    )

    summaries.append(
        summarise_group(
            labelled,
            group_cols=[
                "symbol",
                "bar_type",
                "bar_family",
                "session_utc",
            ],
            level_name="bar_type_session",
        )
    )

    summaries.append(
        summarise_group(
            labelled,
            group_cols=[
                "symbol",
                "bar_type",
                "bar_family",
                "microstructure_regime",
                "session_utc",
            ],
            level_name="bar_type_regime_session",
        )
    )

    summary = pd.concat(summaries, ignore_index=True)

    save_symbol_outputs(
        symbol=symbol,
        labelled=labelled,
        summary=summary,
        input_path=input_path,
    )

    display_cols = [
        "summary_level",
        "symbol",
        "bar_type",
        "bar_family",
        "directional_observations",
        "persist_pct_h1",
        "flip_pct_h1",
        "persistence_edge_h1",
        "persist_pct_h2",
        "persistence_edge_h2",
        "persist_pct_h3",
        "persistence_edge_h3",
        "persist_pct_h5",
        "persistence_edge_h5",
        "persistence_score",
        "persistence_bias",
    ]

    available_display_cols = [col for col in display_cols if col in summary.columns]

    print(
        summary[summary["summary_level"] == "bar_type"][available_display_cols]
        .to_string(index=False)
    )

    return summary


def save_master_outputs(all_summaries: list[pd.DataFrame]) -> None:
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_summary = pd.concat(all_summaries, ignore_index=True)

    master_csv = master_analysis_dir / "master_directional_persistence_latest.csv"
    master_parquet = master_analysis_dir / "master_directional_persistence_latest.parquet"

    master_summary.to_csv(master_csv, index=False)
    master_summary.to_parquet(master_parquet, index=False)

    bar_level = master_summary[master_summary["summary_level"] == "bar_type"].copy()

    symbol_winners = (
        bar_level
        .sort_values(
            ["symbol", "abs_persistence_score"],
            ascending=[True, False],
        )
        .groupby("symbol", as_index=False)
        .head(1)
        .sort_values("abs_persistence_score", ascending=False)
        .reset_index(drop=True)
    )

    winner_csv = master_analysis_dir / "symbol_winners_directional_persistence_latest.csv"
    winner_parquet = master_analysis_dir / "symbol_winners_directional_persistence_latest.parquet"
    winner_report = master_report_dir / "symbol_winners_directional_persistence_latest.txt"

    symbol_winners.to_csv(winner_csv, index=False)
    symbol_winners.to_parquet(winner_parquet, index=False)

    winner_report.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - SYMBOL WINNERS DIRECTIONAL PERSISTENCE",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                "-" * 90,
                symbol_winners.to_string(index=False),
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    top_rows = (
        master_summary
        .sort_values("abs_persistence_score", ascending=False)
        .head(50)
    )

    master_report_path = master_report_dir / "master_directional_persistence_report_latest.txt"

    master_report_path.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - MASTER DIRECTIONAL PERSISTENCE REPORT",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                f"Horizons:        {HORIZONS}",
                "-" * 90,
                "",
                "SYMBOL WINNERS",
                "-" * 90,
                symbol_winners.to_string(index=False),
                "",
                "TOP ABSOLUTE PERSISTENCE ROWS",
                "-" * 90,
                top_rows.to_string(index=False),
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    print("-" * 90)
    print("[DONE] Master directional persistence outputs created.")
    print(f"Master CSV:    {master_csv}")
    print(f"Winner CSV:    {winner_csv}")
    print(f"Master report: {master_report_path}")


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 19 MEASURE DIRECTIONAL PERSISTENCE - MULTI SYMBOL")
    print("=" * 90)
    print(f"Input root:           {INPUT_ROOT}")
    print(f"Output analysis root: {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output report root:   {OUTPUT_REPORT_ROOT}")
    print(f"Horizons:             {HORIZONS}")
    print(f"Minimum obs:          {MIN_OBSERVATIONS}")
    print(f"Symbols:              {SYMBOLS}")
    print("-" * 90)

    all_summaries = []

    for symbol in SYMBOLS:
        summary = process_symbol(symbol)

        if not summary.empty:
            all_summaries.append(summary)

    if not all_summaries:
        print("[WARN] No directional persistence summaries created.")
        return

    save_master_outputs(all_summaries)

    print("-" * 90)
    print("[COMPLETE] Multi-symbol directional persistence analysis complete.")
    print(f"Symbols analysed: {len(all_summaries)}")
    print("=" * 90)


if __name__ == "__main__":
    main()