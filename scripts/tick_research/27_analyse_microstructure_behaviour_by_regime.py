"""
BACQE TICK RESEARCH - 27 Analyse Microstructure Behaviour By Regime - Multi Symbol

Analyses how microstructure behaviour changes across BACQE regime states.

Inputs:
    E:/Quant_Lab/data/processed/tick_research/regime_fusion/symbol=<SYMBOL>/
        <SYMBOL>_microstructure_regime_fusion_latest.parquet

Outputs:
    Per-symbol:
        E:/Quant_Lab/data/analysis/tick_research/microstructure_by_regime/symbol=<SYMBOL>/
        E:/Quant_Lab/reports/tick_research/microstructure_by_regime/symbol=<SYMBOL>/

    Master:
        E:/Quant_Lab/data/analysis/tick_research/microstructure_by_regime/_master/
        E:/Quant_Lab/reports/tick_research/microstructure_by_regime/_master/
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
    / "regime_fusion"
)

OUTPUT_ANALYSIS_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "microstructure_by_regime"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "microstructure_by_regime"
)

MIN_ROWS = 20


def safe_autocorr(series: pd.Series, lag: int = 1) -> float:
    clean = (
        pd.to_numeric(series, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )

    if len(clean) <= lag + 2:
        return np.nan

    return clean.autocorr(lag=lag)


def detect_selected_timeframe(data: pd.DataFrame) -> str:
    if "selected_regime_timeframe" in data.columns:
        values = data["selected_regime_timeframe"].dropna().astype(str).unique()
        if len(values) > 0:
            return values[0]

    for col in data.columns:
        if col.startswith("regime_") and col.endswith("_time"):
            return col.replace("regime_", "").replace("_time", "").upper()

    return "UNKNOWN"


def get_regime_cols(timeframe: str) -> dict:
    prefix = timeframe.lower()

    return {
        "time": f"regime_{prefix}_time",
        "trend": f"{prefix}_trend_state",
        "volatility": f"{prefix}_volatility_state",
        "momentum": f"{prefix}_momentum_state",
        "trend_strength": f"{prefix}_trend_strength_state",
        "composite": f"{prefix}_composite_regime",
        "confidence": f"{prefix}_regime_confidence",
    }


def prepare_data(df: pd.DataFrame, symbol: str) -> tuple[pd.DataFrame, str, dict]:
    data = df.copy()

    data["symbol"] = symbol

    timeframe = detect_selected_timeframe(data)
    regime_cols = get_regime_cols(timeframe)

    numeric_cols = [
        "return",
        "abs_return",
        "range",
        "duration_seconds",
        "tick_count",
        "ticks_per_second",
        "range_per_tick",
        "volatility_per_tick",
        "imbalance_ratio",
        "abs_imbalance_ratio",
        "target_up_h1",
        "target_direction_persist_h1",
        "target_direction_flip_h1",
        "future_return_h1",
        "future_abs_return_h1",
        regime_cols["confidence"],
        "regime_alignment_gap_minutes",
    ]

    for col in numeric_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    if "abs_imbalance_ratio" not in data.columns and "imbalance_ratio" in data.columns:
        data["abs_imbalance_ratio"] = data["imbalance_ratio"].abs()

    if "microstructure_regime" in data.columns:
        data["is_directional_imbalance_regime"] = (
            data["microstructure_regime"]
            .astype(str)
            .str.contains("directional_imbalance", na=False)
        ).astype(int)

        data["is_volatility_expansion_micro"] = (
            data["microstructure_regime"]
            .astype(str)
            .str.contains("volatility_expansion", na=False)
        ).astype(int)

        data["is_compressed_micro"] = (
            data["microstructure_regime"]
            .astype(str)
            .eq("compressed_low_vol")
        ).astype(int)
    else:
        data["microstructure_regime"] = "unknown_microstructure_regime"
        data["is_directional_imbalance_regime"] = 0
        data["is_volatility_expansion_micro"] = 0
        data["is_compressed_micro"] = 0

    data["has_selected_regime"] = (
        data["has_selected_regime"].astype(bool)
        if "has_selected_regime" in data.columns
        else False
    )

    return data, timeframe, regime_cols


def summarise_by_group(
    data: pd.DataFrame,
    group_cols: list[str],
    summary_level: str,
    timeframe: str,
    regime_cols: dict,
) -> pd.DataFrame:
    existing_group_cols = [col for col in group_cols if col in data.columns]

    if not existing_group_cols:
        return pd.DataFrame()

    records = []

    for keys, group in data.groupby(existing_group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)

        base = {
            "symbol": group["symbol"].iloc[0] if "symbol" in group.columns else None,
            "selected_timeframe": timeframe,
            "summary_level": summary_level,
            "rows": len(group),
            "matched_regime_pct": group["has_selected_regime"].mean() * 100
            if "has_selected_regime" in group.columns
            else np.nan,
            "sample_quality": "low_sample",
            "analysis_time_utc": datetime.now(timezone.utc).isoformat(),
        }

        for col, value in zip(existing_group_cols, keys):
            base[col] = value

        if len(group) >= MIN_ROWS:
            base["sample_quality"] = "usable"

        if len(group) >= 100:
            base["sample_quality"] = "stronger"

        confidence_col = regime_cols["confidence"]

        base.update(
            {
                "avg_return": group["return"].mean() if "return" in group.columns else np.nan,
                "avg_abs_return": group["abs_return"].mean() if "abs_return" in group.columns else np.nan,
                "return_std": group["return"].std() if "return" in group.columns else np.nan,
                "avg_future_return_h1": group["future_return_h1"].mean()
                if "future_return_h1" in group.columns
                else np.nan,
                "avg_future_abs_return_h1": group["future_abs_return_h1"].mean()
                if "future_abs_return_h1" in group.columns
                else np.nan,
                "target_up_h1_pct": group["target_up_h1"].mean() * 100
                if "target_up_h1" in group.columns
                else np.nan,
                "target_direction_persist_h1_pct": group["target_direction_persist_h1"].mean() * 100
                if "target_direction_persist_h1" in group.columns
                else np.nan,
                "target_direction_flip_h1_pct": group["target_direction_flip_h1"].mean() * 100
                if "target_direction_flip_h1" in group.columns
                else np.nan,
                "avg_range": group["range"].mean() if "range" in group.columns else np.nan,
                "avg_duration_seconds": group["duration_seconds"].mean()
                if "duration_seconds" in group.columns
                else np.nan,
                "median_duration_seconds": group["duration_seconds"].median()
                if "duration_seconds" in group.columns
                else np.nan,
                "avg_tick_count": group["tick_count"].mean()
                if "tick_count" in group.columns
                else np.nan,
                "median_tick_count": group["tick_count"].median()
                if "tick_count" in group.columns
                else np.nan,
                "avg_ticks_per_second": group["ticks_per_second"].mean()
                if "ticks_per_second" in group.columns
                else np.nan,
                "avg_range_per_tick": group["range_per_tick"].mean()
                if "range_per_tick" in group.columns
                else np.nan,
                "avg_volatility_per_tick": group["volatility_per_tick"].mean()
                if "volatility_per_tick" in group.columns
                else np.nan,
                "avg_abs_imbalance_ratio": group["abs_imbalance_ratio"].mean()
                if "abs_imbalance_ratio" in group.columns
                else np.nan,
                "directional_imbalance_micro_pct": group[
                    "is_directional_imbalance_regime"
                ].mean()
                * 100,
                "volatility_expansion_micro_pct": group[
                    "is_volatility_expansion_micro"
                ].mean()
                * 100,
                "compressed_micro_pct": group["is_compressed_micro"].mean() * 100,
                "avg_regime_confidence": group[confidence_col].mean()
                if confidence_col in group.columns
                else np.nan,
                "avg_alignment_gap_minutes": group["regime_alignment_gap_minutes"].mean()
                if "regime_alignment_gap_minutes" in group.columns
                else np.nan,
                "abs_return_autocorr_lag1": safe_autocorr(group["abs_return"], lag=1)
                if "abs_return" in group.columns
                else np.nan,
                "return_autocorr_lag1": safe_autocorr(group["return"], lag=1)
                if "return" in group.columns
                else np.nan,
            }
        )

        base["persistence_edge_h1"] = (
            base["target_direction_persist_h1_pct"]
            - base["target_direction_flip_h1_pct"]
        )

        records.append(base)

    summary = pd.DataFrame(records)

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    return summary


def build_symbol_report(symbol: str, summary: pd.DataFrame, input_path: Path) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    usable = summary[summary["sample_quality"].isin(["usable", "stronger"])].copy()

    strongest_persistence = usable.sort_values(
        "persistence_edge_h1",
        ascending=False,
        na_position="last",
    ).head(30)

    strongest_activity = usable.sort_values(
        "avg_ticks_per_second",
        ascending=False,
        na_position="last",
    ).head(30)

    strongest_volatility = usable.sort_values(
        "avg_abs_return",
        ascending=False,
        na_position="last",
    ).head(30)

    display_cols = [
        "symbol",
        "selected_timeframe",
        "summary_level",
        "bar_type",
        "bar_family",
        "rows",
        "matched_regime_pct",
        "sample_quality",
        "avg_abs_return",
        "return_std",
        "avg_ticks_per_second",
        "directional_imbalance_micro_pct",
        "volatility_expansion_micro_pct",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
        "persistence_edge_h1",
    ]

    available_cols = [col for col in display_cols if col in summary.columns]

    lines = []
    lines.append("=" * 90)
    lines.append(f"BACQE TICK RESEARCH - MICROSTRUCTURE BEHAVIOUR BY REGIME - {symbol}")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {input_path}")
    lines.append(f"Minimum rows:    {MIN_ROWS}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("STRONGEST H1 PERSISTENCE EDGES")
    lines.append("-" * 90)
    lines.append(strongest_persistence[available_cols].to_string(index=False))

    lines.append("")
    lines.append("HIGHEST ACTIVITY STATES")
    lines.append("-" * 90)
    lines.append(strongest_activity[available_cols].to_string(index=False))

    lines.append("")
    lines.append("HIGHEST VOLATILITY STATES")
    lines.append("-" * 90)
    lines.append(strongest_volatility[available_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This is diagnostic research, not a trading system.")
    lines.append("Persistence edge = persistence % - flip %.")
    lines.append("High activity states may reflect faster information arrival.")
    lines.append("High volatility states may reflect larger movement but not necessarily directional edge.")
    lines.append("Small samples should be treated as hypotheses only.")
    lines.append("=" * 90)

    return "\n".join(lines)


def process_symbol(symbol: str) -> pd.DataFrame:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    input_path = (
        INPUT_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_regime_fusion_latest.parquet"
    )

    if not input_path.exists():
        print(f"[WARN] {symbol}: fusion file not found: {input_path}")
        return pd.DataFrame()

    df = pd.read_parquet(input_path)

    print(f"[INFO] Rows loaded:    {len(df):,}")
    print(f"[INFO] Columns loaded: {len(df.columns):,}")

    data, timeframe, regime_cols = prepare_data(df, symbol)

    composite_col = regime_cols["composite"]
    trend_col = regime_cols["trend"]
    volatility_col = regime_cols["volatility"]

    summaries = []

    summaries.append(
        summarise_by_group(
            data,
            ["bar_type", "bar_family", composite_col],
            "bar_type_composite_regime",
            timeframe,
            regime_cols,
        )
    )

    summaries.append(
        summarise_by_group(
            data,
            ["bar_type", "bar_family", trend_col, volatility_col],
            "bar_type_trend_volatility",
            timeframe,
            regime_cols,
        )
    )

    summaries.append(
        summarise_by_group(
            data,
            ["bar_type", "bar_family", composite_col, "microstructure_regime"],
            "bar_type_composite_micro_regime",
            timeframe,
            regime_cols,
        )
    )

    summaries = [s for s in summaries if not s.empty]

    if not summaries:
        print(f"[WARN] {symbol}: no summaries generated.")
        return pd.DataFrame()

    summary = pd.concat(summaries, ignore_index=True)

    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    csv_path = analysis_dir / f"{symbol}_microstructure_behaviour_by_regime_latest.csv"
    parquet_path = analysis_dir / f"{symbol}_microstructure_behaviour_by_regime_latest.parquet"
    report_path = report_dir / f"{symbol}_microstructure_behaviour_by_regime_report_latest.txt"

    summary.to_csv(csv_path, index=False)
    summary.to_parquet(parquet_path, index=False)

    report = build_symbol_report(symbol, summary, input_path)
    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: CSV:     {csv_path}")
    print(f"[DONE] {symbol}: Parquet: {parquet_path}")
    print(f"[DONE] {symbol}: Report:  {report_path}")

    return summary


def save_master_outputs(summary_frames: list[pd.DataFrame]) -> None:
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_summary = pd.concat(summary_frames, ignore_index=True)

    csv_path = master_analysis_dir / "master_microstructure_behaviour_by_regime_latest.csv"
    parquet_path = master_analysis_dir / "master_microstructure_behaviour_by_regime_latest.parquet"
    report_path = master_report_dir / "master_microstructure_behaviour_by_regime_report_latest.txt"

    master_summary.to_csv(csv_path, index=False)
    master_summary.to_parquet(parquet_path, index=False)

    usable = master_summary[
        master_summary["sample_quality"].isin(["usable", "stronger"])
    ].copy()

    strongest_persistence = usable.sort_values(
        "persistence_edge_h1",
        ascending=False,
        na_position="last",
    ).head(50)

    display_cols = [
        "symbol",
        "selected_timeframe",
        "summary_level",
        "bar_type",
        "bar_family",
        "rows",
        "matched_regime_pct",
        "sample_quality",
        "avg_abs_return",
        "avg_ticks_per_second",
        "target_direction_persist_h1_pct",
        "target_direction_flip_h1_pct",
        "persistence_edge_h1",
    ]

    available_cols = [col for col in display_cols if col in master_summary.columns]

    report_path.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - MASTER MICROSTRUCTURE BEHAVIOUR BY REGIME",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                "-" * 90,
                "",
                "STRONGEST CROSS-SYMBOL H1 PERSISTENCE EDGES",
                "-" * 90,
                strongest_persistence[available_cols].to_string(index=False),
                "",
                "INTERPRETATION NOTES",
                "-" * 90,
                "This is diagnostic research, not a trading system.",
                "Persistence edge = persistence % - flip %.",
                "Use this output to identify regime-conditioned microstructure hypotheses.",
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    print("-" * 90)
    print("[DONE] Master microstructure behaviour by regime analysis created.")
    print(f"Master CSV:     {csv_path}")
    print(f"Master Parquet: {parquet_path}")
    print(f"Master Report:  {report_path}")
    print("-" * 90)
    print("MASTER PREVIEW")
    print(strongest_persistence[available_cols].head(50).to_string(index=False))


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 27 ANALYSE MICROSTRUCTURE BEHAVIOUR BY REGIME - MULTI SYMBOL")
    print("=" * 90)
    print(f"Input root:       {INPUT_ROOT}")
    print(f"Output analysis:  {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output reports:   {OUTPUT_REPORT_ROOT}")
    print(f"Symbols:          {SYMBOLS}")
    print("-" * 90)

    summary_frames = []

    for symbol in SYMBOLS:
        summary = process_symbol(symbol)

        if not summary.empty:
            summary_frames.append(summary)

    if not summary_frames:
        print("[WARN] No behaviour-by-regime summaries created.")
        return

    save_master_outputs(summary_frames)

    print("-" * 90)
    print("[COMPLETE] Multi-symbol microstructure behaviour by regime analysis complete.")
    print(f"Symbols analysed: {len(summary_frames)}")
    print("=" * 90)


if __name__ == "__main__":
    main()