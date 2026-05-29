"""
BACQE TICK RESEARCH - 15 Measure Bar Entropy - Multi Symbol

Measures directional entropy and transition entropy across fixed tick bars
and tick imbalance bars.

Outputs:
    Per-symbol:
        E:/Quant_Lab/data/analysis/tick_research/bar_entropy/symbol=<SYMBOL>/
        E:/Quant_Lab/reports/tick_research/bar_entropy/symbol=<SYMBOL>/

    Master:
        E:/Quant_Lab/data/analysis/tick_research/bar_entropy/_master/
        E:/Quant_Lab/reports/tick_research/bar_entropy/_master/
"""

from pathlib import Path
from datetime import datetime, timezone
import math
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

TICK_BAR_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_bars"
IMBALANCE_BAR_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "tick_imbalance_bars"

OUTPUT_ANALYSIS_ROOT = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research" / "bar_entropy"
OUTPUT_REPORT_ROOT = DATA_LAKE_ROOT / "reports" / "tick_research" / "bar_entropy"

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

    if "close" not in bars.columns and "close_mid" in bars.columns:
        bars["close"] = bars["close_mid"]

    if "return" not in bars.columns and "close" in bars.columns:
        bars["return"] = bars["close"].pct_change()

    if "direction" not in bars.columns:
        if {"open", "close"}.issubset(bars.columns):
            bars["direction"] = 0
            bars.loc[bars["close"] > bars["open"], "direction"] = 1
            bars.loc[bars["close"] < bars["open"], "direction"] = -1

    return bars


def shannon_entropy(values: pd.Series) -> float:
    clean = values.dropna()

    if clean.empty:
        return np.nan

    probs = clean.value_counts(normalize=True)
    return -sum(p * math.log2(p) for p in probs if p > 0)


def normalized_entropy(values: pd.Series) -> float:
    clean = values.dropna()

    if clean.empty:
        return np.nan

    unique_count = clean.nunique(dropna=True)

    if unique_count <= 1:
        return 0.0

    entropy = shannon_entropy(clean)
    max_entropy = math.log2(unique_count)

    return entropy / max_entropy if max_entropy > 0 else np.nan


def transition_entropy(values: pd.Series) -> float:
    clean = values.dropna().astype(int)

    if len(clean) < 2:
        return np.nan

    transitions = clean.astype(str).shift(1) + "->" + clean.astype(str)
    transitions = transitions.dropna()

    return shannon_entropy(transitions)


def transition_matrix_summary(values: pd.Series) -> dict:
    clean = values.dropna().astype(int)

    if len(clean) < 2:
        return {
            "same_direction_pct": np.nan,
            "direction_flip_pct": np.nan,
            "up_after_up_pct": np.nan,
            "down_after_down_pct": np.nan,
        }

    prev = clean.shift(1)
    curr = clean

    valid = pd.DataFrame({"prev": prev, "curr": curr}).dropna()

    if valid.empty:
        return {
            "same_direction_pct": np.nan,
            "direction_flip_pct": np.nan,
            "up_after_up_pct": np.nan,
            "down_after_down_pct": np.nan,
        }

    same = (valid["prev"] == valid["curr"]).mean() * 100
    flip = (valid["prev"] != valid["curr"]).mean() * 100

    prev_up = valid[valid["prev"] == 1]
    prev_down = valid[valid["prev"] == -1]

    up_after_up = (prev_up["curr"] == 1).mean() * 100 if len(prev_up) else np.nan
    down_after_down = (prev_down["curr"] == -1).mean() * 100 if len(prev_down) else np.nan

    return {
        "same_direction_pct": same,
        "direction_flip_pct": flip,
        "up_after_up_pct": up_after_up,
        "down_after_down_pct": down_after_down,
    }


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


def analyse_entropy(symbol: str, bars: pd.DataFrame) -> dict:
    bars = bars.copy()

    if "return" not in bars.columns:
        raise ValueError(f"{symbol}: bars missing return column.")

    if "direction" not in bars.columns:
        raise ValueError(f"{symbol}: bars missing direction column.")

    returns = pd.to_numeric(bars["return"], errors="coerce").replace([np.inf, -np.inf], np.nan)

    return_sign = pd.Series(0, index=bars.index, dtype="int64")
    return_sign.loc[returns > 0] = 1
    return_sign.loc[returns < 0] = -1

    direction = pd.to_numeric(bars["direction"], errors="coerce")

    direction_transitions = transition_matrix_summary(direction)
    return_sign_transitions = transition_matrix_summary(return_sign)

    result = {
        "symbol": symbol,
        "broker": BROKER,
        "bar_family": bars["bar_family"].iloc[0],
        "bar_type": bars["bar_type"].iloc[0],
        "bar_parameter": bars["bar_parameter"].iloc[0],
        "bar_count": len(bars),
        "first_bar_time": bars["bar_start_time"].min() if "bar_start_time" in bars.columns else None,
        "last_bar_time": bars["bar_end_time"].max() if "bar_end_time" in bars.columns else None,
        "direction_entropy": shannon_entropy(direction),
        "direction_entropy_normalized": normalized_entropy(direction),
        "return_sign_entropy": shannon_entropy(return_sign),
        "return_sign_entropy_normalized": normalized_entropy(return_sign),
        "direction_transition_entropy": transition_entropy(direction),
        "return_sign_transition_entropy": transition_entropy(return_sign),
        "up_pct": (direction == 1).mean() * 100,
        "down_pct": (direction == -1).mean() * 100,
        "flat_pct": (direction == 0).mean() * 100,
        "return_up_pct": (return_sign == 1).mean() * 100,
        "return_down_pct": (return_sign == -1).mean() * 100,
        "return_flat_pct": (return_sign == 0).mean() * 100,
        "analysis_time_utc": datetime.now(timezone.utc).isoformat(),
    }

    result.update({f"direction_{k}": v for k, v in direction_transitions.items()})
    result.update({f"return_sign_{k}": v for k, v in return_sign_transitions.items()})

    return result


def add_entropy_scores(analysis: pd.DataFrame, rank_scope: str) -> pd.DataFrame:
    scored = analysis.copy()

    numeric_cols = [
        "direction_entropy_normalized",
        "return_sign_entropy_normalized",
        "direction_transition_entropy",
        "return_sign_transition_entropy",
        "direction_same_direction_pct",
        "direction_direction_flip_pct",
        "return_sign_same_direction_pct",
        "return_sign_direction_flip_pct",
    ]

    for col in numeric_cols:
        if col in scored.columns:
            scored[col] = pd.to_numeric(scored[col], errors="coerce")

    scored["direction_structure_score"] = 1 - scored["direction_entropy_normalized"].fillna(1)
    scored["return_sign_structure_score"] = 1 - scored["return_sign_entropy_normalized"].fillna(1)

    scored["transition_persistence_score"] = (
        scored["return_sign_same_direction_pct"].fillna(0) / 100
    )

    max_direction_transition_entropy = scored["direction_transition_entropy"].max()
    max_return_transition_entropy = scored["return_sign_transition_entropy"].max()

    scored["direction_transition_structure_score"] = np.where(
        max_direction_transition_entropy > 0,
        1 - (scored["direction_transition_entropy"] / max_direction_transition_entropy),
        0,
    )

    scored["return_transition_structure_score"] = np.where(
        max_return_transition_entropy > 0,
        1 - (scored["return_sign_transition_entropy"] / max_return_transition_entropy),
        0,
    )

    scored["entropy_structure_score"] = (
        scored["direction_structure_score"] * 0.25
        + scored["return_sign_structure_score"] * 0.25
        + scored["transition_persistence_score"] * 0.20
        + scored["direction_transition_structure_score"] * 0.15
        + scored["return_transition_structure_score"] * 0.15
    ).round(8)

    scored["entropy_rank"] = (
        scored["entropy_structure_score"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    scored["rank_scope"] = rank_scope
    scored["sort_order"] = scored["bar_type"].map(BAR_ORDER).fillna(999)

    scored = scored.sort_values(
        ["entropy_rank", "entropy_structure_score", "sort_order"],
        ascending=[True, False, True],
    ).drop(columns=["sort_order"]).reset_index(drop=True)

    return scored


def build_report(analysis: pd.DataFrame, title: str, input_label: str) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()
    top = analysis.iloc[0]

    display_cols = [
        "entropy_rank",
        "symbol",
        "bar_type",
        "bar_family",
        "bar_count",
        "direction_entropy_normalized",
        "return_sign_entropy_normalized",
        "direction_transition_entropy",
        "return_sign_transition_entropy",
        "direction_same_direction_pct",
        "direction_direction_flip_pct",
        "return_sign_same_direction_pct",
        "return_sign_direction_flip_pct",
        "entropy_structure_score",
    ]

    available_cols = [col for col in display_cols if col in analysis.columns]

    lines = []
    lines.append("=" * 90)
    lines.append(title)
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input source:     {input_label}")
    lines.append("-" * 90)
    lines.append(f"Most structured symbol:   {top.get('symbol', 'UNKNOWN')}")
    lines.append(f"Most structured bar type: {top['bar_type']}")
    lines.append(f"Entropy structure score:  {top['entropy_structure_score']}")
    lines.append("-" * 90)
    lines.append("")
    lines.append("ENTROPY STRUCTURE RANKING")
    lines.append("-" * 90)
    lines.append(analysis[available_cols].to_string(index=False))
    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("Entropy measures randomness or uncertainty in direction/return signs.")
    lines.append("Normalized entropy near 1 suggests near-maximum directional randomness.")
    lines.append("Lower entropy suggests more structure, persistence, or directional imbalance.")
    lines.append("Transition entropy measures randomness from one state to the next.")
    lines.append("Higher entropy_structure_score means the bar type appears less random under this v1 diagnostic.")
    lines.append("This is diagnostic research, not a trading signal.")
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

    csv_path = analysis_dir / f"{file_prefix}_bar_entropy_analysis_latest.csv"
    parquet_path = analysis_dir / f"{file_prefix}_bar_entropy_analysis_latest.parquet"
    report_path = report_dir / f"{file_prefix}_bar_entropy_report_latest.txt"

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

        records.append(analyse_entropy(symbol, bars))
        print(f"[DONE] {symbol}: analysed tick bars {tick_size} | bars={len(bars):,}")

    for threshold in IMBALANCE_THRESHOLDS:
        bars = load_imbalance_bars(symbol, threshold)

        if bars.empty:
            continue

        records.append(analyse_entropy(symbol, bars))
        print(f"[DONE] {symbol}: analysed imbalance bars {threshold} | bars={len(bars):,}")

    if not records:
        print(f"[WARN] {symbol}: no entropy records created.")
        return pd.DataFrame()

    analysis = pd.DataFrame(records)

    numeric_cols = analysis.select_dtypes(include=["float", "int"]).columns
    analysis[numeric_cols] = analysis[numeric_cols].round(8)

    analysis = add_entropy_scores(
        analysis=analysis,
        rank_scope=f"symbol={symbol}",
    )

    save_analysis(
        analysis=analysis,
        analysis_dir=OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}",
        report_dir=OUTPUT_REPORT_ROOT / f"symbol={symbol}",
        file_prefix=symbol,
        report_title=f"BACQE TICK RESEARCH - BAR ENTROPY REPORT - {symbol}",
        input_label=f"Processed tick and imbalance bars for {symbol}",
    )

    display_cols = [
        "entropy_rank",
        "symbol",
        "bar_type",
        "bar_family",
        "bar_count",
        "direction_entropy_normalized",
        "return_sign_entropy_normalized",
        "return_sign_same_direction_pct",
        "entropy_structure_score",
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
        "direction_entropy_normalized",
        "return_sign_entropy_normalized",
        "direction_transition_entropy",
        "return_sign_transition_entropy",
        "return_sign_same_direction_pct",
        "entropy_structure_score",
        "rank_scope",
    ]

    available_cols = [col for col in keep_cols if col in winner_summary.columns]

    return winner_summary[available_cols].sort_values(
        "entropy_structure_score",
        ascending=False,
    ).reset_index(drop=True)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 15 MEASURE BAR ENTROPY - MULTI SYMBOL")
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
        print("[WARN] No entropy analyses created.")
        return

    master_input = pd.concat(symbol_analyses, ignore_index=True)

    master_analysis = add_entropy_scores(
        analysis=master_input,
        rank_scope="master_cross_symbol",
    )

    save_analysis(
        analysis=master_analysis,
        analysis_dir=OUTPUT_ANALYSIS_ROOT / "_master",
        report_dir=OUTPUT_REPORT_ROOT / "_master",
        file_prefix="master",
        report_title="BACQE TICK RESEARCH - BAR ENTROPY REPORT - MASTER CROSS-SYMBOL",
        input_label="All processed multi-symbol tick and imbalance bars",
    )

    winner_summary = build_winner_summary(symbol_analyses)

    if not winner_summary.empty:
        winner_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
        winner_report_dir = OUTPUT_REPORT_ROOT / "_master"
        winner_analysis_dir.mkdir(parents=True, exist_ok=True)
        winner_report_dir.mkdir(parents=True, exist_ok=True)

        winner_csv = winner_analysis_dir / "symbol_winners_bar_entropy_latest.csv"
        winner_parquet = winner_analysis_dir / "symbol_winners_bar_entropy_latest.parquet"
        winner_txt = winner_report_dir / "symbol_winners_bar_entropy_latest.txt"

        winner_summary.to_csv(winner_csv, index=False)
        winner_summary.to_parquet(winner_parquet, index=False)

        winner_txt.write_text(
            "\n".join(
                [
                    "=" * 90,
                    "BACQE TICK RESEARCH - SYMBOL WINNERS BAR ENTROPY",
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
    print("[COMPLETE] Multi-symbol bar entropy analysis complete.")
    print(f"Symbols analysed: {len(symbol_analyses)}")
    print(f"Master rows:      {len(master_analysis):,}")
    print("=" * 90)


if __name__ == "__main__":
    main()
