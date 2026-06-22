"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 07 - EXTENDED HORIZON CONTEXT CONDITIONING ENGINE

Purpose:
    Analyse Script 06 dynamic-cost survivors by context:
    hour, session, spread regime, volatility regime, and combined regimes.

Pilot:
    EURJPY
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURJPY"
DEFAULT_TOP_N = 5

BASE_DIR = Path("E:/Quant_Lab")

FEATURE_ROOT = BASE_DIR / "data" / "processed" / "dukascopy_extended_horizon_features"

DYNAMIC_COST_ROOT = (
    BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "dynamic_cost_survival"
)

REPORT_ROOT = (
    BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "context_conditioning"
)


def print_header(symbol: str, top_n: int) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 07 - CONTEXT CONDITIONING ENGINE")
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


def load_dynamic_survivors(symbol: str, top_n: int) -> pd.DataFrame:
    path = DYNAMIC_COST_ROOT / f"{symbol.lower()}_extended_horizon_dynamic_cost_survivors_latest.csv"

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 06 survivors file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Script 06 dynamic survivors file is empty.")

    df = df.sort_values("dynamic_cost_score", ascending=False).head(top_n).copy()

    required = [
        "target",
        "feature",
        "candidate_side",
        "threshold_quantile",
        "threshold_side",
        "dynamic_cost_scenario",
        "dynamic_survival_status",
        "dynamic_cost_score",
    ]

    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Dynamic survivor file missing required columns: {missing}")

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
        df["context_session"].astype(str) + "__" + df["context_spread_regime"].astype(str)
    )

    df["context_session_vol"] = (
        df["context_session"].astype(str) + "__" + df["context_vol_regime"].astype(str)
    )

    df["context_spread_vol"] = (
        df["context_spread_regime"].astype(str) + "__" + df["context_vol_regime"].astype(str)
    )

    return df


def calculate_candidate_context_rows(
    df: pd.DataFrame,
    candidate: dict,
    spread_col: str | None,
    file_path: Path,
) -> pd.DataFrame:
    feature = candidate["feature"]
    target = candidate["target"]
    side = candidate["candidate_side"]
    q = float(candidate["threshold_quantile"])
    threshold_side = candidate["threshold_side"]

    if feature not in df.columns or target not in df.columns:
        return pd.DataFrame()

    needed = [
        feature,
        target,
        "context_hour",
        "context_session",
        "context_spread_regime",
        "context_vol_regime",
        "context_session_spread",
        "context_session_vol",
        "context_spread_vol",
    ]

    if spread_col and spread_col in df.columns and spread_col not in needed:
        needed.append(spread_col)

    needed = list(dict.fromkeys(needed))

    data = df.loc[:, needed].replace([np.inf, -np.inf], np.nan).dropna(subset=[feature, target])

    if len(data) < 500:
        return pd.DataFrame()

    feature_series = data[feature]

    if feature_series.nunique(dropna=True) <= 1:
        return pd.DataFrame()

    threshold = feature_series.quantile(q)

    if threshold_side == "lower":
        signal_mask = feature_series <= threshold
    else:
        signal_mask = feature_series >= threshold

    signal_data = data.loc[signal_mask].copy()

    if len(signal_data) < 100:
        return pd.DataFrame()

    gross_returns = signal_data[target]
    if side == "short":
        gross_returns = -gross_returns

    if spread_col and spread_col in signal_data.columns:
        dynamic_cost = signal_data[spread_col].abs() * 0.5
    else:
        dynamic_cost = 0.0

    signal_data["gross_return"] = gross_returns
    signal_data["dynamic_cost"] = dynamic_cost
    signal_data["net_return"] = gross_returns - dynamic_cost

    signal_data["file"] = str(file_path)
    signal_data["filename"] = file_path.name
    signal_data["target"] = target
    signal_data["feature"] = feature
    signal_data["candidate_side"] = side
    signal_data["threshold_quantile"] = q
    signal_data["threshold_side"] = threshold_side
    signal_data["dynamic_cost_scenario"] = candidate["dynamic_cost_scenario"]
    signal_data["dynamic_survival_status"] = candidate["dynamic_survival_status"]
    signal_data["dynamic_cost_score"] = candidate["dynamic_cost_score"]

    keep_cols = [
        "file",
        "filename",
        "target",
        "feature",
        "candidate_side",
        "threshold_quantile",
        "threshold_side",
        "dynamic_cost_scenario",
        "dynamic_survival_status",
        "dynamic_cost_score",
        "context_hour",
        "context_session",
        "context_spread_regime",
        "context_vol_regime",
        "context_session_spread",
        "context_session_vol",
        "context_spread_vol",
        "gross_return",
        "dynamic_cost",
        "net_return",
    ]

    return signal_data[keep_cols]


def profit_factor(returns: pd.Series) -> float:
    wins = returns[returns > 0].sum()
    losses = returns[returns < 0].sum()

    if losses == 0:
        return np.inf if wins > 0 else np.nan

    return float(wins / abs(losses))


def aggregate_by_context(raw: pd.DataFrame, context_col: str) -> pd.DataFrame:
    grouped = (
        raw.groupby(
            [
                "target",
                "feature",
                "candidate_side",
                "threshold_quantile",
                "threshold_side",
                "dynamic_cost_scenario",
                context_col,
            ],
            dropna=False,
        )
        .agg(
            files_tested=("file", "nunique"),
            trades=("net_return", "count"),
            gross_total_return=("gross_return", "sum"),
            total_dynamic_cost=("dynamic_cost", "sum"),
            net_total_return=("net_return", "sum"),
            mean_net_return=("net_return", "mean"),
            median_net_return=("net_return", "median"),
            win_rate=("net_return", lambda x: float((x > 0).mean())),
            profit_factor=("net_return", profit_factor),
            mean_dynamic_cost_score=("dynamic_cost_score", "mean"),
        )
        .reset_index()
    )

    grouped["context_type"] = context_col
    grouped = grouped.rename(columns={context_col: "context_value"})

    grouped["context_score"] = (
        grouped["net_total_return"].fillna(0)
        + grouped["median_net_return"].fillna(0) * 100000
        + (grouped["win_rate"].fillna(0.5) - 0.5) * 100
        + grouped["profit_factor"].replace([np.inf, -np.inf], np.nan).fillna(0) * 5
    )

    grouped["context_status"] = np.select(
        [
            (grouped["trades"] >= 10_000)
            & (grouped["net_total_return"] > 0)
            & (grouped["median_net_return"] > 0)
            & (grouped["win_rate"] > 0.52)
            & (grouped["profit_factor"] > 1.10),

            (grouped["trades"] >= 10_000)
            & (grouped["net_total_return"] > 0)
            & (grouped["median_net_return"] > 0)
            & (grouped["win_rate"] > 0.505),
        ],
        ["context_pass_primary", "context_pass_secondary"],
        default="context_fail_or_watchlist",
    )

    return grouped


def write_outputs(symbol: str, raw: pd.DataFrame, ranked: pd.DataFrame) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    raw_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_context_raw_latest.csv"
    ranked_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_context_ranked_latest.csv"
    passed_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_context_passed_latest.csv"
    txt_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_context_report_latest.txt"

    # Raw context output disabled for memory/disk safety.
    # raw.to_csv(raw_path, index=False)
    ranked.to_csv(ranked_path, index=False)

    passed = ranked[
        ranked["context_status"].isin(["context_pass_primary", "context_pass_secondary"])
    ].copy()

    passed.to_csv(passed_path, index=False)

    primary_passes = ranked[ranked["context_status"] == "context_pass_primary"].copy()

    primary_passes = primary_passes.sort_values(by=["profit_factor", "win_rate", "net_total_return", "trades"],
        ascending=[False, False, False, False], )

    primary_path = (REPORT_ROOT / f"{symbol.lower()}_extended_horizon_context_primary_passes_latest.csv")

    primary_passes.to_csv(primary_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 07 - CONTEXT CONDITIONING REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Raw context rows: {len(raw)}\n")
        f.write(f"Ranked context rows: {len(ranked)}\n")
        f.write(f"Passed context rows: {len(passed)}\n\n")

        if not ranked.empty:
            f.write("STATUS COUNTS BY CONTEXT TYPE\n")
            f.write("-" * 90 + "\n")
            f.write(ranked.groupby(["context_type", "context_status"]).size().to_string())
            f.write("\n\n")

            display_cols = [
                "context_type",
                "context_value",
                "target",
                "feature",
                "threshold_quantile",
                "threshold_side",
                "context_status",
                "context_score",
                "trades",
                "win_rate",
                "median_net_return",
                "profit_factor",
                "net_total_return",
                "files_tested",
            ]

            f.write("TOP 100 CONTEXT-CONDITIONED CANDIDATES\n")
            f.write("-" * 90 + "\n")
            f.write(ranked[display_cols].head(100).to_string(index=False))
            f.write("\n\n")
            f.write("TOP PRIMARY PASSES ONLY\n")
            f.write("-" * 90 + "\n")

            primary_passes = ranked[ranked["context_status"] == "context_pass_primary"].copy()

            if not primary_passes.empty:

                primary_passes = primary_passes.sort_values(
                    by=["profit_factor", "win_rate", "net_total_return", "trades"],
                    ascending=[False, False, False, False])

                f.write(primary_passes[display_cols].head(100).to_string(index=False))

            else:
                f.write("No primary passes found.\n")
        else:
            f.write("No ranked context rows produced.\n")

    print("Raw context:     disabled for memory/disk safety")
    print(f"Ranked context:  {ranked_path}")
    print(f"Passed context:  {passed_path}")
    print(f"Text report:     {txt_path}")
    print(f"Primary passes:  {primary_path}")



def main(symbol: str, top_n: int) -> None:
    print_header(symbol, top_n)

    files = find_feature_files(symbol)
    survivors = load_dynamic_survivors(symbol, top_n)
    survivor_records = survivors.to_dict("records")

    print(f"Feature files found: {len(files):,}")
    print(f"Survivors loaded:    {len(survivors):,}")
    print("-" * 90)

    all_context_rows = []

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

            for candidate in survivor_records:
                needed_cols.add(candidate["feature"])
                needed_cols.add(candidate["target"])

            available_cols = [col for col in needed_cols if col in columns]
            df = pd.read_parquet(file_path, columns=available_cols)

            df = add_context_columns(df, spread_col=spread_col, vol_col=vol_col)

            file_rows = 0

            for candidate in survivor_records:
                context_rows = calculate_candidate_context_rows(
                    df=df,
                    candidate=candidate,
                    spread_col=spread_col,
                    file_path=file_path,
                )

                if not context_rows.empty:
                    all_context_rows.append(context_rows)
                    file_rows += len(context_rows)

            print(
                f"[OK] {idx:>4}/{len(files)} "
                f"context_rows={file_rows:>8} "
                f"spread_col={spread_col} "
                f"vol_col={vol_col} "
                f"file={file_path.name}"
            )

        except Exception as exc:
            print(f"[ERROR] {idx:>4}/{len(files)} {file_path.name} :: {exc}")

    if all_context_rows:
        raw = pd.concat(all_context_rows, ignore_index=True)
    else:
        raw = pd.DataFrame()

    if raw.empty:
        ranked = pd.DataFrame()
    else:
        context_frames = []

        for context_col in [
            "context_hour",
            "context_session",
            "context_spread_regime",
            "context_vol_regime",
            "context_session_spread",
            "context_session_vol",
            "context_spread_vol",
        ]:
            context_frames.append(aggregate_by_context(raw, context_col))

        ranked = pd.concat(context_frames, ignore_index=True)

        ranked = ranked.sort_values(
            by=[
                "context_status",
                "context_score",
                "net_total_return",
                "profit_factor",
                "win_rate",
            ],
            ascending=[True, False, False, False, False],
        )

    print("-" * 90)
    print(f"Raw context rows:    {len(raw):,}")
    print(f"Ranked context rows: {len(ranked):,}")

    if not ranked.empty:
        print("Context status counts:")
        print(ranked.groupby(["context_type", "context_status"]).size())

    print("-" * 90)

    write_outputs(symbol, raw, ranked)

    print("-" * 90)
    print("[DONE] Extended horizon context conditioning complete")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)

    args = parser.parse_args()

    main(symbol=args.symbol.upper(), top_n=args.top_n)