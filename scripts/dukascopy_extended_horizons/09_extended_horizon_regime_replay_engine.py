"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 09 - EXTENDED HORIZON REGIME REPLAY ENGINE

Purpose:
    Replay top Script 08 primary regimes chronologically across all EURJPY
    extended horizon feature files.

Checks:
    - Year stability
    - Quarter stability
    - Monthly stability
    - File/day stability
    - Simple cumulative return and drawdown proxy

Pilot:
    EURJPY
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURJPY"
DEFAULT_TOP_N = 50

BASE_DIR = Path("E:/Quant_Lab")

FEATURE_ROOT = BASE_DIR / "data" / "processed" / "dukascopy_extended_horizon_features"

REGIME_ROOT = (
    BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "regime_edge_engine"
)

REPORT_ROOT = (
    BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "regime_replay"
)


def print_header(symbol: str, top_n: int) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 09 - REGIME REPLAY ENGINE")
    print("=" * 90)
    print(f"Symbol:       {symbol}")
    print(f"Top N:        {top_n}")
    print(f"Feature root: {FEATURE_ROOT}")
    print(f"Regime root:  {REGIME_ROOT}")
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


def load_primary_regimes(symbol: str, top_n: int) -> pd.DataFrame:
    path = REGIME_ROOT / f"{symbol.lower()}_extended_horizon_primary_regimes_latest.csv"

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 08 primary regimes file: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError("Script 08 primary regimes file is empty.")

    df = df.sort_values(
        by=["quality_score", "profit_factor", "win_rate", "net_total_return"],
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
        "win_rate",
        "profit_factor",
        "quality_score",
    ]

    missing = [col for col in required if col not in df.columns]

    if missing:
        raise ValueError(f"Primary regimes file missing required columns: {missing}")

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
        df["context_session"].astype(str) + "__" + df["context_spread_regime"].astype(str)
    )

    df["context_session_vol"] = (
        df["context_session"].astype(str) + "__" + df["context_vol_regime"].astype(str)
    )

    df["context_spread_vol"] = (
        df["context_spread_regime"].astype(str) + "__" + df["context_vol_regime"].astype(str)
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


def calculate_regime_file_result(
    df: pd.DataFrame,
    regime: dict,
    spread_col: str | None,
    file_path: Path,
) -> dict | None:
    feature = regime["feature"]
    target = regime["target"]
    context_type = regime["context_type"]
    context_value = regime["context_value"]
    threshold_quantile = float(regime["threshold_quantile"])
    threshold_side = regime["threshold_side"]

    if feature not in df.columns or target not in df.columns:
        return None

    needed = [
        feature,
        target,
        context_type,
    ]

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

    # Current branch has long-only primary regimes.
    net_cost = signal_data[spread_col].abs() * 0.5 if spread_col and spread_col in signal_data.columns else 0.0
    net_returns = gross_returns - net_cost

    file_date = parse_date_from_filename(file_path)

    return {
        "regime_id": regime["regime_id"],
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
        "script08_quality_score": regime.get("quality_score", np.nan),
        "script08_win_rate": regime.get("win_rate", np.nan),
        "script08_profit_factor": regime.get("profit_factor", np.nan),
        "trades": int(len(net_returns)),
        "gross_total_return": float(gross_returns.sum()),
        "total_dynamic_cost": float(net_cost.sum()) if isinstance(net_cost, pd.Series) else float(net_cost),
        "net_total_return": float(net_returns.sum()),
        "net_mean_return": float(net_returns.mean()),
        "net_median_return": float(net_returns.median()),
        "net_win_rate": float((net_returns > 0).mean()),
        "net_profit_factor": profit_factor(net_returns),
    }


def profit_factor(returns: pd.Series) -> float:
    wins = returns[returns > 0].sum()
    losses = returns[returns < 0].sum()

    if losses == 0:
        return np.inf if wins > 0 else np.nan

    return float(wins / abs(losses))


def aggregate_replay(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw

    grouped = (
        raw.groupby(
            [
                "regime_id",
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
            active_files=("trades", lambda x: int((x > 0).sum())),
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
            script08_quality_score=("script08_quality_score", "mean"),
            script08_win_rate=("script08_win_rate", "mean"),
            script08_profit_factor=("script08_profit_factor", "mean"),
        )
        .reset_index()
    )

    grouped["replay_score"] = (
        grouped["net_total_return"].fillna(0)
        + grouped["median_file_return"].fillna(0) * 100
        + (grouped["positive_file_rate"].fillna(0.5) - 0.5) * 100
        + (grouped["median_net_win_rate"].fillna(0.5) - 0.5) * 100
        + grouped["median_net_profit_factor"].replace([np.inf, -np.inf], np.nan).fillna(0) * 10
    )

    grouped["replay_status"] = np.select(
        [
            (grouped["net_total_return"] > 0)
            & (grouped["positive_file_rate"] > 0.55)
            & (grouped["median_net_win_rate"] > 0.52)
            & (grouped["median_net_profit_factor"] > 1.10),

            (grouped["net_total_return"] > 0)
            & (grouped["positive_file_rate"] > 0.50)
            & (grouped["median_net_win_rate"] > 0.505),
        ],
        ["replay_pass_primary", "replay_pass_secondary"],
        default="replay_fail_or_watchlist",
    )

    grouped = grouped.sort_values(
        by=[
            "replay_status",
            "replay_score",
            "net_total_return",
            "positive_file_rate",
            "median_net_profit_factor",
        ],
        ascending=[True, False, False, False, False],
    )

    return grouped


def aggregate_period(raw: pd.DataFrame, period_col: str) -> pd.DataFrame:
    if raw.empty:
        return raw

    grouped = (
        raw.groupby(["regime_id", period_col], dropna=False)
        .agg(
            files=("file", "nunique"),
            trades=("trades", "sum"),
            net_total_return=("net_total_return", "sum"),
            mean_file_return=("net_total_return", "mean"),
            positive_file_rate=("net_total_return", lambda x: float((x > 0).mean())),
            median_net_win_rate=("net_win_rate", "median"),
            median_net_profit_factor=("net_profit_factor", "median"),
        )
        .reset_index()
    )

    grouped = grouped.sort_values(
        by=["regime_id", period_col],
        ascending=[True, True],
    )

    return grouped


def build_drawdown_summary(raw: pd.DataFrame) -> pd.DataFrame:
    rows = []

    if raw.empty:
        return pd.DataFrame()

    for regime_id, g in raw.sort_values("date").groupby("regime_id"):
        g = g.sort_values("date").copy()

        equity = g["net_total_return"].cumsum()
        running_max = equity.cummax()
        drawdown = equity - running_max

        rows.append(
            {
                "regime_id": regime_id,
                "periods": len(g),
                "total_return": float(equity.iloc[-1]) if len(equity) else np.nan,
                "max_drawdown": float(drawdown.min()) if len(drawdown) else np.nan,
                "final_equity": float(equity.iloc[-1]) if len(equity) else np.nan,
                "best_file_return": float(g["net_total_return"].max()),
                "worst_file_return": float(g["net_total_return"].min()),
                "positive_file_rate": float((g["net_total_return"] > 0).mean()),
            }
        )

    return pd.DataFrame(rows).sort_values(
        by=["total_return", "positive_file_rate"],
        ascending=[False, False],
    )


def write_outputs(
    symbol: str,
    raw: pd.DataFrame,
    ranked: pd.DataFrame,
    yearly: pd.DataFrame,
    quarterly: pd.DataFrame,
    monthly: pd.DataFrame,
    drawdown: pd.DataFrame,
) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    raw_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_replay_raw_latest.csv"
    ranked_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_replay_ranked_latest.csv"
    yearly_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_replay_yearly_latest.csv"
    quarterly_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_replay_quarterly_latest.csv"
    monthly_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_replay_monthly_latest.csv"
    drawdown_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_replay_drawdown_latest.csv"
    txt_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_regime_replay_report_latest.txt"

    raw.to_csv(raw_path, index=False)
    ranked.to_csv(ranked_path, index=False)
    yearly.to_csv(yearly_path, index=False)
    quarterly.to_csv(quarterly_path, index=False)
    monthly.to_csv(monthly_path, index=False)
    drawdown.to_csv(drawdown_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 09 - REGIME REPLAY ENGINE REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Raw replay rows: {len(raw)}\n")
        f.write(f"Ranked regimes: {len(ranked)}\n\n")

        if not ranked.empty:
            f.write("REPLAY STATUS COUNTS\n")
            f.write("-" * 90 + "\n")
            f.write(ranked["replay_status"].value_counts().to_string())
            f.write("\n\n")

            display_cols = [
                "replay_status",
                "regime_id",
                "context_type",
                "context_value",
                "target",
                "feature",
                "threshold_quantile",
                "threshold_side",
                "replay_score",
                "files_tested",
                "total_trades",
                "net_total_return",
                "positive_file_rate",
                "median_net_win_rate",
                "median_net_profit_factor",
                "first_date",
                "last_date",
            ]

            f.write("TOP 50 REPLAYED REGIMES\n")
            f.write("-" * 90 + "\n")
            f.write(ranked[display_cols].head(50).to_string(index=False))
            f.write("\n\n")

            primary = ranked[ranked["replay_status"] == "replay_pass_primary"].copy()

            f.write("PRIMARY REPLAY PASSES ONLY\n")
            f.write("-" * 90 + "\n")
            if primary.empty:
                f.write("No primary replay passes found.\n")
            else:
                primary = primary.sort_values(
                    by=[
                        "median_net_profit_factor",
                        "positive_file_rate",
                        "net_total_return",
                        "total_trades",
                    ],
                    ascending=[False, False, False, False],
                )
                f.write(primary[display_cols].head(50).to_string(index=False))
                f.write("\n\n")

        if not yearly.empty:
            f.write("YEARLY SUMMARY SAMPLE\n")
            f.write("-" * 90 + "\n")
            f.write(yearly.head(100).to_string(index=False))
            f.write("\n\n")

        if not drawdown.empty:
            f.write("DRAWDOWN SUMMARY SAMPLE\n")
            f.write("-" * 90 + "\n")
            f.write(drawdown.head(100).to_string(index=False))

    print(f"Raw replay:      {raw_path}")
    print(f"Ranked replay:   {ranked_path}")
    print(f"Yearly replay:   {yearly_path}")
    print(f"Quarterly replay:{quarterly_path}")
    print(f"Monthly replay:  {monthly_path}")
    print(f"Drawdown replay: {drawdown_path}")
    print(f"Text report:     {txt_path}")


def main(symbol: str, top_n: int) -> None:
    print_header(symbol, top_n)

    files = find_feature_files(symbol)
    regimes = load_primary_regimes(symbol, top_n)
    regime_records = regimes.to_dict("records")

    print(f"Feature files found: {len(files):,}")
    print(f"Regimes loaded:      {len(regimes):,}")
    print("-" * 90)

    all_rows = []

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

                context_type = regime["context_type"]

                if context_type in [
                    "context_hour",
                    "context_session",
                    "context_spread_regime",
                    "context_vol_regime",
                    "context_session_spread",
                    "context_session_vol",
                    "context_spread_vol",
                ]:
                    needed_cols.add("hour")
                    if spread_col:
                        needed_cols.add(spread_col)
                    if vol_col:
                        needed_cols.add(vol_col)

            available_cols = [col for col in needed_cols if col in columns]

            df = pd.read_parquet(file_path, columns=available_cols)
            df = add_context_columns(df, spread_col=spread_col, vol_col=vol_col)

            file_rows = 0

            for regime in regime_records:
                result = calculate_regime_file_result(
                    df=df,
                    regime=regime,
                    spread_col=spread_col,
                    file_path=file_path,
                )

                if result is not None:
                    all_rows.append(result)
                    file_rows += 1

            print(
                f"[OK] {idx:>4}/{len(files)} "
                f"regime_results={file_rows:>4} "
                f"spread_col={spread_col} "
                f"vol_col={vol_col} "
                f"file={file_path.name}"
            )

        except Exception as exc:
            print(f"[ERROR] {idx:>4}/{len(files)} {file_path.name} :: {exc}")

    raw = pd.DataFrame(all_rows)

    ranked = aggregate_replay(raw)
    yearly = aggregate_period(raw, "year") if not raw.empty else pd.DataFrame()
    quarterly = aggregate_period(raw, "quarter") if not raw.empty else pd.DataFrame()
    monthly = aggregate_period(raw, "month") if not raw.empty else pd.DataFrame()
    drawdown = build_drawdown_summary(raw)

    print("-" * 90)
    print(f"Raw replay rows: {len(raw):,}")
    print(f"Ranked regimes:  {len(ranked):,}")

    if not ranked.empty:
        print("Replay status counts:")
        print(ranked["replay_status"].value_counts())

    print("-" * 90)

    write_outputs(
        symbol=symbol,
        raw=raw,
        ranked=ranked,
        yearly=yearly,
        quarterly=quarterly,
        monthly=monthly,
        drawdown=drawdown,
    )

    print("-" * 90)
    print("[DONE] Extended horizon regime replay engine complete")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)

    args = parser.parse_args()

    main(symbol=args.symbol.upper(), top_n=args.top_n)