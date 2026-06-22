"""
BACQE DUKASCOPY EXTENDED HORIZONS
SCRIPT 06 - EXTENDED HORIZON DYNAMIC COST ENGINE

Purpose:
    Re-test validated extended-horizon signals using dynamic costs derived
    from the actual signal rows.

Pilot:
    EURJPY
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd


DEFAULT_SYMBOL = "EURJPY"
DEFAULT_TOP_N = 75

BASE_DIR = Path("E:/Quant_Lab")

FEATURE_ROOT = BASE_DIR / "data" / "processed" / "dukascopy_extended_horizon_features"

VALIDATION_ROOT = (
    BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "signal_validation"
)

REPORT_ROOT = (
    BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "dynamic_cost_survival"
)

QUANTILES = [0.10, 0.20, 0.25, 0.75, 0.80, 0.90]

DYNAMIC_COST_SCENARIOS = {
    "spread_only": 1.00,
    "half_spread": 0.50,
    "spread_plus_25pct": 1.25,
    "spread_plus_50pct": 1.50,
}


def print_header(symbol: str, top_n: int) -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY EXTENDED HORIZONS")
    print("SCRIPT 06 - EXTENDED HORIZON DYNAMIC COST ENGINE")
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


def load_validated_candidates(symbol: str, top_n: int) -> pd.DataFrame:
    path = VALIDATION_ROOT / f"{symbol.lower()}_extended_horizon_signal_validation_ranked_latest.csv"

    if not path.exists():
        raise FileNotFoundError(f"Missing Script 04 ranked validation file: {path}")

    df = pd.read_csv(path)

    df = df[
        df["validation_status"].isin(
            ["validation_pass_primary", "validation_pass_secondary"]
        )
    ].copy()

    if df.empty:
        raise ValueError("No passed validation candidates found.")

    df = df.sort_values("validation_score", ascending=False).head(top_n).copy()

    required = [
        "target",
        "feature",
        "candidate_side",
        "threshold_quantile",
        "threshold_side",
        "validation_status",
        "validation_score",
    ]

    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Validation ranked file missing columns: {missing}")

    return df


def detect_spread_column(columns: list[str]) -> str | None:
    preferred = [
        "spread",
        "spread_points",
        "rolling_spread_mean_5",
        "rolling_spread_mean_10",
    ]

    for col in preferred:
        if col in columns:
            return col

    return None


def calculate_signal_returns(
    df: pd.DataFrame,
    candidate: dict,
    spread_col: str | None,
    file_path: Path,
) -> list[dict]:

    feature = candidate["feature"]
    target = candidate["target"]
    side = candidate["candidate_side"]
    q = float(candidate["threshold_quantile"])
    threshold_side = candidate["threshold_side"]

    needed = [feature, target]

    if spread_col and spread_col not in needed:
        needed.append(spread_col)

    needed = list(dict.fromkeys(needed))

    data = df.loc[:, needed].replace([np.inf, -np.inf], np.nan).dropna()

    if len(data) < 500:
        return []

    feature_series = data[feature]
    target_series = data[target]

    if feature_series.nunique(dropna=True) <= 1:
        return []

    threshold = feature_series.quantile(q)

    if threshold_side == "lower":
        signal_mask = feature_series <= threshold
    else:
        signal_mask = feature_series >= threshold

    signal_data = data.loc[signal_mask].copy()

    if len(signal_data) < 100:
        return []

    gross_returns = signal_data[target]
    if side == "short":
        gross_returns = -gross_returns

    if spread_col:
        dynamic_cost = signal_data[spread_col].abs()
    else:
        dynamic_cost = pd.Series(0.0, index=signal_data.index)

    rows = []

    for scenario, multiplier in DYNAMIC_COST_SCENARIOS.items():
        net_returns = gross_returns - (dynamic_cost * multiplier)

        wins = net_returns[net_returns > 0]
        losses = net_returns[net_returns < 0]

        if len(losses) == 0:
            pf = np.inf if len(wins) > 0 else np.nan
        else:
            pf = float(wins.sum() / abs(losses.sum()))

        rows.append(
            {
                "file": str(file_path),
                "filename": file_path.name,
                "target": target,
                "feature": feature,
                "candidate_side": side,
                "threshold_quantile": q,
                "threshold_side": threshold_side,
                "threshold_value": float(threshold),
                "dynamic_cost_scenario": scenario,
                "cost_multiplier": multiplier,
                "spread_col_used": spread_col if spread_col else "none",
                "trades": int(len(net_returns)),
                "gross_avg_return": float(gross_returns.mean()),
                "gross_median_return": float(gross_returns.median()),
                "gross_total_return": float(gross_returns.sum()),
                "avg_dynamic_cost": float(dynamic_cost.mean()),
                "median_dynamic_cost": float(dynamic_cost.median()),
                "total_dynamic_cost": float((dynamic_cost * multiplier).sum()),
                "net_avg_return": float(net_returns.mean()),
                "net_median_return": float(net_returns.median()),
                "net_total_return": float(net_returns.sum()),
                "net_win_rate": float((net_returns > 0).mean()),
                "net_profit_factor": pf,
                "validation_status": candidate["validation_status"],
                "validation_score": candidate["validation_score"],
            }
        )

    return rows


def aggregate_dynamic_cost(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw

    grouped = (
        raw.groupby(
            [
                "dynamic_cost_scenario",
                "cost_multiplier",
                "target",
                "feature",
                "candidate_side",
                "threshold_quantile",
                "threshold_side",
                "spread_col_used",
            ],
            dropna=False,
        )
        .agg(
            files_tested=("file", "nunique"),
            total_trades=("trades", "sum"),
            gross_total_return=("gross_total_return", "sum"),
            mean_gross_avg_return=("gross_avg_return", "mean"),
            median_gross_avg_return=("gross_avg_return", "median"),
            mean_dynamic_cost=("avg_dynamic_cost", "mean"),
            median_dynamic_cost=("median_dynamic_cost", "median"),
            total_dynamic_cost=("total_dynamic_cost", "sum"),
            net_total_return=("net_total_return", "sum"),
            mean_net_avg_return=("net_avg_return", "mean"),
            median_net_avg_return=("net_avg_return", "median"),
            mean_net_win_rate=("net_win_rate", "mean"),
            median_net_win_rate=("net_win_rate", "median"),
            mean_net_profit_factor=("net_profit_factor", "mean"),
            median_net_profit_factor=("net_profit_factor", "median"),
            worst_file_net_return=("net_total_return", "min"),
            best_file_net_return=("net_total_return", "max"),
            mean_validation_score=("validation_score", "mean"),
        )
        .reset_index()
    )

    grouped["dynamic_cost_score"] = (
        grouped["net_total_return"].fillna(0)
        + grouped["median_net_avg_return"].fillna(0) * 100000
        + (grouped["median_net_win_rate"].fillna(0.5) - 0.5) * 100
        + grouped["median_net_profit_factor"]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
        * 5
    )

    grouped["dynamic_survival_status"] = np.select(
        [
            (grouped["net_total_return"] > 0)
            & (grouped["median_net_avg_return"] > 0)
            & (grouped["median_net_win_rate"] > 0.52)
            & (grouped["median_net_profit_factor"] > 1.10),

            (grouped["net_total_return"] > 0)
            & (grouped["median_net_avg_return"] > 0)
            & (grouped["median_net_win_rate"] > 0.505),

            grouped["net_total_return"] <= 0,
        ],
        [
            "dynamic_survivor_primary",
            "dynamic_survivor_secondary",
            "dynamic_cost_fail",
        ],
        default="dynamic_watchlist",
    )

    grouped = grouped.sort_values(
        by=[
            "dynamic_cost_scenario",
            "dynamic_survival_status",
            "dynamic_cost_score",
        ],
        ascending=[True, True, False],
    )

    return grouped


def write_outputs(symbol: str, raw: pd.DataFrame, ranked: pd.DataFrame) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    raw_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_dynamic_cost_raw_latest.csv"
    ranked_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_dynamic_cost_ranked_latest.csv"
    survivors_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_dynamic_cost_survivors_latest.csv"
    txt_path = REPORT_ROOT / f"{symbol.lower()}_extended_horizon_dynamic_cost_report_latest.txt"

    raw.to_csv(raw_path, index=False)
    ranked.to_csv(ranked_path, index=False)

    survivors = ranked[
        ranked["dynamic_survival_status"].isin(
            ["dynamic_survivor_primary", "dynamic_survivor_secondary"]
        )
    ].copy()

    survivors.to_csv(survivors_path, index=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY EXTENDED HORIZONS\n")
        f.write("SCRIPT 06 - EXTENDED HORIZON DYNAMIC COST ENGINE REPORT\n")
        f.write("=" * 90 + "\n")
        f.write(f"Symbol: {symbol}\n")
        f.write(f"Raw rows: {len(raw)}\n")
        f.write(f"Ranked rows: {len(ranked)}\n")
        f.write(f"Survivors: {len(survivors)}\n\n")

        if not ranked.empty:
            f.write("STATUS COUNTS BY DYNAMIC COST SCENARIO\n")
            f.write("-" * 90 + "\n")
            f.write(
                ranked.groupby(["dynamic_cost_scenario", "dynamic_survival_status"])
                .size()
                .to_string()
            )
            f.write("\n\n")

            cols = [
                "dynamic_cost_scenario",
                "target",
                "feature",
                "candidate_side",
                "threshold_quantile",
                "threshold_side",
                "dynamic_survival_status",
                "dynamic_cost_score",
                "total_trades",
                "median_net_win_rate",
                "median_net_avg_return",
                "median_net_profit_factor",
                "net_total_return",
                "median_dynamic_cost",
                "files_tested",
            ]

            f.write("TOP 75 DYNAMIC COST CANDIDATES\n")
            f.write("-" * 90 + "\n")
            f.write(ranked[cols].head(75).to_string(index=False))
        else:
            f.write("No ranked dynamic cost rows produced.\n")

    print(f"Raw dynamic cost:      {raw_path}")
    print(f"Ranked dynamic cost:   {ranked_path}")
    print(f"Survivors:             {survivors_path}")
    print(f"Text report:           {txt_path}")


def main(symbol: str, top_n: int) -> None:
    print_header(symbol, top_n)

    files = find_feature_files(symbol)
    candidates = load_validated_candidates(symbol, top_n)

    print(f"Feature files found: {len(files):,}")
    print(f"Candidates loaded:   {len(candidates):,}")
    print("-" * 90)

    all_rows = []

    candidate_records = candidates.to_dict("records")

    for idx, file_path in enumerate(files, start=1):
        try:
            sample_cols = pd.read_parquet(file_path, engine="pyarrow").columns.tolist()
            spread_col = detect_spread_column(sample_cols)

            needed_cols = set()
            for candidate in candidate_records:
                needed_cols.add(candidate["feature"])
                needed_cols.add(candidate["target"])

            if spread_col:
                needed_cols.add(spread_col)

            available_cols = [col for col in needed_cols if col in sample_cols]
            df = pd.read_parquet(file_path, columns=available_cols)

            file_rows = 0

            for candidate in candidate_records:
                rows = calculate_signal_returns(
                    df=df,
                    candidate=candidate,
                    spread_col=spread_col,
                    file_path=file_path,
                )

                all_rows.extend(rows)
                file_rows += len(rows)

            print(
                f"[OK] {idx:>4}/{len(files)} "
                f"rows={file_rows:>5} "
                f"spread_col={spread_col} "
                f"file={file_path.name}"
            )

        except Exception as exc:
            print(f"[ERROR] {idx:>4}/{len(files)} {file_path.name} :: {exc}")

    raw = pd.DataFrame(all_rows)
    ranked = aggregate_dynamic_cost(raw)

    print("-" * 90)
    print(f"Raw rows:    {len(raw):,}")
    print(f"Ranked rows: {len(ranked):,}")

    if not ranked.empty:
        print("Dynamic survival status counts:")
        print(ranked.groupby(["dynamic_cost_scenario", "dynamic_survival_status"]).size())

    print("-" * 90)

    write_outputs(symbol, raw, ranked)

    print("-" * 90)
    print("[DONE] Extended horizon dynamic cost engine complete")
    print("=" * 90)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)

    args = parser.parse_args()

    main(symbol=args.symbol.upper(), top_n=args.top_n)