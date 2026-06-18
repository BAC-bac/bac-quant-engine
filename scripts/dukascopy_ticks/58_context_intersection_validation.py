"""
BACQE DUKASCOPY 58 - CONTEXT INTERSECTION VALIDATION

Purpose:
    Validate Script 57 context-conditioned survivors
    across:

        2023 Train
        2024 Validation
        2025 OOS

Goal:
    Determine whether context-conditioned
    cost-surviving edges persist through time.
"""

from pathlib import Path
import numpy as np
import pandas as pd


SYMBOL = "EURUSD"

QUANT_LAB = Path(r"E:\Quant_Lab")

SURVIVOR_FILE = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_context_conditioning_research"
    / f"symbol={SYMBOL}"
    / "tables"
    / "context_conditioning_ranked_latest.csv"
)

LEDGER_FILE = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_horizon_candidate_replay"
    / "trade_ledgers"
    / "candidate_replay_ledger_latest.parquet"
)

OUTPUT_ROOT = (
    QUANT_LAB
    / "data"
    / "analysis"
    / "dukascopy_context_intersection_validation"
    / f"symbol={SYMBOL}"
)

MIN_TRADES_PER_YEAR = 5000


def ensure_dirs(output_root: Path) -> None:
    for folder in [
        output_root,
        output_root / "yearly_validation",
        output_root / "oos_rankings",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)


def evaluate_returns(returns: pd.Series) -> dict:
    returns = returns.replace([np.inf, -np.inf], np.nan).dropna()

    if returns.empty:
        return {
            "trade_count": 0,
            "win_rate": np.nan,
            "mean_return": np.nan,
            "total_return": np.nan,
            "profit_factor": np.nan,
            "max_drawdown_return": np.nan,
        }

    wins = returns[returns > 0]
    losses = returns[returns < 0]

    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())

    profit_factor = gross_profit / gross_loss if gross_loss != 0 else np.nan

    equity = returns.cumsum()
    drawdown = equity - equity.cummax()

    return {
        "trade_count": len(returns),
        "win_rate": (returns > 0).mean(),
        "mean_return": returns.mean(),
        "total_return": returns.sum(),
        "profit_factor": profit_factor,
        "max_drawdown_return": drawdown.min(),
    }


def classify_oos(row: pd.Series) -> str:
    if (
        row["train_profit_factor"] >= 1.05
        and row["validation_profit_factor"] >= 1.05
        and row["oos_profit_factor"] >= 1.05
        and row["profitable_years"] == 3
    ):
        return "oos_pass"

    if (
        row["validation_profit_factor"] >= 1.00
        and row["oos_profit_factor"] >= 1.00
        and row["profitable_years"] >= 2
    ):
        return "oos_watchlist"

    return "oos_reject"


def parse_context_label(label: str) -> dict:
    """
    Convert:

    session=asia | day_of_week=Monday

    into:

    {
        "session": "asia",
        "day_of_week": "Monday"
    }
    """

    filters = {}

    for part in str(label).split("|"):
        part = part.strip()

        if "=" not in part:
            continue

        key, value = part.split("=", 1)

        filters[key.strip()] = value.strip()

    return filters


def apply_context_filter(
    df: pd.DataFrame,
    context_label: str,
) -> pd.DataFrame:
    """
    Apply Script 57 context filter to replay ledger.
    """

    filters = parse_context_label(context_label)

    filtered = df

    for column, value in filters.items():

        if column not in filtered.columns:
            return pd.DataFrame()

        filtered = filtered[
            filtered[column].astype(str) == str(value)
        ]

    return filtered.copy()


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:

    if not SURVIVOR_FILE.exists():
        raise FileNotFoundError(
            f"Missing survivor file: {SURVIVOR_FILE}"
        )

    if not LEDGER_FILE.exists():
        raise FileNotFoundError(
            f"Missing ledger file: {LEDGER_FILE}"
        )

    survivors = pd.read_csv(SURVIVOR_FILE)

    ledger = pd.read_parquet(LEDGER_FILE)

    return survivors, ledger

def prepare_survivors(
    survivors: pd.DataFrame,
) -> pd.DataFrame:

    survivors = survivors.copy()

    survivors = survivors[
        survivors["strong_survivor"] == True
    ]

    survivors = survivors.reset_index(drop=True)

    return survivors


def prepare_ledger(
    ledger: pd.DataFrame,
) -> pd.DataFrame:

    ledger = ledger.copy()

    ledger["year"] = pd.to_numeric(
        ledger["year"],
        errors="coerce"
    )

    ledger["signal_return"] = pd.to_numeric(
        ledger["signal_return"],
        errors="coerce"
    )

    ledger["spread"] = pd.to_numeric(
        ledger["spread"],
        errors="coerce"
    )

    ledger = ledger.replace(
        [np.inf, -np.inf],
        np.nan
    )

    ledger = ledger.dropna(
        subset=[
            "year",
            "signal_return",
            "spread",
        ]
    )

    return ledger


def build_net_returns(
    df: pd.DataFrame,
    cost_scenario: str,
) -> pd.Series:

    scenario_map = {
        "half_spread_plus_low_commission": {
            "spread_fraction": 0.5,
            "commission": 0.000005,
        },
        "spread_only": {
            "spread_fraction": 1.0,
            "commission": 0.0,
        },
        "spread_plus_low_commission": {
            "spread_fraction": 1.0,
            "commission": 0.000005,
        },
        "spread_plus_medium_commission": {
            "spread_fraction": 1.0,
            "commission": 0.000010,
        },
    }

    params = scenario_map[cost_scenario]

    total_cost = (
        df["spread"] * params["spread_fraction"]
        + params["commission"]
    )

    return df["signal_return"] - total_cost


def validate_context(
    context_row: pd.Series,
    ledger: pd.DataFrame,
) -> dict | None:

    filtered = apply_context_filter(
        ledger,
        context_row["context_label"]
    )

    if filtered.empty:
        return None

    cost_scenario = context_row["cost_scenario"]

    result = {
        "context_group":
            context_row["context_group"],
        "context_label":
            context_row["context_label"],
        "cost_scenario":
            cost_scenario,
    }

    profitable_years = 0

    for year, prefix in [
        (2023, "train"),
        (2024, "validation"),
        (2025, "oos"),
    ]:

        year_df = filtered[
            filtered["year"] == year
        ].copy()

        net_returns = build_net_returns(
            year_df,
            cost_scenario
        )

        stats = evaluate_returns(
            net_returns
        )

        result[
            f"{prefix}_trade_count"
        ] = stats["trade_count"]

        result[
            f"{prefix}_win_rate"
        ] = stats["win_rate"]

        result[
            f"{prefix}_total_return"
        ] = stats["total_return"]

        result[
            f"{prefix}_profit_factor"
        ] = stats["profit_factor"]

        if (
            stats["trade_count"]
            >= MIN_TRADES_PER_YEAR
            and stats["total_return"] > 0
        ):
            profitable_years += 1

    result[
        "profitable_years"
    ] = profitable_years

    result[
        "oos_label"
    ] = classify_oos(
        pd.Series(result)
    )

    return result


def main() -> None:

    print("=" * 90)
    print("BACQE DUKASCOPY 58 - CONTEXT INTERSECTION VALIDATION")
    print("=" * 90)
    print(f"Symbol:        {SYMBOL}")
    print(f"Survivors:     {SURVIVOR_FILE}")
    print(f"Ledger:        {LEDGER_FILE}")
    print(f"Output root:   {OUTPUT_ROOT}")
    print("-" * 90)

    ensure_dirs(OUTPUT_ROOT)

    survivors, ledger = load_inputs()

    survivors = prepare_survivors(
        survivors
    )

    ledger = prepare_ledger(
        ledger
    )

    print(
        f"Strong survivors loaded: "
        f"{len(survivors):,}"
    )

    print(
        f"Ledger rows loaded: "
        f"{len(ledger):,}"
    )

    results = []

    total = len(survivors)

    for idx, (_, row) in enumerate(
        survivors.iterrows(),
        start=1,
    ):

        print(
            f"[{idx}/{total}] "
            f"{row['context_label']}"
        )

        validation = validate_context(
            row,
            ledger,
        )

        if validation is not None:
            results.append(
                validation
            )

    if not results:
        print(
            "[STOP] No validation "
            "results generated."
        )
        return

    results_df = pd.DataFrame(
        results
    )

    results_df = results_df.sort_values(
        [
            "oos_label",
            "oos_profit_factor",
            "validation_profit_factor",
            "train_profit_factor",
        ],
        ascending=[
            True,
            False,
            False,
            False,
        ],
    )

    results_df.insert(
        0,
        "oos_rank",
        range(
            1,
            len(results_df) + 1
        ),
    )

    yearly_path = (
        OUTPUT_ROOT
        / "yearly_validation"
        / "context_yearly_validation_latest.csv"
    )

    ranked_path = (
        OUTPUT_ROOT
        / "oos_rankings"
        / "context_intersection_ranked_latest.csv"
    )

    report_path = (
        OUTPUT_ROOT
        / "reports"
        / "context_intersection_validation_report_latest.txt"
    )

    results_df.to_csv(
        yearly_path,
        index=False,
    )

    results_df.to_csv(
        ranked_path,
        index=False,
    )

    with open(
        report_path,
        "w",
        encoding="utf-8",
    ) as f:

        f.write(
            "BACQE DUKASCOPY CONTEXT "
            "INTERSECTION VALIDATION REPORT\n"
        )

        f.write(
            "=" * 80 + "\n\n"
        )

        f.write(
            f"Symbol: {SYMBOL}\n"
        )

        f.write(
            f"Strong survivors: "
            f"{len(survivors):,}\n"
        )

        f.write(
            f"Validated contexts: "
            f"{len(results_df):,}\n\n"
        )

        f.write(
            "OOS LABEL COUNTS\n"
        )

        f.write(
            "-" * 80 + "\n"
        )

        f.write(
            results_df[
                "oos_label"
            ].value_counts().to_string()
        )

        f.write("\n\n")

        f.write(
            "TOP VALIDATED CONTEXTS\n"
        )

        f.write(
            "-" * 80 + "\n"
        )

        cols = [
            "oos_rank",
            "context_group",
            "context_label",
            "cost_scenario",
            "train_trade_count",
            "train_profit_factor",
            "validation_trade_count",
            "validation_profit_factor",
            "oos_trade_count",
            "oos_profit_factor",
            "profitable_years",
            "oos_label",
        ]

        f.write(
            results_df[
                cols
            ]
            .head(50)
            .to_string(index=False)
        )

    print("=" * 90)
    print(
        "[DONE] Context intersection "
        "validation complete."
    )
    print(
        f"Results: {ranked_path}"
    )
    print(
        f"Report:  {report_path}"
    )
    print("=" * 90)

if __name__ == "__main__":
    main()