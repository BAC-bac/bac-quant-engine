"""EH05: classify survival under observed bid/ask execution economics."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import numpy as np
import pandas as pd

DUKASCOPY_TICKS_DIR = Path(__file__).resolve().parents[1] / "dukascopy_ticks"
if str(DUKASCOPY_TICKS_DIR) not in sys.path:
    sys.path.insert(0, str(DUKASCOPY_TICKS_DIR))

from dukascopy_contract import get_symbol_metadata  # noqa: E402
from extended_horizons_e2_contract import (  # noqa: E402
    EH05_COST_METHODOLOGY_VERSION, ECONOMIC_UNIT_MODEL, EXECUTION_MODEL,
    economic_contract_id,
)

DEFAULT_SYMBOL = "EURJPY"
BASE_DIR = Path("E:/Quant_Lab")
VALIDATION_ROOT = BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "signal_validation"
REPORT_ROOT = BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "cost_survival"
BASE_COST_SCENARIO = "observed_bid_ask_round_trip"


def load_validation_raw(symbol: str) -> pd.DataFrame:
    path = VALIDATION_ROOT / f"{symbol.lower()}_extended_horizon_signal_validation_raw_latest.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing Script 04 raw validation file: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError("Script 04 raw validation report is empty")
    return frame


def apply_cost_scenarios(raw: pd.DataFrame) -> pd.DataFrame:
    """Expose the observed bid/ask execution result as the only E2 base model.

    Legacy pip subtraction is intentionally removed: pips and quote-price
    distances are not dimensionless returns. EH06 owns explicit spread stress.
    """
    required = {
        "file", "target", "feature", "candidate_side", "threshold_quantile",
        "threshold_side", "threshold_operator", "learned_threshold_value",
        "candidate_contract_id", "evaluation_status", "execution_evidence_status",
        "gross_mid_trades", "gross_mid_win_rate", "gross_mid_avg_return",
        "gross_mid_median_return", "gross_mid_total_return", "gross_mid_profit_factor",
        "executable_trades", "executable_win_rate", "executable_avg_return",
        "executable_median_return", "executable_total_return", "executable_profit_factor",
        "validation_methodology_version", "discovery_methodology_version",
        "input_dataset_fingerprint", "economic_unit_model", "execution_model",
        "expected_files", "attempted_files", "successful_files", "failed_files",
        "skipped_files", "coverage_status", "file_date",
    }
    missing = sorted(required - set(raw.columns))
    if missing:
        raise ValueError(f"EH05 requires canonical EH04 execution fields: {missing}")
    if not (raw["evaluation_status"] == "success").all():
        raise ValueError("EH05 refuses incomplete/failed/skipped EH04 evidence")
    if not (raw["execution_evidence_status"] == "executable_observed_bid_ask").all():
        raise ValueError("EH05 cannot claim cost survival without observed bid/ask execution evidence")
    if not (raw["economic_unit_model"] == ECONOMIC_UNIT_MODEL).all():
        raise ValueError("EH05 input economic unit model is incompatible")
    if not (raw["execution_model"] == EXECUTION_MODEL).all():
        raise ValueError("EH05 input execution model is incompatible")
    if not (raw["coverage_status"] == "complete").all():
        raise ValueError("EH05 refuses incomplete EH04 coverage")

    output = raw.copy()
    output["cost_scenario"] = BASE_COST_SCENARIO
    output["cost_input_field"] = "bid,ask,mid"
    output["cost_input_unit"] = "quote_price_units"
    output["cost_transformation"] = EXECUTION_MODEL
    output["cost_interpretation"] = "observed entry and exit crossing embedded in executable return"
    output["cost_component_avg_return"] = output["gross_mid_avg_return"] - output["executable_avg_return"]
    output["cost_component_total_return"] = output["gross_mid_total_return"] - output["executable_total_return"]
    output["cost_component_unit"] = "dimensionless_decimal_simple_return"
    output["cost_methodology_version"] = EH05_COST_METHODOLOGY_VERSION
    output["economic_contract_id"] = output["candidate_contract_id"].map(
        lambda value: economic_contract_id(value, BASE_COST_SCENARIO, 1.0)
    )
    # Safe compatibility aliases: these are observed executable returns, not a
    # heuristic subtraction from gross returns.
    output["net_avg_return"] = output["executable_avg_return"]
    output["net_median_return"] = output["executable_median_return"]
    output["net_total_return"] = output["executable_total_return"]
    return output


def aggregate_cost_survival(costed: pd.DataFrame) -> pd.DataFrame:
    identity = [
        "cost_scenario", "target", "feature", "candidate_side", "threshold_quantile",
        "threshold_side", "threshold_operator", "learned_threshold_value",
        "candidate_contract_id", "economic_contract_id",
    ]
    grouped = costed.groupby(identity, dropna=False).agg(
        expected_files=("expected_files", "max"), attempted_files=("attempted_files", "max"),
        successful_files=("evaluation_status", lambda s: int((s == "success").sum())),
        failed_files=("evaluation_status", lambda s: int((s == "failed").sum())),
        skipped_files=("evaluation_status", lambda s: int((s == "skipped").sum())),
        total_trades=("executable_trades", "sum"),
        gross_mid_total_return=("gross_mid_total_return", "sum"),
        gross_mid_median_avg_return=("gross_mid_avg_return", "median"),
        executable_total_return=("executable_total_return", "sum"),
        executable_median_avg_return=("executable_avg_return", "median"),
        executable_median_win_rate=("executable_win_rate", "median"),
        executable_median_profit_factor=("executable_profit_factor", "median"),
        cost_component_total_return=("cost_component_total_return", "sum"),
        input_dataset_fingerprint=("input_dataset_fingerprint", "first"),
        discovery_methodology_version=("discovery_methodology_version", "first"),
        validation_methodology_version=("validation_methodology_version", "first"),
        cost_methodology_version=("cost_methodology_version", "first"),
        economic_unit_model=("economic_unit_model", "first"),
        execution_model=("execution_model", "first"),
        evaluation_dataset_fingerprint=("evaluation_dataset_fingerprint", "first"),
        threshold_learning_method=("threshold_learning_method", "first"),
        threshold_provenance=("threshold_provenance", "first"),
        selected_side_provenance=("selected_side_provenance", "first"),
        symbol_metadata_schema_version=("symbol_metadata_schema_version", "first"),
        symbol_registry_fingerprint=("symbol_registry_fingerprint", "first"),
        feature_role_contract_version=("feature_role_contract_version", "first"),
        target_contract_version=("target_contract_version", "first"),
        feature_contract_fingerprint=("feature_contract_fingerprint", "first"),
        earliest_processed_date=("file_date", "min"), latest_processed_date=("file_date", "max"),
    ).reset_index()
    grouped["files_tested"] = grouped["successful_files"]
    grouped["coverage_status"] = np.where(
        (grouped["successful_files"] == grouped["expected_files"])
        & (grouped["failed_files"] == 0) & (grouped["skipped_files"] == 0), "complete", "incomplete"
    )
    grouped["cost_survival_score"] = (
        grouped["executable_total_return"].fillna(0)
        + grouped["executable_median_avg_return"].fillna(0) * 100000
        + (grouped["executable_median_win_rate"].fillna(0.5) - 0.5) * 100
    )
    complete = grouped["coverage_status"] == "complete"
    grouped["survival_status"] = np.select(
        [~complete,
         (grouped["executable_total_return"] > 0) & (grouped["executable_median_avg_return"] > 0)
         & (grouped["executable_median_win_rate"] > 0.52) & (grouped["executable_median_profit_factor"] > 1.10),
         (grouped["executable_total_return"] > 0) & (grouped["executable_median_avg_return"] > 0)
         & (grouped["executable_median_win_rate"] > 0.505), grouped["executable_total_return"] <= 0],
        ["cost_evidence_incomplete", "cost_survivor_primary", "cost_survivor_secondary", "cost_fail"],
        default="cost_watchlist",
    )
    grouped["net_total_return"] = grouped["executable_total_return"]
    grouped["net_median_avg_return"] = grouped["executable_median_avg_return"]
    return grouped.sort_values(["survival_status", "cost_survival_score"], ascending=[True, False])


def write_outputs(symbol: str, raw: pd.DataFrame, ranked: pd.DataFrame) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    prefix = f"{symbol.lower()}_extended_horizon_cost_survival"
    raw.to_csv(REPORT_ROOT / f"{prefix}_raw_latest.csv", index=False)
    ranked.to_csv(REPORT_ROOT / f"{prefix}_ranked_latest.csv", index=False)
    ranked[ranked["survival_status"].isin(["cost_survivor_primary", "cost_survivor_secondary"])].to_csv(
        REPORT_ROOT / f"{symbol.lower()}_extended_horizon_cost_survivors_latest.csv", index=False
    )


def main(symbol: str) -> None:
    get_symbol_metadata(symbol)  # metadata-driven symbol certification; no JPY branch
    costed = apply_cost_scenarios(load_validation_raw(symbol))
    write_outputs(symbol, costed, aggregate_cost_survival(costed))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    args = parser.parse_args()
    main(args.symbol.upper())
