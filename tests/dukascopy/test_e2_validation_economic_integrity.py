from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TICK_DIR = REPO_ROOT / "scripts" / "dukascopy_ticks"
EH_DIR = REPO_ROOT / "scripts" / "dukascopy_extended_horizons"
sys.path.insert(0, str(TICK_DIR))
sys.path.insert(0, str(EH_DIR))

from dukascopy_contract import get_symbol_metadata  # noqa: E402
from extended_horizons_e2_contract import (  # noqa: E402
    E2ContractError, candidate_contract_id, economic_contract_id, trade_returns,
)


def load_script(filename: str, name: str):
    path = EH_DIR / filename
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


eh04 = load_script("04_extended_horizon_signal_validation.py", "e2_eh04")
eh05 = load_script("05_extended_horizon_cost_survival_engine.py", "e2_eh05")
eh06 = load_script("06_extended_horizon_dynamic_cost_engine.py", "e2_eh06")


def candidate(side: str = "long", threshold: float = 0.0) -> dict:
    is_long = side == "long"
    row = {
        "feature": "mid_return_1", "target": "future_return_1",
        "selected_side": side, "candidate_side": side, "best_side": side,
        "threshold_quantile": 0.75 if is_long else 0.25,
        "threshold_side": "upper" if is_long else "lower",
        "threshold_operator": ">=" if is_long else "<=",
        "learned_threshold_value": threshold, "threshold_value": threshold,
        "threshold_learning_method": "median_of_e1_file_feature_only_q25_q75",
        "threshold_provenance": "frozen_e1_candidate",
        "selected_side_method": "e1_stability_selection",
        "discovery_methodology_version": "extended_horizon_discovery_integrity_e1_v1",
        "stability_methodology_version": "extended_horizon_stability_integrity_e1_v1",
        "input_dataset_fingerprint": "a" * 64,
        "discovery_interval_start": "2024-01-01", "discovery_interval_end": "2024-01-31",
        "stability_score": 10.0, "stability_status": "stable_candidate",
        "validation_status": "validation_pass_primary", "validation_score": 20.0,
    }
    row["candidate_contract_id"] = candidate_contract_id(row)
    return row


def frame(feature_values: list[float] | None = None) -> pd.DataFrame:
    feature_values = feature_values or [-1.0, 1.0, 1.0, 1.0]
    mid = pd.Series([100.0, 101.0, 102.0, 103.0])
    bid = mid - 0.1
    ask = mid + 0.1
    return pd.DataFrame({
        "mid_return_1": feature_values, "mid": mid, "bid": bid, "ask": ask,
        "future_return_1": mid.shift(-1) / mid - 1.0,
    })


def validation_row(data: pd.DataFrame | None = None, rule: dict | None = None) -> dict:
    row = eh04.validate_candidate_on_file(
        data if data is not None else frame(), rule if rule is not None else candidate(),
        Path("EURJPY_2024-01-01_features.parquet"), min_trades=1,
    )
    row.update({
        "expected_files": 1, "attempted_files": 1, "successful_files": 1,
        "failed_files": 0, "skipped_files": 0, "coverage_status": "complete",
    })
    return row


def test_eh04_uses_frozen_threshold_not_evaluation_quantile() -> None:
    rule = candidate(threshold=0.123)
    first = validation_row(frame([-10.0, 1.0, 2.0, 3.0]), rule)
    second = validation_row(frame([-1000.0, 100.0, 200.0, 300.0]), rule)
    assert first["learned_threshold_value"] == second["learned_threshold_value"] == 0.123
    assert first["threshold_learning_method"] == rule["threshold_learning_method"]


def test_target_null_pattern_cannot_change_rule_definition() -> None:
    first = frame()
    second = first.copy()
    second.loc[[1, 2], "future_return_1"] = np.nan
    a, b = validation_row(first), validation_row(second)
    assert (a["learned_threshold_value"], a["threshold_side"]) == (
        b["learned_threshold_value"], b["threshold_side"]
    )


@pytest.mark.parametrize("side", ["long", "short"])
def test_eh04_preserves_selected_side(side: str) -> None:
    rule = candidate(side, threshold=0.0)
    row = validation_row(frame(), rule)
    assert row["candidate_side"] == row["selected_side"] == side


def test_incoherent_side_cannot_be_flipped_for_validation() -> None:
    rule = candidate("long")
    rule["candidate_side"] = rule["selected_side"] = "short"
    with pytest.raises(E2ContractError, match="incoherent"):
        eh04.validate_candidate_on_file(frame(), rule, Path("EURJPY_2024-01-01.parquet"), min_trades=1)


def test_long_executable_return_matches_bid_ask_formula() -> None:
    data = pd.DataFrame({
        "mid_return_1": [1.0, -1.0], "mid": [100.0, 110.0],
        "bid": [99.0, 109.0], "ask": [101.0, 111.0], "future_return_1": [0.1, np.nan],
    })
    _, executable, _ = trade_returns(data, candidate("long"))
    assert executable is not None
    assert executable.iloc[0] == pytest.approx(109.0 / 101.0 - 1.0)


def test_short_executable_return_matches_bid_ask_formula() -> None:
    data = pd.DataFrame({
        "mid_return_1": [-1.0, 1.0], "mid": [100.0, 90.0],
        "bid": [99.0, 89.0], "ask": [101.0, 91.0], "future_return_1": [-0.1, np.nan],
    })
    _, executable, _ = trade_returns(data, candidate("short"))
    assert executable is not None
    assert executable.iloc[0] == pytest.approx(1.0 - 91.0 / 99.0)


def test_jpy_and_non_jpy_use_metadata_without_ad_hoc_return_scaling() -> None:
    assert get_symbol_metadata("EURJPY").raw_price_scale == 1_000
    assert get_symbol_metadata("EURUSD").raw_price_scale == 100_000
    source = (EH_DIR / "05_extended_horizon_cost_survival_engine.py").read_text(encoding="utf-8")
    assert "JPY_PIP_SIZE" not in source and "spread_points" not in source


def test_eh05_separates_gross_cost_and_executable_evidence() -> None:
    costed = eh05.apply_cost_scenarios(pd.DataFrame([validation_row()])).iloc[0]
    assert costed["gross_mid_avg_return"] != costed["executable_avg_return"]
    assert costed["cost_component_avg_return"] == pytest.approx(
        costed["gross_mid_avg_return"] - costed["executable_avg_return"]
    )
    assert costed["net_avg_return"] == costed["executable_avg_return"]


def test_eh05_fails_closed_without_executable_semantics() -> None:
    no_quotes = frame().drop(columns=["bid", "ask"])
    raw = validation_row(no_quotes)
    with pytest.raises(ValueError, match="observed bid/ask"):
        eh05.apply_cost_scenarios(pd.DataFrame([raw]))


def test_quote_spread_and_spread_points_are_not_return_cost_inputs() -> None:
    with pytest.raises(ValueError, match="spread-like substitutes"):
        eh06.validate_execution_columns(["mid", "spread", "spread_points"])
    source = (EH_DIR / "06_extended_horizon_dynamic_cost_engine.py").read_text(encoding="utf-8")
    assert "detect_spread_column" not in source


def test_explicit_spread_stress_is_unit_consistent_and_monotone() -> None:
    rows = eh06.calculate_signal_returns(
        frame(), candidate(), Path("EURJPY_2024-01-01_features.parquet"), min_trades=1
    )
    observed = next(row for row in rows if row["cost_multiplier"] == 1.0)
    stressed = next(row for row in rows if row["cost_multiplier"] == 1.5)
    assert observed["cost_input_unit"] == "quote_price_units"
    assert stressed["executable_avg_return"] < observed["executable_avg_return"]
    assert stressed["gross_avg_return"] == pytest.approx(observed["gross_avg_return"])


def test_incomplete_coverage_cannot_produce_pass_status() -> None:
    rows = pd.DataFrame([validation_row()])
    ranked = eh04.aggregate_validation(rows, expected_files=2)
    assert ranked.iloc[0]["coverage_status"] == "incomplete"
    assert ranked.iloc[0]["validation_status"] == "validation_incomplete"


def test_candidate_and_economic_ids_bind_rule_and_execution() -> None:
    first = candidate("long", 0.0)
    changed_threshold = candidate("long", 0.1)
    changed_side = candidate("short", 0.0)
    assert len({candidate_contract_id(first), candidate_contract_id(changed_threshold), candidate_contract_id(changed_side)}) == 3
    base = economic_contract_id(candidate_contract_id(first), "observed_bid_ask", 1.0)
    stress = economic_contract_id(candidate_contract_id(first), "observed_spread_stress", 1.5)
    assert base != stress


def test_eh07_required_compatibility_fields_remain_available() -> None:
    raw = pd.DataFrame(eh06.calculate_signal_returns(
        frame(), candidate(), Path("EURJPY_2024-01-01_features.parquet"), min_trades=1
    ))
    ranked = eh06.aggregate_dynamic_cost(raw, expected_files=1)
    required = {
        "target", "feature", "candidate_side", "threshold_quantile", "threshold_side",
        "dynamic_cost_scenario", "dynamic_survival_status", "dynamic_cost_score",
        "net_total_return", "median_net_avg_return", "median_net_win_rate", "median_net_profit_factor",
    }
    assert required.issubset(ranked.columns)
