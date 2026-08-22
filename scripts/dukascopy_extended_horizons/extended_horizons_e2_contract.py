"""Shared E2 rule identity, units, and executable-return contract."""

from __future__ import annotations

from hashlib import sha256
import json
import re
from typing import Any

import numpy as np
import pandas as pd


EH04_VALIDATION_METHODOLOGY_VERSION = "eh04_frozen_post_selection_validation_e2_v1"
EH05_COST_METHODOLOGY_VERSION = "eh05_observed_bid_ask_cost_survival_e2_v1"
EH06_DYNAMIC_COST_METHODOLOGY_VERSION = "eh06_explicit_spread_stress_e2_v1"
ECONOMIC_UNIT_MODEL = "quote_price_to_dimensionless_simple_return_e2_v1"
EXECUTION_MODEL = "observed_bid_ask_entry_exit_simple_return_v1"
VALIDATION_EVIDENCE_CLASS = "within_discovery_post_selection_validation"
THRESHOLD_OPERATOR = {"lower": "<=", "upper": ">="}
TARGET_PATTERN = re.compile(r"^future_return_(\d+)$")


class E2ContractError(ValueError):
    """Raised when a rule or economic input is outside the E2 contract."""


def target_horizon(target: str) -> int:
    match = TARGET_PATTERN.fullmatch(str(target))
    if match is None or int(match.group(1)) <= 0:
        raise E2ContractError(f"Unsupported forward-return target: {target!r}")
    return int(match.group(1))


def _finite_float(value: Any, label: str) -> float:
    result = float(value)
    if not np.isfinite(result):
        raise E2ContractError(f"{label} must be finite, got {value!r}")
    return result


def normalise_rule(candidate: dict[str, Any]) -> dict[str, Any]:
    side = str(candidate.get("selected_side", candidate.get("candidate_side", candidate.get("best_side", ""))))
    threshold_side = str(candidate.get("threshold_side", ""))
    if side not in {"long", "short"}:
        raise E2ContractError(f"selected side must be long/short, got {side!r}")
    if threshold_side not in THRESHOLD_OPERATOR:
        raise E2ContractError(f"threshold_side must be lower/upper, got {threshold_side!r}")
    threshold = _finite_float(
        candidate.get("learned_threshold_value", candidate.get("threshold_value")),
        "learned_threshold_value",
    )
    quantile = _finite_float(candidate.get("threshold_quantile"), "threshold_quantile")
    if not 0 < quantile < 1:
        raise E2ContractError(f"threshold_quantile must be in (0, 1), got {quantile}")
    expected_side = "upper" if side == "long" else "lower"
    expected_quantile = 0.75 if side == "long" else 0.25
    if threshold_side != expected_side or not np.isclose(quantile, expected_quantile):
        raise E2ContractError(
            "E1 directional rule is incoherent: "
            f"side={side}, threshold_side={threshold_side}, quantile={quantile}"
        )
    return {
        "feature": str(candidate["feature"]),
        "target": str(candidate["target"]),
        "selected_side": side,
        "candidate_side": side,
        "threshold_quantile": quantile,
        "threshold_side": threshold_side,
        "threshold_operator": THRESHOLD_OPERATOR[threshold_side],
        "learned_threshold_value": threshold,
        "threshold_value": threshold,
    }


def candidate_contract_id(candidate: dict[str, Any]) -> str:
    rule = normalise_rule(candidate)
    payload = {
        **rule,
        "threshold_learning_method": str(candidate.get("threshold_learning_method", "")),
        "discovery_methodology_version": str(candidate.get("discovery_methodology_version", "")),
        "input_dataset_fingerprint": str(candidate.get("input_dataset_fingerprint", "")),
        "discovery_interval_start": str(candidate.get("discovery_interval_start", "")),
        "discovery_interval_end": str(candidate.get("discovery_interval_end", "")),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "e2c_" + sha256(encoded).hexdigest()


def economic_contract_id(candidate_id: str, scenario: str, spread_multiplier: float) -> str:
    payload = {
        "candidate_contract_id": str(candidate_id),
        "economic_unit_model": ECONOMIC_UNIT_MODEL,
        "execution_model": EXECUTION_MODEL,
        "scenario": str(scenario),
        "spread_multiplier": _finite_float(spread_multiplier, "spread_multiplier"),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "e2e_" + sha256(encoded).hexdigest()


def frozen_signal_mask(feature: pd.Series, threshold: float, threshold_side: str) -> pd.Series:
    values = pd.to_numeric(feature, errors="coerce").replace([np.inf, -np.inf], np.nan)
    value = _finite_float(threshold, "threshold")
    if threshold_side == "lower":
        return values.notna() & (values <= value)
    if threshold_side == "upper":
        return values.notna() & (values >= value)
    raise E2ContractError(f"Unapproved threshold side: {threshold_side!r}")


def trade_returns(
    frame: pd.DataFrame,
    candidate: dict[str, Any],
    *,
    spread_multiplier: float = 1.0,
) -> tuple[pd.Series, pd.Series | None, pd.Series]:
    """Return gross mid, executable bid/ask, and the frozen predictor mask.

    Long: ``Bid_exit / Ask_entry - 1``.
    Short: ``1 - Ask_exit / Bid_entry`` (profit divided by short-sale entry proceeds).
    Spread stress scales each observed half-spread around ``mid`` before applying
    the same formulas. Returns are dimensionless decimal simple returns.
    """
    rule = normalise_rule(candidate)
    feature = rule["feature"]
    target = rule["target"]
    required = {feature, target}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise E2ContractError(f"Missing frozen-rule columns: {missing}")
    signal_mask = frozen_signal_mask(
        frame[feature], rule["learned_threshold_value"], rule["threshold_side"]
    )
    target_values = pd.to_numeric(frame[target], errors="coerce").replace([np.inf, -np.inf], np.nan)
    gross = target_values if rule["selected_side"] == "long" else -target_values

    execution_columns = {"bid", "ask", "mid"}
    if not execution_columns.issubset(frame.columns):
        return gross[signal_mask].dropna(), None, signal_mask

    multiplier = _finite_float(spread_multiplier, "spread_multiplier")
    if multiplier < 1.0:
        raise E2ContractError("Executable spread multipliers below 1.0 are not approved")
    horizon = target_horizon(target)
    bid = pd.to_numeric(frame["bid"], errors="coerce")
    ask = pd.to_numeric(frame["ask"], errors="coerce")
    mid = pd.to_numeric(frame["mid"], errors="coerce")
    if ((ask - bid).dropna() < 0).any():
        raise E2ContractError("Crossed bid/ask observations cannot be execution evidence")
    half_spread = (ask - bid) / 2.0
    stressed_bid = mid - multiplier * half_spread
    stressed_ask = mid + multiplier * half_spread
    bid_exit = stressed_bid.shift(-horizon)
    ask_exit = stressed_ask.shift(-horizon)
    if rule["selected_side"] == "long":
        executable = bid_exit / stressed_ask - 1.0
    else:
        executable = 1.0 - ask_exit / stressed_bid
    executable = executable.replace([np.inf, -np.inf], np.nan)
    valid = signal_mask & gross.notna() & executable.notna()
    return gross[valid], executable[valid], signal_mask


def profit_factor(returns: pd.Series) -> float:
    clean = pd.to_numeric(returns, errors="coerce").dropna()
    wins = clean[clean > 0].sum()
    losses = clean[clean < 0].sum()
    if losses == 0:
        return np.inf if wins > 0 else np.nan
    return float(wins / abs(losses))
