"""Authoritative D2 roles and causal dependencies for Dukascopy features."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


FEATURE_SCHEMA_VERSION = "dukascopy_engineered_features_v2"
FEATURE_ROLE_CONTRACT_VERSION = "dukascopy_feature_roles_v1"
CAUSAL_LOOKBACK_SPEC_VERSION = "dukascopy_causal_lookback_v1"
TARGET_CONTRACT_VERSION = "dukascopy_event_time_targets_v1"
APPROVED_TARGET_PRICE_BASIS = "mid"

RAW_MARKET_FIELD = "raw_market_field"
PREDICTOR_CAUSAL = "predictor_causal"
TARGET_FORWARD = "target_forward"
CONTEXT = "context"
IDENTIFIER = "identifier"
DIAGNOSTIC_ONLY = "diagnostic_only"
ROLES = {
    RAW_MARKET_FIELD,
    PREDICTOR_CAUSAL,
    TARGET_FORWARD,
    CONTEXT,
    IDENTIFIER,
    DIAGNOSTIC_ONLY,
}

ROLLING_WINDOWS = (5, 10, 25, 50, 100, 250)
SCRIPT23_FORWARD_HORIZONS = (1, 3, 5, 10, 20, 50)
SCRIPT30_FORWARD_HORIZONS = (1, 3, 5, 10, 20, 25, 50, 100, 250, 500, 1000)
EH01_DEFAULT_HORIZONS = (2500, 5000, 10000, 20000)


class FeatureContractError(ValueError):
    """Raised when a column or dataset violates the D2 feature contract."""


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    role: str
    dependencies: tuple[str, ...]
    lookback_observations: int
    lookahead_observations: int
    definition: str

    def __post_init__(self) -> None:
        if self.role not in ROLES:
            raise FeatureContractError(f"Unknown feature role {self.role!r}")
        if self.lookback_observations < 0 or self.lookahead_observations < 0:
            raise FeatureContractError(f"Negative dependency for {self.name}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _spec(
    name: str,
    role: str,
    dependencies: Iterable[str] = (),
    *,
    lookback: int = 0,
    lookahead: int = 0,
    definition: str,
) -> ColumnSpec:
    return ColumnSpec(
        name=name,
        role=role,
        dependencies=tuple(dependencies),
        lookback_observations=lookback,
        lookahead_observations=lookahead,
        definition=definition,
    )


def _base_specs() -> list[ColumnSpec]:
    specs = [
        _spec("timestamp_utc", IDENTIFIER, definition="UTC event timestamp"),
        _spec("symbol", IDENTIFIER, definition="certified Dukascopy symbol"),
        _spec("source", IDENTIFIER, definition="market-data source identifier"),
        _spec("bid", RAW_MARKET_FIELD, definition="normalised best bid"),
        _spec("ask", RAW_MARKET_FIELD, definition="normalised best ask"),
        _spec("mid", RAW_MARKET_FIELD, ("bid", "ask"), definition="(bid + ask) / 2"),
        _spec("spread", RAW_MARKET_FIELD, ("ask", "bid"), definition="ask - bid"),
        _spec("spread_points", RAW_MARKET_FIELD, ("spread", "point_size"), definition="spread / point_size"),
        _spec("bid_volume", RAW_MARKET_FIELD, definition="Dukascopy bid volume"),
        _spec("ask_volume", RAW_MARKET_FIELD, definition="Dukascopy ask volume"),
        _spec("quote_volume", RAW_MARKET_FIELD, ("bid_volume", "ask_volume"), definition="bid_volume + ask_volume"),
        _spec("hour", CONTEXT, ("timestamp_utc",), definition="UTC event hour"),
        _spec("year", CONTEXT, ("timestamp_utc",), definition="UTC event year"),
        _spec("month", CONTEXT, ("timestamp_utc",), definition="UTC event month"),
        _spec("day_of_week", CONTEXT, ("timestamp_utc",), definition="UTC weekday"),
        _spec("session", CONTEXT, ("hour",), definition="named UTC session"),
        _spec("spread_regime", CONTEXT, ("spread",), definition="downstream spread context"),
        _spec("volatility_regime", CONTEXT, definition="downstream volatility context"),
    ]
    for name in (
        "normalisation_schema_version",
        "symbol_metadata_version",
        "symbol_registry_fingerprint",
        "raw_price_scale",
        "point_size",
        "pip_size",
        "coverage_status",
        "source_fingerprint",
    ):
        specs.append(_spec(name, DIAGNOSTIC_ONLY, definition="D1 lineage metadata"))
    return specs


def _predictor_specs() -> list[ColumnSpec]:
    specs = [
        _spec("mid_return_1", PREDICTOR_CAUSAL, ("mid",), lookback=1, definition="mid.pct_change(1)"),
        _spec("mid_return_5", PREDICTOR_CAUSAL, ("mid",), lookback=5, definition="mid.pct_change(5)"),
        _spec("mid_return_10", PREDICTOR_CAUSAL, ("mid",), lookback=10, definition="mid.pct_change(10)"),
        _spec("mid_return_25", PREDICTOR_CAUSAL, ("mid",), lookback=25, definition="mid.pct_change(25)"),
        _spec("mid_diff_1", PREDICTOR_CAUSAL, ("mid",), lookback=1, definition="mid.diff(1)"),
        _spec("bid_diff_1", PREDICTOR_CAUSAL, ("bid",), lookback=1, definition="bid.diff(1)"),
        _spec("ask_diff_1", PREDICTOR_CAUSAL, ("ask",), lookback=1, definition="ask.diff(1)"),
        _spec("spread_change", PREDICTOR_CAUSAL, ("spread",), lookback=1, definition="spread.diff(1)"),
        _spec("spread_pct_change", PREDICTOR_CAUSAL, ("spread",), lookback=1, definition="spread.pct_change(1)"),
        _spec("tick_direction", PREDICTOR_CAUSAL, ("mid_diff_1",), lookback=1, definition="sign(mid_diff_1), initial stream value zero"),
        _spec("up_tick", PREDICTOR_CAUSAL, ("tick_direction",), lookback=1, definition="tick_direction > 0"),
        _spec("down_tick", PREDICTOR_CAUSAL, ("tick_direction",), lookback=1, definition="tick_direction < 0"),
        _spec("flat_tick", PREDICTOR_CAUSAL, ("tick_direction",), lookback=1, definition="tick_direction == 0"),
        _spec("signed_tick", PREDICTOR_CAUSAL, ("tick_direction",), lookback=1, definition="tick_direction"),
        _spec("absolute_mid_move", PREDICTOR_CAUSAL, ("mid_diff_1",), lookback=1, definition="abs(mid_diff_1)"),
        _spec("price_acceleration", PREDICTOR_CAUSAL, ("mid_diff_1",), lookback=2, definition="mid_diff_1.diff(1)"),
    ]
    for window in ROLLING_WINDOWS:
        specs.extend(
            [
                _spec(f"rolling_return_mean_{window}", PREDICTOR_CAUSAL, ("mid_return_1",), lookback=window, definition=f"trailing mean of {window} one-tick returns"),
                _spec(f"rolling_return_std_{window}", PREDICTOR_CAUSAL, ("mid_return_1",), lookback=window, definition=f"trailing sample std of {window} one-tick returns"),
                _spec(f"rolling_spread_mean_{window}", PREDICTOR_CAUSAL, ("spread",), lookback=window - 1, definition=f"trailing mean of {window} spreads"),
                _spec(f"rolling_spread_std_{window}", PREDICTOR_CAUSAL, ("spread",), lookback=window - 1, definition=f"trailing sample std of {window} spreads"),
                _spec(f"rolling_up_ticks_{window}", PREDICTOR_CAUSAL, ("up_tick",), lookback=window, definition=f"trailing sum of {window} up-tick indicators"),
                _spec(f"rolling_down_ticks_{window}", PREDICTOR_CAUSAL, ("down_tick",), lookback=window, definition=f"trailing sum of {window} down-tick indicators"),
                _spec(f"rolling_flat_ticks_{window}", PREDICTOR_CAUSAL, ("flat_tick",), lookback=window, definition=f"trailing sum of {window} flat-tick indicators"),
                _spec(f"tick_imbalance_{window}", PREDICTOR_CAUSAL, (f"rolling_up_ticks_{window}", f"rolling_down_ticks_{window}"), lookback=window, definition="rolling_up_ticks - rolling_down_ticks"),
                _spec(f"tick_imbalance_ratio_{window}", PREDICTOR_CAUSAL, (f"tick_imbalance_{window}",), lookback=window, definition=f"tick_imbalance / {window}"),
                _spec(f"buy_pressure_{window}", PREDICTOR_CAUSAL, (f"rolling_up_ticks_{window}",), lookback=window, definition=f"rolling_up_ticks / {window}"),
                _spec(f"sell_pressure_{window}", PREDICTOR_CAUSAL, (f"rolling_down_ticks_{window}",), lookback=window, definition=f"rolling_down_ticks / {window}"),
                _spec(f"order_flow_ratio_{window}", PREDICTOR_CAUSAL, (f"rolling_up_ticks_{window}", f"rolling_down_ticks_{window}"), lookback=window, definition="(up - down) / (up + down)"),
                _spec(f"rolling_abs_move_mean_{window}", PREDICTOR_CAUSAL, ("absolute_mid_move",), lookback=window, definition=f"trailing mean of {window} absolute mid moves"),
                _spec(f"rolling_abs_move_sum_{window}", PREDICTOR_CAUSAL, ("absolute_mid_move",), lookback=window, definition=f"trailing sum of {window} absolute mid moves"),
                _spec(f"spread_zscore_{window}", PREDICTOR_CAUSAL, ("spread",), lookback=window - 1, definition=f"spread z-score over trailing {window} spreads"),
                _spec(f"volatility_zscore_{window}", PREDICTOR_CAUSAL, (f"rolling_return_std_{window}",), lookback=2 * window - 1, definition=f"z-score of trailing-{window} return std over a trailing {window}-value volatility window"),
            ]
        )
    return specs


def forward_target_spec(horizon: int) -> ColumnSpec:
    value = int(horizon)
    if value <= 0:
        raise FeatureContractError(f"Forward horizon must be positive, got {horizon!r}")
    return _spec(
        f"future_return_{value}",
        TARGET_FORWARD,
        (APPROVED_TARGET_PRICE_BASIS,),
        lookahead=value,
        definition=f"event-time simple return mid[t+{value}] / mid[t] - 1",
    )


def _registry() -> dict[str, ColumnSpec]:
    specs = [*_base_specs(), *_predictor_specs()]
    horizons = sorted(set(SCRIPT23_FORWARD_HORIZONS + SCRIPT30_FORWARD_HORIZONS + EH01_DEFAULT_HORIZONS))
    specs.extend(forward_target_spec(horizon) for horizon in horizons)
    registry = {spec.name: spec for spec in specs}
    if len(registry) != len(specs):
        raise FeatureContractError("Duplicate D2 column registration")
    return registry


COLUMN_SPECS = _registry()


def column_spec(name: str, *, approved_extra_targets: Iterable[str] = ()) -> ColumnSpec:
    if name in COLUMN_SPECS:
        return COLUMN_SPECS[name]
    extras = {str(value) for value in approved_extra_targets}
    for target in extras:
        if target == name:
            try:
                horizon = int(target.removeprefix("future_return_"))
            except ValueError as exc:
                raise FeatureContractError(f"Invalid explicitly approved target {target!r}") from exc
            spec = forward_target_spec(horizon)
            if spec.name != target:
                raise FeatureContractError(f"Non-canonical target name {target!r}")
            return spec
    raise FeatureContractError(f"Unregistered D2 column {name!r}")


def predictor_columns(
    frame: pd.DataFrame,
    *,
    fail_on_unknown_numeric: bool = True,
    approved_extra_targets: Iterable[str] = (),
) -> list[str]:
    predictors: list[str] = []
    unknown_numeric: list[str] = []
    for name in frame.columns:
        try:
            spec = column_spec(name, approved_extra_targets=approved_extra_targets)
        except FeatureContractError:
            if pd.api.types.is_numeric_dtype(frame[name]):
                unknown_numeric.append(name)
            continue
        if spec.role == PREDICTOR_CAUSAL:
            if not pd.api.types.is_numeric_dtype(frame[name]):
                raise FeatureContractError(f"Registered predictor {name!r} is not numeric")
            predictors.append(name)
    if fail_on_unknown_numeric and unknown_numeric:
        raise FeatureContractError(
            f"Unknown numeric columns are not research-eligible: {sorted(unknown_numeric)}"
        )
    return predictors


def require_role(name: str, role: str, *, approved_extra_targets: Iterable[str] = ()) -> ColumnSpec:
    spec = column_spec(name, approved_extra_targets=approved_extra_targets)
    if spec.role != role:
        raise FeatureContractError(f"Column {name!r} has role {spec.role!r}, required {role!r}")
    return spec


def require_predictor(name: str) -> ColumnSpec:
    return require_role(name, PREDICTOR_CAUSAL)


def require_target(name: str, *, approved_extra_targets: Iterable[str] = ()) -> ColumnSpec:
    return require_role(name, TARGET_FORWARD, approved_extra_targets=approved_extra_targets)


def causal_predictor_specs() -> tuple[ColumnSpec, ...]:
    return tuple(spec for spec in COLUMN_SPECS.values() if spec.role == PREDICTOR_CAUSAL)


def maximum_causal_lookback() -> int:
    return max(spec.lookback_observations for spec in causal_predictor_specs())


def contract_payload() -> dict[str, Any]:
    return {
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "feature_role_contract_version": FEATURE_ROLE_CONTRACT_VERSION,
        "causal_lookback_spec_version": CAUSAL_LOOKBACK_SPEC_VERSION,
        "target_contract_version": TARGET_CONTRACT_VERSION,
        "approved_target_price_basis": APPROVED_TARGET_PRICE_BASIS,
        "columns": [COLUMN_SPECS[name].to_dict() for name in sorted(COLUMN_SPECS)],
    }


def feature_contract_fingerprint() -> str:
    payload = json.dumps(contract_payload(), sort_keys=True, separators=(",", ":"))
    return sha256(payload.encode("utf-8")).hexdigest()


def write_feature_parquet(frame: pd.DataFrame, path: Path, lineage: dict[str, Any]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    metadata = dict(table.schema.metadata or {})
    footer = {
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "feature_role_contract_version": FEATURE_ROLE_CONTRACT_VERSION,
        "causal_lookback_spec_version": CAUSAL_LOOKBACK_SPEC_VERSION,
        "target_contract_version": TARGET_CONTRACT_VERSION,
        "feature_contract_fingerprint": feature_contract_fingerprint(),
        "maximum_causal_lookback": maximum_causal_lookback(),
        "target_price_basis": APPROVED_TARGET_PRICE_BASIS,
        **lineage,
    }
    metadata.update({str(key).encode(): str(value).encode() for key, value in footer.items()})
    pq.write_table(table.replace_schema_metadata(metadata), path)


def read_feature_metadata(path: Path) -> dict[str, str]:
    import pyarrow.parquet as pq

    raw = pq.read_metadata(path).metadata or {}
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in raw.items()
        if key != b"ARROW:schema"
    }


def validate_feature_parquet(path: Path) -> dict[str, Any]:
    try:
        footer = read_feature_metadata(path)
    except Exception as exc:
        return {"certified": False, "reason": f"metadata_read_error:{exc}", "metadata": {}}
    required = {
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "feature_role_contract_version": FEATURE_ROLE_CONTRACT_VERSION,
        "causal_lookback_spec_version": CAUSAL_LOOKBACK_SPEC_VERSION,
        "target_contract_version": TARGET_CONTRACT_VERSION,
        "feature_contract_fingerprint": feature_contract_fingerprint(),
        "target_price_basis": APPROVED_TARGET_PRICE_BASIS,
    }
    for key, expected in required.items():
        if footer.get(key) != str(expected):
            return {
                "certified": False,
                "reason": f"{key}={footer.get(key)!r}, expected {expected!r}",
                "metadata": footer,
            }
    return {"certified": True, "reason": "certified", "metadata": footer}


assert maximum_causal_lookback() == 499
