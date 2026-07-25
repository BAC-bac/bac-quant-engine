"""Schema definitions for BACQE Convex Survival attribution datasets."""

from __future__ import annotations

IDENTITY_COLUMNS: tuple[str, ...] = (
    "run_id",
    "bar_time",
    "symbol",
    "timeframe",
    "bias",
)

MEASURE_COLUMNS: tuple[str, ...] = (
    "spread_points",
    "atr",
    "atr_ma",
    "atr_long_ma",
    "atr_past_expansion",
    "atr_past_rising",
    "adx",
    "ema_sep_atr_ratio",
    "fail_count",
)

ATTRIBUTION_COLUMNS: tuple[str, ...] = (
    "sole_veto",
    "first_veto",
    "all_vetoes",
    "all_pass",
)

PASS_COLUMNS: tuple[str, ...] = (
    "trade_ready_pass",
    "time_pass",
    "spread_pass",
    "cooldown_pass",
    "daily_risk_pass",
    "max_total_pass",
    "max_symbol_pass",
    "bias_pass",
    "atr_available_pass",
    "adx_pass",
    "atr_short_ma_pass",
    "atr_expansion_pass",
    "ema_separation_pass",
    "atr_long_ma_pass",
    "atr_rising_pass",
    "breakout_price_pass",
    "candle_body_pass",
    "close_location_pass",
)

REQUIRED_COLUMNS: tuple[str, ...] = (
    *IDENTITY_COLUMNS,
    *MEASURE_COLUMNS,
    *ATTRIBUTION_COLUMNS,
    *PASS_COLUMNS,
)

NUMERIC_COLUMNS: tuple[str, ...] = MEASURE_COLUMNS
BOOLEAN_COLUMNS: tuple[str, ...] = ("all_pass", *PASS_COLUMNS)
ALLOWED_BIAS_VALUES: frozenset[str] = frozenset({"LONG", "SHORT", "NONE"})

SCHEMA_VERSION = "1.0.0"
ENGINE_VERSION = "1.0.0"
