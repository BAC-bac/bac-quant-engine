from __future__ import annotations

from hashlib import sha256
import importlib.util
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal


REPO_ROOT = Path(__file__).resolve().parents[2]
TICK_DIR = REPO_ROOT / "scripts" / "dukascopy_ticks"
EH_DIR = REPO_ROOT / "scripts" / "dukascopy_extended_horizons"
sys.path.insert(0, str(TICK_DIR))
sys.path.insert(0, str(EH_DIR))

from dukascopy_feature_contract import (  # noqa: E402
    COLUMN_SPECS,
    PREDICTOR_CAUSAL,
    TARGET_FORWARD,
    FeatureContractError,
    column_spec,
    contract_payload,
    feature_contract_fingerprint,
    maximum_causal_lookback,
    predictor_columns,
    read_feature_metadata,
    require_predictor,
    validate_feature_parquet,
    write_feature_parquet,
)


def load_script(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


features23 = load_script(TICK_DIR / "23_build_engineered_tick_features.py", "d2_features_23")
horizons30 = load_script(TICK_DIR / "30_horizon_expansion_engine.py", "d2_horizons_30")
discovery22 = load_script(TICK_DIR / "22_feature_discovery_engine.py", "d2_discovery_22")
validation25 = load_script(TICK_DIR / "25_signal_validation_engine.py", "d2_validation_25")
optimizer27 = load_script(TICK_DIR / "27_signal_filter_optimizer.py", "d2_optimizer_27")
replay28 = load_script(TICK_DIR / "28_candidate_replay_engine.py", "d2_replay_28")
eh01 = load_script(EH_DIR / "01_build_extended_horizon_targets.py", "d2_eh01")
eh02 = load_script(EH_DIR / "02_extended_horizon_feature_discovery.py", "d2_eh02")


def ticks(count: int, start: str, offset: int = 0) -> pd.DataFrame:
    index = np.arange(offset, offset + count, dtype=float)
    mid = 1.10 + index * 0.000001 + np.sin(index / 13.0) * 0.00002
    spread = 0.00002 + (index % 7) * 0.000001
    bid = mid - spread / 2
    ask = mid + spread / 2
    timestamp = pd.date_range(start, periods=count, freq="s", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp_utc": timestamp,
            "symbol": "EURUSD",
            "source": "dukascopy",
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "spread": spread,
            "spread_points": spread / 0.00001,
            "bid_volume": 1.0 + (index % 5),
            "ask_volume": 2.0 + (index % 3),
            "quote_volume": 3.0 + (index % 5) + (index % 3),
            "hour": timestamp.hour,
            "normalisation_schema_version": "dukascopy_normalised_ticks_v2",
            "symbol_metadata_version": "dukascopy_symbol_metadata_v1",
            "symbol_registry_fingerprint": "a" * 64,
            "raw_price_scale": 100000,
            "point_size": 0.00001,
            "pip_size": 0.0001,
            "coverage_status": "complete_coverage",
            "source_fingerprint": "b" * 64,
        }
    )


def three_partitions() -> list[pd.DataFrame]:
    return [
        ticks(600, "2024-01-01T23:50:00Z", 0),
        ticks(10, "2024-01-02T00:00:00Z", 600),
        ticks(700, "2024-01-03T00:00:00Z", 610),
    ]


def test_dependency_graph_has_nested_499_observation_maximum() -> None:
    assert maximum_causal_lookback() == 499
    assert COLUMN_SPECS["rolling_return_mean_250"].lookback_observations == 250
    assert COLUMN_SPECS["rolling_spread_mean_250"].lookback_observations == 249
    assert COLUMN_SPECS["volatility_zscore_250"].lookback_observations == 499


def test_partitioned_equals_monolithic_for_every_causal_predictor() -> None:
    partitions = three_partitions()
    continuous = pd.concat(partitions, ignore_index=True)
    expected = features23.add_causal_features(continuous)
    predictor_names = [
        name for name, spec in COLUMN_SPECS.items() if spec.role == PREDICTOR_CAUSAL
    ]
    emitted: list[pd.DataFrame] = []
    history = pd.DataFrame()
    for index, current in enumerate(partitions):
        remaining = partitions[index + 1 :]
        future = (
            pd.concat(remaining, ignore_index=True).head(50)
            if remaining
            else pd.DataFrame()
        )
        output = features23.build_partition_output(
            current,
            carry_in=history.tail(maximum_causal_lookback()),
            future=future,
        )
        emitted.append(output)
        history = pd.concat([history, current], ignore_index=True)
    actual = pd.concat(emitted, ignore_index=True)
    assert_frame_equal(
        actual[predictor_names],
        expected[predictor_names],
        check_dtype=False,
        rtol=1e-10,
        atol=5e-11,
    )


def test_exact_boundary_rows_and_short_partition_survive() -> None:
    first, short, future = three_partitions()
    output = features23.build_partition_output(
        short,
        carry_in=first.tail(maximum_causal_lookback()),
        future=future.head(50),
    )
    assert len(output) == len(short) == 10
    assert output["timestamp_utc"].tolist() == short["timestamp_utc"].tolist()
    assert output.loc[0, "mid_return_1"] == pytest.approx(
        short.loc[0, "mid"] / first.iloc[-1]["mid"] - 1
    )
    assert pd.notna(output.loc[0, "volatility_zscore_250"])


def test_nested_feature_needs_all_499_prior_observations() -> None:
    first, short, future = three_partitions()
    enough = features23.build_partition_output(
        short,
        carry_in=first.tail(499),
        future=future.head(50),
    )
    insufficient = features23.build_partition_output(
        short,
        carry_in=first.tail(498),
        future=future.head(50),
    )
    assert pd.notna(enough.loc[0, "volatility_zscore_250"])
    assert pd.isna(insufficient.loc[0, "volatility_zscore_250"])


def test_future_observations_cannot_change_earlier_predictors() -> None:
    first, current, future = three_partitions()
    altered = future.copy()
    for column in ("bid", "ask", "mid", "spread"):
        altered[column] = altered[column] * 1000
    original = features23.build_partition_output(current, carry_in=first.tail(499), future=future.head(50))
    changed = features23.build_partition_output(current, carry_in=first.tail(499), future=altered.head(50))
    names = predictor_columns(original)
    assert_frame_equal(original[names], changed[names], check_dtype=False)


def test_insufficient_history_nulls_features_without_dropping_rows() -> None:
    current = ticks(10, "2024-01-01T00:00:00Z")
    output = features23.build_partition_output(current)
    assert len(output) == 10
    assert output["timestamp_utc"].equals(current["timestamp_utc"])
    assert output["volatility_zscore_250"].isna().all()
    assert output["rolling_return_mean_25"].isna().all()


def test_roles_block_targets_and_unknown_numeric_fields() -> None:
    frame = features23.build_partition_output(ticks(600, "2024-01-01T00:00:00Z"))
    names = predictor_columns(frame)
    assert "future_return_1" not in names
    assert column_spec("future_return_1").role == TARGET_FORWARD
    with pytest.raises(FeatureContractError):
        require_predictor("future_return_1")
    frame["unknown_numeric_diagnostic"] = 1.0
    with pytest.raises(FeatureContractError, match="Unknown numeric"):
        predictor_columns(frame)
    with pytest.raises(FeatureContractError):
        eh02.get_numeric_feature_columns(frame, ["future_return_2500"])


def test_all_direct_consumers_reject_target_as_predictor(tmp_path: Path) -> None:
    bad_feature = "future_return_1"
    target = "future_return_5"
    fixtures = [
        (
            validation25.load_candidate_features,
            {"feature": bad_feature, "target": target, "dominant_direction": "positive", "rank": 1},
        ),
        (
            optimizer27.load_candidates,
            {"feature": bad_feature, "target": target, "forensic_label": "robust_candidate"},
        ),
        (
            replay28.load_candidates,
            {"feature": bad_feature, "target": target, "side": "long", "filter_type": "all", "filter_value": "all", "filter_rank": 1},
        ),
    ]
    for index, (loader, row) in enumerate(fixtures):
        path = tmp_path / f"candidate_{index}.csv"
        pd.DataFrame([row]).to_csv(path, index=False)
        with pytest.raises(FeatureContractError):
            loader(path)


def test_script22_uses_only_registered_predictors() -> None:
    frame = features23.build_partition_output(ticks(600, "2024-01-01T00:00:00Z"))
    names = discovery22.get_feature_columns(frame)
    assert names
    assert all(column_spec(name).role == PREDICTOR_CAUSAL for name in names)
    assert "future_return_1" not in names


@pytest.mark.parametrize("kind", ["duplicate", "non_monotonic"])
def test_bad_chronology_fails_closed(kind: str) -> None:
    frame = ticks(10, "2024-01-01T00:00:00Z")
    if kind == "duplicate":
        frame.loc[5, "timestamp_utc"] = frame.loc[4, "timestamp_utc"]
    else:
        frame.loc[5, "timestamp_utc"] = frame.loc[3, "timestamp_utc"]
    with pytest.raises(ValueError):
        features23.validate_partition_frame(frame, label=kind)


def test_ambiguous_partition_date_fails_closed() -> None:
    with pytest.raises(ValueError, match="Ambiguous"):
        features23.partition_date_from_path(Path("EURUSD_no_date_ticks.parquet"))


def test_script30_targets_cross_partition_boundary() -> None:
    current = ticks(5, "2024-01-01T23:59:55Z", 0)
    future = ticks(5, "2024-01-02T00:00:00Z", 5)
    output = horizons30.build_partition_targets(current, future=future, horizons=[1, 3])
    combined = pd.concat([current, future], ignore_index=True)
    expected = combined["mid"].shift(-3) / combined["mid"] - 1
    assert output.loc[4, "future_return_1"] == pytest.approx(
        future.loc[0, "mid"] / current.loc[4, "mid"] - 1
    )
    assert output["future_return_3"].tolist() == pytest.approx(
        expected.iloc[: len(current)].tolist()
    )


def test_eh01_requires_explicit_mid_price_basis(tmp_path: Path) -> None:
    root = tmp_path / "input"
    symbol_root = root / "symbol=EURJPY"
    symbol_root.mkdir(parents=True)
    (symbol_root / "placeholder.parquet").write_bytes(b"not-read")
    with pytest.raises(ValueError, match="must be 'mid'"):
        eh01.main(
            symbol="EURJPY",
            horizons=[1],
            input_root=root,
            output_root=tmp_path / "output",
            report_root=tmp_path / "reports",
            price_column="close",
        )


def test_contract_fingerprint_changes_with_definition() -> None:
    original = feature_contract_fingerprint()
    payload = contract_payload()
    payload["columns"][0]["definition"] += " changed"
    changed = sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert changed != original


def test_engineered_footer_carries_d1_d2_and_partition_lineage(tmp_path: Path) -> None:
    frame = features23.build_partition_output(ticks(10, "2024-01-01T00:00:00Z"))
    path = tmp_path / "features.parquet"
    lineage = {
        "d1_normalisation_schema_version": "dukascopy_normalised_ticks_v2",
        "d1_symbol_metadata_version": "dukascopy_symbol_metadata_v1",
        "d1_symbol_registry_fingerprint": "a" * 64,
        "source_partition": "synthetic_day_1",
        "carry_in_rows": 0,
        "output_row_owner": "synthetic_day_1",
    }
    write_feature_parquet(frame, path, lineage)
    result = validate_feature_parquet(path)
    assert result["certified"] is True
    footer = read_feature_metadata(path)
    for key, value in lineage.items():
        assert footer[key] == str(value)
    assert footer["maximum_causal_lookback"] == "499"
