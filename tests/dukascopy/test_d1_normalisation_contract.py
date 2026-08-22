from __future__ import annotations

from datetime import datetime
import importlib.util
import lzma
from pathlib import Path
import struct
import sys

import numpy as np
import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = REPO_ROOT / "scripts" / "dukascopy_ticks"
sys.path.insert(0, str(SCRIPT_DIR))

from dukascopy_contract import (  # noqa: E402
    NORMALISATION_SCHEMA_VERSION,
    SYMBOL_METADATA_SCHEMA_VERSION,
    DukascopyContractError,
    certified_symbols,
    get_symbol_metadata,
    validate_normalised_parquet,
)


def load_script(filename: str, module_name: str):
    path = SCRIPT_DIR / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


normaliser = load_script("08_normalise_dukascopy_date_range.py", "d1_normaliser_08")
legacy04 = load_script("04_normalise_dukascopy_day_to_parquet.py", "d1_legacy_04")
bars09 = load_script("09_build_dukascopy_tick_bars_date_range.py", "d1_bars_09")
tibs10 = load_script("10_build_dukascopy_tibs_date_range.py", "d1_tibs_10")
selectivity56 = load_script("56_selectivity_research_engine.py", "d1_selectivity_56")
orchestrator64 = load_script("64_dukascopy_pipeline_orchestrator.py", "d1_orchestrator_64")
state65 = load_script("65_dukascopy_pipeline_state_registry.py", "d1_state_65")
onboarding74 = load_script("74_dukascopy_new_symbol_onboarding_engine.py", "d1_onboarding_74")


GOLDEN = {
    "EURUSD": (110370, 110366, 1.10370, 1.10366, 100_000, 0.00001, 0.0001),
    "GBPUSD": (121091, 120921, 1.21091, 1.20921, 100_000, 0.00001, 0.0001),
    "EURGBP": (85436, 85136, 0.85436, 0.85136, 100_000, 0.00001, 0.0001),
    "USDJPY": (131001, 130925, 131.001, 130.925, 1_000, 0.001, 0.01),
    "EURJPY": (140339, 139992, 140.339, 139.992, 1_000, 0.001, 0.01),
    "GBPJPY": (144570, 144082, 144.570, 144.082, 1_000, 0.001, 0.01),
}


def write_bi5(path: Path, records: list[tuple[int, int, int, float, float]]) -> None:
    raw = b"".join(struct.pack(">IIIff", *record) for record in records)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(lzma.compress(raw))


@pytest.mark.parametrize("symbol", sorted(GOLDEN))
def test_known_repository_golden_record_decodes_with_certified_scale(tmp_path: Path, symbol: str):
    ask_raw, bid_raw, ask, bid, scale, point, pip = GOLDEN[symbol]
    metadata = get_symbol_metadata(symbol)
    assert (metadata.raw_price_scale, metadata.point_size, metadata.pip_size) == (scale, point, pip)
    fixture = tmp_path / f"{symbol}_fixture.bi5"
    write_bi5(fixture, [(1234, ask_raw, bid_raw, 2.0, 3.0)])
    result = normaliser.decode_hour_path(fixture, symbol, datetime(2024, 1, 2), 7, metadata)
    assert result.status == "decoded"
    row = result.frame.iloc[0]
    assert row["ask"] == pytest.approx(ask)
    assert row["bid"] == pytest.approx(bid)
    assert row["mid"] == pytest.approx((ask + bid) / 2)
    assert row["spread"] == pytest.approx(ask - bid)
    assert row["spread_points"] == pytest.approx((ask - bid) / point)
    assert row["timestamp_utc"] == pd.Timestamp("2024-01-02T07:00:01.234Z")


def test_registry_is_explicit_and_fail_closed():
    assert certified_symbols() == tuple(sorted(GOLDEN))
    for symbol in certified_symbols():
        metadata = get_symbol_metadata(symbol)
        assert metadata.metadata_schema_version == SYMBOL_METADATA_SCHEMA_VERSION
        assert metadata.certification_status == "certified"
        assert metadata.raw_price_scale > 0
        assert metadata.point_size > 0
        assert metadata.pip_size > metadata.point_size
    with pytest.raises(DukascopyContractError):
        get_symbol_metadata("XAUUSD")


def test_unknown_symbol_fails_before_any_output_is_created(tmp_path: Path, monkeypatch):
    raw, processed, reports = tmp_path / "raw", tmp_path / "processed", tmp_path / "reports"
    monkeypatch.setattr(normaliser, "RAW_ROOT", raw)
    monkeypatch.setattr(normaliser, "PROCESSED_ROOT", processed)
    monkeypatch.setattr(normaliser, "REPORT_ROOT", reports)
    with pytest.raises(DukascopyContractError):
        normaliser.run_normalisation("UNKNOWN", "2024-01-02", "2024-01-02")
    assert not raw.exists() and not processed.exists() and not reports.exists()


def test_cleaning_enforces_price_volume_order_and_duplicates():
    metadata = get_symbol_metadata("EURUSD")
    valid = {
        "timestamp_utc": pd.Timestamp("2024-01-02T00:00:01Z"),
        "symbol": "EURUSD", "source": "dukascopy", "bid": 1.1, "ask": 1.10002,
        "mid": 0.0, "spread": 0.0, "spread_points": 0.0,
        "bid_volume": 1.0, "ask_volume": 2.0, "quote_volume": 0.0, "hour": 0,
    }
    rows = [
        {**valid, "timestamp_utc": pd.Timestamp("2024-01-02T00:00:02Z")}, valid, valid,
        {**valid, "bid": -1.0}, {**valid, "bid": 1.2, "ask": 1.1},
        {**valid, "ask_volume": -1.0},
    ]
    cleaned, report = normaliser.clean_ticks(pd.DataFrame(rows), metadata)
    assert len(cleaned) == 2 and cleaned["timestamp_utc"].is_monotonic_increasing
    assert report["removed_duplicates"] == 1
    assert report["removed_non_positive_prices"] == 1
    assert report["removed_crossed_spread"] == 1
    assert report["removed_negative_volume"] == 1
    assert np.allclose(cleaned["mid"], (cleaned["bid"] + cleaned["ask"]) / 2)
    assert np.allclose(cleaned["spread"], cleaned["ask"] - cleaned["bid"])
    assert np.allclose(cleaned["spread_points"], cleaned["spread"] / metadata.point_size)


def test_missing_empty_and_decode_failure_are_distinct(tmp_path: Path):
    metadata = get_symbol_metadata("EURUSD")
    dt = datetime(2024, 1, 2)
    missing = normaliser.decode_hour_path(tmp_path / "missing.bi5", "EURUSD", dt, 0, metadata)
    empty_path = tmp_path / "empty.bi5"; empty_path.write_bytes(b"")
    empty = normaliser.decode_hour_path(empty_path, "EURUSD", dt, 1, metadata)
    bad_path = tmp_path / "bad.bi5"; bad_path.write_bytes(b"not-lzma")
    failed = normaliser.decode_hour_path(bad_path, "EURUSD", dt, 2, metadata)
    assert (missing.status, empty.status, failed.status) == (
        "missing_file", "empty_file", "decode_failure"
    )
    coverage = normaliser.build_coverage([missing, empty, failed])
    assert coverage["files_missing"] == coverage["empty_files"] == coverage["decode_failures"] == 1
    assert coverage["coverage_status"] == "no_decoded_data"


def test_full_day_output_contains_certified_lineage(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(normaliser, "RAW_ROOT", tmp_path / "raw")
    monkeypatch.setattr(normaliser, "PROCESSED_ROOT", tmp_path / "processed")
    monkeypatch.setattr(normaliser, "REPORT_ROOT", tmp_path / "reports")
    dt = datetime(2024, 1, 2)
    for hour in range(24):
        write_bi5(
            normaliser.raw_bi5_path("EURUSD", dt, hour),
            [(100, 110002 + hour, 110000 + hour, 1.0, 1.0)],
        )
    report_path = normaliser.run_normalisation("EURUSD", "2024-01-02", "2024-01-02")
    output = normaliser.processed_output_path("EURUSD", dt)
    assert output.exists() and report_path.exists()
    frame = pd.read_parquet(output)
    assert len(frame) == 24 and frame["timestamp_utc"].is_monotonic_increasing
    assert set(frame["normalisation_schema_version"]) == {NORMALISATION_SCHEMA_VERSION}
    assert set(frame["symbol_metadata_version"]) == {SYMBOL_METADATA_SCHEMA_VERSION}
    assert set(frame["coverage_status"]) == {"complete_coverage"}
    certification = validate_normalised_parquet(output, "EURUSD")
    assert certification["certified"], certification["reason"]
    quality = pd.read_csv(normaliser.quality_report_path("EURUSD", dt)).iloc[0]
    assert quality["expected_hourly_files"] == 24
    assert quality["successfully_decoded_files"] == 24
    assert quality["coverage_status"] == "complete_coverage"
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        normaliser.run_normalisation("EURUSD", "2024-01-02", "2024-01-02")


def test_incomplete_day_cannot_certify(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(normaliser, "RAW_ROOT", tmp_path / "raw")
    monkeypatch.setattr(normaliser, "PROCESSED_ROOT", tmp_path / "processed")
    monkeypatch.setattr(normaliser, "REPORT_ROOT", tmp_path / "reports")
    dt = datetime(2024, 1, 2)
    write_bi5(normaliser.raw_bi5_path("EURUSD", dt, 0), [(100, 110002, 110000, 1.0, 1.0)])
    normaliser.run_normalisation("EURUSD", "2024-01-02", "2024-01-02")
    result = validate_normalised_parquet(normaliser.processed_output_path("EURUSD", dt), "EURUSD")
    assert not result["certified"]
    assert "coverage_status='incomplete_coverage'" in result["reason"]


def test_jpy_bar_range_uses_registry_point_size():
    frame = pd.DataFrame({
        "timestamp_utc": pd.to_datetime(["2024-01-02T00:00:00Z", "2024-01-02T00:00:01Z"]),
        "symbol": ["USDJPY", "USDJPY"], "source": ["dukascopy", "dukascopy"],
        "mid": [130.000, 130.010], "bid": [129.999, 130.009], "ask": [130.001, 130.011],
        "spread_points": [2.0, 2.0], "bid_volume": [1.0, 1.0],
        "ask_volume": [1.0, 1.0], "quote_volume": [2.0, 2.0],
    })
    bars = bars09.build_tick_bars(frame, tick_size=2)
    assert bars.loc[0, "range_points"] == pytest.approx(10.0)
    tibs = tibs10.build_tick_imbalance_bars(frame, threshold=2)
    assert tibs.loc[0, "range_points"] == pytest.approx(10.0)


def test_script_56_point_conversion_uses_symbol_registry():
    ledger = pd.DataFrame({
        "feature": [selectivity56.FEATURE], "target": [selectivity56.TARGET],
        "side": [selectivity56.SIDE], "feature_value": [0.1],
        "signal_return": [0.01], "spread_points": [2.0],
    })
    prepared = selectivity56.prepare_candidate_ledger(ledger, symbol="USDJPY")
    assert prepared.loc[0, "spread"] == pytest.approx(0.002)


def test_state_registry_requires_certified_contract_not_file_count_only():
    assert state65.certified_processed_status(10, 0, 10, 10) == "legacy_or_uncertified_contract"
    assert state65.certified_processed_status(10, 9, 10, 0) == "legacy_or_uncertified_contract"
    assert state65.certified_processed_status(10, 10, 10, 0) == "certified_complete"


def test_legacy_writer_and_point_consumers_have_no_local_constants():
    for filename in [
        "04_normalise_dukascopy_day_to_parquet.py",
        "09_build_dukascopy_tick_bars_date_range.py",
        "10_build_dukascopy_tibs_date_range.py",
        "56_selectivity_research_engine.py",
    ]:
        text = (SCRIPT_DIR / filename).read_text(encoding="utf-8")
        assert "PRICE_SCALE =" not in text and "POINT_SIZE =" not in text
    assert "08_normalise_dukascopy_date_range.py" in (
        SCRIPT_DIR / "04_normalise_dukascopy_day_to_parquet.py"
    ).read_text(encoding="utf-8")
    assert legacy04.load_run_normalisation().__name__ == "run_normalisation"


def test_orchestration_and_onboarding_reject_uncertified_symbols():
    with pytest.raises(DukascopyContractError):
        onboarding74.configured_symbols({"symbols": ["EURUSD", "NOTREAL"]})
    assert orchestrator64.build_command(
        "08", orchestrator64.STAGE_SCRIPTS["08"], "EURUSD", "2024-01-01", "2024-01-02"
    )[2:4] == ["--symbol", "EURUSD"]


def test_script_56_blocks_unresolved_return_space_costs():
    with pytest.raises(RuntimeError, match="return-space cost conversion"):
        selectivity56.run_selectivity_tests(pd.DataFrame())
