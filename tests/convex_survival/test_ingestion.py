from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bacqe.convex_survival.ingestion import (
    AttributionIngestionError,
    ingest_attribution_csv,
    load_and_validate,
)
from bacqe.convex_survival.schemas import REQUIRED_COLUMNS


def valid_row() -> dict[str, object]:
    row: dict[str, object] = {column: 1 for column in REQUIRED_COLUMNS}
    row.update(
        {
            "run_id": "TEST_RUN",
            "bar_time": "2026.01.01 12:00",
            "symbol": "EURUSD",
            "timeframe": "PERIOD_H1",
            "bias": "LONG",
            "spread_points": 10,
            "atr": 0.001,
            "atr_ma": 0.001,
            "atr_long_ma": 0.001,
            "atr_past_expansion": 0.001,
            "atr_past_rising": 0.001,
            "adx": 30.0,
            "ema_sep_atr_ratio": 0.75,
            "fail_count": 0,
            "sole_veto": None,
            "first_veto": None,
            "all_vetoes": None,
            "all_pass": 1,
        }
    )
    return row


def write_csv(tmp_path: Path, rows: list[dict[str, object]]) -> Path:
    path = tmp_path / "source.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def test_valid_csv_is_normalised(tmp_path: Path) -> None:
    path = write_csv(tmp_path, [valid_row()])
    frame, audit = load_and_validate(path)

    assert len(frame) == 1
    assert str(frame["bar_time"].dtype) == "datetime64[ns, UTC]"
    assert bool(frame.loc[0, "all_pass"]) is True
    assert audit["all_pass_rows"] == 1


def test_missing_mandatory_column_fails(tmp_path: Path) -> None:
    row = valid_row()
    row.pop("adx")
    path = write_csv(tmp_path, [row])

    with pytest.raises(AttributionIngestionError, match="Missing mandatory columns"):
        load_and_validate(path)


def test_invalid_boolean_fails(tmp_path: Path) -> None:
    row = valid_row()
    row["time_pass"] = 2
    path = write_csv(tmp_path, [row])

    with pytest.raises(AttributionIngestionError, match="outside 0/1"):
        load_and_validate(path)


def test_attribution_invariant_fails(tmp_path: Path) -> None:
    row = valid_row()
    row["all_pass"] = 1
    row["fail_count"] = 1
    row["sole_veto"] = "adx"
    row["first_veto"] = "adx"
    row["all_vetoes"] = "adx"
    path = write_csv(tmp_path, [row])

    with pytest.raises(AttributionIngestionError, match="Attribution invariant failure"):
        load_and_validate(path)


def test_full_ingestion_writes_verified_outputs(tmp_path: Path) -> None:
    path = write_csv(tmp_path, [valid_row()])
    result = ingest_attribution_csv(
        source_csv=path,
        staging_root=tmp_path / "staging",
        report_root=tmp_path / "reports",
    )

    assert Path(result.staged_parquet).exists()
    assert Path(result.audit_json).exists()
    assert Path(result.audit_report).exists()
    assert result.row_count == 1
