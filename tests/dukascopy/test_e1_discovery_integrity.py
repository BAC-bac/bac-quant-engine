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

from dukascopy_feature_contract import FeatureContractError  # noqa: E402


def load_script(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


script22 = load_script(TICK_DIR / "22_feature_discovery_engine.py", "e1_script22")
eh02 = load_script(EH_DIR / "02_extended_horizon_feature_discovery.py", "e1_eh02")
eh03 = load_script(EH_DIR / "03_extended_horizon_stability_engine.py", "e1_eh03")


def discovery_rows(*, long_return: float, short_return: float, long_win: float, short_win: float) -> pd.DataFrame:
    rows = []
    for index, (long_count, short_count) in enumerate(((100, 10), (10, 100)), start=1):
        rows.append(
            {
                "file": f"file_{index}",
                "filename": f"EURJPY_2024-01-0{index}_features.parquet",
                "file_date": f"2024-01-0{index}",
                "target": "future_return_2500",
                "feature": "mid_return_1",
                "rows": 500,
                "valid_target_rows": 450,
                "correlation": 0.1,
                "abs_correlation": 0.1,
                "long_win_rate": long_win,
                "short_win_rate": short_win,
                "long_avg_return": long_return,
                "short_avg_return": short_return,
                "long_median_return": long_return,
                "short_median_return": short_return,
                "long_count": long_count,
                "short_count": short_count,
                "long_positive_count": round(long_count * long_win),
                "short_positive_count": round(short_count * short_win),
                "long_return_sum": long_count * long_return,
                "short_return_sum": short_count * short_return,
                "expected_files": 2,
                "input_coverage_status": "complete",
                "input_dataset_fingerprint": "a" * 64,
                "lower_threshold": -0.25 - index * 0.01,
                "upper_threshold": 0.25 + index * 0.01,
                "threshold_learning_method": "predictor_only_file_q25_q75_v1",
                "discovery_methodology_version": "extended_horizon_discovery_integrity_e1_v1",
            }
        )
    return pd.DataFrame(rows)


def test_target_null_pattern_does_not_change_predictor_threshold() -> None:
    feature = pd.Series(np.arange(200, dtype=float))
    target_a = pd.Series(np.linspace(-0.1, 0.1, 200))
    target_b = target_a.copy()
    target_b.iloc[::3] = np.nan
    stats_a = eh02.directional_stats(feature, target_a, min_evaluation_rows=1)
    stats_b = eh02.directional_stats(feature, target_b, min_evaluation_rows=1)
    assert stats_a["lower_threshold"] == stats_b["lower_threshold"]
    assert stats_a["upper_threshold"] == stats_b["upper_threshold"]


def test_target_values_do_not_influence_predictor_threshold() -> None:
    feature = pd.Series(np.arange(200, dtype=float))
    first = eh02.directional_stats(feature, pd.Series(np.arange(200)), min_evaluation_rows=1)
    second = eh02.directional_stats(feature, pd.Series(np.arange(200)[::-1]), min_evaluation_rows=1)
    assert (first["lower_threshold"], first["upper_threshold"]) == (
        second["lower_threshold"], second["upper_threshold"]
    )


def test_long_selected_candidate_uses_only_long_metrics() -> None:
    ranked = eh02.rank_results(
        discovery_rows(long_return=0.02, short_return=0.01, long_win=0.51, short_win=0.90)
    ).iloc[0]
    assert ranked["selected_side"] == ranked["best_side"] == "long"
    assert ranked["selected_file_balanced_mean_win_rate"] == pytest.approx(0.51)
    assert ranked["best_win_rate"] == pytest.approx(0.51)


def test_short_selected_candidate_uses_only_short_metrics() -> None:
    ranked = eh02.rank_results(
        discovery_rows(long_return=0.01, short_return=0.03, long_win=0.95, short_win=0.54)
    ).iloc[0]
    assert ranked["selected_side"] == ranked["best_side"] == "short"
    assert ranked["selected_file_balanced_mean_win_rate"] == pytest.approx(0.54)
    assert ranked["best_win_rate"] == pytest.approx(0.54)


def test_feature_roles_reject_unknown_numeric_and_forward_target_predictors() -> None:
    frame = pd.DataFrame(
        {
            "mid_return_1": np.arange(10, dtype=float),
            "future_return_2500": np.arange(10, dtype=float),
            "unknown_numeric": np.arange(10, dtype=float),
        }
    )
    with pytest.raises(FeatureContractError, match="Unknown numeric"):
        eh02.get_numeric_feature_columns(frame, ["future_return_2500"])
    frame = frame.drop(columns="unknown_numeric")
    assert eh02.get_numeric_feature_columns(frame, ["future_return_2500"]) == ["mid_return_1"]


def test_script22_requires_precomputed_continuous_targets() -> None:
    frame = pd.DataFrame({"mid_return_1": np.arange(10, dtype=float)})
    with pytest.raises(FeatureContractError, match="file-local shift"):
        script22.require_approved_targets(frame)
    source = (TICK_DIR / "22_feature_discovery_engine.py").read_text(encoding="utf-8")
    assert ".shift(-" not in source


def test_incomplete_file_coverage_is_explicit() -> None:
    results, coverage = eh02.process_files(
        [Path("EURJPY_2024-01-01_features.parquet"), Path("undated.parquet")],
        ["future_return_2500"],
    )
    assert results.empty
    assert set(coverage["status"]) == {"failed"}
    assert len(coverage) == 2
    with pytest.raises(ValueError, match="refuses incomplete"):
        eh03.validate_discovery_coverage(coverage)


def test_unknown_or_missing_file_dates_are_rejected() -> None:
    frame = discovery_rows(long_return=0.02, short_return=0.01, long_win=0.6, short_win=0.5)
    frame.loc[0, "file_date"] = "unknown"
    with pytest.raises(ValueError, match="cannot be temporal evidence"):
        eh03.calculate_stability(frame)


def test_file_balanced_and_row_weighted_metrics_are_distinct() -> None:
    raw = discovery_rows(long_return=0.02, short_return=0.01, long_win=0.9, short_win=0.5)
    raw.loc[1, "long_win_rate"] = 0.1
    raw.loc[1, "long_positive_count"] = 1
    stability = eh03.calculate_stability(raw).iloc[0]
    assert stability["file_balanced_mean_long_win_rate"] == pytest.approx(0.5)
    assert stability["row_weighted_long_win_rate"] == pytest.approx(91 / 110)


def test_stability_selected_side_is_coherent_and_eh04_aliases_survive() -> None:
    stability = eh03.calculate_stability(
        discovery_rows(long_return=0.02, short_return=0.01, long_win=0.51, short_win=0.90)
    ).iloc[0]
    assert stability["selected_side"] == stability["best_side"] == "long"
    assert stability["best_mean_win_rate"] == pytest.approx(0.51)
    assert stability["best_mean_return"] == pytest.approx(0.02)
    assert {"target", "feature", "best_side", "stability_score", "stability_status"}.issubset(
        stability.index
    )


def test_ranking_indices_are_deterministic_and_not_confidence() -> None:
    raw = pd.concat(
        [
            discovery_rows(long_return=0.02, short_return=0.01, long_win=0.6, short_win=0.5),
            discovery_rows(long_return=0.02, short_return=0.01, long_win=0.6, short_win=0.5).assign(
                feature="mid_return_5"
            ),
        ],
        ignore_index=True,
    )
    first = eh02.rank_results(raw)[["feature", "discovery_ranking_index"]].reset_index(drop=True)
    second = eh02.rank_results(raw.sample(frac=1, random_state=7))[
        ["feature", "discovery_ranking_index"]
    ].reset_index(drop=True)
    pd.testing.assert_frame_equal(first, second)
    assert (eh02.rank_results(raw)["ranking_interpretation"] == eh02.RANKING_INTERPRETATION).all()
