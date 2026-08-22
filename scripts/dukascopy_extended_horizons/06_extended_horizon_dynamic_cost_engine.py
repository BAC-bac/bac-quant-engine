"""EH06: explicit, unit-safe observed-spread stress scenarios."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys

import numpy as np
import pandas as pd

DUKASCOPY_TICKS_DIR = Path(__file__).resolve().parents[1] / "dukascopy_ticks"
if str(DUKASCOPY_TICKS_DIR) not in sys.path:
    sys.path.insert(0, str(DUKASCOPY_TICKS_DIR))

from dukascopy_contract import file_sha256, get_symbol_metadata  # noqa: E402
from extended_horizons_e2_contract import (  # noqa: E402
    EH06_DYNAMIC_COST_METHODOLOGY_VERSION, ECONOMIC_UNIT_MODEL, EXECUTION_MODEL,
    candidate_contract_id, economic_contract_id, normalise_rule, profit_factor, trade_returns,
)

DEFAULT_SYMBOL = "EURJPY"
DEFAULT_TOP_N = 75
MIN_TRADES_PER_FILE = 100
BASE_DIR = Path("E:/Quant_Lab")
FEATURE_ROOT = BASE_DIR / "data" / "processed" / "dukascopy_extended_horizon_features"
VALIDATION_ROOT = BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "signal_validation"
REPORT_ROOT = BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "dynamic_cost_survival"

DYNAMIC_COST_SCENARIOS = {
    "observed_bid_ask": 1.00,
    "observed_spread_stress_1_25x": 1.25,
    "observed_spread_stress_1_50x": 1.50,
}
DATE_PATTERN = re.compile(r"(\d{4}[-_]\d{2}[-_]\d{2})")


def file_date(path: Path) -> str:
    matches = sorted(set(DATE_PATTERN.findall(path.name)))
    if len(matches) != 1:
        raise ValueError(f"Expected one processing date in {path.name!r}, found {matches}")
    return matches[0].replace("_", "-")


def find_feature_files(symbol: str) -> list[Path]:
    root = FEATURE_ROOT / f"symbol={symbol}"
    if not root.exists():
        raise FileNotFoundError(f"Missing feature folder: {root}")
    files = sorted(root.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found under: {root}")
    return files


def load_validated_candidates(symbol: str, top_n: int) -> pd.DataFrame:
    path = VALIDATION_ROOT / f"{symbol.lower()}_extended_horizon_signal_validation_ranked_latest.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing Script 04 ranked validation file: {path}")
    frame = pd.read_csv(path)
    required = {
        "target", "feature", "candidate_side", "threshold_quantile", "threshold_side",
        "learned_threshold_value", "candidate_contract_id", "validation_status", "validation_score",
        "coverage_status", "threshold_learning_method", "discovery_methodology_version",
        "input_dataset_fingerprint",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Validation ranked file lacks frozen E2 fields: {missing}")
    frame = frame[
        frame["validation_status"].isin(["validation_pass_primary", "validation_pass_secondary"])
        & (frame["coverage_status"] == "complete")
    ].copy()
    if frame.empty:
        raise ValueError("No complete passed validation candidates found")
    for row in frame.to_dict("records"):
        normalise_rule(row)
        if str(row["candidate_contract_id"]) != candidate_contract_id(row):
            raise ValueError("EH06 candidate ID does not match the frozen rule")
    return frame.sort_values("validation_score", ascending=False).head(top_n).copy()


def validate_execution_columns(columns: list[str]) -> None:
    missing = sorted({"bid", "ask", "mid"} - set(columns))
    if missing:
        raise ValueError(
            f"EH06 requires bid/ask/mid quote-price inputs; spread-like substitutes are rejected: {missing}"
        )


def _stats(values: pd.Series | None) -> dict:
    if values is None or len(values) == 0:
        return {"trades": 0, "avg": np.nan, "median": np.nan, "total": np.nan, "win_rate": np.nan, "pf": np.nan}
    clean = pd.to_numeric(values, errors="coerce").dropna()
    return {
        "trades": int(len(clean)), "avg": float(clean.mean()), "median": float(clean.median()),
        "total": float(clean.sum()), "win_rate": float((clean > 0).mean()), "pf": profit_factor(clean),
    }


def calculate_signal_returns(
    df: pd.DataFrame, candidate: dict, file_path: Path, *, min_trades: int = MIN_TRADES_PER_FILE,
    input_file_sha256: str = "",
) -> list[dict]:
    rule = normalise_rule(candidate)
    rows: list[dict] = []
    for scenario, multiplier in DYNAMIC_COST_SCENARIOS.items():
        base = {
            "file": str(file_path), "filename": file_path.name, "file_date": file_date(file_path), **rule,
            "candidate_contract_id": candidate_contract_id(candidate),
            "economic_contract_id": economic_contract_id(candidate_contract_id(candidate), scenario, multiplier),
            "dynamic_cost_scenario": scenario, "cost_multiplier": multiplier,
            "cost_input_field": "bid,ask,mid", "cost_input_unit": "quote_price_units",
            "cost_transformation": "scale observed half-spread around mid, then construct bid/ask execution return",
            "cost_interpretation": "observed execution" if multiplier == 1.0 else "counterfactual spread stress",
            "spread_col_used": "derived_ask_minus_bid_quote_price",
            "threshold_learning_method": candidate.get("threshold_learning_method", ""),
            "threshold_provenance": candidate.get("threshold_provenance", "frozen_e1_candidate"),
            "selected_side_provenance": candidate.get("selected_side_provenance", "e1_stability_selection"),
            "discovery_methodology_version": candidate.get("discovery_methodology_version", ""),
            "validation_methodology_version": candidate.get("validation_methodology_version", ""),
            "dynamic_cost_methodology_version": EH06_DYNAMIC_COST_METHODOLOGY_VERSION,
            "economic_unit_model": ECONOMIC_UNIT_MODEL, "execution_model": EXECUTION_MODEL,
            "input_dataset_fingerprint": candidate.get("input_dataset_fingerprint", ""),
            "evaluation_dataset_fingerprint": candidate.get("evaluation_dataset_fingerprint", ""),
            "symbol_metadata_schema_version": candidate.get("symbol_metadata_schema_version", ""),
            "symbol_registry_fingerprint": candidate.get("symbol_registry_fingerprint", ""),
            "feature_role_contract_version": candidate.get("feature_role_contract_version", ""),
            "target_contract_version": candidate.get("target_contract_version", ""),
            "feature_contract_fingerprint": candidate.get("feature_contract_fingerprint", ""),
            "input_file_sha256": input_file_sha256,
            "validation_status": candidate.get("validation_status", ""),
            "validation_score": candidate.get("validation_score", np.nan),
        }
        try:
            validate_execution_columns(df.columns.tolist())
            gross, executable, _ = trade_returns(df, candidate, spread_multiplier=multiplier)
            if executable is None:
                raise ValueError("Executable returns unavailable")
            gross_stats, exec_stats = _stats(gross), _stats(executable)
            status, reason = "success", ""
            if exec_stats["trades"] < min_trades:
                status, reason = "skipped", f"executable_trades_below_{min_trades}"
            rows.append({
                **base, "evaluation_status": status, "skip_reason": reason,
                "execution_evidence_status": "executable_observed_bid_ask" if multiplier == 1.0 else "executable_spread_stress",
                "trades": exec_stats["trades"],
                "gross_avg_return": gross_stats["avg"], "gross_median_return": gross_stats["median"],
                "gross_total_return": gross_stats["total"],
                "executable_avg_return": exec_stats["avg"], "executable_median_return": exec_stats["median"],
                "executable_total_return": exec_stats["total"], "executable_win_rate": exec_stats["win_rate"],
                "executable_profit_factor": exec_stats["pf"],
                "avg_dynamic_cost": gross_stats["avg"] - exec_stats["avg"],
                "median_dynamic_cost": gross_stats["median"] - exec_stats["median"],
                "total_dynamic_cost": gross_stats["total"] - exec_stats["total"],
                # EH07 compatibility aliases; explicitly executable-return semantics.
                "net_avg_return": exec_stats["avg"], "net_median_return": exec_stats["median"],
                "net_total_return": exec_stats["total"], "net_win_rate": exec_stats["win_rate"],
                "net_profit_factor": exec_stats["pf"], "threshold_value": rule["learned_threshold_value"],
            })
        except Exception as exc:
            rows.append({
                **base, "evaluation_status": "failed", "skip_reason": f"{type(exc).__name__}:{exc}",
                "execution_evidence_status": "execution_unavailable", "trades": 0,
                "gross_avg_return": np.nan, "gross_median_return": np.nan, "gross_total_return": np.nan,
                "executable_avg_return": np.nan, "executable_median_return": np.nan,
                "executable_total_return": np.nan, "executable_win_rate": np.nan,
                "executable_profit_factor": np.nan, "avg_dynamic_cost": np.nan,
                "median_dynamic_cost": np.nan, "total_dynamic_cost": np.nan,
                "net_avg_return": np.nan, "net_median_return": np.nan, "net_total_return": np.nan,
                "net_win_rate": np.nan, "net_profit_factor": np.nan,
                "threshold_value": rule["learned_threshold_value"],
            })
    return rows


def aggregate_dynamic_cost(raw: pd.DataFrame, expected_files: int | None = None) -> pd.DataFrame:
    if raw.empty:
        return raw
    identity = [
        "dynamic_cost_scenario", "cost_multiplier", "target", "feature", "candidate_side",
        "threshold_quantile", "threshold_side", "learned_threshold_value", "candidate_contract_id",
        "economic_contract_id", "spread_col_used",
    ]
    grouped = raw.groupby(identity, dropna=False).agg(
        attempted_files=("file", "nunique"),
        successful_files=("evaluation_status", lambda s: int((s == "success").sum())),
        failed_files=("evaluation_status", lambda s: int((s == "failed").sum())),
        skipped_files=("evaluation_status", lambda s: int((s == "skipped").sum())),
        total_trades=("trades", "sum"), gross_total_return=("gross_total_return", "sum"),
        median_gross_avg_return=("gross_avg_return", "median"),
        executable_total_return=("executable_total_return", "sum"),
        median_executable_avg_return=("executable_avg_return", "median"),
        median_executable_win_rate=("executable_win_rate", "median"),
        median_executable_profit_factor=("executable_profit_factor", "median"),
        median_dynamic_cost=("median_dynamic_cost", "median"), total_dynamic_cost=("total_dynamic_cost", "sum"),
        mean_validation_score=("validation_score", "mean"),
        dynamic_cost_methodology_version=("dynamic_cost_methodology_version", "first"),
        economic_unit_model=("economic_unit_model", "first"), execution_model=("execution_model", "first"),
        input_dataset_fingerprint=("input_dataset_fingerprint", "first"),
        evaluation_dataset_fingerprint=("evaluation_dataset_fingerprint", "first"),
        threshold_learning_method=("threshold_learning_method", "first"),
        threshold_provenance=("threshold_provenance", "first"),
        selected_side_provenance=("selected_side_provenance", "first"),
        discovery_methodology_version=("discovery_methodology_version", "first"),
        validation_methodology_version=("validation_methodology_version", "first"),
        symbol_metadata_schema_version=("symbol_metadata_schema_version", "first"),
        symbol_registry_fingerprint=("symbol_registry_fingerprint", "first"),
        feature_role_contract_version=("feature_role_contract_version", "first"),
        target_contract_version=("target_contract_version", "first"),
        feature_contract_fingerprint=("feature_contract_fingerprint", "first"),
        earliest_processed_date=("file_date", "min"), latest_processed_date=("file_date", "max"),
    ).reset_index()
    grouped["expected_files"] = int(expected_files if expected_files is not None else raw["file"].nunique())
    grouped["files_tested"] = grouped["successful_files"]
    grouped["coverage_status"] = np.where(
        (grouped["attempted_files"] == grouped["expected_files"])
        & (grouped["successful_files"] == grouped["expected_files"])
        & (grouped["failed_files"] == 0) & (grouped["skipped_files"] == 0), "complete", "incomplete"
    )
    grouped["dynamic_cost_score"] = (
        grouped["executable_total_return"].fillna(0)
        + grouped["median_executable_avg_return"].fillna(0) * 100000
        + (grouped["median_executable_win_rate"].fillna(0.5) - 0.5) * 100
        + grouped["median_executable_profit_factor"].replace([np.inf, -np.inf], np.nan).fillna(0) * 5
    )
    complete = grouped["coverage_status"] == "complete"
    grouped["dynamic_survival_status"] = np.select(
        [~complete,
         (grouped["executable_total_return"] > 0) & (grouped["median_executable_avg_return"] > 0)
         & (grouped["median_executable_win_rate"] > 0.52) & (grouped["median_executable_profit_factor"] > 1.10),
         (grouped["executable_total_return"] > 0) & (grouped["median_executable_avg_return"] > 0)
         & (grouped["median_executable_win_rate"] > 0.505), grouped["executable_total_return"] <= 0],
        ["dynamic_evidence_incomplete", "dynamic_survivor_primary", "dynamic_survivor_secondary", "dynamic_cost_fail"],
        default="dynamic_watchlist",
    )
    # EH07 aliases.
    grouped["net_total_return"] = grouped["executable_total_return"]
    grouped["median_net_avg_return"] = grouped["median_executable_avg_return"]
    grouped["median_net_win_rate"] = grouped["median_executable_win_rate"]
    grouped["median_net_profit_factor"] = grouped["median_executable_profit_factor"]
    return grouped.sort_values(["dynamic_survival_status", "dynamic_cost_score"], ascending=[True, False])


def write_outputs(symbol: str, raw: pd.DataFrame, ranked: pd.DataFrame, coverage: pd.DataFrame) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    prefix = f"{symbol.lower()}_extended_horizon_dynamic_cost"
    raw.to_csv(REPORT_ROOT / f"{prefix}_raw_latest.csv", index=False)
    ranked.to_csv(REPORT_ROOT / f"{prefix}_ranked_latest.csv", index=False)
    ranked[ranked["dynamic_survival_status"].isin(["dynamic_survivor_primary", "dynamic_survivor_secondary"])].to_csv(
        REPORT_ROOT / f"{symbol.lower()}_extended_horizon_dynamic_cost_survivors_latest.csv", index=False
    )
    coverage.to_csv(REPORT_ROOT / f"{prefix}_coverage_latest.csv", index=False)


def main(symbol: str, top_n: int) -> None:
    get_symbol_metadata(symbol)
    files = find_feature_files(symbol)
    candidates = load_validated_candidates(symbol, top_n)
    records = candidates.to_dict("records")
    rows: list[dict] = []
    coverage_rows: list[dict] = []
    for path in files:
        fingerprint = file_sha256(path)
        try:
            columns = pd.read_parquet(path).columns.tolist()
            validate_execution_columns(columns)
            needed = sorted({name for row in records for name in (row["feature"], row["target"])} | {"bid", "ask", "mid"})
            frame = pd.read_parquet(path, columns=[name for name in needed if name in columns])
            for candidate in records:
                rows.extend(calculate_signal_returns(frame, candidate, path, input_file_sha256=fingerprint))
            coverage_rows.append({"file": str(path), "status": "success", "reason": "", "input_file_sha256": fingerprint})
        except Exception as exc:
            coverage_rows.append({"file": str(path), "status": "failed", "reason": f"{type(exc).__name__}:{exc}", "input_file_sha256": fingerprint})
            for candidate in records:
                rows.extend(calculate_signal_returns(pd.DataFrame(), candidate, path, input_file_sha256=fingerprint))
    raw = pd.DataFrame(rows)
    write_outputs(symbol, raw, aggregate_dynamic_cost(raw, len(files)), pd.DataFrame(coverage_rows))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    args = parser.parse_args()
    main(args.symbol.upper(), args.top_n)
