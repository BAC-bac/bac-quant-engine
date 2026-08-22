"""EH04: replay frozen E1 candidates as post-selection validation evidence."""

from __future__ import annotations

import argparse
from hashlib import sha256
from pathlib import Path
import re
import sys

import numpy as np
import pandas as pd

DUKASCOPY_TICKS_DIR = Path(__file__).resolve().parents[1] / "dukascopy_ticks"
if str(DUKASCOPY_TICKS_DIR) not in sys.path:
    sys.path.insert(0, str(DUKASCOPY_TICKS_DIR))

from dukascopy_contract import (  # noqa: E402
    SYMBOL_METADATA_SCHEMA_VERSION, file_sha256, get_symbol_metadata, registry_fingerprint,
)
from dukascopy_feature_contract import (  # noqa: E402
    FEATURE_ROLE_CONTRACT_VERSION, TARGET_CONTRACT_VERSION, feature_contract_fingerprint,
    require_predictor, require_target,
)
from extended_horizons_e2_contract import (  # noqa: E402
    EH04_VALIDATION_METHODOLOGY_VERSION, ECONOMIC_UNIT_MODEL, EXECUTION_MODEL,
    VALIDATION_EVIDENCE_CLASS, candidate_contract_id, normalise_rule, profit_factor,
    trade_returns,
)

DEFAULT_SYMBOL = "EURJPY"
DEFAULT_TOP_N = 75
MIN_TRADES_PER_FILE = 100
BASE_DIR = Path("E:/Quant_Lab")
FEATURE_ROOT = BASE_DIR / "data" / "processed" / "dukascopy_extended_horizon_features"
STABILITY_ROOT = BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "feature_stability"
REPORT_ROOT = BASE_DIR / "data" / "analysis" / "dukascopy_extended_horizons" / "signal_validation"
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


def load_candidates(symbol: str, top_n: int) -> pd.DataFrame:
    path = STABILITY_ROOT / f"{symbol.lower()}_extended_horizon_stable_candidates_latest.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing Script 03 stable candidates file: {path}")
    frame = pd.read_csv(path)
    if frame.empty:
        raise ValueError("Stable candidates file is empty")
    required = {
        "target", "feature", "selected_side", "threshold_quantile", "threshold_side",
        "learned_threshold_value", "threshold_learning_method", "discovery_methodology_version",
        "input_dataset_fingerprint", "stability_score", "stability_status",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Stable candidates file lacks frozen E1 fields: {missing}")
    for row in frame.to_dict("records"):
        rule = normalise_rule(row)
        require_predictor(rule["feature"])
        require_target(rule["target"], approved_extra_targets=[rule["target"]])
        expected_id = candidate_contract_id(row)
        if "candidate_contract_id" in frame and str(row["candidate_contract_id"]) != expected_id:
            raise ValueError("Candidate contract ID does not match the frozen rule")
    return frame.sort_values("stability_score", ascending=False).head(top_n).copy()


def _return_stats(prefix: str, values: pd.Series | None) -> dict:
    if values is None or len(values) == 0:
        return {
            f"{prefix}_trades": 0, f"{prefix}_win_rate": np.nan,
            f"{prefix}_avg_return": np.nan, f"{prefix}_median_return": np.nan,
            f"{prefix}_total_return": np.nan, f"{prefix}_profit_factor": np.nan,
        }
    clean = pd.to_numeric(values, errors="coerce").dropna()
    return {
        f"{prefix}_trades": int(len(clean)), f"{prefix}_win_rate": float((clean > 0).mean()),
        f"{prefix}_avg_return": float(clean.mean()), f"{prefix}_median_return": float(clean.median()),
        f"{prefix}_total_return": float(clean.sum()), f"{prefix}_profit_factor": profit_factor(clean),
    }


def validate_candidate_on_file(
    df: pd.DataFrame, candidate: dict, file_path: Path, *,
    min_trades: int = MIN_TRADES_PER_FILE, input_file_sha256: str = "",
) -> dict:
    """Evaluate one immutable E1 rule; no quantile is learned here."""
    rule = normalise_rule(candidate)
    base = {
        "file": str(file_path), "filename": file_path.name, "file_date": file_date(file_path), **rule,
        "candidate_contract_id": candidate_contract_id(candidate),
        "threshold_learning_method": str(candidate["threshold_learning_method"]),
        "threshold_provenance": str(candidate.get("threshold_provenance", "frozen_e1_candidate")),
        "selected_side_provenance": str(candidate.get("selected_side_method", "e1_stability_selection")),
        "discovery_methodology_version": str(candidate["discovery_methodology_version"]),
        "stability_methodology_version": str(candidate.get("stability_methodology_version", "")),
        "input_dataset_fingerprint": str(candidate["input_dataset_fingerprint"]),
        "evaluation_dataset_fingerprint": str(candidate.get("evaluation_dataset_fingerprint", "")),
        "discovery_interval_start": str(candidate.get("discovery_interval_start", "")),
        "discovery_interval_end": str(candidate.get("discovery_interval_end", "")),
        "input_file_sha256": input_file_sha256,
        "validation_evidence_class": VALIDATION_EVIDENCE_CLASS,
        "validation_methodology_version": EH04_VALIDATION_METHODOLOGY_VERSION,
        "economic_unit_model": ECONOMIC_UNIT_MODEL, "execution_model": EXECUTION_MODEL,
        "symbol_metadata_schema_version": SYMBOL_METADATA_SCHEMA_VERSION,
        "symbol_registry_fingerprint": registry_fingerprint(),
        "feature_role_contract_version": FEATURE_ROLE_CONTRACT_VERSION,
        "target_contract_version": TARGET_CONTRACT_VERSION,
        "feature_contract_fingerprint": feature_contract_fingerprint(),
        "stability_score": candidate.get("stability_score", np.nan),
        "stability_status": candidate.get("stability_status", ""),
    }
    try:
        gross, executable, signal_mask = trade_returns(df, candidate, spread_multiplier=1.0)
        stats = {**_return_stats("gross_mid", gross), **_return_stats("executable", executable)}
        execution_status = "executable_observed_bid_ask" if executable is not None else "execution_unavailable_missing_bid_ask"
        status, reason = "success", ""
        if len(gross) < min_trades:
            status, reason = "skipped", f"gross_trades_below_{min_trades}"
        stats.update({
            "trades": stats["gross_mid_trades"], "win_rate": stats["gross_mid_win_rate"],
            "avg_return": stats["gross_mid_avg_return"], "median_return": stats["gross_mid_median_return"],
            "total_return": stats["gross_mid_total_return"], "profit_factor": stats["gross_mid_profit_factor"],
            "expectancy": stats["gross_mid_avg_return"],
        })
        return {
            **base, **stats, "signal_rows": int(signal_mask.sum()),
            "evaluation_status": status, "skip_reason": reason,
            "execution_evidence_status": execution_status,
        }
    except Exception as exc:
        return {
            **base, **_return_stats("gross_mid", None), **_return_stats("executable", None),
            "trades": 0, "win_rate": np.nan, "avg_return": np.nan, "median_return": np.nan,
            "total_return": np.nan, "profit_factor": np.nan, "expectancy": np.nan,
            "signal_rows": 0, "evaluation_status": "failed",
            "skip_reason": f"{type(exc).__name__}:{exc}",
            "execution_evidence_status": "execution_unavailable_evaluation_failed",
        }


def aggregate_validation(raw: pd.DataFrame, expected_files: int | None = None) -> pd.DataFrame:
    if raw.empty:
        return raw
    identity = [
        "target", "feature", "candidate_side", "threshold_quantile", "threshold_side",
        "threshold_operator", "learned_threshold_value", "candidate_contract_id",
    ]
    grouped = raw.groupby(identity, dropna=False).agg(
        attempted_files=("file", "nunique"),
        successful_files=("evaluation_status", lambda s: int((s == "success").sum())),
        failed_files=("evaluation_status", lambda s: int((s == "failed").sum())),
        skipped_files=("evaluation_status", lambda s: int((s == "skipped").sum())),
        total_trades=("gross_mid_trades", "sum"), mean_win_rate=("gross_mid_win_rate", "mean"),
        median_win_rate=("gross_mid_win_rate", "median"), mean_avg_return=("gross_mid_avg_return", "mean"),
        median_avg_return=("gross_mid_avg_return", "median"), total_return=("gross_mid_total_return", "sum"),
        median_profit_factor=("gross_mid_profit_factor", "median"),
        executable_total_return=("executable_total_return", lambda s: s.sum(min_count=1)),
        executable_median_avg_return=("executable_avg_return", "median"),
        executable_median_win_rate=("executable_win_rate", "median"),
        executable_median_profit_factor=("executable_profit_factor", "median"),
        validation_evidence_class=("validation_evidence_class", "first"),
        validation_methodology_version=("validation_methodology_version", "first"),
        discovery_methodology_version=("discovery_methodology_version", "first"),
        input_dataset_fingerprint=("input_dataset_fingerprint", "first"),
        evaluation_dataset_fingerprint=("evaluation_dataset_fingerprint", "first"),
        threshold_learning_method=("threshold_learning_method", "first"),
        execution_evidence_status=("execution_evidence_status", "first"),
        economic_unit_model=("economic_unit_model", "first"), execution_model=("execution_model", "first"),
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
        & (grouped["failed_files"] == 0) & (grouped["skipped_files"] == 0), "complete", "incomplete",
    )
    grouped["validation_score"] = (
        (grouped["mean_win_rate"].fillna(0.5) - 0.5) * 100
        + grouped["mean_avg_return"].fillna(0) * 100000
        + np.log1p(grouped["total_trades"].fillna(0)) * 2
        + grouped["median_profit_factor"].replace([np.inf, -np.inf], np.nan).fillna(0)
    )
    complete = grouped["coverage_status"] == "complete"
    grouped["validation_status"] = np.select(
        [~complete,
         (grouped["total_trades"] >= 10_000) & (grouped["median_win_rate"] > 0.52)
         & (grouped["median_avg_return"] > 0) & (grouped["median_profit_factor"] > 1.05),
         (grouped["total_trades"] >= 10_000) & (grouped["median_win_rate"] > 0.505)
         & (grouped["median_avg_return"] > 0), grouped["total_trades"] < 10_000],
        ["validation_incomplete", "validation_pass_primary", "validation_pass_secondary", "insufficient_trades"],
        default="validation_fail",
    )
    return grouped.sort_values(["validation_status", "validation_score"], ascending=[True, False])


def write_outputs(symbol: str, raw: pd.DataFrame, ranked: pd.DataFrame, coverage: pd.DataFrame) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    prefix = f"{symbol.lower()}_extended_horizon_signal_validation"
    raw.to_csv(REPORT_ROOT / f"{prefix}_raw_latest.csv", index=False)
    ranked.to_csv(REPORT_ROOT / f"{prefix}_ranked_latest.csv", index=False)
    ranked[ranked["validation_status"].isin(["validation_pass_primary", "validation_pass_secondary"])].to_csv(
        REPORT_ROOT / f"{prefix}_passed_latest.csv", index=False)
    coverage.to_csv(REPORT_ROOT / f"{prefix}_coverage_latest.csv", index=False)


def main(symbol: str, top_n: int) -> None:
    get_symbol_metadata(symbol)
    files = find_feature_files(symbol)
    candidates = load_candidates(symbol, top_n)
    fingerprints = {path: file_sha256(path) for path in files}
    evaluation_fingerprint = sha256("\n".join(fingerprints[path] for path in files).encode("utf-8")).hexdigest()
    all_rows: list[dict] = []
    coverage_rows: list[dict] = []
    records = candidates.to_dict("records")
    for path in files:
        try:
            columns = pd.read_parquet(path).columns.tolist()
            needed = sorted({field for row in records for field in (row["feature"], row["target"])} | {"bid", "ask", "mid"})
            frame = pd.read_parquet(path, columns=[name for name in needed if name in columns])
            for candidate in records:
                row = validate_candidate_on_file(frame, candidate, path, input_file_sha256=fingerprints[path])
                row["evaluation_dataset_fingerprint"] = evaluation_fingerprint
                all_rows.append(row)
            coverage_rows.append({"file": str(path), "status": "success", "reason": "", "input_file_sha256": fingerprints[path]})
        except Exception as exc:
            coverage_rows.append({"file": str(path), "status": "failed", "reason": f"{type(exc).__name__}:{exc}", "input_file_sha256": fingerprints[path]})
            for candidate in records:
                row = validate_candidate_on_file(pd.DataFrame(), candidate, path, input_file_sha256=fingerprints[path])
                row["evaluation_dataset_fingerprint"] = evaluation_fingerprint
                all_rows.append(row)
    raw = pd.DataFrame(all_rows)
    raw["expected_files"] = len(files)
    raw["attempted_files"] = raw.groupby("candidate_contract_id")["file"].transform("nunique")
    raw["successful_files"] = raw.groupby("candidate_contract_id")["evaluation_status"].transform(lambda s: int((s == "success").sum()))
    raw["failed_files"] = raw.groupby("candidate_contract_id")["evaluation_status"].transform(lambda s: int((s == "failed").sum()))
    raw["skipped_files"] = raw.groupby("candidate_contract_id")["evaluation_status"].transform(lambda s: int((s == "skipped").sum()))
    raw["coverage_status"] = np.where(
        (raw["attempted_files"] == raw["expected_files"])
        & (raw["successful_files"] == raw["expected_files"])
        & (raw["failed_files"] == 0) & (raw["skipped_files"] == 0), "complete", "incomplete"
    )
    write_outputs(symbol, raw, aggregate_validation(raw, len(files)), pd.DataFrame(coverage_rows))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    args = parser.parse_args()
    main(args.symbol.upper(), args.top_n)
