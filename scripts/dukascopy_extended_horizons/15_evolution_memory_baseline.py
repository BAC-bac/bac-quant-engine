from __future__ import annotations

"""BACQE EH15 - Evolution Memory Baseline Research Engine.

Stage 1 establishes a durable, append-only scientific memory for EH14 edge
families. Each distinct EH14 family snapshot contributes one observation per
family. Re-running EH15 against unchanged EH14 outputs is idempotent and does
not create duplicate history.
"""

import argparse
import hashlib
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

ANALYSIS_ROOT = Path(r"E:\Quant_Lab\data\analysis\dukascopy_extended_horizons")
EH14_OUTPUT_NAME = "candidate_census"
OUTPUT_NAME = "evolution_memory"
ENGINE_VERSION = "1.0.0"
HISTORY_SCHEMA_VERSION = "1.0"
WIDTH = 110

FAMILY_REQUIRED = {
    "edge_family_id",
    "target",
    "feature_family",
    "threshold_side",
    "context_type",
    "parent_context",
    "candidate_count",
    "symbol_count",
    "tier_1_count",
    "tier_2_count",
    "tier_3_count",
    "reject_count",
}

MEMBER_REQUIRED = {
    "edge_family_id",
    "candidate_id",
    "test_symbol",
    "candidate_tier",
    "candidate_layer",
    "context_type",
    "context_value",
    "parent_context",
}

TIER_COLUMNS = {
    "tier_1_priority_candidate": "tier_1_count",
    "tier_2_research_candidate": "tier_2_count",
    "tier_3_watchlist_candidate": "tier_3_count",
    "reject_or_hold": "reject_count",
}

NUMERIC_FAMILY_COLUMNS = [
    "candidate_count",
    "symbol_count",
    "tier_1_count",
    "tier_2_count",
    "tier_3_count",
    "reject_count",
    "total_trades",
    "total_net_return",
    "median_candidate_score",
    "max_candidate_score",
    "median_confidence_score",
    "median_win_rate",
    "median_profit_factor",
    "median_positive_year_rate",
    "transfer_success_rate",
    "year_stable_rate",
    "family_independence_score",
]

HISTORY_KEY = ["snapshot_id", "edge_family_id"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create the EH15 Stage 1 edge-family evolution memory baseline."
    )
    parser.add_argument("--family-registry", type=Path)
    parser.add_argument("--family-members", type=Path)
    parser.add_argument("--analysis-root", type=Path, default=ANALYSIS_ROOT)
    parser.add_argument("--eh14-dir", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--observation-utc",
        help="Optional ISO-8601 UTC observation time. Defaults to current UTC.",
    )
    parser.add_argument("--top-n", type=int, default=15)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_observation_utc(value: str | None) -> datetime:
    if not value:
        return utc_now()
    text = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def resolve_inputs(
    family_registry: Path | None,
    family_members: Path | None,
    eh14_dir: Path,
) -> tuple[Path, Path]:
    registry = family_registry or eh14_dir / "candidate_family_registry_latest.csv"
    members = family_members or eh14_dir / "candidate_family_members_latest.csv"
    missing = [str(path) for path in (registry, members) if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing EH14 input file(s): " + ", ".join(missing)
            + ". Run EH14 first or provide explicit paths."
        )
    return registry, members


def read_csv(path: Path, required: set[str], label: str) -> pd.DataFrame:
    frame = pd.read_csv(path, low_memory=False)
    if frame.empty:
        raise ValueError(f"{label} is empty: {path}")
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {', '.join(missing)}")
    return frame


def load_family_registry(path: Path) -> pd.DataFrame:
    frame = read_csv(path, FAMILY_REQUIRED, "EH14 family registry")
    if frame["edge_family_id"].isna().any():
        raise ValueError("EH14 family registry contains missing edge_family_id values.")
    if frame["edge_family_id"].duplicated().any():
        duplicates = sorted(
            frame.loc[frame["edge_family_id"].duplicated(False), "edge_family_id"]
            .astype(str)
            .unique()
        )
        raise ValueError(
            "EH14 family registry must contain one row per family. Duplicate IDs: "
            + ", ".join(duplicates[:10])
        )
    for column in NUMERIC_FAMILY_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def load_family_members(path: Path) -> pd.DataFrame:
    frame = read_csv(path, MEMBER_REQUIRED, "EH14 family members")
    if frame["edge_family_id"].isna().any():
        raise ValueError("EH14 family members contain missing edge_family_id values.")
    return frame


def validate_family_alignment(families: pd.DataFrame, members: pd.DataFrame) -> None:
    family_ids = set(families["edge_family_id"].astype(str))
    member_ids = set(members["edge_family_id"].astype(str))
    unknown_members = sorted(member_ids - family_ids)
    empty_families = sorted(family_ids - member_ids)
    if unknown_members:
        raise ValueError(
            "EH14 members reference families absent from the registry: "
            + ", ".join(unknown_members[:10])
        )
    if empty_families:
        raise ValueError(
            "EH14 family registry contains families with no member rows: "
            + ", ".join(empty_families[:10])
        )


def canonical_scalar(value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        number = float(value)
        if not math.isfinite(number):
            return None
        return round(number, 12)
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    return str(value).strip()


def canonical_records(frame: pd.DataFrame, columns: Iterable[str]) -> list[dict[str, object]]:
    selected = [column for column in columns if column in frame.columns]
    ordered = frame[selected].copy()
    ordered = ordered.sort_values(selected, kind="stable", na_position="last").reset_index(drop=True)
    return [
        {column: canonical_scalar(value) for column, value in row.items()}
        for row in ordered.to_dict(orient="records")
    ]


def snapshot_id(families: pd.DataFrame, members: pd.DataFrame) -> str:
    family_columns = sorted(families.columns)
    member_columns = sorted(
        column
        for column in members.columns
        if column not in {"generated_utc", "run_utc", "observation_utc"}
    )
    payload = {
        "families": canonical_records(families, family_columns),
        "members": canonical_records(members, member_columns),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "eh14_" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:20]


def safe_numeric(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values[np.isfinite(values)]


def safe_sum(series: pd.Series) -> float:
    values = safe_numeric(series)
    return float(values.sum()) if not values.empty else np.nan


def safe_mean(series: pd.Series) -> float:
    values = safe_numeric(series)
    return float(values.mean()) if not values.empty else np.nan


def safe_median(series: pd.Series) -> float:
    values = safe_numeric(series)
    return float(values.median()) if not values.empty else np.nan


def safe_max(series: pd.Series) -> float:
    values = safe_numeric(series)
    return float(values.max()) if not values.empty else np.nan


def join_unique(series: pd.Series) -> str:
    values = sorted({str(value).strip() for value in series.dropna() if str(value).strip()})
    return ", ".join(values)


def current_observations(
    families: pd.DataFrame,
    members: pd.DataFrame,
    observation_utc: datetime,
    source_snapshot_id: str,
    registry_path: Path,
    members_path: Path,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    observed_at = iso_utc(observation_utc)

    for family_id, group in members.groupby("edge_family_id", sort=True, observed=False):
        family_row = families.loc[families["edge_family_id"] == family_id].iloc[0]
        tiers = group["candidate_tier"].value_counts()
        symbols = sorted(group["test_symbol"].dropna().astype(str).str.strip().unique())
        contexts = sorted(
            (
                group["context_type"].fillna("").astype(str).str.strip()
                + "::"
                + group["context_value"].fillna("").astype(str).str.strip()
            ).unique()
        )
        contexts = [context for context in contexts if context != "::"]
        layers = sorted(group["candidate_layer"].dropna().astype(str).str.strip().unique())
        candidate_ids = group["candidate_id"].dropna().astype(str).str.strip()

        row: dict[str, object] = {
            "history_schema_version": HISTORY_SCHEMA_VERSION,
            "engine_version": ENGINE_VERSION,
            "snapshot_id": source_snapshot_id,
            "observation_utc": observed_at,
            "edge_family_id": family_id,
            "target": family_row.get("target"),
            "feature_family": family_row.get("feature_family"),
            "threshold_side": family_row.get("threshold_side"),
            "context_type": family_row.get("context_type"),
            "parent_context": family_row.get("parent_context"),
            "member_count": int(len(group)),
            "distinct_candidate_count": int(candidate_ids.nunique()),
            "tier_1_count": int(tiers.get("tier_1_priority_candidate", 0)),
            "tier_2_count": int(tiers.get("tier_2_research_candidate", 0)),
            "tier_3_count": int(tiers.get("tier_3_watchlist_candidate", 0)),
            "reject_count": int(tiers.get("reject_or_hold", 0)),
            "priority_member_count": int(
                group["candidate_tier"].isin(
                    {"tier_1_priority_candidate", "tier_2_research_candidate"}
                ).sum()
            ),
            "symbol_count": int(len(symbols)),
            "symbols_present": ", ".join(symbols),
            "context_count": int(len(contexts)),
            "contexts_present": ", ".join(contexts),
            "candidate_layer_count": int(len(layers)),
            "candidate_layers_present": ", ".join(layers),
            "is_orphan_family": bool(len(group) == 1),
            "family_population_class": family_row.get("family_population_class"),
            "recommended_research_status": family_row.get("recommended_research_status"),
            "recommended_next_step": family_row.get("recommended_next_step"),
            "family_concentration_risk": family_row.get("family_concentration_risk"),
            "total_trades": canonical_scalar(family_row.get("total_trades")),
            "total_net_return": canonical_scalar(family_row.get("total_net_return")),
            "median_candidate_score": canonical_scalar(family_row.get("median_candidate_score")),
            "max_candidate_score": canonical_scalar(family_row.get("max_candidate_score")),
            "median_confidence_score": canonical_scalar(family_row.get("median_confidence_score")),
            "median_win_rate": canonical_scalar(family_row.get("median_win_rate")),
            "median_profit_factor": canonical_scalar(family_row.get("median_profit_factor")),
            "median_positive_year_rate": canonical_scalar(family_row.get("median_positive_year_rate")),
            "transfer_success_rate": canonical_scalar(family_row.get("transfer_success_rate")),
            "year_stable_rate": canonical_scalar(family_row.get("year_stable_rate")),
            "family_independence_score": canonical_scalar(family_row.get("family_independence_score")),
            "source_family_registry": str(registry_path),
            "source_family_members": str(members_path),
        }
        row["observation_id"] = hashlib.sha1(
            f"{source_snapshot_id}||{family_id}".encode("utf-8")
        ).hexdigest()[:24]
        rows.append(row)

    result = pd.DataFrame(rows)
    if result.empty:
        raise ValueError("No family observations were produced.")
    if result["edge_family_id"].duplicated().any():
        raise AssertionError("Current observation contains duplicate edge_family_id values.")
    return result.sort_values("edge_family_id", kind="stable").reset_index(drop=True)


def load_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path, low_memory=False)
    if frame.empty:
        return frame
    missing = sorted(set(HISTORY_KEY) - set(frame.columns))
    if missing:
        raise ValueError(
            "Existing EH15 history is incompatible; missing columns: " + ", ".join(missing)
        )
    if frame.duplicated(HISTORY_KEY).any():
        raise ValueError("Existing EH15 history contains duplicate snapshot/family observations.")
    return frame


def append_history(history: pd.DataFrame, current: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if history.empty:
        combined = current.copy()
        return combined, len(current)

    history_keys = pd.MultiIndex.from_frame(history[HISTORY_KEY].astype(str))
    current_keys = pd.MultiIndex.from_frame(current[HISTORY_KEY].astype(str))
    new_mask = ~current_keys.isin(history_keys)
    additions = current.loc[new_mask].copy()
    all_columns = list(dict.fromkeys([*history.columns, *additions.columns]))
    combined = history.reindex(columns=all_columns).copy()
    for _, addition in additions.reindex(columns=all_columns).iterrows():
        combined.loc[len(combined)] = addition
    combined = combined.drop_duplicates(HISTORY_KEY, keep="first")
    combined = combined.sort_values(
        ["observation_utc", "edge_family_id"], kind="stable"
    ).reset_index(drop=True)
    return combined, len(additions)


def build_latest(current: pd.DataFrame, history: pd.DataFrame) -> pd.DataFrame:
    historical = history.copy()
    historical["observation_utc"] = pd.to_datetime(
        historical["observation_utc"], utc=True, errors="coerce"
    )
    if historical["observation_utc"].isna().any():
        raise ValueError("EH15 history contains invalid observation_utc values.")

    lifecycle = historical.groupby("edge_family_id", observed=False).agg(
        first_seen_utc=("observation_utc", "min"),
        last_seen_utc=("observation_utc", "max"),
        observation_count=("snapshot_id", "nunique"),
    ).reset_index()
    lifecycle["first_seen_utc"] = lifecycle["first_seen_utc"].map(
        lambda value: value.isoformat()
    )
    lifecycle["last_seen_utc"] = lifecycle["last_seen_utc"].map(
        lambda value: value.isoformat()
    )

    latest = current.merge(lifecycle, on="edge_family_id", how="left", validate="one_to_one")
    latest["is_baseline_observation"] = latest["observation_count"].eq(1)
    latest["memory_status"] = np.where(
        latest["is_baseline_observation"], "baseline_observation", "continuing_observation"
    )
    preferred = [
        "edge_family_id",
        "memory_status",
        "first_seen_utc",
        "last_seen_utc",
        "observation_count",
        "is_baseline_observation",
        "snapshot_id",
        "observation_utc",
        "target",
        "feature_family",
        "threshold_side",
        "context_type",
        "parent_context",
        "member_count",
        "distinct_candidate_count",
        "priority_member_count",
        "tier_1_count",
        "tier_2_count",
        "tier_3_count",
        "reject_count",
        "symbol_count",
        "symbols_present",
        "context_count",
        "contexts_present",
        "candidate_layer_count",
        "candidate_layers_present",
        "is_orphan_family",
        "family_population_class",
        "recommended_research_status",
        "family_concentration_risk",
        "total_trades",
        "total_net_return",
        "median_candidate_score",
        "max_candidate_score",
        "median_confidence_score",
        "median_win_rate",
        "median_profit_factor",
        "median_positive_year_rate",
        "transfer_success_rate",
        "year_stable_rate",
        "family_independence_score",
        "recommended_next_step",
        "observation_id",
        "history_schema_version",
        "engine_version",
        "source_family_registry",
        "source_family_members",
    ]
    columns = [column for column in preferred if column in latest.columns]
    return latest[columns].sort_values(
        ["tier_1_count", "tier_2_count", "symbol_count", "median_candidate_score"],
        ascending=[False, False, False, False],
        kind="stable",
    ).reset_index(drop=True)


def snapshot_summary(
    latest: pd.DataFrame,
    history: pd.DataFrame,
    snapshot: str,
    additions: int,
) -> pd.DataFrame:
    total_snapshots = int(history["snapshot_id"].nunique()) if not history.empty else 0
    metrics = [
        ("current_snapshot_id", snapshot, "Content-derived identity of the current EH14 census."),
        ("families_observed", len(latest), "Families represented in the current EH14 snapshot."),
        ("priority_families", int((latest["priority_member_count"] > 0).sum()), "Families with at least one Tier 1 or Tier 2 member."),
        ("tier_1_families", int((latest["tier_1_count"] > 0).sum()), "Families containing at least one Tier 1 member."),
        ("cross_symbol_families", int((latest["symbol_count"] > 1).sum()), "Families observed on more than one symbol."),
        ("single_symbol_families", int((latest["symbol_count"] == 1).sum()), "Families currently confined to one symbol."),
        ("orphan_families", int(latest["is_orphan_family"].fillna(False).sum()), "Families represented by one current member."),
        ("historical_observations", len(history), "All unique family observations retained by EH15."),
        ("distinct_snapshots", total_snapshots, "Distinct EH14 family snapshots retained by EH15."),
        ("new_observations_appended", additions, "Family observations appended during this run."),
        ("baseline_families", int(latest["is_baseline_observation"].sum()), "Current families with exactly one recorded snapshot."),
        ("continuing_families", int((~latest["is_baseline_observation"]).sum()), "Current families recorded in multiple snapshots."),
    ]
    return pd.DataFrame(metrics, columns=["metric", "value", "interpretation"])


def report_text(
    registry_path: Path,
    members_path: Path,
    latest: pd.DataFrame,
    history: pd.DataFrame,
    summary: pd.DataFrame,
    additions: int,
    top_n: int,
) -> str:
    top_columns = [
        "edge_family_id",
        "memory_status",
        "member_count",
        "tier_1_count",
        "tier_2_count",
        "symbol_count",
        "symbols_present",
        "median_candidate_score",
        "recommended_research_status",
    ]
    top = latest[[column for column in top_columns if column in latest.columns]].head(top_n)
    lines = [
        "BACQE DUKASCOPY EXTENDED HORIZONS",
        f"EH15 v{ENGINE_VERSION} - EVOLUTION MEMORY BASELINE REPORT",
        "=" * WIDTH,
        f"Family registry: {registry_path}",
        f"Family members:  {members_path}",
        f"Observation UTC: {latest['observation_utc'].iloc[0]}",
        f"Snapshot ID:     {latest['snapshot_id'].iloc[0]}",
        "-" * WIDTH,
        "MEMORY SUMMARY",
        summary.to_string(index=False),
        "-" * WIDTH,
        "CURRENT LEADING FAMILIES",
        top.to_string(index=False),
        "-" * WIDTH,
        "MEMORY INTERPRETATION",
    ]
    if history["snapshot_id"].nunique() == 1:
        lines.extend([
            "This is the first recorded family snapshot. EH15 has established the longitudinal baseline;",
            "growth, decline, survival and tier-migration conclusions require later distinct EH14 snapshots.",
        ])
    elif additions == 0:
        lines.extend([
            "The current EH14 snapshot already existed in memory. No duplicate observations were appended.",
            "The latest outputs were refreshed without altering the scientific history.",
        ])
    else:
        lines.extend([
            "A distinct EH14 snapshot was appended to memory. Current families now carry updated first-seen,",
            "last-seen and observation-count fields; Stage 2 can use this history for change analytics.",
        ])
    lines.append("=" * WIDTH)
    return "\n".join(lines) + "\n"


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def atomic_write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    os.close(fd)
    try:
        frame.to_csv(temp_name, index=False)
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def run_live(
    registry_path: Path,
    members_path: Path,
    output_dir: Path,
    observation_utc: datetime,
    top_n: int,
) -> None:
    families = load_family_registry(registry_path)
    members = load_family_members(members_path)
    validate_family_alignment(families, members)

    source_snapshot_id = snapshot_id(families, members)
    current = current_observations(
        families,
        members,
        observation_utc,
        source_snapshot_id,
        registry_path,
        members_path,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    history_path = output_dir / "edge_family_history.csv"
    existing_history = load_history(history_path)
    history, additions = append_history(existing_history, current)
    latest = build_latest(current, history)
    summary = snapshot_summary(latest, history, source_snapshot_id, additions)
    report = report_text(
        registry_path,
        members_path,
        latest,
        history,
        summary,
        additions,
        top_n,
    )

    state = {
        "engine": "EH15 Evolution Memory Baseline",
        "engine_version": ENGINE_VERSION,
        "history_schema_version": HISTORY_SCHEMA_VERSION,
        "status": "complete",
        "observation_utc": iso_utc(observation_utc),
        "snapshot_id": source_snapshot_id,
        "families_observed": int(len(latest)),
        "new_observations_appended": int(additions),
        "historical_observations": int(len(history)),
        "distinct_snapshots": int(history["snapshot_id"].nunique()),
        "source_family_registry": str(registry_path),
        "source_family_members": str(members_path),
    }

    # History is written first so latest/state never advertise an observation
    # that was not durably retained.
    atomic_write_csv(history_path, history)
    atomic_write_csv(output_dir / "edge_family_evolution_latest.csv", latest)
    atomic_write_csv(output_dir / "family_observation_summary_latest.csv", summary)
    atomic_write_text(output_dir / "edge_family_evolution_report_latest.txt", report)
    atomic_write_text(
        output_dir / "evolution_memory_state_latest.json",
        json.dumps(state, indent=2) + "\n",
    )

    print("=" * WIDTH)
    print(
        f"BACQE DUKASCOPY EXTENDED HORIZONS EH15 v{ENGINE_VERSION} - "
        "EVOLUTION MEMORY BASELINE ENGINE"
    )
    print("=" * WIDTH)
    print(f"Families observed:         {len(latest):,}")
    print(f"Priority families:         {int((latest['priority_member_count'] > 0).sum()):,}")
    print(f"Cross-symbol families:     {int((latest['symbol_count'] > 1).sum()):,}")
    print(f"Historical observations:   {len(history):,}")
    print(f"Distinct EH14 snapshots:   {history['snapshot_id'].nunique():,}")
    print(f"New observations appended: {additions:,}")
    print("-" * WIDTH)
    print(summary.to_string(index=False))
    print("-" * WIDTH)
    print(f"Evolution latest: {output_dir / 'edge_family_evolution_latest.csv'}")
    print(f"Evolution history:{history_path}")
    print(f"Report:           {output_dir / 'edge_family_evolution_report_latest.txt'}")
    print("=" * WIDTH)


def synthetic_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    families = pd.DataFrame([
        {
            "edge_family_id": "family_a",
            "target": "future_return_20000",
            "feature_family": "hour",
            "threshold_side": "upper",
            "context_type": "context_hour",
            "parent_context": "hour_14",
            "candidate_count": 2,
            "symbol_count": 2,
            "symbols_present": "EURJPY, USDJPY",
            "tier_1_count": 1,
            "tier_2_count": 1,
            "tier_3_count": 0,
            "reject_count": 0,
            "total_trades": 2000,
            "total_net_return": 220.0,
            "median_candidate_score": 180.0,
            "max_candidate_score": 200.0,
            "median_confidence_score": 88.0,
            "median_win_rate": 0.61,
            "median_profit_factor": 2.1,
            "median_positive_year_rate": 0.9,
            "transfer_success_rate": 1.0,
            "year_stable_rate": 0.5,
            "family_independence_score": 80.0,
            "family_population_class": "small_family",
            "recommended_research_status": "transferable_research_family",
            "recommended_next_step": "Retain for deeper validation.",
            "family_concentration_risk": "low",
        },
        {
            "edge_family_id": "family_b",
            "target": "future_return_50000",
            "feature_family": "spread",
            "threshold_side": "lower",
            "context_type": "context_session",
            "parent_context": "london_newyork_overlap",
            "candidate_count": 1,
            "symbol_count": 1,
            "symbols_present": "EURJPY",
            "tier_1_count": 0,
            "tier_2_count": 0,
            "tier_3_count": 1,
            "reject_count": 0,
            "total_trades": np.nan,
            "total_net_return": np.nan,
            "median_candidate_score": 75.0,
            "max_candidate_score": 75.0,
            "median_confidence_score": 55.0,
            "median_win_rate": np.nan,
            "median_profit_factor": np.nan,
            "median_positive_year_rate": np.nan,
            "transfer_success_rate": 0.0,
            "year_stable_rate": 0.0,
            "family_independence_score": 100.0,
            "family_population_class": "orphan_family",
            "recommended_research_status": "hold_or_reject_family",
            "recommended_next_step": "Hold.",
            "family_concentration_risk": "low",
        },
    ])
    members = pd.DataFrame([
        {
            "edge_family_id": "family_a",
            "candidate_id": "candidate_1",
            "test_symbol": "EURJPY",
            "candidate_tier": "tier_1_priority_candidate",
            "candidate_layer": "base_discovery",
            "context_type": "context_hour",
            "context_value": "14",
            "parent_context": "hour_14",
        },
        {
            "edge_family_id": "family_a",
            "candidate_id": "candidate_1",
            "test_symbol": "USDJPY",
            "candidate_tier": "tier_2_research_candidate",
            "candidate_layer": "cross_symbol_transfer",
            "context_type": "context_hour",
            "context_value": "14",
            "parent_context": "hour_14",
        },
        {
            "edge_family_id": "family_b",
            "candidate_id": "candidate_2",
            "test_symbol": "EURJPY",
            "candidate_tier": "tier_3_watchlist_candidate",
            "candidate_layer": "base_discovery",
            "context_type": "context_session",
            "context_value": "london_newyork_overlap",
            "parent_context": "london_newyork_overlap",
        },
    ])
    return families, members


def self_test() -> None:
    families, members = synthetic_inputs()
    validate_family_alignment(families, members)
    observation = datetime(2026, 7, 19, 8, 0, tzinfo=timezone.utc)
    snap = snapshot_id(families, members)
    current = current_observations(
        families,
        members,
        observation,
        snap,
        Path("family_registry.csv"),
        Path("family_members.csv"),
    )
    history_1, added_1 = append_history(pd.DataFrame(), current)
    history_2, added_2 = append_history(history_1, current)
    latest_1 = build_latest(current, history_2)

    changed_families = families.copy()
    changed_families.loc[changed_families.edge_family_id == "family_a", "candidate_count"] = 3
    changed_members = pd.concat([
        members,
        pd.DataFrame([{
            "edge_family_id": "family_a",
            "candidate_id": "candidate_3",
            "test_symbol": "GBPJPY",
            "candidate_tier": "tier_1_priority_candidate",
            "candidate_layer": "cross_symbol_transfer",
            "context_type": "context_hour",
            "context_value": "14",
            "parent_context": "hour_14",
        }]),
    ], ignore_index=True)
    changed_snap = snapshot_id(changed_families, changed_members)
    current_2 = current_observations(
        changed_families,
        changed_members,
        datetime(2026, 7, 20, 8, 0, tzinfo=timezone.utc),
        changed_snap,
        Path("family_registry.csv"),
        Path("family_members.csv"),
    )
    history_3, added_3 = append_history(history_2, current_2)
    latest_2 = build_latest(current_2, history_3)
    summary = snapshot_summary(latest_2, history_3, changed_snap, added_3)

    tests = {
        "snapshot deterministic": snap == snapshot_id(families.copy(), members.copy()),
        "snapshot changes with evidence": snap != changed_snap,
        "one current row per family": current["edge_family_id"].is_unique and len(current) == 2,
        "member metrics correct": int(current.loc[current.edge_family_id == "family_a", "member_count"].iloc[0]) == 2,
        "distinct candidates correct": int(current.loc[current.edge_family_id == "family_a", "distinct_candidate_count"].iloc[0]) == 1,
        "tier metrics correct": int(current.loc[current.edge_family_id == "family_a", "tier_1_count"].iloc[0]) == 1,
        "symbol coverage correct": int(current.loc[current.edge_family_id == "family_a", "symbol_count"].iloc[0]) == 2,
        "orphan detection": bool(current.loc[current.edge_family_id == "family_b", "is_orphan_family"].iloc[0]),
        "first snapshot appended": added_1 == 2 and len(history_1) == 2,
        "same snapshot idempotent": added_2 == 0 and len(history_2) == 2,
        "changed snapshot appended": added_3 == 2 and len(history_3) == 4,
        "lifecycle observation count": latest_2["observation_count"].eq(2).all(),
        "continuing status": latest_2["memory_status"].eq("continuing_observation").all(),
        "summary output": len(summary) >= 10,
        "all-NaN optional metrics safe": pd.isna(
            current.loc[current.edge_family_id == "family_b", "median_win_rate"].iloc[0]
        ),
    }

    print("=" * WIDTH)
    print("BACQE EH15 - SELF TEST")
    print("=" * WIDTH)
    for name, passed in tests.items():
        print(f"[{'PASS' if bool(passed) else 'FAIL'}] {name}")
    print("-" * WIDTH)
    print(f"Passed: {sum(bool(value) for value in tests.values())}/{len(tests)}")
    print("=" * WIDTH)
    if not all(bool(value) for value in tests.values()):
        raise AssertionError("EH15 self-test failed.")


def main() -> None:
    options = parse_args()
    if options.self_test:
        self_test()
        return

    eh14_dir = options.eh14_dir or options.analysis_root / EH14_OUTPUT_NAME
    output_dir = options.output_dir or options.analysis_root / OUTPUT_NAME
    registry_path, members_path = resolve_inputs(
        options.family_registry,
        options.family_members,
        eh14_dir,
    )
    run_live(
        registry_path,
        members_path,
        output_dir,
        parse_observation_utc(options.observation_utc),
        max(options.top_n, 1),
    )


if __name__ == "__main__":
    main()