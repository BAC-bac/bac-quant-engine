from __future__ import annotations

"""BACQE EH16 - Edge Family Evolution Analytics Research Engine.

Compares the two most recent distinct EH15 edge-family snapshots, classifies
family evolution, records deterministic append-only evolution events, and
behaves honestly when only one historical snapshot exists.
"""

import argparse
import hashlib
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

ANALYSIS_ROOT = Path(r"E:\Quant_Lab\data\analysis\dukascopy_extended_horizons")
INPUT_NAME = "evolution_memory"
OUTPUT_NAME = "evolution_analytics"
ENGINE_VERSION = "1.0.0"
EVENT_SCHEMA_VERSION = "1.0"
WIDTH = 110

REQUIRED_COLUMNS = {
    "snapshot_id", "observation_utc", "edge_family_id", "member_count",
    "priority_member_count", "symbol_count", "context_count", "tier_1_count",
    "tier_2_count", "tier_3_count", "reject_count",
}

METRICS = [
    "member_count", "priority_member_count", "symbol_count", "context_count",
    "tier_1_count", "tier_2_count", "tier_3_count", "reject_count",
    "distinct_candidate_count", "candidate_layer_count", "total_trades",
    "total_net_return", "median_candidate_score", "max_candidate_score",
    "median_confidence_score", "median_win_rate", "median_profit_factor",
    "median_positive_year_rate", "transfer_success_rate", "year_stable_rate",
    "family_independence_score",
]

IDENTITY_COLUMNS = [
    "target", "feature_family", "threshold_side", "context_type", "parent_context"
]

ANALYTICS_COLUMNS = [
    "engine_version", "analysis_status", "comparison_id", "previous_snapshot_id",
    "current_snapshot_id", "previous_observation_utc", "current_observation_utc",
    "edge_family_id", *IDENTITY_COLUMNS, "evolution_class", "evolution_score",
    "observation_count", "first_observation_utc", "last_observation_utc",
    "days_observed",
]
for metric in METRICS:
    ANALYTICS_COLUMNS.extend([f"previous_{metric}", f"current_{metric}", f"{metric}_delta"])
ANALYTICS_COLUMNS.extend([
    "member_growth_rate", "priority_growth_rate", "symbol_growth_rate",
    "context_growth_rate", "has_member_growth", "has_priority_growth",
    "has_symbol_expansion", "has_context_expansion", "has_contraction",
    "evolution_reason",
])

EVENT_COLUMNS = [
    "event_schema_version", "engine_version", "event_id", "comparison_id",
    "previous_snapshot_id", "current_snapshot_id", "event_utc", "edge_family_id",
    "event_type", "metric_name", "previous_value", "current_value", "delta",
    "evolution_class",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyse EH15 edge-family evolution.")
    parser.add_argument("--history", type=Path)
    parser.add_argument("--analysis-root", type=Path, default=ANALYSIS_ROOT)
    parser.add_argument("--input-dir", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--top-n", type=int, default=15)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def atomic_write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, dir=path.parent,
                                     newline="", encoding="utf-8") as handle:
        temp = Path(handle.name)
        frame.to_csv(handle, index=False)
    temp.replace(path)


def atomic_write_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, dir=path.parent,
                                     encoding="utf-8") as handle:
        temp = Path(handle.name)
        handle.write(text)
    temp.replace(path)


def read_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"EH15 history not found: {path}. Run EH15 first.")
    frame = pd.read_csv(path, low_memory=False)
    if frame.empty:
        raise ValueError(f"EH15 history is empty: {path}")
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError("EH15 history is missing required columns: " + ", ".join(missing))
    for column in METRICS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["observation_dt"] = pd.to_datetime(frame["observation_utc"], utc=True, errors="coerce")
    if frame["observation_dt"].isna().any():
        raise ValueError("EH15 history contains invalid observation_utc values.")
    if frame[["snapshot_id", "edge_family_id"]].duplicated().any():
        raise ValueError("EH15 history contains duplicate snapshot/family observations.")
    return frame


def ordered_snapshots(history: pd.DataFrame) -> pd.DataFrame:
    return (history.groupby("snapshot_id", as_index=False)["observation_dt"].max()
            .sort_values(["observation_dt", "snapshot_id"], kind="stable")
            .reset_index(drop=True))


def stable_id(prefix: str, *parts: object, length: int = 20) -> str:
    text = "|".join("" if value is None else str(value) for value in parts)
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:length]
    return f"{prefix}_{digest}"


def safe_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number if math.isfinite(number) else math.nan


def delta(previous: object, current: object) -> float:
    p, c = safe_float(previous), safe_float(current)
    return c - p if math.isfinite(p) and math.isfinite(c) else math.nan


def growth_rate(previous: object, current: object) -> float:
    p, c = safe_float(previous), safe_float(current)
    if not (math.isfinite(p) and math.isfinite(c)):
        return math.nan
    if p == 0:
        return 0.0 if c == 0 else math.nan
    return (c - p) / abs(p)


def family_history_stats(history: pd.DataFrame) -> pd.DataFrame:
    grouped = history.groupby("edge_family_id")["observation_dt"]
    stats = grouped.agg(observation_count="count", first_observation_dt="min",
                        last_observation_dt="max").reset_index()
    stats["first_observation_utc"] = stats["first_observation_dt"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    stats["last_observation_utc"] = stats["last_observation_dt"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    stats["days_observed"] = (stats["last_observation_dt"] - stats["first_observation_dt"]).dt.total_seconds() / 86400.0
    return stats.drop(columns=["first_observation_dt", "last_observation_dt"])


def classify(row: dict[str, object], present_previous: bool, present_current: bool) -> tuple[str, str]:
    if present_previous and not present_current:
        return "RETIRED", "Family was present previously but is absent from the current snapshot."
    if present_current and not present_previous:
        return "NEW", "Family appears for the first time in the current snapshot."
    changes = {metric: safe_float(row.get(f"{metric}_delta")) for metric in
               ["member_count", "priority_member_count", "symbol_count", "context_count"]}
    negative = [name for name, value in changes.items() if math.isfinite(value) and value < 0]
    if negative:
        return "DECLINING", "Contraction detected in: " + ", ".join(negative) + "."
    expanding = [name for name in ("symbol_count", "context_count")
                 if math.isfinite(changes[name]) and changes[name] > 0]
    if expanding:
        return "EXPANDING", "Coverage expanded in: " + ", ".join(expanding) + "."
    growing = [name for name in ("member_count", "priority_member_count")
               if math.isfinite(changes[name]) and changes[name] > 0]
    if growing:
        return "GROWING", "Population growth detected in: " + ", ".join(growing) + "."
    return "STABLE", "No material count or coverage change between snapshots."


def evolution_score(row: dict[str, object], evolution_class: str) -> float:
    if evolution_class == "NEW":
        return 1.0
    if evolution_class == "RETIRED":
        return -1.0
    components = []
    for metric, weight in (("member_count", 0.4), ("priority_member_count", 0.3),
                           ("symbol_count", 0.2), ("context_count", 0.1)):
        rate = safe_float(row.get(f"{metric.replace('_count', '')}_growth_rate"))
        if math.isfinite(rate):
            components.append(weight * max(-1.0, min(1.0, rate)))
    return round(sum(components), 6) if components else 0.0


def baseline_analytics(history: pd.DataFrame, current_snapshot: str) -> pd.DataFrame:
    current = history[history["snapshot_id"] == current_snapshot].copy()
    stats = family_history_stats(history)
    rows = []
    for record in current.to_dict(orient="records"):
        row = {
            "engine_version": ENGINE_VERSION, "analysis_status": "insufficient_history",
            "comparison_id": "", "previous_snapshot_id": "", "current_snapshot_id": current_snapshot,
            "previous_observation_utc": "", "current_observation_utc": record["observation_utc"],
            "edge_family_id": record["edge_family_id"], "evolution_class": "BASELINE",
            "evolution_score": 0.0, "evolution_reason": "Minimum of two distinct snapshots required.",
        }
        for column in IDENTITY_COLUMNS:
            row[column] = record.get(column, "")
        for metric in METRICS:
            row[f"previous_{metric}"] = math.nan
            row[f"current_{metric}"] = record.get(metric, math.nan)
            row[f"{metric}_delta"] = math.nan
        for name in ("member_growth_rate", "priority_growth_rate", "symbol_growth_rate", "context_growth_rate"):
            row[name] = math.nan
        for name in ("has_member_growth", "has_priority_growth", "has_symbol_expansion",
                     "has_context_expansion", "has_contraction"):
            row[name] = False
        rows.append(row)
    result = pd.DataFrame(rows).merge(stats, on="edge_family_id", how="left")
    return result.reindex(columns=ANALYTICS_COLUMNS)


def compare_snapshots(history: pd.DataFrame, previous_id: str, current_id: str) -> pd.DataFrame:
    previous = history[history["snapshot_id"] == previous_id].set_index("edge_family_id", drop=False)
    current = history[history["snapshot_id"] == current_id].set_index("edge_family_id", drop=False)
    stats = family_history_stats(history).set_index("edge_family_id")
    comparison_id = stable_id("cmp", previous_id, current_id)
    previous_utc = previous["observation_utc"].iloc[0]
    current_utc = current["observation_utc"].iloc[0]
    rows = []
    for family_id in sorted(set(previous.index) | set(current.index)):
        has_previous, has_current = family_id in previous.index, family_id in current.index
        p = previous.loc[family_id].to_dict() if has_previous else {}
        c = current.loc[family_id].to_dict() if has_current else {}
        source = c or p
        row = {
            "engine_version": ENGINE_VERSION, "analysis_status": "comparison_available",
            "comparison_id": comparison_id, "previous_snapshot_id": previous_id,
            "current_snapshot_id": current_id, "previous_observation_utc": previous_utc,
            "current_observation_utc": current_utc, "edge_family_id": family_id,
        }
        for column in IDENTITY_COLUMNS:
            row[column] = source.get(column, "")
        for metric in METRICS:
            pv, cv = p.get(metric, math.nan), c.get(metric, math.nan)
            row[f"previous_{metric}"] = pv
            row[f"current_{metric}"] = cv
            row[f"{metric}_delta"] = delta(pv, cv)
        row["member_growth_rate"] = growth_rate(p.get("member_count"), c.get("member_count"))
        row["priority_growth_rate"] = growth_rate(p.get("priority_member_count"), c.get("priority_member_count"))
        row["symbol_growth_rate"] = growth_rate(p.get("symbol_count"), c.get("symbol_count"))
        row["context_growth_rate"] = growth_rate(p.get("context_count"), c.get("context_count"))
        row["has_member_growth"] = safe_float(row["member_count_delta"]) > 0
        row["has_priority_growth"] = safe_float(row["priority_member_count_delta"]) > 0
        row["has_symbol_expansion"] = safe_float(row["symbol_count_delta"]) > 0
        row["has_context_expansion"] = safe_float(row["context_count_delta"]) > 0
        finite_deltas = [safe_float(row[f"{m}_delta"]) for m in
                         ("member_count", "priority_member_count", "symbol_count", "context_count")]
        row["has_contraction"] = any(math.isfinite(v) and v < 0 for v in finite_deltas)
        klass, reason = classify(row, has_previous, has_current)
        row["evolution_class"], row["evolution_reason"] = klass, reason
        row["evolution_score"] = evolution_score(row, klass)
        history_row = stats.loc[family_id]
        row.update(history_row.to_dict())
        rows.append(row)
    return pd.DataFrame(rows).reindex(columns=ANALYTICS_COLUMNS)


def build_events(analytics: pd.DataFrame) -> pd.DataFrame:
    if analytics.empty or analytics["analysis_status"].eq("insufficient_history").all():
        return pd.DataFrame(columns=EVENT_COLUMNS)
    events = []
    event_utc = analytics["current_observation_utc"].iloc[0]
    for row in analytics.to_dict(orient="records"):
        family_id, klass = row["edge_family_id"], row["evolution_class"]
        if klass in {"NEW", "RETIRED"}:
            event_type = "family_appeared" if klass == "NEW" else "family_retired"
            events.append((family_id, event_type, "family_presence", math.nan, math.nan, math.nan, klass))
        for metric in ("member_count", "priority_member_count", "symbol_count", "context_count"):
            d = safe_float(row.get(f"{metric}_delta"))
            if math.isfinite(d) and d != 0:
                event_type = f"{metric}_{'increased' if d > 0 else 'decreased'}"
                events.append((family_id, event_type, metric, row.get(f"previous_{metric}"),
                               row.get(f"current_{metric}"), d, klass))
    records = []
    for family_id, event_type, metric, previous, current, change, klass in events:
        comparison_id = analytics["comparison_id"].iloc[0]
        record = {
            "event_schema_version": EVENT_SCHEMA_VERSION, "engine_version": ENGINE_VERSION,
            "event_id": stable_id("evt", comparison_id, family_id, event_type, metric),
            "comparison_id": comparison_id,
            "previous_snapshot_id": analytics["previous_snapshot_id"].iloc[0],
            "current_snapshot_id": analytics["current_snapshot_id"].iloc[0],
            "event_utc": event_utc, "edge_family_id": family_id, "event_type": event_type,
            "metric_name": metric, "previous_value": previous, "current_value": current,
            "delta": change, "evolution_class": klass,
        }
        records.append(record)
    return pd.DataFrame(records, columns=EVENT_COLUMNS)


def append_events(existing_path: Path, new_events: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if existing_path.exists():
        existing = pd.read_csv(existing_path, low_memory=False)
        missing = set(EVENT_COLUMNS) - set(existing.columns)
        if missing:
            raise ValueError("Existing event log has incompatible schema: " + ", ".join(sorted(missing)))
        existing = existing[EVENT_COLUMNS]
    else:
        existing = pd.DataFrame(columns=EVENT_COLUMNS)
    before = len(existing)
    if existing.empty:
        combined = new_events.copy()
    elif new_events.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, new_events], ignore_index=True)
    if not combined.empty:
        combined = (combined.drop_duplicates("event_id", keep="first")
                    .sort_values(["event_utc", "edge_family_id", "event_type"], kind="stable")
                    .reset_index(drop=True))
    return combined, len(combined) - before


def transition_matrix(analytics: pd.DataFrame) -> pd.DataFrame:
    if analytics.empty or analytics["analysis_status"].eq("insufficient_history").all():
        return pd.DataFrame(columns=["previous_state", "current_state", "family_count"])
    previous_state = np.where(analytics["evolution_class"].eq("NEW"), "ABSENT", "OBSERVED")
    current_state = np.where(analytics["evolution_class"].eq("RETIRED"), "ABSENT", analytics["evolution_class"])
    frame = pd.DataFrame({"previous_state": previous_state, "current_state": current_state})
    return (frame.value_counts().rename("family_count").reset_index()
            .sort_values(["family_count", "current_state"], ascending=[False, True], kind="stable"))


def growth_rankings(analytics: pd.DataFrame) -> pd.DataFrame:
    columns = ["edge_family_id", "evolution_class", "evolution_score", "member_count_delta",
               "priority_member_count_delta", "symbol_count_delta", "context_count_delta",
               "current_member_count", "current_priority_member_count"]
    return analytics.reindex(columns=columns).sort_values(
        ["evolution_score", "member_count_delta", "edge_family_id"],
        ascending=[False, False, True], kind="stable", na_position="last").reset_index(drop=True)


def survival_rankings(analytics: pd.DataFrame) -> pd.DataFrame:
    columns = ["edge_family_id", "evolution_class", "observation_count", "days_observed",
               "first_observation_utc", "last_observation_utc", "current_member_count",
               "current_priority_member_count"]
    return analytics.reindex(columns=columns).sort_values(
        ["observation_count", "days_observed", "edge_family_id"],
        ascending=[False, False, True], kind="stable").reset_index(drop=True)


def summary_frame(history: pd.DataFrame, analytics: pd.DataFrame, appended_events: int) -> pd.DataFrame:
    snapshots = ordered_snapshots(history)
    status = analytics["analysis_status"].iloc[0]
    counts = analytics["evolution_class"].value_counts().to_dict()
    rows = [
        ("analysis_status", status, "Whether consecutive-snapshot evolution can be measured."),
        ("historical_snapshots", len(snapshots), "Distinct EH15 snapshots available."),
        ("families_analysed", len(analytics), "Families in the latest comparison or baseline."),
        ("new_families", counts.get("NEW", 0), "Families newly appearing."),
        ("growing_families", counts.get("GROWING", 0), "Families growing in population."),
        ("expanding_families", counts.get("EXPANDING", 0), "Families expanding symbol or context coverage."),
        ("stable_families", counts.get("STABLE", 0), "Families showing no count or coverage change."),
        ("declining_families", counts.get("DECLINING", 0), "Families showing contraction."),
        ("retired_families", counts.get("RETIRED", 0), "Families absent from the current snapshot."),
        ("baseline_families", counts.get("BASELINE", 0), "Families awaiting a second snapshot."),
        ("new_events_appended", appended_events, "New deterministic events added this run."),
    ]
    return pd.DataFrame(rows, columns=["metric", "value", "interpretation"])


def render_report(summary: pd.DataFrame, analytics: pd.DataFrame, history_path: Path,
                  event_path: Path, top_n: int) -> str:
    lines = ["=" * WIDTH,
             f"BACQE DUKASCOPY EXTENDED HORIZONS EH16 v{ENGINE_VERSION} - EDGE FAMILY EVOLUTION ANALYTICS",
             "=" * WIDTH, summary.to_string(index=False), "-" * WIDTH]
    status = str(summary.loc[summary["metric"] == "analysis_status", "value"].iloc[0])
    if status == "insufficient_history":
        lines.extend(["Evolution analysis is not yet available.",
                      "A minimum of two distinct EH15 snapshots is required.",
                      "The engine has created the output schema without inventing change."])
    else:
        leaders = growth_rankings(analytics).head(max(1, top_n))
        lines.extend(["TOP EVOLUTION RANKINGS", leaders.to_string(index=False)])
    lines.extend(["-" * WIDTH, f"History input: {history_path}", f"Event log:     {event_path}", "=" * WIDTH])
    return "\n".join(lines) + "\n"


def run_engine(history_path: Path, output_dir: Path, top_n: int = 15) -> dict[str, object]:
    history = read_history(history_path)
    snapshots = ordered_snapshots(history)
    current_id = str(snapshots.iloc[-1]["snapshot_id"])
    if len(snapshots) < 2:
        analytics = baseline_analytics(history, current_id)
    else:
        previous_id = str(snapshots.iloc[-2]["snapshot_id"])
        analytics = compare_snapshots(history, previous_id, current_id)
    new_events = build_events(analytics)
    event_path = output_dir / "family_evolution_events.csv"
    event_log, appended_events = append_events(event_path, new_events)
    transitions = transition_matrix(analytics)
    growth = growth_rankings(analytics)
    survival = survival_rankings(analytics)
    summary = summary_frame(history, analytics, appended_events)
    report = render_report(summary, analytics, history_path, event_path, top_n)

    outputs = {
        "analytics": output_dir / "edge_family_evolution_analytics_latest.csv",
        "transitions": output_dir / "family_transition_matrix_latest.csv",
        "growth": output_dir / "family_growth_rankings_latest.csv",
        "survival": output_dir / "family_survival_rankings_latest.csv",
        "events": event_path,
        "summary": output_dir / "edge_family_evolution_summary_latest.csv",
        "report": output_dir / "edge_family_evolution_analytics_report_latest.txt",
        "state": output_dir / "edge_family_evolution_state_latest.json",
    }
    atomic_write_csv(analytics, outputs["analytics"])
    atomic_write_csv(transitions, outputs["transitions"])
    atomic_write_csv(growth, outputs["growth"])
    atomic_write_csv(survival, outputs["survival"])
    atomic_write_csv(event_log, outputs["events"])
    atomic_write_csv(summary, outputs["summary"])
    atomic_write_text(report, outputs["report"])
    state = {
        "engine_version": ENGINE_VERSION, "generated_utc": utc_now_iso(),
        "analysis_status": analytics["analysis_status"].iloc[0],
        "historical_snapshots": int(len(snapshots)), "families_analysed": int(len(analytics)),
        "comparison_id": str(analytics["comparison_id"].iloc[0]),
        "new_events_appended": int(appended_events), "history_input": str(history_path),
    }
    atomic_write_text(json.dumps(state, indent=2) + "\n", outputs["state"])
    return {"history": history, "analytics": analytics, "events": event_log,
            "new_events": new_events, "appended_events": appended_events,
            "summary": summary, "outputs": outputs, "report": report}


def synthetic_history() -> pd.DataFrame:
    common = {"history_schema_version": "1.0", "engine_version": "1.0.0",
              "target": "future_return_1000", "feature_family": "spread",
              "threshold_side": "upper", "context_type": "context_hour",
              "parent_context": "hour_13", "tier_2_count": 0, "tier_3_count": 0,
              "reject_count": 0, "distinct_candidate_count": 1, "candidate_layer_count": 1}
    rows = []
    specs = [
        ("s1", "2026-01-01T00:00:00+00:00", "A", 2, 1, 1, 1),
        ("s1", "2026-01-01T00:00:00+00:00", "B", 3, 2, 1, 1),
        ("s1", "2026-01-01T00:00:00+00:00", "D", 4, 2, 2, 1),
        ("s1", "2026-01-01T00:00:00+00:00", "E", 2, 1, 1, 1),
        ("s2", "2026-02-01T00:00:00+00:00", "A", 4, 2, 1, 1),
        ("s2", "2026-02-01T00:00:00+00:00", "B", 3, 2, 2, 1),
        ("s2", "2026-02-01T00:00:00+00:00", "C", 1, 1, 1, 1),
        ("s2", "2026-02-01T00:00:00+00:00", "E", 1, 1, 1, 1),
    ]
    for snapshot, utc, family, members, priority, symbols, contexts in specs:
        row = dict(common)
        row.update({"snapshot_id": snapshot, "observation_utc": utc,
                    "edge_family_id": family, "member_count": members,
                    "priority_member_count": priority, "symbol_count": symbols,
                    "context_count": contexts, "tier_1_count": priority,
                    "observation_id": stable_id("obs", snapshot, family)})
        rows.append(row)
    return pd.DataFrame(rows)


def run_self_test() -> None:
    tests: list[tuple[str, bool]] = []
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        one = synthetic_history().query("snapshot_id == 's1'").copy()
        one_path = root / "one.csv"
        one.to_csv(one_path, index=False)
        first = run_engine(one_path, root / "baseline")
        a = first["analytics"]
        tests.extend([
            ("baseline status", a["analysis_status"].eq("insufficient_history").all()),
            ("baseline classification", a["evolution_class"].eq("BASELINE").all()),
            ("baseline emits no events", first["new_events"].empty),
            ("baseline family count", len(a) == 4),
        ])

        full_path = root / "full.csv"
        synthetic_history().to_csv(full_path, index=False)
        out = root / "comparison"
        second = run_engine(full_path, out)
        b = second["analytics"].set_index("edge_family_id")
        tests.extend([
            ("comparison status", b["analysis_status"].eq("comparison_available").all()),
            ("growing classification", b.loc["A", "evolution_class"] == "GROWING"),
            ("expanding classification", b.loc["B", "evolution_class"] == "EXPANDING"),
            ("new classification", b.loc["C", "evolution_class"] == "NEW"),
            ("retired classification", b.loc["D", "evolution_class"] == "RETIRED"),
            ("declining classification", b.loc["E", "evolution_class"] == "DECLINING"),
            ("member delta correct", b.loc["A", "member_count_delta"] == 2),
            ("observation count correct", b.loc["A", "observation_count"] == 2),
            ("events created", len(second["new_events"]) > 0),
            ("event ids unique", second["new_events"]["event_id"].is_unique),
        ])
        third = run_engine(full_path, out)
        tests.extend([
            ("event log idempotent", third["appended_events"] == 0),
            ("comparison deterministic", second["analytics"].equals(third["analytics"])),
            ("transition matrix produced", not pd.read_csv(second["outputs"]["transitions"]).empty),
            ("report produced", second["outputs"]["report"].exists()),
        ])
    print("=" * WIDTH)
    print("BACQE EH16 - SELF TEST")
    print("=" * WIDTH)
    for name, passed in tests:
        print(f"[{'PASS' if passed else 'FAIL'}] {name}")
    passed_count = sum(bool(value) for _, value in tests)
    print("-" * WIDTH)
    print(f"Passed: {passed_count}/{len(tests)}")
    print("=" * WIDTH)
    if passed_count != len(tests):
        raise AssertionError("EH16 self-test failed.")


def main() -> None:
    args = parse_args()
    if args.self_test:
        run_self_test()
        return
    input_dir = args.input_dir or args.analysis_root / INPUT_NAME
    output_dir = args.output_dir or args.analysis_root / OUTPUT_NAME
    history_path = args.history or input_dir / "edge_family_history.csv"
    result = run_engine(history_path, output_dir, args.top_n)
    analytics, summary = result["analytics"], result["summary"]
    print("=" * WIDTH)
    print(f"BACQE DUKASCOPY EXTENDED HORIZONS EH16 v{ENGINE_VERSION} - EDGE FAMILY EVOLUTION ANALYTICS")
    print("=" * WIDTH)
    print(f"Historical snapshots: {summary.loc[summary.metric == 'historical_snapshots', 'value'].iloc[0]}")
    print(f"Families analysed:    {len(analytics)}")
    print(f"Analysis status:      {analytics['analysis_status'].iloc[0]}")
    print(f"New events appended:  {result['appended_events']}")
    print("-" * WIDTH)
    print(summary.to_string(index=False))
    print("-" * WIDTH)
    print(f"Analytics: {result['outputs']['analytics']}")
    print(f"Report:    {result['outputs']['report']}")
    print("=" * WIDTH)


if __name__ == "__main__":
    main()