from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np


QUANT_ROOT = Path("E:/Quant_Lab")

SIGNAL_FACTORY_FILE = QUANT_ROOT / "data/analysis/microstructure/signal_factory/microstructure_signal_factory_latest.csv"
CANDIDATE_REGISTRY_FILE = QUANT_ROOT / "data/analysis/microstructure/candidate_registry/microstructure_candidate_registry_latest.csv"
VALIDATION_FILE = QUANT_ROOT / "data/analysis/microstructure/candidate_validation_review/microstructure_candidate_validation_review_latest.csv"

RESEARCH_DATASET_ROOT = QUANT_ROOT / "data/analysis/microstructure/research_datasets"

REPORT_DIR = QUANT_ROOT / "data/analysis/microstructure/signal_context_review"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = REPORT_DIR / "microstructure_signal_context_review_latest.csv"
OUTPUT_JSON = REPORT_DIR / "microstructure_signal_context_review_latest.json"
OUTPUT_TXT = REPORT_DIR / "microstructure_signal_context_review_latest.txt"


MIN_ROWS = 25


def profit_factor(returns: pd.Series) -> float:
    wins = returns[returns > 0].sum()
    losses = returns[returns < 0].sum()

    if losses == 0:
        return np.nan if wins == 0 else np.inf

    return abs(wins / losses)


def sharpe_like(returns: pd.Series) -> float:
    std = returns.std()
    if std == 0 or pd.isna(std):
        return np.nan
    return returns.mean() / std


def split_values(value) -> list[str]:
    if pd.isna(value):
        return []

    return [
        item.strip()
        for item in str(value).split(",")
        if item.strip()
    ]


def load_csv(path: Path, name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {name}: {path}")
    return pd.read_csv(path)


def find_research_dataset(symbol: str, bar_type: str, parameter: str) -> Path | None:
    path = (
        RESEARCH_DATASET_ROOT
        / f"symbol={symbol}"
        / f"bar_type={bar_type}"
        / f"parameter={parameter}"
        / "microstructure_research_dataset.parquet"
    )

    return path if path.exists() else None


def ensure_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    datetime_candidates = [
        "timestamp",
        "time",
        "datetime",
        "bar_time",
        "end_time",
        "close_time",
    ]

    found = None
    for col in datetime_candidates:
        if col in df.columns:
            found = col
            break

    if found is not None:
        df["timestamp"] = pd.to_datetime(df[found], errors="coerce")
    else:
        df["timestamp"] = pd.NaT

    if "timestamp" in df.columns and df["timestamp"].notna().any():
        df["weekday"] = df["timestamp"].dt.day_name()
        df["hour"] = df["timestamp"].dt.hour
    else:
        if "weekday" not in df.columns:
            df["weekday"] = None
        if "hour" not in df.columns:
            df["hour"] = np.nan

    if "session" not in df.columns:
        df["session"] = df["hour"].apply(assign_session)

    return df


def assign_session(hour) -> str:
    if pd.isna(hour):
        return "unknown"

    hour = int(hour)

    if 0 <= hour < 7:
        return "asia_late_overnight"
    if 7 <= hour < 10:
        return "london_open"
    if 10 <= hour < 13:
        return "london_mid_morning"
    if 13 <= hour < 15:
        return "pre_new_york"
    if 15 <= hour < 20:
        return "new_york"
    return "late_us"


def evaluate_subset(df: pd.DataFrame, target: str, feature: str) -> dict:
    if target not in df.columns:
        return {
            "rows": 0,
            "win_rate": np.nan,
            "avg_return": np.nan,
            "median_return": np.nan,
            "total_return": np.nan,
            "profit_factor": np.nan,
            "sharpe_like": np.nan,
            "feature_mean": np.nan,
            "feature_median": np.nan,
            "feature_std": np.nan,
        }

    returns = pd.to_numeric(df[target], errors="coerce").dropna()

    if len(returns) == 0:
        return {
            "rows": 0,
            "win_rate": np.nan,
            "avg_return": np.nan,
            "median_return": np.nan,
            "total_return": np.nan,
            "profit_factor": np.nan,
            "sharpe_like": np.nan,
            "feature_mean": np.nan,
            "feature_median": np.nan,
            "feature_std": np.nan,
        }

    if feature in df.columns:
        feature_series = pd.to_numeric(df[feature], errors="coerce")
    else:
        feature_series = pd.Series(dtype=float)

    return {
        "rows": int(len(returns)),
        "win_rate": float((returns > 0).mean()),
        "avg_return": float(returns.mean()),
        "median_return": float(returns.median()),
        "total_return": float(returns.sum()),
        "profit_factor": float(profit_factor(returns)) if not pd.isna(profit_factor(returns)) else np.nan,
        "sharpe_like": float(sharpe_like(returns)) if not pd.isna(sharpe_like(returns)) else np.nan,
        "feature_mean": float(feature_series.mean()) if len(feature_series.dropna()) else np.nan,
        "feature_median": float(feature_series.median()) if len(feature_series.dropna()) else np.nan,
        "feature_std": float(feature_series.std()) if len(feature_series.dropna()) else np.nan,
    }


def classify_context(row: dict) -> str:
    candidate_rows = row.get("candidate_context_rows", 0)
    outside_rows = row.get("outside_context_rows", 0)

    if candidate_rows < MIN_ROWS:
        return "low_sample"

    edge_delta = row.get("context_avg_return_delta", np.nan)
    pf_delta = row.get("context_profit_factor_delta", np.nan)
    feature_delta = row.get("context_feature_mean_delta", np.nan)

    if pd.isna(edge_delta):
        return "insufficient_target_data"

    if edge_delta > 0 and (pd.isna(pf_delta) or pf_delta >= 0):
        if not pd.isna(feature_delta) and abs(feature_delta) > 0:
            return "context_supports_candidate_edge"
        return "context_return_edge_only"

    if outside_rows >= MIN_ROWS and edge_delta <= 0:
        return "context_does_not_support_candidate"

    return "context_inconclusive"


def main() -> None:
    print("=" * 90)
    print("BACQE MICROSTRUCTURE 48 - SIGNAL CONTEXT REVIEW")
    print("=" * 90)

    created_at = datetime.now(timezone.utc)

    print(f"Created at UTC: {created_at.isoformat()}")
    print(f"Signal factory:      {SIGNAL_FACTORY_FILE}")
    print(f"Candidate registry:  {CANDIDATE_REGISTRY_FILE}")
    print(f"Validation review:   {VALIDATION_FILE}")
    print(f"Research root:       {RESEARCH_DATASET_ROOT}")
    print("-" * 90)

    signals = load_csv(SIGNAL_FACTORY_FILE, "signal factory")
    registry = load_csv(CANDIDATE_REGISTRY_FILE, "candidate registry")
    validation = load_csv(VALIDATION_FILE, "validation review")

    print(f"Signal rows:     {len(signals)}")
    print(f"Registry rows:   {len(registry)}")
    print(f"Validation rows: {len(validation)}")
    print("-" * 90)

    strong_signals = signals[
        signals["signal_label"].isin(["strong_signal_candidate", "research_signal_candidate"])
    ].copy()

    clean_candidates = validation[
        validation["validation_label"].isin([
            "validation_pass_primary",
            "investigate_insufficient_date_coverage",
        ])
    ].copy()

    rows = []

    for c_idx, candidate in clean_candidates.iterrows():
        candidate_symbol = str(candidate.get("symbols", "")).strip()
        candidate_sessions = split_values(candidate.get("sessions"))
        candidate_weekdays = split_values(candidate.get("weekdays"))
        candidate_name = candidate.get("filter_name")
        candidate_label = candidate.get("validation_label")
        candidate_rank = candidate.get("validation_rank")
        candidate_cost = candidate.get("cost_per_trade")

        candidate_signals = strong_signals[
            strong_signals["symbol"].astype(str).str.strip().eq(candidate_symbol)
        ].copy()

        if candidate_signals.empty:
            continue

        for _, signal in candidate_signals.iterrows():
            symbol = str(signal["symbol"]).strip()
            bar_type = str(signal["bar_type"]).strip()
            parameter = str(signal["parameter"]).strip()
            feature = str(signal["spread_feature"]).strip()
            target = str(signal["target"]).strip()

            dataset_path = find_research_dataset(symbol, bar_type, parameter)

            base = {
                "created_at_utc": created_at.isoformat(),
                "candidate_name": candidate_name,
                "candidate_validation_label": candidate_label,
                "candidate_validation_rank": candidate_rank,
                "candidate_cost_per_trade": candidate_cost,
                "symbol": symbol,
                "candidate_sessions": ",".join(candidate_sessions),
                "candidate_weekdays": ",".join(candidate_weekdays),
                "signal_rank": signal.get("signal_rank"),
                "bar_type": bar_type,
                "parameter": parameter,
                "feature": feature,
                "target": target,
                "signal_label": signal.get("signal_label"),
                "signal_direction": signal.get("signal_direction"),
                "signal_hit_rate": signal.get("hit_rate"),
                "signal_mean_forward_return": signal.get("mean_forward_return"),
                "signal_stability_score": signal.get("stability_score"),
                "dataset_path": str(dataset_path) if dataset_path else None,
            }

            if dataset_path is None:
                base["context_label"] = "missing_research_dataset"
                rows.append(base)
                continue

            df = pd.read_parquet(dataset_path)
            df = ensure_datetime_columns(df)

            if feature not in df.columns:
                base["context_label"] = "missing_feature_in_dataset"
                rows.append(base)
                continue

            if target not in df.columns:
                base["context_label"] = "missing_target_in_dataset"
                rows.append(base)
                continue

            candidate_mask = pd.Series(True, index=df.index)

            if candidate_sessions:
                candidate_mask &= df["session"].isin(candidate_sessions)

            if candidate_weekdays:
                candidate_mask &= df["weekday"].isin(candidate_weekdays)

            candidate_df = df[candidate_mask].copy()
            outside_df = df[~candidate_mask].copy()

            candidate_stats = evaluate_subset(candidate_df, target, feature)
            outside_stats = evaluate_subset(outside_df, target, feature)

            row = {
                **base,
                "candidate_context_rows": candidate_stats["rows"],
                "candidate_context_win_rate": candidate_stats["win_rate"],
                "candidate_context_avg_return": candidate_stats["avg_return"],
                "candidate_context_median_return": candidate_stats["median_return"],
                "candidate_context_total_return": candidate_stats["total_return"],
                "candidate_context_profit_factor": candidate_stats["profit_factor"],
                "candidate_context_sharpe_like": candidate_stats["sharpe_like"],
                "candidate_context_feature_mean": candidate_stats["feature_mean"],
                "candidate_context_feature_median": candidate_stats["feature_median"],
                "candidate_context_feature_std": candidate_stats["feature_std"],

                "outside_context_rows": outside_stats["rows"],
                "outside_context_win_rate": outside_stats["win_rate"],
                "outside_context_avg_return": outside_stats["avg_return"],
                "outside_context_median_return": outside_stats["median_return"],
                "outside_context_total_return": outside_stats["total_return"],
                "outside_context_profit_factor": outside_stats["profit_factor"],
                "outside_context_sharpe_like": outside_stats["sharpe_like"],
                "outside_context_feature_mean": outside_stats["feature_mean"],
                "outside_context_feature_median": outside_stats["feature_median"],
                "outside_context_feature_std": outside_stats["feature_std"],
            }

            row["context_win_rate_delta"] = (
                row["candidate_context_win_rate"] - row["outside_context_win_rate"]
            )
            row["context_avg_return_delta"] = (
                row["candidate_context_avg_return"] - row["outside_context_avg_return"]
            )
            row["context_profit_factor_delta"] = (
                row["candidate_context_profit_factor"] - row["outside_context_profit_factor"]
            )
            row["context_feature_mean_delta"] = (
                row["candidate_context_feature_mean"] - row["outside_context_feature_mean"]
            )

            row["context_label"] = classify_context(row)

            rows.append(row)

    result = pd.DataFrame(rows)

    if result.empty:
        print("[WARNING] No context review rows produced.")
        result.to_csv(OUTPUT_CSV, index=False)
        return

    sort_columns = [
        "context_label",
        "context_avg_return_delta",
        "candidate_context_rows",
        "signal_stability_score",
    ]

    for col in sort_columns:
        if col not in result.columns:
            result[col] = np.nan

    result = result.sort_values(by=sort_columns, ascending=[True, False, False, False], )

    result.to_csv(OUTPUT_CSV, index=False)
    result.to_json(OUTPUT_JSON, orient="records", indent=2)

    label_counts = result["context_label"].value_counts(dropna=False).to_dict()

    txt_lines = [
        "=" * 90,
        "BACQE MICROSTRUCTURE 48 - SIGNAL CONTEXT REVIEW",
        "=" * 90,
        f"Created at UTC: {created_at.isoformat()}",
        "",
        f"Signal rows:     {len(signals)}",
        f"Registry rows:   {len(registry)}",
        f"Validation rows: {len(validation)}",
        f"Review rows:     {len(result)}",
        "",
        f"Context labels: {label_counts}",
        "-" * 90,
        "TOP CONTEXT REVIEW RESULTS",
        "-" * 90,
        result.head(40).to_string(index=False),
        "",
        "=" * 90,
    ]

    OUTPUT_TXT.write_text("\n".join(txt_lines), encoding="utf-8")

    print(f"Review rows: {len(result)}")
    print(f"Context labels: {label_counts}")
    print("-" * 90)
    display_columns = ["candidate_name", "symbol", "bar_type", "parameter", "feature", "target",
        "candidate_context_rows", "outside_context_rows", "context_avg_return_delta", "context_profit_factor_delta",
        "context_feature_mean_delta", "context_label", ]

    for col in display_columns:
        if col not in result.columns:
            result[col] = np.nan

    display_columns = ["candidate_name", "symbol", "bar_type", "parameter", "feature", "target",
        "candidate_context_rows", "outside_context_rows", "context_avg_return_delta", "context_profit_factor_delta",
        "context_feature_mean_delta", "context_label", ]

    for col in display_columns:
        if col not in result.columns:
            result[col] = np.nan

    print(result.head(30)[display_columns])
    print("-" * 90)
    print(f"[SAVED] {OUTPUT_CSV}")
    print(f"[SAVED] {OUTPUT_JSON}")
    print(f"[SAVED] {OUTPUT_TXT}")
    print("=" * 90)


if __name__ == "__main__":
    main()