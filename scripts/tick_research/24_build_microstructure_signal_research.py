"""
BACQE TICK RESEARCH - 24 Build Microstructure Signal Research

Builds simple research signals from the strongest Phase 2 baseline setup:

    bar_type = imbalance_50
    target   = target_direction_persist_h1

This is NOT a trading system.
It is a diagnostic signal research layer.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import balanced_accuracy_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")
SYMBOL = "GBPUSD"

INPUT_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "feature_store"
    / f"{SYMBOL}_microstructure_feature_store_latest.parquet"
)

OUTPUT_ANALYSIS_DIR = DATA_LAKE_ROOT / "data" / "analysis" / "tick_research"
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "microstructure_signal_research"

BAR_TYPE = "imbalance_50"
TARGET = "target_direction_persist_h1"
TEST_SIZE_PCT = 0.30

PROBABILITY_THRESHOLDS = [0.50, 0.55, 0.60, 0.65, 0.70]

EXCLUDE_COLUMNS = {
    "symbol",
    "broker",
    "bar_start_time",
    "bar_end_time",
    "date_utc",
    "feature_store_build_time_utc",
    "build_time_utc",
    "regime_build_time_utc",
}


def get_feature_columns(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    feature_cols = []

    for col in df.columns:
        if col in EXCLUDE_COLUMNS:
            continue

        if col.startswith("future_") or col.startswith("target_"):
            continue

        if df[col].isna().all():
            continue

        feature_cols.append(col)

    categorical_cols = [
        col for col in feature_cols
        if df[col].dtype == "object" or str(df[col].dtype).startswith("category")
    ]

    numeric_cols = [
        col for col in feature_cols
        if col not in categorical_cols
    ]

    return numeric_cols, categorical_cols


def chronological_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    data = df.sort_values("bar_start_time").reset_index(drop=True)
    split_idx = int(len(data) * (1 - TEST_SIZE_PCT))

    return data.iloc[:split_idx].copy(), data.iloc[split_idx:].copy()


def build_model(numeric_cols: list[str], categorical_cols: list[str]) -> Pipeline:
    numeric_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )

    categorical_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", numeric_pipe, numeric_cols),
            ("categorical", categorical_pipe, categorical_cols),
        ],
        remainder="drop",
    )

    model = RandomForestClassifier(
        n_estimators=500,
        max_depth=5,
        min_samples_leaf=10,
        random_state=42,
        class_weight="balanced_subsample",
        n_jobs=-1,
    )

    return Pipeline(
        steps=[
            ("preprocess", preprocessor),
            ("model", model),
        ]
    )


def build_signal_dataset(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    subset = df[df["bar_type"] == BAR_TYPE].copy()
    subset = subset.dropna(subset=[TARGET, "bar_start_time"])

    subset["bar_start_time"] = pd.to_datetime(subset["bar_start_time"], errors="coerce", utc=True)
    subset = subset.dropna(subset=["bar_start_time"]).sort_values("bar_start_time").reset_index(drop=True)

    numeric_cols, categorical_cols = get_feature_columns(subset)

    train, test = chronological_split(subset)

    X_train = train[numeric_cols + categorical_cols]
    y_train = pd.to_numeric(train[TARGET], errors="coerce").fillna(0).astype(int)

    X_test = test[numeric_cols + categorical_cols]
    y_test = pd.to_numeric(test[TARGET], errors="coerce").fillna(0).astype(int)

    model = build_model(numeric_cols, categorical_cols)
    model.fit(X_train, y_train)

    proba = model.predict_proba(X_test)[:, 1]
    preds = model.predict(X_test)

    signal_df = test.copy()
    signal_df["model_probability_persist_h1"] = proba
    signal_df["model_prediction_persist_h1"] = preds
    signal_df["actual_persist_h1"] = y_test.values

    signal_df["model_confidence_bucket"] = pd.cut(
        signal_df["model_probability_persist_h1"],
        bins=[0.0, 0.5, 0.55, 0.60, 0.65, 0.70, 1.0],
        labels=["<0.50", "0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70+"],
        include_lowest=True,
    )

    metrics = {
        "bar_type": BAR_TYPE,
        "target": TARGET,
        "rows_total": len(subset),
        "train_rows": len(train),
        "test_rows": len(test),
        "feature_count": len(numeric_cols) + len(categorical_cols),
        "balanced_accuracy": balanced_accuracy_score(y_test, preds),
        "roc_auc": roc_auc_score(y_test, proba) if y_test.nunique() == 2 else np.nan,
        "test_positive_rate": float(y_test.mean()),
        "predicted_positive_rate": float(np.mean(preds)),
        "run_time_utc": datetime.now(timezone.utc).isoformat(),
    }

    return signal_df, metrics


def summarise_thresholds(signal_df: pd.DataFrame) -> pd.DataFrame:
    records = []

    for threshold in PROBABILITY_THRESHOLDS:
        fired = signal_df[signal_df["model_probability_persist_h1"] >= threshold].copy()

        if fired.empty:
            records.append(
                {
                    "threshold": threshold,
                    "signals": 0,
                    "signal_rate_pct": 0,
                    "actual_persist_rate_pct": np.nan,
                    "avg_future_return_h1": np.nan,
                    "avg_future_abs_return_h1": np.nan,
                    "avg_model_probability": np.nan,
                    "avg_duration_seconds": np.nan,
                    "avg_tick_count": np.nan,
                }
            )
            continue

        records.append(
            {
                "threshold": threshold,
                "signals": len(fired),
                "signal_rate_pct": len(fired) / len(signal_df) * 100,
                "actual_persist_rate_pct": fired["actual_persist_h1"].mean() * 100,
                "avg_future_return_h1": fired["future_return_h1"].mean(),
                "avg_future_abs_return_h1": fired["future_abs_return_h1"].mean(),
                "avg_model_probability": fired["model_probability_persist_h1"].mean(),
                "avg_duration_seconds": fired["duration_seconds"].mean(),
                "avg_tick_count": fired["tick_count"].mean(),
            }
        )

    summary = pd.DataFrame(records)

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary


def summarise_by_bucket(signal_df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        signal_df.groupby("model_confidence_bucket", observed=False)
        .agg(
            rows=("model_probability_persist_h1", "count"),
            avg_probability=("model_probability_persist_h1", "mean"),
            actual_persist_rate_pct=("actual_persist_h1", "mean"),
            avg_future_return_h1=("future_return_h1", "mean"),
            avg_future_abs_return_h1=("future_abs_return_h1", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            avg_tick_count=("tick_count", "mean"),
        )
        .reset_index()
    )

    summary["actual_persist_rate_pct"] *= 100

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary


def summarise_by_regime_session(signal_df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        signal_df.groupby(["microstructure_regime", "session_utc"], dropna=False)
        .agg(
            rows=("model_probability_persist_h1", "count"),
            avg_probability=("model_probability_persist_h1", "mean"),
            actual_persist_rate_pct=("actual_persist_h1", "mean"),
            avg_future_return_h1=("future_return_h1", "mean"),
            avg_future_abs_return_h1=("future_abs_return_h1", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            avg_tick_count=("tick_count", "mean"),
        )
        .reset_index()
    )

    summary["actual_persist_rate_pct"] *= 100

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary.sort_values(["actual_persist_rate_pct", "rows"], ascending=[False, False])


def build_report(
    metrics: dict,
    threshold_summary: pd.DataFrame,
    bucket_summary: pd.DataFrame,
    regime_session_summary: pd.DataFrame,
) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - MICROSTRUCTURE SIGNAL RESEARCH REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append(f"Bar type:        {BAR_TYPE}")
    lines.append(f"Target:          {TARGET}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("MODEL METRICS")
    lines.append("-" * 90)
    lines.append(json.dumps(metrics, indent=4, default=str))

    lines.append("")
    lines.append("PROBABILITY THRESHOLD SUMMARY")
    lines.append("-" * 90)
    lines.append(threshold_summary.to_string(index=False))

    lines.append("")
    lines.append("CONFIDENCE BUCKET SUMMARY")
    lines.append("-" * 90)
    lines.append(bucket_summary.to_string(index=False))

    lines.append("")
    lines.append("REGIME + SESSION SUMMARY")
    lines.append("-" * 90)
    lines.append(regime_session_summary.head(30).to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This is diagnostic research, not a trading system.")
    lines.append("Signals are model-confidence events, not execution recommendations.")
    lines.append("The current dataset is small, so results should be treated as hypotheses.")
    lines.append("The goal is to identify where microstructure persistence may concentrate.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 24 BUILD MICROSTRUCTURE SIGNAL RESEARCH")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Feature store not found: {INPUT_PATH}")

    df = pd.read_parquet(INPUT_PATH)

    print(f"Rows loaded:    {len(df):,}")
    print(f"Columns loaded: {len(df.columns):,}")

    signal_df, metrics = build_signal_dataset(df)

    threshold_summary = summarise_thresholds(signal_df)
    bucket_summary = summarise_by_bucket(signal_df)
    regime_session_summary = summarise_by_regime_session(signal_df)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    signal_path = OUTPUT_ANALYSIS_DIR / "microstructure_signal_research_events_latest.parquet"
    signal_csv = OUTPUT_ANALYSIS_DIR / "microstructure_signal_research_events_latest.csv"

    threshold_csv = OUTPUT_ANALYSIS_DIR / "microstructure_signal_threshold_summary_latest.csv"
    bucket_csv = OUTPUT_ANALYSIS_DIR / "microstructure_signal_bucket_summary_latest.csv"
    regime_session_csv = OUTPUT_ANALYSIS_DIR / "microstructure_signal_regime_session_summary_latest.csv"

    metrics_json = OUTPUT_ANALYSIS_DIR / "microstructure_signal_model_metrics_latest.json"
    report_path = OUTPUT_REPORT_DIR / "microstructure_signal_research_report_latest.txt"

    signal_df.to_parquet(signal_path, index=False)
    signal_df.to_csv(signal_csv, index=False)

    threshold_summary.to_csv(threshold_csv, index=False)
    bucket_summary.to_csv(bucket_csv, index=False)
    regime_session_summary.to_csv(regime_session_csv, index=False)

    with open(metrics_json, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=4, default=str)

    report = build_report(metrics, threshold_summary, bucket_summary, regime_session_summary)
    report_path.write_text(report, encoding="utf-8")

    print("[DONE] Microstructure signal research created.")
    print(f"Signals Parquet:       {signal_path}")
    print(f"Signals CSV:           {signal_csv}")
    print(f"Threshold Summary CSV: {threshold_csv}")
    print(f"Bucket Summary CSV:    {bucket_csv}")
    print(f"Regime Session CSV:    {regime_session_csv}")
    print(f"Metrics JSON:          {metrics_json}")
    print(f"Report:                {report_path}")
    print("-" * 90)

    print("MODEL METRICS")
    print(json.dumps(metrics, indent=4, default=str))
    print("-" * 90)

    print("THRESHOLD SUMMARY")
    print(threshold_summary.to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()