"""
BACQE TICK RESEARCH - 24 Build Microstructure Signal Research - Multi Symbol

Builds diagnostic model-confidence signal research from the strongest
baseline model setups identified by Script 22.

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

INPUT_FEATURE_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "processed"
    / "tick_research"
    / "feature_store"
)

BASELINE_WINNERS_PATH = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "baseline_models"
    / "_master"
    / "symbol_target_winners_baseline_model_latest.csv"
)

OUTPUT_ANALYSIS_ROOT = (
    DATA_LAKE_ROOT
    / "data"
    / "analysis"
    / "tick_research"
    / "microstructure_signal_research"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "microstructure_signal_research"
)

TEST_SIZE_PCT = 0.30
MIN_ROWS = 200

PROBABILITY_THRESHOLDS = [0.50, 0.55, 0.60, 0.65, 0.70]

EXCLUDE_COLUMNS = {
    "symbol",
    "broker",
    "bar_start_time",
    "bar_end_time",
    "first_bar_time",
    "last_bar_time",
    "date_utc",
    "feature_store_build_time_utc",
    "build_time_utc",
    "regime_build_time_utc",
    "summary_time_utc",
    "analysis_time_utc",
}

KNOWN_CATEGORICAL_COLS = {
    "bar_type",
    "bar_family",
    "bar_parameter",
    "microstructure_regime",
    "session_utc",
}


def get_feature_columns(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    exclude_prefixes = ["future_", "target_"]

    numeric_cols = []
    categorical_cols = []

    for col in df.columns:
        if col in EXCLUDE_COLUMNS:
            continue

        if any(col.startswith(prefix) for prefix in exclude_prefixes):
            continue

        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue

        if pd.api.types.is_timedelta64_dtype(df[col]):
            continue

        sample = df[col].dropna().head(100)

        if len(sample) > 0:
            contains_temporal_objects = sample.apply(
                lambda x: isinstance(x, (pd.Timestamp, pd.Timedelta))
            ).any()

            if contains_temporal_objects:
                continue

        if pd.api.types.is_numeric_dtype(df[col]):
            if df[col].notna().sum() == 0:
                continue

            numeric_cols.append(col)
            continue

        if col in KNOWN_CATEGORICAL_COLS:
            categorical_cols.append(col)

    return numeric_cols, categorical_cols


def chronological_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    data = df.sort_values("bar_start_time").reset_index(drop=True)
    split_idx = int(len(data) * (1 - TEST_SIZE_PCT))

    train = data.iloc[:split_idx].copy()
    test = data.iloc[split_idx:].copy()

    return train, test


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


def load_baseline_winners() -> pd.DataFrame:
    if not BASELINE_WINNERS_PATH.exists():
        raise FileNotFoundError(
            f"Baseline winners file not found: {BASELINE_WINNERS_PATH}"
        )

    winners = pd.read_csv(BASELINE_WINNERS_PATH, low_memory=False)

    required_cols = {"symbol", "target", "bar_type"}

    missing = required_cols - set(winners.columns)
    if missing:
        raise ValueError(
            f"Baseline winners file is missing required columns: {missing}"
        )

    winners = winners.copy()
    winners["experiment_name"] = (
        winners["symbol"].astype(str)
        + "_"
        + winners["target"].astype(str)
        + "_"
        + winners["bar_type"].astype(str)
    )

    return winners


def build_signal_dataset(
    feature_store: pd.DataFrame,
    experiment: pd.Series,
) -> tuple[pd.DataFrame, dict]:
    symbol = str(experiment["symbol"])
    target = str(experiment["target"])
    bar_type = str(experiment["bar_type"])
    experiment_name = str(experiment["experiment_name"])

    subset = feature_store[feature_store["bar_type"] == bar_type].copy()
    subset = subset.dropna(subset=[target, "bar_start_time"])

    subset["bar_start_time"] = pd.to_datetime(
        subset["bar_start_time"],
        errors="coerce",
        utc=True,
    )

    subset = (
        subset.dropna(subset=["bar_start_time"])
        .sort_values("bar_start_time")
        .reset_index(drop=True)
    )

    if len(subset) < MIN_ROWS:
        return pd.DataFrame(), {
            "symbol": symbol,
            "experiment_name": experiment_name,
            "bar_type": bar_type,
            "target": target,
            "status": "skipped_low_rows",
            "rows_total": len(subset),
        }

    y = pd.to_numeric(subset[target], errors="coerce").fillna(0).astype(int)

    if y.nunique() < 2:
        return pd.DataFrame(), {
            "symbol": symbol,
            "experiment_name": experiment_name,
            "bar_type": bar_type,
            "target": target,
            "status": "skipped_single_class",
            "rows_total": len(subset),
        }

    numeric_cols, categorical_cols = get_feature_columns(subset)

    if not numeric_cols and not categorical_cols:
        return pd.DataFrame(), {
            "symbol": symbol,
            "experiment_name": experiment_name,
            "bar_type": bar_type,
            "target": target,
            "status": "skipped_no_features",
            "rows_total": len(subset),
        }

    train, test = chronological_split(subset)

    X_train = train[numeric_cols + categorical_cols]
    y_train = pd.to_numeric(train[target], errors="coerce").fillna(0).astype(int)

    X_test = test[numeric_cols + categorical_cols]
    y_test = pd.to_numeric(test[target], errors="coerce").fillna(0).astype(int)

    if y_train.nunique() < 2 or y_test.nunique() < 2:
        return pd.DataFrame(), {
            "symbol": symbol,
            "experiment_name": experiment_name,
            "bar_type": bar_type,
            "target": target,
            "status": "skipped_single_class_split",
            "rows_total": len(subset),
            "train_rows": len(train),
            "test_rows": len(test),
        }

    model = build_model(numeric_cols, categorical_cols)
    model.fit(X_train, y_train)

    proba = model.predict_proba(X_test)[:, 1]
    preds = model.predict(X_test)

    signal_df = test.copy()
    signal_df["symbol"] = symbol
    signal_df["experiment_name"] = experiment_name
    signal_df["signal_bar_type"] = bar_type
    signal_df["signal_target"] = target
    signal_df["baseline_winning_model"] = experiment.get("model", np.nan)
    signal_df["baseline_balanced_accuracy"] = experiment.get("balanced_accuracy", np.nan)
    signal_df["baseline_roc_auc"] = experiment.get("roc_auc", np.nan)

    signal_df["model_probability"] = proba
    signal_df["model_prediction"] = preds
    signal_df["actual_target"] = y_test.values

    signal_df["model_confidence_bucket"] = pd.cut(
        signal_df["model_probability"],
        bins=[0.0, 0.5, 0.55, 0.60, 0.65, 0.70, 1.0],
        labels=["<0.50", "0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70+"],
        include_lowest=True,
    )

    metrics = {
        "symbol": symbol,
        "experiment_name": experiment_name,
        "bar_type": bar_type,
        "target": target,
        "status": "success",
        "rows_total": len(subset),
        "train_rows": len(train),
        "test_rows": len(test),
        "numeric_feature_count": len(numeric_cols),
        "categorical_feature_count": len(categorical_cols),
        "feature_count": len(numeric_cols) + len(categorical_cols),
        "balanced_accuracy": balanced_accuracy_score(y_test, preds),
        "roc_auc": roc_auc_score(y_test, proba) if y_test.nunique() == 2 else np.nan,
        "test_positive_rate": float(y_test.mean()),
        "predicted_positive_rate": float(np.mean(preds)),
        "baseline_winning_model": experiment.get("model", np.nan),
        "baseline_balanced_accuracy": experiment.get("balanced_accuracy", np.nan),
        "baseline_roc_auc": experiment.get("roc_auc", np.nan),
        "run_time_utc": datetime.now(timezone.utc).isoformat(),
    }

    return signal_df, metrics


def summarise_thresholds(signal_df: pd.DataFrame) -> pd.DataFrame:
    records = []

    if signal_df.empty:
        return pd.DataFrame()

    group_cols = ["symbol", "experiment_name", "signal_bar_type", "signal_target"]

    for keys, group in signal_df.groupby(group_cols, dropna=False):
        symbol, experiment_name, bar_type, target = keys

        for threshold in PROBABILITY_THRESHOLDS:
            fired = group[group["model_probability"] >= threshold].copy()

            if fired.empty:
                records.append(
                    {
                        "symbol": symbol,
                        "experiment_name": experiment_name,
                        "bar_type": bar_type,
                        "target": target,
                        "threshold": threshold,
                        "signals": 0,
                        "signal_rate_pct": 0,
                        "actual_target_rate_pct": np.nan,
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
                    "symbol": symbol,
                    "experiment_name": experiment_name,
                    "bar_type": bar_type,
                    "target": target,
                    "threshold": threshold,
                    "signals": len(fired),
                    "signal_rate_pct": len(fired) / len(group) * 100,
                    "actual_target_rate_pct": fired["actual_target"].mean() * 100,
                    "avg_future_return_h1": fired["future_return_h1"].mean()
                    if "future_return_h1" in fired.columns
                    else np.nan,
                    "avg_future_abs_return_h1": fired["future_abs_return_h1"].mean()
                    if "future_abs_return_h1" in fired.columns
                    else np.nan,
                    "avg_model_probability": fired["model_probability"].mean(),
                    "avg_duration_seconds": fired["duration_seconds"].mean()
                    if "duration_seconds" in fired.columns
                    else np.nan,
                    "avg_tick_count": fired["tick_count"].mean()
                    if "tick_count" in fired.columns
                    else np.nan,
                }
            )

    summary = pd.DataFrame(records)

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary


def summarise_by_bucket(signal_df: pd.DataFrame) -> pd.DataFrame:
    if signal_df.empty:
        return pd.DataFrame()

    summary = (
        signal_df.groupby(
            [
                "symbol",
                "experiment_name",
                "signal_bar_type",
                "signal_target",
                "model_confidence_bucket",
            ],
            observed=False,
            dropna=False,
        )
        .agg(
            rows=("model_probability", "count"),
            avg_probability=("model_probability", "mean"),
            actual_target_rate_pct=("actual_target", "mean"),
            avg_future_return_h1=("future_return_h1", "mean"),
            avg_future_abs_return_h1=("future_abs_return_h1", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            avg_tick_count=("tick_count", "mean"),
        )
        .reset_index()
    )

    summary["actual_target_rate_pct"] *= 100

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary


def summarise_by_regime_session(signal_df: pd.DataFrame) -> pd.DataFrame:
    if signal_df.empty:
        return pd.DataFrame()

    summary = (
        signal_df.groupby(
            [
                "symbol",
                "experiment_name",
                "signal_bar_type",
                "signal_target",
                "microstructure_regime",
                "session_utc",
            ],
            dropna=False,
        )
        .agg(
            rows=("model_probability", "count"),
            avg_probability=("model_probability", "mean"),
            actual_target_rate_pct=("actual_target", "mean"),
            avg_future_return_h1=("future_return_h1", "mean"),
            avg_future_abs_return_h1=("future_abs_return_h1", "mean"),
            avg_duration_seconds=("duration_seconds", "mean"),
            avg_tick_count=("tick_count", "mean"),
        )
        .reset_index()
    )

    summary["actual_target_rate_pct"] *= 100

    numeric_cols = summary.select_dtypes(include=["float", "int"]).columns
    summary[numeric_cols] = summary[numeric_cols].round(8)

    summary["summary_time_utc"] = datetime.now(timezone.utc).isoformat()

    return summary.sort_values(
        ["actual_target_rate_pct", "rows"],
        ascending=[False, False],
    )


def build_symbol_report(
    symbol: str,
    metrics_df: pd.DataFrame,
    threshold_summary: pd.DataFrame,
    bucket_summary: pd.DataFrame,
    regime_session_summary: pd.DataFrame,
) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    lines = []
    lines.append("=" * 90)
    lines.append(f"BACQE TICK RESEARCH - MICROSTRUCTURE SIGNAL RESEARCH REPORT - {symbol}")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("MODEL METRICS")
    lines.append("-" * 90)
    lines.append(metrics_df.to_string(index=False))

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
    lines.append("The goal is to identify where microstructure probability signals may concentrate.")
    lines.append("Use threshold summaries to study selectivity versus hit rate.")
    lines.append("=" * 90)

    return "\n".join(lines)


def process_symbol(
    symbol: str,
    symbol_experiments: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    input_path = (
        INPUT_FEATURE_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_feature_store_latest.parquet"
    )

    if not input_path.exists():
        print(f"[WARN] {symbol}: feature store not found: {input_path}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    feature_store = pd.read_parquet(input_path)

    print(f"[INFO] {symbol}: rows loaded:    {len(feature_store):,}")
    print(f"[INFO] {symbol}: columns loaded: {len(feature_store.columns):,}")

    signal_frames = []
    metrics_records = []

    for _, experiment in symbol_experiments.iterrows():
        print(
            f"[RUN] {symbol} | "
            f"target={experiment['target']} | "
            f"bar_type={experiment['bar_type']}"
        )

        signal_df, metrics = build_signal_dataset(feature_store, experiment)

        if not signal_df.empty:
            signal_frames.append(signal_df)

        metrics_records.append(metrics)

    symbol_signals = (
        pd.concat(signal_frames, ignore_index=True)
        if signal_frames
        else pd.DataFrame()
    )

    metrics_df = pd.DataFrame(metrics_records)
    threshold_summary = summarise_thresholds(symbol_signals)
    bucket_summary = summarise_by_bucket(symbol_signals)
    regime_session_summary = summarise_by_regime_session(symbol_signals)

    for df in [metrics_df, threshold_summary, bucket_summary, regime_session_summary]:
        numeric_cols = df.select_dtypes(include=["float", "int"]).columns
        if len(numeric_cols) > 0:
            df[numeric_cols] = df[numeric_cols].round(8)

    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    signals_parquet = analysis_dir / f"{symbol}_microstructure_signal_events_latest.parquet"
    signals_csv = analysis_dir / f"{symbol}_microstructure_signal_events_latest.csv"

    threshold_csv = analysis_dir / f"{symbol}_microstructure_signal_threshold_summary_latest.csv"
    bucket_csv = analysis_dir / f"{symbol}_microstructure_signal_bucket_summary_latest.csv"
    regime_session_csv = analysis_dir / f"{symbol}_microstructure_signal_regime_session_summary_latest.csv"

    metrics_csv = analysis_dir / f"{symbol}_microstructure_signal_model_metrics_latest.csv"
    metrics_json = analysis_dir / f"{symbol}_microstructure_signal_model_metrics_latest.json"

    report_path = report_dir / f"{symbol}_microstructure_signal_research_report_latest.txt"

    symbol_signals.to_parquet(signals_parquet, index=False)
    symbol_signals.to_csv(signals_csv, index=False)

    threshold_summary.to_csv(threshold_csv, index=False)
    bucket_summary.to_csv(bucket_csv, index=False)
    regime_session_summary.to_csv(regime_session_csv, index=False)

    metrics_df.to_csv(metrics_csv, index=False)

    with open(metrics_json, "w", encoding="utf-8") as f:
        json.dump(metrics_df.to_dict(orient="records"), f, indent=4, default=str)

    report = build_symbol_report(
        symbol=symbol,
        metrics_df=metrics_df,
        threshold_summary=threshold_summary,
        bucket_summary=bucket_summary,
        regime_session_summary=regime_session_summary,
    )

    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: signals CSV:   {signals_csv}")
    print(f"[DONE] {symbol}: threshold CSV: {threshold_csv}")
    print(f"[DONE] {symbol}: bucket CSV:    {bucket_csv}")
    print(f"[DONE] {symbol}: regime CSV:    {regime_session_csv}")
    print(f"[DONE] {symbol}: metrics CSV:   {metrics_csv}")
    print(f"[DONE] {symbol}: report:        {report_path}")

    return (
        symbol_signals,
        metrics_df,
        threshold_summary,
        bucket_summary,
        regime_session_summary,
    )


def save_master_outputs(
    signal_frames: list[pd.DataFrame],
    metrics_frames: list[pd.DataFrame],
    threshold_frames: list[pd.DataFrame],
    bucket_frames: list[pd.DataFrame],
    regime_session_frames: list[pd.DataFrame],
) -> None:
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_signals = pd.concat(signal_frames, ignore_index=True) if signal_frames else pd.DataFrame()
    master_metrics = pd.concat(metrics_frames, ignore_index=True) if metrics_frames else pd.DataFrame()
    master_thresholds = pd.concat(threshold_frames, ignore_index=True) if threshold_frames else pd.DataFrame()
    master_buckets = pd.concat(bucket_frames, ignore_index=True) if bucket_frames else pd.DataFrame()
    master_regime_sessions = (
        pd.concat(regime_session_frames, ignore_index=True)
        if regime_session_frames
        else pd.DataFrame()
    )

    signals_parquet = master_analysis_dir / "master_microstructure_signal_events_latest.parquet"
    signals_csv = master_analysis_dir / "master_microstructure_signal_events_latest.csv"

    metrics_csv = master_analysis_dir / "master_microstructure_signal_model_metrics_latest.csv"
    metrics_json = master_analysis_dir / "master_microstructure_signal_model_metrics_latest.json"

    threshold_csv = master_analysis_dir / "master_microstructure_signal_threshold_summary_latest.csv"
    bucket_csv = master_analysis_dir / "master_microstructure_signal_bucket_summary_latest.csv"
    regime_session_csv = master_analysis_dir / "master_microstructure_signal_regime_session_summary_latest.csv"

    master_signals.to_parquet(signals_parquet, index=False)
    master_signals.to_csv(signals_csv, index=False)

    master_metrics.to_csv(metrics_csv, index=False)

    with open(metrics_json, "w", encoding="utf-8") as f:
        json.dump(master_metrics.to_dict(orient="records"), f, indent=4, default=str)

    master_thresholds.to_csv(threshold_csv, index=False)
    master_buckets.to_csv(bucket_csv, index=False)
    master_regime_sessions.to_csv(regime_session_csv, index=False)

    signal_winners = pd.DataFrame()

    if not master_thresholds.empty:
        usable = master_thresholds[master_thresholds["signals"] >= 20].copy()

        if not usable.empty:
            signal_winners = (
                usable.sort_values(
                    ["symbol", "target", "actual_target_rate_pct", "signals"],
                    ascending=[True, True, False, False],
                )
                .groupby(["symbol", "target"], as_index=False)
                .head(1)
                .sort_values("actual_target_rate_pct", ascending=False)
                .reset_index(drop=True)
            )

    winners_csv = master_analysis_dir / "master_microstructure_signal_winners_latest.csv"
    signal_winners.to_csv(winners_csv, index=False)

    report_path = master_report_dir / "master_microstructure_signal_research_report_latest.txt"

    report_path.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - MASTER MICROSTRUCTURE SIGNAL RESEARCH REPORT",
                "=" * 90,
                f"Report time UTC: {datetime.now(timezone.utc).isoformat()}",
                "-" * 90,
                "",
                "MODEL METRICS",
                "-" * 90,
                master_metrics.to_string(index=False)
                if not master_metrics.empty
                else "No metrics generated.",
                "",
                "SIGNAL WINNERS",
                "-" * 90,
                signal_winners.to_string(index=False)
                if not signal_winners.empty
                else "No signal winners generated.",
                "",
                "THRESHOLD SUMMARY",
                "-" * 90,
                master_thresholds.to_string(index=False)
                if not master_thresholds.empty
                else "No threshold summary generated.",
                "",
                "INTERPRETATION NOTES",
                "-" * 90,
                "This is diagnostic signal research, not a trading system.",
                "Signals are model-confidence events, not execution recommendations.",
                "Threshold summaries show selectivity versus realised target rate.",
                "Future work should add costs, spread filters, walk-forward validation, and live-safe execution rules.",
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    print("-" * 90)
    print("[DONE] Master signal research outputs created.")
    print(f"Master signals CSV:   {signals_csv}")
    print(f"Master metrics CSV:   {metrics_csv}")
    print(f"Master threshold CSV: {threshold_csv}")
    print(f"Master bucket CSV:    {bucket_csv}")
    print(f"Master regime CSV:    {regime_session_csv}")
    print(f"Signal winners CSV:   {winners_csv}")
    print(f"Master report:        {report_path}")

    if not signal_winners.empty:
        print("-" * 90)
        print("SIGNAL WINNERS")
        print(signal_winners.to_string(index=False))

    if not master_thresholds.empty:
        print("-" * 90)
        print("THRESHOLD SUMMARY")
        print(
            master_thresholds.sort_values(
                ["actual_target_rate_pct", "signals"],
                ascending=[False, False],
            )
            .head(40)
            .to_string(index=False)
        )


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 24 BUILD MICROSTRUCTURE SIGNAL RESEARCH - MULTI SYMBOL")
    print("=" * 90)
    print(f"Feature root:     {INPUT_FEATURE_ROOT}")
    print(f"Baseline winners: {BASELINE_WINNERS_PATH}")
    print(f"Output analysis:  {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output reports:   {OUTPUT_REPORT_ROOT}")
    print("-" * 90)

    winners = load_baseline_winners()

    print(f"[INFO] Winner experiments loaded: {len(winners):,}")

    signal_frames = []
    metrics_frames = []
    threshold_frames = []
    bucket_frames = []
    regime_session_frames = []

    for symbol, symbol_experiments in winners.groupby("symbol"):
        (
            symbol_signals,
            metrics_df,
            threshold_summary,
            bucket_summary,
            regime_session_summary,
        ) = process_symbol(symbol, symbol_experiments)

        if not symbol_signals.empty:
            signal_frames.append(symbol_signals)

        if not metrics_df.empty:
            metrics_frames.append(metrics_df)

        if not threshold_summary.empty:
            threshold_frames.append(threshold_summary)

        if not bucket_summary.empty:
            bucket_frames.append(bucket_summary)

        if not regime_session_summary.empty:
            regime_session_frames.append(regime_session_summary)

    if not metrics_frames:
        print("[WARN] No signal research metrics created.")
        return

    save_master_outputs(
        signal_frames=signal_frames,
        metrics_frames=metrics_frames,
        threshold_frames=threshold_frames,
        bucket_frames=bucket_frames,
        regime_session_frames=regime_session_frames,
    )

    print("-" * 90)
    print("[COMPLETE] Multi-symbol microstructure signal research complete.")
    print(f"Symbols analysed: {len(metrics_frames)}")
    print("=" * 90)


if __name__ == "__main__":
    main()