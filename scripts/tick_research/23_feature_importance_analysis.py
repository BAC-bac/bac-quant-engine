"""
BACQE TICK RESEARCH - 23 Feature Importance Analysis - Multi Symbol

Analyses feature importance for the strongest baseline modelling setups
identified by Script 22.

Inputs:
    E:/Quant_Lab/data/processed/tick_research/feature_store/symbol=<SYMBOL>/
    E:/Quant_Lab/data/analysis/tick_research/baseline_models/_master/
        symbol_target_winners_baseline_model_latest.csv

Outputs:
    Per-symbol:
        E:/Quant_Lab/data/analysis/tick_research/feature_importance/symbol=<SYMBOL>/
        E:/Quant_Lab/reports/tick_research/feature_importance/symbol=<SYMBOL>/

    Master:
        E:/Quant_Lab/data/analysis/tick_research/feature_importance/_master/
        E:/Quant_Lab/reports/tick_research/feature_importance/_master/

Notes:
    - Uses RandomForestClassifier impurity-based feature importance.
    - Even if Logistic Regression won in Script 22, RandomForest is used here
      as a diagnostic feature ranking model.
    - Future and target columns are excluded.
    - Datetime-like columns are excluded.
    - Chronological train/test split is used.
    - This is diagnostic research, not a trading system.
"""

from pathlib import Path
from datetime import datetime, timezone
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
    / "feature_importance"
)

OUTPUT_REPORT_ROOT = (
    DATA_LAKE_ROOT
    / "reports"
    / "tick_research"
    / "feature_importance"
)

TEST_SIZE_PCT = 0.30
MIN_ROWS = 200

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


def get_transformed_feature_names(
    pipeline: Pipeline,
    numeric_cols: list[str],
    categorical_cols: list[str],
) -> list[str]:
    preprocessor = pipeline.named_steps["preprocess"]

    feature_names = []
    feature_names.extend(numeric_cols)

    if categorical_cols:
        onehot = preprocessor.named_transformers_["categorical"].named_steps["onehot"]
        onehot_names = onehot.get_feature_names_out(categorical_cols).tolist()
        feature_names.extend(onehot_names)

    return feature_names


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


def run_experiment(
    feature_store: pd.DataFrame,
    experiment: pd.Series,
) -> tuple[pd.DataFrame, dict]:
    symbol = str(experiment["symbol"])
    target = str(experiment["target"])
    bar_type = str(experiment["bar_type"])
    experiment_name = str(experiment["experiment_name"])

    subset = feature_store[feature_store["bar_type"] == bar_type].copy()
    subset = subset.dropna(subset=[target, "bar_start_time"])

    if len(subset) < MIN_ROWS:
        return pd.DataFrame(), {
            "symbol": symbol,
            "experiment_name": experiment_name,
            "bar_type": bar_type,
            "target": target,
            "status": "skipped_low_rows",
            "rows": len(subset),
        }

    y = pd.to_numeric(subset[target], errors="coerce").fillna(0).astype(int)

    if y.nunique() < 2:
        return pd.DataFrame(), {
            "symbol": symbol,
            "experiment_name": experiment_name,
            "bar_type": bar_type,
            "target": target,
            "status": "skipped_single_class",
            "rows": len(subset),
        }

    numeric_cols, categorical_cols = get_feature_columns(subset)

    if not numeric_cols and not categorical_cols:
        return pd.DataFrame(), {
            "symbol": symbol,
            "experiment_name": experiment_name,
            "bar_type": bar_type,
            "target": target,
            "status": "skipped_no_features",
            "rows": len(subset),
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
            "rows": len(subset),
            "train_rows": len(train),
            "test_rows": len(test),
        }

    pipeline = build_model(numeric_cols, categorical_cols)

    try:
        pipeline.fit(X_train, y_train)

        preds = pipeline.predict(X_test)
        proba = pipeline.predict_proba(X_test)[:, 1]

        balanced_acc = balanced_accuracy_score(y_test, preds)
        roc_auc = roc_auc_score(y_test, proba) if y_test.nunique() == 2 else np.nan

        model = pipeline.named_steps["model"]
        importances = model.feature_importances_

        feature_names = get_transformed_feature_names(
            pipeline,
            numeric_cols,
            categorical_cols,
        )

        importance = pd.DataFrame(
            {
                "symbol": symbol,
                "experiment_name": experiment_name,
                "bar_type": bar_type,
                "target": target,
                "baseline_winning_model": experiment.get("model", np.nan),
                "baseline_balanced_accuracy": experiment.get(
                    "balanced_accuracy",
                    np.nan,
                ),
                "baseline_roc_auc": experiment.get("roc_auc", np.nan),
                "feature": feature_names,
                "importance": importances,
            }
        )

        importance = importance.sort_values(
            "importance",
            ascending=False,
        ).reset_index(drop=True)

        importance["importance_rank"] = importance.index + 1
        importance["run_time_utc"] = datetime.now(timezone.utc).isoformat()

        metrics = {
            "symbol": symbol,
            "experiment_name": experiment_name,
            "bar_type": bar_type,
            "target": target,
            "status": "success",
            "rows": len(subset),
            "train_rows": len(train),
            "test_rows": len(test),
            "numeric_feature_count": len(numeric_cols),
            "categorical_feature_count": len(categorical_cols),
            "transformed_feature_count": len(feature_names),
            "balanced_accuracy": balanced_acc,
            "roc_auc": roc_auc,
            "test_positive_rate": float(y_test.mean()),
            "predicted_positive_rate": float(np.mean(preds)),
            "baseline_winning_model": experiment.get("model", np.nan),
            "baseline_balanced_accuracy": experiment.get("balanced_accuracy", np.nan),
            "baseline_roc_auc": experiment.get("roc_auc", np.nan),
        }

        return importance, metrics

    except Exception as exc:
        return pd.DataFrame(), {
            "symbol": symbol,
            "experiment_name": experiment_name,
            "bar_type": bar_type,
            "target": target,
            "status": "failed",
            "rows": len(subset),
            "error_message": str(exc)[:500],
        }


def build_symbol_report(
    symbol: str,
    importance: pd.DataFrame,
    metrics: pd.DataFrame,
) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    lines = []
    lines.append("=" * 90)
    lines.append(f"BACQE TICK RESEARCH - FEATURE IMPORTANCE REPORT - {symbol}")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("MODEL METRICS")
    lines.append("-" * 90)
    lines.append(metrics.to_string(index=False))

    lines.append("")
    lines.append("TOP FEATURES BY EXPERIMENT")
    lines.append("-" * 90)

    if importance.empty:
        lines.append("No feature importance rows generated.")
    else:
        for experiment_name in importance["experiment_name"].unique():
            lines.append("")
            lines.append(f"Experiment: {experiment_name}")
            lines.append("-" * 90)

            top = importance[importance["experiment_name"] == experiment_name].head(25)

            display_cols = [
                "importance_rank",
                "feature",
                "importance",
                "bar_type",
                "target",
            ]

            lines.append(top[display_cols].to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This uses RandomForest impurity-based feature importance.")
    lines.append("Feature importance is diagnostic, not proof of causality.")
    lines.append("Categorical features are one-hot encoded.")
    lines.append("Future and target columns are excluded.")
    lines.append("Datetime-like columns are excluded.")
    lines.append("Chronological train/test split is used.")
    lines.append("=" * 90)

    return "\n".join(lines)


def process_symbol(
    symbol: str,
    symbol_experiments: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    input_path = (
        INPUT_FEATURE_ROOT
        / f"symbol={symbol}"
        / f"{symbol}_microstructure_feature_store_latest.parquet"
    )

    if not input_path.exists():
        print(f"[WARN] {symbol}: feature store not found: {input_path}")
        return pd.DataFrame(), pd.DataFrame()

    feature_store = pd.read_parquet(input_path)

    print(f"[INFO] {symbol}: rows loaded:    {len(feature_store):,}")
    print(f"[INFO] {symbol}: columns loaded: {len(feature_store.columns):,}")

    importance_frames = []
    metrics_records = []

    for _, experiment in symbol_experiments.iterrows():
        print(
            f"[RUN] {symbol} | "
            f"target={experiment['target']} | "
            f"bar_type={experiment['bar_type']}"
        )

        importance, metrics = run_experiment(feature_store, experiment)

        if not importance.empty:
            importance_frames.append(importance)

        metrics_records.append(metrics)

    importance_df = (
        pd.concat(importance_frames, ignore_index=True)
        if importance_frames
        else pd.DataFrame()
    )

    metrics_df = pd.DataFrame(metrics_records)

    importance_numeric_cols = importance_df.select_dtypes(include=["float", "int"]).columns
    if len(importance_numeric_cols) > 0:
        importance_df[importance_numeric_cols] = importance_df[
            importance_numeric_cols
        ].round(10)

    metrics_numeric_cols = metrics_df.select_dtypes(include=["float", "int"]).columns
    if len(metrics_numeric_cols) > 0:
        metrics_df[metrics_numeric_cols] = metrics_df[metrics_numeric_cols].round(8)

    analysis_dir = OUTPUT_ANALYSIS_ROOT / f"symbol={symbol}"
    report_dir = OUTPUT_REPORT_ROOT / f"symbol={symbol}"

    analysis_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    importance_csv = analysis_dir / f"{symbol}_feature_importance_latest.csv"
    importance_parquet = analysis_dir / f"{symbol}_feature_importance_latest.parquet"

    metrics_csv = analysis_dir / f"{symbol}_feature_importance_model_metrics_latest.csv"
    metrics_parquet = analysis_dir / f"{symbol}_feature_importance_model_metrics_latest.parquet"

    report_path = report_dir / f"{symbol}_feature_importance_report_latest.txt"

    importance_df.to_csv(importance_csv, index=False)
    importance_df.to_parquet(importance_parquet, index=False)

    metrics_df.to_csv(metrics_csv, index=False)
    metrics_df.to_parquet(metrics_parquet, index=False)

    report = build_symbol_report(symbol, importance_df, metrics_df)
    report_path.write_text(report, encoding="utf-8")

    print(f"[DONE] {symbol}: importance CSV: {importance_csv}")
    print(f"[DONE] {symbol}: metrics CSV:    {metrics_csv}")
    print(f"[DONE] {symbol}: report:         {report_path}")

    return importance_df, metrics_df


def save_master_outputs(
    importance_frames: list[pd.DataFrame],
    metrics_frames: list[pd.DataFrame],
) -> None:
    master_analysis_dir = OUTPUT_ANALYSIS_ROOT / "_master"
    master_report_dir = OUTPUT_REPORT_ROOT / "_master"

    master_analysis_dir.mkdir(parents=True, exist_ok=True)
    master_report_dir.mkdir(parents=True, exist_ok=True)

    master_importance = (
        pd.concat(importance_frames, ignore_index=True)
        if importance_frames
        else pd.DataFrame()
    )

    master_metrics = (
        pd.concat(metrics_frames, ignore_index=True)
        if metrics_frames
        else pd.DataFrame()
    )

    importance_csv = master_analysis_dir / "master_feature_importance_latest.csv"
    importance_parquet = master_analysis_dir / "master_feature_importance_latest.parquet"

    metrics_csv = master_analysis_dir / "master_feature_importance_model_metrics_latest.csv"
    metrics_parquet = master_analysis_dir / "master_feature_importance_model_metrics_latest.parquet"

    master_importance.to_csv(importance_csv, index=False)
    master_importance.to_parquet(importance_parquet, index=False)

    master_metrics.to_csv(metrics_csv, index=False)
    master_metrics.to_parquet(metrics_parquet, index=False)

    top_features = (
        master_importance
        .sort_values("importance", ascending=False)
        .groupby(["symbol", "experiment_name"], as_index=False)
        .head(10)
        .reset_index(drop=True)
        if not master_importance.empty
        else pd.DataFrame()
    )

    top_features_csv = master_analysis_dir / "master_top_features_by_experiment_latest.csv"
    top_features_parquet = master_analysis_dir / "master_top_features_by_experiment_latest.parquet"

    top_features.to_csv(top_features_csv, index=False)
    top_features.to_parquet(top_features_parquet, index=False)

    report_path = master_report_dir / "master_feature_importance_report_latest.txt"

    report_path.write_text(
        "\n".join(
            [
                "=" * 90,
                "BACQE TICK RESEARCH - MASTER FEATURE IMPORTANCE REPORT",
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
                "TOP FEATURES BY EXPERIMENT",
                "-" * 90,
                top_features[
                    [
                        "symbol",
                        "experiment_name",
                        "importance_rank",
                        "feature",
                        "importance",
                        "bar_type",
                        "target",
                    ]
                ].to_string(index=False)
                if not top_features.empty
                else "No feature importance generated.",
                "",
                "INTERPRETATION NOTES",
                "-" * 90,
                "This uses RandomForest impurity-based feature importance.",
                "Feature importance is diagnostic, not proof of causality.",
                "Future and target columns are excluded.",
                "Datetime-like columns are excluded.",
                "Chronological train/test split is used.",
                "=" * 90,
            ]
        ),
        encoding="utf-8",
    )

    print("-" * 90)
    print("[DONE] Master feature importance outputs created.")
    print(f"Master importance CSV: {importance_csv}")
    print(f"Master metrics CSV:    {metrics_csv}")
    print(f"Top features CSV:      {top_features_csv}")
    print(f"Master report:         {report_path}")

    if not master_metrics.empty:
        print("-" * 90)
        print("MODEL METRICS")
        print(master_metrics.to_string(index=False))

    if not top_features.empty:
        print("-" * 90)
        print("TOP FEATURES")
        print(
            top_features[
                [
                    "symbol",
                    "experiment_name",
                    "importance_rank",
                    "feature",
                    "importance",
                ]
            ].head(50).to_string(index=False)
        )


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 23 FEATURE IMPORTANCE ANALYSIS - MULTI SYMBOL")
    print("=" * 90)
    print(f"Feature root:     {INPUT_FEATURE_ROOT}")
    print(f"Baseline winners: {BASELINE_WINNERS_PATH}")
    print(f"Output analysis:  {OUTPUT_ANALYSIS_ROOT}")
    print(f"Output reports:   {OUTPUT_REPORT_ROOT}")
    print("-" * 90)

    winners = load_baseline_winners()

    print(f"[INFO] Winner experiments loaded: {len(winners):,}")

    importance_frames = []
    metrics_frames = []

    for symbol, symbol_experiments in winners.groupby("symbol"):
        importance_df, metrics_df = process_symbol(symbol, symbol_experiments)

        if not importance_df.empty:
            importance_frames.append(importance_df)

        if not metrics_df.empty:
            metrics_frames.append(metrics_df)

    if not metrics_frames:
        print("[WARN] No feature importance metrics created.")
        return

    save_master_outputs(
        importance_frames=importance_frames,
        metrics_frames=metrics_frames,
    )

    print("-" * 90)
    print("[COMPLETE] Multi-symbol feature importance analysis complete.")
    print(f"Symbols analysed: {len(metrics_frames)}")
    print("=" * 90)


if __name__ == "__main__":
    main()