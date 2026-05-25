"""
BACQE TICK RESEARCH - 23 Feature Importance Analysis

Analyses feature importance for the strongest baseline modelling setups.

Focus:
    - target_up_h1 on imbalance_25
    - target_direction_persist_h1 on imbalance_50
    - target_direction_persist_h1 on tick_500

Outputs:
    E:/Quant_Lab/data/analysis/tick_research/feature_importance_latest.csv
    E:/Quant_Lab/reports/tick_research/feature_importance/feature_importance_report_latest.txt
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
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "feature_importance"

TEST_SIZE_PCT = 0.30

EXPERIMENTS = [
    {
        "experiment_name": "imbalance_25_predict_up_h1",
        "bar_type": "imbalance_25",
        "target": "target_up_h1",
    },
    {
        "experiment_name": "imbalance_50_predict_persist_h1",
        "bar_type": "imbalance_50",
        "target": "target_direction_persist_h1",
    },
    {
        "experiment_name": "tick_500_predict_persist_h1",
        "bar_type": "tick_500",
        "target": "target_direction_persist_h1",
    },
]

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

    pipeline = Pipeline(
        steps=[
            ("preprocess", preprocessor),
            ("model", model),
        ]
    )

    return pipeline


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


def run_experiment(df: pd.DataFrame, experiment: dict) -> tuple[pd.DataFrame, dict]:
    name = experiment["experiment_name"]
    bar_type = experiment["bar_type"]
    target = experiment["target"]

    subset = df[df["bar_type"] == bar_type].copy()
    subset = subset.dropna(subset=[target, "bar_start_time"])

    if len(subset) < 100:
        return pd.DataFrame(), {
            "experiment_name": name,
            "bar_type": bar_type,
            "target": target,
            "status": "skipped_low_rows",
            "rows": len(subset),
        }

    y = pd.to_numeric(subset[target], errors="coerce").fillna(0).astype(int)

    if y.nunique() < 2:
        return pd.DataFrame(), {
            "experiment_name": name,
            "bar_type": bar_type,
            "target": target,
            "status": "skipped_single_class",
            "rows": len(subset),
        }

    numeric_cols, categorical_cols = get_feature_columns(subset)

    train, test = chronological_split(subset)

    X_train = train[numeric_cols + categorical_cols]
    y_train = pd.to_numeric(train[target], errors="coerce").fillna(0).astype(int)

    X_test = test[numeric_cols + categorical_cols]
    y_test = pd.to_numeric(test[target], errors="coerce").fillna(0).astype(int)

    pipeline = build_model(numeric_cols, categorical_cols)

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
            "experiment_name": name,
            "bar_type": bar_type,
            "target": target,
            "feature": feature_names,
            "importance": importances,
        }
    )

    importance = importance.sort_values("importance", ascending=False).reset_index(drop=True)
    importance["importance_rank"] = importance.index + 1
    importance["run_time_utc"] = datetime.now(timezone.utc).isoformat()

    metrics = {
        "experiment_name": name,
        "bar_type": bar_type,
        "target": target,
        "status": "success",
        "rows": len(subset),
        "train_rows": len(train),
        "test_rows": len(test),
        "feature_count": len(feature_names),
        "balanced_accuracy": balanced_acc,
        "roc_auc": roc_auc,
        "test_positive_rate": float(y_test.mean()),
        "predicted_positive_rate": float(np.mean(preds)),
    }

    return importance, metrics


def build_report(importance: pd.DataFrame, metrics: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - FEATURE IMPORTANCE REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("MODEL METRICS")
    lines.append("-" * 90)
    lines.append(metrics.to_string(index=False))

    lines.append("")
    lines.append("TOP FEATURES BY EXPERIMENT")
    lines.append("-" * 90)

    for experiment_name in importance["experiment_name"].unique():
        lines.append("")
        lines.append(f"Experiment: {experiment_name}")
        lines.append("-" * 90)

        top = importance[importance["experiment_name"] == experiment_name].head(25)
        lines.append(
            top[
                [
                    "importance_rank",
                    "feature",
                    "importance",
                    "bar_type",
                    "target",
                ]
            ].to_string(index=False)
        )

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This uses RandomForest impurity-based feature importance.")
    lines.append("Feature importance is diagnostic, not proof of causality.")
    lines.append("Categorical features are one-hot encoded.")
    lines.append("Future and target columns are excluded.")
    lines.append("Chronological train/test split is used.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 23 FEATURE IMPORTANCE ANALYSIS")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Feature store not found: {INPUT_PATH}")

    df = pd.read_parquet(INPUT_PATH)

    print(f"Rows loaded:    {len(df):,}")
    print(f"Columns loaded: {len(df.columns):,}")

    all_importance = []
    all_metrics = []

    for experiment in EXPERIMENTS:
        print(
            f"[RUN] {experiment['experiment_name']} | "
            f"bar_type={experiment['bar_type']} | "
            f"target={experiment['target']}"
        )

        importance, metrics = run_experiment(df, experiment)

        if not importance.empty:
            all_importance.append(importance)

        all_metrics.append(metrics)

    importance_df = pd.concat(all_importance, ignore_index=True) if all_importance else pd.DataFrame()
    metrics_df = pd.DataFrame(all_metrics)

    numeric_cols = importance_df.select_dtypes(include=["float", "int"]).columns
    if len(numeric_cols) > 0:
        importance_df[numeric_cols] = importance_df[numeric_cols].round(10)

    metric_numeric_cols = metrics_df.select_dtypes(include=["float", "int"]).columns
    if len(metric_numeric_cols) > 0:
        metrics_df[metric_numeric_cols] = metrics_df[metric_numeric_cols].round(8)

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    importance_csv = OUTPUT_ANALYSIS_DIR / "feature_importance_latest.csv"
    importance_parquet = OUTPUT_ANALYSIS_DIR / "feature_importance_latest.parquet"

    metrics_csv = OUTPUT_ANALYSIS_DIR / "feature_importance_model_metrics_latest.csv"
    metrics_parquet = OUTPUT_ANALYSIS_DIR / "feature_importance_model_metrics_latest.parquet"

    report_path = OUTPUT_REPORT_DIR / "feature_importance_report_latest.txt"

    importance_df.to_csv(importance_csv, index=False)
    importance_df.to_parquet(importance_parquet, index=False)

    metrics_df.to_csv(metrics_csv, index=False)
    metrics_df.to_parquet(metrics_parquet, index=False)

    report = build_report(importance_df, metrics_df)
    report_path.write_text(report, encoding="utf-8")

    print("-" * 90)
    print("[DONE] Feature importance analysis created.")
    print(f"Importance CSV:     {importance_csv}")
    print(f"Importance Parquet: {importance_parquet}")
    print(f"Metrics CSV:        {metrics_csv}")
    print(f"Metrics Parquet:    {metrics_parquet}")
    print(f"Report:             {report_path}")
    print("-" * 90)

    print(metrics_df.to_string(index=False))
    print("-" * 90)

    if not importance_df.empty:
        print(
            importance_df.groupby("experiment_name")
            .head(10)[
                [
                    "experiment_name",
                    "importance_rank",
                    "feature",
                    "importance",
                ]
            ]
            .to_string(index=False)
        )

    print("=" * 90)


if __name__ == "__main__":
    main()