"""
BACQE TICK RESEARCH - 22 Train Simple Microstructure Baseline Model

Trains simple baseline classifiers on the microstructure feature store.

Targets:
    target_up_h1
    target_direction_persist_h1

Models:
    LogisticRegression
    RandomForestClassifier

This is diagnostic research, not a trading system.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, balanced_accuracy_score, classification_report, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


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
OUTPUT_REPORT_DIR = DATA_LAKE_ROOT / "reports" / "tick_research" / "baseline_models"

TARGETS = [
    "target_up_h1",
    "target_direction_persist_h1",
]

BAR_TYPES_TO_TEST = [
    "tick_100",
    "tick_250",
    "tick_500",
    "tick_1000",
    "imbalance_25",
    "imbalance_50",
]

MIN_ROWS = 200
TEST_SIZE_PCT = 0.30

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
    exclude_prefixes = [
        "future_",
        "target_",
    ]

    feature_cols = []

    for col in df.columns:
        if col in EXCLUDE_COLUMNS:
            continue

        if any(col.startswith(prefix) for prefix in exclude_prefixes):
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


def build_models(numeric_cols: list[str], categorical_cols: list[str]) -> dict:
    numeric_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
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

    models = {
        "logistic_regression": Pipeline(
            steps=[
                ("preprocess", preprocessor),
                (
                    "model",
                    LogisticRegression(
                        max_iter=1000,
                        class_weight="balanced",
                        solver="lbfgs",
                    ),
                ),
            ]
        ),
        "random_forest": Pipeline(
            steps=[
                ("preprocess", preprocessor),
                (
                    "model",
                    RandomForestClassifier(
                        n_estimators=300,
                        max_depth=5,
                        min_samples_leaf=10,
                        random_state=42,
                        class_weight="balanced_subsample",
                        n_jobs=-1,
                    ),
                ),
            ]
        ),
    }

    return models


def chronological_train_test_split(df: pd.DataFrame, test_size_pct: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    data = df.sort_values("bar_start_time").reset_index(drop=True)

    split_idx = int(len(data) * (1 - test_size_pct))

    train = data.iloc[:split_idx].copy()
    test = data.iloc[split_idx:].copy()

    return train, test


def evaluate_model(model, X_train, y_train, X_test, y_test) -> dict:
    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(X_test)[:, 1]
    else:
        proba = None

    result = {
        "accuracy": accuracy_score(y_test, preds),
        "balanced_accuracy": balanced_accuracy_score(y_test, preds),
        "test_positive_rate": float(np.mean(y_test)),
        "predicted_positive_rate": float(np.mean(preds)),
    }

    if proba is not None and len(set(y_test)) == 2:
        result["roc_auc"] = roc_auc_score(y_test, proba)
    else:
        result["roc_auc"] = np.nan

    report = classification_report(y_test, preds, output_dict=True, zero_division=0)

    result["precision_class_1"] = report.get("1", {}).get("precision", np.nan)
    result["recall_class_1"] = report.get("1", {}).get("recall", np.nan)
    result["f1_class_1"] = report.get("1", {}).get("f1-score", np.nan)

    return result


def run_experiment(df: pd.DataFrame, target: str, bar_type: str) -> list[dict]:
    subset = df[df["bar_type"] == bar_type].copy()

    subset = subset.dropna(subset=[target, "bar_start_time"])

    if len(subset) < MIN_ROWS:
        return [
            {
                "target": target,
                "bar_type": bar_type,
                "model": "skipped",
                "status": "skipped_low_rows",
                "rows": len(subset),
            }
        ]

    y = pd.to_numeric(subset[target], errors="coerce").fillna(0).astype(int)

    if y.nunique() < 2:
        return [
            {
                "target": target,
                "bar_type": bar_type,
                "model": "skipped",
                "status": "skipped_single_class",
                "rows": len(subset),
            }
        ]

    numeric_cols, categorical_cols = get_feature_columns(subset)

    train, test = chronological_train_test_split(subset, TEST_SIZE_PCT)

    X_train = train[numeric_cols + categorical_cols]
    y_train = pd.to_numeric(train[target], errors="coerce").fillna(0).astype(int)

    X_test = test[numeric_cols + categorical_cols]
    y_test = pd.to_numeric(test[target], errors="coerce").fillna(0).astype(int)

    if y_train.nunique() < 2 or y_test.nunique() < 2:
        return [
            {
                "target": target,
                "bar_type": bar_type,
                "model": "skipped",
                "status": "skipped_single_class_split",
                "rows": len(subset),
                "train_rows": len(train),
                "test_rows": len(test),
            }
        ]

    models = build_models(numeric_cols, categorical_cols)

    records = []

    baseline_positive_rate = float(y_train.mean())
    naive_pred = np.repeat(int(baseline_positive_rate >= 0.5), len(y_test))

    records.append(
        {
            "target": target,
            "bar_type": bar_type,
            "model": "naive_majority",
            "status": "success",
            "rows": len(subset),
            "train_rows": len(train),
            "test_rows": len(test),
            "feature_count": len(numeric_cols) + len(categorical_cols),
            "accuracy": accuracy_score(y_test, naive_pred),
            "balanced_accuracy": balanced_accuracy_score(y_test, naive_pred),
            "roc_auc": np.nan,
            "test_positive_rate": float(y_test.mean()),
            "predicted_positive_rate": float(np.mean(naive_pred)),
            "precision_class_1": np.nan,
            "recall_class_1": np.nan,
            "f1_class_1": np.nan,
        }
    )

    for model_name, model in models.items():
        try:
            metrics = evaluate_model(
                model,
                X_train,
                y_train,
                X_test,
                y_test,
            )

            records.append(
                {
                    "target": target,
                    "bar_type": bar_type,
                    "model": model_name,
                    "status": "success",
                    "rows": len(subset),
                    "train_rows": len(train),
                    "test_rows": len(test),
                    "feature_count": len(numeric_cols) + len(categorical_cols),
                    **metrics,
                }
            )

        except Exception as exc:
            records.append(
                {
                    "target": target,
                    "bar_type": bar_type,
                    "model": model_name,
                    "status": "failed",
                    "rows": len(subset),
                    "error_message": str(exc)[:500],
                }
            )

    return records


def build_report(results: pd.DataFrame) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    successful = results[results["status"] == "success"].copy()

    display_cols = [
        "target",
        "bar_type",
        "model",
        "rows",
        "train_rows",
        "test_rows",
        "accuracy",
        "balanced_accuracy",
        "roc_auc",
        "test_positive_rate",
        "predicted_positive_rate",
        "f1_class_1",
    ]

    available_cols = [col for col in display_cols if col in results.columns]

    ranked = successful.copy()
    if "balanced_accuracy" in ranked.columns:
        ranked = ranked.sort_values("balanced_accuracy", ascending=False, na_position="last")

    lines = []

    lines.append("=" * 90)
    lines.append("BACQE TICK RESEARCH - SIMPLE MICROSTRUCTURE BASELINE MODEL REPORT")
    lines.append("=" * 90)
    lines.append(f"Report time UTC: {now_utc}")
    lines.append(f"Input:           {INPUT_PATH}")
    lines.append(f"Symbol:          {SYMBOL}")
    lines.append(f"Test size pct:   {TEST_SIZE_PCT}")
    lines.append("-" * 90)

    lines.append("")
    lines.append("ALL RESULTS")
    lines.append("-" * 90)
    lines.append(results[available_cols].to_string(index=False))

    lines.append("")
    lines.append("RANKED SUCCESSFUL MODELS BY BALANCED ACCURACY")
    lines.append("-" * 90)
    if ranked.empty:
        lines.append("No successful model results.")
    else:
        lines.append(ranked[available_cols].head(30).to_string(index=False))

    lines.append("")
    lines.append("INTERPRETATION NOTES")
    lines.append("-" * 90)
    lines.append("This is a diagnostic baseline, not a trading system.")
    lines.append("Chronological split is used to reduce lookahead bias.")
    lines.append("Naive majority is included as a baseline.")
    lines.append("Balanced accuracy is more useful than raw accuracy when labels are imbalanced.")
    lines.append("Future and target columns are excluded from features.")
    lines.append("A result near 50% balanced accuracy means little/no directional signal.")
    lines.append("=" * 90)

    return "\n".join(lines)


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 22 TRAIN SIMPLE MICROSTRUCTURE BASELINE MODEL")
    print("=" * 90)
    print(f"Input: {INPUT_PATH}")
    print("-" * 90)

    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Feature store not found: {INPUT_PATH}")

    df = pd.read_parquet(INPUT_PATH)

    print(f"Rows loaded:    {len(df):,}")
    print(f"Columns loaded: {len(df.columns):,}")

    all_records = []

    for target in TARGETS:
        for bar_type in BAR_TYPES_TO_TEST:
            print(f"[RUN] target={target} | bar_type={bar_type}")
            all_records.extend(run_experiment(df, target, bar_type))

    results = pd.DataFrame(all_records)

    numeric_cols = results.select_dtypes(include=["float", "int"]).columns
    results[numeric_cols] = results[numeric_cols].round(8)

    results["run_time_utc"] = datetime.now(timezone.utc).isoformat()

    OUTPUT_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_ANALYSIS_DIR / "microstructure_baseline_model_results_latest.csv"
    parquet_path = OUTPUT_ANALYSIS_DIR / "microstructure_baseline_model_results_latest.parquet"
    json_path = OUTPUT_ANALYSIS_DIR / "microstructure_baseline_model_results_latest.json"
    report_path = OUTPUT_REPORT_DIR / "microstructure_baseline_model_report_latest.txt"

    results.to_csv(csv_path, index=False)
    results.to_parquet(parquet_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results.to_dict(orient="records"), f, indent=4, default=str)

    report = build_report(results)
    report_path.write_text(report, encoding="utf-8")

    print("-" * 90)
    print("[DONE] Baseline modelling complete.")
    print(f"CSV:     {csv_path}")
    print(f"Parquet: {parquet_path}")
    print(f"JSON:    {json_path}")
    print(f"Report:  {report_path}")
    print("-" * 90)

    display_cols = [
        "target",
        "bar_type",
        "model",
        "status",
        "rows",
        "balanced_accuracy",
        "roc_auc",
        "test_positive_rate",
        "predicted_positive_rate",
        "f1_class_1",
    ]

    print(results[display_cols].to_string(index=False))
    print("=" * 90)


if __name__ == "__main__":
    main()