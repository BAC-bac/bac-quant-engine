"""
BACQE MICROSTRUCTURE 25 - ADVANCED MODELS

Purpose:
    Run heavier baseline classifiers on the Script 22 research datasets.

Models:
    - ExtraTreesClassifier
    - GradientBoostingClassifier
    - HistGradientBoostingClassifier

Targets:
    - forward_return_1 > 0
    - forward_return_3 > 0
    - forward_return_5 > 0
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd

from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.ensemble import (
    ExtraTreesClassifier,
    GradientBoostingClassifier,
    HistGradientBoostingClassifier,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"

TARGET_COLUMNS = ["forward_return_1", "forward_return_3", "forward_return_5"]

METADATA_COLUMNS = {
    "symbol",
    "start_time",
    "end_time",
    "bar_type",
    "parameter_name",
    "parameter_value",
}


def print_header(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_analysis_dir(micro_cfg: dict) -> Path:
    return Path(
        micro_cfg["output"].get(
            "analysis_dir",
            "E:/Quant_Lab/data/analysis/microstructure",
        )
    )


def parse_dataset_metadata(file_path: Path) -> dict:
    metadata = {"symbol": None, "bar_type": None, "parameter": None}

    for part in file_path.parts:
        if part.startswith("symbol="):
            metadata["symbol"] = part.replace("symbol=", "")
        elif part.startswith("bar_type="):
            metadata["bar_type"] = part.replace("bar_type=", "")
        elif part.startswith("parameter="):
            metadata["parameter"] = part.replace("parameter=", "")

    return metadata


def build_models() -> dict:
    return {
        "extra_trees": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    ExtraTreesClassifier(
                        n_estimators=300,
                        max_depth=6,
                        min_samples_leaf=20,
                        class_weight="balanced",
                        random_state=42,
                        n_jobs=-1,
                    ),
                ),
            ]
        ),
        "gradient_boosting": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    GradientBoostingClassifier(
                        n_estimators=150,
                        learning_rate=0.03,
                        max_depth=3,
                        min_samples_leaf=20,
                        random_state=42,
                    ),
                ),
            ]
        ),
        "hist_gradient_boosting": Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    HistGradientBoostingClassifier(
                        max_iter=200,
                        learning_rate=0.03,
                        max_leaf_nodes=15,
                        l2_regularization=0.1,
                        random_state=42,
                    ),
                ),
            ]
        ),
    }


def safe_roc_auc(y_true, y_proba):
    try:
        if len(np.unique(y_true)) < 2:
            return None
        return float(roc_auc_score(y_true, y_proba))
    except Exception:
        return None


def evaluate_predictions(y_true, y_pred, y_proba) -> dict:
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "roc_auc": safe_roc_auc(y_true, y_proba),
    }


def classify_model_result(status: str, balanced_accuracy, roc_auc, test_rows: int) -> str:
    if status != "ok":
        return "not_usable"

    if test_rows < 100:
        return "low_sample"

    if pd.notna(roc_auc) and roc_auc >= 0.62 and balanced_accuracy >= 0.58:
        return "strong_advanced"

    if pd.notna(roc_auc) and roc_auc >= 0.57 and balanced_accuracy >= 0.54:
        return "research_advanced"

    if balanced_accuracy >= 0.515:
        return "weak_advanced"

    return "no_edge_advanced"


def run_model_for_target(
    df: pd.DataFrame,
    feature_cols: list[str],
    target_col: str,
    model_name: str,
    model,
    metadata: dict,
    dataset_file: Path,
) -> dict:
    result = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": metadata["symbol"],
        "bar_type": metadata["bar_type"],
        "parameter": metadata["parameter"],
        "dataset_file": str(dataset_file),
        "model_name": model_name,
        "target": target_col,
        "binary_target": f"{target_col}_up",
        "status": "unknown",
        "row_count": len(df),
        "feature_count": len(feature_cols),
        "train_rows": 0,
        "test_rows": 0,
        "train_positive_rate": None,
        "test_positive_rate": None,
        "accuracy": None,
        "balanced_accuracy": None,
        "precision": None,
        "recall": None,
        "roc_auc": None,
        "model_label": "not_usable",
        "error": None,
    }

    work_df = df[feature_cols + [target_col]].copy()
    work_df = work_df.replace([np.inf, -np.inf], np.nan)
    work_df = work_df.dropna(subset=[target_col]).reset_index(drop=True)

    if len(work_df) < 200:
        result["status"] = "low_rows"
        result["error"] = "Fewer than 200 usable rows."
        return result

    work_df[f"{target_col}_up"] = (work_df[target_col] > 0).astype(int)

    if work_df[f"{target_col}_up"].nunique() < 2:
        result["status"] = "single_class"
        result["error"] = "Target has only one class."
        return result

    split_idx = int(len(work_df) * 0.70)

    train_df = work_df.iloc[:split_idx].copy()
    test_df = work_df.iloc[split_idx:].copy()

    if len(train_df) < 150 or len(test_df) < 75:
        result["status"] = "low_split_rows"
        result["error"] = "Train/test split too small."
        return result

    X_train = train_df[feature_cols]
    y_train = train_df[f"{target_col}_up"]

    X_test = test_df[feature_cols]
    y_test = test_df[f"{target_col}_up"]

    if y_train.nunique() < 2 or y_test.nunique() < 2:
        result["status"] = "single_class_split"
        result["error"] = "Train or test split has only one class."
        return result

    try:
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        if hasattr(model, "predict_proba"):
            y_proba = model.predict_proba(X_test)[:, 1]
        elif hasattr(model, "decision_function"):
            y_proba = model.decision_function(X_test)
        else:
            y_proba = y_pred

        metrics = evaluate_predictions(y_test, y_pred, y_proba)

    except Exception as exc:
        result["status"] = "model_failed"
        result["error"] = str(exc)
        return result

    result["status"] = "ok"
    result["train_rows"] = len(train_df)
    result["test_rows"] = len(test_df)
    result["train_positive_rate"] = float(y_train.mean())
    result["test_positive_rate"] = float(y_test.mean())
    result.update(metrics)

    result["model_label"] = classify_model_result(
        status=result["status"],
        balanced_accuracy=result["balanced_accuracy"],
        roc_auc=result["roc_auc"],
        test_rows=result["test_rows"],
    )

    return result


def run_dataset_models(dataset_file: Path) -> list[dict]:
    metadata = parse_dataset_metadata(dataset_file)

    try:
        df = pd.read_parquet(dataset_file)
    except Exception as exc:
        return [{
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "symbol": metadata["symbol"],
            "bar_type": metadata["bar_type"],
            "parameter": metadata["parameter"],
            "dataset_file": str(dataset_file),
            "model_name": None,
            "target": None,
            "binary_target": None,
            "status": "failed_read",
            "row_count": 0,
            "feature_count": 0,
            "train_rows": 0,
            "test_rows": 0,
            "train_positive_rate": None,
            "test_positive_rate": None,
            "accuracy": None,
            "balanced_accuracy": None,
            "precision": None,
            "recall": None,
            "roc_auc": None,
            "model_label": "not_usable",
            "error": str(exc),
        }]

    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
        df = df.sort_values("end_time").reset_index(drop=True)

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    feature_cols = [
        col for col in numeric_cols
        if col not in TARGET_COLUMNS
        and col not in METADATA_COLUMNS
    ]

    available_targets = [target for target in TARGET_COLUMNS if target in df.columns]

    models = build_models()
    records = []

    for target_col in available_targets:
        for model_name, model in models.items():
            records.append(
                run_model_for_target(
                    df=df,
                    feature_cols=feature_cols,
                    target_col=target_col,
                    model_name=model_name,
                    model=model,
                    metadata=metadata,
                    dataset_file=dataset_file,
                )
            )

    return records


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 25 - ADVANCED MODELS")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    research_dataset_root = analysis_dir / "research_datasets"
    report_dir = analysis_dir / "advanced_models"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Research dataset root: {research_dataset_root}")
    print(f"Report dir:            {report_dir}")
    print("-" * 90)

    dataset_files = sorted(
        research_dataset_root.glob("**/microstructure_research_dataset.parquet")
    )

    print(f"Research datasets discovered: {len(dataset_files)}")
    print("-" * 90)

    if not dataset_files:
        raise FileNotFoundError(
            f"No research datasets found in {research_dataset_root}. Run script 22 first."
        )

    all_records = []

    for idx, dataset_file in enumerate(dataset_files, start=1):
        records = run_dataset_models(dataset_file)
        all_records.extend(records)

        metadata = parse_dataset_metadata(dataset_file)

        print(
            f"[ADV_MODEL] {idx:>2}/{len(dataset_files)} "
            f"{metadata['symbol']:<8} "
            f"{metadata['bar_type']:<22} "
            f"{metadata['parameter']:<28} "
            f"tests={len(records)}"
        )

    results_df = pd.DataFrame(all_records)

    results_df = results_df.sort_values(
        ["model_label", "roc_auc", "balanced_accuracy", "test_rows"],
        ascending=[True, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    csv_path = report_dir / "microstructure_advanced_models_latest.csv"
    json_path = report_dir / "microstructure_advanced_models_latest.json"
    txt_path = report_dir / "microstructure_advanced_models_latest.txt"

    results_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results_df.to_dict(orient="records"), f, indent=2, default=str)

    status_counts = results_df["status"].value_counts(dropna=False).to_dict()
    label_counts = results_df["model_label"].value_counts(dropna=False).to_dict()
    model_counts = results_df["model_name"].value_counts(dropna=False).to_dict()
    target_counts = results_df["target"].value_counts(dropna=False).to_dict()

    ok_df = results_df[results_df["status"] == "ok"].copy()

    top_models = ok_df.head(50)[
        [
            "symbol",
            "bar_type",
            "parameter",
            "model_name",
            "target",
            "train_rows",
            "test_rows",
            "train_positive_rate",
            "test_positive_rate",
            "accuracy",
            "balanced_accuracy",
            "precision",
            "recall",
            "roc_auc",
            "model_label",
        ]
    ] if not ok_df.empty else pd.DataFrame()

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE ADVANCED MODELS")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Research datasets discovered: {len(dataset_files)}")
    lines.append(f"Advanced model tests run:      {len(results_df)}")
    lines.append("")
    lines.append(f"Status counts: {status_counts}")
    lines.append(f"Model label counts: {label_counts}")
    lines.append(f"Model counts: {model_counts}")
    lines.append(f"Target counts: {target_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP ADVANCED MODEL RESULTS")
    lines.append("-" * 90)

    if top_models.empty:
        lines.append("No successful advanced model results.")
    else:
        lines.append(top_models.to_string(index=False))

    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Advanced model run complete.")
    print(f"Datasets discovered: {len(dataset_files)}")
    print(f"Model tests run:     {len(results_df)}")
    print(f"Status counts:       {status_counts}")
    print(f"Model label counts:  {label_counts}")
    print(f"Model counts:        {model_counts}")
    print(f"Target counts:       {target_counts}")
    print(f"CSV output:          {csv_path}")
    print(f"JSON output:         {json_path}")
    print(f"TXT output:          {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()