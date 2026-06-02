"""
BACQE MICROSTRUCTURE 29 - WALK FORWARD RESEARCH

Purpose:
    Run walk-forward validation on Script 28 playbook candidates.

Method:
    - Load Tier 1 / Tier 2 playbook candidates
    - Load matching research dataset
    - Train model on expanding/rolling windows
    - Test on next forward window
    - Summarise stability across folds

Important:
    This is still research validation, not live trading.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd

from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import (
    RandomForestClassifier,
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

MIN_TOTAL_ROWS = 300
MIN_TRAIN_ROWS = 150
MIN_TEST_ROWS = 50
N_SPLITS = 5


def print_header(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_analysis_dir(micro_cfg: dict) -> Path:
    return Path(
        micro_cfg["output"].get(
            "analysis_dir",
            "E:/Quant_Lab/data/analysis/microstructure",
        )
    )


def build_model(model_name: str):
    if model_name == "logistic_regression":
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                (
                    "model",
                    LogisticRegression(
                        max_iter=1000,
                        class_weight="balanced",
                        random_state=42,
                    ),
                ),
            ]
        )

    if model_name == "random_forest":
        return Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    RandomForestClassifier(
                        n_estimators=200,
                        max_depth=5,
                        min_samples_leaf=25,
                        class_weight="balanced",
                        random_state=42,
                        n_jobs=-1,
                    ),
                ),
            ]
        )

    if model_name == "extra_trees":
        return Pipeline(
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
        )

    if model_name == "gradient_boosting":
        return Pipeline(
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
        )

    if model_name == "hist_gradient_boosting":
        return Pipeline(
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
        )

    raise ValueError(f"Unknown model_name: {model_name}")


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


def make_walk_forward_splits(n_rows: int, n_splits: int = N_SPLITS) -> list[tuple[int, int, int]]:
    """
    Expanding-window walk-forward splits.

    Returns:
        [(train_start, train_end, test_end), ...]
    """
    splits = []

    if n_rows < MIN_TOTAL_ROWS:
        return splits

    initial_train_end = int(n_rows * 0.50)
    remaining = n_rows - initial_train_end
    test_window = max(int(remaining / n_splits), MIN_TEST_ROWS)

    train_start = 0
    train_end = initial_train_end

    while train_end + test_window <= n_rows:
        test_end = train_end + test_window

        if train_end - train_start >= MIN_TRAIN_ROWS and test_end - train_end >= MIN_TEST_ROWS:
            splits.append((train_start, train_end, test_end))

        train_end = test_end

    return splits


def get_feature_columns(df: pd.DataFrame) -> list[str]:
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    return [
        col for col in numeric_cols
        if col not in TARGET_COLUMNS
        and col not in METADATA_COLUMNS
    ]


def run_walk_forward_for_candidate(row: pd.Series) -> list[dict]:
    dataset_file = Path(row["dataset_file"])
    model_name = row["model_name"]
    target = row["target"]

    base_record = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "playbook_rank": row.get("playbook_rank"),
        "symbol": row.get("symbol"),
        "bar_type": row.get("bar_type"),
        "parameter": row.get("parameter"),
        "target": target,
        "model_name": model_name,
        "model_family": row.get("model_family"),
        "playbook_priority": row.get("playbook_priority"),
        "catalogue_score": row.get("catalogue_score"),
        "dataset_file": str(dataset_file),
    }

    if not dataset_file.exists():
        return [{**base_record, "fold": None, "status": "missing_dataset", "error": "Dataset file missing"}]

    try:
        df = pd.read_parquet(dataset_file)
    except Exception as exc:
        return [{**base_record, "fold": None, "status": "failed_read", "error": str(exc)}]

    if df.empty:
        return [{**base_record, "fold": None, "status": "empty_dataset", "error": "Dataset empty"}]

    if target not in df.columns:
        return [{**base_record, "fold": None, "status": "missing_target", "error": f"Missing target: {target}"}]

    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
        df = df.sort_values("end_time").reset_index(drop=True)

    feature_cols = get_feature_columns(df)

    work_df = df[feature_cols + [target]].copy()
    work_df = work_df.replace([np.inf, -np.inf], np.nan)
    work_df = work_df.dropna(subset=[target]).reset_index(drop=True)

    if len(work_df) < MIN_TOTAL_ROWS:
        return [{
            **base_record,
            "fold": None,
            "status": "low_rows",
            "row_count": len(work_df),
            "error": f"Rows below minimum: {len(work_df)} < {MIN_TOTAL_ROWS}",
        }]

    work_df[f"{target}_up"] = (work_df[target] > 0).astype(int)

    if work_df[f"{target}_up"].nunique() < 2:
        return [{
            **base_record,
            "fold": None,
            "status": "single_class",
            "row_count": len(work_df),
            "error": "Target has only one class.",
        }]

    splits = make_walk_forward_splits(len(work_df))

    if not splits:
        return [{
            **base_record,
            "fold": None,
            "status": "no_valid_splits",
            "row_count": len(work_df),
            "error": "No valid walk-forward splits created.",
        }]

    records = []

    for fold_idx, (train_start, train_end, test_end) in enumerate(splits, start=1):
        train_df = work_df.iloc[train_start:train_end].copy()
        test_df = work_df.iloc[train_end:test_end].copy()

        fold_record = {
            **base_record,
            "fold": fold_idx,
            "status": "unknown",
            "row_count": len(work_df),
            "feature_count": len(feature_cols),
            "train_start_idx": train_start,
            "train_end_idx": train_end,
            "test_start_idx": train_end,
            "test_end_idx": test_end,
            "train_rows": len(train_df),
            "test_rows": len(test_df),
            "train_positive_rate": None,
            "test_positive_rate": None,
            "accuracy": None,
            "balanced_accuracy": None,
            "precision": None,
            "recall": None,
            "roc_auc": None,
            "error": None,
        }

        y_train = train_df[f"{target}_up"]
        y_test = test_df[f"{target}_up"]

        if y_train.nunique() < 2 or y_test.nunique() < 2:
            fold_record["status"] = "single_class_split"
            fold_record["error"] = "Train or test fold has one class."
            records.append(fold_record)
            continue

        X_train = train_df[feature_cols]
        X_test = test_df[feature_cols]

        try:
            model = build_model(model_name)
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
            fold_record["status"] = "model_failed"
            fold_record["error"] = str(exc)
            records.append(fold_record)
            continue

        fold_record["status"] = "ok"
        fold_record["train_positive_rate"] = float(y_train.mean())
        fold_record["test_positive_rate"] = float(y_test.mean())
        fold_record.update(metrics)

        records.append(fold_record)

    return records


def classify_walk_forward(row: pd.Series) -> str:
    ok_folds = row.get("ok_folds", 0)
    avg_auc = row.get("avg_roc_auc")
    avg_bal = row.get("avg_balanced_accuracy")
    positive_fold_rate = row.get("positive_fold_rate", 0)
    min_auc = row.get("min_roc_auc")
    avg_test_rows = row.get("avg_test_rows", 0)

    if ok_folds < 2:
        return "insufficient"

    if avg_test_rows < 50:
        return "low_sample"

    if (
        pd.notna(avg_auc)
        and pd.notna(avg_bal)
        and avg_auc >= 0.60
        and avg_bal >= 0.56
        and positive_fold_rate >= 0.60
    ):
        return "walk_forward_strong"

    if (
        pd.notna(avg_auc)
        and pd.notna(avg_bal)
        and avg_auc >= 0.55
        and avg_bal >= 0.53
        and positive_fold_rate >= 0.50
    ):
        return "walk_forward_research"

    if (
        pd.notna(avg_auc)
        and avg_auc >= 0.52
        and positive_fold_rate >= 0.40
    ):
        return "walk_forward_weak"

    if pd.notna(min_auc) and min_auc < 0.45:
        return "unstable"

    return "no_walk_forward_edge"


def summarise_walk_forward(fold_df: pd.DataFrame) -> pd.DataFrame:
    ok_df = fold_df[fold_df["status"] == "ok"].copy()

    if ok_df.empty:
        return pd.DataFrame()

    group_cols = [
        "playbook_rank",
        "symbol",
        "bar_type",
        "parameter",
        "target",
        "model_name",
        "model_family",
        "playbook_priority",
        "catalogue_score",
        "dataset_file",
    ]

    grouped = (
        ok_df
        .groupby(group_cols, dropna=False)
        .agg(
            ok_folds=("fold", "count"),
            avg_accuracy=("accuracy", "mean"),
            min_accuracy=("accuracy", "min"),
            max_accuracy=("accuracy", "max"),
            avg_balanced_accuracy=("balanced_accuracy", "mean"),
            min_balanced_accuracy=("balanced_accuracy", "min"),
            max_balanced_accuracy=("balanced_accuracy", "max"),
            avg_precision=("precision", "mean"),
            avg_recall=("recall", "mean"),
            avg_roc_auc=("roc_auc", "mean"),
            min_roc_auc=("roc_auc", "min"),
            max_roc_auc=("roc_auc", "max"),
            std_roc_auc=("roc_auc", "std"),
            avg_test_rows=("test_rows", "mean"),
            total_test_rows=("test_rows", "sum"),
            folds_above_auc_55=("roc_auc", lambda s: int((s >= 0.55).sum())),
            folds_above_auc_60=("roc_auc", lambda s: int((s >= 0.60).sum())),
        )
        .reset_index()
    )

    grouped["positive_fold_rate"] = grouped["folds_above_auc_55"] / grouped["ok_folds"]
    grouped["strong_fold_rate"] = grouped["folds_above_auc_60"] / grouped["ok_folds"]

    grouped["walk_forward_label"] = grouped.apply(classify_walk_forward, axis=1)
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    grouped = grouped.sort_values(
        [
            "walk_forward_label",
            "avg_roc_auc",
            "avg_balanced_accuracy",
            "positive_fold_rate",
            "total_test_rows",
        ],
        ascending=[True, False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    label_rank = {
        "walk_forward_strong": 1,
        "walk_forward_research": 2,
        "walk_forward_weak": 3,
        "no_walk_forward_edge": 4,
        "unstable": 5,
        "low_sample": 6,
        "insufficient": 7,
    }

    grouped["label_rank"] = grouped["walk_forward_label"].map(label_rank).fillna(99)

    grouped = grouped.sort_values(
        [
            "label_rank",
            "avg_roc_auc",
            "avg_balanced_accuracy",
            "positive_fold_rate",
            "total_test_rows",
        ],
        ascending=[True, False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    grouped["walk_forward_rank"] = grouped.index + 1

    return grouped


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 29 - WALK FORWARD RESEARCH")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    playbook_path = (
        analysis_dir
        / "playbooks"
        / "microstructure_playbook_latest.csv"
    )

    report_dir = analysis_dir / "walk_forward_research"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Playbook:   {playbook_path}")
    print(f"Report dir: {report_dir}")
    print("-" * 90)

    if not playbook_path.exists():
        raise FileNotFoundError(
            f"Missing playbook file: {playbook_path}. Run script 28 first."
        )

    playbook_df = pd.read_csv(playbook_path)

    if playbook_df.empty:
        raise RuntimeError("Playbook is empty.")

    print(f"Playbook candidates: {len(playbook_df):,}")
    print("-" * 90)

    fold_records = []

    for idx, row in playbook_df.iterrows():
        records = run_walk_forward_for_candidate(row)
        fold_records.extend(records)

        print(
            f"[WF] {idx + 1:>2}/{len(playbook_df)} "
            f"{row['symbol']:<8} "
            f"{row['bar_type']:<22} "
            f"{row['parameter']:<26} "
            f"{row['target']:<16} "
            f"{row['model_name']:<24} "
            f"records={len(records)}"
        )

    fold_df = pd.DataFrame(fold_records)

    summary_df = summarise_walk_forward(fold_df)

    fold_csv = report_dir / "microstructure_walk_forward_folds_latest.csv"
    summary_csv = report_dir / "microstructure_walk_forward_summary_latest.csv"
    json_path = report_dir / "microstructure_walk_forward_research_latest.json"
    txt_path = report_dir / "microstructure_walk_forward_research_latest.txt"

    fold_df.to_csv(fold_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "playbook_candidates": len(playbook_df),
        "fold_rows": len(fold_df),
        "summary_rows": len(summary_df),
        "top_summary": summary_df.head(50).to_dict(orient="records") if not summary_df.empty else [],
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    fold_status_counts = fold_df["status"].value_counts(dropna=False).to_dict()

    if not summary_df.empty:
        label_counts = summary_df["walk_forward_label"].value_counts(dropna=False).to_dict()
        priority_counts = summary_df["playbook_priority"].value_counts(dropna=False).to_dict()
        top_summary = summary_df.head(30)
    else:
        label_counts = {}
        priority_counts = {}
        top_summary = pd.DataFrame()

    display_cols = [
        "walk_forward_rank",
        "symbol",
        "bar_type",
        "parameter",
        "target",
        "model_name",
        "playbook_priority",
        "ok_folds",
        "avg_roc_auc",
        "min_roc_auc",
        "max_roc_auc",
        "std_roc_auc",
        "avg_balanced_accuracy",
        "positive_fold_rate",
        "strong_fold_rate",
        "total_test_rows",
        "walk_forward_label",
    ]

    available_display_cols = [
        c for c in display_cols if not top_summary.empty and c in top_summary.columns
    ]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE WALK FORWARD RESEARCH")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Playbook candidates: {len(playbook_df):,}")
    lines.append(f"Fold rows:           {len(fold_df):,}")
    lines.append(f"Summary rows:        {len(summary_df):,}")
    lines.append("")
    lines.append(f"Fold status counts:  {fold_status_counts}")
    lines.append(f"WF label counts:     {label_counts}")
    lines.append(f"Priority counts:     {priority_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP WALK FORWARD RESULTS")
    lines.append("-" * 90)

    if top_summary.empty:
        lines.append("No successful walk-forward summaries.")
    else:
        lines.append(top_summary[available_display_cols].to_string(index=False))

    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Walk-forward research complete.")
    print(f"Playbook candidates: {len(playbook_df):,}")
    print(f"Fold rows:           {len(fold_df):,}")
    print(f"Summary rows:        {len(summary_df):,}")
    print(f"Fold status counts:  {fold_status_counts}")
    print(f"WF label counts:     {label_counts}")
    print(f"Fold CSV:            {fold_csv}")
    print(f"Summary CSV:         {summary_csv}")
    print(f"JSON output:         {json_path}")
    print(f"TXT output:          {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()