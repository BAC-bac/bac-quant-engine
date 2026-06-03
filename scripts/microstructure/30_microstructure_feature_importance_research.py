"""
BACQE MICROSTRUCTURE 30 - FEATURE IMPORTANCE RESEARCH

Purpose:
    Analyse feature importance for the strongest walk-forward candidates.

Inputs:
    walk_forward_research/
        microstructure_walk_forward_summary_latest.csv

    playbooks/
        microstructure_playbook_latest.csv

Outputs:
    feature_importance_research/
        microstructure_feature_importance_latest.csv
        microstructure_feature_importance_summary_latest.csv
        microstructure_feature_importance_latest.json
        microstructure_feature_importance_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd

from sklearn.impute import SimpleImputer
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

SELECTED_WF_LABELS = {
    "walk_forward_strong",
    "walk_forward_research",
}

MIN_ROWS = 250


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


def classify_feature_family(feature_name: str) -> str:
    name = feature_name.lower()

    if "imbalance" in name or "uptick" in name or "downtick" in name or "signed_tick" in name:
        return "imbalance"

    if "spread" in name:
        return "spread"

    if "return" in name or "realized_vol" in name or "vol" in name:
        return "return_volatility"

    if "range" in name:
        return "range"

    if "duration" in name or "tick_count" in name or "ticks_per_second" in name or "bars_per_hour" in name:
        return "activity"

    if "volume" in name:
        return "volume"

    if "open_" in name or "high_" in name or "low_" in name or "close_" in name:
        return "price_ohlc"

    return "other"


def get_feature_columns(df: pd.DataFrame) -> list[str]:
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    return [
        col for col in numeric_cols
        if col not in TARGET_COLUMNS
        and col not in METADATA_COLUMNS
    ]


def extract_importances(model_pipeline, feature_cols: list[str], model_name: str) -> pd.DataFrame:
    fitted_model = model_pipeline.named_steps["model"]

    if hasattr(fitted_model, "feature_importances_"):
        values = fitted_model.feature_importances_
        importance_type = "tree_importance"

    elif hasattr(fitted_model, "coef_"):
        values = np.abs(fitted_model.coef_[0])
        importance_type = "abs_coefficient"

    else:
        return pd.DataFrame()

    importance_df = pd.DataFrame(
        {
            "feature_name": feature_cols,
            "raw_importance": values,
        }
    )

    total_importance = importance_df["raw_importance"].sum()

    if total_importance > 0:
        importance_df["normalised_importance"] = (
            importance_df["raw_importance"] / total_importance
        )
    else:
        importance_df["normalised_importance"] = 0.0

    importance_df["importance_type"] = importance_type
    importance_df["model_name"] = model_name
    importance_df["feature_family"] = importance_df["feature_name"].apply(classify_feature_family)

    return importance_df.sort_values(
        "normalised_importance",
        ascending=False,
    ).reset_index(drop=True)


def run_importance_for_candidate(row: pd.Series) -> list[dict]:
    dataset_file = Path(row["dataset_file"])
    target = row["target"]
    model_name = row["model_name"]

    base = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "walk_forward_rank": row.get("walk_forward_rank"),
        "playbook_rank": row.get("playbook_rank"),
        "symbol": row.get("symbol"),
        "bar_type": row.get("bar_type"),
        "parameter": row.get("parameter"),
        "target": target,
        "model_name": model_name,
        "model_family": row.get("model_family"),
        "playbook_priority": row.get("playbook_priority"),
        "walk_forward_label": row.get("walk_forward_label"),
        "avg_roc_auc": row.get("avg_roc_auc"),
        "avg_balanced_accuracy": row.get("avg_balanced_accuracy"),
        "dataset_file": str(dataset_file),
    }

    if not dataset_file.exists():
        return [{**base, "status": "missing_dataset", "error": "Dataset file missing"}]

    try:
        df = pd.read_parquet(dataset_file)
    except Exception as exc:
        return [{**base, "status": "failed_read", "error": str(exc)}]

    if df.empty:
        return [{**base, "status": "empty_dataset", "error": "Dataset is empty"}]

    if target not in df.columns:
        return [{**base, "status": "missing_target", "error": f"Missing target: {target}"}]

    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True, errors="coerce")
        df = df.sort_values("end_time").reset_index(drop=True)

    feature_cols = get_feature_columns(df)

    work_df = df[feature_cols + [target]].copy()
    work_df = work_df.replace([np.inf, -np.inf], np.nan)
    work_df = work_df.dropna(subset=[target]).reset_index(drop=True)

    if len(work_df) < MIN_ROWS:
        return [{
            **base,
            "status": "low_rows",
            "row_count": len(work_df),
            "error": f"Rows below minimum: {len(work_df)} < {MIN_ROWS}",
        }]

    work_df[f"{target}_up"] = (work_df[target] > 0).astype(int)

    if work_df[f"{target}_up"].nunique() < 2:
        return [{
            **base,
            "status": "single_class",
            "row_count": len(work_df),
            "error": "Target has one class only.",
        }]

    X = work_df[feature_cols]
    y = work_df[f"{target}_up"]

    try:
        model = build_model(model_name)
        model.fit(X, y)
        importance_df = extract_importances(model, feature_cols, model_name)

    except Exception as exc:
        return [{
            **base,
            "status": "model_failed",
            "row_count": len(work_df),
            "error": str(exc),
        }]

    if importance_df.empty:
        return [{
            **base,
            "status": "no_importance_available",
            "row_count": len(work_df),
            "error": "Model does not expose coefficients or feature_importances_.",
        }]

    records = []

    for rank, imp_row in importance_df.iterrows():
        records.append(
            {
                **base,
                "status": "ok",
                "row_count": len(work_df),
                "feature_rank": rank + 1,
                "feature_name": imp_row["feature_name"],
                "feature_family": imp_row["feature_family"],
                "importance_type": imp_row["importance_type"],
                "raw_importance": float(imp_row["raw_importance"]),
                "normalised_importance": float(imp_row["normalised_importance"]),
                "error": None,
            }
        )

    return records


def summarise_importance(importance_df: pd.DataFrame) -> pd.DataFrame:
    ok_df = importance_df[importance_df["status"] == "ok"].copy()

    if ok_df.empty:
        return pd.DataFrame()

    grouped = (
        ok_df.groupby(["feature_name", "feature_family"], dropna=False)
        .agg(
            appearances=("feature_name", "count"),
            avg_importance=("normalised_importance", "mean"),
            max_importance=("normalised_importance", "max"),
            median_importance=("normalised_importance", "median"),
            avg_rank=("feature_rank", "mean"),
            best_rank=("feature_rank", "min"),
            symbol_count=("symbol", "nunique"),
            symbols=("symbol", lambda s: ",".join(sorted(s.dropna().unique()))),
            bar_type_count=("bar_type", "nunique"),
            bar_types=("bar_type", lambda s: ",".join(sorted(s.dropna().unique()))),
            target_count=("target", "nunique"),
            targets=("target", lambda s: ",".join(sorted(s.dropna().unique()))),
            model_count=("model_name", "nunique"),
            models=("model_name", lambda s: ",".join(sorted(s.dropna().unique()))),
        )
        .reset_index()
    )

    grouped["importance_score"] = (
        grouped["avg_importance"] * 45
        + grouped["max_importance"] * 25
        + grouped["appearances"].clip(upper=20) * 1.0
        + grouped["symbol_count"] * 2.0
        + grouped["bar_type_count"] * 1.5
        + grouped["target_count"] * 1.5
        + grouped["model_count"] * 1.0
        + (31 - grouped["best_rank"]).clip(lower=0) * 0.3
    ).clip(0, 100).round(2)

    grouped = grouped.sort_values(
        ["importance_score", "avg_importance", "appearances", "best_rank"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    grouped["importance_summary_rank"] = grouped.index + 1
    grouped["created_at_utc"] = datetime.now(timezone.utc).isoformat()

    return grouped


def summarise_family_importance(importance_df: pd.DataFrame) -> pd.DataFrame:
    ok_df = importance_df[importance_df["status"] == "ok"].copy()

    if ok_df.empty:
        return pd.DataFrame()

    family = (
        ok_df.groupby(["feature_family"], dropna=False)
        .agg(
            feature_count=("feature_name", "nunique"),
            appearances=("feature_name", "count"),
            avg_importance=("normalised_importance", "mean"),
            max_importance=("normalised_importance", "max"),
            avg_rank=("feature_rank", "mean"),
            best_rank=("feature_rank", "min"),
            symbol_count=("symbol", "nunique"),
            bar_type_count=("bar_type", "nunique"),
            target_count=("target", "nunique"),
            model_count=("model_name", "nunique"),
        )
        .reset_index()
    )

    family["family_importance_score"] = (
        family["avg_importance"] * 50
        + family["max_importance"] * 25
        + family["appearances"].clip(upper=100) * 0.15
        + family["feature_count"] * 1.5
        + family["symbol_count"] * 2.0
        + family["bar_type_count"] * 1.0
        + family["target_count"] * 1.0
        + family["model_count"] * 1.0
    ).clip(0, 100).round(2)

    family = family.sort_values(
        ["family_importance_score", "avg_importance", "appearances"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    family["family_rank"] = family.index + 1

    return family


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 30 - FEATURE IMPORTANCE RESEARCH")

    config = load_config()
    micro_cfg = config["microstructure"]
    analysis_dir = get_analysis_dir(micro_cfg)

    wf_summary_path = (
        analysis_dir
        / "walk_forward_research"
        / "microstructure_walk_forward_summary_latest.csv"
    )

    report_dir = analysis_dir / "feature_importance_research"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Walk-forward summary: {wf_summary_path}")
    print(f"Report dir:           {report_dir}")
    print("-" * 90)

    if not wf_summary_path.exists():
        raise FileNotFoundError(
            f"Missing walk-forward summary: {wf_summary_path}. "
            "Run script 29 first."
        )

    wf_df = pd.read_csv(wf_summary_path)

    candidates_df = wf_df[
        wf_df["walk_forward_label"].isin(SELECTED_WF_LABELS)
    ].copy()

    print(f"Walk-forward rows: {len(wf_df):,}")
    print(f"Selected candidates: {len(candidates_df):,}")
    print("-" * 90)

    if candidates_df.empty:
        raise RuntimeError("No walk-forward strong/research candidates found.")

    records = []

    for idx, row in candidates_df.iterrows():
        candidate_records = run_importance_for_candidate(row)
        records.extend(candidate_records)

        print(
            f"[IMPORTANCE] {idx + 1:>2}/{len(candidates_df)} "
            f"{row['symbol']:<8} "
            f"{row['bar_type']:<22} "
            f"{row['parameter']:<26} "
            f"{row['target']:<16} "
            f"{row['model_name']:<24} "
            f"records={len(candidate_records)}"
        )

    importance_df = pd.DataFrame(records)
    summary_df = summarise_importance(importance_df)
    family_df = summarise_family_importance(importance_df)

    importance_csv = report_dir / "microstructure_feature_importance_latest.csv"
    summary_csv = report_dir / "microstructure_feature_importance_summary_latest.csv"
    family_csv = report_dir / "microstructure_feature_importance_family_latest.csv"
    json_path = report_dir / "microstructure_feature_importance_latest.json"
    txt_path = report_dir / "microstructure_feature_importance_latest.txt"

    importance_df.to_csv(importance_csv, index=False)
    summary_df.to_csv(summary_csv, index=False)
    family_df.to_csv(family_csv, index=False)

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "walk_forward_rows": len(wf_df),
        "selected_candidates": len(candidates_df),
        "importance_rows": len(importance_df),
        "summary_rows": len(summary_df),
        "family_rows": len(family_df),
        "top_features": summary_df.head(50).to_dict(orient="records") if not summary_df.empty else [],
        "feature_families": family_df.to_dict(orient="records") if not family_df.empty else [],
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    status_counts = importance_df["status"].value_counts(dropna=False).to_dict()

    display_cols = [
        "importance_summary_rank",
        "feature_name",
        "feature_family",
        "appearances",
        "avg_importance",
        "max_importance",
        "avg_rank",
        "best_rank",
        "symbol_count",
        "bar_type_count",
        "target_count",
        "model_count",
        "importance_score",
    ]

    available_display_cols = [c for c in display_cols if not summary_df.empty and c in summary_df.columns]

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE FEATURE IMPORTANCE RESEARCH")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Walk-forward rows:     {len(wf_df):,}")
    lines.append(f"Selected candidates:   {len(candidates_df):,}")
    lines.append(f"Importance rows:       {len(importance_df):,}")
    lines.append(f"Feature summary rows:  {len(summary_df):,}")
    lines.append(f"Family summary rows:   {len(family_df):,}")
    lines.append("")
    lines.append(f"Status counts: {status_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP FEATURE IMPORTANCE SUMMARY")
    lines.append("-" * 90)

    if summary_df.empty:
        lines.append("No feature importance summary available.")
    else:
        lines.append(summary_df[available_display_cols].head(40).to_string(index=False))

    lines.append("")
    lines.append("-" * 90)
    lines.append("FEATURE FAMILY IMPORTANCE")
    lines.append("-" * 90)

    if family_df.empty:
        lines.append("No family importance available.")
    else:
        lines.append(family_df.to_string(index=False))

    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Feature importance research complete.")
    print(f"Walk-forward rows:   {len(wf_df):,}")
    print(f"Selected candidates: {len(candidates_df):,}")
    print(f"Importance rows:     {len(importance_df):,}")
    print(f"Summary rows:        {len(summary_df):,}")
    print(f"Family rows:         {len(family_df):,}")
    print(f"Status counts:       {status_counts}")
    print(f"Importance CSV:      {importance_csv}")
    print(f"Summary CSV:         {summary_csv}")
    print(f"Family CSV:          {family_csv}")
    print(f"JSON output:         {json_path}")
    print(f"TXT output:          {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()