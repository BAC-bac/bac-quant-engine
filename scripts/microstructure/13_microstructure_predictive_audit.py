"""
BACQE MICROSTRUCTURE 13 - MICROSTRUCTURE PREDICTIVE AUDIT

Purpose:
    Run a first-pass predictive audit of microstructure features.

Method:
    For each feature dataset:
        - Load feature file
        - Select candidate numeric features
        - Test simple correlation with:
            forward_return_1
            forward_return_3
            forward_return_5

Important:
    This is NOT a trading strategy.
    This is NOT proof of alpha.
    This is a discovery audit to identify features worth deeper research.

Outputs:
    E:/Quant_Lab/data/analysis/microstructure/predictive_audit/
        microstructure_predictive_audit_latest.csv
        microstructure_predictive_audit_latest.json
        microstructure_predictive_audit_summary_latest.txt
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import yaml
import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "microstructure.yaml"


TARGET_COLUMNS = [
    "forward_return_1",
    "forward_return_3",
    "forward_return_5",
]


EXCLUDED_FEATURE_COLUMNS = {
    "symbol",
    "start_time",
    "end_time",
    "bar_type",
    "parameter_name",
    "parameter_value",
    "feature_created_at_utc",
    "created_at_utc",
    "bar_id",
    "tick_size",
    "volume_threshold",
    "imbalance_threshold",
    "is_partial_bar",
    *TARGET_COLUMNS,
}


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


def get_feature_ranking_path(analysis_dir: Path) -> Path:
    return (
        analysis_dir
        / "feature_ranking"
        / "microstructure_feature_ranking_latest.csv"
    )


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


def safe_corr(x: pd.Series, y: pd.Series) -> tuple[float | None, int]:
    pair = pd.concat([x, y], axis=1).replace([np.inf, -np.inf], np.nan).dropna()

    sample_size = len(pair)

    if sample_size < 30:
        return None, sample_size

    if pair.iloc[:, 0].nunique() <= 1 or pair.iloc[:, 1].nunique() <= 1:
        return None, sample_size

    corr = pair.iloc[:, 0].corr(pair.iloc[:, 1])

    if pd.isna(corr):
        return None, sample_size

    return float(corr), sample_size


def signal_strength(abs_corr: float | None, sample_size: int) -> str:
    if abs_corr is None:
        return "insufficient"

    if sample_size < 100:
        return "low_sample"

    if abs_corr >= 0.10 and sample_size >= 1000:
        return "strong_watchlist"

    if abs_corr >= 0.05 and sample_size >= 500:
        return "research_watchlist"

    if abs_corr >= 0.025 and sample_size >= 250:
        return "weak_watchlist"

    return "low_signal"


def audit_feature_file(
    file_path: Path,
    dataset_record: pd.Series,
    ranked_feature_names: set[str] | None = None,
) -> list[dict]:
    if not file_path.exists():
        return []

    try:
        df = pd.read_parquet(file_path)
    except Exception:
        return []

    if df.empty:
        return []

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    candidate_features = [
        col for col in numeric_cols
        if col not in EXCLUDED_FEATURE_COLUMNS
        and col not in TARGET_COLUMNS
    ]

    if ranked_feature_names is not None:
        candidate_features = [
            col for col in candidate_features
            if col in ranked_feature_names
        ]

    records = []

    for feature_name in candidate_features:
        feature_series = pd.to_numeric(df[feature_name], errors="coerce")

        for target in TARGET_COLUMNS:
            if target not in df.columns:
                continue

            target_series = pd.to_numeric(df[target], errors="coerce")
            corr, sample_size = safe_corr(feature_series, target_series)
            abs_corr = abs(corr) if corr is not None else None

            record = {
                "checked_at_utc": datetime.now(timezone.utc).isoformat(),
                "symbol": dataset_record.get("symbol"),
                "bar_type": dataset_record.get("bar_type"),
                "parameter": dataset_record.get("parameter"),
                "dataset_quality_label": dataset_record.get("quality_label"),
                "dataset_quality_score": dataset_record.get("dataset_quality_score", dataset_record.get("quality_score")),
                "file_path": str(file_path),
                "feature_name": feature_name,
                "feature_family": classify_feature_family(feature_name),
                "target": target,
                "correlation": corr,
                "abs_correlation": abs_corr,
                "sample_size": sample_size,
                "signal_strength": signal_strength(abs_corr, sample_size),
            }

            records.append(record)

    return records


def main() -> None:
    print_header("BACQE MICROSTRUCTURE 13 - MICROSTRUCTURE PREDICTIVE AUDIT")

    config = load_config()
    micro_cfg = config["microstructure"]

    analysis_dir = get_analysis_dir(micro_cfg)
    feature_ranking_path = get_feature_ranking_path(analysis_dir)

    report_dir = analysis_dir / "predictive_audit"
    report_dir.mkdir(parents=True, exist_ok=True)

    print(f"Config:          {CONFIG_PATH}")
    print(f"Feature ranking: {feature_ranking_path}")
    print(f"Report dir:      {report_dir}")
    print("-" * 90)

    if not feature_ranking_path.exists():
        raise FileNotFoundError(
            f"Missing feature ranking file: {feature_ranking_path}. "
            "Run script 12 first."
        )

    ranking_df = pd.read_csv(feature_ranking_path)

    usable_feature_labels = {"excellent", "good", "usable"}

    candidate_ranking = ranking_df[
        (ranking_df["feature_role"] == "candidate_feature")
        & (ranking_df["usability_label"].isin(usable_feature_labels))
    ].copy()

    dataset_cols = [
        "symbol",
        "bar_type",
        "parameter",
        "quality_label",
        "dataset_quality_score",
        "quality_score",
        "file_path",
    ]

    available_dataset_cols = [
        c for c in dataset_cols if c in candidate_ranking.columns
    ]

    datasets_df = (
        candidate_ranking[available_dataset_cols]
        .drop_duplicates(subset=["file_path"])
        .reset_index(drop=True)
    )

    print(f"Ranked feature records:       {len(ranking_df):,}")
    print(f"Candidate feature records:    {len(candidate_ranking):,}")
    print(f"Datasets to audit:            {len(datasets_df):,}")
    print("-" * 90)

    all_records = []

    for _, dataset_record in datasets_df.iterrows():
        file_path = Path(dataset_record["file_path"])

        ranked_features_for_file = set(
            candidate_ranking[
                candidate_ranking["file_path"] == str(file_path)
            ]["feature_name"].unique()
        )

        records = audit_feature_file(
            file_path=file_path,
            dataset_record=dataset_record,
            ranked_feature_names=ranked_features_for_file,
        )

        all_records.extend(records)

        print(
            f"[AUDIT] {dataset_record.get('symbol'):<8} "
            f"{dataset_record.get('bar_type'):<22} "
            f"{dataset_record.get('parameter'):<28} "
            f"tests={len(records):,}"
        )

    audit_df = pd.DataFrame(all_records)

    if audit_df.empty:
        raise RuntimeError("No predictive audit records were created.")

    audit_df = audit_df.sort_values(
        ["abs_correlation", "sample_size"],
        ascending=[False, False],
        na_position="last",
    )

    csv_path = report_dir / "microstructure_predictive_audit_latest.csv"
    json_path = report_dir / "microstructure_predictive_audit_latest.json"
    txt_path = report_dir / "microstructure_predictive_audit_summary_latest.txt"

    audit_df.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(audit_df.to_dict(orient="records"), f, indent=2, default=str)

    signal_counts = audit_df["signal_strength"].value_counts(dropna=False).to_dict()
    family_counts = audit_df["feature_family"].value_counts(dropna=False).to_dict()
    target_counts = audit_df["target"].value_counts(dropna=False).to_dict()

    top_overall = audit_df.head(30)[
        [
            "symbol",
            "bar_type",
            "parameter",
            "feature_name",
            "feature_family",
            "target",
            "correlation",
            "abs_correlation",
            "sample_size",
            "signal_strength",
        ]
    ]

    watchlist_df = audit_df[
        audit_df["signal_strength"].isin(
            ["strong_watchlist", "research_watchlist", "weak_watchlist"]
        )
    ].copy()

    top_watchlist = watchlist_df.head(50)[
        [
            "symbol",
            "bar_type",
            "parameter",
            "feature_name",
            "feature_family",
            "target",
            "correlation",
            "abs_correlation",
            "sample_size",
            "signal_strength",
        ]
    ] if not watchlist_df.empty else pd.DataFrame()

    lines = []
    lines.append("=" * 90)
    lines.append("BACQE MICROSTRUCTURE PREDICTIVE AUDIT SUMMARY")
    lines.append("=" * 90)
    lines.append(f"Created at UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append(f"Predictive tests run: {len(audit_df):,}")
    lines.append(f"Signal counts: {signal_counts}")
    lines.append(f"Feature family counts: {family_counts}")
    lines.append(f"Target counts: {target_counts}")
    lines.append("")
    lines.append("-" * 90)
    lines.append("TOP 30 ABSOLUTE CORRELATION RESULTS")
    lines.append("-" * 90)
    lines.append(top_overall.to_string(index=False))
    lines.append("")
    lines.append("-" * 90)
    lines.append("WATCHLIST RESULTS")
    lines.append("-" * 90)

    if top_watchlist.empty:
        lines.append("No watchlist results detected using current thresholds.")
    else:
        lines.append(top_watchlist.to_string(index=False))

    lines.append("")
    lines.append("=" * 90)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("-" * 90)
    print("[DONE] Microstructure predictive audit complete.")
    print(f"Predictive tests run: {len(audit_df):,}")
    print(f"Signal counts:        {signal_counts}")
    print(f"Family counts:        {family_counts}")
    print(f"Target counts:        {target_counts}")
    print(f"Watchlist rows:       {len(watchlist_df):,}")
    print(f"CSV output:           {csv_path}")
    print(f"JSON output:          {json_path}")
    print(f"TXT output:           {txt_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()