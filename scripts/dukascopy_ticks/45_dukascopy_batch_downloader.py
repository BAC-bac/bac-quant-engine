"""
BACQE DUKASCOPY 45 - BATCH DOWNLOADER + NORMALISER
"""

from pathlib import Path
import importlib.util
import pandas as pd
import sys
import yaml

from dukascopy_contract import get_symbol_metadata


CONFIG_PATH = Path("config/dukascopy_research.yaml")

BATCH_PLAN_PATH = Path(
    r"E:\Quant_Lab\data\analysis\dukascopy_download_plan\plans\dukascopy_batch_download_plan_latest.csv"
)


def banner(title: str) -> None:
    print("=" * 90)
    print(title)
    print("=" * 90)


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["dukascopy_research"]


def import_script_function(script_path: Path, function_name: str):
    spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    if not hasattr(module, function_name):
        raise AttributeError(f"{script_path} missing function: {function_name}")

    return getattr(module, function_name)


def ensure_output_dirs(analysis_root: Path) -> Path:
    output_root = analysis_root / "dukascopy_batch_downloader"

    for folder in [
        output_root,
        output_root / "batch_reports",
        output_root / "reports",
    ]:
        folder.mkdir(parents=True, exist_ok=True)

    return output_root


def main() -> None:
    banner("BACQE DUKASCOPY 45 - BATCH DOWNLOADER + NORMALISER")

    cfg = load_config()
    analysis_root = Path(cfg["paths"]["analysis_root"])
    output_root = ensure_output_dirs(analysis_root)

    if not BATCH_PLAN_PATH.exists():
        print(f"[STOP] Missing batch plan: {BATCH_PLAN_PATH}")
        return

    plan = pd.read_csv(BATCH_PLAN_PATH)

    plan = plan[
        (plan["download_stage"] == "processed_ticks")
        & (plan["plan_status"] == "planned")
    ].copy()

    plan["date"] = pd.to_datetime(plan["date"], errors="coerce")
    plan = plan.dropna(subset=["date"])
    plan = plan.sort_values(["symbol", "date"])
    plan["date"] = plan["date"].dt.strftime("%Y-%m-%d")

    print(f"Batch plan rows: {len(plan):,}")
    print(f"Output root:     {output_root}")
    print("-" * 90)

    if plan.empty:
        print("[STOP] No planned rows found.")
        return

    download_func = import_script_function(
        Path("scripts/dukascopy_ticks/07_download_dukascopy_date_range.py"),
        "run_download",
    )

    normalise_func = import_script_function(
        Path("scripts/dukascopy_ticks/08_normalise_dukascopy_date_range.py"),
        "run_normalisation",
    )

    rows = []

    for i, row in enumerate(plan.itertuples(index=False), start=1):
        symbol = get_symbol_metadata(row.symbol).symbol
        date = row.date

        print("=" * 90)
        print(f"[BATCH {i}/{len(plan)}] {symbol} {date}")
        print("=" * 90)

        download_status = "not_run"
        normalise_status = "not_run"
        download_report = ""
        normalise_report = ""
        error = ""

        try:
            download_report = download_func(
                symbol=symbol,
                start_date=date,
                end_date=date,
            )
            download_status = "ok"
        except Exception as exc:
            download_status = "error"
            error = f"download_error={repr(exc)}"
            print(f"[ERROR] Download failed for {symbol} {date}: {exc}")

        if download_status == "ok":
            try:
                normalise_report = normalise_func(
                    symbol=symbol,
                    start_date=date,
                    end_date=date,
                )
                normalise_status = "ok"
            except Exception as exc:
                normalise_status = "error"
                error = f"{error}; normalise_error={repr(exc)}"
                print(f"[ERROR] Normalisation failed for {symbol} {date}: {exc}")

        rows.append({
            "symbol": symbol,
            "date": date,
            "download_status": download_status,
            "normalise_status": normalise_status,
            "download_report": str(download_report),
            "normalise_report": str(normalise_report),
            "error": error,
        })

    results = pd.DataFrame(rows)

    batch_report_path = output_root / "batch_reports" / "dukascopy_batch_downloader_latest.csv"
    text_report_path = output_root / "reports" / "dukascopy_batch_downloader_report_latest.txt"

    results.to_csv(batch_report_path, index=False)

    with open(text_report_path, "w", encoding="utf-8") as f:
        f.write("BACQE DUKASCOPY BATCH DOWNLOADER REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Batch plan: {BATCH_PLAN_PATH}\n")
        f.write(f"Rows attempted: {len(results):,}\n\n")

        f.write("Status Counts\n")
        f.write("-" * 80 + "\n")
        f.write(
            results.groupby(["download_status", "normalise_status"])
            .size()
            .reset_index(name="count")
            .to_string(index=False)
        )

        f.write("\n\nResults Preview\n")
        f.write("-" * 80 + "\n")
        f.write(results.head(50).to_string(index=False))

        f.write("\n\nOutputs:\n")
        f.write(f"Batch report: {batch_report_path}\n")

    print("=" * 90)
    print("[DONE] Batch downloader complete.")
    print(f"Batch report: {batch_report_path}")
    print(f"Text report:  {text_report_path}")
    print("=" * 90)


if __name__ == "__main__":
    main()
