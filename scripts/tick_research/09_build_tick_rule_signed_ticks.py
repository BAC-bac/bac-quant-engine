"""
BACQE TICK RESEARCH - 09 Build Tick-Rule Signed Ticks - Multi Symbol

Uses the tick rule to assign direction to each tick:

    mid price rises   -> +1
    mid price falls   -> -1
    mid unchanged     -> previous non-zero direction

This creates the signed tick flow needed for Tick Imbalance Bars.

Multi-symbol version:
    - Processes all symbols listed in SYMBOLS
    - Skips missing symbols safely
    - Writes one signed tick dataset per symbol
"""

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


DATA_LAKE_ROOT = Path(r"E:\Quant_Lab")

BROKER = "FTMO"

SYMBOLS = [
    "GBPUSD",
    "EURUSD",
    "USDJPY",
    "EURGBP",
    "GBPJPY",
    "XAUUSD",
]

RAW_TICK_ROOT = DATA_LAKE_ROOT / "data" / "raw" / "ticks" / "mt5" / f"broker={BROKER}"

OUTPUT_ROOT = DATA_LAKE_ROOT / "data" / "processed" / "tick_research" / "signed_ticks"

MAX_FILES = None
# For testing:
# MAX_FILES = 100


def load_raw_ticks(symbol: str) -> pd.DataFrame:
    tick_root = RAW_TICK_ROOT / f"symbol={symbol}"
    files = sorted(tick_root.rglob("*.parquet"))

    if MAX_FILES is not None:
        files = files[:MAX_FILES]

    if not files:
        print(f"[WARN] No raw tick files found for {symbol}: {tick_root}")
        return pd.DataFrame()

    print(f"[INFO] {symbol}: raw tick files selected: {len(files):,}")

    frames = []

    for i, file_path in enumerate(files, start=1):
        try:
            df = pd.read_parquet(file_path)
        except Exception as exc:
            print(f"[WARN] {symbol}: failed to read {file_path.name}: {exc}")
            continue

        required = {"time_msc_dt", "bid", "ask", "mid", "spread"}
        missing = required - set(df.columns)

        if missing:
            print(f"[WARN] {symbol}: skipping {file_path.name}, missing: {missing}")
            continue

        frames.append(
            df[
                [
                    "time_msc_dt",
                    "bid",
                    "ask",
                    "mid",
                    "spread",
                ]
            ].copy()
        )

        if i % 500 == 0:
            print(f"[INFO] {symbol}: loaded {i:,}/{len(files):,} files")

    if not frames:
        print(f"[WARN] {symbol}: no valid tick frames loaded.")
        return pd.DataFrame()

    ticks = pd.concat(frames, ignore_index=True)

    ticks["time_msc_dt"] = pd.to_datetime(
        ticks["time_msc_dt"],
        errors="coerce",
        utc=True,
    )

    ticks = ticks.dropna(subset=["time_msc_dt", "bid", "ask", "mid"])
    ticks = ticks.sort_values("time_msc_dt")
    ticks = ticks.drop_duplicates(subset=["time_msc_dt", "bid", "ask", "mid"])
    ticks = ticks.reset_index(drop=True)

    ticks["symbol"] = symbol
    ticks["broker"] = BROKER

    return ticks


def apply_tick_rule(ticks: pd.DataFrame) -> pd.DataFrame:
    signed = ticks.copy()

    signed["price_change"] = signed["mid"].diff()

    signed["raw_tick_direction"] = 0
    signed.loc[signed["price_change"] > 0, "raw_tick_direction"] = 1
    signed.loc[signed["price_change"] < 0, "raw_tick_direction"] = -1

    signed["tick_direction"] = signed["raw_tick_direction"].replace(0, pd.NA)
    signed["tick_direction"] = signed["tick_direction"].ffill().fillna(0).astype(int)

    signed["signed_tick"] = signed["tick_direction"]
    signed["cumulative_signed_ticks"] = signed["signed_tick"].cumsum()
    signed["is_price_change_tick"] = signed["raw_tick_direction"] != 0
    signed["build_time_utc"] = datetime.now(timezone.utc).isoformat()

    return signed


def save_signed_ticks(symbol: str, signed: pd.DataFrame) -> None:
    output_dir = OUTPUT_ROOT / f"symbol={symbol}"
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir / f"{symbol}_signed_ticks_latest.parquet"
    csv_path = output_dir / f"{symbol}_signed_ticks_latest.csv"

    signed.to_parquet(parquet_path, index=False)
    signed.to_csv(csv_path, index=False)

    print(f"[DONE] {symbol}: signed tick dataset created.")
    print(f"       Rows:    {len(signed):,}")
    print(f"       Parquet: {parquet_path}")
    print(f"       CSV:     {csv_path}")


def process_symbol(symbol: str) -> dict:
    print("-" * 90)
    print(f"[SYMBOL] {symbol}")

    ticks = load_raw_ticks(symbol)

    if ticks.empty:
        return {
            "symbol": symbol,
            "status": "missing_or_empty",
            "rows": 0,
            "first_tick": None,
            "last_tick": None,
            "price_change_tick_pct": None,
            "final_cumulative_signed_ticks": None,
        }

    print(f"[INFO] {symbol}: ticks loaded after cleaning: {len(ticks):,}")
    print(f"[INFO] {symbol}: first tick: {ticks['time_msc_dt'].min()}")
    print(f"[INFO] {symbol}: last tick:  {ticks['time_msc_dt'].max()}")

    signed = apply_tick_rule(ticks)

    save_signed_ticks(symbol, signed)

    direction_counts = signed["tick_direction"].value_counts(dropna=False).to_dict()
    raw_direction_counts = signed["raw_tick_direction"].value_counts(dropna=False).to_dict()

    price_change_tick_pct = round(float(signed["is_price_change_tick"].mean() * 100), 4)
    final_cumulative_signed_ticks = int(signed["cumulative_signed_ticks"].iloc[-1])

    print(f"[INFO] {symbol}: tick direction counts after tick rule: {direction_counts}")
    print(f"[INFO] {symbol}: raw tick direction counts before forward fill: {raw_direction_counts}")
    print(f"[INFO] {symbol}: price-change tick percentage: {price_change_tick_pct}")
    print(f"[INFO] {symbol}: final cumulative signed ticks: {final_cumulative_signed_ticks}")

    return {
        "symbol": symbol,
        "status": "ok",
        "rows": len(signed),
        "first_tick": str(signed["time_msc_dt"].min()),
        "last_tick": str(signed["time_msc_dt"].max()),
        "price_change_tick_pct": price_change_tick_pct,
        "final_cumulative_signed_ticks": final_cumulative_signed_ticks,
    }


def main() -> None:
    print("=" * 90)
    print("BACQE TICK RESEARCH - 09 BUILD TICK-RULE SIGNED TICKS - MULTI SYMBOL")
    print("=" * 90)
    print(f"Broker:          {BROKER}")
    print(f"Raw tick root:   {RAW_TICK_ROOT}")
    print(f"Output root:     {OUTPUT_ROOT}")
    print(f"Symbols:         {SYMBOLS}")
    print("=" * 90)

    summary_rows = []

    for symbol in SYMBOLS:
        summary_rows.append(process_symbol(symbol))

    summary = pd.DataFrame(summary_rows)

    summary_dir = OUTPUT_ROOT / "_summary"
    summary_dir.mkdir(parents=True, exist_ok=True)

    summary_csv = summary_dir / "signed_tick_build_summary_latest.csv"
    summary_json = summary_dir / "signed_tick_build_summary_latest.json"

    summary.to_csv(summary_csv, index=False)
    summary.to_json(summary_json, orient="records", indent=2)

    print("-" * 90)
    print("[COMPLETE] Multi-symbol signed tick build complete.")
    print(f"Symbols attempted: {len(SYMBOLS)}")
    print(f"Symbols OK:        {(summary['status'] == 'ok').sum()}")
    print(f"Symbols skipped:   {(summary['status'] != 'ok').sum()}")
    print(f"Summary CSV:       {summary_csv}")
    print(f"Summary JSON:      {summary_json}")
    print("=" * 90)


if __name__ == "__main__":
    main()
