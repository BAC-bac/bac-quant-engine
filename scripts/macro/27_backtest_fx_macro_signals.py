from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SIGNALS_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_signals_v4.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_backtest_v4_local_h1.csv"

PRICE_DIR = Path(r"E:\BAC_Quant_Universe\data\mt5_ohlcv\FTMO\H1")


PAIR_FILES = {
    "GBPUSD": "GBPUSD.parquet",
    "EURUSD": "EURUSD.parquet",
    "USDJPY": "USDJPY.parquet",
    "EURGBP": "EURGBP.parquet",
    "EURJPY": "EURJPY.parquet",
    "GBPJPY": "GBPJPY.parquet",
}


def find_price_file(pair: str) -> Path | None:
    expected = PRICE_DIR / PAIR_FILES[pair]

    if expected.exists():
        return expected

    matches = list(PRICE_DIR.glob(f"*{pair}*.parquet"))

    if matches:
        return matches[0]

    return None


def load_price_data(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)

    df.columns = df.columns.str.lower().str.strip()

    possible_time_cols = ["time", "datetime", "date", "timestamp"]
    time_col = next((col for col in possible_time_cols if col in df.columns), None)

    if time_col is not None:
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df = df.sort_values(time_col)

    if "close" not in df.columns:
        raise ValueError(f"No close column found in {path}")

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])

    df["return_1h"] = df["close"].pct_change()
    df["return_24h"] = df["close"].pct_change(24)
    df["return_120h"] = df["close"].pct_change(120)

    return df


def signal_to_direction(signal: str) -> int:
    if "BUY" in signal:
        return 1
    if "SELL" in signal:
        return -1
    return 0


def main() -> None:
    signals = pd.read_csv(SIGNALS_FILE)

    results = []

    for _, row in signals.iterrows():
        pair = row["pair"]

        if pair not in PAIR_FILES:
            print(f"[SKIP] No local file mapping for {pair}")
            continue

        price_file = find_price_file(pair)

        if price_file is None:
            print(f"[WARN] No price file found for {pair}")
            continue

        df = load_price_data(price_file)

        latest = df.iloc[-1]

        direction = signal_to_direction(row["signal_v4"])

        pnl_1h = direction * latest["return_1h"]
        pnl_24h = direction * latest["return_24h"]
        pnl_120h = direction * latest["return_120h"]

        results.append(
            {
                "pair": pair,
                "signal_v4": row["signal_v4"],
                "direction": direction,
                "price_file": str(price_file),
                "latest_close": latest["close"],
                "return_1h": latest["return_1h"],
                "return_24h": latest["return_24h"],
                "return_120h": latest["return_120h"],
                "signal_pnl_1h": pnl_1h,
                "signal_pnl_24h": pnl_24h,
                "signal_pnl_120h": pnl_120h,
            }
        )

    output = pd.DataFrame(results)

    print("\nLocal FX macro signal backtest:")
    print(output.to_string(index=False))

    if not output.empty:
        print("\nSummary:")
        print(f"Total signal PnL 1H:   {output['signal_pnl_1h'].sum():.6f}")
        print(f"Total signal PnL 24H:  {output['signal_pnl_24h'].sum():.6f}")
        print(f"Total signal PnL 120H: {output['signal_pnl_120h'].sum():.6f}")

    output.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved local FX backtest to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()