from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SIGNALS_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_signals_v4.csv"
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_backtest_v4_local_d1.csv"

PRICE_DIR = Path(r"E:\BAC_Quant_Universe\data\mt5_ohlcv\FTMO\D1")


def find_price_file(pair: str) -> Path | None:
    matches = list(PRICE_DIR.glob(f"*{pair}*.parquet"))
    return matches[0] if matches else None


def signal_to_direction(signal: str) -> int:
    if "BUY" in signal:
        return 1
    if "SELL" in signal:
        return -1
    return 0


def load_price_data(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)

    df.columns = df.columns.str.lower().str.strip()

    time_candidates = ["time", "datetime", "date", "timestamp"]
    time_col = next((col for col in time_candidates if col in df.columns), None)

    if time_col is not None:
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df = df.sort_values(time_col)

    if "close" not in df.columns:
        raise ValueError(f"No close column found in {path}")

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])

    df["return_1d"] = df["close"].pct_change()
    df["return_5d"] = df["close"].pct_change(5)
    df["return_20d"] = df["close"].pct_change(20)
    df["return_60d"] = df["close"].pct_change(60)

    return df


def main() -> None:
    signals = pd.read_csv(SIGNALS_FILE)

    results = []

    for _, row in signals.iterrows():
        pair = row["pair"]
        signal = row["signal_v4"]
        direction = signal_to_direction(signal)

        if direction == 0:
            print(f"[SKIP] {pair}: neutral signal")
            continue

        price_file = find_price_file(pair)

        if price_file is None:
            print(f"[WARN] No D1 parquet price file found for {pair}")
            continue

        df = load_price_data(price_file)
        latest = df.iloc[-1]

        results.append(
            {
                "pair": pair,
                "signal_v4": signal,
                "direction": direction,
                "price_file": str(price_file),
                "latest_close": latest["close"],
                "return_1d": latest["return_1d"],
                "return_5d": latest["return_5d"],
                "return_20d": latest["return_20d"],
                "return_60d": latest["return_60d"],
                "signal_pnl_1d": direction * latest["return_1d"],
                "signal_pnl_5d": direction * latest["return_5d"],
                "signal_pnl_20d": direction * latest["return_20d"],
                "signal_pnl_60d": direction * latest["return_60d"],
            }
        )

    output = pd.DataFrame(results)

    print("\nLocal D1 FX macro signal snapshot validation:")
    print(output.to_string(index=False))

    if not output.empty:
        print("\nSummary:")
        print(f"Total signal PnL 1D:  {output['signal_pnl_1d'].sum():.6f}")
        print(f"Total signal PnL 5D:  {output['signal_pnl_5d'].sum():.6f}")
        print(f"Total signal PnL 20D: {output['signal_pnl_20d'].sum():.6f}")
        print(f"Total signal PnL 60D: {output['signal_pnl_60d'].sum():.6f}")

    output.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved local D1 FX backtest to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()