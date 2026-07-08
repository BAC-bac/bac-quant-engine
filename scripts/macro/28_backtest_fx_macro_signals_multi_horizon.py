from pathlib import Path
import platform
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]

def select_existing_path(candidates: list[str]) -> Path:
    for candidate in candidates:
        if candidate is None:
            continue

        path = Path(candidate)
        if path.exists():
            return path

    return Path(next(candidate for candidate in candidates if candidate is not None))


def load_ftmo_d1_price_dir() -> Path:
    config_file = PROJECT_ROOT / "config" / "paths.yaml"

    with config_file.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    paths = config["market_data"]["mt5_ohlcv"]["ftmo_d1"]

    if platform.system().lower() == "windows":
        return select_existing_path(
            [
                paths.get("windows_network"),
                paths.get("windows_local"),
            ]
        )

    return Path(paths["linux"])

SIGNALS_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_signals_v4.csv"
PRICE_DIR = load_ftmo_d1_price_dir()
OUTPUT_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_backtest_v4_multi_horizon_d1.csv"
SUMMARY_FILE = PROJECT_ROOT / "macro_data" / "processed" / "fx_backtest_v4_multi_horizon_d1_summary.csv"


HORIZONS = {
    "1d": 1,
    "5d": 5,
    "20d": 20,
    "60d": 60,
    "120d": 120,
    "252d": 252,
}


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

    if time_col is None:
        raise ValueError(f"No datetime column found in {path}")

    if "close" not in df.columns:
        raise ValueError(f"No close column found in {path}")

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=[time_col, "close"]).sort_values(time_col)
    df = df.rename(columns={time_col: "datetime"})

    return df[["datetime", "close"]].copy()


def run_pair_backtest(pair: str, signal: str, direction: int, price_file: Path) -> pd.DataFrame:
    df = load_price_data(price_file)

    results = []

    for horizon_name, horizon_bars in HORIZONS.items():
        temp = df.copy()

        temp["future_close"] = temp["close"].shift(-horizon_bars)
        temp["future_return"] = (temp["future_close"] / temp["close"]) - 1
        temp["signal_return"] = direction * temp["future_return"]

        temp = temp.dropna(subset=["future_return", "signal_return"])

        temp["pair"] = pair
        temp["signal_v4"] = signal
        temp["direction"] = direction
        temp["horizon"] = horizon_name
        temp["horizon_bars"] = horizon_bars
        temp["price_file"] = str(price_file)

        results.append(
            temp[
                [
                    "pair",
                    "datetime",
                    "signal_v4",
                    "direction",
                    "horizon",
                    "horizon_bars",
                    "close",
                    "future_close",
                    "future_return",
                    "signal_return",
                    "price_file",
                ]
            ]
        )

    if not results:
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)


def summarise_results(results: pd.DataFrame) -> pd.DataFrame:
    if results.empty:
        return pd.DataFrame()

    summary = (
        results.groupby(["pair", "signal_v4", "direction", "horizon", "horizon_bars"])
        .agg(
            trades=("signal_return", "count"),
            win_rate=("signal_return", lambda x: (x > 0).mean()),
            avg_signal_return=("signal_return", "mean"),
            median_signal_return=("signal_return", "median"),
            total_signal_return=("signal_return", "sum"),
            best_signal_return=("signal_return", "max"),
            worst_signal_return=("signal_return", "min"),
            avg_raw_future_return=("future_return", "mean"),
        )
        .reset_index()
    )

    summary["simple_score"] = summary["win_rate"] * summary["avg_signal_return"]

    return summary.sort_values(["horizon_bars", "total_signal_return"], ascending=[True, False])


def main() -> None:
    signals = pd.read_csv(SIGNALS_FILE)

    all_results = []

    for _, row in signals.iterrows():
        pair = row["pair"]
        signal = row["signal_v4"]
        direction = signal_to_direction(signal)

        if direction == 0:
            print(f"[SKIP] {pair}: neutral signal")
            continue

        price_file = find_price_file(pair)

        if price_file is None:
            print(f"[WARN] {pair}: no D1 parquet price file found")
            continue

        print(f"[RUN] {pair}: {signal} using {price_file}")

        pair_results = run_pair_backtest(
            pair=pair,
            signal=signal,
            direction=direction,
            price_file=price_file,
        )

        all_results.append(pair_results)

    if not all_results:
        print("\nNo D1 backtest results generated.")
        return

    results = pd.concat(all_results, ignore_index=True)
    summary = summarise_results(results)

    results.to_csv(OUTPUT_FILE, index=False)
    summary.to_csv(SUMMARY_FILE, index=False)

    print(f"\nSaved detailed D1 results to: {OUTPUT_FILE}")
    print(f"Saved summary D1 results to:  {SUMMARY_FILE}")

    print("\nD1 summary preview:")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()