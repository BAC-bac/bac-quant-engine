"""
BACQE DUKASCOPY 13 - PREPARE REPLAY SPEC

Purpose:
    Convert the EURUSD candidate validation inventory into a clean replay
    specification for Dukascopy historical TIB replay.

Input:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_ticks\\candidate_replay_prep\\eurusd_candidate_inventory_from_validation_review.csv

Output:
    E:\\Quant_Lab\\data\\analysis\\dukascopy_ticks\\candidate_replay_prep\\eurusd_primary_replay_spec.csv
"""

from pathlib import Path

import pandas as pd


INPUT_PATH = Path(
    r"E:\Quant_Lab\data\analysis\dukascopy_ticks\candidate_replay_prep"
    r"\eurusd_candidate_inventory_from_validation_review.csv"
)

OUTPUT_DIR = Path(
    r"E:\Quant_Lab\data\analysis\dukascopy_ticks\candidate_replay_prep"
)

OUTPUT_PATH = OUTPUT_DIR / "eurusd_primary_replay_spec.csv"

SYMBOL = "EURUSD"
PRIMARY_LABEL = "validation_pass_primary"


def split_csv_field(value) -> list[str]:
    if pd.isna(value):
        return []

    return [
        item.strip()
        for item in str(value).split(",")
        if item.strip()
    ]


def parse_threshold_pairs(value) -> list[dict]:
    pairs = []

    for item in split_csv_field(value):
        try:
            buy_str, sell_str = item.split("_")
            pairs.append({
                "threshold_pair": item,
                "buy_threshold": float(buy_str),
                "sell_threshold": float(sell_str),
            })
        except ValueError:
            pairs.append({
                "threshold_pair": item,
                "buy_threshold": None,
                "sell_threshold": None,
            })

    return pairs


def build_replay_spec(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    primary_df = df[
        df["validation_label"].astype(str).str.lower() == PRIMARY_LABEL
    ].copy()

    for _, row in primary_df.iterrows():
        sessions = split_csv_field(row.get("sessions"))
        weekdays = split_csv_field(row.get("weekdays"))
        threshold_pairs = parse_threshold_pairs(row.get("threshold_pairs"))

        for threshold_pair in threshold_pairs:
            rows.append({
                "replay_id": (
                    f"{row['filter_name']}__"
                    f"rank_{int(row['validation_rank'])}__"
                    f"cost_{row['cost_per_trade']}__"
                    f"{threshold_pair['threshold_pair']}"
                ),
                "symbol": SYMBOL,
                "validation_rank": row["validation_rank"],
                "validation_label": row["validation_label"],
                "filter_name": row["filter_name"],
                "registry_label": row["registry_label"],
                "registry_rank": row["registry_rank"],
                "forensic_health_label": row["forensic_health_label"],
                "cost_per_trade": row["cost_per_trade"],
                "original_trade_count": row["trade_count"],
                "original_net_win_rate": row["net_win_rate"],
                "original_net_avg_return": row["net_avg_return"],
                "original_net_profit_factor": row["net_profit_factor"],
                "original_net_sharpe_like": row["net_sharpe_like"],
                "original_max_drawdown": row["max_drawdown"],
                "sessions": ",".join(sessions),
                "weekdays": ",".join(weekdays),
                "threshold_pair": threshold_pair["threshold_pair"],
                "buy_threshold": threshold_pair["buy_threshold"],
                "sell_threshold": threshold_pair["sell_threshold"],
                "source_inventory": str(INPUT_PATH),
            })

    return pd.DataFrame(rows)


def main() -> None:
    print("=" * 90)
    print("BACQE DUKASCOPY 13 - PREPARE REPLAY SPEC")
    print("=" * 90)

    if not INPUT_PATH.exists():
        print(f"[ERROR] Input file missing: {INPUT_PATH}")
        return

    df = pd.read_csv(INPUT_PATH)

    print(f"Loaded inventory rows: {len(df):,}")
    print(f"Input: {INPUT_PATH}")

    spec_df = build_replay_spec(df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    spec_df.to_csv(OUTPUT_PATH, index=False)

    print("-" * 90)
    print("[SUMMARY]")
    print(f"Primary candidate rows: {(df['validation_label'] == PRIMARY_LABEL).sum()}")
    print(f"Replay spec rows:       {len(spec_df):,}")

    if not spec_df.empty:
        print("\n[BY FILTER]")
        print(spec_df["filter_name"].value_counts().to_string())

        print("\n[BY COST]")
        print(spec_df["cost_per_trade"].value_counts().sort_index().to_string())

        print("\n[BY THRESHOLD PAIR]")
        print(spec_df["threshold_pair"].value_counts().sort_index().to_string())

    print("-" * 90)
    print(f"Output: {OUTPUT_PATH}")
    print("[DONE] Replay spec prepared.")


if __name__ == "__main__":
    main()