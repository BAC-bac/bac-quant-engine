from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import os

import pandas as pd
import feedparser


SOURCE = "rss"
DATASET = "financial_headline_snapshots"

RSS_FEEDS = {
    "marketwatch_topstories": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "marketwatch_marketpulse": "https://feeds.content.dowjones.io/public/rss/mw_marketpulse",
    "investing_com_news": "https://www.investing.com/rss/news.rss",
    "investing_com_forex": "https://www.investing.com/rss/news_1.rss",
    "investing_com_economy": "https://www.investing.com/rss/news_95.rss",
}


def get_data_lake_root() -> Path:
    env_path = os.getenv("DATA_LAKE_ROOT")
    if env_path:
        return Path(env_path)

    linux_path = Path("/mnt/quant_lab")
    if linux_path.exists():
        return linux_path

    raise FileNotFoundError("Could not find /mnt/quant_lab and DATA_LAKE_ROOT is not set.")


def build_output_dir(data_lake_root: Path, run_time_utc: datetime) -> Path:
    return (
        data_lake_root
        / "data"
        / "raw"
        / "information_data"
        / DATASET
        / f"source={SOURCE}"
        / f"year={run_time_utc:%Y}"
        / f"month={run_time_utc:%m}"
    )


def collect_headlines() -> pd.DataFrame:
    run_time_utc = datetime.now(timezone.utc)
    rows = []

    for feed_name, feed_url in RSS_FEEDS.items():
        print(f"[FETCH] {feed_name}")

        try:
            parsed = feedparser.parse(feed_url)

            if parsed.bozo:
                print(f"[WARN] Feed parse issue for {feed_name}: {parsed.bozo_exception}")

            for entry in parsed.entries:
                published = entry.get("published", None)
                updated = entry.get("updated", None)

                rows.append(
                    {
                        "run_time_utc": run_time_utc.isoformat(),
                        "snapshot_date": run_time_utc.date().isoformat(),
                        "source": SOURCE,
                        "feed_name": feed_name,
                        "feed_url": feed_url,
                        "headline": entry.get("title", None),
                        "summary": entry.get("summary", None),
                        "published": published,
                        "updated": updated,
                        "link": entry.get("link", None),
                        "id": entry.get("id", None),
                    }
                )

        except Exception as exc:
            print(f"[ERROR] {feed_name} failed: {exc}")

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["headline_clean"] = (
        df["headline"]
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    df["summary_clean"] = (
        df["summary"]
        .astype(str)
        .str.replace(r"<[^>]+>", "", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    df = df.drop_duplicates(subset=["feed_name", "headline_clean", "link"])

    return df


def save_outputs(df: pd.DataFrame, output_dir: Path, run_time_utc: datetime) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = run_time_utc.strftime("%Y_%m_%d_%H%M%S")

    parquet_path = output_dir / f"{DATASET}_{timestamp}.parquet"
    csv_path = output_dir / f"{DATASET}_{timestamp}.csv"

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    latest_parquet = output_dir / f"{DATASET}_latest.parquet"
    latest_csv = output_dir / f"{DATASET}_latest.csv"

    df.to_parquet(latest_parquet, index=False)
    df.to_csv(latest_csv, index=False)

    print()
    print("[DONE] Financial headline snapshot saved.")
    print(f"Rows:           {len(df):,}")
    print(f"Parquet:        {parquet_path}")
    print(f"CSV:            {csv_path}")
    print(f"Latest parquet: {latest_parquet}")
    print(f"Latest CSV:     {latest_csv}")


def main() -> None:
    print("=" * 90)
    print("BACQE INFORMATION DATA - FINANCIAL HEADLINES RSS SNAPSHOT")
    print("=" * 90)

    run_time_utc = datetime.now(timezone.utc)
    data_lake_root = get_data_lake_root()
    output_dir = build_output_dir(data_lake_root, run_time_utc)

    print(f"Data lake:  {data_lake_root}")
    print(f"Output dir: {output_dir}")
    print("-" * 90)

    df = collect_headlines()

    if df.empty:
        print("[WARN] No headlines collected.")
        return

    save_outputs(df, output_dir, run_time_utc)
    print("=" * 90)


if __name__ == "__main__":
    main()
