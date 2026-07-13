from __future__ import annotations

import os
import platform
from datetime import datetime, timezone
from pathlib import Path
import certifi
import requests
import feedparser
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "paths.yaml"

SOURCE = "rss"
DATASET = "financial_headline_snapshots"

RSS_FEEDS = {
    "marketwatch_topstories": (
        "https://feeds.content.dowjones.io/public/rss/mw_topstories"
    ),
    "marketwatch_marketpulse": (
        "https://feeds.content.dowjones.io/public/rss/mw_marketpulse"
    ),
    "investing_com_news": (
        "https://www.investing.com/rss/news.rss"
    ),
    "investing_com_forex": (
        "https://www.investing.com/rss/news_1.rss"
    ),
    "investing_com_economy": (
        "https://www.investing.com/rss/news_95.rss"
    ),
}


def load_config() -> dict:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f"Could not find configuration file: {CONFIG_FILE}"
        )

    with CONFIG_FILE.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(
            f"Invalid YAML configuration: {CONFIG_FILE}"
        )

    return config


def select_existing_path(
    candidates: list[str | None],
) -> Path:
    valid_candidates = [
        candidate
        for candidate in candidates
        if candidate
    ]

    if not valid_candidates:
        raise ValueError(
            "No Data Lake path candidates were configured."
        )

    for candidate in valid_candidates:
        path = Path(candidate)

        if path.exists():
            return path

    raise FileNotFoundError(
        "None of the configured Data Lake paths exists: "
        + ", ".join(valid_candidates)
    )


def get_data_lake_root() -> Path:
    env_path = os.getenv("DATA_LAKE_ROOT")

    if env_path:
        environment_root = Path(env_path)

        if environment_root.exists():
            return environment_root

        print(
            "[WARN] DATA_LAKE_ROOT is set but does not exist: "
            f"{environment_root}"
        )

    config = load_config()

    if "data_lake_root" not in config:
        raise KeyError(
            f"'data_lake_root' is missing from {CONFIG_FILE}"
        )

    paths = config["data_lake_root"]

    if platform.system().lower() == "windows":
        return select_existing_path(
            [
                paths.get("windows_network"),
                paths.get("windows_local"),
                paths.get("windows"),
            ]
        )

    linux_path = paths.get("linux")

    if not linux_path:
        raise KeyError(
            "'data_lake_root.linux' is missing from config/paths.yaml"
        )

    resolved_linux_path = Path(linux_path)

    if not resolved_linux_path.exists():
        raise FileNotFoundError(
            "Configured Linux Data Lake does not exist: "
            f"{resolved_linux_path}"
        )

    return resolved_linux_path


def build_output_dir(
    data_lake_root: Path,
    run_time_utc: datetime,
) -> Path:
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


def clean_text(series: pd.Series) -> pd.Series:
    return (
        series
        .fillna("")
        .astype(str)
        .str.replace(r"<[^>]+>", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .replace("", pd.NA)
    )


def parse_feed(
    feed_name: str,
    feed_url: str,
    run_time_utc: datetime,
) -> list[dict]:
    print(f"[FETCH] {feed_name:<28} -> {feed_url}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(compatible; BACQEFinancialHeadlinesCollector/1.0)"
        )
    }

    try:
        response = requests.get(
            feed_url,
            headers=headers,
            timeout=30,
            verify=certifi.where(),
        )
        response.raise_for_status()

    except requests.RequestException as exc:
        print(f"[WARN] Feed request failed for {feed_name}: {exc}")
        return []

    parsed = feedparser.parse(response.content)

    if parsed.bozo:
        print(
            f"[WARN] Feed parse issue for {feed_name}: "
            f"{parsed.bozo_exception}"
        )

    entries = getattr(parsed, "entries", [])

    if not entries:
        print(f"[WARN] No entries returned for {feed_name}")
        return []

    rows: list[dict] = []

    for entry in entries:
        rows.append(
            {
                "run_time_utc": run_time_utc.isoformat(),
                "snapshot_date": run_time_utc.date().isoformat(),
                "source": SOURCE,
                "feed_name": feed_name,
                "feed_url": feed_url,
                "headline": entry.get("title"),
                "summary": entry.get("summary"),
                "published": entry.get("published"),
                "updated": entry.get("updated"),
                "link": entry.get("link"),
                "entry_id": entry.get("id"),
            }
        )

    print(f"[OK] {feed_name}: {len(rows):,} entries")

    return rows


def collect_headlines(
    run_time_utc: datetime,
) -> pd.DataFrame:
    rows: list[dict] = []

    for feed_name, feed_url in RSS_FEEDS.items():
        rows.extend(
            parse_feed(
                feed_name=feed_name,
                feed_url=feed_url,
                run_time_utc=run_time_utc,
            )
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    df["headline_clean"] = clean_text(df["headline"])
    df["summary_clean"] = clean_text(df["summary"])

    df["published_utc"] = pd.to_datetime(
        df["published"],
        errors="coerce",
        utc=True,
    )

    df["updated_utc"] = pd.to_datetime(
        df["updated"],
        errors="coerce",
        utc=True,
    )

    df = df.dropna(
        subset=["headline_clean"]
    ).copy()

    duplicate_key = [
        "feed_name",
        "headline_clean",
        "link",
    ]

    before_deduplication = len(df)

    df = (
        df
        .drop_duplicates(
            subset=duplicate_key,
            keep="first",
        )
        .reset_index(drop=True)
    )

    removed_duplicates = (
        before_deduplication - len(df)
    )

    if removed_duplicates:
        print(
            f"[INFO] Removed {removed_duplicates:,} duplicate headlines."
        )

    preferred_columns = [
        "run_time_utc",
        "snapshot_date",
        "source",
        "feed_name",
        "feed_url",
        "headline",
        "headline_clean",
        "summary",
        "summary_clean",
        "published",
        "published_utc",
        "updated",
        "updated_utc",
        "link",
        "entry_id",
    ]

    existing_columns = [
        column
        for column in preferred_columns
        if column in df.columns
    ]

    df = df[existing_columns]

    sort_columns = [
        column
        for column in [
            "published_utc",
            "feed_name",
            "headline_clean",
        ]
        if column in df.columns
    ]

    if sort_columns:
        df = (
            df
            .sort_values(
                sort_columns,
                ascending=[False, True, True],
                na_position="last",
            )
            .reset_index(drop=True)
        )

    return df


def validate_headlines(
    df: pd.DataFrame,
) -> None:
    required_columns = {
        "run_time_utc",
        "source",
        "feed_name",
        "headline_clean",
        "link",
    }

    missing_columns = required_columns.difference(
        df.columns
    )

    if missing_columns:
        raise ValueError(
            "Headline output is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError(
            "Headline output contains no rows."
        )

    if df["headline_clean"].isna().all():
        raise ValueError(
            "Headline output contains no usable headline text."
        )

    duplicate_count = df.duplicated(
        subset=[
            "feed_name",
            "headline_clean",
            "link",
        ],
        keep=False,
    ).sum()

    if duplicate_count:
        raise ValueError(
            "Headline output still contains "
            f"{duplicate_count:,} duplicate-key rows."
        )

    current_time = pd.Timestamp.now(tz="UTC")

    if "published_utc" in df.columns:
        future_rows = df[
            df["published_utc"]
            > current_time + pd.Timedelta(days=1)
        ]

        if not future_rows.empty:
            print(
                "[WARN] Headline output contains "
                f"{len(future_rows):,} future-dated publication rows."
            )


def save_outputs(
    df: pd.DataFrame,
    output_dir: Path,
    run_time_utc: datetime,
) -> None:
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    timestamp = run_time_utc.strftime(
        "%Y_%m_%d_%H%M%S"
    )

    parquet_path = (
        output_dir
        / f"{DATASET}_{timestamp}.parquet"
    )

    csv_path = (
        output_dir
        / f"{DATASET}_{timestamp}.csv"
    )

    latest_parquet = (
        output_dir
        / f"{DATASET}_latest.parquet"
    )

    latest_csv = (
        output_dir
        / f"{DATASET}_latest.csv"
    )

    df.to_parquet(
        parquet_path,
        index=False,
    )

    df.to_csv(
        csv_path,
        index=False,
    )

    df.to_parquet(
        latest_parquet,
        index=False,
    )

    df.to_csv(
        latest_csv,
        index=False,
    )

    feed_counts = (
        df["feed_name"]
        .value_counts()
        .sort_index()
    )

    print()
    print("[DONE] Financial headline snapshot saved.")
    print(f"Rows:           {len(df):,}")
    print(f"Feeds:          {df['feed_name'].nunique():,}")

    if "published_utc" in df.columns:
        valid_published = df["published_utc"].dropna()

        if not valid_published.empty:
            print(
                "Published range:"
                f" {valid_published.min()} -> {valid_published.max()}"
            )

    print()
    print("Feed row counts:")

    for feed_name, count in feed_counts.items():
        print(f"  {feed_name:<28} {count:>6,}")

    print()
    print(f"Parquet:        {parquet_path}")
    print(f"CSV:            {csv_path}")
    print(f"Latest parquet: {latest_parquet}")
    print(f"Latest CSV:     {latest_csv}")


def main() -> None:
    print("=" * 90)
    print(
        "BACQE INFORMATION DATA - "
        "FINANCIAL HEADLINES RSS SNAPSHOT"
    )
    print("=" * 90)

    run_time_utc = datetime.now(timezone.utc)
    data_lake_root = get_data_lake_root()

    output_dir = build_output_dir(
        data_lake_root,
        run_time_utc,
    )

    print(f"Data lake:  {data_lake_root}")
    print(f"Output dir: {output_dir}")
    print(f"Feeds:      {len(RSS_FEEDS):,}")
    print("-" * 90)

    df = collect_headlines(
        run_time_utc,
    )

    if df.empty:
        print("[WARN] No headlines collected.")
        return

    validate_headlines(df)

    save_outputs(
        df,
        output_dir,
        run_time_utc,
    )

    print("=" * 90)


if __name__ == "__main__":
    main()
