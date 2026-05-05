from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import requests


RAW_BASE = Path("/mnt/quant_lab/raw/Greyhound Racing")
MARKETS = ["win", "place"]
TIMEOUT = 30


def month_folder_name(d: date) -> str:
    return d.strftime("%B %Y")


def file_name_for_race_date(d: date, market: str) -> str:
    next_day = d + timedelta(days=1)
    return f"dwbfgreyhound{market}{next_day:%d%m%Y}.csv"


def target_path_for_race_date(d: date, market: str) -> Path:
    folder = RAW_BASE / str(d.year) / "results" / month_folder_name(d)
    folder.mkdir(parents=True, exist_ok=True)
    return folder / file_name_for_race_date(d, market)


def url_for_race_date(d: date, market: str) -> str:
    return f"https://promo.betfair.com/betfairsp/prices/{file_name_for_race_date(d, market)}"


def download_file(url: str, target_path: Path) -> tuple[bool, str]:
    try:
        r = requests.get(url, timeout=TIMEOUT)
        if r.status_code == 200 and r.text.strip():
            target_path.write_bytes(r.content)
            return True, "downloaded"
        return False, f"http_{r.status_code}"
    except Exception as exc:
        return False, str(exc)


def main() -> None:
    race_date = date.today() - timedelta(days=1)

    downloaded = 0
    skipped = 0
    failed = 0

    for market in MARKETS:
        target = target_path_for_race_date(race_date, market)

        if target.exists() and target.stat().st_size > 0:
            print(f"[SKIP] {race_date} {market} -> {target}")
            skipped += 1
            continue

        url = url_for_race_date(race_date, market)
        print(f"[INFO] Fetching {url}")

        success, msg = download_file(url, target)

        if success:
            print(f"[OK]   {race_date} {market} -> {target}")
            downloaded += 1
        else:
            print(f"[FAIL] {race_date} {market} -> {msg}")
            failed += 1
            if target.exists() and target.stat().st_size == 0:
                target.unlink(missing_ok=True)

    print("\nSummary")
    print(f"Downloaded: {downloaded}")
    print(f"Skipped:    {skipped}")
    print(f"Failed:     {failed}")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
