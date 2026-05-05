from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import requests


RAW_BASE = Path("/mnt/quant_lab/raw/Greyhound Racing")
START_DATE = date(2026, 3, 5)
END_DATE = date(2026, 3, 5)
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
    fname = file_name_for_race_date(d, market)
    return f"https://promo.betfair.com/betfairsp/prices/{fname}"


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
    current = START_DATE
    downloaded = 0
    skipped = 0
    failed = 0

    while current <= END_DATE:
        for market in MARKETS:
            target = target_path_for_race_date(current, market)

            if target.exists() and target.stat().st_size > 0:
                print(f"[SKIP] {current} {market} -> {target.name}")
                skipped += 1
                continue

            url = url_for_race_date(current, market)
            success, msg = download_file(url, target)

            if success:
                print(f"[OK]   {current} {market} -> {target}")
                downloaded += 1
            else:
                print(f"[FAIL] {current} {market} -> {msg}")
                failed += 1
                if target.exists() and target.stat().st_size == 0:
                    target.unlink(missing_ok=True)

        current += timedelta(days=1)

    print("\nSummary")
    print(f"Downloaded: {downloaded}")
    print(f"Skipped:    {skipped}")
    print(f"Failed:     {failed}")


if __name__ == "__main__":
    main()
