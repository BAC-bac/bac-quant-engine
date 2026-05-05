from pathlib import Path
from datetime import date, timedelta
import requests

RAW_BASE = Path("/mnt/quant_lab/raw/Greyhound Racing")
TIMEOUT = 30

def month_folder_name(d: date) -> str:
    return d.strftime("%B %Y")

def file_name_for_race_date(d: date) -> str:
    next_day = d + timedelta(days=1)
    return f"dwbfgreyhoundwin{next_day.strftime('%d%m%Y')}.csv"

def target_path_for_race_date(d: date) -> Path:
    folder = RAW_BASE / str(d.year) / "Results" / month_folder_name(d)
    folder.mkdir(parents=True, exist_ok=True)
    return folder / file_name_for_race_date(d)

def url_for_race_date(d: date) -> str:
    next_day = d + timedelta(days=1)
    fname = f"dwbfgreyhoundwin{next_day.strftime('%d%m%Y')}.csv"
    return f"https://promo.betfair.com/betfairsp/prices/{fname}"

def main() -> None:
    race_date = date.today() - timedelta(days=1)
    target = target_path_for_race_date(race_date)

    if target.exists() and target.stat().st_size > 0:
        print(f"[SKIP] Already exists: {target}")
        return

    url = url_for_race_date(race_date)
    print(f"[INFO] Fetching {url}")

    r = requests.get(url, timeout=TIMEOUT)
    if r.status_code == 200 and r.text.strip():
        target.write_bytes(r.content)
        print(f"[OK] Saved {target}")
    else:
        print(f"[FAIL] HTTP {r.status_code}")

if __name__ == "__main__":
    main()