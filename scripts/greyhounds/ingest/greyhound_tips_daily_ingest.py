from __future__ import annotations

import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


DATA_LAKE_ROOT = Path("/mnt/quant_lab")
LOG_DIR = DATA_LAKE_ROOT / "logs"
HEADLESS = True
PAGE_TIMEOUT_SECONDS = 25
WAIT_SECONDS = 15


@dataclass
class ScrapeConfig:
    data_lake_root: Path
    headless: bool = True
    page_timeout_seconds: int = 25
    wait_seconds: int = 15


def validate_data_lake_root(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Data lake path does not exist: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Data lake path is not a directory: {path}")

    test_file = path / ".write_test_greyhound_ingest.tmp"

    try:
        test_file.write_text("write-test", encoding="utf-8")
        test_file.unlink(missing_ok=True)
    except OSError as exc:
        raise PermissionError(f"Data lake path is not writable: {path}") from exc


def setup_logging(log_dir: Path) -> None:
    validate_data_lake_root(log_dir.parent)
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"greyhound_tips_ingest_{datetime.now():%Y%m%d}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def clean_track_name(track: Optional[str]) -> Optional[str]:
    if track is None:
        return None
    track = track.strip()
    track = re.sub(r"\s+", " ", track)
    return track or None


def clean_time_value(time_text: Optional[str]) -> Optional[str]:
    if not time_text:
        return None
    time_text = time_text.strip()
    m = re.match(r"^(\d{1,2}:\d{2})$", time_text)
    return m.group(1) if m else time_text


def clean_name(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    name = re.sub(r"\s+", " ", name.strip())
    return name or None


def extract_last_digit(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    digits = re.findall(r"(\d)", value)
    return digits[-1] if digits else None


def parse_time_and_name(raw_text: str) -> tuple[Optional[str], Optional[str]]:
    raw_text = re.sub(r"\s+", " ", raw_text.strip())
    parts = raw_text.split(" ", 1)
    if len(parts) == 2:
        return clean_time_value(parts[0]), clean_name(parts[1])
    if len(parts) == 1:
        return None, clean_name(parts[0])
    return None, None


def build_driver(config: ScrapeConfig) -> webdriver.Chrome:
    options = Options()
    if config.headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1800")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(config.page_timeout_seconds)
    return driver


def wait_for_page(driver: webdriver.Chrome, wait_seconds: int) -> None:
    wait = WebDriverWait(driver, wait_seconds)
    wait.until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "#meeting-list-scroll div.scrollContent div h3")
        )
    )


def scrape_tip_block(
    driver: webdriver.Chrome,
    css_selector: str,
    race_index: int,
    row_index: int,
    block_col_class: str,
    tip_type: str,
    scrape_date: str,
    track: Optional[str],
) -> Optional[dict]:
    try:
        base = f"{css_selector} > li:nth-child({row_index}) > a > span.{block_col_class}.tipsGrid"
        time_name_element = driver.find_element(By.CSS_SELECTOR, f"{base} > span.betOptionsRow.row2")
        time_value, dog_name = parse_time_and_name(time_name_element.text)

        if not dog_name:
            return None

        try:
            stars_element = driver.find_element(
                By.CSS_SELECTOR,
                f"{base} > span.betOptionsRow.row1 > div > div",
            )
            stars = extract_last_digit(stars_element.get_attribute("class"))
        except NoSuchElementException:
            stars = None

        try:
            trap_element = driver.find_element(
                By.CSS_SELECTOR,
                f"{base} > span.betOptionsRow.row2 > i",
            )
            trap = extract_last_digit(trap_element.get_attribute("class"))
        except NoSuchElementException:
            trap = None

        return {
            "scrape_date": scrape_date,
            "tip_type": tip_type,
            "track": clean_track_name(track),
            "race_time": clean_time_value(time_value),
            "dog_name": clean_name(dog_name),
            "stars": pd.to_numeric(stars, errors="coerce"),
            "trap": trap,
            "source": "racingpost_greyhoundbet",
            "race_section_index": race_index,
            "race_row_index": row_index,
            "ingested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except NoSuchElementException:
        return None


def scrape_daily_tips(config: ScrapeConfig, run_date: Optional[datetime] = None) -> pd.DataFrame:
    run_date = run_date or datetime.today()
    date_str = run_date.strftime("%Y-%m-%d")
    url = f"https://greyhoundbet.racingpost.com/#meeting-list/r_date={date_str}"

    logging.info("Opening %s", url)
    driver = build_driver(config)
    records: list[dict] = []

    try:
        driver.get(url)
        wait_for_page(driver, config.wait_seconds)
        time.sleep(2)

        h3_elements = driver.find_elements(By.CSS_SELECTOR, "#meeting-list-scroll > div.scrollContent > div > h3")
        race_type_count = len(h3_elements)
        logging.info("Found %s race sections", race_type_count)

        for section in range(race_type_count):
            ul_index = 4 + 2 * section
            css_selector = f"#meeting-list-scroll > div.scrollContent > div > ul:nth-child({ul_index})"
            line_items = driver.find_elements(By.CSS_SELECTOR, f"{css_selector} > li")
            logging.info("Section %s has %s race rows", section, len(line_items))

            for row_index in range(1, len(line_items) + 1):
                try:
                    selector_course = f"{css_selector} > li:nth-child({row_index}) > a > span.race-details-col1 > h4"
                    track_element = driver.find_element(By.CSS_SELECTOR, selector_course)
                    track = track_element.text.strip()
                except NoSuchElementException:
                    track = None

                naps_record = scrape_tip_block(
                    driver=driver,
                    css_selector=css_selector,
                    race_index=section,
                    row_index=row_index,
                    block_col_class="race-details-col2",
                    tip_type="NAP",
                    scrape_date=date_str,
                    track=track,
                )
                if naps_record:
                    records.append(naps_record)

                nb_record = scrape_tip_block(
                    driver=driver,
                    css_selector=css_selector,
                    race_index=section,
                    row_index=row_index,
                    block_col_class="race-details-col3",
                    tip_type="NB",
                    scrape_date=date_str,
                    track=track,
                )
                if nb_record:
                    records.append(nb_record)

    except TimeoutException:
        logging.exception("Timed out while loading page")
        raise
    finally:
        driver.quit()

    df = pd.DataFrame(records)
    if df.empty:
        logging.warning("No records scraped for %s", date_str)
        return df

    df["stars"] = pd.to_numeric(df["stars"], errors="coerce").astype("Int64")
    df["trap"] = df["trap"].astype("string")
    df = df.sort_values(["track", "race_time", "tip_type", "dog_name"], na_position="last").reset_index(drop=True)
    df = df.drop_duplicates(subset=["scrape_date", "tip_type", "track", "race_time"], keep="first")

    logging.info("Scraped %s unique rows", len(df))
    return df


def build_output_paths(data_lake_root: Path, run_date: datetime) -> tuple[Path, Path, Path]:
    raw_root = data_lake_root / "raw" / "rpg_tips"
    day_folder = raw_root / f"year={run_date:%Y}" / f"month={run_date:%m}" / f"date={run_date:%Y-%m-%d}"
    day_folder.mkdir(parents=True, exist_ok=True)

    daily_csv_path = day_folder / f"rpg_tips_{run_date:%Y-%m-%d}.csv"
    latest_csv_path = raw_root / "latest_rpg_tips.csv"
    historical_csv_path = raw_root / "rpg_tips_history.csv"

    return daily_csv_path, latest_csv_path, historical_csv_path


def save_outputs(df: pd.DataFrame, data_lake_root: Path, run_date: datetime) -> tuple[Path, Path, Path]:
    daily_csv_path, latest_csv_path, historical_csv_path = build_output_paths(data_lake_root, run_date)

    df.to_csv(daily_csv_path, index=False)
    df.to_csv(latest_csv_path, index=False)
    logging.info("Saved daily file to %s", daily_csv_path)
    logging.info("Updated latest snapshot at %s", latest_csv_path)

    if historical_csv_path.exists():
        hist = pd.read_csv(historical_csv_path)
        combined = pd.concat([hist, df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["scrape_date", "tip_type", "track", "race_time"],
            keep="last",
        )
    else:
        combined = df.copy()

    combined = combined.sort_values(["scrape_date", "track", "race_time", "tip_type"]).reset_index(drop=True)
    combined.to_csv(historical_csv_path, index=False)
    logging.info("Updated historical file at %s", historical_csv_path)

    return daily_csv_path, latest_csv_path, historical_csv_path


def write_run_summary(
    data_lake_root: Path,
    run_started_at: datetime,
    run_finished_at: datetime,
    status: str,
    rows_scraped: int,
    output_path: Optional[Path],
    error_message: Optional[str] = None,
) -> None:
    meta_dir = data_lake_root / "meta" / "run_logs"
    meta_dir.mkdir(parents=True, exist_ok=True)

    summary_path = meta_dir / "rpg_tips_run_summary.csv"

    duration_seconds = round((run_finished_at - run_started_at).total_seconds(), 2)

    if rows_scraped == 0 and status == "success":
        health_flag = "warning_empty"
    elif 0 < rows_scraped < 10 and status == "success":
        health_flag = "warning_low_rows"
    elif status == "failed":
        health_flag = "failed"
    else:
        health_flag = "ok"

    row = pd.DataFrame([{
        "run_started_at": run_started_at.strftime("%Y-%m-%d %H:%M:%S"),
        "run_finished_at": run_finished_at.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": duration_seconds,
        "status": status,
        "health_flag": health_flag,
        "rows_scraped": rows_scraped,
        "output_path": str(output_path) if output_path else None,
        "error_message": error_message,
    }])

    if summary_path.exists():
        existing = pd.read_csv(summary_path)
        combined = pd.concat([existing, row], ignore_index=True)
    else:
        combined = row

    combined.to_csv(summary_path, index=False)
    logging.info("Updated run summary at %s", summary_path)


def main() -> None:
    config = ScrapeConfig(
        data_lake_root=DATA_LAKE_ROOT,
        headless=HEADLESS,
        page_timeout_seconds=PAGE_TIMEOUT_SECONDS,
        wait_seconds=WAIT_SECONDS,
    )

    run_started_at = datetime.now()
    rows_scraped = 0
    output_path: Optional[Path] = None

    try:
        setup_logging(LOG_DIR)
        logging.info("Starting greyhound tips daily ingest")

        df = scrape_daily_tips(config=config, run_date=datetime.today())
        rows_scraped = len(df)

        if df.empty:
            raise RuntimeError("No Racing Post greyhound tips scraped; treating as pipeline failure")

        daily_csv_path, _, _ = save_outputs(df=df, data_lake_root=config.data_lake_root, run_date=datetime.today())
        output_path = daily_csv_path

        run_finished_at = datetime.now()
        write_run_summary(
            data_lake_root=config.data_lake_root,
            run_started_at=run_started_at,
            run_finished_at=run_finished_at,
            status="success",
            rows_scraped=rows_scraped,
            output_path=output_path,
            error_message=None,
        )

        logging.info("Greyhound tips ingest complete")

    except Exception as exc:
        logging.exception("Greyhound tips ingest failed")
        run_finished_at = datetime.now()

        try:
            write_run_summary(
                data_lake_root=config.data_lake_root,
                run_started_at=run_started_at,
                run_finished_at=run_finished_at,
                status="failed",
                rows_scraped=rows_scraped,
                output_path=output_path,
                error_message=str(exc),
            )
        except Exception:
            pass

        raise


if __name__ == "__main__":
    main()
