"""
================================================================================
WORLD CUP LAB
SCRIPT 22 - DOWNLOAD WORLD CUP RESULTS FROM API-FOOTBALL
================================================================================

Purpose:
    Download World Cup 2026 fixtures/results from API-Football and save completed
    matches in a format suitable for our World Cup Lab pipeline.

Inputs:
    .env with API_FOOTBALL_KEY
    data/world_cup_2026/fixtures.csv

Outputs:
    data/world_cup_2026/api_football_fixtures_raw.json
    data/world_cup_2026/actual_results_api_raw.csv
    data/world_cup_2026/actual_results_auto.csv
================================================================================
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

FIXTURES_PATH = WC_DIR / "fixtures.csv"

RAW_JSON_PATH = WC_DIR / "api_football_fixtures_raw.json"
API_RAW_CSV_PATH = WC_DIR / "actual_results_api_raw.csv"
AUTO_RESULTS_PATH = WC_DIR / "actual_results_auto.csv"
MATCH_REVIEW_PATH = OUTPUT_DIR / "api_results_match_review.csv"

API_BASE_URL = "https://v3.football.api-sports.io"
API_ENDPOINT = f"{API_BASE_URL}/fixtures"

WORLD_CUP_LEAGUE_ID = 1
WORLD_CUP_SEASON = 2026

COMPLETED_STATUSES = {"FT", "AET", "PEN"}

TEAM_NAME_MAP = {
    "USA": "United States",
    "United States": "United States",
    "Korea Republic": "South Korea",
    "South Korea": "South Korea",
    "Czech Republic": "Czechia",
    "Czechia": "Czechia",
    "Côte d'Ivoire": "Ivory Coast",
    "Ivory Coast": "Ivory Coast",
    "Curaçao": "Curacao",
    "Curacao": "Curacao",
    "DR Congo": "DR Congo",
    "Congo DR": "DR Congo",
    "Bosnia & Herzegovina": "Bosnia and Herzegovina",
    "Bosnia and Herzegovina": "Bosnia and Herzegovina",
}


def load_api_key() -> str:
    load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

    api_key = os.getenv("API_FOOTBALL_KEY")

    if not api_key:
        raise RuntimeError(
            "API_FOOTBALL_KEY not found. Check your .env file in the project root."
        )

    return api_key


def normalise_team_name(name: str) -> str:
    if pd.isna(name):
        return ""

    cleaned = str(name).strip()
    return TEAM_NAME_MAP.get(cleaned, cleaned)


def call_api(api_key: str) -> dict[str, Any]:
    headers = {
        "x-apisports-key": api_key,
    }

    params = {
        "league": WORLD_CUP_LEAGUE_ID,
        "season": WORLD_CUP_SEASON,
    }

    response = requests.get(
        API_ENDPOINT,
        headers=headers,
        params=params,
        timeout=30,
    )

    print(f"API status code: {response.status_code}")

    if response.status_code != 200:
        raise RuntimeError(
            f"API request failed: {response.status_code}\n{response.text[:500]}"
        )

    payload = response.json()

    RAW_JSON_PATH.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    return payload


def parse_api_response(payload: dict[str, Any]) -> pd.DataFrame:
    rows = []

    for item in payload.get("response", []):
        fixture = item.get("fixture", {})
        league = item.get("league", {})
        teams = item.get("teams", {})
        goals = item.get("goals", {})
        score = item.get("score", {})

        status = fixture.get("status", {}) or {}

        home_team = normalise_team_name((teams.get("home") or {}).get("name"))
        away_team = normalise_team_name((teams.get("away") or {}).get("name"))

        row = {
            "api_fixture_id": fixture.get("id"),
            "api_date": fixture.get("date"),
            "api_timezone": fixture.get("timezone"),
            "api_venue": (fixture.get("venue") or {}).get("name"),
            "api_city": (fixture.get("venue") or {}).get("city"),
            "api_status_long": status.get("long"),
            "api_status_short": status.get("short"),
            "api_elapsed": status.get("elapsed"),
            "league_id": league.get("id"),
            "league_name": league.get("name"),
            "season": league.get("season"),
            "round": league.get("round"),
            "home_team_api": home_team,
            "away_team_api": away_team,
            "home_goals": goals.get("home"),
            "away_goals": goals.get("away"),
            "halftime_home": (score.get("halftime") or {}).get("home"),
            "halftime_away": (score.get("halftime") or {}).get("away"),
            "fulltime_home": (score.get("fulltime") or {}).get("home"),
            "fulltime_away": (score.get("fulltime") or {}).get("away"),
            "extratime_home": (score.get("extratime") or {}).get("home"),
            "extratime_away": (score.get("extratime") or {}).get("away"),
            "penalty_home": (score.get("penalty") or {}).get("home"),
            "penalty_away": (score.get("penalty") or {}).get("away"),
        }

        rows.append(row)

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df.to_csv(API_RAW_CSV_PATH, index=False)

    return df


def load_local_fixtures() -> pd.DataFrame:
    fixtures = pd.read_csv(FIXTURES_PATH)

    fixtures["home_team_norm"] = fixtures["home_team"].apply(normalise_team_name)
    fixtures["away_team_norm"] = fixtures["away_team"].apply(normalise_team_name)

    return fixtures


def match_api_results_to_local_fixtures(api_df: pd.DataFrame, fixtures: pd.DataFrame):
    completed = api_df[
        api_df["api_status_short"].isin(COMPLETED_STATUSES)
    ].copy()

    if completed.empty:
        return pd.DataFrame(), completed

    completed["home_team_norm"] = completed["home_team_api"].apply(normalise_team_name)
    completed["away_team_norm"] = completed["away_team_api"].apply(normalise_team_name)

    merged = completed.merge(
        fixtures[
            [
                "fixture_id",
                "stage",
                "group",
                "home_team",
                "away_team",
                "home_team_norm",
                "away_team_norm",
            ]
        ],
        on=["home_team_norm", "away_team_norm"],
        how="left",
        suffixes=("_api", "_local"),
    )

    merged["matched_local_fixture"] = merged["fixture_id"].notna()

    return merged, completed


def classify_result(home_goals, away_goals):
    if home_goals > away_goals:
        return "home_win"
    if home_goals < away_goals:
        return "away_win"
    return "draw"


def build_actual_results_auto(merged: pd.DataFrame) -> pd.DataFrame:
    matched = merged[merged["matched_local_fixture"]].copy()

    if matched.empty:
        return pd.DataFrame()

    matched["home_goals"] = pd.to_numeric(matched["home_goals"], errors="coerce")
    matched["away_goals"] = pd.to_numeric(matched["away_goals"], errors="coerce")

    matched = matched.dropna(subset=["home_goals", "away_goals"]).copy()

    matched["home_goals"] = matched["home_goals"].astype(int)
    matched["away_goals"] = matched["away_goals"].astype(int)
    matched["fixture_id"] = matched["fixture_id"].astype(int)

    actuals = pd.DataFrame(
        {
            "fixture_id": matched["fixture_id"],
            "home_team": matched["home_team"],
            "away_team": matched["away_team"],
            "home_goals": matched["home_goals"],
            "away_goals": matched["away_goals"],
            "red_cards_total": None,
            "host_team_involved": matched["home_team"].isin(
                ["Mexico", "United States", "Canada"]
            ) | matched["away_team"].isin(["Mexico", "United States", "Canada"]),
            "host_team": matched.apply(
                lambda row: row["home_team"]
                if row["home_team"] in ["Mexico", "United States", "Canada"]
                else (
                    row["away_team"]
                    if row["away_team"] in ["Mexico", "United States", "Canada"]
                    else None
                ),
                axis=1,
            ),
            "observer_notes": "Auto-downloaded from API-Football. Manual observer notes not yet added.",
        }
    )

    actuals["actual_result"] = actuals.apply(
        lambda row: classify_result(row["home_goals"], row["away_goals"]),
        axis=1,
    )

    actuals["total_goals"] = actuals["home_goals"] + actuals["away_goals"]
    actuals["both_teams_scored"] = (
        (actuals["home_goals"] > 0) & (actuals["away_goals"] > 0)
    )
    actuals["over_25_goals"] = actuals["total_goals"] > 2.5

    actuals = actuals.sort_values("fixture_id").reset_index(drop=True)

    return actuals


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 22 - DOWNLOAD WORLD CUP RESULTS FROM API-FOOTBALL")
    print("=" * 80)

    api_key = load_api_key()
    print(f"API key loaded safely: {api_key[:6]}...{api_key[-4:]}")

    payload = call_api(api_key)
    api_df = parse_api_response(payload)

    print(f"API fixtures returned: {len(api_df):,}")

    if api_df.empty:
        print("[STOP] No fixtures returned from API.")
        return

    fixtures = load_local_fixtures()
    merged, completed = match_api_results_to_local_fixtures(api_df, fixtures)

    print(f"Completed API fixtures: {len(completed):,}")
    print(f"Matched completed fixtures: {merged['matched_local_fixture'].sum():,}")

    merged.to_csv(MATCH_REVIEW_PATH, index=False)

    actuals_auto = build_actual_results_auto(merged)

    if actuals_auto.empty:
        print("[WARNING] No local fixtures matched completed API results.")
    else:
        actuals_auto.to_csv(AUTO_RESULTS_PATH, index=False)
        print(f"Auto actual results saved: {AUTO_RESULTS_PATH}")

    print("-" * 80)

    if not merged.empty:
        unmatched = merged[~merged["matched_local_fixture"]]

        if not unmatched.empty:
            print("[WARNING] Unmatched completed API fixtures:")
            print(
                unmatched[
                    [
                        "api_fixture_id",
                        "home_team_api",
                        "away_team_api",
                        "home_goals",
                        "away_goals",
                        "api_status_short",
                    ]
                ].to_string(index=False)
            )
        else:
            print("All completed API fixtures matched local fixtures.")

    print("-" * 80)
    print(f"Raw JSON saved:       {RAW_JSON_PATH}")
    print(f"Raw API CSV saved:    {API_RAW_CSV_PATH}")
    print(f"Match review saved:   {MATCH_REVIEW_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
