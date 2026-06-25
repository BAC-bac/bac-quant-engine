"""
================================================================================
WORLD CUP LAB
SCRIPT 07 - BUILD FULL TOURNAMENT DATASET
================================================================================

Purpose:
    Build structured tournament input files for the World Cup simulation engine.

Outputs:
    data/world_cup_2026/teams.csv
    data/world_cup_2026/groups.csv
    data/world_cup_2026/fixtures.csv
================================================================================
"""

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
WC_DIR.mkdir(parents=True, exist_ok=True)

TEAMS_PATH = WC_DIR / "teams.csv"
GROUPS_PATH = WC_DIR / "groups.csv"
FIXTURES_PATH = WC_DIR / "fixtures.csv"


GROUPS = {
    "A": ["Mexico", "South Africa", "South Korea", "Czechia"],
    "B": ["Canada", "Bosnia and Herzegovina", "Qatar", "Switzerland"],
    "C": ["Brazil", "Morocco", "Haiti", "Scotland"],
    "D": ["United States", "Paraguay", "Australia", "Turkey"],
    "E": ["Germany", "Curacao", "Ivory Coast", "Ecuador"],
    "F": ["Netherlands", "Japan", "Sweden", "Tunisia"],
    "G": ["Belgium", "Egypt", "Iran", "New Zealand"],
    "H": ["Spain", "Cape Verde", "Saudi Arabia", "Uruguay"],
    "I": ["France", "Senegal", "Norway", "Iraq"],
    "J": ["Argentina", "Algeria", "Austria", "Jordan"],
    "K": ["Portugal", "Uzbekistan", "Colombia", "DR Congo"],
    "L": ["England", "Croatia", "Ghana", "Panama"],
}


def build_teams(groups):
    rows = []

    for group, teams in groups.items():
        for team in teams:
            rows.append(
                {
                    "team": team,
                    "group": group,
                }
            )

    return pd.DataFrame(rows)


def build_groups(groups):
    rows = []

    for group, teams in groups.items():
        for position, team in enumerate(teams, start=1):
            rows.append(
                {
                    "group": group,
                    "group_position_seed": position,
                    "team": team,
                }
            )

    return pd.DataFrame(rows)


def build_group_fixtures(groups):
    rows = []
    fixture_id = 1

    for group, teams in groups.items():
        pairings = [
            (teams[0], teams[1]),
            (teams[2], teams[3]),
            (teams[0], teams[2]),
            (teams[3], teams[1]),
            (teams[3], teams[0]),
            (teams[1], teams[2]),
        ]

        for match_number, (home_team, away_team) in enumerate(pairings, start=1):
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "stage": "group",
                    "group": group,
                    "group_match_number": match_number,
                    "home_team": home_team,
                    "away_team": away_team,
                }
            )

            fixture_id += 1

    return pd.DataFrame(rows)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 07 - BUILD FULL TOURNAMENT DATASET")
    print("=" * 80)

    teams_df = build_teams(GROUPS)
    groups_df = build_groups(GROUPS)
    fixtures_df = build_group_fixtures(GROUPS)

    teams_df.to_csv(TEAMS_PATH, index=False)
    groups_df.to_csv(GROUPS_PATH, index=False)
    fixtures_df.to_csv(FIXTURES_PATH, index=False)

    print(f"Teams saved:    {len(teams_df):,} -> {TEAMS_PATH}")
    print(f"Groups saved:   {len(groups_df):,} -> {GROUPS_PATH}")
    print(f"Fixtures saved: {len(fixtures_df):,} -> {FIXTURES_PATH}")

    print("-" * 80)
    print("Group preview:")
    print(groups_df.head(12))

    print("-" * 80)
    print("Fixture preview:")
    print(fixtures_df.head(12))

    print("=" * 80)


if __name__ == "__main__":
    main()
