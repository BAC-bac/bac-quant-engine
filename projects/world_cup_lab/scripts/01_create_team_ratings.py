"""
================================================================================
WORLD CUP LAB
SCRIPT 01 - CREATE TEAM RATINGS
================================================================================

Purpose:
    Create an initial team ratings table using Elo ratings.

Output:
    data/processed/team_ratings.csv

Author:
    Ben Cole / BACQE
================================================================================
"""

from pathlib import Path

import pandas as pd


# =============================================================================
# PROJECT PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# TEAM RATINGS
# =============================================================================

TEAM_RATINGS = [
    {"team": "Spain",          "elo": 2155},
    {"team": "Argentina",      "elo": 2113},
    {"team": "France",         "elo": 2062},
    {"team": "England",        "elo": 2020},
    {"team": "Brazil",         "elo": 1988},
    {"team": "Portugal",       "elo": 1986},
    {"team": "Colombia",       "elo": 1982},
    {"team": "Netherlands",    "elo": 1948},
    {"team": "Germany",        "elo": 1932},
    {"team": "Italy",          "elo": 1915},
    {"team": "Belgium",        "elo": 1908},
    {"team": "Croatia",        "elo": 1895},
    {"team": "Uruguay",        "elo": 1888},
    {"team": "Mexico",         "elo": 1800},
    {"team": "United States",  "elo": 1780},
    {"team": "Canada",         "elo": 1765},
    {"team": "Japan",          "elo": 1755},
    {"team": "South Korea",    "elo": 1725},
    {"team": "Australia",      "elo": 1705},
    {"team": "South Africa",   "elo": 1650},
]

df = pd.DataFrame(TEAM_RATINGS)


# =============================================================================
# FEATURE ENGINEERING
# =============================================================================

mean_elo = df["elo"].mean()

df["elo_diff_from_average"] = df["elo"] - mean_elo

# Attack strength
df["attack_strength"] = (
    1.0 + (df["elo_diff_from_average"] / 1000)
)

# Defence strength
df["defence_strength"] = (
    1.0 - (df["elo_diff_from_average"] / 1200)
)

# Prevent unrealistic values
df["attack_strength"] = df["attack_strength"].clip(
    lower=0.70,
    upper=1.40
)

df["defence_strength"] = df["defence_strength"].clip(
    lower=0.70,
    upper=1.40
)

# Ranking column
df["ranking"] = (
    df["elo"]
    .rank(ascending=False, method="dense")
    .astype(int)
)

df = df.sort_values(
    by="elo",
    ascending=False
).reset_index(drop=True)


# =============================================================================
# SAVE OUTPUT
# =============================================================================

output_file = PROCESSED_DIR / "team_ratings.csv"

df.to_csv(
    output_file,
    index=False
)


# =============================================================================
# REPORTING
# =============================================================================

print("=" * 80)
print("WORLD CUP LAB")
print("SCRIPT 01 - CREATE TEAM RATINGS")
print("=" * 80)

print(f"Teams Loaded: {len(df):,}")
print(f"Average Elo: {mean_elo:.2f}")

print("-" * 80)
print("TOP 10 TEAMS")
print("-" * 80)

print(
    df[
        [
            "ranking",
            "team",
            "elo",
            "attack_strength",
            "defence_strength",
        ]
    ].head(10)
)

print("-" * 80)
print(f"Output Saved: {output_file}")
print("=" * 80)
