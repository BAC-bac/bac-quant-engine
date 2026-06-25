from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"

TEAMS_PATH = WC_DIR / "teams.csv"
FINAL_OUTPUT_PATH = WC_DIR / "team_ratings_real_elo.csv"
RAW_OUTPUT_PATH = WC_DIR / "real_elo_world_raw.csv"

WORLD_ELO_URL = "https://www.eloratings.net/World.tsv"
TEAM_NAMES_URL = "https://www.eloratings.net/en.teams.tsv"


NAME_MAP = {
    "United States": "USA",
    "Czechia": "Czech Republic",
    "Ivory Coast": "Côte d'Ivoire",
    "DR Congo": "Congo DR",
    "Curacao": "Curaçao",
}


def load_world_cup_teams():
    return pd.read_csv(TEAMS_PATH)


def download_real_elo():
    ratings = pd.read_csv(WORLD_ELO_URL, sep="\t", header=None)

    team_names = pd.read_csv(
        TEAM_NAMES_URL,
        sep="\t",
        header=None,
        usecols=[0, 1],
        engine="python",
    )

    ratings_clean = ratings[[2, 3]].copy()
    ratings_clean.columns = ["team_code", "elo"]

    names_clean = team_names.copy()
    names_clean.columns = ["team_code", "team_raw"]

    merged = ratings_clean.merge(names_clean, on="team_code", how="left")

    merged = merged[["team_code", "team_raw", "elo"]]
    merged["elo"] = pd.to_numeric(merged["elo"], errors="coerce")

    merged.to_csv(RAW_OUTPUT_PATH, index=False)

    return merged


def match_world_cup_teams(wc_teams, elo_df):
    rows = []

    elo_lookup = dict(zip(elo_df["team_raw"], elo_df["elo"]))

    for _, row in wc_teams.iterrows():
        team = row["team"]
        group = row["group"]

        candidates = [
            team,
            NAME_MAP.get(team),
        ]

        candidates = [x for x in candidates if x is not None]

        matched_name = None
        elo = None

        for candidate in candidates:
            if candidate in elo_lookup:
                matched_name = candidate
                elo = elo_lookup[candidate]
                break

        rows.append({
            "team": team,
            "group": group,
            "matched_elo_name": matched_name,
            "elo": elo,
        })

    matched = pd.DataFrame(rows)

    missing = matched[matched["elo"].isna()]

    if not missing.empty:
        print("[WARNING] Missing Elo ratings:")
        print(missing["team"].tolist())

        median_elo = matched["elo"].median()
        matched["elo"] = matched["elo"].fillna(median_elo)

    matched["elo"] = matched["elo"].astype(int)

    return matched


def add_strength_features(df):
    ratings = df.copy()

    mean_elo = ratings["elo"].mean()
    ratings["elo_diff_from_average"] = ratings["elo"] - mean_elo

    ratings["attack_strength"] = 1.0 + ratings["elo_diff_from_average"] / 3000
    ratings["defence_strength"] = 1.0 - ratings["elo_diff_from_average"] / 3500

    ratings["attack_strength"] = ratings["attack_strength"].clip(0.80, 1.25)
    ratings["defence_strength"] = ratings["defence_strength"].clip(0.80, 1.25)

    ratings["rating_rank"] = (
        ratings["elo"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    ratings = ratings.sort_values(["rating_rank", "team"]).reset_index(drop=True)

    return ratings


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 11 - DOWNLOAD REAL ELO RATINGS")
    print("=" * 80)

    wc_teams = load_world_cup_teams()
    elo_raw = download_real_elo()

    final = match_world_cup_teams(wc_teams, elo_raw)
    final = add_strength_features(final)

    final.to_csv(FINAL_OUTPUT_PATH, index=False)

    print(f"World Cup teams loaded: {len(wc_teams):,}")
    print(f"Real Elo rows loaded:   {len(elo_raw):,}")
    print(f"Final ratings saved:   {FINAL_OUTPUT_PATH}")

    print("-" * 80)
    print("Top 15:")
    print(final[[
        "rating_rank",
        "team",
        "group",
        "matched_elo_name",
        "elo",
        "attack_strength",
        "defence_strength",
    ]].head(15))

    print("=" * 80)


if __name__ == "__main__":
    main()
