from pathlib import Path

import pandas as pd
from scipy.stats import poisson


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ratings_path = PROCESSED_DIR / "team_ratings.csv"
ratings = pd.read_csv(ratings_path).set_index("team")


def elo_win_probability(elo_a, elo_b):
    return 1 / (1 + 10 ** ((elo_b - elo_a) / 400))


def match_probabilities(team_a, team_b, draw_rate=0.26):
    elo_a = ratings.loc[team_a, "elo"]
    elo_b = ratings.loc[team_b, "elo"]

    raw_a = elo_win_probability(elo_a, elo_b)
    raw_b = 1 - raw_a

    p_draw = draw_rate
    p_a = raw_a * (1 - p_draw)
    p_b = raw_b * (1 - p_draw)

    return p_a, p_draw, p_b


def expected_goals(team_a, team_b, base_goals=1.35):
    attack_a = ratings.loc[team_a, "attack_strength"]
    defence_a = ratings.loc[team_a, "defence_strength"]

    attack_b = ratings.loc[team_b, "attack_strength"]
    defence_b = ratings.loc[team_b, "defence_strength"]

    xg_a = base_goals * attack_a * defence_b
    xg_b = base_goals * attack_b * defence_a

    return xg_a, xg_b


def score_matrix(team_a, team_b, max_goals=6):
    xg_a, xg_b = expected_goals(team_a, team_b)

    rows = []

    for goals_a in range(max_goals + 1):
        for goals_b in range(max_goals + 1):
            probability = poisson.pmf(goals_a, xg_a) * poisson.pmf(goals_b, xg_b)

            rows.append({
                "team_a": team_a,
                "team_b": team_b,
                "goals_a": goals_a,
                "goals_b": goals_b,
                "scoreline": f"{goals_a}-{goals_b}",
                "probability": probability,
            })

    df = pd.DataFrame(rows)
    df["probability"] = df["probability"] / df["probability"].sum()

    return df.sort_values("probability", ascending=False)


if __name__ == "__main__":
    team_a = "Mexico"
    team_b = "South Africa"

    p_a, p_draw, p_b = match_probabilities(team_a, team_b)
    xg_a, xg_b = expected_goals(team_a, team_b)
    scores = score_matrix(team_a, team_b)

    output_path = OUTPUT_DIR / "mexico_vs_south_africa_score_probs.csv"
    scores.to_csv(output_path, index=False)

    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 02 - PREDICT MATCH")
    print("=" * 80)
    print(f"Fixture: {team_a} v {team_b}")
    print("-" * 80)
    print(f"{team_a} win: {p_a:.2%}")
    print(f"Draw: {p_draw:.2%}")
    print(f"{team_b} win: {p_b:.2%}")
    print("-" * 80)
    print(f"{team_a} expected goals: {xg_a:.2f}")
    print(f"{team_b} expected goals: {xg_b:.2f}")
    print("-" * 80)
    print("Top 10 scorelines:")
    print(scores[["scoreline", "probability"]].head(10))
    print("-" * 80)
    print(f"Output Saved: {output_path}")
    print("=" * 80)
