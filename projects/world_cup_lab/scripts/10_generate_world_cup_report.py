"""
================================================================================
WORLD CUP LAB
SCRIPT 10 - GENERATE WORLD CUP REPORT
================================================================================

Purpose:
    Review the output from Script 09 and generate a clean report.

Input:
    outputs/full_tournament_simulation_summary.csv

Outputs:
    outputs/world_cup_report.txt
    outputs/world_cup_report.csv
================================================================================
"""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "outputs"

INPUT_PATH = OUTPUT_DIR / "full_tournament_simulation_summary.csv"
REPORT_TXT_PATH = OUTPUT_DIR / "world_cup_report.txt"
REPORT_CSV_PATH = OUTPUT_DIR / "world_cup_report.csv"


def load_results():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_PATH}")

    return pd.read_csv(INPUT_PATH)


def format_percent(value):
    return f"{value:.2f}%"


def section(title):
    line = "=" * 80
    return f"\n{line}\n{title}\n{line}\n"


def build_report(results):
    lines = []

    lines.append(section("WORLD CUP LAB - SCRIPT 10 REPORT"))
    lines.append(f"Input file: {INPUT_PATH}\n")
    lines.append(f"Teams analysed: {len(results):,}\n")

    winner_sum = results["winner_probability_pct"].sum()
    lines.append(f"Winner probability total: {winner_sum:.2f}%\n")

    lines.append(section("TOP 20 TOURNAMENT WINNER PROBABILITIES"))
    top_winners = results.sort_values("winner_probability_pct", ascending=False).head(20)

    lines.append(
        top_winners[
            [
                "team",
                "qualification_probability_pct",
                "quarter_final_probability_pct",
                "semi_final_probability_pct",
                "final_probability_pct",
                "winner_probability_pct",
            ]
        ].to_string(index=False)
    )

    lines.append(section("TOP 20 QUALIFICATION PROBABILITIES"))
    top_qualifiers = results.sort_values(
        "qualification_probability_pct",
        ascending=False,
    ).head(20)

    lines.append(
        top_qualifiers[
            [
                "team",
                "qualification_probability_pct",
                "quarter_final_probability_pct",
                "winner_probability_pct",
            ]
        ].to_string(index=False)
    )

    lines.append(section("DARK HORSE CANDIDATES"))
    dark_horses = results[
        (results["qualification_probability_pct"] >= 65)
        & (results["winner_probability_pct"] >= 0.75)
        & (results["winner_probability_pct"] <= 3.00)
    ].sort_values("winner_probability_pct", ascending=False)

    if dark_horses.empty:
        lines.append("No dark horse candidates found using current thresholds.")
    else:
        lines.append(
            dark_horses[
                [
                    "team",
                    "qualification_probability_pct",
                    "quarter_final_probability_pct",
                    "semi_final_probability_pct",
                    "winner_probability_pct",
                ]
            ].to_string(index=False)
        )

    lines.append(section("BIGGEST UNDERDOGS"))
    underdogs = results.sort_values("winner_probability_pct", ascending=True).head(15)

    lines.append(
        underdogs[
            [
                "team",
                "qualification_probability_pct",
                "quarter_final_probability_pct",
                "winner_probability_pct",
            ]
        ].to_string(index=False)
    )

    lines.append(section("MODEL CHECKS"))

    checks = []

    if abs(winner_sum - 100) <= 0.5:
        checks.append("PASS: Winner probabilities sum close to 100%.")
    else:
        checks.append("WARNING: Winner probabilities do not sum close to 100%.")

    impossible_quals = results[
        (results["qualification_probability_pct"] < 0)
        | (results["qualification_probability_pct"] > 100)
    ]

    if impossible_quals.empty:
        checks.append("PASS: Qualification probabilities are within 0-100%.")
    else:
        checks.append("WARNING: Some qualification probabilities are outside 0-100%.")

    impossible_winners = results[
        (results["winner_probability_pct"] < 0)
        | (results["winner_probability_pct"] > 100)
    ]

    if impossible_winners.empty:
        checks.append("PASS: Winner probabilities are within 0-100%.")
    else:
        checks.append("WARNING: Some winner probabilities are outside 0-100%.")

    high_qual_low_win = results[
        (results["qualification_probability_pct"] >= 80)
        & (results["winner_probability_pct"] < 1)
    ].sort_values("qualification_probability_pct", ascending=False)

    if not high_qual_low_win.empty:
        checks.append(
            "NOTE: Some teams qualify often but rarely win. This may reflect weak knockout strength."
        )

    lines.extend(checks)

    if not high_qual_low_win.empty:
        lines.append("\nHigh qualification / low winner teams:")
        lines.append(
            high_qual_low_win[
                [
                    "team",
                    "qualification_probability_pct",
                    "quarter_final_probability_pct",
                    "winner_probability_pct",
                ]
            ].to_string(index=False)
        )

    return "\n".join(lines)


def create_review_csv(results):
    review = results.copy()

    review["winner_rank"] = (
        review["winner_probability_pct"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    review["qualification_rank"] = (
        review["qualification_probability_pct"]
        .rank(ascending=False, method="dense")
        .astype(int)
    )

    review["dark_horse_flag"] = (
        (review["qualification_probability_pct"] >= 65)
        & (review["winner_probability_pct"] >= 0.75)
        & (review["winner_probability_pct"] <= 3.00)
    )

    review["underdog_flag"] = review["winner_probability_pct"] < 0.25

    review = review.sort_values("winner_rank").reset_index(drop=True)

    return review


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 10 - GENERATE WORLD CUP REPORT")
    print("=" * 80)

    results = load_results()

    report = build_report(results)
    review_csv = create_review_csv(results)

    REPORT_TXT_PATH.write_text(report, encoding="utf-8")
    review_csv.to_csv(REPORT_CSV_PATH, index=False)

    print(report)

    print("-" * 80)
    print(f"Text report saved: {REPORT_TXT_PATH}")
    print(f"CSV report saved:  {REPORT_CSV_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
