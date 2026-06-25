"""
================================================================================
WORLD CUP LAB
SCRIPT 13 - ACTUAL RESULTS TRACKER
================================================================================

Purpose:
    Track real World Cup match results against model expectations and observer notes.

Inputs:
    data/world_cup_2026/fixtures.csv
    outputs/fixture_predictions_poisson.csv  optional/future use

Outputs:
    data/world_cup_2026/actual_results.csv
    outputs/actual_results_review.csv
================================================================================
"""

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WC_DIR = PROJECT_ROOT / "data" / "world_cup_2026"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

FIXTURES_PATH = WC_DIR / "fixtures.csv"
ACTUAL_RESULTS_PATH = WC_DIR / "actual_results.csv"
REVIEW_OUTPUT_PATH = OUTPUT_DIR / "actual_results_review.csv"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


ACTUAL_RESULTS = [
    {
        "fixture_id": 1,
        "home_team": "Mexico",
        "away_team": "South Africa",
        "home_goals": 2,
        "away_goals": 0,
        "red_cards_total": 3,
        "host_team_involved": True,
        "host_team": "Mexico",
        "observer_notes": (
            "Mexico were technically stronger. South Africa had spells, "
            "but Mexico's passing quality was better. Red cards were unusually high."
        ),
    },
    {
        "fixture_id": 2,
        "home_team": "South Korea",
        "away_team": "Czechia",
        "home_goals": 2,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": (
            "South Korea reportedly stronger technically. Came from 1-0 down "
            "to win, showing resilience and character."
        ),
    },
    {
        "fixture_id": 7,
        "home_team": "Canada",
        "away_team": "Bosnia and Herzegovina",
        "home_goals": 1,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": True,
        "host_team": "Canada",
        "observer_notes": (
            "Host advantage may have helped Canada remain competitive. "
            "Useful evidence for testing host Elo boost scenarios."
        ),
    },
    {
        "fixture_id": 19,
        "home_team": "United States",
        "away_team": "Paraguay",
        "home_goals": 4,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": True,
        "host_team": "United States",
        "observer_notes": (
            "Strong USA result. Supports the idea that host advantage may be "
            "material, especially for the three host nations."
        ),
    },
    {
        "fixture_id": 8,
        "home_team": "Qatar",
        "away_team": "Switzerland",
        "home_goals": 1,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Limited observation available. Result suggests Qatar were competitive against stronger-rated Switzerland.",
    },
    {
        "fixture_id": 13,
        "home_team": "Brazil",
        "away_team": "Morocco",
        "home_goals": 1,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Limited observation available. Morocco holding Brazil to a draw is potentially a positive signal for Morocco and a mild warning for Brazil.",
    },
    {
        "fixture_id": 14,
        "home_team": "Haiti",
        "away_team": "Scotland",
        "home_goals": 0,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Limited observation available. Scotland secured a narrow win.",
    },
    {
        "fixture_id": 20,
        "home_team": "Australia",
        "away_team": "Turkey",
        "home_goals": 2,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Limited observation available. Australia beating Turkey 2-0 may be a positive model-watch signal for Australia or a negative signal for Turkey.",
    },
        {
        "fixture_id": 25,
        "home_team": "Germany",
        "away_team": "Curacao",
        "home_goals": 7,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Germany produced a dominant 7-1 win. Strong positive signal for Germany and significant negative signal for Curacao.",
    },
    {
        "fixture_id": 31,
        "home_team": "Netherlands",
        "away_team": "Japan",
        "home_goals": 2,
        "away_goals": 2,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Japan holding Netherlands to a 2-2 draw is a positive signal for Japan and a mild warning for Netherlands.",
    },
    {
        "fixture_id": 27,
        "home_team": "Ivory Coast",
        "away_team": "Ecuador",
        "home_goals": 1,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Ivory Coast beating highly rated Ecuador is a strong positive model-watch signal for Ivory Coast and negative signal for Ecuador.",
    },
    {
        "fixture_id": 32,
        "home_team": "Sweden",
        "away_team": "Tunisia",
        "home_goals": 5,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Sweden produced a heavy 5-1 win. Strong positive signal for Sweden and negative signal for Tunisia.",
    },
    {
        "fixture_id": 37,
        "home_team": "Belgium",
        "away_team": "Egypt",
        "home_goals": 1,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Belgium drawing with Egypt is a mild negative signal for Belgium and positive signal for Egypt.",
    },
    {
        "fixture_id": 43,
        "home_team": "Spain",
        "away_team": "Cape Verde",
        "home_goals": 0,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Spain drawing 0-0 with Cape Verde is a significant negative result relative to pre-match expectations and a positive defensive signal for Cape Verde.",
    },
    {
        "fixture_id": 44,
        "home_team": "Saudi Arabia",
        "away_team": "Uruguay",
        "home_goals": 1,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Uruguay dropping points against Saudi Arabia is a negative signal for Uruguay and positive signal for Saudi Arabia.",
    },
    {
        "fixture_id": 38,
        "home_team": "Iran",
        "away_team": "New Zealand",
        "home_goals": 2,
        "away_goals": 2,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Iran and New Zealand shared a 2-2 draw. Positive attacking signs for both, but defensive questions remain.",
    },
    {
        "fixture_id": 49,
        "home_team": "France",
        "away_team": "Senegal",
        "home_goals": 3,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. France secured a strong 3-1 victory over Senegal. Positive signal for France and slight negative signal for Senegal.",
    },
    {
        "fixture_id": 50,
        "home_team": "Norway",
        "away_team": "Iraq",
        "home_goals": 4,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Norway produced an impressive 4-1 win over Iraq. Strong positive signal for Norway.",
    },
    {
        "fixture_id": 55,
        "home_team": "Argentina",
        "away_team": "Algeria",
        "home_goals": 3,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Argentina delivered a comfortable 3-0 victory. Positive signal for one of the tournament favourites.",
    },
    {
        "fixture_id": 56,
        "home_team": "Austria",
        "away_team": "Jordan",
        "home_goals": 3,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Austria secured a solid 3-1 win over Jordan. Positive signal for Austria.",
    },
        {
        "fixture_id": 61,
        "home_team": "Portugal",
        "away_team": "DR Congo",
        "home_goals": 1,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": (
            "Did not watch. Portugal were held to a 1-1 draw by DR Congo. "
            "Potentially a positive signal for DR Congo and a mild concern for Portugal."
        ),
    },
    {
        "fixture_id": 67,
        "home_team": "England",
        "away_team": "Croatia",
        "home_goals": 4,
        "away_goals": 2,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": (
            "Watched match. England looked very strong going forward and played with freedom. "
            "High pressing was maintained throughout the game, including deep into stoppage time, "
            "indicating excellent fitness and energy levels. Croatia's goalkeeper made several "
            "important saves, including saving England's first penalty before it was retaken and scored. "
            "Croatia scored two high-quality goals and there was little evidence of poor defending. "
            "Overall a very encouraging opening performance from England."
        ),
    },
    {
        "fixture_id": 68,
        "home_team": "Ghana",
        "away_team": "Panama",
        "home_goals": 1,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": (
            "Did not watch. Ghana secured a narrow 1-0 victory over Panama. "
            "Positive signal for Ghana and a setback for Panama."
        ),
    },
    {
        "fixture_id": 73,
        "home_team": "Uzbekistan",
        "away_team": "Colombia",
        "home_goals": 1,
        "away_goals": 3,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": (
            "Did not watch. Colombia produced a convincing 3-1 victory over Uzbekistan. "
            "Positive signal for Colombia and evidence they may justify their strong pre-tournament rating."
        ),
    },
    {
        "fixture_id": 4,
        "home_team": "Czechia",
        "away_team": "South Africa",
        "home_goals": 1,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Czechia and South Africa shared a 1-1 draw. The result keeps Group A open behind Mexico.",
    },
    {
        "fixture_id": 10,
        "home_team": "Switzerland",
        "away_team": "Bosnia and Herzegovina",
        "home_goals": 4,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Switzerland produced a dominant 4-1 victory over Bosnia and Herzegovina. A strong response after their opening draw with Qatar.",
    },
    {
        "fixture_id": 9,
        "home_team": "Canada",
        "away_team": "Qatar",
        "home_goals": 6,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": True,
        "host_team": "Canada",
        "observer_notes": "Did not watch. Canada delivered one of the most emphatic performances of the tournament so far, defeating Qatar 6-0. A major boost to their qualification prospects and goal difference.",
    },
    {
        "fixture_id": 3,
        "home_team": "Mexico",
        "away_team": "South Korea",
        "home_goals": 1,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": True,
        "host_team": "Mexico",
        "observer_notes": "Did not watch. Mexico secured a narrow but valuable 1-0 victory over South Korea. The win strengthens Mexico's position at the top of Group A.",
    },
        {
        "fixture_id": 21,
        "home_team": "United States",
        "away_team": "Australia",
        "home_goals": 2,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": True,
        "host_team": "United States",
        "observer_notes": "Did not watch. The United States secured a strong 2-0 win over Australia, strengthening the host-nation advantage theme and improving their qualification position.",
    },
    {
        "fixture_id": 16,
        "home_team": "Scotland",
        "away_team": "Morocco",
        "home_goals": 0,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Morocco edged Scotland 1-0, a valuable result in Group C and a possible setback to Scotland's positive early momentum.",
    },
    {
        "fixture_id": 15,
        "home_team": "Brazil",
        "away_team": "Haiti",
        "home_goals": 3,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Brazil produced a controlled 3-0 win over Haiti, a positive response after their opening draw with Morocco.",
    },
    {
        "fixture_id": 22,
        "home_team": "Turkey",
        "away_team": "Paraguay",
        "home_goals": 0,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Paraguay secured a narrow 1-0 win over Turkey, increasing pressure on Turkey after a poor start to the group.",
    },
    {
        "fixture_id": 33,
        "home_team": "Netherlands",
        "away_team": "Sweden",
        "home_goals": 5,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. The Netherlands delivered a dominant 5-1 victory over Sweden, a major positive signal and a sharp correction to Sweden's early tournament momentum.",
    },
    {
        "fixture_id": 26,
        "home_team": "Germany",
        "away_team": "Ivory Coast",
        "home_goals": 2,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Germany beat Ivory Coast 2-1 in a significant Group E match between two positive early storylines.",
    },
    {
        "fixture_id": 28,
        "home_team": "Ecuador",
        "away_team": "Curacao",
        "home_goals": 0,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Ecuador and Curacao drew 0-0, a disappointing result for Ecuador and a useful point for Curacao.",
    },
    {
        "fixture_id": 34,
        "home_team": "Tunisia",
        "away_team": "Japan",
        "home_goals": 0,
        "away_goals": 4,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Japan produced a dominant 4-0 win over Tunisia, a strong positive signal for Japan and a major concern for Tunisia.",
    },
    {
        "fixture_id": 45,
        "home_team": "Spain",
        "away_team": "Saudi Arabia",
        "home_goals": 4,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Spain responded strongly with a 4-0 win over Saudi Arabia after their opening draw with Cape Verde.",
    },
    {
        "fixture_id": 39,
        "home_team": "Belgium",
        "away_team": "Iran",
        "home_goals": 0,
        "away_goals": 0,
        "red_cards_total": 1,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Belgium and Iran played out a 0-0 draw, with Belgium receiving a red card. Belgium remain a concern after another low-scoring result, and the dismissal may help explain the lack of attacking output.",
    },
    {
        "fixture_id": 46,
        "home_team": "Uruguay",
        "away_team": "Cape Verde",
        "home_goals": 2,
        "away_goals": 2,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Uruguay and Cape Verde drew 2-2, another competitive result from Cape Verde and a concern for Uruguay's group control.",
    },
    {
        "fixture_id": 40,
        "home_team": "New Zealand",
        "away_team": "Egypt",
        "home_goals": 1,
        "away_goals": 3,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Egypt beat New Zealand 3-1, giving Egypt a strong boost in Group G.",
    },
        {
        "fixture_id": 57,
        "home_team": "Argentina",
        "away_team": "Austria",
        "home_goals": 2,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Argentina secured a professional 2-0 victory over Austria to strengthen their position at the top of Group J. Another composed performance from one of the tournament favourites."
    },
    {
        "fixture_id": 53,
        "home_team": "France",
        "away_team": "Iraq",
        "home_goals": 3,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. France produced another convincing display, defeating Iraq 3-0. Their quality in possession and attacking depth continue to make them one of the strongest teams in the tournament."
    },
    {
        "fixture_id": 54,
        "home_team": "Senegal",
        "away_team": "Norway",
        "home_goals": 2,
        "away_goals": 3,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Norway edged an entertaining five-goal thriller 3-2 against Senegal. An important victory that underlines Norway's attacking threat."
    },
    {
        "fixture_id": 58,
        "home_team": "Jordan",
        "away_team": "Algeria",
        "home_goals": 1,
        "away_goals": 2,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Algeria recovered well to defeat Jordan 2-1 in a competitive Group J encounter, keeping qualification hopes alive."
    },
    {
        "fixture_id": 61,
        "home_team": "Portugal",
        "away_team": "Uzbekistan",
        "home_goals": 5,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Portugal delivered one of the performances of the tournament so far, sweeping Uzbekistan aside 5-0 with a dominant attacking display."
    },
    {
        "fixture_id": 69,
        "home_team": "England",
        "away_team": "Ghana",
        "home_goals": 0,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. England were held to a goalless draw by Ghana despite entering the match in strong form following their opening victory over Croatia. A frustrating result but one that still leaves qualification firmly in England's hands."
    },
    {
        "fixture_id": 70,
        "home_team": "Panama",
        "away_team": "Croatia",
        "home_goals": 0,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Croatia responded well after defeat to England by earning a valuable 1-0 victory over Panama, keeping their qualification hopes alive."
    },
    {
        "fixture_id": 18,
        "home_team": "Morocco",
        "away_team": "Haiti",
        "home_goals": 4,
        "away_goals": 2,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Morocco produced an impressive attacking display to defeat Haiti 4-2 in an entertaining encounter, reinforcing their credentials as one of the stronger sides in Group C."
    },
    {
        "fixture_id": 17,
        "home_team": "Scotland",
        "away_team": "Brazil",
        "home_goals": 0,
        "away_goals": 3,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Brazil bounced back strongly with a commanding 3-0 victory over Scotland, demonstrating the attacking quality expected from one of the tournament favourites."
    },
    {
        "fixture_id": 62,
        "home_team": "Colombia",
        "away_team": "DR Congo",
        "home_goals": 1,
        "away_goals": 0,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Colombia secured a hard-fought 1-0 victory over DR Congo to strengthen their position in Group K. A disciplined defensive display complemented an efficient attacking performance."
    },
    {
        "fixture_id": 12,
        "home_team": "Bosnia and Herzegovina",
        "away_team": "Qatar",
        "home_goals": 3,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": False,
        "host_team": None,
        "observer_notes": "Did not watch. Bosnia and Herzegovina claimed a deserved 3-1 victory over Qatar to finish their group campaign on a positive note."
    },
    {
        "fixture_id": 11,
        "home_team": "Switzerland",
        "away_team": "Canada",
        "home_goals": 2,
        "away_goals": 1,
        "red_cards_total": None,
        "host_team_involved": True,
        "host_team": "Canada",
        "observer_notes": "Did not watch. Switzerland defeated host nation Canada 2-1 in an important Group B encounter. Despite the defeat, Canada had already produced some outstanding performances earlier in the tournament."
    },
]


def classify_result(home_goals, away_goals):
    if home_goals > away_goals:
        return "home_win"
    if home_goals < away_goals:
        return "away_win"
    return "draw"


def build_actual_results():
    df = pd.DataFrame(ACTUAL_RESULTS)

    df["actual_result"] = df.apply(
        lambda row: classify_result(row["home_goals"], row["away_goals"]),
        axis=1,
    )

    df["total_goals"] = df["home_goals"] + df["away_goals"]
    df["both_teams_scored"] = (df["home_goals"] > 0) & (df["away_goals"] > 0)
    df["over_25_goals"] = df["total_goals"] > 2.5

    return df


def merge_with_fixtures(actuals):
    fixtures = pd.read_csv(FIXTURES_PATH)

    review = actuals.merge(
        fixtures[["fixture_id", "stage", "group"]],
        on="fixture_id",
        how="left",
    )

    return review


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 13 - ACTUAL RESULTS TRACKER")
    print("=" * 80)

    actuals = build_actual_results()
    review = merge_with_fixtures(actuals)

    actuals.to_csv(ACTUAL_RESULTS_PATH, index=False)
    review.to_csv(REVIEW_OUTPUT_PATH, index=False)

    print(f"Actual results tracked: {len(actuals):,}")
    print(f"Saved actual results:   {ACTUAL_RESULTS_PATH}")
    print(f"Saved review file:      {REVIEW_OUTPUT_PATH}")

    print("-" * 80)
    print(
        review[
            [
                "fixture_id",
                "group",
                "home_team",
                "away_team",
                "home_goals",
                "away_goals",
                "actual_result",
                "host_team_involved",
                "host_team",
                "total_goals",
                "over_25_goals",
                "both_teams_scored",
            ]
        ].to_string(index=False)
    )

    print("-" * 80)

    host_matches = review[review["host_team_involved"] == True]
    if not host_matches.empty:
        print("Host nation match summary:")
        print(
            host_matches[
                [
                    "host_team",
                    "home_team",
                    "away_team",
                    "home_goals",
                    "away_goals",
                    "actual_result",
                ]
            ].to_string(index=False)
        )

    print("=" * 80)


if __name__ == "__main__":
    main()
