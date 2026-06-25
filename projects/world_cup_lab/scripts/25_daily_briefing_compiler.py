"""
================================================================================
WORLD CUP LAB
SCRIPT 25 - DAILY BRIEFING COMPILER
================================================================================

Purpose:
    Combine the key daily World Cup Lab reports into one readable briefing.

Inputs:
    outputs/daily_command_centre_report.txt
    outputs/tournament_storylines_report.txt
    outputs/prematch_intelligence_report.txt

Outputs:
    outputs/world_cup_daily_briefing.txt
================================================================================
"""

from pathlib import Path
from datetime import datetime


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "outputs"

COMMAND_CENTRE_PATH = OUTPUT_DIR / "daily_command_centre_report.txt"
STORYLINES_PATH = OUTPUT_DIR / "tournament_storylines_report.txt"
PREMATCH_PATH = OUTPUT_DIR / "prematch_intelligence_report.txt"

BRIEFING_OUTPUT_PATH = OUTPUT_DIR / "world_cup_daily_briefing.txt"


def read_section(path):
    if not path.exists():
        return f"[MISSING FILE] {path}\n"

    return path.read_text(encoding="utf-8")


def build_briefing():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    command_centre = read_section(COMMAND_CENTRE_PATH)
    storylines = read_section(STORYLINES_PATH)
    prematch = read_section(PREMATCH_PATH)

    lines = []

    lines.append("=" * 80)
    lines.append("WORLD CUP LAB - DAILY BRIEFING")
    lines.append("=" * 80)
    lines.append(f"Generated: {now}")
    lines.append("")
    lines.append("This briefing combines:")
    lines.append("- Daily Command Centre")
    lines.append("- Tournament Storylines Engine")
    lines.append("- Pre-Match Intelligence Engine")
    lines.append("=" * 80)

    lines.append("")
    lines.append("\n\n" + "#" * 80)
    lines.append("SECTION 1 - DAILY COMMAND CENTRE")
    lines.append("#" * 80)
    lines.append(command_centre)

    lines.append("")
    lines.append("\n\n" + "#" * 80)
    lines.append("SECTION 2 - TOURNAMENT STORYLINES")
    lines.append("#" * 80)
    lines.append(storylines)

    lines.append("")
    lines.append("\n\n" + "#" * 80)
    lines.append("SECTION 3 - PRE-MATCH INTELLIGENCE")
    lines.append("#" * 80)
    lines.append(prematch)

    lines.append("")
    lines.append("=" * 80)
    lines.append("END OF DAILY BRIEFING")
    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    print("=" * 80)
    print("WORLD CUP LAB")
    print("SCRIPT 25 - DAILY BRIEFING COMPILER")
    print("=" * 80)

    briefing = build_briefing()

    BRIEFING_OUTPUT_PATH.write_text(briefing, encoding="utf-8")

    print(f"Daily briefing saved: {BRIEFING_OUTPUT_PATH}")
    print("=" * 80)


if __name__ == "__main__":
    main()
