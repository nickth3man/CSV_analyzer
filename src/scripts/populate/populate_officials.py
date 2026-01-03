#!/usr/bin/env python3
"""Populate officials_directory table with NBA referee information.

TODO: ROADMAP Phase 4.3 - Implement officials data population
- Current Status: Not yet implemented (0 rows)
- Requirements:
  1. Fetch referee/official information from NBA
  2. Track: official_id, name, jersey_number, years_experience
  3. Link officials to games they worked
  4. Support historical officials data
- Use Cases:
  - Referee assignment patterns
  - Home/away officiating bias analysis
  - Foul calling tendencies
  - Playoff officiating experience
- Priority: LOW (Phase 4.3)
- Data Source: NBA official referee roster, game officiating crews
Reference: docs/roadmap.md Phase 4.3

This script will fetch NBA referee/official data and populate the officials_directory table.

Planned Features:
- Current NBA referee roster with details
- Historical referee data
- Link referees to games they officiated
- Track referee statistics (games worked, fouls called, etc.)
- Crew chief identification

Usage (planned):
    # Populate current NBA officials
    python scripts/populate/populate_officials.py

    # Include historical officials
    python scripts/populate/populate_officials.py --include-historical

    # Link officials to games for specific seasons
    python scripts/populate/populate_officials.py --seasons 2023-24 --with-games

Official Information to Track:
- Official ID
- Full name
- Jersey number
- Years of experience
- Career start date
- Status (active/retired)
- Position (referee, crew chief, replay center)

Game-Official Linkage (separate table or view):
- game_id
- official_id
- position (referee 1, 2, 3, crew chief)
- For analysis:
  - Fouls called per game
  - Technical fouls called
  - Home team win %
  - Playoff games worked

Potential Data Sources:
- NBA official referee roster
- NBA game boxscores (officiating crew)
- Basketball Reference referee data
- NBA Last Two Minute reports (for accuracy tracking)
"""

import argparse
import sys
from typing import Any, NoReturn


from src.scripts.populate.helpers import configure_logging
from src.scripts.populate.placeholders import log_placeholder_banner, raise_not_implemented


# Configure logging
configure_logging()


def populate_officials(*args: Any, **kwargs: Any) -> NoReturn:
    """Populate the officials_directory table with referee records and game linkages.

    Intended to fetch current (and optionally historical) NBA referee data, store biographical
    and officiating statistics, and create links between officials and games. See
    docs/roadmap.md Phase 4.3 for detailed requirements and data sources.

    Raises:
        NotImplementedError: Function is a placeholder; implementation planned per docs/roadmap.md Phase 4.3.
    """
    raise_not_implemented("Officials", "4.3")


def main() -> None:
    """Command-line entry point that reports the placeholder status for officials population.

    Parses the command-line options --include-historical, --seasons, and --with-games, emits a series of warning messages describing that officials population is not yet implemented and listing required decisions, references docs/roadmap.md Phase 4.3, and exits the process with status code 1.
    """
    parser = argparse.ArgumentParser(
        description="Populate officials data (NOT YET IMPLEMENTED)",
    )
    parser.add_argument(
        "--include-historical", action="store_true", help="Include retired officials"
    )
    parser.add_argument(
        "--seasons", nargs="+", help="Link officials to games for these seasons"
    )
    parser.add_argument(
        "--with-games", action="store_true", help="Create game-official linkages"
    )

    _args = parser.parse_args()

    log_placeholder_banner(
        "Officials",
        "4.3",
        status="Current officials_directory table status: 0 rows",
        decisions=[
            "Data source (NBA official roster, game boxscores)",
            "Schema for game-official linkage",
            "What referee statistics to track",
            "How to handle historical officials (retired)",
        ],
    )

    sys.exit(1)


if __name__ == "__main__":
    main()
