#!/usr/bin/env python3
"""Populate arenas table with NBA arena/venue information.

TODO: ROADMAP Phase 4.1 - Implement arenas data population
- Current Status: Not yet implemented (0 rows)
- Requirements:
  1. Fetch arena/venue information for all NBA teams
  2. Track: arena_id, name, city, state, capacity, opened_year, closed_year
  3. Support historical arenas (teams that moved/changed venues)
  4. Link to team_id (teams can have multiple arenas over time)
- Use Cases:
  - Home court advantage analysis
  - Venue-specific performance
  - Historical context for team locations
  - Travel distance calculations
- Priority: LOW (Phase 4.1)
- Data Source: NBA API, Wikipedia, or manual data entry
Reference: docs/roadmap.md Phase 4.1

This script will fetch NBA arena/venue data and populate the arenas table.

Planned Features:
- Comprehensive arena details (name, location, capacity, etc.)
- Historical arena tracking (including defunct venues)
- Link arenas to teams with date ranges
- Store geographical coordinates for distance calculations
- Track arena renovations and capacity changes

Usage (planned):
    # Populate all current NBA arenas
    python scripts/populate/populate_arenas.py

    # Include historical arenas
    python scripts/populate/populate_arenas.py --include-historical

    # For specific teams
    python scripts/populate/populate_arenas.py --teams LAL GSW

Arena Information to Track:
- Arena name (current and historical names)
- Location (city, state, country)
- Capacity (original and current after renovations)
- Opened/Closed dates
- Geographical coordinates (lat/long)
- Surface type
- Roof type (indoor/outdoor)
- Team(s) that use the arena

Potential Data Sources:
- NBA official team pages
- Wikipedia arena data
- Basketball Reference venue info
- Manual data entry from reliable sources
"""

import argparse
import logging
import sys


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def populate_arenas(*args, **kwargs):
    """TODO: ROADMAP Phase 4.1 (see docs/roadmap.md for requirements).

    Populate the arenas table with comprehensive NBA venue data (placeholder - not implemented).

    Intended to collect arena metadata, associate arenas with teams and date ranges, track arena name changes, and add geographic coordinates as specified in docs/roadmap.md Phase 4.1.

    Raises:
        NotImplementedError: Function is a placeholder and not yet implemented; see docs/roadmap.md Phase 4.1 for requirements.
    """
    logger.error("Arenas population not yet implemented")
    logger.info("TODO: See docs/roadmap.md Phase 4.1 for implementation requirements")
    raise NotImplementedError(
        "Arenas population is planned but not yet implemented. "
        "See docs/roadmap.md Phase 4.1 for requirements."
    )


def main():
    """CLI entry point that parses command-line options and exits indicating the feature is not implemented.

    Parses the optional `--teams` and `--include-historical` arguments, emits warning logs describing that arena population is a placeholder and listing key design decisions, and then exits the process with status code 1.
    """
    parser = argparse.ArgumentParser(
        description="Populate arenas data (NOT YET IMPLEMENTED)",
    )
    parser.add_argument("--teams", nargs="+", help="Specific team abbreviations")
    parser.add_argument(
        "--include-historical",
        action="store_true",
        help="Include defunct/historical arenas",
    )

    _args = parser.parse_args()

    logger.warning("=" * 70)
    logger.warning("ARENAS POPULATION - NOT YET IMPLEMENTED")
    logger.warning("=" * 70)
    logger.warning("This script is a placeholder for future development.")
    logger.warning("See docs/roadmap.md Phase 4.1 for implementation requirements.")
    logger.warning("")
    logger.warning("Current arenas table status: 0 rows")
    logger.warning("")
    logger.warning("Key Decisions Needed:")
    logger.warning("1. Data source (NBA API, Wikipedia, manual entry)")
    logger.warning("2. Schema for arena-team relationships (with date ranges)")
    logger.warning("3. How to track arena name changes over time")
    logger.warning("4. Geographical coordinate source")
    logger.warning("=" * 70)

    sys.exit(1)


if __name__ == "__main__":
    main()
