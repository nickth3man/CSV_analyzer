#!/usr/bin/env python3
"""Populate franchises table with NBA franchise history information.

TODO: ROADMAP Phase 4.2 - Implement franchises data population
- Current Status: Not yet implemented
- Requirements:
  1. Track complete franchise history including relocations
  2. Store: franchise_id, name, city, founded_year, folded_year
  3. Link to team_id (franchises can have multiple team identities)
  4. Track: relocations, name changes, ownership changes
- Use Cases:
  - Franchise history analysis
  - Team legacy and records
  - Relocation patterns
  - Historical context for team comparisons
- Priority: LOW (Phase 4.2)
- Data Source: NBA official records, Basketball Reference, Wikipedia
Reference: ROADMAP.md Phase 4.2

This script will fetch NBA franchise history and populate the franchises table.

Planned Features:
- Complete franchise lineage (e.g., Seattle SuperSonics -> Oklahoma City Thunder)
- Track name changes (e.g., New Orleans Hornets -> New Orleans Pelicans)
- Store relocation history with dates
- Link championships and records to correct franchise
- Handle defunct franchises (e.g., Vancouver Grizzlies)

Usage (planned):
    # Populate all NBA franchises
    python scripts/populate/populate_franchises.py

    # Include defunct franchises
    python scripts/populate/populate_franchises.py --include-defunct

    # For specific teams/franchises
    python scripts/populate/populate_franchises.py --teams LAL GSW

Franchise History Examples:
- Charlotte Hornets (1988-2002) -> New Orleans Hornets (2002-2013) -> New Orleans Pelicans (2013-present)
- Charlotte Bobcats (2004-2014) -> Charlotte Hornets (2014-present) [reclaimed history]
- Seattle SuperSonics (1967-2008) -> Oklahoma City Thunder (2008-present)
- Vancouver Grizzlies (1995-2001) -> Memphis Grizzlies (2001-present)
- New Jersey Nets -> Brooklyn Nets

Data to Track:
- Franchise ID (permanent identifier)
- Team name history (with date ranges)
- City/location history (with date ranges)
- Founded/folded dates
- Championships (by location/name)
- Retired numbers
- Hall of Famers associated with franchise
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


def populate_franchises(*args, **kwargs):
    """
    Populate the franchises table with complete franchise history and lineage data.
    
    This function is intended to ingest franchise history (relocations, name changes, eras),
    store canonical franchise timelines, and associate championships with the correct franchise eras.
    Currently unimplemented.
    
    Raises:
        NotImplementedError: Always raised until the population logic required by ROADMAP.md Phase 4.2 is implemented.
    """
    logger.error("Franchises population not yet implemented")
    logger.info("TODO: See ROADMAP.md Phase 4.2 for implementation requirements")
    raise NotImplementedError(
        "Franchises population is planned but not yet implemented. "
        "See ROADMAP.md Phase 4.2 for requirements."
    )


def main():
    """
    CLI entry point that announces the franchises-population feature is unimplemented and exits.
    
    Logs multiple warning messages describing the placeholder status and roadmap decisions required, then terminates the process with exit status 1.
    """
    parser = argparse.ArgumentParser(
        description="Populate franchises data (NOT YET IMPLEMENTED)",
    )
    parser.add_argument("--teams", nargs="+", help="Specific team abbreviations")
    parser.add_argument("--include-defunct", action="store_true",
                        help="Include defunct franchises")

    _args = parser.parse_args()

    logger.warning("=" * 70)
    logger.warning("FRANCHISES POPULATION - NOT YET IMPLEMENTED")
    logger.warning("=" * 70)
    logger.warning("This script is a placeholder for future development.")
    logger.warning("See ROADMAP.md Phase 4.2 for implementation requirements.")
    logger.warning("")
    logger.warning("Key Decisions Needed:")
    logger.warning("1. Data source for franchise history")
    logger.warning("2. How to handle complex franchise lineages (Charlotte)")
    logger.warning("3. Schema for tracking relocations and name changes")
    logger.warning("4. How to attribute championships (location vs franchise)")
    logger.warning("=" * 70)

    sys.exit(1)


if __name__ == "__main__":
    main()