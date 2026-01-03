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
Reference: docs/roadmap.md Phase 4.2

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
import sys
from typing import Any, NoReturn


from src.scripts.populate.helpers import configure_logging
from src.scripts.populate.placeholders import log_placeholder_banner, raise_not_implemented


# Configure logging
configure_logging()


def populate_franchises(*args: Any, **kwargs: Any) -> NoReturn:
    """Populate the franchises table with complete franchise history and lineage data.

    This function is intended to ingest franchise history (relocations, name changes, eras),
    store canonical franchise timelines, and associate championships with the correct franchise eras.
    Currently unimplemented.

    Raises:
        NotImplementedError: Always raised until the population logic required by docs/roadmap.md Phase 4.2 is implemented.
    """
    raise_not_implemented("Franchises", "4.2")


def main() -> None:
    """CLI entry point that reports the franchises-population feature is not implemented and exits.

    Logs warning messages describing the placeholder status and required roadmap decisions, then terminates the process with exit code 1.
    """
    parser = argparse.ArgumentParser(
        description="Populate franchises data (NOT YET IMPLEMENTED)",
    )
    parser.add_argument("--teams", nargs="+", help="Specific team abbreviations")
    parser.add_argument(
        "--include-defunct", action="store_true", help="Include defunct franchises"
    )

    _args = parser.parse_args()

    log_placeholder_banner(
        "Franchises",
        "4.2",
        decisions=[
            "Data source for franchise history",
            "How to handle complex franchise lineages (Charlotte)",
            "Schema for tracking relocations and name changes",
            "How to attribute championships (location vs franchise)",
        ],
    )

    sys.exit(1)


if __name__ == "__main__":
    main()
