#!/usr/bin/env python3
"""Populate salaries table with NBA player salary data.

TODO: ROADMAP Phase 3.3 - Implement salary data population
- Current Status: Not yet implemented
- Requirements:
  1. Identify reliable salary data source (NBA API doesn't provide this)
  2. Options:
     a. Scrape from Basketball Reference (salary pages)
     b. Use HoopsHype salary database API
     c. Use Spotrac NBA salary data
     d. ESPN salary data
  3. Store player_id, season, team_id, salary amount, contract details
  4. Handle multi-year contracts, options, guarantees
- Use Cases:
  - Value analysis (production per dollar)
  - Cap space tracking
  - Contract comparison
  - Team payroll analysis
- Priority: MEDIUM (Phase 3.3)
- Blocks: Value-based player analysis
Reference: docs/roadmap.md Phase 3.3

This script will fetch NBA player salary information and populate the salaries table.

Planned Features:
- Support multiple data sources for salary information
- Track salary by player, team, season
- Store contract details (years, options, guaranteed amounts)
- Support historical salary data
- Validate against team salary cap totals

Usage (planned):
    # Populate salaries for a specific season
    python scripts/populate/populate_salaries.py --seasons 2023-24

    # For specific teams
    python scripts/populate/populate_salaries.py --teams LAL GSW

    # All available seasons
    python scripts/populate/populate_salaries.py --all-seasons

Potential Data Sources:
- Basketball Reference: https://www.basketball-reference.com/contracts/
- HoopsHype: https://hoopshype.com/salaries/
- Spotrac: https://www.spotrac.com/nba/
- ESPN (requires scraping)

Note: Salary data may require web scraping or external API access
"""

import argparse
import sys
from typing import Any, NoReturn


from src.scripts.populate.helpers import configure_logging
from src.scripts.populate.placeholders import log_placeholder_banner, raise_not_implemented


# Configure logging
configure_logging()


def populate_salaries(*args: Any, **kwargs: Any) -> NoReturn:
    """Placeholder for populating the NBA salaries table.

    This function is not implemented and will raise NotImplementedError directing implementers to docs/roadmap.md Phase 3.3 for requirements, data source options, and implementation notes.

    Raises:
        NotImplementedError: Always raised with a message pointing to docs/roadmap.md Phase 3.3.
    """
    raise_not_implemented(
        "Salary",
        "3.3",
        message=(
            "Salary population is planned but not yet implemented. "
            "See docs/roadmap.md Phase 3.3 for requirements and data source options."
        ),
    )


def main() -> None:
    """Command-line entry point that outlines salary population options and exits because the feature is unimplemented.

    Parses command-line options for seasons, teams, an all-seasons flag, and a data-source choice; logs roadmap notes and key implementation decisions required, then terminates the process with exit status 1. See docs/roadmap.md Phase 3.3 for implementation requirements.
    """
    parser = argparse.ArgumentParser(
        description="Populate salary data (NOT YET IMPLEMENTED)",
    )
    parser.add_argument("--seasons", nargs="+", help="Seasons to process")
    parser.add_argument("--teams", nargs="+", help="Specific team abbreviations")
    parser.add_argument(
        "--all-seasons", action="store_true", help="All available seasons"
    )
    parser.add_argument(
        "--source",
        choices=["basketball-reference", "hoopshype", "spotrac"],
        default="basketball-reference",
        help="Data source to use",
    )

    _args = parser.parse_args()

    log_placeholder_banner(
        "Salary",
        "3.3",
        decisions=[
            "Select primary data source (Basketball Reference, HoopsHype, Spotrac)",
            "Implement web scraping or API integration",
            "Define schema for contract details (options, guarantees, etc.)",
        ],
    )

    sys.exit(1)


if __name__ == "__main__":
    main()
