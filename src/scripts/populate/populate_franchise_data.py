#!/usr/bin/env python3
"""Populate franchise_history and franchise_leaders tables from NBA API.

This script fetches franchise history and all-time statistical leaders:
- franchise_history: Historical team information (name changes, relocations, years active)
- franchise_leaders: All-time career leaders for each franchise

Usage:
    # Full population (all teams)
    python scripts/populate/populate_franchise_data.py

    # Specific teams only
    python scripts/populate/populate_franchise_data.py --team-ids 1610612747 1610612744

    # Dry run
    python scripts/populate/populate_franchise_data.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Any

import pandas as pd

from src.scripts.populate.api_client import get_client
from src.scripts.populate.base import BasePopulator
from src.scripts.populate.config import get_db_path
from src.scripts.populate.helpers import configure_logging


configure_logging()
logger = logging.getLogger(__name__)


# Expected columns for the franchise_history table
FRANCHISE_HISTORY_COLUMNS = [
    "league_id",
    "team_id",
    "team_city",
    "team_name",
    "start_year",
    "end_year",
    "years_active",
    "games_played",
    "wins",
    "losses",
    "win_pct",
    "playoff_appearances",
    "division_titles",
    "conference_titles",
    "championships",
    "filename",
]

# Expected columns for the franchise_leaders table
FRANCHISE_LEADERS_COLUMNS = [
    "team_id",
    "player_id",
    "player_name",
    "stat_category",
    "stat_value",
    "stat_rank",
    "games_played",
    "field_goals_made",
    "field_goals_attempted",
    "three_pointers_made",
    "three_pointers_attempted",
    "free_throws_made",
    "free_throws_attempted",
    "filename",
]


class FranchiseHistoryPopulator(BasePopulator):
    """Populator for franchise_history table."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_table_name(self) -> str:
        return "franchise_history"

    def get_key_columns(self) -> list[str]:
        return ["team_id", "start_year"]

    def get_expected_columns(self) -> list[str]:
        return FRANCHISE_HISTORY_COLUMNS

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        """Fetch franchise history for all teams (single API call)."""
        logger.info("Fetching franchise history for all teams...")

        try:
            df = self.client.get_franchise_history(league_id="00")
            self.metrics.api_calls += 1

            if df is not None and not df.empty:
                logger.info("Found %d franchise history records", len(df))
                return df
            logger.info("No franchise history data returned")
            return None

        except Exception as e:
            logger.exception("Error fetching franchise history: %s", e)
            self.metrics.add_error(str(e), {"operation": "fetch_franchise_history"})
            return None

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform franchise history data to match schema."""
        if df.empty:
            return df

        output = pd.DataFrame()

        # Map API columns to schema columns
        output["league_id"] = df.get("LEAGUE_ID", "00")
        output["team_id"] = pd.to_numeric(df["TEAM_ID"], errors="coerce").astype(
            "Int64"
        )
        output["team_city"] = df.get("TEAM_CITY", "")
        output["team_name"] = df.get("TEAM_NAME", "")
        output["start_year"] = pd.to_numeric(
            df.get("START_YEAR"), errors="coerce"
        ).astype("Int64")
        output["end_year"] = pd.to_numeric(df.get("END_YEAR"), errors="coerce").astype(
            "Int64"
        )
        output["years_active"] = pd.to_numeric(df.get("YEARS"), errors="coerce").astype(
            "Int64"
        )
        output["games_played"] = pd.to_numeric(df.get("GAMES"), errors="coerce").astype(
            "Int64"
        )
        output["wins"] = pd.to_numeric(df.get("WINS"), errors="coerce").astype("Int64")
        output["losses"] = pd.to_numeric(df.get("LOSSES"), errors="coerce").astype(
            "Int64"
        )
        output["win_pct"] = pd.to_numeric(df.get("WIN_PCT"), errors="coerce")
        output["playoff_appearances"] = pd.to_numeric(
            df.get("PO_APPEARANCES"), errors="coerce"
        ).astype("Int64")
        output["division_titles"] = pd.to_numeric(
            df.get("DIV_TITLES"), errors="coerce"
        ).astype("Int64")
        output["conference_titles"] = pd.to_numeric(
            df.get("CONF_TITLES"), errors="coerce"
        ).astype("Int64")
        output["championships"] = pd.to_numeric(
            df.get("LEAGUE_TITLES"), errors="coerce"
        ).astype("Int64")

        # Add filename for provenance
        output["filename"] = "nba_api.franchisehistory"

        # Ensure all expected columns exist
        for col in FRANCHISE_HISTORY_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return output[FRANCHISE_HISTORY_COLUMNS]

    def validate_data(self, df: pd.DataFrame, **kwargs) -> bool:
        """Validate franchise history data."""
        if df.empty:
            logger.warning("Empty DataFrame provided for validation")
            return False

        # Check required columns exist
        required_cols = ["team_id", "start_year", "team_name"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logger.error("Missing required columns: %s", missing)
            return False

        # Validate team_id is not null
        null_team_ids = df["team_id"].isna().sum()
        if null_team_ids > 0:
            logger.warning("Found %d records with null team_id", null_team_ids)

        # Validate start_year is reasonable (1946 is NBA founding year)
        if "start_year" in df.columns:
            invalid_years = (
                (df["start_year"] < 1946) | (df["start_year"] > 2030)
            ).sum()
            if invalid_years > 0:
                logger.warning(
                    "Found %d records with invalid start_year", invalid_years
                )

        # Validate win_pct is between 0 and 1
        if "win_pct" in df.columns:
            invalid_pct = ((df["win_pct"] < 0) | (df["win_pct"] > 1)).sum()
            if invalid_pct > 0:
                logger.warning("Found %d records with invalid win_pct", invalid_pct)

        logger.info("Validation passed for %d records", len(df))
        return True


class FranchiseLeadersPopulator(BasePopulator):
    """Populator for franchise_leaders table."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._fetched_team_keys: list[str] = []

    def get_table_name(self) -> str:
        return "franchise_leaders"

    def get_key_columns(self) -> list[str]:
        return ["team_id", "stat_category", "player_id"]

    def get_expected_columns(self) -> list[str]:
        return FRANCHISE_LEADERS_COLUMNS

    def _get_team_ids(self, team_ids: list[int] | None = None) -> list[int]:
        """Get team IDs to process.

        Args:
            team_ids: Optional list of specific team IDs

        Returns:
            List of team IDs to fetch leaders for
        """
        if team_ids:
            return team_ids

        # Get all current NBA team IDs from static data
        teams = self.client.get_all_teams()
        return [team["id"] for team in teams]

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        """Fetch franchise leaders for all teams."""
        team_ids: list[int] | None = kwargs.get("team_ids")
        resume = kwargs.get("resume", True)

        teams_to_process = self._get_team_ids(team_ids)
        all_data = []

        logger.info(
            "Fetching franchise leaders for %d teams",
            len(teams_to_process),
        )

        for idx, team_id in enumerate(teams_to_process, start=1):
            progress_key = f"team_{team_id}"

            # Check if already completed
            if resume and self.progress.is_completed(progress_key):
                logger.info(
                    "[%d/%d] Skipping team %d (already completed)",
                    idx,
                    len(teams_to_process),
                    team_id,
                )
                continue

            logger.info(
                "[%d/%d] Fetching leaders for team %d...",
                idx,
                len(teams_to_process),
                team_id,
            )

            try:
                df = self.client.get_franchise_leaders(team_id=team_id)
                self.metrics.api_calls += 1

                if df is not None and not df.empty:
                    # Add team_id as metadata (may not be in response)
                    df["_team_id"] = team_id
                    all_data.append(df)
                    logger.info(
                        "  Found %d leader records for team %d",
                        len(df),
                        team_id,
                    )
                    # Track for deferred progress marking
                    self._fetched_team_keys.append(progress_key)
                else:
                    logger.info("  No leader data for team %d", team_id)

                # Respect rate limiting
                time.sleep(self.client.config.request_delay)

            except Exception as e:
                logger.exception(
                    "Error fetching leaders for team %d: %s",
                    team_id,
                    e,
                )
                self.progress.add_error(progress_key, str(e))
                self.metrics.add_error(str(e), {"team_id": team_id})

        if not all_data:
            logger.info("No franchise leader data fetched")
            return None

        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info("Total leader records fetched: %d", len(combined_df))
        return combined_df

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform franchise leaders data to match schema.

        The API returns one row per player with multiple stat columns.
        We transform this to a normalized format with one row per stat category.
        """
        if df.empty:
            return df

        # Stat categories available in the API response
        stat_categories = [
            ("PTS", "pts_rank", "PTS"),
            ("AST", "ast_rank", "AST"),
            ("REB", "reb_rank", "REB"),
            ("BLK", "blk_rank", "BLK"),
            ("STL", "stl_rank", "STL"),
        ]

        all_rows = []

        for _, row in df.iterrows():
            team_id = row.get("TEAM_ID") or row.get("_team_id")
            player_id = row.get("PLAYER_ID")
            player_name = row.get("PLAYER", "")

            # Extract common stats
            games_played = row.get("GP")
            fgm = row.get("FGM")
            fga = row.get("FGA")
            fg3m = row.get("FG3M")
            fg3a = row.get("FG3A")
            ftm = row.get("FTM")
            fta = row.get("FTA")

            # Create a row for each stat category
            for stat_col, rank_col, category_name in stat_categories:
                stat_value = row.get(stat_col)
                stat_rank = row.get(rank_col.upper(), row.get(rank_col))

                if pd.notna(stat_value) and stat_value is not None:
                    all_rows.append(
                        {
                            "team_id": team_id,
                            "player_id": player_id,
                            "player_name": player_name,
                            "stat_category": category_name,
                            "stat_value": stat_value,
                            "stat_rank": stat_rank,
                            "games_played": games_played,
                            "field_goals_made": fgm,
                            "field_goals_attempted": fga,
                            "three_pointers_made": fg3m,
                            "three_pointers_attempted": fg3a,
                            "free_throws_made": ftm,
                            "free_throws_attempted": fta,
                            "filename": "nba_api.franchiseleaders",
                        }
                    )

        if not all_rows:
            return pd.DataFrame(columns=FRANCHISE_LEADERS_COLUMNS)

        output = pd.DataFrame(all_rows)

        # Convert types
        output["team_id"] = pd.to_numeric(output["team_id"], errors="coerce").astype(
            "Int64"
        )
        output["player_id"] = pd.to_numeric(
            output["player_id"], errors="coerce"
        ).astype("Int64")
        output["stat_value"] = pd.to_numeric(output["stat_value"], errors="coerce")
        output["stat_rank"] = pd.to_numeric(
            output["stat_rank"], errors="coerce"
        ).astype("Int64")
        output["games_played"] = pd.to_numeric(
            output["games_played"], errors="coerce"
        ).astype("Int64")
        output["field_goals_made"] = pd.to_numeric(
            output["field_goals_made"], errors="coerce"
        ).astype("Int64")
        output["field_goals_attempted"] = pd.to_numeric(
            output["field_goals_attempted"], errors="coerce"
        ).astype("Int64")
        output["three_pointers_made"] = pd.to_numeric(
            output["three_pointers_made"], errors="coerce"
        ).astype("Int64")
        output["three_pointers_attempted"] = pd.to_numeric(
            output["three_pointers_attempted"], errors="coerce"
        ).astype("Int64")
        output["free_throws_made"] = pd.to_numeric(
            output["free_throws_made"], errors="coerce"
        ).astype("Int64")
        output["free_throws_attempted"] = pd.to_numeric(
            output["free_throws_attempted"], errors="coerce"
        ).astype("Int64")

        # Ensure all expected columns exist
        for col in FRANCHISE_LEADERS_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return output[FRANCHISE_LEADERS_COLUMNS]

    def validate_data(self, df: pd.DataFrame, **kwargs) -> bool:
        """Validate franchise leaders data."""
        if df.empty:
            logger.warning("Empty DataFrame provided for validation")
            return False

        # Check required columns exist
        required_cols = ["team_id", "player_id", "stat_category", "stat_value"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logger.error("Missing required columns: %s", missing)
            return False

        # Validate team_id is not null
        null_team_ids = df["team_id"].isna().sum()
        if null_team_ids > 0:
            logger.warning("Found %d records with null team_id", null_team_ids)

        # Validate player_id is not null
        null_player_ids = df["player_id"].isna().sum()
        if null_player_ids > 0:
            logger.warning("Found %d records with null player_id", null_player_ids)

        # Validate stat_category is valid
        valid_categories = {"PTS", "AST", "REB", "BLK", "STL"}
        invalid_categories = ~df["stat_category"].isin(valid_categories)
        if invalid_categories.sum() > 0:
            logger.warning(
                "Found %d records with invalid stat_category", invalid_categories.sum()
            )

        # Validate stat_value is non-negative
        if "stat_value" in df.columns:
            negative_values = (df["stat_value"] < 0).sum()
            if negative_values > 0:
                logger.warning(
                    "Found %d records with negative stat_value", negative_values
                )

        logger.info("Validation passed for %d records", len(df))
        return True

    def pre_run_hook(self, **kwargs) -> None:
        """Reset fetched keys for this run."""
        self._fetched_team_keys = []

    def post_run_hook(self, **kwargs) -> None:
        """Mark fetched teams as completed after successful database writes."""
        dry_run = kwargs.get("dry_run", False)
        if dry_run:
            logger.info(
                "DRY RUN - not marking progress for fetched teams (data was not written)"
            )
            return

        if self._fetched_team_keys:
            for progress_key in self._fetched_team_keys:
                self.progress.mark_completed(progress_key)
            self.progress.save()
            logger.info(
                "Marked %d teams as completed",
                len(self._fetched_team_keys),
            )


def populate_franchise_history(
    db_path: str | None = None,
    delay: float = 0.6,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Populate franchise_history table.

    Args:
        db_path: Path to DuckDB database
        delay: Delay between API requests in seconds
        dry_run: If True, don't actually insert data

    Returns:
        Dictionary with population statistics
    """
    db_path = db_path or str(get_db_path())

    # Create client with custom delay
    client = get_client()
    client.config.request_delay = delay

    logger.info("=" * 70)
    logger.info("NBA FRANCHISE HISTORY POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Request Delay: {delay}s")

    # Create and run populator
    populator = FranchiseHistoryPopulator(
        db_path=db_path,
        client=client,
    )

    return populator.run(
        dry_run=dry_run,
    )


def populate_franchise_leaders(
    db_path: str | None = None,
    team_ids: list[int] | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Populate franchise_leaders table.

    Args:
        db_path: Path to DuckDB database
        team_ids: List of team IDs to fetch leaders for (None for all teams)
        delay: Delay between API requests in seconds
        reset_progress: Reset progress tracking before starting
        dry_run: If True, don't actually insert data

    Returns:
        Dictionary with population statistics
    """
    db_path = db_path or str(get_db_path())

    # Create client with custom delay
    client = get_client()
    client.config.request_delay = delay

    logger.info("=" * 70)
    logger.info("NBA FRANCHISE LEADERS POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    if team_ids:
        logger.info(f"Team IDs: {team_ids}")
    else:
        logger.info("Team IDs: All teams")
    logger.info(f"Request Delay: {delay}s")

    # Create and run populator
    populator = FranchiseLeadersPopulator(
        db_path=db_path,
        client=client,
    )

    return populator.run(
        team_ids=team_ids,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def populate_franchise_data(
    db_path: str | None = None,
    team_ids: list[int] | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
    skip_history: bool = False,
    skip_leaders: bool = False,
) -> dict[str, Any]:
    """Main function to populate all franchise data tables.

    Args:
        db_path: Path to DuckDB database
        team_ids: List of team IDs to fetch leaders for (None for all teams)
        delay: Delay between API requests in seconds
        reset_progress: Reset progress tracking before starting
        dry_run: If True, don't actually insert data
        skip_history: If True, skip franchise history population
        skip_leaders: If True, skip franchise leaders population

    Returns:
        Dictionary with combined population statistics
    """
    results = {
        "franchise_history": None,
        "franchise_leaders": None,
    }

    if not skip_history:
        logger.info("\n" + "=" * 70)
        logger.info("STEP 1: Populating franchise_history table")
        logger.info("=" * 70 + "\n")
        results["franchise_history"] = populate_franchise_history(
            db_path=db_path,
            delay=delay,
            dry_run=dry_run,
        )

    if not skip_leaders:
        logger.info("\n" + "=" * 70)
        logger.info("STEP 2: Populating franchise_leaders table")
        logger.info("=" * 70 + "\n")
        results["franchise_leaders"] = populate_franchise_leaders(
            db_path=db_path,
            team_ids=team_ids,
            delay=delay,
            reset_progress=reset_progress,
            dry_run=dry_run,
        )

    # Summarize results
    logger.info("\n" + "=" * 70)
    logger.info("FRANCHISE DATA POPULATION COMPLETE")
    logger.info("=" * 70)

    total_inserted = 0
    total_errors = 0

    for table_name, stats in results.items():
        if stats:
            inserted = stats.get("records_inserted", 0)
            errors = stats.get("error_count", 0)
            total_inserted += inserted
            total_errors += errors
            logger.info(f"{table_name}: {inserted:,} records inserted, {errors} errors")

    logger.info(f"Total: {total_inserted:,} records inserted, {total_errors} errors")

    return results


def main() -> None:
    """Parse command-line arguments and run the franchise data population process."""
    parser = argparse.ArgumentParser(
        description="Populate franchise_history and franchise_leaders tables from NBA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (all teams)
  python scripts/populate/populate_franchise_data.py

  # Specific teams only (for leaders)
  python scripts/populate/populate_franchise_data.py --team-ids 1610612747 1610612744

  # Skip franchise history (only fetch leaders)
  python scripts/populate/populate_franchise_data.py --skip-history

  # Skip franchise leaders (only fetch history)
  python scripts/populate/populate_franchise_data.py --skip-leaders

  # Reset progress and start fresh
  python scripts/populate/populate_franchise_data.py --reset-progress

  # Dry run (no database writes)
  python scripts/populate/populate_franchise_data.py --dry-run
        """,
    )

    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database (default: src/backend/data/nba.duckdb)",
    )
    parser.add_argument(
        "--team-ids",
        nargs="+",
        type=int,
        help="Team IDs to fetch leaders for (default: all teams)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.6,
        help="Delay between API requests in seconds (default: 0.6)",
    )
    parser.add_argument(
        "--skip-history",
        action="store_true",
        help="Skip franchise history population",
    )
    parser.add_argument(
        "--skip-leaders",
        action="store_true",
        help="Skip franchise leaders population",
    )
    parser.add_argument(
        "--reset-progress",
        action="store_true",
        help="Reset progress tracking before starting",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually write to database",
    )

    args = parser.parse_args()

    try:
        results = populate_franchise_data(
            db_path=args.db,
            team_ids=args.team_ids,
            delay=args.delay,
            reset_progress=args.reset_progress,
            dry_run=args.dry_run,
            skip_history=args.skip_history,
            skip_leaders=args.skip_leaders,
        )

        # Check for errors in any table
        has_errors = False
        for stats in results.values():
            if stats and stats.get("error_count", 0) > 0:
                has_errors = True
                break

        if has_errors:
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
