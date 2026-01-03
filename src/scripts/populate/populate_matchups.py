#!/usr/bin/env python3
"""Populate matchup_stats table using LeagueSeasonMatchups endpoint.

This script fetches player defensive matchup statistics showing how offensive
players perform when defended by specific defenders. The data includes:
- Minutes spent in matchup
- Points scored
- Shot accuracy (FGM, FGA, FG%)
- Assists and turnovers in the matchup

Features:
- Uses LeagueSeasonMatchups endpoint for league-wide matchup data
- Fetches all matchups for a season in a single API call
- Supports incremental updates (skip existing seasons)
- Progress tracking and resumability
- Data validation before insertion

Usage:
    # Full population (recent seasons)
    python scripts/populate/populate_matchups.py

    # Specific seasons only
    python scripts/populate/populate_matchups.py --seasons 2025-26 2024-25

    # With custom delay for rate limiting
    python scripts/populate/populate_matchups.py --delay 1.0
"""

import argparse
import logging
import sys
import time
from typing import Any, cast

import pandas as pd

from src.scripts.populate.api_client import get_client
from src.scripts.populate.base import BasePopulator
from src.scripts.populate.config import (
    ALL_SEASONS,
    DEFAULT_SEASON_TYPES,
    get_db_path,
)
from src.scripts.populate.helpers import configure_logging, resolve_season_types


# Configure logging
configure_logging()
logger = logging.getLogger(__name__)


# Season type mapping for the API
SEASON_TYPE_MAP = {
    "Regular Season": "Regular Season",
    "Playoffs": "Playoffs",
    "Pre Season": "Pre Season",
    "All Star": "All Star",
}

# Expected columns in matchup_stats table
MATCHUP_STATS_COLUMNS = [
    "season_id",
    "season_type",
    "offensive_player_id",
    "defensive_player_id",
    "matchup_min",
    "partial_poss",
    "player_pts",
    "team_pts",
    "matchup_ast",
    "matchup_tov",
    "matchup_fgm",
    "matchup_fga",
    "matchup_fg_pct",
]


class MatchupStatsPopulator(BasePopulator):
    """Populator for matchup_stats table using LeagueSeasonMatchups endpoint."""

    def __init__(self, **kwargs) -> None:
        """Initialize the MatchupStatsPopulator.

        Forwards keyword arguments to the BasePopulator constructor, sets
        `seasons` to an empty list, `season_types` to `DEFAULT_SEASON_TYPES`,
        and initializes `_fetched_season_keys` to track seasons fetched during
        the current run (used for deferred progress marking after successful writes).
        """
        super().__init__(**kwargs)
        self.seasons: list[str] = []
        self.season_types: list[str] = DEFAULT_SEASON_TYPES
        # Track seasons fetched in current run; marked complete after DB write
        self._fetched_season_keys: list[str] = []

    def get_table_name(self) -> str:
        """Target table name for matchup statistics.

        Returns:
            The table name "matchup_stats".
        """
        return "matchup_stats"

    def get_key_columns(self) -> list[str]:
        """Return the composite primary key columns for the matchup_stats table.

        Returns:
            A list containing the key column names.
        """
        return ["season_id", "season_type", "offensive_player_id", "defensive_player_id"]

    def get_expected_columns(self) -> list[str]:
        """Return the expected column names for the matchup_stats table schema.

        Returns:
            List[str]: Column names defining the canonical matchup_stats schema.
        """
        return MATCHUP_STATS_COLUMNS

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        """Fetches matchup statistics in bulk for the given seasons and season types.

        Parameters:
            seasons (List[str], optional): Seasons to fetch (defaults to the last five seasons).
            season_types (List[str], optional): Season types to fetch (defaults to configured DEFAULT_SEASON_TYPES).

        Returns:
            Optional[pd.DataFrame]: A single DataFrame concatenating fetched records with two added columns `_season` and `_season_type`, or `None` if no data was fetched.
        """
        seasons = kwargs.get("seasons", ALL_SEASONS[-5:])  # Default: last 5 seasons
        season_types = kwargs.get("season_types", DEFAULT_SEASON_TYPES)

        all_data = []

        for season in seasons:
            for season_type in season_types:
                # Check if already populated
                progress_key = f"{season}_{season_type}"
                if self.progress.is_completed(progress_key):
                    logger.info(f"Skipping {season} {season_type} (already completed)")
                    continue

                logger.info(f"Fetching {season} {season_type}...")

                try:
                    # Fetch league-wide matchup data
                    df = self._fetch_season_data(season, season_type)

                    if df is not None and not df.empty:
                        df["_season"] = season
                        df["_season_type"] = season_type
                        all_data.append(df)
                        logger.info(f"  Found {len(df):,} matchup records")
                        self.metrics.api_calls += 1
                        # Track this season for deferred progress marking
                        # (completion will be marked only after successful DB write)
                        self._fetched_season_keys.append(progress_key)

                    # Respect rate limiting
                    time.sleep(self.client.config.request_delay)

                except Exception as e:
                    logger.exception(f"Error fetching {season} {season_type}: {e}")
                    self.progress.add_error(progress_key, str(e))
                    self.metrics.add_error(
                        str(e),
                        {
                            "season": season,
                            "season_type": season_type,
                        },
                    )

        if not all_data:
            return None

        return pd.concat(all_data, ignore_index=True)

    def _fetch_season_data(self, season: str, season_type: str) -> pd.DataFrame | None:
        """Retrieve matchup stats for a given season and season type.

        Maps the provided season_type to the API's expected value and requests the full set
        of matchup statistics for that season (no player filters).

        Returns:
            pd.DataFrame or None: A DataFrame containing matchup records for the
            specified season and season type, or `None` if no data available.

        Raises:
            Exception: Propagates errors from the underlying API call.
        """
        try:
            # Map season type to API parameter
            api_season_type = SEASON_TYPE_MAP.get(season_type, season_type)
            return self.client.get_matchup_stats(
                season=season,
                season_type=api_season_type,
            )
        except Exception as e:
            logger.exception(f"API error: {e}")
            raise

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform a LeagueSeasonMatchups DataFrame into the matchup_stats table schema.

        Converts and maps source columns from the LeagueSeasonMatchups payload into the expected
        MATCHUP_STATS_COLUMNS order. Numeric fields are coerced to appropriate types.

        Parameters:
            df (pd.DataFrame): DataFrame returned by the LeagueSeasonMatchups endpoint.
            **kwargs: Ignored; present for interface compatibility.

        Returns:
            pd.DataFrame: A DataFrame with columns ordered as MATCHUP_STATS_COLUMNS,
            containing transformed and type-normalized matchup statistics.
        """
        if df.empty:
            return df

        output = pd.DataFrame()

        # Extract season_id from _season (e.g., "2024-25" -> "22024")
        # The API uses format like "22024" for 2024-25 season
        output["season_id"] = df["_season"].apply(self._convert_season_to_id)
        output["season_type"] = df["_season_type"].fillna("Regular Season")

        # Player identifiers
        output["offensive_player_id"] = cast(
            "pd.Series", pd.to_numeric(df["OFF_PLAYER_ID"], errors="coerce")
        ).astype("Int64")
        output["defensive_player_id"] = cast(
            "pd.Series", pd.to_numeric(df["DEF_PLAYER_ID"], errors="coerce")
        ).astype("Int64")

        # Matchup statistics
        stat_cols = [
            ("MATCHUP_MIN", "matchup_min"),
            ("PARTIAL_POSS", "partial_poss"),
            ("PLAYER_PTS", "player_pts"),
            ("TEAM_PTS", "team_pts"),
            ("MATCHUP_AST", "matchup_ast"),
            ("MATCHUP_TOV", "matchup_tov"),
            ("MATCHUP_FGM", "matchup_fgm"),
            ("MATCHUP_FGA", "matchup_fga"),
        ]

        for api_col, our_col in stat_cols:
            if api_col in df.columns:
                if our_col == "matchup_min":
                    # Minutes might be float
                    output[our_col] = pd.to_numeric(df[api_col], errors="coerce")
                else:
                    # Counting stats
                    output[our_col] = cast(
                        "pd.Series", pd.to_numeric(df[api_col], errors="coerce")
                    ).astype("Int64")
            else:
                output[our_col] = None

        # Field goal percentage
        if "MATCHUP_FG_PCT" in df.columns:
            output["matchup_fg_pct"] = pd.to_numeric(df["MATCHUP_FG_PCT"], errors="coerce")
        else:
            output["matchup_fg_pct"] = None

        # Ensure correct column order
        for col in MATCHUP_STATS_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return cast("pd.DataFrame", output[MATCHUP_STATS_COLUMNS])

    @staticmethod
    def _convert_season_to_id(season: str) -> str:
        """Convert season string to season_id format.

        Args:
            season: Season string (e.g., "2024-25")

        Returns:
            Season ID string (e.g., "22024")
        """
        if not season or "-" not in season:
            return season

        try:
            # Extract first year from "YYYY-YY" format
            year = season.split("-")[0]
            # API format is "2" + year (e.g., "22024")
            return f"2{year}"
        except Exception:
            return season

    def pre_run_hook(self, **kwargs) -> None:
        """Reset fetched keys for this run."""
        self._fetched_season_keys = []

    def post_run_hook(self, **kwargs) -> None:
        """Mark fetched seasons as completed after successful database writes.

        This ensures that progress is only saved when data has actually been
        persisted to the database, preserving resumability if transform or
        insert steps fail. In dry-run mode, progress is not marked to allow
        re-running with actual writes.
        """
        dry_run = kwargs.get("dry_run", False)
        if dry_run:
            logger.info(
                "DRY RUN - not marking progress for fetched seasons "
                "(data was not written)",
            )
            return

        if self._fetched_season_keys:
            for progress_key in self._fetched_season_keys:
                self.progress.mark_completed(progress_key)
            self.progress.save()
            logger.info(
                f"Marked {len(self._fetched_season_keys)} season(s) as completed",
            )


def populate_matchup_stats(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    season_types: list[str] | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate matchup_stats table using LeagueSeasonMatchups endpoint.

    Args:
        db_path: Path to DuckDB database
        seasons: List of seasons to fetch (e.g., ["2023-24", "2022-23"])
        season_types: List of season types (e.g., ["Regular Season", "Playoffs"])
        delay: Delay between API requests in seconds
        reset_progress: Reset progress tracking before starting
        dry_run: If True, don't actually insert data

    Returns:
        Dictionary with population statistics
    """
    db_path = db_path or str(get_db_path())
    seasons = seasons or ALL_SEASONS[-5:]  # Default: last 5 seasons
    season_types = season_types or DEFAULT_SEASON_TYPES

    # Create client with custom delay
    client = get_client()
    client.config.request_delay = delay

    logger.info("=" * 70)
    logger.info("NBA MATCHUP STATS POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Seasons: {len(seasons)} ({seasons[0]} to {seasons[-1]})")
    logger.info(f"Season Types: {season_types}")
    logger.info(f"Request Delay: {delay}s")

    # Create and run populator
    populator = MatchupStatsPopulator(
        db_path=db_path,
        client=client,
    )

    return populator.run(
        seasons=seasons,
        season_types=season_types,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def main() -> None:
    """Parse command-line arguments and run the matchup_stats population process.

    This function is the CLI entry point for populating the matchup_stats table via the
    LeagueSeasonMatchups endpoint. It accepts command-line options to specify the DuckDB
    path (--db), one or more seasons to fetch (--seasons), per-request delay in seconds
    (--delay), filters to fetch only regular season or only playoffs (--regular-season-only,
    --playoffs-only), a flag to reset progress tracking (--reset-progress), and a dry-run
    mode that avoids database writes (--dry-run). On completion the process exits with
    status 0 on success and 1 if any errors occurred, if interrupted by the user, or on
    a fatal error.
    """
    parser = argparse.ArgumentParser(
        description="Populate matchup_stats using LeagueSeasonMatchups endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (last 5 seasons)
  python scripts/populate/populate_matchups.py

  # Specific seasons only
  python scripts/populate/populate_matchups.py --seasons 2023-24 2022-23

  # Reset progress and start fresh
  python scripts/populate/populate_matchups.py --reset-progress

  # Dry run (no database writes)
  python scripts/populate/populate_matchups.py --dry-run
        """,
    )

    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database (default: src/backend/data/nba.duckdb)",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        help="Seasons to fetch (e.g., 2023-24 2022-23)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.6,
        help="Delay between API requests in seconds (default: 0.6)",
    )
    parser.add_argument(
        "--regular-season-only",
        action="store_true",
        help="Only fetch regular season matchups",
    )
    parser.add_argument(
        "--playoffs-only",
        action="store_true",
        help="Only fetch playoff matchups",
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

    # Determine season types
    season_types = resolve_season_types(
        DEFAULT_SEASON_TYPES,
        regular_only=args.regular_season_only,
        playoffs_only=args.playoffs_only,
    )

    try:
        stats = populate_matchup_stats(
            db_path=args.db,
            seasons=args.seasons,
            season_types=season_types,
            delay=args.delay,
            reset_progress=args.reset_progress,
            dry_run=args.dry_run,
        )

        if stats.get("error_count", 0) > 0:
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
