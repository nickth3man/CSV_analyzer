#!/usr/bin/env python3
"""Populate player_game_stats table using bulk PlayerGameLogs endpoint.

This is an optimized version that uses the PlayerGameLogs endpoint to fetch
ALL player game logs for a season in a single API call, rather than making
individual calls per player.

Performance improvement:
- Old approach: ~5000 API calls per season (one per player)
- New approach: 2 API calls per season (regular season + playoffs)

Features:
- Uses bulk PlayerGameLogs endpoint for efficiency
- Fetches all players for a season in one API call
- Supports incremental updates (skip existing seasons)
- Progress tracking and resumability
- Data validation before insertion

Usage:
    # Full population (recent seasons)
    python scripts/populate/populate_player_game_stats_v2.py

    # Specific seasons only
    python scripts/populate/populate_player_game_stats_v2.py --seasons 2025-26 2024-25

    # With custom delay for rate limiting
    python scripts/populate/populate_player_game_stats_v2.py --delay 1.0

Based on nba_api documentation:
- reference/nba_api/src/nba_api/stats/endpoints/playergamelogs.py
"""

import argparse
import logging
import sys
import time
from typing import Any

import duckdb
import pandas as pd

from src.scripts.populate.api_client import get_client
from src.scripts.populate.base import BasePopulator
from src.scripts.populate.config import (
    ALL_SEASONS,
    DEFAULT_SEASON_TYPES,
    PLAYER_GAME_STATS_COLUMNS,
    get_db_path,
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Season type mapping for the API
SEASON_TYPE_MAP = {
    "Regular Season": "Regular Season",
    "Playoffs": "Playoffs",
    "Pre Season": "Pre Season",
    "All Star": "All Star",
}


class PlayerGameStatsPopulator(BasePopulator):
    """Populator for player_game_stats table using bulk endpoint."""

    def __init__(self, **kwargs) -> None:
        """Initialize the PlayerGameStatsPopulator.

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
        """Target table name for player game statistics.

        Returns:
            The table name "player_game_stats".
        """
        return "player_game_stats"

    def get_key_columns(self) -> list[str]:
        """Return the composite primary key columns for the player_game_stats table.

        Returns:
            A list containing the key column names: "game_id" and "player_id".
        """
        return ["game_id", "player_id"]

    def get_expected_columns(self) -> list[str]:
        """Return the expected column names for the player_game_stats table schema.

        Returns:
            List[str]: Column names defining the canonical player_game_stats schema in the expected order.
        """
        return PLAYER_GAME_STATS_COLUMNS

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        """Fetches player game logs in bulk for the given seasons and season types.

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
                    # Use the bulk endpoint
                    df = self._fetch_season_data(season, season_type)

                    if df is not None and not df.empty:
                        df["_season"] = season
                        df["_season_type"] = season_type
                        all_data.append(df)
                        logger.info(f"  Found {len(df):,} records")
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
        """Retrieve player game logs for a given season and season type.

        Maps the provided season_type to the API's expected value and requests the full set
        of player game logs for that season. Returns `None` when the `nba_api` package is
        not installed (mock/testing mode).

        Returns:
            pd.DataFrame or None: A DataFrame containing player game log rows for the
            specified season and season type, or `None` if `nba_api` is unavailable.

        Raises:
            Exception: Propagates errors from the underlying API call.
        """
        try:
            from nba_api.stats.endpoints import PlayerGameLogs

            # Map season type to API parameter
            api_season_type = SEASON_TYPE_MAP.get(season_type, season_type)

            response = PlayerGameLogs(
                season_nullable=season,
                season_type_nullable=api_season_type,
                timeout=60,
            )

            return response.player_game_logs.get_data_frame()

        except ImportError:
            logger.warning("nba_api not installed, using mock data for testing")
            return None
        except Exception as e:
            logger.exception(f"API error: {e}")
            raise

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform a PlayerGameLogs DataFrame into the player_game_stats table schema.

        Converts and maps source columns from the PlayerGameLogs payload into the expected
        PLAYER_GAME_STATS_COLUMNS order. Numeric counting and percentage fields are
        coerced to numeric types (integer columns use pandas' Int64 nullable dtype where
        possible); the `min` field is normalized via _parse_minutes. Any missing source
        columns are represented as None in the output.

        Parameters:
            df (pd.DataFrame): DataFrame returned by the PlayerGameLogs endpoint.
            **kwargs: Ignored; present for interface compatibility.

        Returns:
            pd.DataFrame: A DataFrame with columns ordered as PLAYER_GAME_STATS_COLUMNS,
            containing transformed and type-normalized player game statistics.
        """
        if df.empty:
            return df

        output = pd.DataFrame()

        # Map columns from PlayerGameLogs to our schema
        # PlayerGameLogs columns: GAME_ID, PLAYER_ID, PLAYER_NAME, TEAM_ID, etc.

        output["game_id"] = pd.to_numeric(df["GAME_ID"], errors="coerce").astype(
            "Int64",
        )
        output["team_id"] = pd.to_numeric(df["TEAM_ID"], errors="coerce").astype(
            "Int64",
        )
        output["player_id"] = pd.to_numeric(df["PLAYER_ID"], errors="coerce").astype(
            "Int64",
        )
        output["player_name"] = df["PLAYER_NAME"].fillna("")

        # These fields aren't in PlayerGameLogs
        output["start_position"] = None
        output["comment"] = None

        # Minutes - already in correct format from PlayerGameLogs
        if "MIN" in df.columns:
            output["min"] = df["MIN"].apply(self._parse_minutes)
        else:
            output["min"] = None

        # Counting stats
        int_cols = [
            ("FGM", "fgm"),
            ("FGA", "fga"),
            ("FG3M", "fg3m"),
            ("FG3A", "fg3a"),
            ("FTM", "ftm"),
            ("FTA", "fta"),
            ("OREB", "oreb"),
            ("DREB", "dreb"),
            ("REB", "reb"),
            ("AST", "ast"),
            ("STL", "stl"),
            ("BLK", "blk"),
            ("TOV", "tov"),
            ("PF", "pf"),
            ("PTS", "pts"),
        ]

        for api_col, our_col in int_cols:
            if api_col in df.columns:
                output[our_col] = pd.to_numeric(df[api_col], errors="coerce").astype(
                    "Int64",
                )
            else:
                output[our_col] = None

        # Percentage stats
        pct_cols = [
            ("FG_PCT", "fg_pct"),
            ("FG3_PCT", "fg3_pct"),
            ("FT_PCT", "ft_pct"),
        ]

        for api_col, our_col in pct_cols:
            if api_col in df.columns:
                output[our_col] = pd.to_numeric(df[api_col], errors="coerce")
            else:
                output[our_col] = None

        # Plus/minus
        if "PLUS_MINUS" in df.columns:
            output["plus_minus"] = pd.to_numeric(df["PLUS_MINUS"], errors="coerce")
        else:
            output["plus_minus"] = None

        # Ensure correct column order
        for col in PLAYER_GAME_STATS_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return output[PLAYER_GAME_STATS_COLUMNS]

    def _parse_minutes(self, min_val) -> str | None:
        """Convert a minutes value to a normalized string or return None for missing values.

        Parameters:
            min_val: The minutes value from source data (may be int, float, str, or NA).

        Returns:
            A string representation of the minutes, with numeric values converted to integers before stringification (e.g., 35.0 -> "35"), or `None` if `min_val` is NA or `None`.
        """
        if pd.isna(min_val) or min_val is None:
            return None
        if isinstance(min_val, (int, float)):
            return str(int(min_val))
        return str(min_val)

    def pre_run_hook(self, **kwargs) -> None:
        """Ensure the target table exists, creating it if missing.

        Checks for the presence of the table named by get_table_name(); if the
        table does not exist, creates it by calling _create_table. Also resets
        the list of fetched season keys for the current run.
        """
        # Reset fetched keys for this run
        self._fetched_season_keys = []
        conn = self.connect()

        # Check if table exists
        try:
            conn.execute(f"SELECT 1 FROM {self.get_table_name()} LIMIT 1")
            logger.info(f"Table {self.get_table_name()} exists")
        except duckdb.CatalogException:
            logger.info(f"Creating table {self.get_table_name()}...")
            self._create_table(conn)

    def _create_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create the player_game_stats table if it doesn't exist."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS player_game_stats (
                game_id BIGINT,
                team_id BIGINT,
                player_id BIGINT,
                player_name VARCHAR,
                start_position VARCHAR,
                comment VARCHAR,
                min VARCHAR,
                fgm INTEGER,
                fga INTEGER,
                fg_pct DOUBLE,
                fg3m INTEGER,
                fg3a INTEGER,
                fg3_pct DOUBLE,
                ftm INTEGER,
                fta INTEGER,
                ft_pct DOUBLE,
                oreb INTEGER,
                dreb INTEGER,
                reb INTEGER,
                ast INTEGER,
                stl INTEGER,
                blk INTEGER,
                tov INTEGER,
                pf INTEGER,
                pts INTEGER,
                plus_minus DOUBLE,
                PRIMARY KEY (game_id, player_id)
            )
        """)
        logger.info("Table created successfully")

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


def populate_player_game_stats_v2(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    season_types: list[str] | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate player_game_stats table using bulk endpoint.

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
    logger.info("NBA PLAYER GAME STATS POPULATION (BULK ENDPOINT)")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Seasons: {len(seasons)} ({seasons[0]} to {seasons[-1]})")
    logger.info(f"Season Types: {season_types}")
    logger.info(f"Request Delay: {delay}s")

    # Create and run populator
    populator = PlayerGameStatsPopulator(
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
    """Parse command-line arguments and run the player_game_stats population process.

    This function is the CLI entry point for populating the player_game_stats table via the bulk PlayerGameLogs endpoint. It accepts command-line options to specify the DuckDB path (--db), one or more seasons to fetch (--seasons), per-request delay in seconds (--delay), filters to fetch only regular season or only playoffs (--regular-season-only, --playoffs-only), a flag to reset progress tracking (--reset-progress), and a dry-run mode that avoids database writes (--dry-run). On completion the process exits with status 0 on success and 1 if any errors occurred, if interrupted by the user, or on a fatal error.
    """
    parser = argparse.ArgumentParser(
        description="Populate player_game_stats using bulk PlayerGameLogs endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (last 5 seasons)
  python scripts/populate/populate_player_game_stats_v2.py

  # Specific seasons only
  python scripts/populate/populate_player_game_stats_v2.py --seasons 2023-24 2022-23

  # Reset progress and start fresh
  python scripts/populate/populate_player_game_stats_v2.py --reset-progress

  # Dry run (no database writes)
  python scripts/populate/populate_player_game_stats_v2.py --dry-run
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
        help="Only fetch regular season games",
    )
    parser.add_argument(
        "--playoffs-only",
        action="store_true",
        help="Only fetch playoff games",
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
    season_types = DEFAULT_SEASON_TYPES
    if args.regular_season_only:
        season_types = ["Regular Season"]
    elif args.playoffs_only:
        season_types = ["Playoffs"]

    try:
        stats = populate_player_game_stats_v2(
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
