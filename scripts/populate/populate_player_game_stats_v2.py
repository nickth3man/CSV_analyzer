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
    python scripts/populate/populate_player_game_stats_v2.py --seasons 2023-24 2022-23

    # With custom delay for rate limiting
    python scripts/populate/populate_player_game_stats_v2.py --delay 1.0

Based on nba_api documentation:
- reference/nba_api/src/nba_api/stats/endpoints/playergamelogs.py
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

from scripts.populate.base import BasePopulator, PopulationMetrics
from scripts.populate.config import (
    ALL_SEASONS,
    DEFAULT_SEASON_TYPES,
    PLAYER_GAME_STATS_COLUMNS,
    get_db_path,
)
from scripts.populate.api_client import get_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.seasons: List[str] = []
        self.season_types: List[str] = DEFAULT_SEASON_TYPES

    def get_table_name(self) -> str:
        return "player_game_stats"

    def get_key_columns(self) -> List[str]:
        return ["game_id", "player_id"]

    def get_expected_columns(self) -> List[str]:
        return PLAYER_GAME_STATS_COLUMNS

    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """Fetch player game logs using bulk PlayerGameLogs endpoint.

        This method fetches ALL player game logs for specified seasons
        in bulk, rather than making individual API calls per player.
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
                        df['_season'] = season
                        df['_season_type'] = season_type
                        all_data.append(df)
                        logger.info(f"  Found {len(df):,} records")
                        self.metrics.api_calls += 1

                    self.progress.mark_completed(progress_key)
                    self.progress.save()

                    # Respect rate limiting
                    time.sleep(self.client.config.request_delay)

                except Exception as e:
                    logger.error(f"Error fetching {season} {season_type}: {e}")
                    self.progress.add_error(progress_key, str(e))
                    self.metrics.add_error(str(e), {
                        "season": season,
                        "season_type": season_type
                    })

        if not all_data:
            return None

        return pd.concat(all_data, ignore_index=True)

    def _fetch_season_data(self, season: str, season_type: str) -> Optional[pd.DataFrame]:
        """Fetch all player game logs for a season using PlayerGameLogs endpoint."""
        try:
            from nba_api.stats.endpoints import PlayerGameLogs

            # Map season type to API parameter
            api_season_type = SEASON_TYPE_MAP.get(season_type, season_type)

            response = PlayerGameLogs(
                season_nullable=season,
                season_type_nullable=api_season_type,
                timeout=60,
            )

            df = response.player_game_logs.get_data_frame()
            return df

        except ImportError:
            logger.warning("nba_api not installed, using mock data for testing")
            return None
        except Exception as e:
            logger.error(f"API error: {e}")
            raise

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform PlayerGameLogs data to match player_game_stats schema."""
        if df.empty:
            return df

        output = pd.DataFrame()

        # Map columns from PlayerGameLogs to our schema
        # PlayerGameLogs columns: GAME_ID, PLAYER_ID, PLAYER_NAME, TEAM_ID, etc.

        output['game_id'] = pd.to_numeric(df['GAME_ID'], errors='coerce').astype('Int64')
        output['team_id'] = pd.to_numeric(df['TEAM_ID'], errors='coerce').astype('Int64')
        output['player_id'] = pd.to_numeric(df['PLAYER_ID'], errors='coerce').astype('Int64')
        output['player_name'] = df['PLAYER_NAME'].fillna('')

        # These fields aren't in PlayerGameLogs
        output['start_position'] = None
        output['comment'] = None

        # Minutes - already in correct format from PlayerGameLogs
        if 'MIN' in df.columns:
            output['min'] = df['MIN'].apply(self._parse_minutes)
        else:
            output['min'] = None

        # Counting stats
        int_cols = [
            ('FGM', 'fgm'), ('FGA', 'fga'),
            ('FG3M', 'fg3m'), ('FG3A', 'fg3a'),
            ('FTM', 'ftm'), ('FTA', 'fta'),
            ('OREB', 'oreb'), ('DREB', 'dreb'), ('REB', 'reb'),
            ('AST', 'ast'), ('STL', 'stl'), ('BLK', 'blk'),
            ('TOV', 'tov'), ('PF', 'pf'), ('PTS', 'pts')
        ]

        for api_col, our_col in int_cols:
            if api_col in df.columns:
                output[our_col] = pd.to_numeric(df[api_col], errors='coerce').astype('Int64')
            else:
                output[our_col] = None

        # Percentage stats
        pct_cols = [
            ('FG_PCT', 'fg_pct'), ('FG3_PCT', 'fg3_pct'), ('FT_PCT', 'ft_pct')
        ]

        for api_col, our_col in pct_cols:
            if api_col in df.columns:
                output[our_col] = pd.to_numeric(df[api_col], errors='coerce')
            else:
                output[our_col] = None

        # Plus/minus
        if 'PLUS_MINUS' in df.columns:
            output['plus_minus'] = pd.to_numeric(df['PLUS_MINUS'], errors='coerce')
        else:
            output['plus_minus'] = None

        # Ensure correct column order
        for col in PLAYER_GAME_STATS_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return output[PLAYER_GAME_STATS_COLUMNS]

    def _parse_minutes(self, min_val) -> Optional[str]:
        """Parse minutes value to string."""
        if pd.isna(min_val) or min_val is None:
            return None
        if isinstance(min_val, (int, float)):
            return str(int(min_val))
        return str(min_val)

    def pre_run_hook(self, **kwargs):
        """Ensure the table exists before population."""
        conn = self.connect()

        # Check if table exists
        try:
            conn.execute(f"SELECT 1 FROM {self.get_table_name()} LIMIT 1")
            logger.info(f"Table {self.get_table_name()} exists")
        except duckdb.CatalogException:
            logger.info(f"Creating table {self.get_table_name()}...")
            self._create_table(conn)

    def _create_table(self, conn: duckdb.DuckDBPyConnection):
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


def populate_player_game_stats_v2(
    db_path: Optional[str] = None,
    seasons: Optional[List[str]] = None,
    season_types: Optional[List[str]] = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Main function to populate player_game_stats table using bulk endpoint.

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


def main():
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
        """
    )

    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database (default: data/nba.duckdb)"
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        help="Seasons to fetch (e.g., 2023-24 2022-23)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.6,
        help="Delay between API requests in seconds (default: 0.6)"
    )
    parser.add_argument(
        "--regular-season-only",
        action="store_true",
        help="Only fetch regular season games"
    )
    parser.add_argument(
        "--playoffs-only",
        action="store_true",
        help="Only fetch playoff games"
    )
    parser.add_argument(
        "--reset-progress",
        action="store_true",
        help="Reset progress tracking before starting"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually write to database"
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
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
