#!/usr/bin/env python3
"""Populate player_splits table from NBA API.

This script fetches player dashboard splits from multiple NBA API endpoints:
- PlayerDashboardByClutch: Clutch-time performance splits
- PlayerDashboardByGameSplits: Performance by win/loss, home/away, etc.
- PlayerDashboardByLastNGames: Recent performance trends
- PlayerDashboardByShootingSplits: Performance by shot type/zone

Usage:
    # Full population for active players (current season)
    python -m src.scripts.populate.populate_player_splits

    # Specific seasons
    python -m src.scripts.populate.populate_player_splits --seasons 2024-25 2023-24

    # Specific split types only
    python -m src.scripts.populate.populate_player_splits --split-types clutch game_splits

    # Specific players only
    python -m src.scripts.populate.populate_player_splits --player-ids 201566 203507

    # Limit number of players (for testing)
    python -m src.scripts.populate.populate_player_splits --limit 10

    # Dry run (no database writes)
    python -m src.scripts.populate.populate_player_splits --dry-run

    # Regular season only
    python -m src.scripts.populate.populate_player_splits --regular-season-only
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Any

import pandas as pd

from src.scripts.populate.api_client import get_client
from src.scripts.populate.base import BasePopulator, ProgressMixin
from src.scripts.populate.config import (
    CURRENT_SEASON,
    DEFAULT_SEASON_TYPES,
    get_db_path,
)
from src.scripts.populate.helpers import configure_logging, resolve_season_types


configure_logging()
logger = logging.getLogger(__name__)


# =============================================================================
# SPLIT TYPE DEFINITIONS
# =============================================================================

# Available split types and their corresponding API methods
SPLIT_TYPES = {
    "clutch": "get_player_clutch_splits",
    "game_splits": "get_player_game_splits",
    "last_n_games": "get_player_last_n_games_splits",
    "shooting": "get_player_shooting_splits",
}

# Expected columns for the player_splits table
EXPECTED_COLUMNS = [
    "season_id",
    "season_type",
    "player_id",
    "player_name",
    "team_id",
    "team_abbreviation",
    "split_type",
    "split_category",
    "split_value",
    "games_played",
    "wins",
    "losses",
    "win_pct",
    "minutes",
    "fgm",
    "fga",
    "fg_pct",
    "fg3m",
    "fg3a",
    "fg3_pct",
    "ftm",
    "fta",
    "ft_pct",
    "oreb",
    "dreb",
    "reb",
    "ast",
    "stl",
    "blk",
    "tov",
    "pf",
    "pts",
    "plus_minus",
    "efg_pct",
    "filename",
]


class PlayerSplitsPopulator(BasePopulator, ProgressMixin):
    """Populator for player_splits table.

    Fetches player dashboard splits across multiple split types:
    - Clutch: Performance in clutch situations
    - Game Splits: By result, location, margin, days rest
    - Last N Games: Recent performance trends
    - Shooting: By shot type, zone, and context
    """

    def __init__(self, **kwargs) -> None:
        """Initialize the PlayerSplitsPopulator."""
        super().__init__(**kwargs)
        self._fetched_progress_keys: list[str] = []

    def get_table_name(self) -> str:
        """Return the target table name."""
        return "player_splits"

    def get_key_columns(self) -> list[str]:
        """Return primary key columns for the table."""
        return [
            "season_id",
            "season_type",
            "player_id",
            "split_type",
            "split_category",
            "split_value",
        ]

    def get_expected_columns(self) -> list[str]:
        """Return expected columns for validation."""
        return EXPECTED_COLUMNS

    def _get_active_players(self, player_ids: list[int] | None = None) -> list[dict]:
        """Get list of players to fetch splits for.

        Args:
            player_ids: Optional list of specific player IDs to use.

        Returns:
            List of player dictionaries with id and full_name keys.
        """
        if player_ids:
            # Fetch specific players
            players = []
            for pid in player_ids:
                player = self.client.find_player_by_id(pid)
                if player:
                    players.append(player)
                else:
                    logger.warning("Player ID %d not found in static data", pid)
            return players

        # Get all active players
        return self.client.get_active_players()

    def _fetch_player_split(
        self,
        player_id: int,
        player_name: str,
        season: str,
        season_type: str,
        split_type: str,
    ) -> pd.DataFrame | None:
        """Fetch a specific split type for a player.

        Args:
            player_id: NBA player ID.
            player_name: Player name for logging.
            season: Season string (e.g., "2024-25").
            season_type: Season type (e.g., "Regular Season").
            split_type: Split type key (clutch, game_splits, etc.).

        Returns:
            DataFrame with split data or None if no data.
        """
        method_name = SPLIT_TYPES.get(split_type)
        if not method_name:
            logger.error("Unknown split type: %s", split_type)
            return None

        method = getattr(self.client, method_name, None)
        if not method:
            logger.error("API method not found: %s", method_name)
            return None

        try:
            result = method(
                player_id=player_id,
                season=season,
                season_type=season_type,
            )

            if not result:
                return None

            # Flatten all DataFrames from the result dict
            all_rows = []
            for category, category_df in result.items():
                if category_df is None or category_df.empty:
                    continue

                # Add category identifier
                category_df_copy = category_df.copy()
                category_df_copy["_split_category"] = category
                category_df_copy["_split_type"] = split_type
                category_df_copy["_season"] = season
                category_df_copy["_season_type"] = season_type
                category_df_copy["_player_id"] = player_id
                category_df_copy["_player_name"] = player_name
                all_rows.append(category_df_copy)

            if not all_rows:
                return None

            return pd.concat(all_rows, ignore_index=True)

        except Exception as e:
            logger.warning(
                "Error fetching %s for player %d (%s): %s",
                split_type,
                player_id,
                player_name,
                e,
            )
            return None

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        """Fetch player splits data for all specified players, seasons, and split types.

        Args:
            **kwargs: Fetch parameters including:
                - seasons: List of seasons to fetch
                - season_types: List of season types
                - player_ids: Optional list of specific player IDs
                - split_types: List of split types to fetch
                - limit: Optional limit on number of players
                - resume: Whether to skip completed items

        Returns:
            Combined DataFrame with all split data or None if no data.
        """
        seasons: list[str] = kwargs.get("seasons") or [CURRENT_SEASON]
        season_types: list[str] = kwargs.get("season_types") or DEFAULT_SEASON_TYPES
        player_ids: list[int] | None = kwargs.get("player_ids")
        split_types: list[str] = kwargs.get("split_types") or list(SPLIT_TYPES.keys())
        limit: int | None = kwargs.get("limit")
        resume: bool = kwargs.get("resume", True)

        # Validate split types
        valid_split_types = [st for st in split_types if st in SPLIT_TYPES]
        if not valid_split_types:
            logger.error(
                "No valid split types specified. Available: %s",
                list(SPLIT_TYPES.keys()),
            )
            return None

        # Get players to process
        players = self._get_active_players(player_ids)
        if limit:
            players = players[:limit]

        if not players:
            logger.info("No players found to process")
            return None

        total_combinations = (
            len(players) * len(seasons) * len(season_types) * len(valid_split_types)
        )

        logger.info(
            "Fetching player splits: %d players x %d seasons x %d season types x %d split types = %d combinations",
            len(players),
            len(seasons),
            len(season_types),
            len(valid_split_types),
            total_combinations,
        )

        all_data = []
        processed = 0
        skipped = 0

        for player in players:
            player_id = player["id"]
            player_name = player.get("full_name", f"Player {player_id}")

            for season in seasons:
                for season_type in season_types:
                    for split_type in valid_split_types:
                        processed += 1
                        progress_key = (
                            f"{player_id}_{season}_{season_type}_{split_type}"
                        )

                        # Check if already completed
                        if resume and self.progress.is_completed(progress_key):
                            logger.debug(
                                "[%d/%d] Skipping %s - %s (%s)",
                                processed,
                                total_combinations,
                                player_name,
                                split_type,
                                progress_key,
                            )
                            skipped += 1
                            continue

                        logger.info(
                            "[%d/%d] Fetching %s for %s (%s %s)...",
                            processed,
                            total_combinations,
                            split_type,
                            player_name,
                            season,
                            season_type,
                        )

                        try:
                            df = self._fetch_player_split(
                                player_id=player_id,
                                player_name=player_name,
                                season=season,
                                season_type=season_type,
                                split_type=split_type,
                            )

                            if df is not None and not df.empty:
                                all_data.append(df)
                                logger.info(
                                    "  Found %d split records for %s",
                                    len(df),
                                    split_type,
                                )
                                # Track for deferred progress marking
                                self._fetched_progress_keys.append(progress_key)
                            else:
                                logger.debug(
                                    "  No %s data for %s in %s %s",
                                    split_type,
                                    player_name,
                                    season,
                                    season_type,
                                )

                            self.metrics.api_calls += 1

                            # Respect rate limiting
                            time.sleep(self.client.config.request_delay)

                        except Exception as e:
                            logger.exception(
                                "Error fetching %s for %s: %s",
                                split_type,
                                player_name,
                                e,
                            )
                            self.progress.add_error(progress_key, str(e))
                            self.metrics.add_error(
                                str(e),
                                {
                                    "player_id": player_id,
                                    "season": season,
                                    "season_type": season_type,
                                    "split_type": split_type,
                                },
                            )

        if skipped > 0:
            logger.info("Skipped %d already-completed combinations", skipped)

        if not all_data:
            logger.info("No data fetched")
            return None

        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info("Total records fetched: %d", len(combined_df))
        return combined_df

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform raw split data to match the player_splits schema.

        Args:
            df: Raw DataFrame from API.
            **kwargs: Additional transformation parameters.

        Returns:
            Transformed DataFrame matching the expected schema.
        """
        if df.empty:
            return df

        def safe_numeric(column_name: str, default_col: str | None = None) -> pd.Series:
            """Safely convert column to numeric, handling missing columns."""
            value = df.get(column_name)
            if value is None and default_col:
                value = df.get(default_col)
            if value is None:
                return pd.Series([None] * len(df), dtype="float64")
            return pd.to_numeric(value, errors="coerce")

        def safe_int(column_name: str, default_col: str | None = None) -> pd.Series:
            """Safely convert column to nullable Int64, handling missing columns."""
            numeric = safe_numeric(column_name, default_col)
            return numeric.astype("Int64")

        output = pd.DataFrame()

        # Extract metadata from added columns
        output["season_id"] = df["_season"]
        output["season_type"] = df["_season_type"]
        output["player_id"] = pd.to_numeric(df["_player_id"], errors="coerce").astype(
            "Int64"
        )
        output["player_name"] = df["_player_name"]

        # Team info (from API response if available - not all endpoints have this)
        output["team_id"] = safe_int("TEAM_ID")
        output["team_abbreviation"] = df.get(
            "TEAM_ABBREVIATION", pd.Series([""] * len(df))
        )
        if output["team_abbreviation"].isna().all():
            output["team_abbreviation"] = ""

        # Split identifiers
        output["split_type"] = df["_split_type"]
        output["split_category"] = df["_split_category"]

        # GROUP_VALUE is the specific split value (e.g., "Win", "Home", "Last 5 Games")
        split_value = df.get("GROUP_VALUE")
        if split_value is None:
            split_value = df.get("SHOT_TYPE")
        if split_value is None:
            split_value = df.get("GROUP_SET")
        if split_value is None:
            split_value = pd.Series(["Overall"] * len(df))
        output["split_value"] = split_value

        # Game counts
        output["games_played"] = safe_int("GP", "G")
        output["wins"] = safe_int("W")
        output["losses"] = safe_int("L")
        output["win_pct"] = safe_numeric("W_PCT")

        # Minutes
        output["minutes"] = safe_numeric("MIN")

        # Field Goals
        output["fgm"] = safe_int("FGM")
        output["fga"] = safe_int("FGA")
        output["fg_pct"] = safe_numeric("FG_PCT")

        # Three Pointers
        output["fg3m"] = safe_int("FG3M")
        output["fg3a"] = safe_int("FG3A")
        output["fg3_pct"] = safe_numeric("FG3_PCT")

        # Free Throws
        output["ftm"] = safe_int("FTM")
        output["fta"] = safe_int("FTA")
        output["ft_pct"] = safe_numeric("FT_PCT")

        # Rebounds
        output["oreb"] = safe_int("OREB")
        output["dreb"] = safe_int("DREB")
        output["reb"] = safe_int("REB")

        # Other stats
        output["ast"] = safe_int("AST")
        output["stl"] = safe_int("STL")
        output["blk"] = safe_int("BLK")
        output["tov"] = safe_int("TOV")
        output["pf"] = safe_int("PF")
        output["pts"] = safe_int("PTS")
        output["plus_minus"] = safe_numeric("PLUS_MINUS")

        # Effective FG% (available in shooting splits)
        output["efg_pct"] = safe_numeric("EFG_PCT")

        # Add filename for lineage
        output["filename"] = "nba_api.playerdashboard"

        # Ensure all expected columns exist
        for col in EXPECTED_COLUMNS:
            if col not in output.columns:
                output[col] = None

        # Validate: drop rows without key identifiers
        output = output.dropna(subset=["player_id", "split_type", "split_category"])

        # Filter to expected columns in order
        return output[EXPECTED_COLUMNS]

    def pre_run_hook(self, **kwargs) -> None:
        """Reset fetched keys tracking before the run."""
        self._fetched_progress_keys = []

    def post_run_hook(self, **kwargs) -> None:
        """Mark fetched items as completed after successful database writes."""
        dry_run = kwargs.get("dry_run", False)
        if dry_run:
            logger.info(
                "DRY RUN - not marking progress for %d fetched combinations",
                len(self._fetched_progress_keys),
            )
            return

        if self._fetched_progress_keys:
            for progress_key in self._fetched_progress_keys:
                self.progress.mark_completed(progress_key)
            self.progress.save()
            logger.info(
                "Marked %d player/split combinations as completed",
                len(self._fetched_progress_keys),
            )


def populate_player_splits(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    season_types: list[str] | None = None,
    player_ids: list[int] | None = None,
    split_types: list[str] | None = None,
    limit: int | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate player_splits table.

    Args:
        db_path: Path to DuckDB database.
        seasons: List of seasons to fetch (e.g., ["2024-25", "2023-24"]).
        season_types: List of season types (e.g., ["Regular Season", "Playoffs"]).
        player_ids: Optional list of specific player IDs to fetch.
        split_types: List of split types to fetch (clutch, game_splits, etc.).
        limit: Optional limit on number of players to process.
        delay: Delay between API requests in seconds.
        reset_progress: Reset progress tracking before starting.
        dry_run: If True, don't actually insert data.

    Returns:
        Dictionary with population statistics.
    """
    db_path = db_path or str(get_db_path())
    seasons = seasons or [CURRENT_SEASON]
    season_types = season_types or DEFAULT_SEASON_TYPES
    split_types = split_types or list(SPLIT_TYPES.keys())

    # Create client with custom delay
    client = get_client()
    client.config.request_delay = delay

    logger.info("=" * 70)
    logger.info("NBA PLAYER SPLITS POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Seasons: {seasons}")
    logger.info(f"Season Types: {season_types}")
    logger.info(f"Split Types: {split_types}")
    if player_ids:
        logger.info(f"Player IDs: {player_ids}")
    if limit:
        logger.info(f"Player Limit: {limit}")
    logger.info(f"Request Delay: {delay}s")

    # Create and run populator
    populator = PlayerSplitsPopulator(
        db_path=db_path,
        client=client,
    )

    return populator.run(
        seasons=seasons,
        season_types=season_types,
        player_ids=player_ids,
        split_types=split_types,
        limit=limit,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def main() -> None:
    """Parse command-line arguments and run the player splits population process."""
    parser = argparse.ArgumentParser(
        description="Populate player_splits table from NBA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population for active players (current season)
  python -m src.scripts.populate.populate_player_splits

  # Specific seasons
  python -m src.scripts.populate.populate_player_splits --seasons 2024-25 2023-24

  # Specific split types only (faster)
  python -m src.scripts.populate.populate_player_splits --split-types clutch game_splits

  # Specific players only
  python -m src.scripts.populate.populate_player_splits --player-ids 201566 203507

  # Limit number of players (for testing)
  python -m src.scripts.populate.populate_player_splits --limit 10

  # Regular season only
  python -m src.scripts.populate.populate_player_splits --regular-season-only

  # Reset progress and start fresh
  python -m src.scripts.populate.populate_player_splits --reset-progress

  # Dry run (no database writes)
  python -m src.scripts.populate.populate_player_splits --dry-run

Available split types:
  - clutch: Clutch-time performance (last 5min, 3min, 1min, etc.)
  - game_splits: Performance by win/loss, home/away, days rest, etc.
  - last_n_games: Recent performance (last 5, 10, 15, 20 games)
  - shooting: Performance by shot type, zone, shot clock, etc.
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
        help="Seasons to fetch (e.g., 2024-25 2023-24)",
    )
    parser.add_argument(
        "--split-types",
        nargs="+",
        choices=list(SPLIT_TYPES.keys()),
        help="Split types to fetch (default: all)",
    )
    parser.add_argument(
        "--player-ids",
        nargs="+",
        type=int,
        help="Specific player IDs to fetch (default: all active players)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of players to process (for testing)",
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
        help="Only fetch regular season data",
    )
    parser.add_argument(
        "--playoffs-only",
        action="store_true",
        help="Only fetch playoff data",
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
        stats = populate_player_splits(
            db_path=args.db,
            seasons=args.seasons,
            season_types=season_types,
            player_ids=args.player_ids,
            split_types=args.split_types,
            limit=args.limit,
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
