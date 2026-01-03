#!/usr/bin/env python3
"""Populate player_game_stats_raw table from NBA API.

This script fetches player game logs from the NBA API and populates the
player_game_stats_raw table in the DuckDB database.

Features:
- Fetches game logs for all players (or a subset)
- Respects NBA API rate limits (configurable delay between requests)
- Implements caching to avoid redundant API calls
- Supports incremental updates (skip already populated seasons)
- Progress tracking and resumability
- Error handling with retry logic

Usage:
    # Full population (all players, all seasons)
    python scripts/populate/populate_player_game_stats.py

    # Specific seasons only
    python scripts/populate/populate_player_game_stats.py --seasons 2023-24 2022-23

    # Active players only (faster for recent data)
    python scripts/populate/populate_player_game_stats.py --active-only

    # Resume from a specific player ID
    python scripts/populate/populate_player_game_stats.py --resume-from 2544

    # Limit number of players (for testing)
    python scripts/populate/populate_player_game_stats.py --limit 10

    # Custom request delay (default: 0.6 seconds)
    python scripts/populate/populate_player_game_stats.py --delay 1.0
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import TYPE_CHECKING, Any

import pandas as pd

from src.scripts.populate.api_client import NBAClient, get_client
from src.scripts.populate.base import (
    BasePopulator,
    ProgressMixin,
    ProgressTracker,
    SeasonIteratorMixin,
)
from src.scripts.populate.config import (
    ALL_SEASONS,
    DEFAULT_SEASON_TYPES,
    PLAYER_GAME_STATS_COLUMNS,
    get_db_path,
)
from src.scripts.populate.helpers import configure_logging, resolve_season_types
from src.scripts.populate.transform_utils import parse_minutes
from src.scripts.utils.ui import (
    create_progress_bar,
    print_step,
    print_summary_table,
)


if TYPE_CHECKING:
    from src.scripts.populate.constants import SeasonType


# Configure logging
configure_logging()
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

TABLE_NAME = "player_game_stats_raw"
KEY_COLUMNS = ["game_id", "player_id"]


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================


class PlayerInfo:
    """Player information container."""

    __slots__ = ("full_name", "id")

    def __init__(self, player_id: int, full_name: str) -> None:
        """Initialize player info."""
        self.id = player_id
        self.full_name = full_name

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PlayerInfo:
        """Create PlayerInfo from dictionary."""
        return cls(
            player_id=data["id"],
            full_name=data.get("full_name", ""),
        )


# =============================================================================
# TRANSFORMATION FUNCTIONS
# =============================================================================


def transform_game_log(df: pd.DataFrame, player_info: PlayerInfo) -> pd.DataFrame:
    """Transform NBA API game log to our schema.

    Args:
        df: Raw DataFrame from NBA API.
        player_info: Player information.

    Returns:
        Transformed DataFrame matching player_game_stats_raw schema.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # Check for game_id column (mixed case from API)
    if "Game_ID" not in df.columns and "GAME_ID" not in df.columns:
        return pd.DataFrame()

    # Make a copy to avoid modifying original
    df_work = df.copy()

    # Build output DataFrame with exact schema
    output = pd.DataFrame()

    # game_id - Handle mixed case from API
    game_id_col = "Game_ID" if "Game_ID" in df_work.columns else "GAME_ID"
    output["game_id"] = pd.to_numeric(df_work[game_id_col], errors="coerce").astype(
        "Int64"
    )

    # team_id - Not available in PlayerGameLog endpoint
    output["team_id"] = pd.Series(dtype="Int64")

    # player_id - from API or player_info
    if "Player_ID" in df_work.columns:
        output["player_id"] = pd.to_numeric(
            df_work["Player_ID"], errors="coerce"
        ).astype("Int64")
    else:
        output["player_id"] = player_info.id

    # player_name
    output["player_name"] = player_info.full_name

    # start_position, comment - not in game log API
    output["start_position"] = None
    output["comment"] = None

    # min - minutes played (can be "MM:SS" or just "MM")
    output["min"] = df_work["MIN"].apply(parse_minutes) if "MIN" in df_work.columns else None

    # Integer counting stats
    int_col_mappings = [
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
    for api_col, our_col in int_col_mappings:
        if api_col in df_work.columns:
            output[our_col] = pd.to_numeric(df_work[api_col], errors="coerce").astype(
                "Int64"
            )
        else:
            output[our_col] = pd.Series(dtype="Int64")

    # Percentage stats (floats)
    pct_col_mappings = [
        ("FG_PCT", "fg_pct"),
        ("FG3_PCT", "fg3_pct"),
        ("FT_PCT", "ft_pct"),
    ]
    for api_col, our_col in pct_col_mappings:
        if api_col in df_work.columns:
            output[our_col] = pd.to_numeric(df_work[api_col], errors="coerce")
        else:
            output[our_col] = None

    # plus_minus
    if "PLUS_MINUS" in df_work.columns:
        output["plus_minus"] = pd.to_numeric(df_work["PLUS_MINUS"], errors="coerce")
    else:
        output["plus_minus"] = None

    # Ensure all expected columns exist and reorder
    for col in PLAYER_GAME_STATS_COLUMNS:
        if col not in output.columns:
            output[col] = pd.Series(dtype="object")

    return output[PLAYER_GAME_STATS_COLUMNS]


# =============================================================================
# POPULATOR CLASS
# =============================================================================


class PlayerGameStatsPopulator(ProgressMixin, SeasonIteratorMixin, BasePopulator):
    """Populates player_game_stats_raw from per-player game log API.

    This populator fetches game logs for each player individually,
    which allows for fine-grained progress tracking but is slower
    than the bulk league-level endpoints.

    Use this populator when:
    - You need data for specific players only
    - You need to resume from a specific player
    - The league-level endpoints are not available

    Prefer populate_player_game_stats_v2.py for bulk population.
    """

    def __init__(
        self,
        db_path: str | None = None,
        client: NBAClient | None = None,
        batch_size: int = 1000,
    ) -> None:
        """Initialize the populator.

        Args:
            db_path: Path to DuckDB database.
            client: NBA API client.
            batch_size: Records per batch for database operations.
        """
        super().__init__(db_path, client, batch_size)

        # Override progress tracker name
        self.progress = ProgressTracker("player_game_stats")

        # Player processing state
        self._players: list[dict[str, Any]] = []
        self._player_games_buffer: list[pd.DataFrame] = []

    def get_table_name(self) -> str:
        """Return target table name."""
        return TABLE_NAME

    def get_key_columns(self) -> list[str]:
        """Return primary key columns."""
        return KEY_COLUMNS

    def get_expected_columns(self) -> list[str]:
        """Return expected columns for validation."""
        return PLAYER_GAME_STATS_COLUMNS

    def get_data_type(self) -> str:
        """Return data type for validation."""
        return "player_game_stats"

    def fetch_data(self, **kwargs: Any) -> pd.DataFrame | None:
        """Fetch data by iterating over players and seasons.

        This implementation fetches data player-by-player rather than
        in bulk, enabling fine-grained progress tracking.

        Args:
            **kwargs: Population parameters including:
                - seasons: List of seasons to fetch
                - season_types: List of season types
                - active_only: Whether to fetch only active players
                - limit: Maximum number of players
                - resume_from: Player ID to resume from
                - resume: Whether to resume from progress
                - dry_run: Whether this is a dry run

        Returns:
            Combined DataFrame of all fetched game logs.
        """
        seasons = kwargs.get("seasons") or ALL_SEASONS
        season_types = kwargs.get("season_types") or DEFAULT_SEASON_TYPES
        active_only = kwargs.get("active_only", False)
        limit = kwargs.get("limit")
        resume_from = kwargs.get("resume_from")
        resume = kwargs.get("resume", True)
        dry_run = kwargs.get("dry_run", False)

        # Resolve season types to enum
        resolved_types = self.resolve_season_types(season_types)

        # Fetch player list
        logger.info("Fetching player list...")
        if active_only:
            self._players = self.client.get_active_players()
        else:
            self._players = self.client.get_all_players()

        # Sort by ID for consistent ordering
        self._players = sorted(self._players, key=lambda x: x["id"])
        logger.info(f"Found {len(self._players)} players")

        # Handle resume from specific player
        if resume_from:
            self._players = [p for p in self._players if p["id"] >= resume_from]
            logger.info(f"After resume filter: {len(self._players)} players")

        # Apply limit
        if limit:
            self._players = self._players[:limit]
            logger.info(f"After limit: {len(self._players)} players")

        # Process players
        all_games: list[pd.DataFrame] = []
        total_players = len(self._players)

        with create_progress_bar() as progress:
            task = progress.add_task("Processing players", total=total_players)

            for idx, player_data in enumerate(self._players):
                player = PlayerInfo.from_dict(player_data)
                progress_key = f"player_{player.id}"

                # Check if already completed
                if resume and self.progress.is_completed(progress_key):
                    progress.update(task, advance=1)
                    continue

                # Process this player's seasons
                player_games = self._fetch_player_games(
                    player=player,
                    seasons=seasons,
                    season_types=resolved_types,
                )

                if not player_games.empty:
                    all_games.append(player_games)
                    logger.info(
                        f"[{idx + 1}/{total_players}] {player.full_name}: "
                        f"+{len(player_games)} games"
                    )

                # Mark completed
                if not dry_run:
                    self.progress.mark_completed(progress_key)

                    # Periodic save
                    if (idx + 1) % 10 == 0:
                        self.progress.save()

                progress.update(task, advance=1)

        # Combine all games
        if all_games:
            return pd.concat(all_games, ignore_index=True)
        return pd.DataFrame()

    def _fetch_player_games(
        self,
        player: PlayerInfo,
        seasons: list[str],
        season_types: list[SeasonType],
    ) -> pd.DataFrame:
        """Fetch game logs for a single player across seasons.

        Args:
            player: Player information.
            seasons: List of seasons to fetch.
            season_types: List of season types.

        Returns:
            Combined DataFrame of player's games.
        """
        player_games: list[pd.DataFrame] = []

        for season in seasons:
            for season_type in season_types:
                try:
                    df = self.client.get_player_game_log(
                        player_id=player.id,
                        season=season,
                        season_type=season_type.value,
                    )
                    self.metrics.api_calls += 1

                    if df is not None and not df.empty:
                        transformed = transform_game_log(df, player)
                        if not transformed.empty:
                            player_games.append(transformed)

                except Exception as e:
                    self.record_item_error(
                        f"player_{player.id}_{season}",
                        e,
                        log_error=True,
                    )

        if player_games:
            return pd.concat(player_games, ignore_index=True)
        return pd.DataFrame()

    def transform_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """Transform data (already done in fetch_data for this populator)."""
        # Transformation happens per-player in fetch_data
        # Just ensure column types are correct
        if df.empty:
            return df

        # Ensure integer columns are nullable int
        int_cols = [
            "game_id",
            "team_id",
            "player_id",
            "fgm",
            "fga",
            "fg3m",
            "fg3a",
            "ftm",
            "fta",
            "oreb",
            "dreb",
            "reb",
            "ast",
            "stl",
            "blk",
            "tov",
            "pf",
            "pts",
        ]
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        return df


# =============================================================================
# PUBLIC API
# =============================================================================


def populate_player_game_stats(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    active_only: bool = False,
    limit: int | None = None,
    resume_from: int | None = None,
    delay: float = 0.6,
    season_types: list[str] | None = None,
    client: NBAClient | None = None,
    reset: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate player_game_stats_raw table.

    This is the public API that wraps the PlayerGameStatsPopulator class.

    Args:
        db_path: Path to DuckDB database.
        seasons: List of seasons to fetch (e.g., ["2023-24", "2022-23"]).
        active_only: If True, only fetch active players.
        limit: Maximum number of players to process.
        resume_from: Resume from a specific player ID.
        delay: Delay between API requests in seconds.
        season_types: List of season types (e.g., ["Regular Season", "Playoffs"]).
        client: NBAClient instance (creates default if None).
        reset: Whether to reset progress before starting.
        dry_run: If True, don't write to database.

    Returns:
        Dictionary with statistics about the population process.
    """
    # Create client with configured delay
    if client is None:
        client = get_client()
    client.config.request_delay = delay

    # Create and run populator
    populator = PlayerGameStatsPopulator(
        db_path=db_path or str(get_db_path()),
        client=client,
    )

    return populator.run(
        resume=not reset,
        reset_progress=reset,
        dry_run=dry_run,
        seasons=seasons,
        season_types=season_types,
        active_only=active_only,
        limit=limit,
        resume_from=resume_from,
    )


# =============================================================================
# CLI
# =============================================================================


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Populate player_game_stats_raw table from NBA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (all players, all seasons) - TAKES MANY HOURS
  python -m src.scripts.populate.populate_player_game_stats

  # Recent seasons only (faster)
  python -m src.scripts.populate.populate_player_game_stats --seasons 2024-25 2023-24

  # Active players only
  python -m src.scripts.populate.populate_player_game_stats --active-only --seasons 2024-25

  # Test with 5 players
  python -m src.scripts.populate.populate_player_game_stats --limit 5 --seasons 2023-24

  # Resume interrupted run
  python -m src.scripts.populate.populate_player_game_stats --resume-from 2544
        """,
    )

    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database (default: from config)",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        help="Seasons to fetch (e.g., 2023-24 2022-23). Default: all seasons",
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only fetch active players",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of players to process",
    )
    parser.add_argument(
        "--resume-from",
        type=int,
        help="Resume from a specific player ID",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.6,
        help="Delay between API requests in seconds (default: 0.6)",
    )
    parser.add_argument(
        "--regular-only",
        action="store_true",
        help="Only fetch regular season games (skip playoffs)",
    )
    parser.add_argument(
        "--playoffs-only",
        action="store_true",
        help="Only fetch playoff games",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset progress and start fresh",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to database",
    )

    args = parser.parse_args()

    print_step("Initializing player game stats population")

    # Determine season types
    season_types = resolve_season_types(
        DEFAULT_SEASON_TYPES,
        regular_only=args.regular_only,
        playoffs_only=args.playoffs_only,
    )

    print_step("Running population")

    # Run population
    result = populate_player_game_stats(
        db_path=args.db,
        seasons=args.seasons,
        active_only=args.active_only,
        limit=args.limit,
        resume_from=args.resume_from,
        delay=args.delay,
        season_types=season_types,
        reset=args.reset,
        dry_run=args.dry_run,
    )

    print_step("Complete")
    print_summary_table("Player Game Stats Population", result)

    # Exit with error code if there were errors
    return 1 if result.get("error_count", 0) > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
