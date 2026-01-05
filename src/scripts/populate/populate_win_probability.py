#!/usr/bin/env python3
"""Populate win_probability table from NBA API.

This script fetches win probability play-by-play data for games and populates
the win_probability table. Win probability data includes real-time win probability
for each play, useful for:
- Clutch analysis (games decided in final moments)
- Momentum tracking throughout a game
- Game flow visualization
- High-leverage play identification
- Understanding swing plays that changed game outcomes

Usage:
    # Populate win probability for recent seasons
    python scripts/populate/populate_win_probability.py --seasons 2024-25 2023-24

    # Specific games
    python scripts/populate/populate_win_probability.py --games 0022400001 0022400002

    # Dry run (no database writes)
    python scripts/populate/populate_win_probability.py --dry-run --seasons 2024-25

    # With custom delay for rate limiting
    python scripts/populate/populate_win_probability.py --delay 1.0 --limit 100
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from typing import Any

import pandas as pd
from pydantic import Field, field_validator

from src.scripts.populate.api_client import NBAClient, get_client
from src.scripts.populate.base import BasePopulator
from src.scripts.populate.config import (
    ALL_SEASONS,
    CACHE_DIR,
    DEFAULT_SEASON_TYPES,
    get_db_path,
)
from src.scripts.populate.exceptions import DataNotFoundError, TransientError
from src.scripts.populate.helpers import (
    configure_logging,
    format_duration,
    load_json_file,
    resolve_season_types,
    save_json_file,
)
from src.scripts.populate.schemas import NBABaseModel


configure_logging()
logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

# Progress file for tracking completed games
WIN_PROBABILITY_PROGRESS_FILE = CACHE_DIR / "win_probability_progress.json"

# Expected columns in the win_probability table
WIN_PROBABILITY_COLUMNS = [
    "game_id",
    "event_num",
    "home_pct",
    "visitor_pct",
    "home_pts",
    "visitor_pts",
    "home_score_margin",
    "period",
    "seconds_remaining",
    "description",
    "location",
    "event_type",
    "player_id",
    "team_id",
    "player_name",
    "home_team_id",
    "home_team_abbreviation",
    "visitor_team_id",
    "visitor_team_abbreviation",
    "game_date",
    "season_id",
    "season_type",
]

# Key columns for deduplication (unique identifier for each event)
KEY_COLUMNS = ["game_id", "event_num"]


# =============================================================================
# PYDANTIC SCHEMA FOR WIN PROBABILITY VALIDATION
# =============================================================================


class WinProbabilityRecord(NBABaseModel):
    """Pydantic schema for win probability records.

    Validates individual win probability records from the NBA API WinProbabilityPBP endpoint.
    """

    # Primary identifiers
    game_id: str = Field(
        ...,
        alias="GAME_ID",
        min_length=10,
        max_length=10,
        description="10-digit game ID",
    )
    event_num: int = Field(
        ...,
        alias="EVENT_NUM",
        ge=0,
        description="Event number within the game",
    )

    # Win probability metrics
    home_pct: float | None = Field(
        None,
        alias="HOME_PCT",
        ge=0.0,
        le=1.0,
        description="Home team win probability (0-1)",
    )
    visitor_pct: float | None = Field(
        None,
        alias="VISITOR_PCT",
        ge=0.0,
        le=1.0,
        description="Visitor team win probability (0-1)",
    )

    # Score information
    home_pts: int | None = Field(
        None,
        alias="HOME_PTS",
        ge=0,
        description="Home team points at this moment",
    )
    visitor_pts: int | None = Field(
        None,
        alias="VISITOR_PTS",
        ge=0,
        description="Visitor team points at this moment",
    )
    home_score_margin: int | None = Field(
        None,
        alias="HOME_SCORE_MARGIN",
        description="Home score margin (positive = home leading)",
    )

    # Time context
    period: int | None = Field(
        None,
        alias="PERIOD",
        ge=1,
        le=10,
        description="Game period (1-4, or OT periods 5+)",
    )
    seconds_remaining: int | None = Field(
        None,
        alias="SECONDS_REMAINING",
        ge=0,
        description="Seconds remaining in period",
    )

    # Event details
    description: str | None = Field(
        None,
        alias="DESCRIPTION",
        description="Play description",
    )
    location: str | None = Field(
        None,
        alias="LOCATION",
        description="Location indicator (HOME/VISITOR)",
    )
    event_type: int | None = Field(
        None,
        alias="EVENTMSGTYPE",
        description="Event message type code",
    )

    # Player and team info
    player_id: int | None = Field(
        None,
        alias="PLAYER_ID",
        description="Player ID associated with the event",
    )
    team_id: int | None = Field(
        None,
        alias="TEAM_ID",
        description="Team ID associated with the event",
    )

    @field_validator("game_id", mode="before")
    @classmethod
    def validate_game_id(cls, v: Any) -> str:
        """Ensure game_id is a 10-character string."""
        if v is None:
            raise ValueError("game_id cannot be None")
        game_id_str = str(v).strip()
        if game_id_str.isdigit():
            game_id_str = game_id_str.zfill(10)
        return game_id_str


# =============================================================================
# WIN PROBABILITY POPULATOR CLASS
# =============================================================================


class WinProbabilityPopulator(BasePopulator):
    """Populator for win_probability table.

    Fetches win probability data from the NBA API WinProbabilityPBP endpoint
    for each game in the specified seasons. This follows the per-game fetching
    pattern similar to ShotChartPopulator.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the win probability populator.

        Args:
            **kwargs: Arguments passed to BasePopulator.
        """
        super().__init__(**kwargs)
        self._fetched_game_keys: list[str] = []
        self._game_metadata: dict[str, dict[str, Any]] = {}

    def get_table_name(self) -> str:
        """Return the target table name."""
        return "win_probability"

    def get_key_columns(self) -> list[str]:
        """Return primary key columns for deduplication."""
        return KEY_COLUMNS

    def get_expected_columns(self) -> list[str]:
        """Return expected columns for validation."""
        return WIN_PROBABILITY_COLUMNS

    def get_data_type(self) -> str:
        """Return data type identifier for validation."""
        return "win_probability"

    def _load_game_ids_from_db(
        self,
        seasons: list[str] | None = None,
        season_types: list[str] | None = None,
    ) -> list[str]:
        """Load game IDs from the database based on filters.

        Args:
            seasons: List of seasons to filter (e.g., ["2024-25", "2023-24"]).
            season_types: List of season types to filter.

        Returns:
            List of game IDs to process.
        """
        conn = self.connect()

        # Try multiple tables to find game IDs
        table_candidates = [
            "league_game_log_raw",
            "league_game_log",
            "game_gold",
            "game_silver",
            "game_raw",
            "game",
            "games",
        ]

        for table_name in table_candidates:
            try:
                # Check if table exists and has data
                count = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                if count == 0:
                    continue

                # Get column names
                cols_result = conn.execute(
                    f"PRAGMA table_info('{table_name}')"
                ).fetchall()
                cols = [col[1] for col in cols_result]

                # Build query
                query_parts = [f"SELECT DISTINCT game_id FROM {table_name}"]
                where_clauses = []
                params: list[Any] = []

                # Season filter
                if seasons and "season_id" in cols:
                    # Convert season format to season_id format
                    # e.g., "2024-25" -> "22024" (Regular Season)
                    season_ids = []
                    for season in seasons:
                        # Format: "2024-25" -> "22024" (Regular Season)
                        year = season.split("-")[0]
                        season_ids.append(f"2{year}")
                    placeholders = ", ".join(["?" for _ in season_ids])
                    where_clauses.append(f"season_id IN ({placeholders})")
                    params.extend(season_ids)

                # Season type filter
                if season_types and "season_type" in cols:
                    placeholders = ", ".join(["?" for _ in season_types])
                    where_clauses.append(f"season_type IN ({placeholders})")
                    params.extend(season_types)

                # Build final query
                if where_clauses:
                    query_parts.append("WHERE " + " AND ".join(where_clauses))
                query_parts.append("ORDER BY game_id DESC")

                query = " ".join(query_parts)
                result = conn.execute(query, params).fetchall()
                game_ids = [str(row[0]).zfill(10) for row in result if row[0]]

                if game_ids:
                    logger.info(
                        "Found %d game IDs from %s table",
                        len(game_ids),
                        table_name,
                    )

                    # If we have league_game_log, also get metadata
                    if "league_game_log" in table_name:
                        self._load_game_metadata(conn, table_name, game_ids)

                    return game_ids

            except Exception as e:
                logger.debug("Could not query %s: %s", table_name, e)
                continue

        logger.warning("No game tables found with data")
        return []

    def _load_game_metadata(
        self,
        conn: Any,
        table_name: str,
        game_ids: list[str],
    ) -> None:
        """Load game metadata (game_date, season_id, season_type) for games.

        Args:
            conn: Database connection.
            table_name: Table to query.
            game_ids: List of game IDs to get metadata for.
        """
        try:
            # Check available columns
            cols_result = conn.execute(
                f"PRAGMA table_info('{table_name}')"
            ).fetchall()
            cols = [col[1] for col in cols_result]

            select_cols = ["game_id"]
            if "game_date" in cols:
                select_cols.append("game_date")
            if "season_id" in cols:
                select_cols.append("season_id")
            if "season_type" in cols:
                select_cols.append("season_type")

            if len(select_cols) > 1:
                # Query in batches to avoid SQL size limits
                batch_size = 500
                for i in range(0, len(game_ids), batch_size):
                    batch = game_ids[i : i + batch_size]
                    placeholders = ", ".join(["?" for _ in batch])
                    query = f"""
                        SELECT DISTINCT {', '.join(select_cols)}
                        FROM {table_name}
                        WHERE game_id IN ({placeholders})
                    """
                    result = conn.execute(query, batch).fetchall()

                    for row in result:
                        game_id = str(row[0]).zfill(10)
                        metadata: dict[str, Any] = {}
                        for idx, col in enumerate(select_cols[1:], 1):
                            metadata[col] = row[idx]
                        self._game_metadata[game_id] = metadata

            logger.info("Loaded metadata for %d games", len(self._game_metadata))

        except Exception as e:
            logger.warning("Could not load game metadata: %s", e)

    def _load_progress(self) -> dict[str, Any]:
        """Load progress from file."""
        default: dict[str, Any] = {
            "completed_games": [],
            "no_data_games": [],
            "last_game_id": None,
            "errors": [],
        }
        return load_json_file(WIN_PROBABILITY_PROGRESS_FILE, default)

    def _save_progress(self, progress: dict[str, Any]) -> None:
        """Save progress to file."""
        save_json_file(WIN_PROBABILITY_PROGRESS_FILE, progress)

    def _fetch_game_win_probability(self, game_id: str) -> pd.DataFrame | None:
        """Fetch win probability data for a single game.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            DataFrame with win probability data or None if not available

        Raises:
            DataNotFoundError: When data is not found for the game
            TransientError: When a retriable error occurs
        """
        try:
            df = self.client.get_win_probability(game_id)

            if df is None or df.empty:
                return None

            # Add game_id column if not present
            if "GAME_ID" not in df.columns:
                df["GAME_ID"] = game_id

            return df

        except Exception as e:
            error_str = str(e).lower()
            if "404" in error_str or "not found" in error_str:
                raise DataNotFoundError(
                    message=f"Win probability not found for game {game_id}",
                    resource_type="win_probability",
                    resource_id=game_id,
                ) from e
            if "429" in error_str or "rate" in error_str:
                raise TransientError(
                    message=f"Rate limited fetching game {game_id}",
                    retry_after=60.0,
                ) from e
            raise

    def fetch_data(self, **kwargs: Any) -> pd.DataFrame | None:
        """Fetch win probability data for all games in the specified parameters.

        Args:
            **kwargs: Population parameters including:
                - games: Explicit list of game IDs to process.
                - seasons: List of seasons to fetch.
                - season_types: List of season types to fetch.
                - limit: Maximum number of games to process.
                - resume: Whether to skip completed games.

        Returns:
            DataFrame with all win probability data, or None if no data found.
        """
        games: list[str] | None = kwargs.get("games")
        seasons: list[str] | None = kwargs.get("seasons") or ALL_SEASONS[:3]
        season_types: list[str] | None = kwargs.get("season_types") or DEFAULT_SEASON_TYPES
        limit: int | None = kwargs.get("limit")
        resume: bool = kwargs.get("resume", True)

        # Get list of games to process
        if games:
            games_to_process = [str(g).zfill(10) for g in games]
        else:
            games_to_process = self._load_game_ids_from_db(
                seasons=seasons,
                season_types=season_types,
            )

        if not games_to_process:
            logger.warning("No games found to process")
            return None

        # Apply limit
        if limit:
            games_to_process = games_to_process[:limit]

        # Load progress and filter completed games
        progress = self._load_progress()
        completed_games = set(progress.get("completed_games", []))
        no_data_games = set(progress.get("no_data_games", []))

        if resume:
            remaining_games = [
                g
                for g in games_to_process
                if g not in completed_games and g not in no_data_games
            ]
            logger.info(
                "Games to process: %d (skipping %d completed, %d no-data)",
                len(remaining_games),
                len(completed_games & set(games_to_process)),
                len(no_data_games & set(games_to_process)),
            )
        else:
            remaining_games = games_to_process

        if not remaining_games:
            logger.info("All games already processed")
            return None

        logger.info(
            "Fetching win probability data for %d games...",
            len(remaining_games),
        )

        # Fetch data for each game
        all_data: list[pd.DataFrame] = []
        start_time = time.monotonic()
        games_processed = 0
        games_with_data = 0
        games_no_data = 0
        errors_count = 0

        for idx, game_id in enumerate(remaining_games, 1):
            try:
                # Log progress
                if idx % 10 == 0 or idx == 1:
                    elapsed = time.monotonic() - start_time
                    avg_time = elapsed / idx if idx > 0 else 0
                    eta = (len(remaining_games) - idx) * avg_time
                    logger.info(
                        "[%d/%d] Processing game %s (elapsed=%s, eta=%s)",
                        idx,
                        len(remaining_games),
                        game_id,
                        format_duration(elapsed),
                        format_duration(eta),
                    )

                # Fetch win probability data
                df = self._fetch_game_win_probability(game_id)

                if df is not None and not df.empty:
                    # Add metadata
                    df["_game_id"] = game_id
                    metadata = self._game_metadata.get(game_id, {})
                    df["_game_date"] = metadata.get("game_date")
                    df["_season_id"] = metadata.get("season_id")
                    df["_season_type"] = metadata.get("season_type")

                    all_data.append(df)
                    games_with_data += 1
                    self._fetched_game_keys.append(game_id)
                    self.metrics.api_calls += 1
                    completed_games.add(game_id)

                    logger.debug(
                        "  Game %s: %d events",
                        game_id,
                        len(df),
                    )
                else:
                    logger.debug("  Game %s: no data", game_id)
                    no_data_games.add(game_id)
                    games_no_data += 1

                # Update progress periodically
                games_processed += 1
                if games_processed % 50 == 0:
                    progress["completed_games"] = list(completed_games)
                    progress["no_data_games"] = list(no_data_games)
                    progress["last_game_id"] = game_id
                    self._save_progress(progress)

                # Rate limiting
                time.sleep(self.client.config.request_delay)

            except DataNotFoundError:
                logger.debug("  Game %s: data not found (expected)", game_id)
                no_data_games.add(game_id)
                games_no_data += 1

            except TransientError as e:
                logger.warning("Transient error for game %s: %s", game_id, e)
                errors_count += 1
                self.metrics.add_error(str(e), {"game_id": game_id})
                progress.setdefault("errors", []).append(
                    {
                        "game_id": game_id,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                    }
                )
                # Wait longer before continuing
                time.sleep(e.retry_after or 30.0)

            except Exception as e:
                logger.warning("Error fetching game %s: %s", game_id, e)
                errors_count += 1
                self.metrics.add_error(str(e), {"game_id": game_id})
                progress.setdefault("errors", []).append(
                    {
                        "game_id": game_id,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                    }
                )
                continue

        # Save final progress
        progress["completed_games"] = list(completed_games)
        progress["no_data_games"] = list(no_data_games)
        self._save_progress(progress)

        logger.info(
            "Fetch complete: %d games with data, %d no data, %d errors",
            games_with_data,
            games_no_data,
            errors_count,
        )

        if not all_data:
            logger.info("No win probability data fetched")
            return None

        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info("Total events fetched: %d", len(combined_df))
        return combined_df

    def transform_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """Transform win probability data to match the database schema.

        Args:
            df: Raw DataFrame from the API.
            **kwargs: Additional transformation parameters.

        Returns:
            Transformed DataFrame ready for insertion.
        """
        if df.empty:
            return df

        output = pd.DataFrame()

        # Primary identifiers
        output["game_id"] = df.get("GAME_ID", df.get("_game_id")).apply(
            lambda x: str(x).zfill(10) if pd.notna(x) else None
        )
        output["event_num"] = pd.to_numeric(
            df.get("EVENT_NUM"), errors="coerce"
        ).astype("Int64")

        # Win probability metrics
        # Handle percentages - API may return 0-100 or 0-1
        home_pct = pd.to_numeric(df.get("HOME_PCT"), errors="coerce")
        visitor_pct = pd.to_numeric(df.get("VISITOR_PCT"), errors="coerce")

        # Convert to 0-1 range if needed
        if home_pct.max() > 1:
            home_pct = home_pct / 100.0
        if visitor_pct.max() > 1:
            visitor_pct = visitor_pct / 100.0

        output["home_pct"] = home_pct
        output["visitor_pct"] = visitor_pct

        # Score information
        output["home_pts"] = pd.to_numeric(
            df.get("HOME_PTS"), errors="coerce"
        ).astype("Int64")
        output["visitor_pts"] = pd.to_numeric(
            df.get("VISITOR_PTS"), errors="coerce"
        ).astype("Int64")
        output["home_score_margin"] = pd.to_numeric(
            df.get("HOME_SCORE_MARGIN"), errors="coerce"
        ).astype("Int64")

        # Time context
        output["period"] = pd.to_numeric(df.get("PERIOD"), errors="coerce").astype(
            "Int64"
        )
        output["seconds_remaining"] = pd.to_numeric(
            df.get("SECONDS_REMAINING"), errors="coerce"
        ).astype("Int64")

        # Event details
        output["description"] = df.get("DESCRIPTION", "")
        output["location"] = df.get("LOCATION", "")
        output["event_type"] = pd.to_numeric(
            df.get("EVENTMSGTYPE"), errors="coerce"
        ).astype("Int64")

        # Player and team info
        output["player_id"] = pd.to_numeric(
            df.get("PLAYER_ID"), errors="coerce"
        ).astype("Int64")
        output["team_id"] = pd.to_numeric(df.get("TEAM_ID"), errors="coerce").astype(
            "Int64"
        )
        output["player_name"] = df.get("PLAYER_NAME", "")

        # Team abbreviations and IDs
        output["home_team_id"] = pd.to_numeric(
            df.get("HOME_TEAM_ID"), errors="coerce"
        ).astype("Int64")
        output["home_team_abbreviation"] = df.get("HOME_TEAM_ABB", "")
        output["visitor_team_id"] = pd.to_numeric(
            df.get("VISITOR_TEAM_ID"), errors="coerce"
        ).astype("Int64")
        output["visitor_team_abbreviation"] = df.get("VISITOR_TEAM_ABB", "")

        # Game metadata (from our added columns or API)
        output["game_date"] = df.get("GAME_DATE", df.get("_game_date"))
        output["season_id"] = df.get("_season_id")
        output["season_type"] = df.get("_season_type")

        # Ensure all expected columns exist
        for col in WIN_PROBABILITY_COLUMNS:
            if col not in output.columns:
                output[col] = None

        # Reorder columns
        output = output[WIN_PROBABILITY_COLUMNS]

        # Drop duplicates based on key columns
        output = output.drop_duplicates(subset=KEY_COLUMNS, keep="first")

        logger.info("Transformed %d win probability records", len(output))
        return output

    def pre_run_hook(self, **kwargs: Any) -> None:
        """Reset fetched keys for this run and ensure table exists."""
        self._fetched_game_keys = []
        self._game_metadata = {}

        # Ensure raw table exists
        conn = self.connect()
        try:
            # Check if table exists
            result = conn.execute("""
                SELECT count(*) FROM information_schema.tables
                WHERE table_name = 'win_probability_raw'
            """).fetchone()

            if result[0] == 0:
                logger.info("Creating win_probability_raw table")
                conn.execute("""
                    CREATE TABLE win_probability_raw (
                        game_id VARCHAR NOT NULL,
                        event_num INTEGER NOT NULL,
                        home_pct DOUBLE,
                        visitor_pct DOUBLE,
                        home_pts INTEGER,
                        visitor_pts INTEGER,
                        home_score_margin INTEGER,
                        period INTEGER,
                        seconds_remaining INTEGER,
                        description VARCHAR,
                        location VARCHAR,
                        event_type INTEGER,
                        player_id INTEGER,
                        team_id INTEGER,
                        player_name VARCHAR,
                        home_team_id INTEGER,
                        home_team_abbreviation VARCHAR,
                        visitor_team_id INTEGER,
                        visitor_team_abbreviation VARCHAR,
                        game_date VARCHAR,
                        season_id VARCHAR,
                        season_type VARCHAR,
                        populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (game_id, event_num)
                    )
                """)
                conn.commit()
        except Exception as e:
            logger.warning("Could not check/create table: %s", e)

    def post_run_hook(self, **kwargs: Any) -> None:
        """Mark fetched games as completed after successful database writes."""
        dry_run = kwargs.get("dry_run", False)
        if dry_run:
            logger.info(
                "DRY RUN - not marking progress for fetched games (data was not written)"
            )
            return

        # Progress is saved incrementally during fetch_data
        logger.info(
            "Processed %d games in this run",
            len(self._fetched_game_keys),
        )


# =============================================================================
# MAIN POPULATION FUNCTION
# =============================================================================


def populate_win_probability(
    db_path: str | None = None,
    games: list[str] | None = None,
    seasons: list[str] | None = None,
    season_types: list[str] | None = None,
    limit: int | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate win_probability table.

    Args:
        db_path: Path to DuckDB database.
        games: Explicit list of game IDs to process.
        seasons: List of seasons to fetch (e.g., ["2024-25", "2023-24"]).
        season_types: List of season types (e.g., ["Regular Season", "Playoffs"]).
        limit: Maximum number of games to process.
        delay: Delay between API requests in seconds.
        reset_progress: Reset progress tracking before starting.
        dry_run: If True, don't actually insert data.

    Returns:
        Dictionary with population statistics.
    """
    db_path = db_path or str(get_db_path())
    seasons = seasons or ALL_SEASONS[:3]  # Default: last 3 seasons
    season_types = season_types or DEFAULT_SEASON_TYPES

    # Create client with custom delay
    client = get_client()
    client.config.request_delay = delay

    logger.info("=" * 70)
    logger.info("NBA WIN PROBABILITY POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Seasons: {seasons}")
    logger.info(f"Season Types: {season_types}")
    if games:
        logger.info(f"Games: {len(games)} specified")
    if limit:
        logger.info(f"Limit: {limit} games")
    logger.info(f"Request Delay: {delay}s")

    # Create and run populator
    populator = WinProbabilityPopulator(
        db_path=db_path,
        client=client,
    )

    return populator.run(
        games=games,
        seasons=seasons,
        season_types=season_types,
        limit=limit,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


# =============================================================================
# CLI
# =============================================================================


def main() -> None:
    """Parse command-line arguments and run the win probability population process."""
    parser = argparse.ArgumentParser(
        description="Populate win_probability table from NBA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (last 3 seasons)
  python scripts/populate/populate_win_probability.py

  # Specific seasons only
  python scripts/populate/populate_win_probability.py --seasons 2024-25 2023-24

  # Specific games
  python scripts/populate/populate_win_probability.py --games 0022400001 0022400002

  # Regular season only
  python scripts/populate/populate_win_probability.py --regular-season-only

  # Playoffs only
  python scripts/populate/populate_win_probability.py --playoffs-only

  # Limit number of games
  python scripts/populate/populate_win_probability.py --limit 100

  # Reset progress and start fresh
  python scripts/populate/populate_win_probability.py --reset-progress

  # Dry run (no database writes)
  python scripts/populate/populate_win_probability.py --dry-run
        """,
    )

    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database (default: src/backend/data/nba.duckdb)",
    )
    parser.add_argument(
        "--games",
        nargs="+",
        help="Specific game IDs to process",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        help="Seasons to fetch (e.g., 2024-25 2023-24)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of games to process",
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
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh, don't skip completed games",
    )

    args = parser.parse_args()

    # Determine season types
    season_types = resolve_season_types(
        DEFAULT_SEASON_TYPES,
        regular_only=args.regular_season_only,
        playoffs_only=args.playoffs_only,
    )

    try:
        stats = populate_win_probability(
            db_path=args.db,
            games=args.games,
            seasons=args.seasons,
            season_types=season_types,
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
