#!/usr/bin/env python3
"""Populate game_rotation table from NBA API.

This script fetches game rotation data (player substitution patterns, stint durations)
for games and populates the game_rotation table. Rotation data includes:
- Player in/out times (IN_TIME_REAL, OUT_TIME_REAL) in tenths of seconds
- Stint duration calculated from in/out times
- Points scored during stint (PLAYER_PTS)
- Point differential during stint (PT_DIFF)
- Usage percentage during stint (USG_PCT)

Note: The GameRotation API endpoint does NOT provide full box score stats
(AST, REB, STL, BLK, etc.) per stint. For detailed per-stint stats, you would
need to correlate with play-by-play data.

Use Cases:
- Player rotation analysis
- Lineup combination research
- Minutes distribution patterns
- Fatigue and rest analysis
- Substitution pattern optimization
- Usage rate analysis per stint

Usage:
    # Populate rotation data for recent seasons
    python scripts/populate/populate_game_rotation.py --seasons 2024-25 2023-24

    # Specific games
    python scripts/populate/populate_game_rotation.py --games 0022400001 0022400002

    # For specific teams
    python scripts/populate/populate_game_rotation.py --team-id 1610612747 --seasons 2023-24

    # Dry run (no database writes)
    python scripts/populate/populate_game_rotation.py --dry-run --seasons 2024-25

    # With custom delay for rate limiting
    python scripts/populate/populate_game_rotation.py --delay 1.0 --limit 100

    # Reset progress and start fresh
    python scripts/populate/populate_game_rotation.py --reset-progress --seasons 2024-25
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from typing import Any

import pandas as pd
from pydantic import Field, field_validator, model_validator

from src.scripts.populate.api_client import get_client
from src.scripts.populate.base import BasePopulator
from src.scripts.populate.config import (
    ALL_SEASONS,
    CACHE_DIR,
    DEFAULT_SEASON_TYPES,
    get_db_path,
)
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
GAME_ROTATION_PROGRESS_FILE = CACHE_DIR / "game_rotation_progress.json"

# Expected columns in the game_rotation table
# Note: The GameRotation API endpoint provides limited stats per stint:
# - Timing: IN_TIME_REAL, OUT_TIME_REAL
# - Points: PLAYER_PTS (points during stint)
# - Point differential: PT_DIFF
# - Usage: USG_PCT
# It does NOT provide full box score stats (AST, REB, etc.) per stint
GAME_ROTATION_COLUMNS = [
    "game_id",
    "team_id",
    "team_city",
    "team_name",
    "person_id",
    "player_first",
    "player_last",
    "player_name",
    "in_time_real",
    "out_time_real",
    "stint_duration",
    "stint_number",
    "player_pts",
    "pt_diff",
    "usg_pct",
    "game_date",
    "season_id",
    "season_type",
    "filename",
]

# Key columns for deduplication (unique identifier for each rotation entry)
KEY_COLUMNS = [
    "game_id",
    "team_id",
    "person_id",
    "stint_number",
]


# =============================================================================
# PYDANTIC SCHEMA FOR GAME ROTATION VALIDATION
# =============================================================================


class GameRotationRecord(NBABaseModel):
    """Pydantic schema for game rotation records.

    Validates individual rotation records from the NBA API GameRotation endpoint.
    Note: The GameRotation endpoint provides limited stats per stint:
    - Timing: IN_TIME_REAL, OUT_TIME_REAL
    - Points: PLAYER_PTS (points scored during stint)
    - Point differential: PT_DIFF (team point differential during stint)
    - Usage: USG_PCT (usage percentage during stint)
    """

    # Primary identifiers
    game_id: str = Field(
        ...,
        alias="GAME_ID",
        min_length=10,
        max_length=10,
        description="10-digit game ID",
    )
    team_id: int = Field(
        ...,
        alias="TEAM_ID",
        ge=1,
        description="Team ID",
    )
    team_city: str | None = Field(
        None,
        alias="TEAM_CITY",
        description="Team city name",
    )
    team_name: str | None = Field(
        None,
        alias="TEAM_NAME",
        description="Team name",
    )
    person_id: int = Field(
        ...,
        alias="PERSON_ID",
        ge=1,
        description="NBA player ID",
    )
    player_first: str | None = Field(
        None,
        alias="PLAYER_FIRST",
        description="Player first name",
    )
    player_last: str | None = Field(
        None,
        alias="PLAYER_LAST",
        description="Player last name",
    )

    # Timing data
    in_time_real: float | None = Field(
        None,
        alias="IN_TIME_REAL",
        ge=0,
        description="Time player entered the game (in tenths of seconds from game start)",
    )
    out_time_real: float | None = Field(
        None,
        alias="OUT_TIME_REAL",
        ge=0,
        description="Time player exited the game (in tenths of seconds from game start)",
    )

    # Stint statistics
    player_pts: int | None = Field(
        None,
        alias="PLAYER_PTS",
        ge=0,
        description="Points scored during this stint",
    )
    pt_diff: int | None = Field(
        None,
        alias="PT_DIFF",
        description="Team point differential during this stint",
    )
    usg_pct: float | None = Field(
        None,
        alias="USG_PCT",
        ge=0,
        le=1,
        description="Player usage percentage during this stint",
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

    @model_validator(mode="after")
    def validate_timing_consistency(self) -> GameRotationRecord:
        """Validate timing data consistency."""
        if (
            self.in_time_real is not None
            and self.out_time_real is not None
            and self.out_time_real < self.in_time_real
        ):
            logger.warning(
                "Inconsistent timing data: out_time (%s) < in_time (%s) for player %s",
                self.out_time_real,
                self.in_time_real,
                self.person_id,
            )
        return self


# =============================================================================
# GAME ROTATION POPULATOR CLASS
# =============================================================================


class GameRotationPopulator(BasePopulator):
    """Populator for game_rotation table.

    Fetches game rotation data from the NBA API GameRotation endpoint
    for each game in the specified seasons.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the game rotation populator.

        Args:
            **kwargs: Arguments passed to BasePopulator.
        """
        super().__init__(**kwargs)
        self._fetched_game_keys: list[str] = []
        self._game_metadata: dict[str, dict[str, Any]] = {}

    def get_table_name(self) -> str:
        """Return the target table name."""
        return "game_rotation"

    def get_key_columns(self) -> list[str]:
        """Return primary key columns for deduplication."""
        return KEY_COLUMNS

    def get_expected_columns(self) -> list[str]:
        """Return expected columns for validation."""
        return GAME_ROTATION_COLUMNS

    def get_data_type(self) -> str:
        """Return data type identifier for validation."""
        return "game_rotation"

    def _load_game_ids_from_db(
        self,
        seasons: list[str] | None = None,
        season_types: list[str] | None = None,
        team_id: int | None = None,
    ) -> list[str]:
        """Load game IDs from the database based on filters.

        Args:
            seasons: List of seasons to filter (e.g., ["2024-25", "2023-24"]).
            season_types: List of season types to filter.
            team_id: Optional team ID filter.

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
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
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

                # Team filter
                if team_id and "team_id" in cols:
                    where_clauses.append("team_id = ?")
                    params.append(team_id)

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
            cols_result = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
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
                        SELECT DISTINCT {", ".join(select_cols)}
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
        return load_json_file(GAME_ROTATION_PROGRESS_FILE, default)

    def _save_progress(self, progress: dict[str, Any]) -> None:
        """Save progress to file."""
        save_json_file(GAME_ROTATION_PROGRESS_FILE, progress)

    def _assign_stint_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Assign stint numbers to rotation entries.

        Each player's stints within a game are numbered sequentially by in_time.

        Args:
            df: DataFrame with rotation data.

        Returns:
            DataFrame with stint_number column added.
        """
        if df.empty or "IN_TIME_REAL" not in df.columns:
            df["stint_number"] = 0
            return df

        # Sort by player and in_time, then assign stint numbers
        df = df.sort_values(["GAME_ID", "TEAM_ID", "PERSON_ID", "IN_TIME_REAL"]).copy()

        # Group by game, team, player and assign sequential stint numbers
        df["stint_number"] = (
            df.groupby(["GAME_ID", "TEAM_ID", "PERSON_ID"]).cumcount() + 1
        )

        return df

    def _calculate_stint_duration(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate stint duration from in/out times.

        Args:
            df: DataFrame with IN_TIME_REAL and OUT_TIME_REAL columns.

        Returns:
            DataFrame with stint_duration column added.
        """
        if df.empty:
            df["stint_duration"] = None
            return df

        # Calculate duration (times are in tenths of seconds)
        # Duration = OUT_TIME - IN_TIME, converted to seconds
        if "IN_TIME_REAL" in df.columns and "OUT_TIME_REAL" in df.columns:
            df["stint_duration"] = (
                pd.to_numeric(df["OUT_TIME_REAL"], errors="coerce")
                - pd.to_numeric(df["IN_TIME_REAL"], errors="coerce")
            ) / 10.0  # Convert from tenths of seconds to seconds
        else:
            df["stint_duration"] = None

        return df

    def fetch_data(self, **kwargs: Any) -> pd.DataFrame | None:
        """Fetch game rotation data for all games in the specified parameters.

        Args:
            **kwargs: Population parameters including:
                - games: Explicit list of game IDs to process.
                - seasons: List of seasons to fetch.
                - season_types: List of season types to fetch.
                - team_id: Optional team ID filter.
                - limit: Maximum number of games to process.
                - resume: Whether to skip completed games.

        Returns:
            DataFrame with all game rotation data, or None if no data found.
        """
        games: list[str] | None = kwargs.get("games")
        seasons: list[str] | None = kwargs.get("seasons") or ALL_SEASONS[:3]
        season_types: list[str] | None = (
            kwargs.get("season_types") or DEFAULT_SEASON_TYPES
        )
        team_id: int | None = kwargs.get("team_id")
        limit: int | None = kwargs.get("limit")
        resume: bool = kwargs.get("resume", True)

        # Get list of games to process
        if games:
            games_to_process = [str(g).zfill(10) for g in games]
        else:
            games_to_process = self._load_game_ids_from_db(
                seasons=seasons,
                season_types=season_types,
                team_id=team_id,
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
            "Fetching game rotation data for %d games...",
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

                # Fetch game rotation data
                rotation_data = self.client.get_game_rotation(game_id=game_id)

                if rotation_data is not None:
                    home_df = rotation_data.get("home_team", pd.DataFrame())
                    away_df = rotation_data.get("away_team", pd.DataFrame())

                    # Combine home and away rotations
                    combined_df = pd.concat(
                        [home_df, away_df],
                        ignore_index=True,
                    )

                    if not combined_df.empty:
                        # Add game_id if not present
                        if "GAME_ID" not in combined_df.columns:
                            combined_df["GAME_ID"] = game_id

                        # Assign stint numbers and calculate duration
                        combined_df = self._assign_stint_numbers(combined_df)
                        combined_df = self._calculate_stint_duration(combined_df)

                        # Add metadata
                        combined_df["_game_id"] = game_id
                        metadata = self._game_metadata.get(game_id, {})
                        combined_df["_game_date"] = metadata.get("game_date")
                        combined_df["_season_id"] = metadata.get("season_id")
                        combined_df["_season_type"] = metadata.get("season_type")

                        all_data.append(combined_df)
                        games_with_data += 1
                        self._fetched_game_keys.append(game_id)
                        self.metrics.api_calls += 1

                        logger.debug(
                            "  Game %s: %d rotation entries",
                            game_id,
                            len(combined_df),
                        )
                    else:
                        logger.debug("  Game %s: no rotation data", game_id)
                        no_data_games.add(game_id)
                        games_no_data += 1
                else:
                    logger.debug("  Game %s: no data returned", game_id)
                    no_data_games.add(game_id)
                    games_no_data += 1

                # Update progress periodically
                games_processed += 1
                if games_processed % 50 == 0:
                    progress["completed_games"] = list(
                        completed_games | set(self._fetched_game_keys)
                    )
                    progress["no_data_games"] = list(no_data_games)
                    progress["last_game_id"] = game_id
                    self._save_progress(progress)

                # Rate limiting
                time.sleep(self.client.config.request_delay)

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
        progress["completed_games"] = list(
            completed_games | set(self._fetched_game_keys)
        )
        progress["no_data_games"] = list(no_data_games)
        self._save_progress(progress)

        logger.info(
            "Fetch complete: %d games with data, %d no data, %d errors",
            games_with_data,
            games_no_data,
            errors_count,
        )

        if not all_data:
            logger.info("No game rotation data fetched")
            return None

        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info("Total rotation entries fetched: %d", len(combined_df))
        return combined_df

    def transform_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """Transform game rotation data to match the database schema.

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
        output["team_id"] = pd.to_numeric(df["TEAM_ID"], errors="coerce").astype(
            "Int64"
        )
        output["team_city"] = df.get("TEAM_CITY", "")
        output["team_name"] = df.get("TEAM_NAME", "")
        output["person_id"] = pd.to_numeric(df["PERSON_ID"], errors="coerce").astype(
            "Int64"
        )

        # Player name fields (API provides PLAYER_FIRST and PLAYER_LAST)
        player_first = (
            df["PLAYER_FIRST"].fillna("")
            if "PLAYER_FIRST" in df.columns
            else pd.Series([""] * len(df))
        )
        player_last = (
            df["PLAYER_LAST"].fillna("")
            if "PLAYER_LAST" in df.columns
            else pd.Series([""] * len(df))
        )
        output["player_first"] = player_first
        output["player_last"] = player_last
        # Create combined player_name for convenience
        output["player_name"] = (player_first + " " + player_last).str.strip()

        # Timing data
        output["in_time_real"] = pd.to_numeric(df["IN_TIME_REAL"], errors="coerce")
        output["out_time_real"] = pd.to_numeric(df["OUT_TIME_REAL"], errors="coerce")
        output["stint_duration"] = (
            pd.to_numeric(df["stint_duration"], errors="coerce")
            if "stint_duration" in df.columns
            else None
        )
        output["stint_number"] = (
            pd.to_numeric(df["stint_number"], errors="coerce").astype("Int64")
            if "stint_number" in df.columns
            else 1
        )

        # Stint statistics (actual fields from GameRotation API)
        output["player_pts"] = (
            pd.to_numeric(df["PLAYER_PTS"], errors="coerce").astype("Int64")
            if "PLAYER_PTS" in df.columns
            else None
        )
        output["pt_diff"] = (
            pd.to_numeric(df["PT_DIFF"], errors="coerce").astype("Int64")
            if "PT_DIFF" in df.columns
            else None
        )
        output["usg_pct"] = (
            pd.to_numeric(df["USG_PCT"], errors="coerce")
            if "USG_PCT" in df.columns
            else None
        )

        # Game metadata
        output["game_date"] = df.get("GAME_DATE", df.get("_game_date"))
        output["season_id"] = df.get("_season_id")
        output["season_type"] = df.get("_season_type")

        # Filename for tracking
        output["filename"] = "nba_api.gamerotation"

        # Ensure all expected columns exist
        for col in GAME_ROTATION_COLUMNS:
            if col not in output.columns:
                output[col] = None

        # Reorder columns
        output = output[GAME_ROTATION_COLUMNS]

        # Drop duplicates based on key columns
        output = output.drop_duplicates(subset=KEY_COLUMNS, keep="first")

        logger.info("Transformed %d rotation records", len(output))
        return output

    def pre_run_hook(self, **kwargs: Any) -> None:
        """Reset fetched keys for this run."""
        self._fetched_game_keys = []
        self._game_metadata = {}

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


def populate_game_rotation(
    db_path: str | None = None,
    games: list[str] | None = None,
    seasons: list[str] | None = None,
    season_types: list[str] | None = None,
    team_id: int | None = None,
    limit: int | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate game_rotation table.

    Args:
        db_path: Path to DuckDB database.
        games: Explicit list of game IDs to process.
        seasons: List of seasons to fetch (e.g., ["2024-25", "2023-24"]).
        season_types: List of season types (e.g., ["Regular Season", "Playoffs"]).
        team_id: Optional team ID filter.
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
    logger.info("NBA GAME ROTATION POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Seasons: {seasons}")
    logger.info(f"Season Types: {season_types}")
    if games:
        logger.info(f"Games: {len(games)} specified")
    if team_id:
        logger.info(f"Team ID Filter: {team_id}")
    if limit:
        logger.info(f"Limit: {limit} games")
    logger.info(f"Request Delay: {delay}s")

    # Create and run populator
    populator = GameRotationPopulator(
        db_path=db_path,
        client=client,
    )

    return populator.run(
        games=games,
        seasons=seasons,
        season_types=season_types,
        team_id=team_id,
        limit=limit,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


# =============================================================================
# CLI
# =============================================================================


def main() -> None:
    """Parse command-line arguments and run the game rotation population process."""
    parser = argparse.ArgumentParser(
        description="Populate game_rotation table from NBA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (last 3 seasons)
  python scripts/populate/populate_game_rotation.py

  # Specific seasons only
  python scripts/populate/populate_game_rotation.py --seasons 2024-25 2023-24

  # Specific games
  python scripts/populate/populate_game_rotation.py --games 0022400001 0022400002

  # For a specific team
  python scripts/populate/populate_game_rotation.py --team-id 1610612747 --seasons 2023-24

  # Regular season only
  python scripts/populate/populate_game_rotation.py --regular-season-only

  # Playoffs only
  python scripts/populate/populate_game_rotation.py --playoffs-only

  # Limit number of games
  python scripts/populate/populate_game_rotation.py --limit 100

  # Reset progress and start fresh
  python scripts/populate/populate_game_rotation.py --reset-progress

  # Dry run (no database writes)
  python scripts/populate/populate_game_rotation.py --dry-run
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
        "--team-id",
        type=int,
        help="Filter by team ID",
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

    args = parser.parse_args()

    # Determine season types
    season_types = resolve_season_types(
        DEFAULT_SEASON_TYPES,
        regular_only=args.regular_season_only,
        playoffs_only=args.playoffs_only,
    )

    try:
        stats = populate_game_rotation(
            db_path=args.db,
            games=args.games,
            seasons=args.seasons,
            season_types=season_types,
            team_id=args.team_id,
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
