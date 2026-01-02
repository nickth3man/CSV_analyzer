#!/usr/bin/env python3
"""Populate play_by_play table from NBA API (PlayByPlayV3).

This script fetches play-by-play data for games and populates the play_by_play table
using the PlayByPlayV3 endpoint schema.

TODO: ROADMAP Phase 3.1 - Resolve NBA API issues blocking play-by-play population
- Current Status: Script implemented but blocked by NBA API access issues
- API endpoints may require authentication or have changed
- Consider alternatives:
  1. Use nba_api library's PlayByPlayV2/V3 endpoints
  2. Investigate if API keys/tokens are needed
  3. Check for rate limiting or IP blocking
  4. Consider caching/historical data sources if API unavailable
- Impact: Blocks clutch analysis, lineup analysis, and detailed event tracking
- Priority: HIGH (Phase 3.1)
Reference: docs/roadmap.md Phase 3.1

Features:
- Fetches play-by-play data for specific games or seasons
- Uses shared NBAClient with rate limiting and retry logic
- Handles different event types and action types
- Supports incremental updates with progress tracking
- Error handling with retry logic

Usage:
    # Populate play-by-play for recent games
    python scripts/populate/populate_play_by_play.py --limit 10 --seasons 2022-23

    # Specific games
    python scripts/populate/populate_play_by_play.py --games 0022200001 0022200002

    # With custom delay for rate limiting
    python scripts/populate/populate_play_by_play.py --delay 1.0 --limit 5
"""

import argparse
import json
import logging
import sys
import time
import traceback
from datetime import datetime
from typing import Any, cast

import duckdb
import pandas as pd

from src.scripts.populate.api_client import NBAClient, get_client

# Import shared modules from the populate package
from src.scripts.populate.config import (
    CACHE_DIR,
    ensure_cache_dir,
    get_db_path,
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

PROGRESS_FILE = CACHE_DIR / "play_by_play_progress.json"

PLAY_BY_PLAY_COLUMNS = [
    "game_id",
    "action_number",
    "clock",
    "period",
    "team_id",
    "team_tricode",
    "person_id",
    "player_name",
    "player_name_i",
    "x_legacy",
    "y_legacy",
    "shot_distance",
    "shot_result",
    "is_field_goal",
    "score_home",
    "score_away",
    "points_total",
    "location",
    "description",
    "action_type",
    "sub_type",
    "video_available",
    "shot_value",
    "action_id",
]

# Column mapping from NBA API (v3) to our schema
COLUMN_MAPPING = {
    "gameId": "game_id",
    "actionNumber": "action_number",
    "clock": "clock",
    "period": "period",
    "teamId": "team_id",
    "teamTricode": "team_tricode",
    "personId": "person_id",
    "playerName": "player_name",
    "playerNameI": "player_name_i",
    "xLegacy": "x_legacy",
    "yLegacy": "y_legacy",
    "shotDistance": "shot_distance",
    "shotResult": "shot_result",
    "isFieldGoal": "is_field_goal",
    "scoreHome": "score_home",
    "scoreAway": "score_away",
    "pointsTotal": "points_total",
    "location": "location",
    "description": "description",
    "actionType": "action_type",
    "subType": "sub_type",
    "videoAvailable": "video_available",
    "shotValue": "shot_value",
    "actionId": "action_id",
}


def ensure_play_by_play_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Ensure play_by_play table matches the PlayByPlayV3 schema."""
    expected = PLAY_BY_PLAY_COLUMNS
    try:
        cols = conn.execute("PRAGMA table_info('play_by_play')").fetchall()
        existing = [col[1] for col in cols]
        pk_cols = [
            col[1]
            for col in sorted(
                ((col[5], col[1]) for col in cols if col[5]),
                key=lambda item: item[0],
            )
        ]
    except Exception:
        existing = []
        pk_cols = []

    if existing != expected or pk_cols != ["game_id", "action_number"]:
        conn.execute("DROP TABLE IF EXISTS play_by_play")
        conn.execute(
            """
            CREATE TABLE play_by_play (
                game_id BIGINT NOT NULL,
                action_number BIGINT NOT NULL,
                clock VARCHAR,
                period INTEGER,
                team_id BIGINT,
                team_tricode VARCHAR,
                person_id BIGINT,
                player_name VARCHAR,
                player_name_i VARCHAR,
                x_legacy DOUBLE,
                y_legacy DOUBLE,
                shot_distance DOUBLE,
                shot_result VARCHAR,
                is_field_goal INTEGER,
                score_home VARCHAR,
                score_away VARCHAR,
                points_total INTEGER,
                location VARCHAR,
                description VARCHAR,
                action_type VARCHAR,
                sub_type VARCHAR,
                video_available INTEGER,
                shot_value INTEGER,
                action_id INTEGER,
                PRIMARY KEY (game_id, action_number)
            )
        """,
        )


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def load_progress() -> dict[str, Any]:
    """Load the persistent progress state for play-by-play population.

    Reads the JSON progress file and returns its contents. If the progress file does not exist, returns a default structure.

    Returns:
        progress (dict): Progress dictionary with keys:
            - completed_games (List[str]): game IDs that have been completed.
            - no_data_games (List[str]): game IDs that returned no data.
            - last_game_id (Optional[str]): the most recently processed game ID or None.
            - errors (List[Any]): recorded errors encountered during processing.
    """
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {
        "completed_games": [],
        "no_data_games": [],
        "last_game_id": None,
        "errors": [],
    }


def save_progress(progress: dict[str, Any]) -> None:
    """Persist the progress dictionary to the progress file in the cache directory.

    Parameters:
        progress (dict): Progress state to save. Expected keys include:
            - "completed_games" (list): game IDs already processed.
            - "last_game_id" (str|None): most recently processed game ID.
            - "errors" (list): recorded error entries.
    """
    ensure_cache_dir()
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    minutes, secs = divmod(total_seconds, 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s" if minutes else f"{secs}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{secs:02d}s"


def process_play_by_play_data(df: pd.DataFrame, game_id: int | None) -> pd.DataFrame:
    """Normalize a raw NBA play-by-play DataFrame to the play_by_play table schema.

    Ensures all expected NBA API columns are present (filling missing columns with None), renames columns according to the internal COLUMN_MAPPING, injects the provided game_id, and returns the DataFrame with columns ordered for insertion.

    Parameters:
        df (pd.DataFrame): Raw play-by-play DataFrame returned by the NBA API.
        game_id (str): Game identifier to assign to every row in the processed DataFrame.

    Returns:
        pd.DataFrame: Processed DataFrame with 'game_id' as the first column followed by the mapped play_by_play fields, ready for database insertion.
    """
    if df.empty:
        return df

    # Required columns for play_by_play table
    required_columns = list(COLUMN_MAPPING.keys())

    # Create a copy and ensure all required columns exist
    processed_df = df.copy()

    for col in required_columns:
        if col not in processed_df.columns:
            processed_df[col] = None

    # Rename columns to lowercase schema names
    processed_df = processed_df.rename(columns=COLUMN_MAPPING)
    processed_df["game_id"] = game_id
    processed_df = processed_df.drop_duplicates(subset=["game_id", "action_number"])

    # Ensure all output columns exist
    for col in PLAY_BY_PLAY_COLUMNS:
        if col not in processed_df.columns:
            processed_df[col] = None

    return cast("pd.DataFrame", processed_df[PLAY_BY_PLAY_COLUMNS])


def insert_play_by_play(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Insert processed play-by-play rows from a DataFrame into the play_by_play table.

    Parameters:
        conn (duckdb.DuckDBPyConnection): Active DuckDB connection used for the insert.
        df (pd.DataFrame): DataFrame whose columns match the play_by_play table schema.

    Returns:
        int: Number of rows inserted; returns 0 if `df` is empty or if an error occurs during insertion.
    """
    if df.empty:
        return 0

    try:
        conn.register("temp_pbp", df)

        conn.execute("""
            INSERT OR IGNORE INTO play_by_play (
                game_id, action_number, clock, period, team_id, team_tricode,
                person_id, player_name, player_name_i, x_legacy, y_legacy,
                shot_distance, shot_result, is_field_goal, score_home, score_away,
                points_total, location, description, action_type, sub_type,
                video_available, shot_value, action_id
            )
            SELECT * FROM temp_pbp
        """)

        conn.unregister("temp_pbp")
        return len(df)

    except Exception as e:
        logger.exception(f"Insert error: {e}")
        return 0


# =============================================================================
# MAIN POPULATION FUNCTION
# =============================================================================


def populate_play_by_play(
    db_path: str | None = None,
    games: list[str] | None = None,
    seasons: list[str] | None = None,
    limit: int | None = None,
    delay: float = 0.6,
    resume_from: str | None = None,
    log_every: int = 10,
    client: NBAClient | None = None,
) -> dict[str, Any]:
    """Populate the play_by_play table in the DuckDB database with play-by-play data fetched from the NBA API.

    Parameters:
        db_path (Optional[str]): Path to the DuckDB database file; uses the configured default when omitted.
        games (Optional[List[str]]): Explicit list of game IDs to process; when provided, overrides reading game IDs from the database.
        seasons (Optional[List[str]]): List of seasons to filter game IDs when querying the database (e.g., "2022-23"); used only when `games` is not provided.
        limit (Optional[int]): Maximum number of games to process from the resolved game list.
        delay (float): Per-request delay in seconds applied to the NBAClient to avoid rate limits.
        resume_from (Optional[str]): If provided, processing will start from this game ID within the remaining (uncompleted) games.
        client (Optional[NBAClient]): NBAClient instance to use for API calls; a default client is created if not supplied.

    Returns:
        dict: Statistics and metadata about the run, including at least:
            - start_time: ISO timestamp when processing started.
            - end_time: ISO timestamp when processing ended.
            - games_processed: Number of games successfully processed.
            - events_added: Total number of play-by-play events inserted.
            - final_count: Final row count of the play_by_play table (best-effort).
            - net_added: final_count minus initial table count (best-effort).
            - errors: List of error messages encountered during processing.
    """
    db_path = db_path or str(get_db_path())
    client = client or get_client()

    # Update client delay
    client.config.request_delay = delay

    log_every = max(1, int(log_every))

    logger.info("=" * 70)
    logger.info("NBA PLAY-BY-PLAY POPULATION SCRIPT")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Request Delay: {delay}s")

    # Connect to database
    logger.info("Connecting to database...")
    conn = duckdb.connect(db_path)

    ensure_play_by_play_schema(conn)

    # Get initial count
    try:
        initial_count = conn.execute("SELECT COUNT(*) FROM play_by_play").fetchone()[0]
    except Exception:
        initial_count = 0
    logger.info(f"Initial play_by_play count: {initial_count}")

    # Get games to process
    if games:
        games_to_process = games
    else:
        candidates = []
        for table_name in (
            "games",
            "game_gold",
            "game_silver",
            "game_raw",
            "game",
        ):
            try:
                row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                if row:
                    candidates.append((row[0], table_name))
            except Exception:
                logger.exception(
                    "Failed to inspect table %s for play-by-play candidates",
                    table_name,
                )
                continue

        if not candidates:
            logger.warning("No game tables found to source game IDs.")
            games_to_process = []
        else:
            base_table = max(candidates, key=lambda item: item[0])[1]
            logger.info("Using %s as game source table", base_table)
            query = f"SELECT DISTINCT game_id FROM {base_table}"
            if seasons:
                years = [season.split("-")[0] for season in seasons]
                cols = [
                    col[1]
                    for col in conn.execute(
                        f"PRAGMA table_info('{base_table}')"
                    ).fetchall()
                ]
                if "season_id" in cols:
                    year_list = ", ".join([f"'{year}'" for year in years])
                    query = f"""
                        SELECT DISTINCT game_id
                        FROM {base_table}
                        WHERE RIGHT(CAST(season_id AS VARCHAR), 4) IN ({year_list})
                        ORDER BY game_id
                    """
                elif "game_date" in cols:
                    year_list = ", ".join([f"'{year}'" for year in years])
                    query = f"""
                        SELECT DISTINCT game_id
                        FROM {base_table}
                        WHERE CAST(EXTRACT(year FROM game_date) AS VARCHAR) IN ({year_list})
                        ORDER BY game_id
                    """
                else:
                    query = (
                        f"SELECT DISTINCT game_id FROM {base_table} ORDER BY game_id"
                    )

            try:
                result = conn.execute(query).fetchall()
                games_to_process = [row[0] for row in result]
            except Exception as e:
                logger.warning(f"Could not query {base_table} table: {e}")
                games_to_process = []

    if limit:
        games_to_process = games_to_process[:limit]

    logger.info(f"Games to process: {len(games_to_process)}")

    # Load progress
    progress = load_progress()
    completed_games = set(progress.get("completed_games", []))
    no_data_games = set(progress.get("no_data_games", []))

    # Check already populated games in the database (guard against missing progress)
    try:
        existing_rows = conn.execute(
            "SELECT DISTINCT game_id FROM play_by_play"
        ).fetchall()
        existing_games = {row[0] for row in existing_rows}
    except Exception:
        existing_games = set()

    # Only trust completed games if they exist in the database
    completed_games &= existing_games

    # Filter out already completed games
    games_set = set(games_to_process)
    completed_games &= games_set
    no_data_games &= games_set
    existing_games &= games_set
    remaining_games = [
        g
        for g in games_to_process
        if g not in completed_games
        and g not in no_data_games
        and g not in existing_games
    ]

    logger.info(
        "Skipped games: completed=%s no_data=%s existing=%s",
        len(completed_games),
        len(no_data_games),
        len(existing_games),
    )

    if resume_from:
        # Resume from specific game
        try:
            resume_index = list(remaining_games).index(resume_from)
            remaining_games = remaining_games[resume_index:]
        except ValueError:
            logger.warning(f"Game {resume_from} not found in remaining games")

    logger.info(f"Remaining games to process: {len(remaining_games)}")

    if not remaining_games:
        logger.info("No games to process!")
        return {"games_processed": 0, "events_added": 0, "errors": []}

    # Statistics
    stats: dict[str, Any] = {
        "start_time": datetime.now().isoformat(),
        "games_processed": 0,
        "games_no_data": 0,
        "events_added": 0,
        "errors": [],
    }

    logger.info("=" * 70)
    logger.info("STARTING POPULATION")
    logger.info("=" * 70)

    start_time = time.monotonic()
    total_games = 0

    def log_progress(processed: int, total: int, force: bool = False) -> None:
        if not force and processed % log_every != 0:
            return
        elapsed = time.monotonic() - start_time
        avg = elapsed / processed if processed else 0
        remaining = (total - processed) * avg
        pct = (processed / total) * 100 if total else 0.0
        logger.info(
            "Progress: %s/%s (%.1f%%) | games_with_data=%s no_data=%s errors=%s | elapsed=%s eta=%s",
            processed,
            total,
            pct,
            stats["games_processed"],
            stats["games_no_data"],
            len(stats["errors"]),
            _format_duration(elapsed),
            _format_duration(remaining),
        )

    try:
        total_games = len(remaining_games)
        for i, game_id in enumerate(remaining_games, 1):
            try:
                game_id_str = str(game_id)
                if game_id_str.isdigit():
                    game_id_str = game_id_str.zfill(10)
                logger.info(
                    "[%s/%s] Processing game %s",
                    i,
                    len(remaining_games),
                    game_id_str,
                )

                # Fetch play-by-play data using API client
                df = client.get_play_by_play(game_id_str)

                if df is None or df.empty:
                    logger.info(f"      No data for game {game_id}")
                    no_data_games.add(game_id)
                    stats["games_no_data"] += 1
                    progress["no_data_games"] = list(no_data_games)
                    progress["last_game_id"] = game_id
                    log_progress(i, total_games)
                    continue

                # Process the data
                game_id_int = int(game_id_str) if game_id_str.isdigit() else None
                processed_df = process_play_by_play_data(df, game_id_int)

                if processed_df.empty:
                    logger.info(
                        f"      No valid data after processing for game {game_id}",
                    )
                    continue

                # Insert into database
                events_added = insert_play_by_play(conn, processed_df)

                if events_added == 0:
                    warn_msg = f"No events inserted for game {game_id}"
                    logger.warning(f"      {warn_msg}")
                    stats["errors"].append(warn_msg)
                    if "errors" not in progress:
                        progress["errors"] = []
                    progress["errors"].append(
                        {
                            "game_id": game_id,
                            "error": warn_msg,
                            "timestamp": datetime.now().isoformat(),
                        },
                    )
                    log_progress(i, total_games)
                    continue

                stats["events_added"] += events_added
                stats["games_processed"] += 1

                logger.info(f"      Added {events_added} events")

                # Update progress
                completed_games.add(game_id)
                progress["completed_games"] = list(completed_games)
                progress["no_data_games"] = list(no_data_games)
                progress["last_game_id"] = game_id

                # Save progress periodically
                if i % 10 == 0:
                    save_progress(progress)
                    conn.commit()
                    logger.info(f"  [Progress: {i}/{len(remaining_games)} games]")
                log_progress(i, total_games)

            except Exception as e:
                error_msg = f"Error processing game {game_id}: {e!s}"
                logger.exception(f"      ERROR: {error_msg}")
                stats["errors"].append(error_msg)
                log_progress(i, total_games)

                # Save error to progress
                if "errors" not in progress:
                    progress["errors"] = []
                progress["errors"].append(
                    {
                        "game_id": game_id,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                    },
                )
                continue

    except KeyboardInterrupt:
        logger.info("*** INTERRUPTED BY USER ***")
        logger.info(
            f"Progress saved. Resume with: --resume-from {progress.get('last_game_id')}",
        )

    except Exception as e:
        logger.exception(f"*** ERROR: {e} ***")
        traceback.print_exc()
        stats["errors"].append(str(e))

    finally:
        # Final save
        save_progress(progress)
        conn.commit()

        # Get final count
        try:
            final_count = conn.execute("SELECT COUNT(*) FROM play_by_play").fetchone()[
                0
            ]
        except Exception:
            final_count = initial_count + stats["events_added"]

        conn.close()

    # Update stats
    log_progress(total_games, total_games, force=True)

    stats["end_time"] = datetime.now().isoformat()
    stats["final_count"] = final_count
    stats["net_added"] = final_count - initial_count

    # Print summary
    logger.info("=" * 70)
    logger.info("PLAY-BY-PLAY POPULATION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Games processed: {stats['games_processed']}")
    logger.info(f"Games with no data: {stats['games_no_data']}")
    logger.info(f"Total events added: {stats['events_added']}")
    logger.info(f"Final count: {final_count}")
    logger.info(f"Net rows added: {stats['net_added']}")

    if stats["errors"]:
        logger.info(f"Errors encountered: {len(stats['errors'])}")
        for error in stats["errors"][:3]:  # Show first 3 errors
            logger.info(f"  - {error}")

    return stats


# =============================================================================
# CLI
# =============================================================================


def main() -> None:
    """Entry point for the play-by-play population CLI.

    Parses command-line arguments for database path, game IDs, seasons, processing limit, API call delay, and resume point; invokes populate_play_by_play with those options and exits with a non-zero status when the operation is cancelled, returns errors, or a fatal exception occurs.
    """
    parser = argparse.ArgumentParser(
        description="Populate play-by-play data from NBA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Populate play-by-play for recent games
  python scripts/populate/populate_play_by_play.py --limit 10 --seasons 2022-23

  # Specific games
  python scripts/populate/populate_play_by_play.py --games 0022200001 0022200002

  # With custom delay for rate limiting
  python scripts/populate/populate_play_by_play.py --delay 1.0 --limit 5
        """,
    )

    parser.add_argument("--db", default=None, help="Database path")
    parser.add_argument("--games", nargs="+", help="Specific game IDs to process")
    parser.add_argument(
        "--seasons",
        nargs="+",
        help="Seasons to process (e.g., 2022-23)",
    )
    parser.add_argument("--limit", type=int, help="Limit number of games to process")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.6,
        help="Delay between API calls",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=10,
        help="Log progress every N games (default: 10)",
    )
    parser.add_argument("--resume-from", help="Resume from specific game ID")

    args = parser.parse_args()

    try:
        result = populate_play_by_play(
            db_path=args.db,
            games=args.games,
            seasons=args.seasons,
            limit=args.limit,
            delay=args.delay,
            resume_from=args.resume_from,
            log_every=args.log_every,
        )

        if result["errors"]:
            logger.info(f"Completed with {len(result['errors'])} errors")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
