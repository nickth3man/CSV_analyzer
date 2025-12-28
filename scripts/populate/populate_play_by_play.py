#!/usr/bin/env python3
"""Populate play_by_play table from NBA API.

This script fetches play-by-play data for games and populates the play_by_play table.
Play-by-play data includes every event that happens during a game (shots, rebounds, turnovers, etc.)

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

Based on nba_api documentation:
- reference/nba_api/src/nba_api/stats/endpoints/playbyplayv2.py
- reference/nba_api/src/nba_api/stats/endpoints/playbyplayv3.py
"""

import argparse
import json
import logging
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

import duckdb
import pandas as pd

# Import shared modules from the populate package
from scripts.populate.config import (
    CACHE_DIR,
    ensure_cache_dir,
    get_db_path,
    PLAY_BY_PLAY_FIELDS,
)
from scripts.populate.api_client import NBAClient, get_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

PROGRESS_FILE = CACHE_DIR / "play_by_play_progress.json"

# Column mapping from NBA API (v2) to our schema
COLUMN_MAPPING = {
    'GAME_ID': 'game_id',
    'EVENTNUM': 'event_num',
    'EVENTMSGTYPE': 'event_msg_type',
    'EVENTMSGACTIONTYPE': 'event_msg_action_type',
    'PERIOD': 'period',
    'WCTIMESTRING': 'wctimestring',
    'PCTIMESTRING': 'pctimestring',
    'HOMEDESCRIPTION': 'home_description',
    'NEUTRALDESCRIPTION': 'neutral_description',
    'VISITORDESCRIPTION': 'visitor_description',
    'SCORE': 'score',
    'SCOREMARGIN': 'score_margin',
    'PERSON1TYPE': 'person1type',
    'PLAYER1_ID': 'player1_id',
    'PLAYER1_NAME': 'player1_name',
    'PLAYER1_TEAM_ID': 'player1_team_id',
    'PERSON2TYPE': 'person2type',
    'PLAYER2_ID': 'player2_id',
    'PLAYER2_NAME': 'player2_name',
    'PLAYER2_TEAM_ID': 'player2_team_id',
    'PERSON3TYPE': 'person3type',
    'PLAYER3_ID': 'player3_id',
    'PLAYER3_NAME': 'player3_name',
    'PLAYER3_TEAM_ID': 'player3_team_id',
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def load_progress() -> Dict[str, Any]:
    """Load progress from file."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"completed_games": [], "last_game_id": None, "errors": []}


def save_progress(progress: Dict[str, Any]):
    """Save progress to file."""
    ensure_cache_dir()
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def process_play_by_play_data(df: pd.DataFrame, game_id: str) -> pd.DataFrame:
    """Process play-by-play data for insertion into database.

    Args:
        df: Raw DataFrame from NBA API
        game_id: Game ID for this data

    Returns:
        Processed DataFrame matching play_by_play table schema
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
    processed_df['game_id'] = game_id

    # Select only the columns we need in the right order
    output_columns = ['game_id'] + [COLUMN_MAPPING[col] for col in required_columns if col != 'GAME_ID']

    # Ensure all output columns exist
    for col in output_columns:
        if col not in processed_df.columns:
            processed_df[col] = None

    return processed_df[output_columns]


def insert_play_by_play(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Insert play-by-play data into database.

    Args:
        conn: DuckDB connection
        df: DataFrame to insert

    Returns:
        Number of rows inserted
    """
    if df.empty:
        return 0

    try:
        conn.register("temp_pbp", df)

        conn.execute("""
            INSERT INTO play_by_play (
                game_id, event_num, event_msg_type, event_msg_action_type,
                period, wctimestring, pctimestring, home_description,
                neutral_description, visitor_description, score, score_margin,
                person1type, player1_id, player1_name, player1_team_id,
                person2type, player2_id, player2_name, player2_team_id,
                person3type, player3_id, player3_name, player3_team_id
            )
            SELECT * FROM temp_pbp
        """)

        conn.unregister("temp_pbp")
        return len(df)

    except Exception as e:
        logger.error(f"Insert error: {e}")
        return 0


# =============================================================================
# MAIN POPULATION FUNCTION
# =============================================================================

def populate_play_by_play(
    db_path: Optional[str] = None,
    games: Optional[List[str]] = None,
    seasons: Optional[List[str]] = None,
    limit: Optional[int] = None,
    delay: float = 0.6,
    resume_from: Optional[str] = None,
    client: Optional[NBAClient] = None,
) -> Dict[str, Any]:
    """Populate play_by_play table with NBA data.

    Args:
        db_path: Path to DuckDB database
        games: Optional list of specific game IDs to process
        seasons: Optional list of seasons to process
        limit: Maximum number of games to process
        delay: Delay between API requests in seconds
        resume_from: Resume from specific game ID
        client: NBAClient instance (creates default if None)

    Returns:
        Dictionary with statistics about the population process
    """
    db_path = db_path or str(get_db_path())
    client = client or get_client()

    # Update client delay
    client.config.request_delay = delay

    logger.info("=" * 70)
    logger.info("NBA PLAY-BY-PLAY POPULATION SCRIPT")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Request Delay: {delay}s")

    # Connect to database
    logger.info("Connecting to database...")
    conn = duckdb.connect(db_path)

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
        # Get games from game_gold table
        query = "SELECT DISTINCT game_id FROM game_gold ORDER BY game_id"
        if seasons:
            # Filter by season - need to extract season from game_id
            season_conditions = " OR ".join([
                f"game_id LIKE '{season.replace('-', '')}%'" for season in seasons
            ])
            query = f"""
                SELECT DISTINCT game_id FROM game_gold
                WHERE {season_conditions}
                ORDER BY game_id
            """

        try:
            result = conn.execute(query).fetchall()
            games_to_process = [row[0] for row in result]
        except Exception as e:
            logger.warning(f"Could not query game_gold table: {e}")
            games_to_process = []

    if limit:
        games_to_process = games_to_process[:limit]

    logger.info(f"Games to process: {len(games_to_process)}")

    # Load progress
    progress = load_progress()
    completed_games = set(progress.get("completed_games", []))

    # Filter out already completed games
    remaining_games = [g for g in games_to_process if g not in completed_games]

    if resume_from:
        # Resume from specific game
        try:
            resume_index = [g for g in remaining_games].index(resume_from)
            remaining_games = remaining_games[resume_index:]
        except ValueError:
            logger.warning(f"Game {resume_from} not found in remaining games")

    logger.info(f"Remaining games to process: {len(remaining_games)}")

    if not remaining_games:
        logger.info("No games to process!")
        return {"games_processed": 0, "events_added": 0, "errors": []}

    # Statistics
    stats = {
        "start_time": datetime.now().isoformat(),
        "games_processed": 0,
        "events_added": 0,
        "errors": []
    }

    logger.info("=" * 70)
    logger.info("STARTING POPULATION")
    logger.info("=" * 70)

    try:
        for i, game_id in enumerate(remaining_games, 1):
            try:
                logger.info(f"[{i}/{len(remaining_games)}] Processing game {game_id}")

                # Fetch play-by-play data using API client
                df = client.get_play_by_play(game_id)

                if df is None or df.empty:
                    logger.info(f"      No data for game {game_id}")
                    continue

                # Process the data
                processed_df = process_play_by_play_data(df, game_id)

                if processed_df.empty:
                    logger.info(f"      No valid data after processing for game {game_id}")
                    continue

                # Insert into database
                events_added = insert_play_by_play(conn, processed_df)

                stats["events_added"] += events_added
                stats["games_processed"] += 1

                logger.info(f"      Added {events_added} events")

                # Update progress
                completed_games.add(game_id)
                progress["completed_games"] = list(completed_games)
                progress["last_game_id"] = game_id

                # Save progress periodically
                if i % 10 == 0:
                    save_progress(progress)
                    conn.commit()
                    logger.info(f"  [Progress: {i}/{len(remaining_games)} games]")

            except Exception as e:
                error_msg = f"Error processing game {game_id}: {str(e)}"
                logger.error(f"      ERROR: {error_msg}")
                stats["errors"].append(error_msg)

                # Save error to progress
                if "errors" not in progress:
                    progress["errors"] = []
                progress["errors"].append({
                    "game_id": game_id,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                continue

    except KeyboardInterrupt:
        logger.info("*** INTERRUPTED BY USER ***")
        logger.info(f"Progress saved. Resume with: --resume-from {progress.get('last_game_id')}")

    except Exception as e:
        logger.error(f"*** ERROR: {e} ***")
        traceback.print_exc()
        stats["errors"].append(str(e))

    finally:
        # Final save
        save_progress(progress)
        conn.commit()

        # Get final count
        try:
            final_count = conn.execute("SELECT COUNT(*) FROM play_by_play").fetchone()[0]
        except Exception:
            final_count = initial_count + stats["events_added"]

        conn.close()

    # Update stats
    stats["end_time"] = datetime.now().isoformat()
    stats["final_count"] = final_count
    stats["net_added"] = final_count - initial_count

    # Print summary
    logger.info("=" * 70)
    logger.info("PLAY-BY-PLAY POPULATION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Games processed: {stats['games_processed']}")
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

def main():
    parser = argparse.ArgumentParser(
        description='Populate play-by-play data from NBA API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Populate play-by-play for recent games
  python scripts/populate/populate_play_by_play.py --limit 10 --seasons 2022-23

  # Specific games
  python scripts/populate/populate_play_by_play.py --games 0022200001 0022200002

  # With custom delay for rate limiting
  python scripts/populate/populate_play_by_play.py --delay 1.0 --limit 5
        """
    )

    parser.add_argument('--db', default=None, help='Database path')
    parser.add_argument('--games', nargs='+', help='Specific game IDs to process')
    parser.add_argument('--seasons', nargs='+', help='Seasons to process (e.g., 2022-23)')
    parser.add_argument('--limit', type=int, help='Limit number of games to process')
    parser.add_argument('--delay', type=float, default=0.6, help='Delay between API calls')
    parser.add_argument('--resume-from', help='Resume from specific game ID')

    args = parser.parse_args()

    try:
        result = populate_play_by_play(
            db_path=args.db,
            games=args.games,
            seasons=args.seasons,
            limit=args.limit,
            delay=args.delay,
            resume_from=args.resume_from
        )

        if result["errors"]:
            logger.info(f"Completed with {len(result['errors'])} errors")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
