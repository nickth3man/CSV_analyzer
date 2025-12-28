#!/usr/bin/env python3
"""Populate play_by_play table from NBA API.

This script fetches play-by-play data for games and populates the play_by_play table.
Play-by-play data includes every event that happens during a game (shots, rebounds, turnovers, etc.)

Features:
- Fetches play-by-play data for specific games or seasons
- Respects NBA API rate limits
- Handles different event types and action types
- Supports incremental updates
- Error handling with retry logic

Usage:
    # Populate play-by-play for recent games
    python scripts/populate_play_by_play.py --limit 10 --seasons 2022-23
    
    # Specific games
    python scripts/populate_play_by_play.py --games 0022200001 0022200002
    
    # With custom delay for rate limiting
    python scripts/populate_play_by_play.py --delay 1.0 --limit 5
"""

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

import duckdb
import pandas as pd

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from nba_api.stats.endpoints import playbyplayv2
    from nba_api.stats.static import teams
except ImportError:
    print("Error: nba_api not installed. Please install with: pip install nba_api")
    sys.exit(1)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Cache directory
CACHE_DIR = Path(".nba_cache")
PROGRESS_FILE = CACHE_DIR / "play_by_play_progress.json"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def ensure_cache_dir():
    """Create cache directory if it doesn't exist."""
    CACHE_DIR.mkdir(exist_ok=True)


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


def get_team_mapping() -> Dict[int, str]:
    """Get mapping of team IDs to abbreviations."""
    all_teams = teams.get_teams()
    return {team['id']: team['abbreviation'] for team in all_teams}


def fetch_play_by_play(
    game_id: str, 
    delay: float = 0.6,
    retries: int = 3
) -> Optional[pd.DataFrame]:
    """Fetch play-by-play data for a specific game."""
    
    for attempt in range(retries):
        try:
            time.sleep(delay)  # Rate limiting
            
            pbp = playbyplayv2.PlayByPlayV2(game_id=game_id)
            df = pbp.get_data_frames()[0]
            
            if df.empty:
                return None
                
            return df
            
        except Exception as e:
            error_str = str(e).lower()
            
            # Check for rate limiting
            if "rate" in error_str or "429" in error_str or "timeout" in error_str:
                wait_time = delay * (2 ** attempt)  # Exponential backoff
                print(f"      Rate limited, waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            
            # Check for game not found (normal for some games)
            if "game" in error_str and ("not found" in error_str or "invalid" in error_str):
                print(f"      Game {game_id} not found or no data available")
                return None
            
            # Other errors
            if attempt < retries - 1:
                print(f"      Error on attempt {attempt + 1}: {e}")
                continue
            else:
                raise
    
    return None


def process_play_by_play_data(df: pd.DataFrame, game_id: str) -> pd.DataFrame:
    """Process play-by-play data for insertion into database."""
    
    if df.empty:
        return df
    
    # Ensure required columns exist, fill missing ones
    required_columns = [
        'GAME_ID', 'EVENTNUM', 'EVENTMSGTYPE', 'EVENTMSGACTIONTYPE', 
        'PERIOD', 'WCTIMESTRING', 'PCTIMESTRING', 'HOMEDESCRIPTION',
        'NEUTRALDESCRIPTION', 'VISITORDESCRIPTION', 'SCORE', 'SCOREMARGIN',
        'PERSON1TYPE', 'PLAYER1_ID', 'PLAYER1_NAME', 'PLAYER1_TEAM_ID',
        'PERSON2TYPE', 'PLAYER2_ID', 'PLAYER2_NAME', 'PLAYER2_TEAM_ID',
        'PERSON3TYPE', 'PLAYER3_ID', 'PLAYER3_NAME', 'PLAYER3_TEAM_ID'
    ]
    
    # Create a copy and ensure all required columns exist
    processed_df = df.copy()
    
    for col in required_columns:
        if col not in processed_df.columns:
            processed_df[col] = None
    
    # Rename columns to lowercase and add game_id
    column_mapping = {col: col.lower() for col in required_columns}
    processed_df = processed_df.rename(columns=column_mapping)
    processed_df['game_id'] = game_id
    
    # Select only the columns we need
    return processed_df[['game_id'] + [col.lower() for col in required_columns]]


def populate_play_by_play(
    db_path: str = "data/nba.duckdb",
    games: List[str] = None,
    seasons: List[str] = None,
    limit: Optional[int] = None,
    delay: float = 0.6,
    resume_from: Optional[str] = None
) -> Dict[str, Any]:
    """Populate play_by_play table with NBA data."""
    
    print(f"Connecting to database: {db_path}")
    conn = duckdb.connect(db_path)
    
    # Get initial count
    initial_count = conn.execute("SELECT COUNT(*) FROM play_by_play").fetchone()[0]
    print(f"Initial play_by_play count: {initial_count}")
    
    # Get games to process
    if games:
        games_to_process = games
    else:
        # Get games from game_gold table
        query = "SELECT DISTINCT game_id FROM game_gold ORDER BY game_id"
        if seasons:
            # Filter by season - need to extract season from game_id
            season_conditions = " OR ".join([
                f"game_id LIKE '{season.replace('-', '')}%" for season in seasons
            ])
            query = f"""
                SELECT DISTINCT game_id FROM game_gold 
                WHERE {season_conditions}
                ORDER BY game_id
            """
        
        result = conn.execute(query).fetchall()
        games_to_process = [row[0] for row in result]
    
    if limit:
        games_to_process = games_to_process[:limit]
    
    print(f"Games to process: {len(games_to_process)}")
    
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
            print(f"Game {resume_from} not found in remaining games")
    
    print(f"Remaining games to process: {len(remaining_games)}")
    
    if not remaining_games:
        print("No games to process!")
        return {"games_processed": 0, "events_added": 0, "errors": []}
    
    # Process games
    total_events = 0
    errors = []
    games_processed = 0
    
    team_mapping = get_team_mapping()
    
    for i, game_id in enumerate(remaining_games, 1):
        try:
            print(f"[{i}/{len(remaining_games)}] Processing game {game_id}")
            
            # Fetch play-by-play data
            df = fetch_play_by_play(game_id, delay=delay)
            
            if df is None or df.empty:
                print(f"      No data for game {game_id}")
                continue
            
            # Process the data
            processed_df = process_play_by_play_data(df, game_id)
            
            if processed_df.empty:
                print(f"      No valid data after processing for game {game_id}")
                continue
            
            # Insert into database
            conn.register("temp_pbp", processed_df)
            
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
            
            events_in_game = len(processed_df)
            total_events += events_in_game
            games_processed += 1
            
            print(f"      Added {events_in_game} events")
            
            # Update progress
            completed_games.add(game_id)
            progress["completed_games"] = list(completed_games)
            progress["last_game_id"] = game_id
            save_progress(progress)
            
        except Exception as e:
            error_msg = f"Error processing game {game_id}: {str(e)}"
            print(f"      ERROR: {error_msg}")
            errors.append(error_msg)
            
            # Save error to progress
            if "errors" not in progress:
                progress["errors"] = []
            progress["errors"].append({
                "game_id": game_id,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
            save_progress(progress)
            continue
    
    # Final count
    final_count = conn.execute("SELECT COUNT(*) FROM play_by_play").fetchone()[0]
    net_added = final_count - initial_count
    
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"PLAY-BY-PLAY POPULATION COMPLETE")
    print(f"{'='*60}")
    print(f"Games processed: {games_processed}")
    print(f"Total events added: {total_events}")
    print(f"Final count: {final_count}")
    print(f"Net rows added: {net_added}")
    
    if errors:
        print(f"Errors encountered: {len(errors)}")
        for error in errors[:3]:  # Show first 3 errors
            print(f"  - {error}")
    
    return {
        "games_processed": games_processed,
        "events_added": total_events,
        "final_count": final_count,
        "net_added": net_added,
        "errors": errors
    }


def main():
    parser = argparse.ArgumentParser(description='Populate play-by-play data')
    parser.add_argument('--db', default='data/nba.duckdb', help='Database path')
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
            print(f"\nCompleted with {len(result['errors'])} errors")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()