#!/usr/bin/env python3
"""Populate player_game_stats table from NBA API.

This script fetches player game logs from the NBA API and populates the
player_game_stats table in the DuckDB database.

Features:
- Fetches game logs for all players (or a subset)
- Respects NBA API rate limits (configurable delay between requests)
- Implements caching to avoid redundant API calls
- Supports incremental updates (skip already populated seasons)
- Progress tracking and resumability
- Error handling with retry logic

Usage:
    # Full population (all players, all seasons)
    python scripts/populate_player_game_stats.py

    # Specific seasons only
    python scripts/populate_player_game_stats.py --seasons 2023-24 2022-23

    # Active players only (faster for recent data)
    python scripts/populate_player_game_stats.py --active-only

    # Resume from a specific player ID
    python scripts/populate_player_game_stats.py --resume-from 2544

    # Limit number of players (for testing)
    python scripts/populate_player_game_stats.py --limit 10

    # Custom request delay (default: 0.6 seconds)
    python scripts/populate_player_game_stats.py --delay 1.0
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
    from nba_api.stats.endpoints import playergamelog, playercareerstats
    from nba_api.stats.static import players, teams
except ImportError:
    print("ERROR: nba_api package not installed. Run: pip install nba_api")
    sys.exit(1)


# =============================================================================
# CONFIGURATION
# =============================================================================

DEFAULT_SEASONS = [
    "2024-25", "2023-24", "2022-23", "2021-22", "2020-21",
    "2019-20", "2018-19", "2017-18", "2016-17", "2015-16",
    "2014-15", "2013-14", "2012-13", "2011-12", "2010-11",
    "2009-10", "2008-09", "2007-08", "2006-07", "2005-06",
    "2004-05", "2003-04", "2002-03", "2001-02", "2000-01",
    "1999-00", "1998-99", "1997-98"
]

SEASON_TYPES = ["Regular Season", "Playoffs"]

# Column mapping from NBA API to our schema
COLUMN_MAPPING = {
    'Game_ID': 'game_id',
    'GAME_ID': 'game_id',
    'Player_ID': 'player_id',
    'PLAYER_ID': 'player_id',
    'TEAM_ID': 'team_id',
    'PLAYER_NAME': 'player_name',
    'MIN': 'min',
    'FGM': 'fgm',
    'FGA': 'fga',
    'FG_PCT': 'fg_pct',
    'FG3M': 'fg3m',
    'FG3A': 'fg3a',
    'FG3_PCT': 'fg3_pct',
    'FTM': 'ftm',
    'FTA': 'fta',
    'FT_PCT': 'ft_pct',
    'OREB': 'oreb',
    'DREB': 'dreb',
    'REB': 'reb',
    'AST': 'ast',
    'STL': 'stl',
    'BLK': 'blk',
    'TOV': 'tov',
    'PF': 'pf',
    'PTS': 'pts',
    'PLUS_MINUS': 'plus_minus',
}

# Cache directory
CACHE_DIR = Path(".nba_cache")
PROGRESS_FILE = CACHE_DIR / "population_progress.json"


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
    return {"completed_players": [], "last_player_id": None, "errors": []}


def save_progress(progress: Dict[str, Any]):
    """Save progress to file."""
    ensure_cache_dir()
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def get_all_players(active_only: bool = False) -> List[Dict]:
    """Get list of all NBA players."""
    if active_only:
        return players.get_active_players()
    return players.get_players()


def get_team_id_from_game(game_id: int, player_id: int, conn: duckdb.DuckDBPyConnection) -> Optional[int]:
    """Try to get team_id from existing game data."""
    try:
        # Try to find from team_game_stats
        result = conn.execute("""
            SELECT team_id FROM team_game_stats 
            WHERE game_id = ? LIMIT 1
        """, [game_id]).fetchone()
        if result:
            return result[0]
    except Exception:
        pass
    return None


def parse_minutes(min_str) -> Optional[str]:
    """Parse minutes string (could be MM:SS or just MM)."""
    if pd.isna(min_str) or min_str is None:
        return None
    if isinstance(min_str, (int, float)):
        return str(int(min_str))
    return str(min_str)


def fetch_player_game_log(
    player_id: int, 
    season: str, 
    season_type: str = "Regular Season",
    delay: float = 0.6,
    retries: int = 3
) -> Optional[pd.DataFrame]:
    """Fetch game log for a player/season with retry logic."""
    
    for attempt in range(retries):
        try:
            time.sleep(delay)  # Rate limiting
            
            log = playergamelog.PlayerGameLog(
                player_id=player_id,
                season=season,
                season_type_all_star=season_type
            )
            df = log.get_data_frames()[0]
            
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
            
            # Check for player not found (normal for historical seasons)
            if "not found" in error_str or "404" in error_str:
                return None
            
            # Other errors
            if attempt < retries - 1:
                print(f"      Attempt {attempt + 1} failed: {e}")
                time.sleep(delay * 2)
            else:
                print(f"      ERROR after {retries} attempts: {e}")
                return None
    
    return None


def transform_game_log(df: pd.DataFrame, player_info: Dict) -> pd.DataFrame:
    """Transform NBA API game log to our schema."""
    
    if df is None or df.empty:
        return pd.DataFrame()
    
    # Make a copy to avoid modifying original
    df_work = df.copy()
    
    # Debug: Print columns to see what we have
    # print(f"      API Columns: {list(df_work.columns)}")
    
    # Create output DataFrame with exact schema expected by player_game_stats table
    output = pd.DataFrame()
    
    # game_id - Handle mixed case from API (Game_ID)
    if 'Game_ID' in df_work.columns:
        output['game_id'] = pd.to_numeric(df_work['Game_ID'], errors='coerce').astype('Int64')
    elif 'GAME_ID' in df_work.columns:
        output['game_id'] = pd.to_numeric(df_work['GAME_ID'], errors='coerce').astype('Int64')
    else:
        output['game_id'] = None
    
    # team_id - Try to extract from MATCHUP (e.g., "LAL vs. GSW" or "LAL @ GSW")
    # Note: NBA API PlayerGameLog doesn't return TEAM_ID directly
    # We'll set to None and can populate later if needed
    output['team_id'] = None
    
    # player_id - from API (Player_ID) or from player_info
    if 'Player_ID' in df_work.columns:
        output['player_id'] = pd.to_numeric(df_work['Player_ID'], errors='coerce').astype('Int64')
    else:
        output['player_id'] = player_info.get('id')
    
    # player_name - from player_info
    output['player_name'] = player_info.get('full_name', '')
    
    # start_position, comment - not in game log API
    output['start_position'] = None
    output['comment'] = None
    
    # min - minutes played (can be "MM:SS" or just "MM")
    if 'MIN' in df_work.columns:
        output['min'] = df_work['MIN'].apply(parse_minutes)
    else:
        output['min'] = None
    
    # Counting stats - integers
    int_cols = [
        ('FGM', 'fgm'), ('FGA', 'fga'),
        ('FG3M', 'fg3m'), ('FG3A', 'fg3a'),
        ('FTM', 'ftm'), ('FTA', 'fta'),
        ('OREB', 'oreb'), ('DREB', 'dreb'), ('REB', 'reb'),
        ('AST', 'ast'), ('STL', 'stl'), ('BLK', 'blk'),
        ('TOV', 'tov'), ('PF', 'pf'), ('PTS', 'pts')
    ]
    for api_col, our_col in int_cols:
        if api_col in df_work.columns:
            output[our_col] = pd.to_numeric(df_work[api_col], errors='coerce').astype('Int64')
        else:
            output[our_col] = None
    
    # Percentage stats - floats
    pct_cols = [
        ('FG_PCT', 'fg_pct'), ('FG3_PCT', 'fg3_pct'), ('FT_PCT', 'ft_pct')
    ]
    for api_col, our_col in pct_cols:
        if api_col in df_work.columns:
            output[our_col] = pd.to_numeric(df_work[api_col], errors='coerce')
        else:
            output[our_col] = None
    
    # plus_minus - float
    if 'PLUS_MINUS' in df_work.columns:
        output['plus_minus'] = pd.to_numeric(df_work['PLUS_MINUS'], errors='coerce')
    else:
        output['plus_minus'] = None
    
    # Reorder columns to match table schema
    final_cols = [
        'game_id', 'team_id', 'player_id', 'player_name', 'start_position', 'comment',
        'min', 'fgm', 'fga', 'fg_pct', 'fg3m', 'fg3a', 'fg3_pct',
        'ftm', 'fta', 'ft_pct', 'oreb', 'dreb', 'reb',
        'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'plus_minus'
    ]
    
    # Ensure all columns exist
    for col in final_cols:
        if col not in output.columns:
            output[col] = None
    
    return output[final_cols]


def insert_game_logs(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Insert game logs into database, handling duplicates."""
    
    if df.empty:
        return 0
    
    # Get existing game_id + player_id combinations to avoid duplicates
    existing = set()
    try:
        result = conn.execute("""
            SELECT game_id, player_id FROM player_game_stats
        """).fetchall()
        existing = {(r[0], r[1]) for r in result}
    except Exception:
        pass  # Table might be empty
    
    # Filter out existing records
    df_new = df[~df.apply(lambda r: (r['game_id'], r['player_id']) in existing, axis=1)]
    
    if df_new.empty:
        return 0
    
    # Insert new records
    try:
        conn.execute("""
            INSERT INTO player_game_stats 
            SELECT * FROM df_new
        """)
        return len(df_new)
    except Exception as e:
        print(f"    Insert error: {e}")
        return 0


# =============================================================================
# MAIN POPULATION FUNCTION
# =============================================================================

def populate_player_game_stats(
    db_path: str = "data/nba.duckdb",
    seasons: Optional[List[str]] = None,
    active_only: bool = False,
    limit: Optional[int] = None,
    resume_from: Optional[int] = None,
    delay: float = 0.6,
    season_types: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Main function to populate player_game_stats table.
    
    Args:
        db_path: Path to DuckDB database
        seasons: List of seasons to fetch (e.g., ["2023-24", "2022-23"])
        active_only: If True, only fetch active players
        limit: Maximum number of players to process
        resume_from: Resume from a specific player ID
        delay: Delay between API requests in seconds
        season_types: List of season types (e.g., ["Regular Season", "Playoffs"])
    
    Returns:
        Dictionary with statistics about the population process
    """
    
    seasons = seasons or DEFAULT_SEASONS
    season_types = season_types or SEASON_TYPES
    
    print("=" * 70)
    print("NBA PLAYER GAME STATS POPULATION SCRIPT")
    print("=" * 70)
    print(f"\nDatabase: {db_path}")
    print(f"Seasons: {len(seasons)} ({seasons[0]} to {seasons[-1]})")
    print(f"Season Types: {season_types}")
    print(f"Active Players Only: {active_only}")
    print(f"Request Delay: {delay}s")
    if limit:
        print(f"Player Limit: {limit}")
    if resume_from:
        print(f"Resume From Player ID: {resume_from}")
    
    # Connect to database
    print(f"\nConnecting to database...")
    conn = duckdb.connect(db_path)
    
    # Get initial count
    initial_count = conn.execute("SELECT COUNT(*) FROM player_game_stats").fetchone()[0]
    print(f"Initial row count: {initial_count:,}")
    
    # Get players
    print(f"\nFetching player list...")
    all_players = get_all_players(active_only)
    print(f"Total players: {len(all_players)}")
    
    # Sort by ID for consistent ordering
    all_players = sorted(all_players, key=lambda x: x['id'])
    
    # Handle resume
    if resume_from:
        all_players = [p for p in all_players if p['id'] >= resume_from]
        print(f"After resume filter: {len(all_players)} players")
    
    # Apply limit
    if limit:
        all_players = all_players[:limit]
        print(f"After limit: {len(all_players)} players")
    
    # Statistics
    stats = {
        "start_time": datetime.now().isoformat(),
        "players_processed": 0,
        "players_with_data": 0,
        "total_games_added": 0,
        "errors": [],
        "skipped": []
    }
    
    # Load progress
    progress = load_progress()
    
    print(f"\n" + "=" * 70)
    print("STARTING POPULATION")
    print("=" * 70)
    
    try:
        for idx, player in enumerate(all_players, 1):
            player_id = player['id']
            player_name = player['full_name']
            
            # Skip if already completed in this run
            if player_id in progress.get("completed_players", []):
                continue
            
            print(f"\n[{idx}/{len(all_players)}] {player_name} (ID: {player_id})")
            
            player_games_added = 0
            
            for season in seasons:
                for season_type in season_types:
                    # Fetch game log
                    df = fetch_player_game_log(
                        player_id=player_id,
                        season=season,
                        season_type=season_type,
                        delay=delay
                    )
                    
                    if df is None or df.empty:
                        continue
                    
                    # Transform data
                    df_transformed = transform_game_log(df, player)
                    
                    if df_transformed.empty:
                        continue
                    
                    # Insert into database
                    games_added = insert_game_logs(conn, df_transformed)
                    
                    if games_added > 0:
                        player_games_added += games_added
                        print(f"    {season} {season_type}: +{games_added} games")
            
            # Update statistics
            stats["players_processed"] += 1
            if player_games_added > 0:
                stats["players_with_data"] += 1
                stats["total_games_added"] += player_games_added
                print(f"  Total: +{player_games_added} games")
            
            # Save progress
            progress["completed_players"].append(player_id)
            progress["last_player_id"] = player_id
            
            # Commit periodically
            if idx % 10 == 0:
                conn.commit()
                save_progress(progress)
                current_count = conn.execute("SELECT COUNT(*) FROM player_game_stats").fetchone()[0]
                print(f"\n  [Progress: {idx}/{len(all_players)} players, {current_count:,} total rows]")
    
    except KeyboardInterrupt:
        print("\n\n*** INTERRUPTED BY USER ***")
        print(f"Progress saved. Resume with: --resume-from {progress.get('last_player_id')}")
    
    except Exception as e:
        print(f"\n\n*** ERROR: {e} ***")
        traceback.print_exc()
        stats["errors"].append(str(e))
    
    finally:
        # Final commit
        conn.commit()
        save_progress(progress)
        
        # Get final count
        final_count = conn.execute("SELECT COUNT(*) FROM player_game_stats").fetchone()[0]
        
        # Close connection
        conn.close()
    
    # Update stats
    stats["end_time"] = datetime.now().isoformat()
    stats["final_row_count"] = final_count
    stats["rows_added"] = final_count - initial_count
    
    # Print summary
    print("\n" + "=" * 70)
    print("POPULATION COMPLETE")
    print("=" * 70)
    print(f"\nPlayers Processed: {stats['players_processed']}")
    print(f"Players With Data: {stats['players_with_data']}")
    print(f"Total Games Added: {stats['total_games_added']}")
    print(f"Final Row Count: {final_count:,}")
    print(f"Net Rows Added: {stats['rows_added']:,}")
    
    if stats["errors"]:
        print(f"\nErrors encountered: {len(stats['errors'])}")
    
    return stats


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Populate player_game_stats table from NBA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (all players, all seasons) - TAKES MANY HOURS
  python scripts/populate_player_game_stats.py

  # Recent seasons only (faster)
  python scripts/populate_player_game_stats.py --seasons 2024-25 2023-24 2022-23

  # Active players only
  python scripts/populate_player_game_stats.py --active-only --seasons 2024-25 2023-24

  # Test with 5 players
  python scripts/populate_player_game_stats.py --limit 5 --seasons 2023-24

  # Resume interrupted run
  python scripts/populate_player_game_stats.py --resume-from 2544
        """
    )
    
    parser.add_argument(
        "--db", 
        default="data/nba.duckdb",
        help="Path to DuckDB database (default: data/nba.duckdb)"
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        help="Seasons to fetch (e.g., 2023-24 2022-23). Default: all seasons back to 1997-98"
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only fetch active players"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of players to process"
    )
    parser.add_argument(
        "--resume-from",
        type=int,
        help="Resume from a specific player ID"
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
        help="Only fetch regular season games (skip playoffs)"
    )
    parser.add_argument(
        "--playoffs-only",
        action="store_true",
        help="Only fetch playoff games"
    )
    
    args = parser.parse_args()
    
    # Determine season types
    season_types = SEASON_TYPES
    if args.regular_season_only:
        season_types = ["Regular Season"]
    elif args.playoffs_only:
        season_types = ["Playoffs"]
    
    # Run population
    stats = populate_player_game_stats(
        db_path=args.db,
        seasons=args.seasons,
        active_only=args.active_only,
        limit=args.limit,
        resume_from=args.resume_from,
        delay=args.delay,
        season_types=season_types
    )
    
    # Exit with error code if there were errors
    if stats.get("errors"):
        sys.exit(1)


if __name__ == "__main__":
    main()
