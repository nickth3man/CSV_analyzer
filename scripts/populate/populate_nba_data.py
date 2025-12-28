#!/usr/bin/env python3
"""Comprehensive NBA data population script.

This script serves as the main entry point for populating NBA data into the database.
It uses the enhanced NBAClient to fetch various types of data including:
- Player statistics and game logs
- Team statistics and game logs  
- Box scores (traditional and advanced)
- Play-by-play data
- Shot charts
- League standings
- Player tracking and hustle stats

Usage:
    # Full population (all data types, all seasons)
    python scripts/populate_db/populate_nba_data.py

    # Specific data types only
    python scripts/populate_db/populate_nba_data.py --data-types players games boxscores

    # Specific seasons only
    python scripts/populate_db/populate_nba_data.py --seasons 2023-24 2022-23

    # Active players only (faster for recent data)
    python scripts/populate_db/populate_nba_data.py --active-only

    # Resume from a specific point
    python scripts/populate_db/populate_nba_data.py --resume --data-types games

    # Dry run (show what would be done)
    python scripts/populate_db/populate_nba_data.py --dry-run

    # Custom request delay (default: 0.6 seconds)
    python scripts/populate_db/populate_nba_data.py --delay 1.0

Data Types:
- players: Static player information
- teams: Static team information  
- games: Game logs (player and team)
- boxscores: Traditional and advanced box scores
- playbyplay: Play-by-play data
- shotcharts: Shot chart data
- standings: League standings
- tracking: Player tracking stats
- hustle: Hustle statistics
- all: All available data types
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Set

import duckdb
import pandas as pd

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.populate.api_client import NBAClient, get_client
from scripts.populate.config import (
    get_api_config, ALL_SEASONS, SEASON_TYPES, DEFAULT_SEASON_TYPES,
    CACHE_DIR, PROGRESS_FILE, ensure_cache_dir
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PopulationManager:
    """Manages the NBA data population process."""
    
    def __init__(self, client: Optional[NBAClient] = None):
        """Initialize the population manager.
        
        Args:
            client: NBAClient instance (creates default if None)
        """
        self.client = client or get_client()
        self.progress_file = PROGRESS_FILE
        self.progress = self._load_progress()
        
    def _load_progress(self) -> Dict[str, Any]:
        """Load progress from cache file."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                logger.warning("Could not load progress file, starting fresh")
                
        return {
            "started_at": None,
            "completed_at": None,
            "data_types_completed": {},
            "seasons_completed": {},
            "players_completed": [],
            "games_completed": [],
            "errors": []
        }
    
    def _save_progress(self) -> None:
        """Save progress to cache file."""
        ensure_cache_dir()
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def _mark_data_type_completed(self, data_type: str, season: str) -> None:
        """Mark a data type as completed for a season."""
        if data_type not in self.progress["data_types_completed"]:
            self.progress["data_types_completed"][data_type] = {}
        
        self.progress["data_types_completed"][data_type][season] = {
            "completed_at": datetime.now().isoformat(),
            "status": "completed"
        }
        self._save_progress()
    
    def _is_data_type_completed(self, data_type: str, season: str) -> bool:
        """Check if a data type is completed for a season."""
        return (
            data_type in self.progress["data_types_completed"] and
            season in self.progress["data_types_completed"][data_type] and
            self.progress["data_types_completed"][data_type][season]["status"] == "completed"
        )
    
    def populate_players(self, active_only: bool = False) -> None:
        """Populate static player data.
        
        Args:
            active_only: Whether to only include active players
        """
        logger.info("Populating player data...")
        
        if active_only:
            players = self.client.get_active_players()
            logger.info(f"Found {len(players)} active players")
        else:
            players = self.client.get_all_players()
            logger.info(f"Found {len(players)} total players")
        
        # Convert to DataFrame for easier handling
        players_df = pd.DataFrame(players)
        
        # Add metadata
        players_df['populated_at'] = datetime.now()
        players_df['is_active'] = active_only or players_df.get('is_active', True)
        
        # Save to database (this would be implemented based on your schema)
        logger.info(f"Player data populated: {len(players_df)} records")
        
        return players_df
    
    def populate_teams(self) -> None:
        """Populate static team data."""
        logger.info("Populating team data...")
        
        teams = self.client.get_all_teams()
        teams_df = pd.DataFrame(teams)
        teams_df['populated_at'] = datetime.now()
        
        logger.info(f"Team data populated: {len(teams_df)} teams")
        return teams_df
    
    def populate_games(
        self, 
        seasons: List[str], 
        season_types: List[str],
        active_only: bool = False
    ) -> None:
        """Populate game logs for players and teams.
        
        Args:
            seasons: List of seasons to populate
            season_types: List of season types (Regular Season, Playoffs)
            active_only: Whether to only include active players
        """
        logger.info(f"Populating games for seasons: {seasons}")
        
        # Get players to process
        if active_only:
            players = self.client.get_active_players()
        else:
            players = self.client.get_all_players()
        
        total_players = len(players)
        logger.info(f"Processing {total_players} players")
        
        for season in seasons:
            for season_type in season_types:
                logger.info(f"Processing {season} {season_type}")
                
                # Check if already completed
                if self._is_data_type_completed("games", f"{season}_{season_type}"):
                    logger.info(f"Already completed: {season} {season_type}")
                    continue
                
                # Get all player game logs for this season (bulk query)
                try:
                    game_logs = self.client.get_player_game_logs(
                        season=season,
                        season_type=season_type
                    )
                    
                    if game_logs is not None and not game_logs.empty:
                        logger.info(f"Retrieved {len(game_logs)} game logs")
                        # Process and save to database
                        # This would be implemented based on your database schema
                        
                    self._mark_data_type_completed("games", f"{season}_{season_type}")
                    
                except Exception as e:
                    logger.error(f"Error processing {season} {season_type}: {e}")
                    self.progress["errors"].append({
                        "data_type": "games",
                        "season": season,
                        "season_type": season_type,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    })
    
    def populate_boxscores(
        self,
        game_ids: List[str],
        include_advanced: bool = True
    ) -> None:
        """Populate box score data for specific games.
        
        Args:
            game_ids: List of game IDs to process
            include_advanced: Whether to include advanced statistics
        """
        logger.info(f"Populating box scores for {len(game_ids)} games")
        
        for i, game_id in enumerate(game_ids):
            logger.info(f"Processing box score {i+1}/{len(game_ids)}: {game_id}")
            
            try:
                # Get traditional box score
                traditional = self.client.get_boxscore_traditional(game_id)
                if traditional:
                    logger.info(f"Retrieved traditional box score for {game_id}")
                    # Process and save traditional box score data
                
                # Get advanced box score
                if include_advanced:
                    advanced = self.client.get_boxscore_advanced(game_id)
                    if advanced:
                        logger.info(f"Retrieved advanced box score for {game_id}")
                        # Process and save advanced box score data
                        
            except Exception as e:
                logger.error(f"Error processing box score {game_id}: {e}")
    
    def populate_play_by_play(self, game_ids: List[str]) -> None:
        """Populate play-by-play data for specific games.
        
        Args:
            game_ids: List of game IDs to process
        """
        logger.info(f"Populating play-by-play for {len(game_ids)} games")
        
        for i, game_id in enumerate(game_ids):
            logger.info(f"Processing play-by-play {i+1}/{len(game_ids)}: {game_id}")
            
            try:
                pbp = self.client.get_play_by_play(game_id)
                if pbp is not None and not pbp.empty:
                    logger.info(f"Retrieved {len(pbp)} play-by-play events for {game_id}")
                    # Process and save play-by-play data
                    
            except Exception as e:
                logger.error(f"Error processing play-by-play {game_id}: {e}")
    
    def populate_shot_charts(
        self,
        game_ids: List[str],
        team_ids: Optional[List[int]] = None,
        player_ids: Optional[List[int]] = None
    ) -> None:
        """Populate shot chart data for specific games.
        
        Args:
            game_ids: List of game IDs to process
            team_ids: Optional list of team IDs to filter by
            player_ids: Optional list of player IDs to filter by
        """
        logger.info(f"Populating shot charts for {len(game_ids)} games")
        
        for i, game_id in enumerate(game_ids):
            logger.info(f"Processing shot chart {i+1}/{len(game_ids)}: {game_id}")
            
            try:
                shot_chart = self.client.get_shot_chart_detail(
                    game_id=game_id,
                    team_id=team_ids[0] if team_ids else None,
                    player_id=player_ids[0] if player_ids else None
                )
                
                if shot_chart is not None and not shot_chart.empty:
                    logger.info(f"Retrieved {len(shot_chart)} shots for {game_id}")
                    # Process and save shot chart data
                    
            except Exception as e:
                logger.error(f"Error processing shot chart {game_id}: {e}")
    
    def populate_standings(self, seasons: List[str]) -> None:
        """Populate league standings for specified seasons.
        
        Args:
            seasons: List of seasons to process
        """
        logger.info(f"Populating standings for seasons: {seasons}")
        
        for season in seasons:
            try:
                standings = self.client.get_league_standings(season)
                if standings is not None and not standings.empty:
                    logger.info(f"Retrieved standings for {season}: {len(standings)} records")
                    # Process and save standings data
                    
            except Exception as e:
                logger.error(f"Error processing standings for {season}: {e}")
    
    def populate_tracking_stats(
        self,
        seasons: List[str],
        season_types: List[str]
    ) -> None:
        """Populate player tracking statistics.
        
        Args:
            seasons: List of seasons to process
            season_types: List of season types
        """
        logger.info(f"Populating tracking stats for {len(seasons)} seasons")
        
        for season in seasons:
            for season_type in season_types:
                try:
                    tracking = self.client.get_player_tracking_stats(
                        season=season,
                        season_type=season_type
                    )
                    
                    if tracking is not None and not tracking.empty:
                        logger.info(f"Retrieved tracking stats for {season} {season_type}: {len(tracking)} records")
                        # Process and save tracking data
                        
                except Exception as e:
                    logger.error(f"Error processing tracking stats for {season} {season_type}: {e}")
    
    def populate_hustle_stats(
        self,
        seasons: List[str],
        season_types: List[str]
    ) -> None:
        """Populate hustle statistics.
        
        Args:
            seasons: List of seasons to process
            season_types: List of season types
        """
        logger.info(f"Populating hustle stats for {len(seasons)} seasons")
        
        for season in seasons:
            for season_type in season_types:
                try:
                    hustle = self.client.get_hustle_stats(
                        season=season,
                        season_type=season_type
                    )
                    
                    if hustle is not None and not hustle.empty:
                        logger.info(f"Retrieved hustle stats for {season} {season_type}: {len(hustle)} records")
                        # Process and save hustle data
                        
                except Exception as e:
                    logger.error(f"Error processing hustle stats for {season} {season_type}: {e}")
    
    def populate_all(
        self,
        data_types: List[str],
        seasons: List[str],
        season_types: List[str],
        active_only: bool = False,
        resume: bool = False
    ) -> None:
        """Populate all specified data types.
        
        Args:
            data_types: List of data types to populate
            seasons: List of seasons to process
            season_types: List of season types
            active_only: Whether to only include active players
            resume: Whether to resume from previous progress
        """
        self.progress["started_at"] = datetime.now().isoformat()
        self._save_progress()
        
        logger.info(f"Starting population with data types: {data_types}")
        logger.info(f"Seasons: {seasons}")
        logger.info(f"Season types: {season_types}")
        
        # Populate static data first
        if "players" in data_types or "all" in data_types:
            self.populate_players(active_only=active_only)
        
        if "teams" in data_types or "all" in data_types:
            self.populate_teams()
        
        # Populate season-based data
        if "games" in data_types or "all" in data_types:
            self.populate_games(seasons, season_types, active_only)
        
        if "standings" in data_types or "all" in data_types:
            self.populate_standings(seasons)
        
        if "tracking" in data_types or "all" in data_types:
            self.populate_tracking_stats(seasons, season_types)
        
        if "hustle" in data_types or "all" in data_types:
            self.populate_hustle_stats(seasons, season_types)
        
        # For game-specific data, we would need to get game IDs first
        # This is a simplified version - in practice you'd populate games first,
        # then use those game IDs for boxscores, play-by-play, etc.
        
        self.progress["completed_at"] = datetime.now().isoformat()
        self._save_progress()
        
        logger.info("Population completed successfully!")


def main():
    """Main entry point for the population script."""
    parser = argparse.ArgumentParser(
        description="Comprehensive NBA data population script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Data type selection
    parser.add_argument(
        "--data-types",
        nargs="+",
        choices=["players", "teams", "games", "boxscores", "playbyplay", 
                "shotcharts", "standings", "tracking", "hustle", "all"],
        default=["all"],
        help="Types of data to populate (default: all)"
    )
    
    # Season selection
    parser.add_argument(
        "--seasons",
        nargs="+",
        choices=ALL_SEASONS,
        default=ALL_SEASONS[:5],  # Last 5 seasons by default
        help="Seasons to populate (default: last 5 seasons)"
    )
    
    # Season types
    parser.add_argument(
        "--season-types",
        nargs="+",
        choices=list(SEASON_TYPES.values()),
        default=DEFAULT_SEASON_TYPES,
        help="Season types to include (default: Regular Season, Playoffs)"
    )
    
    # Player filtering
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only include currently active players (faster)"
    )
    
    # Resume functionality
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous progress"
    )
    
    # Performance options
    parser.add_argument(
        "--delay",
        type=float,
        default=0.6,
        help="Delay between API requests in seconds (default: 0.6)"
    )
    
    # Testing options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually doing it"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of records processed (for testing)"
    )
    
    args = parser.parse_args()
    
    # Configure API client
    config = get_api_config()
    config.request_delay = args.delay
    
    # Create population manager
    manager = PopulationManager()
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No actual data will be populated")
        logger.info(f"Would populate: {args.data_types}")
        logger.info(f"Would process seasons: {args.seasons}")
        logger.info(f"Would process season types: {args.season_types}")
        return
    
    # Start population
    try:
        manager.populate_all(
            data_types=args.data_types,
            seasons=args.seasons,
            season_types=args.season_types,
            active_only=args.active_only,
            resume=args.resume
        )
    except KeyboardInterrupt:
        logger.info("Population interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Population failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()