"""NBA Database Population Package.

This package provides tools for populating NBA data from the NBA API into a DuckDB database.

Modules:
    api_client: Enhanced NBA API client with rate limiting and retry logic
    config: Configuration settings for API and database
    database: Database utilities for DuckDB operations
    validation: Data validation and quality checks
    base: Base class for population scripts with common functionality
    init_db: Database initialization and schema creation
    cli: Unified command-line interface for all population operations
    populate_nba_data: Main comprehensive population script
    populate_player_game_stats: Player game statistics population (per-player)
    populate_player_game_stats_v2: Player game statistics population (bulk endpoint)
    populate_play_by_play: Play-by-play data population
    populate_player_season_stats: Aggregated player season statistics

Usage:
    # Using the unified CLI
    python -m scripts.populate.cli init          # Initialize database
    python -m scripts.populate.cli player-games  # Fetch player game stats (bulk)
    python -m scripts.populate.cli all           # Run full pipeline

    # Using the population manager
    from scripts.populate import NBAClient, PopulationManager

    client = NBAClient()
    manager = PopulationManager(client)
    manager.populate_all(['players', 'games'], ['2023-24'])

    # Using individual population functions
    from scripts.populate.populate_player_game_stats_v2 import populate_player_game_stats_v2
    from scripts.populate.populate_play_by_play import populate_play_by_play
    from scripts.populate.populate_player_season_stats import populate_player_season_stats

    populate_player_game_stats_v2(seasons=['2025-26'])
    populate_play_by_play(seasons=['2025-26'], limit=10)
    populate_player_season_stats()

Based on nba_api library documentation:
    reference/nba_api/
"""

__version__ = "1.2.0"

# Core components
from scripts.populate.api_client import NBAClient, get_client
from scripts.populate.base import BasePopulator, PopulationMetrics, ProgressTracker
from scripts.populate.config import (
    ALL_SEASONS,
    CACHE_DIR,
    CURRENT_SEASON,
    DEFAULT_SEASON_TYPES,
    DEFAULT_SEASONS,
    RECENT_SEASONS,
    SEASON_TYPES,
    ensure_cache_dir,
    get_api_config,
    get_db_path,
)
from scripts.populate.database import DatabaseManager
from scripts.populate.init_db import get_database_info, init_database
from scripts.populate.populate_nba_data import PopulationManager
from scripts.populate.populate_play_by_play import populate_play_by_play

# Population functions
from scripts.populate.populate_player_game_stats import populate_player_game_stats
from scripts.populate.populate_player_game_stats_v2 import populate_player_game_stats_v2
from scripts.populate.populate_player_season_stats import populate_player_season_stats
from scripts.populate.validation import DataValidator


__all__ = [
    "ALL_SEASONS",
    "CACHE_DIR",
    "CURRENT_SEASON",
    "DEFAULT_SEASONS",
    "DEFAULT_SEASON_TYPES",
    "RECENT_SEASONS",
    "SEASON_TYPES",
    # Base classes
    "BasePopulator",
    "DataValidator",
    "DatabaseManager",
    # Core components
    "NBAClient",
    "PopulationManager",
    "PopulationMetrics",
    "ProgressTracker",
    "ensure_cache_dir",
    "get_api_config",
    "get_client",
    "get_database_info",
    "get_db_path",
    # Database utilities
    "init_database",
    "populate_play_by_play",
    # Population functions
    "populate_player_game_stats",
    "populate_player_game_stats_v2",
    "populate_player_season_stats",
]
