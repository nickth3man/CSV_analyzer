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

    populate_player_game_stats_v2(seasons=['2023-24'])
    populate_play_by_play(seasons=['2023-24'], limit=10)
    populate_player_season_stats()

Based on nba_api library documentation:
    reference/nba_api/
"""

__version__ = "1.1.0"

# Core components
from .api_client import NBAClient, get_client
from .config import (
    get_api_config,
    get_db_path,
    ALL_SEASONS,
    SEASON_TYPES,
    DEFAULT_SEASON_TYPES,
    CACHE_DIR,
    ensure_cache_dir,
)
from .database import DatabaseManager
from .validation import DataValidator
from .base import BasePopulator, PopulationMetrics, ProgressTracker
from .populate_nba_data import PopulationManager

# Population functions
from .populate_player_game_stats import populate_player_game_stats
from .populate_player_game_stats_v2 import populate_player_game_stats_v2
from .populate_play_by_play import populate_play_by_play
from .populate_player_season_stats import populate_player_season_stats
from .init_db import init_database, get_database_info

__all__ = [
    # Core components
    'NBAClient',
    'get_client',
    'get_api_config',
    'get_db_path',
    'ALL_SEASONS',
    'SEASON_TYPES',
    'DEFAULT_SEASON_TYPES',
    'CACHE_DIR',
    'ensure_cache_dir',
    'DatabaseManager',
    'DataValidator',
    'PopulationManager',
    # Base classes
    'BasePopulator',
    'PopulationMetrics',
    'ProgressTracker',
    # Population functions
    'populate_player_game_stats',
    'populate_player_game_stats_v2',
    'populate_play_by_play',
    'populate_player_season_stats',
    # Database utilities
    'init_database',
    'get_database_info',
]
