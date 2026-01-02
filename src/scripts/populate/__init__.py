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
    from src.scripts.populate import NBAClient, PopulationManager

    client = NBAClient()
    manager = PopulationManager(client)
    manager.populate_all(['players', 'games'], ['2023-24'])

    # Using individual population functions
    from src.scripts.populate.populate_player_game_stats_v2 import populate_player_game_stats_v2
    from src.scripts.populate.populate_play_by_play import populate_play_by_play
    from src.scripts.populate.populate_player_season_stats import populate_player_season_stats

    populate_player_game_stats_v2(seasons=['2025-26'])
    populate_play_by_play(seasons=['2025-26'], limit=10)
    populate_player_season_stats()
"""

__version__ = "1.2.0"

# Core components
from src.scripts.populate.api_client import NBAClient, get_client
from src.scripts.populate.base import BasePopulator, PopulationMetrics, ProgressTracker
from src.scripts.populate.config import (
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
from src.scripts.populate.database import DatabaseManager
from src.scripts.populate.init_db import get_database_info, init_database

# Population functions
from src.scripts.populate.populate_common_player_info import populate_common_player_info
from src.scripts.populate.populate_draft_combine_stats import (
    populate_draft_combine_stats,
)
from src.scripts.populate.populate_draft_history import populate_draft_history
from src.scripts.populate.populate_league_game_logs import (
    populate_league_game_logs,
)
from src.scripts.populate.populate_nba_data import PopulationManager
from src.scripts.populate.populate_play_by_play import populate_play_by_play
from src.scripts.populate.populate_player_game_stats import populate_player_game_stats
from src.scripts.populate.populate_player_game_stats_v2 import (
    populate_player_game_stats_v2,
)
from src.scripts.populate.populate_player_season_stats import (
    populate_player_season_stats,
)
from src.scripts.populate.populate_team_details import populate_team_details
from src.scripts.populate.populate_team_info_common import populate_team_info_common
from src.scripts.populate.validation import DataValidator


__all__ = [
    "ALL_SEASONS",
    "CACHE_DIR",
    "CURRENT_SEASON",
    "DEFAULT_SEASONS",
    "DEFAULT_SEASON_TYPES",
    "RECENT_SEASONS",
    "SEASON_TYPES",
    "BasePopulator",
    "DataValidator",
    "DatabaseManager",
    "NBAClient",
    "PopulationManager",
    "PopulationMetrics",
    "ProgressTracker",
    "ensure_cache_dir",
    "get_api_config",
    "get_client",
    "get_database_info",
    "get_db_path",
    "init_database",
    "populate_common_player_info",
    "populate_draft_combine_stats",
    "populate_draft_history",
    "populate_league_game_logs",
    "populate_play_by_play",
    "populate_player_game_stats",
    "populate_player_game_stats_v2",
    "populate_player_season_stats",
    "populate_team_details",
    "populate_team_info_common",
]
