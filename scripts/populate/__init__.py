"""NBA Database Population Package.

This package provides tools for populating NBA data from the NBA API into a database.

Modules:
    api_client: Enhanced NBA API client with rate limiting and retry logic
    config: Configuration settings for API and database
    database: Database utilities for DuckDB operations
    validation: Data validation and quality checks
    populate_nba_data: Main population script

Usage:
    from scripts.populate_db import NBAClient, PopulationManager
    
    client = NBAClient()
    manager = PopulationManager(client)
    manager.populate_all(['players', 'games'], ['2023-24'])
"""

__version__ = "1.0.0"

from .api_client import NBAClient, get_client
from .config import get_api_config, ALL_SEASONS, SEASON_TYPES
from .database import DatabaseManager
from .validation import DataValidator
from .populate_nba_data import PopulationManager

__all__ = [
    'NBAClient',
    'get_client',
    'get_api_config', 
    'ALL_SEASONS',
    'SEASON_TYPES',
    'DatabaseManager',
    'DataValidator',
    'PopulationManager'
]
