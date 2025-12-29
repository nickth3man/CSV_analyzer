"""Shared configuration constants for backend modules.

# TODO (Configuration): Add dynamic season detection
# Current default season is hardcoded as "2023-24". Should auto-detect:
#   def get_current_nba_season():
#       now = datetime.now()
#       year = now.year
#       # NBA season starts in October
#       if now.month >= 10:
#           return f"{year}-{str(year+1)[-2:]}"
#       else:
#           return f"{year-1}-{str(year)[-2:]}"
#   NBA_DEFAULT_SEASON = os.environ.get("NBA_API_DEFAULT_SEASON", get_current_nba_season())

# TODO (Configuration): Consolidate all configuration
# Configuration is split between this file, frontend/config.py, and environment
# variables. Consider:
#   1. Use a single configuration class with validation:
#      class Config:
#          data_dir: str = Field(default="src/backend/data/raw/csv")
#          nba_season: str = Field(default_factory=get_current_season)
#          execution_timeout: int = Field(default=30, ge=5, le=300)
#   2. Or use pydantic-settings for environment variable loading:
#      from pydantic_settings import BaseSettings
#      class Settings(BaseSettings):
#          openrouter_api_key: str
#          nba_api_cache_ttl: int = 3600
#          class Config:
#              env_file = ".env"

# TODO (Configuration): Add environment-specific configs
# Different settings for dev/staging/production:
#   ENV = os.environ.get("ENVIRONMENT", "development")
#   if ENV == "production":
#       CSV_EXECUTION_TIMEOUT = 60
#       API_EXECUTION_TIMEOUT = 120
#       LOG_LEVEL = "WARNING"
#   else:
#       CSV_EXECUTION_TIMEOUT = 30
#       API_EXECUTION_TIMEOUT = 60
#       LOG_LEVEL = "DEBUG"

# TODO (Feature): Add configurable LLM parameters
# Allow users to tune LLM behavior:
#   LLM_TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", 0.7))
#   LLM_MAX_TOKENS = int(os.environ.get("LLM_MAX_TOKENS", 4096))
#   LLM_TOP_P = float(os.environ.get("LLM_TOP_P", 0.9))
"""

from __future__ import annotations

import os
import re


DEFAULT_DATA_DIR = "src/backend/data/raw/csv"
NBA_DEFAULT_SEASON = os.environ.get("NBA_API_DEFAULT_SEASON", "2023-24")
ENTITY_SAMPLE_SIZE = 1000
SEARCH_SAMPLE_SIZE = 1000
CSV_EXECUTION_TIMEOUT = 30
API_EXECUTION_TIMEOUT = 60
CHART_HISTORY_LIMIT = 10
CHART_ROW_LIMIT = 10
RAW_RESULT_TRUNCATION = 5000
ANALYSIS_TRUNCATION = 2000
EXEC_RESULT_TRUNCATION = 3000
"""
Constants governing prompt construction.
"""
MAX_PLAN_STEPS = 6
MIN_PLAN_STEPS = 4
SUCCESSFUL_PATTERN_LIMIT = 10

# Season format pattern (e.g., "2023-24")
_SEASON_PATTERN = re.compile(r"^\d{4}-\d{2}$")

# Configuration values that must be positive integers
_POSITIVE_INT_CONFIGS: dict[str, int] = {
    "CSV_EXECUTION_TIMEOUT": CSV_EXECUTION_TIMEOUT,
    "API_EXECUTION_TIMEOUT": API_EXECUTION_TIMEOUT,
    "ENTITY_SAMPLE_SIZE": ENTITY_SAMPLE_SIZE,
    "SEARCH_SAMPLE_SIZE": SEARCH_SAMPLE_SIZE,
    "CHART_HISTORY_LIMIT": CHART_HISTORY_LIMIT,
    "CHART_ROW_LIMIT": CHART_ROW_LIMIT,
    "RAW_RESULT_TRUNCATION": RAW_RESULT_TRUNCATION,
    "ANALYSIS_TRUNCATION": ANALYSIS_TRUNCATION,
    "EXEC_RESULT_TRUNCATION": EXEC_RESULT_TRUNCATION,
    "MAX_PLAN_STEPS": MAX_PLAN_STEPS,
    "MIN_PLAN_STEPS": MIN_PLAN_STEPS,
    "SUCCESSFUL_PATTERN_LIMIT": SUCCESSFUL_PATTERN_LIMIT,
}


class ConfigurationError(Exception):
    """Raised when configuration validation fails."""


def validate_config() -> list[str]:
    """Validate all configuration values.

    Returns:
        List of validation error messages. Empty list if all validations pass.

    Raises:
        ConfigurationError: If any validation fails and raise_on_error is True
            (called via validate_config_or_raise).
    """
    errors: list[str] = []

    # Validate all positive integer configs
    for name, value in _POSITIVE_INT_CONFIGS.items():
        if value <= 0:
            errors.append(f"{name} must be positive, got {value}")

    # Validate season format if provided
    if NBA_DEFAULT_SEASON and not _SEASON_PATTERN.match(NBA_DEFAULT_SEASON):
        errors.append(
            f"NBA_DEFAULT_SEASON must match format 'YYYY-YY' (e.g., '2023-24'), "
            f"got '{NBA_DEFAULT_SEASON}'",
        )

    # Validate plan step constraints
    if MIN_PLAN_STEPS > MAX_PLAN_STEPS:
        errors.append(
            f"MIN_PLAN_STEPS ({MIN_PLAN_STEPS}) cannot be greater than "
            f"MAX_PLAN_STEPS ({MAX_PLAN_STEPS})",
        )

    return errors


def validate_config_or_raise() -> None:
    """Validate configuration and raise an exception if validation fails.

    Raises:
        ConfigurationError: If any configuration value is invalid.
    """
    errors = validate_config()
    if errors:
        raise ConfigurationError(
            "Configuration validation failed:\n  - " + "\n  - ".join(errors),
        )
