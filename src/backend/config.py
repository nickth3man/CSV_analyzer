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
#          data_dir: str = Field(default="CSV")
#          nba_season: str = Field(default_factory=get_current_season)
#          execution_timeout: int = Field(default=30, ge=5, le=300)
#   2. Or use pydantic-settings for environment variable loading:
#      from pydantic_settings import BaseSettings
#      class Settings(BaseSettings):
#          openrouter_api_key: str
#          nba_api_cache_ttl: int = 3600
#          class Config:
#              env_file = ".env"

# TODO (Configuration): Add configuration validation
# Current configs have no validation. Add runtime checks:
#   def validate_config():
#       assert CSV_EXECUTION_TIMEOUT > 0, "Timeout must be positive"
#       assert os.path.isdir(DEFAULT_DATA_DIR), f"Data dir not found: {DEFAULT_DATA_DIR}"
#       if NBA_DEFAULT_SEASON:
#           assert re.match(r"\\d{4}-\\d{2}", NBA_DEFAULT_SEASON), "Invalid season format"

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

import os

DEFAULT_DATA_DIR = "CSV"
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
