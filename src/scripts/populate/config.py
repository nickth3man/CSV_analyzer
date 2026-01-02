"""Configuration settings for NBA database population.

This module centralizes all configuration settings for:
- NBA API request parameters
- Rate limiting settings
- Database paths
- Season and season type definitions
- Column mappings from API to database schema
"""

import json
import logging
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# =============================================================================
# PATHS
# =============================================================================

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Default database path
DEFAULT_DB_PATH = PROJECT_ROOT / "src" / "backend" / "data" / "nba.duckdb"

# Cache directory for progress tracking
CACHE_DIR = PROJECT_ROOT / ".nba_cache"
PROGRESS_FILE = CACHE_DIR / "population_progress.json"


# =============================================================================
# NBA API CONFIGURATION
# =============================================================================


logger = logging.getLogger(__name__)


class NBAAPIConfig(BaseSettings):
    """Configuration for NBA API requests using pydantic-settings.

    Settings can be overridden via environment variables prefixed with NBA_API_.
    Example: NBA_API_TIMEOUT=60 will set the timeout to 60 seconds.
    """

    model_config = SettingsConfigDict(
        env_prefix="NBA_API_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Request timeout in seconds (can be single value or "connect,read" format)
    # NBA.com can be slow - use higher timeout (100+ recommended in docs)
    timeout: float = Field(default=120.0, description="Request timeout in seconds")

    @field_validator("timeout", mode="before")
    @classmethod
    def parse_timeout(cls, v: Any) -> float:
        """Parse timeout value, handling comma-separated 'connect,read' format."""
        if isinstance(v, str):
            # Handle "connect,read" format - use the first (connect) value
            parts = [p.strip() for p in v.split(",") if p.strip()]
            if parts:
                return float(parts[0])
        return float(v) if v is not None else 30.0

    # Proxy settings (None = no proxy)
    proxy: str | None = Field(default=None, description="Proxy URL for requests")

    # Rate limiting - NBA.com recommends 2+ seconds between requests
    request_delay: float = Field(
        default=0.6, ge=0.0, description="Seconds between requests"
    )

    # Retry settings
    max_retries: int = Field(default=3, ge=1, le=10, description="Max retry attempts")
    retry_backoff_factor: float = Field(
        default=2.0, ge=1.0, description="Exponential backoff multiplier"
    )

    # User agent override
    user_agent: str | None = Field(
        default=None, description="Override User-Agent header"
    )

    # Extra headers as JSON string
    headers_json: str | None = Field(
        default=None, description="JSON object of extra headers"
    )

    @property
    def headers(self) -> dict[str, str]:
        """Build request headers with defaults and overrides."""
        base_headers = {
            "Host": "stats.nba.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Referer": "https://stats.nba.com/",
            "Origin": "https://stats.nba.com",
            "x-nba-stats-origin": "stats",
            "x-nba-stats-token": "true",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "Sec-Ch-Ua": '"Chromium";v="140", "Google Chrome";v="140", "Not;A=Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

        # Apply user agent override
        if self.user_agent:
            base_headers["User-Agent"] = self.user_agent

        # Apply extra headers from JSON
        if self.headers_json:
            try:
                parsed = json.loads(self.headers_json)
                if isinstance(parsed, dict):
                    for key, value in parsed.items():
                        base_headers[str(key)] = str(value)
            except json.JSONDecodeError:
                logger.warning("NBA_API_HEADERS_JSON is not valid JSON.")

        return base_headers


class DatabaseConfig(BaseSettings):
    """Database configuration using pydantic-settings."""

    model_config = SettingsConfigDict(
        env_prefix="NBA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    db_path: Path = Field(
        default=Path(__file__).parent.parent.parent.parent
        / "src"
        / "backend"
        / "data"
        / "nba.duckdb",
        description="Path to DuckDB database",
    )

    @field_validator("db_path", mode="before")
    @classmethod
    def validate_db_path(cls, v: Any) -> Path:
        """Convert string paths to Path objects."""
        if isinstance(v, str):
            return Path(v)
        return v


# =============================================================================
# SEASONS CONFIGURATION
# =============================================================================

# All supported seasons (newest first)
# Updated: December 2025 - includes 2025-26 season
ALL_SEASONS: list[str] = [
    "2025-26",
    "2024-25",
    "2023-24",
    "2022-23",
    "2021-22",
    "2020-21",
    "2019-20",
    "2018-19",
    "2017-18",
    "2016-17",
    "2015-16",
    "2014-15",
    "2013-14",
    "2012-13",
    "2011-12",
    "2010-11",
    "2009-10",
    "2008-09",
    "2007-08",
    "2006-07",
    "2005-06",
    "2004-05",
    "2003-04",
    "2002-03",
    "2001-02",
    "2000-01",
    "1999-00",
    "1998-99",
    "1997-98",
    "1996-97",
]

# Current NBA season
CURRENT_SEASON: str = "2025-26"

# Default seasons for population (recent history)
DEFAULT_SEASONS: list[str] = ALL_SEASONS[:10]  # Last 10 seasons

# Recent seasons for quick population
RECENT_SEASONS: list[str] = ALL_SEASONS[:5]  # Last 5 seasons

# Season types from nba_api
SEASON_TYPES = {
    "regular": "Regular Season",
    "playoffs": "Playoffs",
    "all_star": "All Star",
    "preseason": "Pre Season",
}

DEFAULT_SEASON_TYPES: list[str] = ["Regular Season", "Playoffs"]


# =============================================================================
# ENDPOINT FIELD MAPPINGS
# =============================================================================

# PlayerGameLog endpoint expected fields
PLAYER_GAME_LOG_FIELDS = [
    "SEASON_ID",
    "Player_ID",
    "Game_ID",
    "GAME_DATE",
    "MATCHUP",
    "WL",
    "MIN",
    "FGM",
    "FGA",
    "FG_PCT",
    "FG3M",
    "FG3A",
    "FG3_PCT",
    "FTM",
    "FTA",
    "FT_PCT",
    "OREB",
    "DREB",
    "REB",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "PF",
    "PTS",
    "PLUS_MINUS",
    "VIDEO_AVAILABLE",
]

# PlayerGameLogs endpoint expected fields (bulk query)

PLAYER_GAME_LOGS_FIELDS = [
    "SEASON_YEAR",
    "PLAYER_ID",
    "PLAYER_NAME",
    "TEAM_ID",
    "TEAM_ABBREVIATION",
    "TEAM_NAME",
    "GAME_ID",
    "GAME_DATE",
    "MATCHUP",
    "WL",
    "MIN",
    "FGM",
    "FGA",
    "FG_PCT",
    "FG3M",
    "FG3A",
    "FG3_PCT",
    "FTM",
    "FTA",
    "FT_PCT",
    "OREB",
    "DREB",
    "REB",
    "AST",
    "TOV",
    "STL",
    "BLK",
    "BLKA",
    "PF",
    "PFD",
    "PTS",
    "PLUS_MINUS",
    "NBA_FANTASY_PTS",
    "DD2",
    "TD3",
]

# LeagueGameLog endpoint expected fields
LEAGUE_GAME_LOG_FIELDS = [
    "SEASON_ID",
    "TEAM_ID",
    "TEAM_ABBREVIATION",
    "TEAM_NAME",
    "GAME_ID",
    "GAME_DATE",
    "MATCHUP",
    "WL",
    "MIN",
    "FGM",
    "FGA",
    "FG_PCT",
    "FG3M",
    "FG3A",
    "FG3_PCT",
    "FTM",
    "FTA",
    "FT_PCT",
    "OREB",
    "DREB",
    "REB",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "PF",
    "PTS",
    "PLUS_MINUS",
    "VIDEO_AVAILABLE",
]


# =============================================================================
# DATABASE COLUMN MAPPINGS
# =============================================================================

# Mapping from NBA API column names to our database schema
# Handles both uppercase and mixed-case variations from the API
COLUMN_MAPPING: dict[str, str] = {
    # Game identifiers
    "Game_ID": "game_id",
    "GAME_ID": "game_id",
    # Player identifiers
    "Player_ID": "player_id",
    "PLAYER_ID": "player_id",
    "PLAYER_NAME": "player_name",
    # Team identifiers
    "TEAM_ID": "team_id",
    "TEAM_ABBREVIATION": "team_abbreviation",
    "TEAM_NAME": "team_name",
    # Game info
    "SEASON_ID": "season_id",
    "SEASON_YEAR": "season_year",
    "GAME_DATE": "game_date",
    "MATCHUP": "matchup",
    "WL": "wl",
    # Minutes
    "MIN": "min",
    # Field Goals
    "FGM": "fgm",
    "FGA": "fga",
    "FG_PCT": "fg_pct",
    # Three Pointers
    "FG3M": "fg3m",
    "FG3A": "fg3a",
    "FG3_PCT": "fg3_pct",
    # Free Throws
    "FTM": "ftm",
    "FTA": "fta",
    "FT_PCT": "ft_pct",
    # Rebounds
    "OREB": "oreb",
    "DREB": "dreb",
    "REB": "reb",
    # Other stats
    "AST": "ast",
    "STL": "stl",
    "BLK": "blk",
    "BLKA": "blka",
    "TOV": "tov",
    "PF": "pf",
    "PFD": "pfd",
    "PTS": "pts",
    "PLUS_MINUS": "plus_minus",
    # Additional fields from PlayerGameLogs
    "NBA_FANTASY_PTS": "fantasy_pts",
    "DD2": "double_double",
    "TD3": "triple_double",
    "VIDEO_AVAILABLE": "video_available",
}


# =============================================================================
# DATABASE SCHEMA
# =============================================================================

# Expected columns in player_game_stats table
PLAYER_GAME_STATS_COLUMNS = [
    "game_id",
    "team_id",
    "player_id",
    "player_name",
    "start_position",
    "comment",
    "min",
    "fgm",
    "fga",
    "fg_pct",
    "fg3m",
    "fg3a",
    "fg3_pct",
    "ftm",
    "fta",
    "ft_pct",
    "oreb",
    "dreb",
    "reb",
    "ast",
    "stl",
    "blk",
    "tov",
    "pf",
    "pts",
    "plus_minus",
]


# Box score data types
BOXSCORE_TRADITIONAL_FIELDS = [
    "game_id",
    "team_id",
    "team_city",
    "team_name",
    "team_tricode",
    "person_id",
    "first_name",
    "family_name",
    "name_i",
    "player_slug",
    "position",
    "comment",
    "jersey_num",
    "minutes",
    "field_goals_made",
    "field_goals_attempted",
    "field_goals_percentage",
    "three_pointers_made",
    "three_pointers_attempted",
    "three_pointers_percentage",
    "free_throws_made",
    "free_throws_attempted",
    "free_throws_percentage",
    "rebounds_offensive",
    "rebounds_defensive",
    "rebounds_total",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "fouls_personal",
    "points",
    "plus_minus_points",
]

BOXSCORE_ADVANCED_FIELDS = [
    "game_id",
    "team_id",
    "team_city",
    "team_name",
    "team_tricode",
    "person_id",
    "first_name",
    "family_name",
    "name_i",
    "player_slug",
    "position",
    "comment",
    "jersey_num",
    "minutes",
    "off_rating",
    "def_rating",
    "net_rating",
    "ast_pct",
    "ast_to",
    "ast_ratio",
    "oreb_pct",
    "dreb_pct",
    "reb_pct",
    "tm_tov_pct",
    "efg_pct",
    "ts_pct",
    "usg_pct",
    "pace",
    "pie",
]

PLAY_BY_PLAY_FIELDS = [
    "game_id",
    "action_number",
    "clock",
    "period",
    "team_id",
    "team_tricode",
    "person_id",
    "player_name",
    "player_name_i",
    "x_legacy",
    "y_legacy",
    "shot_distance",
    "shot_result",
    "is_field_goal",
    "score_home",
    "score_away",
    "points_total",
    "location",
    "description",
    "action_type",
    "sub_type",
    "video_available",
    "shot_value",
    "action_id",
]

SHOT_CHART_FIELDS = [
    "game_id",
    "grid_type",
    "shot_zone_basic",
    "shot_zone_area",
    "shot_zone_range",
    "shot_distance",
    "loc_x",
    "loc_y",
    "shot_made_flag",
    "player_id",
    "team_id",
    "team_name",
    "period",
    "minutes_remaining",
    "seconds_remaining",
    "event_type",
    "action_type",
    "shot_type",
]

LEAGUE_STANDINGS_FIELDS = [
    "team_id",
    "league_id",
    "season_id",
    "team",
    "team_city",
    "team_name",
    "team_abbreviation",
    "conference",
    "conference_record",
    "playoff_rank",
    "clinch_indicator",
    "division",
    "division_record",
    "division_rank",
    "wins",
    "losses",
    "win_pct",
    "league_rank",
    "record",
    "home_record",
    "road_record",
    "return_to_play_eligibility_flag",
]

PLAYER_TRACKING_FIELDS = [
    "player_id",
    "player_name",
    "team_id",
    "team_abbreviation",
    "age",
    "games_played",
    "wins",
    "losses",
    "minutes",
    "speed",
    "distance",
    "touches",
    "secondary_assists",
    "free_throw_assists",
    "passes",
    "assist_points_created",
    "time_of_possession",
    "drives",
    "points_per_drive",
    "pass_pct",
    "shoot_pct",
    "turnover_pct",
    "foul_pct",
]

HUSTLE_FIELDS = [
    "player_id",
    "player_name",
    "team_id",
    "team_abbreviation",
    "age",
    "games_played",
    "minutes",
    "deflections",
    "deflections_per_game",
    "loose_balls_recovered",
    "loose_balls_recovered_per_game",
    "charges_drawn",
    "charges_drawn_per_game",
    "screen_assists",
    "screen_assist_points",
    "screen_assists_per_game",
    "screen_assist_points_per_game",
]


# =============================================================================
# POPULATION CONFIGURATION
# =============================================================================


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def get_api_config() -> NBAAPIConfig:
    """Get NBA API configuration with environment overrides.

    Environment variables (automatically loaded via pydantic-settings):
        NBA_API_TIMEOUT: Request timeout in seconds
        NBA_API_REQUEST_DELAY: Delay between requests in seconds
        NBA_API_USER_AGENT: Override User-Agent header
        NBA_API_HEADERS_JSON: JSON object merged into request headers
        NBA_API_MAX_RETRIES: Max retry attempts for transient errors
        NBA_API_RETRY_BACKOFF_FACTOR: Backoff multiplier for retries
        NBA_API_PROXY: Proxy URL
    """
    return NBAAPIConfig()


def get_db_path() -> Path:
    """Get database path with environment override.

    Environment variables:
        NBA_DB_PATH: Path to DuckDB database
    """
    return DatabaseConfig().db_path


def ensure_cache_dir() -> None:
    """Create cache directory if it doesn't exist."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
