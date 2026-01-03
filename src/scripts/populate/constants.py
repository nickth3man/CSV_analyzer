"""Centralized constants for NBA data population scripts.

This module consolidates all field mappings, column definitions, and shared
constants used across population scripts. Centralizing these definitions:
- Eliminates duplication across multiple files
- Provides a single source of truth for schema definitions
- Makes it easier to update field mappings when API changes

Usage:
    from src.scripts.populate.constants import (
        PLAYER_GAME_STATS_COLUMNS,
        GAME_TABLE_COLUMNS,
        PlayerGameLogsColumnMap,
    )
"""

from __future__ import annotations

from enum import Enum
from typing import Final


# =============================================================================
# SEASON CONFIGURATION
# =============================================================================


class SeasonType(str, Enum):
    """NBA season types supported by the API."""

    REGULAR = "Regular Season"
    PLAYOFFS = "Playoffs"
    ALL_STAR = "All Star"
    PRESEASON = "Pre Season"


# Season type string mappings for API compatibility
SEASON_TYPE_MAP: Final[dict[str, str]] = {
    "regular": SeasonType.REGULAR.value,
    "playoffs": SeasonType.PLAYOFFS.value,
    "all_star": SeasonType.ALL_STAR.value,
    "preseason": SeasonType.PRESEASON.value,
}


# =============================================================================
# API COLUMN NAME MAPPINGS
# =============================================================================


class ColumnMapping:
    """Base class for column mapping definitions with validation.

    Subclasses must define a MAPPING class attribute as a dict[str, str].
    """

    MAPPING: dict[str, str] = {}  # Override in subclasses

    @classmethod
    def get_api_columns(cls) -> list[str]:
        """Return list of API column names (mapping keys)."""
        return list(cls.MAPPING.keys())

    @classmethod
    def get_db_columns(cls) -> list[str]:
        """Return list of database column names (mapping values)."""
        return list(cls.MAPPING.values())

    @classmethod
    def rename(cls, api_name: str) -> str:
        """Convert API column name to database column name."""
        return cls.MAPPING.get(api_name) or api_name.lower()


class PlayerGameLogColumnMap(ColumnMapping):
    """Column mapping for PlayerGameLog endpoint (per-player)."""

    MAPPING: dict[str, str] = {
        # Identifiers
        "Game_ID": "game_id",
        "GAME_ID": "game_id",
        "Player_ID": "player_id",
        "PLAYER_ID": "player_id",
        "PLAYER_NAME": "player_name",
        # Season/Game info
        "SEASON_ID": "season_id",
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
        "TOV": "tov",
        "PF": "pf",
        "PTS": "pts",
        "PLUS_MINUS": "plus_minus",
        "VIDEO_AVAILABLE": "video_available",
    }


class PlayerGameLogsColumnMap(ColumnMapping):
    """Column mapping for PlayerGameLogs endpoint (bulk query)."""

    MAPPING: dict[str, str] = {
        **PlayerGameLogColumnMap.MAPPING,
        # Additional fields from bulk endpoint
        "SEASON_YEAR": "season_year",
        "TEAM_ID": "team_id",
        "TEAM_ABBREVIATION": "team_abbreviation",
        "TEAM_NAME": "team_name",
        "BLKA": "blka",
        "PFD": "pfd",
        "NBA_FANTASY_PTS": "fantasy_pts",
        "DD2": "double_double",
        "TD3": "triple_double",
    }


class LeagueGameLogColumnMap(ColumnMapping):
    """Column mapping for LeagueGameLog endpoint (team-level)."""

    MAPPING: dict[str, str] = {
        "SEASON_ID": "season_id",
        "TEAM_ID": "team_id",
        "TEAM_ABBREVIATION": "team_abbreviation",
        "TEAM_NAME": "team_name",
        "GAME_ID": "game_id",
        "GAME_DATE": "game_date",
        "MATCHUP": "matchup",
        "WL": "wl",
        "MIN": "min",
        "FGM": "fgm",
        "FGA": "fga",
        "FG_PCT": "fg_pct",
        "FG3M": "fg3m",
        "FG3A": "fg3a",
        "FG3_PCT": "fg3_pct",
        "FTM": "ftm",
        "FTA": "fta",
        "FT_PCT": "ft_pct",
        "OREB": "oreb",
        "DREB": "dreb",
        "REB": "reb",
        "AST": "ast",
        "STL": "stl",
        "BLK": "blk",
        "TOV": "tov",
        "PF": "pf",
        "PTS": "pts",
        "PLUS_MINUS": "plus_minus",
        "VIDEO_AVAILABLE": "video_available",
    }


class BoxscoreTraditionalColumnMap(ColumnMapping):
    """Column mapping for BoxScoreTraditional endpoint."""

    MAPPING: dict[str, str] = {
        "gameId": "game_id",
        "teamId": "team_id",
        "teamCity": "team_city",
        "teamName": "team_name",
        "teamTricode": "team_tricode",
        "personId": "person_id",
        "firstName": "first_name",
        "familyName": "family_name",
        "nameI": "name_i",
        "playerSlug": "player_slug",
        "position": "position",
        "comment": "comment",
        "jerseyNum": "jersey_num",
        "minutes": "minutes",
        "fieldGoalsMade": "field_goals_made",
        "fieldGoalsAttempted": "field_goals_attempted",
        "fieldGoalsPercentage": "field_goals_percentage",
        "threePointersMade": "three_pointers_made",
        "threePointersAttempted": "three_pointers_attempted",
        "threePointersPercentage": "three_pointers_percentage",
        "freeThrowsMade": "free_throws_made",
        "freeThrowsAttempted": "free_throws_attempted",
        "freeThrowsPercentage": "free_throws_percentage",
        "reboundsOffensive": "rebounds_offensive",
        "reboundsDefensive": "rebounds_defensive",
        "reboundsTotal": "rebounds_total",
        "assists": "assists",
        "steals": "steals",
        "blocks": "blocks",
        "turnovers": "turnovers",
        "foulsPersonal": "fouls_personal",
        "points": "points",
        "plusMinusPoints": "plus_minus_points",
    }


class BoxscoreAdvancedColumnMap(ColumnMapping):
    """Column mapping for BoxScoreAdvanced endpoint."""

    MAPPING: dict[str, str] = {
        "gameId": "game_id",
        "teamId": "team_id",
        "teamCity": "team_city",
        "teamName": "team_name",
        "teamTricode": "team_tricode",
        "personId": "person_id",
        "firstName": "first_name",
        "familyName": "family_name",
        "nameI": "name_i",
        "playerSlug": "player_slug",
        "position": "position",
        "comment": "comment",
        "jerseyNum": "jersey_num",
        "minutes": "minutes",
        "estimatedOffensiveRating": "off_rating",
        "offensiveRating": "off_rating",
        "estimatedDefensiveRating": "def_rating",
        "defensiveRating": "def_rating",
        "estimatedNetRating": "net_rating",
        "netRating": "net_rating",
        "assistPercentage": "ast_pct",
        "assistToTurnover": "ast_to",
        "assistRatio": "ast_ratio",
        "offensiveReboundPercentage": "oreb_pct",
        "defensiveReboundPercentage": "dreb_pct",
        "reboundPercentage": "reb_pct",
        "turnoverRatio": "tm_tov_pct",
        "effectiveFieldGoalPercentage": "efg_pct",
        "trueShootingPercentage": "ts_pct",
        "usagePercentage": "usg_pct",
        "estimatedUsagePercentage": "usg_pct",
        "pace": "pace",
        "pacePer40": "pace_per40",
        "possessions": "possessions",
        "PIE": "pie",
    }


class PlayByPlayColumnMap(ColumnMapping):
    """Column mapping for PlayByPlay endpoint."""

    MAPPING: dict[str, str] = {
        "gameId": "game_id",
        "actionNumber": "action_number",
        "clock": "clock",
        "period": "period",
        "teamId": "team_id",
        "teamTricode": "team_tricode",
        "personId": "person_id",
        "playerName": "player_name",
        "playerNameI": "player_name_i",
        "xLegacy": "x_legacy",
        "yLegacy": "y_legacy",
        "shotDistance": "shot_distance",
        "shotResult": "shot_result",
        "isFieldGoal": "is_field_goal",
        "scoreHome": "score_home",
        "scoreAway": "score_away",
        "pointsTotal": "points_total",
        "location": "location",
        "description": "description",
        "actionType": "action_type",
        "subType": "sub_type",
        "videoAvailable": "video_available",
        "shotValue": "shot_value",
        "actionId": "action_id",
    }


class ShotChartColumnMap(ColumnMapping):
    """Column mapping for ShotChartDetail endpoint."""

    MAPPING: dict[str, str] = {
        "GAME_ID": "game_id",
        "GRID_TYPE": "grid_type",
        "SHOT_ZONE_BASIC": "shot_zone_basic",
        "SHOT_ZONE_AREA": "shot_zone_area",
        "SHOT_ZONE_RANGE": "shot_zone_range",
        "SHOT_DISTANCE": "shot_distance",
        "LOC_X": "loc_x",
        "LOC_Y": "loc_y",
        "SHOT_MADE_FLAG": "shot_made_flag",
        "PLAYER_ID": "player_id",
        "TEAM_ID": "team_id",
        "TEAM_NAME": "team_name",
        "PERIOD": "period",
        "MINUTES_REMAINING": "minutes_remaining",
        "SECONDS_REMAINING": "seconds_remaining",
        "EVENT_TYPE": "event_type",
        "ACTION_TYPE": "action_type",
        "SHOT_TYPE": "shot_type",
    }


class CommonPlayerInfoColumnMap(ColumnMapping):
    """Column mapping for CommonPlayerInfo endpoint."""

    MAPPING: dict[str, str] = {
        "PERSON_ID": "person_id",
        "FIRST_NAME": "first_name",
        "LAST_NAME": "last_name",
        "DISPLAY_FIRST_LAST": "display_first_last",
        "DISPLAY_LAST_COMMA_FIRST": "display_last_comma_first",
        "DISPLAY_FI_LAST": "display_fi_last",
        "PLAYER_SLUG": "player_slug",
        "BIRTHDATE": "birthdate",
        "SCHOOL": "school",
        "COUNTRY": "country",
        "LAST_AFFILIATION": "last_affiliation",
        "HEIGHT": "height",
        "WEIGHT": "weight",
        "SEASON_EXP": "season_exp",
        "JERSEY": "jersey",
        "POSITION": "position",
        "ROSTERSTATUS": "rosterstatus",
        "GAMES_PLAYED_CURRENT_SEASON_FLAG": "games_played_current_season_flag",
        "TEAM_ID": "team_id",
        "TEAM_NAME": "team_name",
        "TEAM_ABBREVIATION": "team_abbreviation",
        "TEAM_CODE": "team_code",
        "TEAM_CITY": "team_city",
        "PLAYERCODE": "playercode",
        "FROM_YEAR": "from_year",
        "TO_YEAR": "to_year",
        "DLEAGUE_FLAG": "dleague_flag",
        "NBA_FLAG": "nba_flag",
        "GAMES_PLAYED_FLAG": "games_played_flag",
        "DRAFT_YEAR": "draft_year",
        "DRAFT_ROUND": "draft_round",
        "DRAFT_NUMBER": "draft_number",
        "GREATEST_75_FLAG": "greatest_75_flag",
    }


# =============================================================================
# DATABASE TABLE COLUMN DEFINITIONS
# =============================================================================

# Player game stats columns (matches player_game_stats_raw schema)
PLAYER_GAME_STATS_COLUMNS: Final[list[str]] = [
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

# Extended player game stats columns (includes fantasy and advanced)
PLAYER_GAME_STATS_EXTENDED_COLUMNS: Final[list[str]] = [
    *PLAYER_GAME_STATS_COLUMNS,
    "fantasy_pts",
    "double_double",
    "triple_double",
]

# Game table columns (home/away pivot format)
GAME_TABLE_COLUMNS: Final[list[str]] = [
    "season_id",
    "team_id_home",
    "team_abbreviation_home",
    "team_name_home",
    "game_id",
    "game_date",
    "matchup_home",
    "wl_home",
    "min",
    "fgm_home",
    "fga_home",
    "fg_pct_home",
    "fg3m_home",
    "fg3a_home",
    "fg3_pct_home",
    "ftm_home",
    "fta_home",
    "ft_pct_home",
    "oreb_home",
    "dreb_home",
    "reb_home",
    "ast_home",
    "stl_home",
    "blk_home",
    "tov_home",
    "pf_home",
    "pts_home",
    "plus_minus_home",
    "video_available_home",
    "team_id_away",
    "team_abbreviation_away",
    "team_name_away",
    "matchup_away",
    "wl_away",
    "fgm_away",
    "fga_away",
    "fg_pct_away",
    "fg3m_away",
    "fg3a_away",
    "fg3_pct_away",
    "ftm_away",
    "fta_away",
    "ft_pct_away",
    "oreb_away",
    "dreb_away",
    "reb_away",
    "ast_away",
    "stl_away",
    "blk_away",
    "tov_away",
    "pf_away",
    "pts_away",
    "plus_minus_away",
    "video_available_away",
    "season_type",
    "filename",
]

# BoxScore traditional fields
BOXSCORE_TRADITIONAL_COLUMNS: Final[list[str]] = [
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

# BoxScore advanced fields
BOXSCORE_ADVANCED_COLUMNS: Final[list[str]] = [
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

# Play-by-play fields
PLAY_BY_PLAY_COLUMNS: Final[list[str]] = [
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

# Shot chart fields
SHOT_CHART_COLUMNS: Final[list[str]] = [
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

# Common player info fields
COMMON_PLAYER_INFO_COLUMNS: Final[list[str]] = [
    "person_id",
    "first_name",
    "last_name",
    "display_first_last",
    "display_last_comma_first",
    "display_fi_last",
    "player_slug",
    "birthdate",
    "school",
    "country",
    "last_affiliation",
    "height",
    "weight",
    "season_exp",
    "jersey",
    "position",
    "rosterstatus",
    "games_played_current_season_flag",
    "team_id",
    "team_name",
    "team_abbreviation",
    "team_code",
    "team_city",
    "playercode",
    "from_year",
    "to_year",
    "dleague_flag",
    "nba_flag",
    "games_played_flag",
    "draft_year",
    "draft_round",
    "draft_number",
    "greatest_75_flag",
    "filename",
]

# Draft history fields
DRAFT_HISTORY_COLUMNS: Final[list[str]] = [
    "person_id",
    "player_name",
    "season",
    "round_number",
    "round_pick",
    "overall_pick",
    "draft_type",
    "team_id",
    "team_city",
    "team_name",
    "team_abbreviation",
    "organization",
    "organization_type",
    "player_profile_flag",
    "filename",
]

# Draft combine stats fields
DRAFT_COMBINE_STATS_COLUMNS: Final[list[str]] = [
    "season",
    "player_id",
    "player_name",
    "position",
    "height_wo_shoes",
    "height_wo_shoes_ft_in",
    "height_w_shoes",
    "height_w_shoes_ft_in",
    "weight",
    "wingspan",
    "wingspan_ft_in",
    "standing_reach",
    "standing_reach_ft_in",
    "body_fat_pct",
    "hand_length",
    "hand_width",
    "standing_vertical_leap",
    "max_vertical_leap",
    "lane_agility_time",
    "modified_lane_agility_time",
    "three_quarter_sprint",
    "bench_press",
    "spot_fifteen_corner_left",
    "spot_fifteen_break_left",
    "spot_fifteen_top_key",
    "spot_fifteen_break_right",
    "spot_fifteen_corner_right",
    "spot_college_corner_left",
    "spot_college_break_left",
    "spot_college_top_key",
    "spot_college_break_right",
    "spot_college_corner_right",
    "spot_nba_corner_left",
    "spot_nba_break_left",
    "spot_nba_top_key",
    "spot_nba_break_right",
    "spot_nba_corner_right",
    "off_drib_fifteen_break_left",
    "off_drib_fifteen_top_key",
    "off_drib_fifteen_break_right",
    "off_drib_college_break_left",
    "off_drib_college_top_key",
    "off_drib_college_break_right",
    "on_move_fifteen",
    "on_move_college",
    "filename",
]


# =============================================================================
# NUMERIC FIELD DEFINITIONS (for type coercion)
# =============================================================================

# Integer counting stats (can never be decimal)
INTEGER_STAT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "fgm",
        "fga",
        "fg3m",
        "fg3a",
        "ftm",
        "fta",
        "oreb",
        "dreb",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "pf",
        "pts",
        "field_goals_made",
        "field_goals_attempted",
        "three_pointers_made",
        "three_pointers_attempted",
        "free_throws_made",
        "free_throws_attempted",
        "rebounds_offensive",
        "rebounds_defensive",
        "rebounds_total",
        "assists",
        "steals",
        "blocks",
        "turnovers",
        "fouls_personal",
        "points",
        "games_played",
        "games_started",
    }
)

# Percentage stats (0.0 to 1.0)
PERCENTAGE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "fg_pct",
        "fg3_pct",
        "ft_pct",
        "ts_pct",
        "efg_pct",
        "field_goals_percentage",
        "three_pointers_percentage",
        "free_throws_percentage",
        "ast_pct",
        "oreb_pct",
        "dreb_pct",
        "reb_pct",
        "usg_pct",
    }
)

# Float stats that can have decimals
FLOAT_STAT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "plus_minus",
        "fantasy_pts",
        "plus_minus_points",
        "off_rating",
        "def_rating",
        "net_rating",
        "pace",
        "pie",
        "ast_to",
        "ast_ratio",
        "tm_tov_pct",
    }
)


# =============================================================================
# HOME/AWAY COLUMN MAPPINGS FOR GAME PIVOTING
# =============================================================================

HOME_COLUMN_MAP: Final[dict[str, str]] = {
    "GAME_ID": "game_id",
    "TEAM_ID": "team_id_home",
    "TEAM_ABBREVIATION": "team_abbreviation_home",
    "TEAM_NAME": "team_name_home",
    "MATCHUP": "matchup_home",
    "WL": "wl_home",
    "FGM": "fgm_home",
    "FGA": "fga_home",
    "FG_PCT": "fg_pct_home",
    "FG3M": "fg3m_home",
    "FG3A": "fg3a_home",
    "FG3_PCT": "fg3_pct_home",
    "FTM": "ftm_home",
    "FTA": "fta_home",
    "FT_PCT": "ft_pct_home",
    "OREB": "oreb_home",
    "DREB": "dreb_home",
    "REB": "reb_home",
    "AST": "ast_home",
    "STL": "stl_home",
    "BLK": "blk_home",
    "TOV": "tov_home",
    "PF": "pf_home",
    "PTS": "pts_home",
    "PLUS_MINUS": "plus_minus_home",
    "VIDEO_AVAILABLE": "video_available_home",
}

AWAY_COLUMN_MAP: Final[dict[str, str]] = {
    "GAME_ID": "game_id",
    "TEAM_ID": "team_id_away",
    "TEAM_ABBREVIATION": "team_abbreviation_away",
    "TEAM_NAME": "team_name_away",
    "MATCHUP": "matchup_away",
    "WL": "wl_away",
    "FGM": "fgm_away",
    "FGA": "fga_away",
    "FG_PCT": "fg_pct_away",
    "FG3M": "fg3m_away",
    "FG3A": "fg3a_away",
    "FG3_PCT": "fg3_pct_away",
    "FTM": "ftm_away",
    "FTA": "fta_away",
    "FT_PCT": "ft_pct_away",
    "OREB": "oreb_away",
    "DREB": "dreb_away",
    "REB": "reb_away",
    "AST": "ast_away",
    "STL": "stl_away",
    "BLK": "blk_away",
    "TOV": "tov_away",
    "PF": "pf_away",
    "PTS": "pts_away",
    "PLUS_MINUS": "plus_minus_away",
    "VIDEO_AVAILABLE": "video_available_away",
}

COMMON_GAME_COLUMN_MAP: Final[dict[str, str]] = {
    "GAME_ID": "game_id",
    "SEASON_ID": "season_id",
    "GAME_DATE": "game_date",
    "MIN": "min",
    "_season_type": "season_type",
}


# =============================================================================
# VALIDATION THRESHOLDS
# =============================================================================


class ValidationThresholds:
    """Configurable validation thresholds for data quality checks."""

    # Maximum minutes per game (OT games can exceed 48)
    MAX_MINUTES_PER_GAME: Final[int] = 70

    # Maximum points in a single game (Wilt's 100 is the record)
    MAX_POINTS_PER_GAME: Final[int] = 110

    # Maximum rebounds in a game
    MAX_REBOUNDS_PER_GAME: Final[int] = 60

    # Maximum assists in a game
    MAX_ASSISTS_PER_GAME: Final[int] = 40

    # Minimum games played for season stats
    MIN_GAMES_FOR_SEASON_STATS: Final[int] = 5

    # High statistical variation thresholds for flagging outliers
    HIGH_POINTS_STD: Final[int] = 20
    HIGH_REBOUNDS_STD: Final[int] = 10
    HIGH_ASSISTS_STD: Final[int] = 8

    # Null percentage threshold for warnings
    HIGH_NULL_PERCENTAGE: Final[float] = 50.0
