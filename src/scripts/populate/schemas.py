"""Pydantic schemas for NBA data validation.

This module provides typed data models for validating NBA data from APIs.
Using Pydantic ensures:
- Type safety with runtime validation
- Clear error messages for invalid data
- Cross-field validation with model validators
- Easy serialization/deserialization

Usage:
    from src.scripts.populate.schemas import PlayerGameStats

    # Validate a single record
    stats = PlayerGameStats.model_validate(api_response)

    # Validate a DataFrame
    validated_df = PlayerGameStats.validate_dataframe(df)
"""

from datetime import date, datetime
from enum import Enum
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# =============================================================================
# ENUMS
# =============================================================================


class SeasonType(str, Enum):
    """NBA season types."""

    REGULAR = "Regular Season"
    PLAYOFFS = "Playoffs"
    PRESEASON = "Pre Season"
    ALL_STAR = "All Star"
    PLAY_IN = "PlayIn"


class GameResult(str, Enum):
    """Game result (win/loss)."""

    WIN = "W"
    LOSS = "L"


class Position(str, Enum):
    """Player positions."""

    GUARD = "G"
    FORWARD = "F"
    CENTER = "C"
    GUARD_FORWARD = "G-F"
    FORWARD_GUARD = "F-G"
    FORWARD_CENTER = "F-C"
    CENTER_FORWARD = "C-F"


# =============================================================================
# BASE MODELS
# =============================================================================


class NBABaseModel(BaseModel):
    """Base model with common configuration for all NBA schemas."""

    model_config = ConfigDict(
        # Allow population by field name or alias
        populate_by_name=True,
        # Validate default values
        validate_default=True,
        # Allow extra fields (API may add new fields)
        extra="ignore",
        # Use enum values instead of enum objects
        use_enum_values=True,
        # Coerce strings to appropriate types
        coerce_numbers_to_str=False,
        # Strip whitespace from strings
        str_strip_whitespace=True,
    )

    @classmethod
    def validate_dataframe(
        cls,
        df: pd.DataFrame,
        raise_on_error: bool = False,
    ) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
        """Validate a DataFrame against this schema.

        Args:
            df: DataFrame to validate
            raise_on_error: If True, raise on first error

        Returns:
            Tuple of (valid_df, errors_list)
        """
        valid_records = []
        errors = []

        for idx, row in df.iterrows():
            try:
                record = cls.model_validate(row.to_dict())
                valid_records.append(record.model_dump())
            except Exception as e:
                error_info = {
                    "index": idx,
                    "error": str(e),
                    "data": row.to_dict(),
                }
                if raise_on_error:
                    raise ValueError(f"Validation error at index {idx}: {e}") from e
                errors.append(error_info)

        return pd.DataFrame(valid_records), errors


# =============================================================================
# PLAYER SCHEMAS
# =============================================================================


class Player(NBABaseModel):
    """Player entity schema."""

    id: int = Field(..., alias="player_id", ge=1, description="NBA player ID")
    full_name: str = Field(..., min_length=1, description="Full player name")
    first_name: str | None = Field(None, description="First name")
    last_name: str | None = Field(None, description="Last name")
    is_active: bool = Field(True, description="Whether player is currently active")

    @field_validator("full_name", mode="before")
    @classmethod
    def validate_name(cls, v: Any) -> str:
        """Ensure name is non-empty string."""
        if v is None or (isinstance(v, str) and not v.strip()):
            raise ValueError("Player name cannot be empty")
        return str(v).strip()


class CommonPlayerInfo(NBABaseModel):
    """Detailed player information from CommonPlayerInfo endpoint."""

    person_id: int = Field(..., alias="PERSON_ID", ge=1)
    display_first_last: str | None = Field(None, alias="DISPLAY_FIRST_LAST")
    display_last_comma_first: str | None = Field(None, alias="DISPLAY_LAST_COMMA_FIRST")
    birthdate: date | None = Field(None, alias="BIRTHDATE")
    school: str | None = Field(None, alias="SCHOOL")
    country: str | None = Field(None, alias="COUNTRY")
    height: str | None = Field(None, alias="HEIGHT")
    weight: str | None = Field(None, alias="WEIGHT")
    season_exp: int | None = Field(None, alias="SEASON_EXP", ge=0)
    jersey: str | None = Field(None, alias="JERSEY")
    position: str | None = Field(None, alias="POSITION")
    team_id: int | None = Field(None, alias="TEAM_ID")
    team_name: str | None = Field(None, alias="TEAM_NAME")
    team_abbreviation: str | None = Field(None, alias="TEAM_ABBREVIATION")
    draft_year: int | None = Field(None, alias="DRAFT_YEAR")
    draft_round: int | None = Field(None, alias="DRAFT_ROUND")
    draft_number: int | None = Field(None, alias="DRAFT_NUMBER")
    from_year: int | None = Field(None, alias="FROM_YEAR")
    to_year: int | None = Field(None, alias="TO_YEAR")

    @field_validator("birthdate", mode="before")
    @classmethod
    def parse_birthdate(cls, v: Any) -> date | None:
        """Parse birthdate from various formats."""
        if v is None or v == "" or pd.isna(v):
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            # Try common formats
            for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"]:
                try:
                    return datetime.strptime(v.split("T")[0], fmt.split("T")[0]).date()
                except ValueError:
                    continue
        return None


# =============================================================================
# GAME STATS SCHEMAS
# =============================================================================


class PlayerGameStats(NBABaseModel):
    """Player game statistics schema with cross-field validation."""

    # Identifiers
    game_id: str = Field(
        ...,
        alias="GAME_ID",
        min_length=10,
        max_length=10,
        description="10-digit game ID",
    )
    player_id: int = Field(..., alias="PLAYER_ID", ge=1)
    player_name: str | None = Field(None, alias="PLAYER_NAME")
    team_id: int | None = Field(None, alias="TEAM_ID")
    team_abbreviation: str | None = Field(None, alias="TEAM_ABBREVIATION")

    # Game context
    game_date: date | None = Field(None, alias="GAME_DATE")
    matchup: str | None = Field(None, alias="MATCHUP")
    wl: GameResult | None = Field(None, alias="WL")

    # Playing time
    min: float | None = Field(
        None, alias="MIN", ge=0, le=60, description="Minutes played"
    )

    # Field goals
    fgm: int | None = Field(None, alias="FGM", ge=0, description="Field goals made")
    fga: int | None = Field(
        None, alias="FGA", ge=0, description="Field goals attempted"
    )
    fg_pct: float | None = Field(
        None, alias="FG_PCT", ge=0.0, le=1.0, description="Field goal percentage"
    )

    # Three pointers
    fg3m: int | None = Field(None, alias="FG3M", ge=0, description="3-pointers made")
    fg3a: int | None = Field(
        None, alias="FG3A", ge=0, description="3-pointers attempted"
    )
    fg3_pct: float | None = Field(
        None, alias="FG3_PCT", ge=0.0, le=1.0, description="3-point percentage"
    )

    # Free throws
    ftm: int | None = Field(None, alias="FTM", ge=0, description="Free throws made")
    fta: int | None = Field(
        None, alias="FTA", ge=0, description="Free throws attempted"
    )
    ft_pct: float | None = Field(
        None, alias="FT_PCT", ge=0.0, le=1.0, description="Free throw percentage"
    )

    # Rebounds
    oreb: int | None = Field(None, alias="OREB", ge=0, description="Offensive rebounds")
    dreb: int | None = Field(None, alias="DREB", ge=0, description="Defensive rebounds")
    reb: int | None = Field(None, alias="REB", ge=0, description="Total rebounds")

    # Other stats
    ast: int | None = Field(None, alias="AST", ge=0, description="Assists")
    stl: int | None = Field(None, alias="STL", ge=0, description="Steals")
    blk: int | None = Field(None, alias="BLK", ge=0, description="Blocks")
    tov: int | None = Field(None, alias="TOV", ge=0, description="Turnovers")
    pf: int | None = Field(None, alias="PF", ge=0, le=6, description="Personal fouls")
    pts: int | None = Field(None, alias="PTS", ge=0, le=200, description="Points")
    plus_minus: int | None = Field(None, alias="PLUS_MINUS", description="Plus/minus")

    @field_validator("game_date", mode="before")
    @classmethod
    def parse_game_date(cls, v: Any) -> date | None:
        """Parse game date from various formats."""
        if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            for fmt in ["%Y-%m-%d", "%b %d, %Y", "%m/%d/%Y"]:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue
        return None

    @field_validator("min", mode="before")
    @classmethod
    def parse_minutes(cls, v: Any) -> float | None:
        """Parse minutes from string format (MM:SS) or numeric."""
        if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
            return None
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            if ":" in v:
                parts = v.split(":")
                try:
                    minutes = int(parts[0])
                    seconds = int(parts[1]) if len(parts) > 1 else 0
                    return minutes + seconds / 60.0
                except ValueError:
                    return None
            try:
                return float(v)
            except ValueError:
                return None
        return None

    @model_validator(mode="after")
    def validate_shooting_stats(self) -> "PlayerGameStats":
        """Validate that made shots don't exceed attempts."""
        # Field goals
        if self.fgm is not None and self.fga is not None:
            if self.fgm > self.fga:
                raise ValueError(f"FGM ({self.fgm}) cannot exceed FGA ({self.fga})")

        # Three pointers
        if self.fg3m is not None and self.fg3a is not None:
            if self.fg3m > self.fg3a:
                raise ValueError(f"FG3M ({self.fg3m}) cannot exceed FG3A ({self.fg3a})")

        # Free throws
        if self.ftm is not None and self.fta is not None:
            if self.ftm > self.fta:
                raise ValueError(f"FTM ({self.ftm}) cannot exceed FTA ({self.fta})")

        return self

    @model_validator(mode="after")
    def validate_rebounds(self) -> "PlayerGameStats":
        """Validate rebound totals are consistent."""
        if self.oreb is not None and self.dreb is not None and self.reb is not None:
            expected = self.oreb + self.dreb
            if abs(self.reb - expected) > 2:  # Allow small discrepancy
                raise ValueError(
                    f"REB ({self.reb}) doesn't match OREB ({self.oreb}) + DREB ({self.dreb})"
                )
        return self


class TeamGameStats(NBABaseModel):
    """Team game statistics schema."""

    game_id: str = Field(..., alias="GAME_ID", min_length=10, max_length=10)
    team_id: int = Field(..., alias="TEAM_ID", ge=1)
    team_abbreviation: str | None = Field(None, alias="TEAM_ABBREVIATION")
    team_name: str | None = Field(None, alias="TEAM_NAME")
    game_date: date | None = Field(None, alias="GAME_DATE")
    matchup: str | None = Field(None, alias="MATCHUP")
    wl: GameResult | None = Field(None, alias="WL")

    # Same stats as player but for team totals
    min: int | None = Field(None, alias="MIN", ge=0)
    fgm: int | None = Field(None, alias="FGM", ge=0)
    fga: int | None = Field(None, alias="FGA", ge=0)
    fg_pct: float | None = Field(None, alias="FG_PCT", ge=0.0, le=1.0)
    fg3m: int | None = Field(None, alias="FG3M", ge=0)
    fg3a: int | None = Field(None, alias="FG3A", ge=0)
    fg3_pct: float | None = Field(None, alias="FG3_PCT", ge=0.0, le=1.0)
    ftm: int | None = Field(None, alias="FTM", ge=0)
    fta: int | None = Field(None, alias="FTA", ge=0)
    ft_pct: float | None = Field(None, alias="FT_PCT", ge=0.0, le=1.0)
    oreb: int | None = Field(None, alias="OREB", ge=0)
    dreb: int | None = Field(None, alias="DREB", ge=0)
    reb: int | None = Field(None, alias="REB", ge=0)
    ast: int | None = Field(None, alias="AST", ge=0)
    stl: int | None = Field(None, alias="STL", ge=0)
    blk: int | None = Field(None, alias="BLK", ge=0)
    tov: int | None = Field(None, alias="TOV", ge=0)
    pf: int | None = Field(None, alias="PF", ge=0)
    pts: int | None = Field(None, alias="PTS", ge=0)
    plus_minus: int | None = Field(None, alias="PLUS_MINUS")


# =============================================================================
# BOXSCORE SCHEMAS
# =============================================================================


class BoxScorePlayer(NBABaseModel):
    """Box score player statistics with advanced metrics."""

    game_id: str = Field(..., alias="gameId", min_length=10, max_length=10)
    team_id: int = Field(..., alias="teamId", ge=1)
    person_id: int = Field(..., alias="personId", ge=1)
    name: str | None = Field(None)
    position: str | None = Field(None)

    # Basic stats
    minutes: str | None = Field(None)
    points: int | None = Field(None, ge=0)
    field_goals_made: int | None = Field(None, alias="fieldGoalsMade", ge=0)
    field_goals_attempted: int | None = Field(None, alias="fieldGoalsAttempted", ge=0)
    field_goals_percentage: float | None = Field(
        None, alias="fieldGoalsPercentage", ge=0.0, le=100.0
    )
    three_pointers_made: int | None = Field(None, alias="threePointersMade", ge=0)
    three_pointers_attempted: int | None = Field(
        None, alias="threePointersAttempted", ge=0
    )
    three_pointers_percentage: float | None = Field(
        None, alias="threePointersPercentage", ge=0.0, le=100.0
    )
    free_throws_made: int | None = Field(None, alias="freeThrowsMade", ge=0)
    free_throws_attempted: int | None = Field(None, alias="freeThrowsAttempted", ge=0)
    free_throws_percentage: float | None = Field(
        None, alias="freeThrowsPercentage", ge=0.0, le=100.0
    )

    # Rebounds
    rebounds_offensive: int | None = Field(None, alias="reboundsOffensive", ge=0)
    rebounds_defensive: int | None = Field(None, alias="reboundsDefensive", ge=0)
    rebounds_total: int | None = Field(None, alias="reboundsTotal", ge=0)

    # Other
    assists: int | None = Field(None, ge=0)
    steals: int | None = Field(None, ge=0)
    blocks: int | None = Field(None, ge=0)
    turnovers: int | None = Field(None, ge=0)
    fouls_personal: int | None = Field(None, alias="foulsPersonal", ge=0, le=6)
    plus_minus_points: int | None = Field(None, alias="plusMinusPoints")

    @model_validator(mode="after")
    def validate_shooting(self) -> "BoxScorePlayer":
        """Validate shooting stats consistency."""
        if self.field_goals_made is not None and self.field_goals_attempted is not None:
            if self.field_goals_made > self.field_goals_attempted:
                raise ValueError("FGM cannot exceed FGA")
        return self


# =============================================================================
# PLAY BY PLAY SCHEMAS
# =============================================================================


class PlayByPlayAction(NBABaseModel):
    """Single play-by-play action."""

    game_id: str = Field(..., alias="gameId", min_length=10, max_length=10)
    action_number: int = Field(..., alias="actionNumber", ge=0)
    clock: str | None = Field(None)
    period: int = Field(..., ge=1, le=10, description="Game period (1-4 + OT)")
    team_id: int | None = Field(None, alias="teamId")
    team_tricode: str | None = Field(None, alias="teamTricode")
    person_id: int | None = Field(None, alias="personId")
    player_name: str | None = Field(None, alias="playerName")
    player_name_i: str | None = Field(None, alias="playerNameI")

    # Shot details
    x_legacy: float | None = Field(None, alias="xLegacy")
    y_legacy: float | None = Field(None, alias="yLegacy")
    shot_distance: float | None = Field(None, alias="shotDistance", ge=0)
    shot_result: str | None = Field(None, alias="shotResult")
    is_field_goal: int | None = Field(None, alias="isFieldGoal")
    shot_value: int | None = Field(None, alias="shotValue", ge=0, le=3)

    # Score
    score_home: str | None = Field(None, alias="scoreHome")
    score_away: str | None = Field(None, alias="scoreAway")
    points_total: int | None = Field(None, alias="pointsTotal", ge=0)

    # Action details
    description: str | None = Field(None)
    action_type: str | None = Field(None, alias="actionType")
    sub_type: str | None = Field(None, alias="subType")
    video_available: int | None = Field(None, alias="videoAvailable")


# =============================================================================
# DRAFT SCHEMAS
# =============================================================================


class DraftHistory(NBABaseModel):
    """Draft pick record."""

    person_id: int = Field(..., alias="PERSON_ID", ge=1)
    player_name: str = Field(..., alias="PLAYER_NAME")
    season: str = Field(..., alias="SEASON")
    round_number: int = Field(..., alias="ROUND_NUMBER", ge=1, le=2)
    round_pick: int = Field(..., alias="ROUND_PICK", ge=1)
    overall_pick: int = Field(..., alias="OVERALL_PICK", ge=1)
    team_id: int = Field(..., alias="TEAM_ID", ge=1)
    team_name: str | None = Field(None, alias="TEAM_NAME")
    team_abbreviation: str | None = Field(None, alias="TEAM_ABBREVIATION")
    organization: str | None = Field(None, alias="ORGANIZATION")
    organization_type: str | None = Field(None, alias="ORGANIZATION_TYPE")

    @model_validator(mode="after")
    def validate_pick_numbers(self) -> "DraftHistory":
        """Validate pick numbers are consistent."""
        # Overall pick should roughly equal (round-1)*30 + round_pick
        # But vary due to different draft sizes over years
        if self.overall_pick < self.round_pick:
            raise ValueError(
                f"Overall pick ({self.overall_pick}) cannot be less than round pick ({self.round_pick})"
            )
        return self


class DraftCombineStats(NBABaseModel):
    """Draft combine measurements and stats."""

    player_id: int = Field(..., alias="PLAYER_ID", ge=1)
    player_name: str = Field(..., alias="PLAYER_NAME")
    season: str = Field(..., alias="SEASON")
    position: str | None = Field(None, alias="POSITION")

    # Physical measurements
    height_wo_shoes: float | None = Field(None, alias="HEIGHT_WO_SHOES", ge=60, le=100)
    height_w_shoes: float | None = Field(None, alias="HEIGHT_W_SHOES", ge=60, le=100)
    weight: float | None = Field(None, alias="WEIGHT", ge=100, le=400)
    wingspan: float | None = Field(None, alias="WINGSPAN", ge=60, le=110)
    standing_reach: float | None = Field(None, alias="STANDING_REACH", ge=80, le=130)
    body_fat_pct: float | None = Field(None, alias="BODY_FAT_PCT", ge=0, le=50)
    hand_length: float | None = Field(None, alias="HAND_LENGTH", ge=5, le=15)
    hand_width: float | None = Field(None, alias="HAND_WIDTH", ge=5, le=15)

    # Athletic tests
    standing_vertical_leap: float | None = Field(
        None, alias="STANDING_VERTICAL_LEAP", ge=0, le=50
    )
    max_vertical_leap: float | None = Field(
        None, alias="MAX_VERTICAL_LEAP", ge=0, le=50
    )
    lane_agility_time: float | None = Field(
        None, alias="LANE_AGILITY_TIME", ge=8, le=20
    )
    three_quarter_sprint: float | None = Field(
        None, alias="THREE_QUARTER_SPRINT", ge=2, le=5
    )
    bench_press: int | None = Field(None, alias="BENCH_PRESS", ge=0, le=50)


# =============================================================================
# VALIDATION UTILITIES
# =============================================================================


def validate_with_schema(
    df: pd.DataFrame,
    schema: type[NBABaseModel],
    raise_on_error: bool = False,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Validate a DataFrame using a Pydantic schema.

    Args:
        df: DataFrame to validate
        schema: Pydantic model class to use for validation
        raise_on_error: If True, raise on first validation error

    Returns:
        Tuple of (valid_df, errors_list) where errors_list contains
        details about each failed validation
    """
    return schema.validate_dataframe(df, raise_on_error=raise_on_error)


def get_schema_for_table(table_name: str) -> type[NBABaseModel] | None:
    """Get the appropriate schema for a table name.

    Args:
        table_name: Name of the database table

    Returns:
        Pydantic schema class or None if no schema defined
    """
    schema_map: dict[str, type[NBABaseModel]] = {
        "player": Player,
        "players": Player,
        "player_silver": Player,
        "common_player_info": CommonPlayerInfo,
        "common_player_info_silver": CommonPlayerInfo,
        "player_game_stats": PlayerGameStats,
        "player_game_stats_silver": PlayerGameStats,
        "player_game_stats_raw": PlayerGameStats,
        "team_game_stats": TeamGameStats,
        "team_game_stats_silver": TeamGameStats,
        "team_game_stats_raw": TeamGameStats,
        "play_by_play": PlayByPlayAction,
        "play_by_play_silver": PlayByPlayAction,
        "play_by_play_raw": PlayByPlayAction,
        "draft_history": DraftHistory,
        "draft_history_silver": DraftHistory,
        "draft_history_raw": DraftHistory,
        "draft_combine_stats": DraftCombineStats,
        "draft_combine_stats_silver": DraftCombineStats,
        "draft_combine_stats_raw": DraftCombineStats,
        "boxscores": BoxScorePlayer,
        "boxscores_raw": BoxScorePlayer,
    }
    return schema_map.get(table_name.lower())
