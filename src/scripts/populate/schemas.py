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
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


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

    person_id: int = Field(
        ...,
        validation_alias=AliasChoices("person_id", "player_id", "id"),
        ge=1,
        description="NBA player ID",
    )
    full_name: str = Field(..., min_length=1, description="Full player name")
    first_name: str | None = Field(None, description="First name")
    last_name: str | None = Field(None, description="Last name")
    is_active: bool = Field(True, description="Whether player is currently active")

    @model_validator(mode="before")
    @classmethod
    def populate_full_name(cls, data: Any) -> Any:
        """Populate full_name from first/last when missing."""
        if isinstance(data, dict):
            full_name = data.get("full_name")
            if not full_name:
                first = data.get("first_name")
                last = data.get("last_name")
                if first or last:
                    data["full_name"] = " ".join(
                        part for part in [first, last] if part
                    ).strip()
        return data

    @field_validator("full_name", mode="before")
    @classmethod
    def validate_name(cls, v: Any) -> str:
        """Ensure name is non-empty string."""
        if v is None or (isinstance(v, str) and not v.strip()):
            raise ValueError("Player name cannot be empty")
        return str(v).strip()

    @property
    def player_id(self) -> int:
        """Backward-compatible accessor for person_id."""
        return self.person_id


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
    round_pick: int = Field(..., alias="ROUND_PICK", gt=0)
    overall_pick: int = Field(..., alias="OVERALL_PICK", gt=0)
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
# SYNERGY PLAY TYPE SCHEMAS
# =============================================================================


class SynergyPlayTypeStats(NBABaseModel):
    """Synergy play type efficiency data."""

    # Required identifiers
    season_id: str = Field(..., alias="SEASON_ID", description="Season identifier")
    season_type: str = Field(
        ...,
        alias="SEASON_TYPE",
        description="Season type (Regular Season, Playoffs, etc.)",
    )
    entity_type: str = Field(
        ...,
        alias="ENTITY_TYPE",
        description="Entity type (player or team)",
    )
    entity_id: int = Field(
        ...,
        validation_alias=AliasChoices("ENTITY_ID", "PLAYER_ID", "TEAM_ID"),
        ge=1,
        description="Player or team ID",
    )
    play_type: str = Field(
        ...,
        alias="PLAY_TYPE",
        description="Type of play (Isolation, PnR Ball Handler, etc.)",
    )
    type_grouping: str = Field(
        ...,
        alias="TYPE_GROUPING",
        description="Grouping category (Offensive, Defensive)",
    )

    # Optional identifiers
    entity_name: str | None = Field(
        None,
        validation_alias=AliasChoices("ENTITY_NAME", "PLAYER_NAME", "TEAM_NAME"),
    )
    team_id: int | None = Field(None, alias="TEAM_ID", ge=1)
    team_abbreviation: str | None = Field(None, alias="TEAM_ABBREVIATION")

    # Efficiency stats
    ppp: float | None = Field(
        None,
        alias="PPP",
        ge=0.0,
        le=3.0,
        description="Points per possession",
    )
    percentile: float | None = Field(
        None,
        alias="PERCENTILE",
        ge=0.0,
        le=100.0,
        description="League percentile ranking",
    )
    possessions: float | None = Field(
        None, alias="POSS", ge=0, description="Number of possessions"
    )
    pts: float | None = Field(None, alias="PTS", ge=0, description="Total points")
    fgm: float | None = Field(None, alias="FGM", ge=0, description="Field goals made")
    fga: float | None = Field(
        None, alias="FGA", ge=0, description="Field goals attempted"
    )
    fg_pct: float | None = Field(
        None,
        alias="FG_PCT",
        ge=0.0,
        le=1.0,
        description="Field goal percentage",
    )
    efg_pct: float | None = Field(
        None,
        alias="EFG_PCT",
        ge=0.0,
        le=1.0,
        description="Effective field goal percentage",
    )
    freq_pct: float | None = Field(
        None,
        alias="FREQ_PCT",
        ge=0.0,
        le=1.0,
        description="Frequency percentage",
    )

    # Additional stats
    score_freq: float | None = Field(None, alias="SCORE_FREQ", ge=0.0, le=1.0)
    to_freq: float | None = Field(None, alias="TO_FREQ", ge=0.0, le=1.0)
    foul_freq: float | None = Field(None, alias="FOUL_FREQ", ge=0.0, le=1.0)
    and_one_freq: float | None = Field(None, alias="AND_ONE_FREQ", ge=0.0, le=1.0)

    @model_validator(mode="after")
    def validate_shooting_consistency(self) -> "SynergyPlayTypeStats":
        """Validate shooting stats are consistent."""
        if self.fgm is not None and self.fga is not None and self.fga > 0:
            if self.fgm > self.fga:
                raise ValueError(f"FGM ({self.fgm}) cannot exceed FGA ({self.fga})")
        return self


# =============================================================================
# LINEUP SCHEMAS
# =============================================================================


class LineupStats(NBABaseModel):
    """Lineup combination statistics."""

    # Required identifiers
    season_id: str = Field(
        ...,
        validation_alias=AliasChoices("SEASON_ID", "season_id"),
        description="Season identifier",
    )
    season_type: str = Field(
        ...,
        validation_alias=AliasChoices("SEASON_TYPE", "season_type"),
        description="Season type",
    )
    group_id: str = Field(
        ...,
        alias="GROUP_ID",
        description="Unique identifier for the lineup combination",
    )
    team_id: int = Field(..., alias="TEAM_ID", ge=1, description="Team ID")

    # Lineup info
    group_quantity: int = Field(
        ...,
        alias="GROUP_QUANTITY",
        ge=2,
        le=5,
        description="Number of players in lineup (2-5)",
    )
    team_abbreviation: str | None = Field(None, alias="TEAM_ABBREVIATION")
    group_name: str | None = Field(None, alias="GROUP_NAME")

    # Playing time
    minutes: float | None = Field(None, alias="MIN", ge=0, description="Minutes played")
    gp: int | None = Field(None, alias="GP", ge=0, description="Games played")

    # Record
    wins: int | None = Field(None, alias="W", ge=0, description="Wins")
    losses: int | None = Field(None, alias="L", ge=0, description="Losses")
    w_pct: float | None = Field(None, alias="W_PCT", ge=0.0, le=1.0)

    # Ratings
    off_rating: float | None = Field(
        None,
        alias="OFF_RATING",
        ge=50.0,
        le=200.0,
        description="Offensive rating",
    )
    def_rating: float | None = Field(
        None,
        alias="DEF_RATING",
        ge=50.0,
        le=200.0,
        description="Defensive rating",
    )
    net_rating: float | None = Field(
        None,
        alias="NET_RATING",
        ge=-100.0,
        le=100.0,
        description="Net rating",
    )
    plus_minus: float | None = Field(None, alias="PLUS_MINUS", description="Plus/minus")

    # Basic stats
    fgm: float | None = Field(None, alias="FGM", ge=0)
    fga: float | None = Field(None, alias="FGA", ge=0)
    fg_pct: float | None = Field(None, alias="FG_PCT", ge=0.0, le=1.0)
    fg3m: float | None = Field(None, alias="FG3M", ge=0)
    fg3a: float | None = Field(None, alias="FG3A", ge=0)
    fg3_pct: float | None = Field(None, alias="FG3_PCT", ge=0.0, le=1.0)
    ftm: float | None = Field(None, alias="FTM", ge=0)
    fta: float | None = Field(None, alias="FTA", ge=0)
    ft_pct: float | None = Field(None, alias="FT_PCT", ge=0.0, le=1.0)
    oreb: float | None = Field(None, alias="OREB", ge=0)
    dreb: float | None = Field(None, alias="DREB", ge=0)
    reb: float | None = Field(None, alias="REB", ge=0)
    ast: float | None = Field(None, alias="AST", ge=0)
    tov: float | None = Field(None, alias="TOV", ge=0)
    stl: float | None = Field(None, alias="STL", ge=0)
    blk: float | None = Field(None, alias="BLK", ge=0)
    pf: float | None = Field(None, alias="PF", ge=0)
    pts: float | None = Field(None, alias="PTS", ge=0)

    @model_validator(mode="after")
    def validate_net_rating(self) -> "LineupStats":
        """Validate net rating is consistent with off/def ratings."""
        if (
            self.off_rating is not None
            and self.def_rating is not None
            and self.net_rating is not None
        ):
            expected_net = self.off_rating - self.def_rating
            if abs(self.net_rating - expected_net) > 1.0:  # Allow small discrepancy
                raise ValueError(
                    f"NET_RATING ({self.net_rating}) doesn't match "
                    f"OFF_RATING ({self.off_rating}) - DEF_RATING ({self.def_rating})"
                )
        return self


# =============================================================================
# SHOT CHART SCHEMAS
# =============================================================================


class ShotChartDetail(NBABaseModel):
    """Shot location data."""

    # Required identifiers
    game_id: str = Field(..., alias="GAME_ID", min_length=10, max_length=10)
    player_id: int = Field(..., alias="PLAYER_ID", ge=1)
    period: int = Field(
        ...,
        alias="PERIOD",
        ge=1,
        le=10,
        description="Game period (1-4 regular, 5+ OT)",
    )

    # Player/Team info
    player_name: str | None = Field(None, alias="PLAYER_NAME")
    team_id: int | None = Field(None, alias="TEAM_ID", ge=1)
    team_name: str | None = Field(None, alias="TEAM_NAME")

    # Game context
    game_date: date | None = Field(None, alias="GAME_DATE")
    htm: str | None = Field(None, alias="HTM", description="Home team")
    vtm: str | None = Field(None, alias="VTM", description="Visiting team")

    # Shot location
    loc_x: int = Field(
        ...,
        alias="LOC_X",
        ge=-250,
        le=250,
        description="X coordinate (-250 to 250)",
    )
    loc_y: int = Field(
        ...,
        alias="LOC_Y",
        ge=-50,
        le=900,
        description="Y coordinate (-50 to 900)",
    )
    shot_distance: int | None = Field(
        None,
        alias="SHOT_DISTANCE",
        ge=0,
        le=100,
        description="Distance from basket in feet",
    )

    # Shot info
    shot_type: str | None = Field(
        None,
        alias="SHOT_TYPE",
        description="Type of shot (2PT/3PT)",
    )
    shot_zone_basic: str | None = Field(
        None,
        alias="SHOT_ZONE_BASIC",
        description="Basic zone (Restricted Area, Paint, etc.)",
    )
    shot_zone_area: str | None = Field(
        None,
        alias="SHOT_ZONE_AREA",
        description="Area of court (Center, Left Side, etc.)",
    )
    shot_zone_range: str | None = Field(
        None,
        alias="SHOT_ZONE_RANGE",
        description="Distance range",
    )
    shot_made_flag: int | None = Field(
        None,
        alias="SHOT_MADE_FLAG",
        ge=0,
        le=1,
        description="1 if made, 0 if missed",
    )
    action_type: str | None = Field(None, alias="ACTION_TYPE")
    shot_attempted_flag: int | None = Field(
        None, alias="SHOT_ATTEMPTED_FLAG", ge=0, le=1
    )

    # Time context
    minutes_remaining: int | None = Field(None, alias="MINUTES_REMAINING", ge=0, le=12)
    seconds_remaining: int | None = Field(None, alias="SECONDS_REMAINING", ge=0, le=59)
    event_type: str | None = Field(None, alias="EVENT_TYPE")
    game_event_id: int | None = Field(None, alias="GAME_EVENT_ID")

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
            for fmt in ["%Y-%m-%d", "%Y%m%d", "%b %d, %Y", "%m/%d/%Y"]:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue
        return None


# =============================================================================
# GAME ROTATION SCHEMAS
# =============================================================================


class GameRotation(NBABaseModel):
    """Player substitution patterns."""

    # Required identifiers
    game_id: str = Field(..., alias="GAME_ID", min_length=10, max_length=10)
    team_id: int = Field(..., alias="TEAM_ID", ge=1)
    person_id: int = Field(..., alias="PERSON_ID", ge=1)
    stint_number: int = Field(
        ...,
        alias="STINT_NUMBER",
        ge=1,
        description="Sequential stint number",
    )

    # Player/Team info
    player_name: str | None = Field(None, alias="PLAYER_NAME")
    team_city: str | None = Field(None, alias="TEAM_CITY")
    team_name: str | None = Field(None, alias="TEAM_NAME")
    team_abbreviation: str | None = Field(None, alias="TEAM_ABBREVIATION")

    # Time info (in tenths of seconds from game start)
    in_time_real: float = Field(
        ...,
        alias="IN_TIME_REAL",
        ge=0,
        description="Check-in time (tenths of seconds)",
    )
    out_time_real: float = Field(
        ...,
        alias="OUT_TIME_REAL",
        ge=0,
        description="Check-out time (tenths of seconds)",
    )
    stint_duration: float | None = Field(
        None,
        description="Duration of stint in tenths of seconds",
    )

    # Period info
    in_period: int | None = Field(None, alias="IN_PERIOD", ge=1, le=10)
    out_period: int | None = Field(None, alias="OUT_PERIOD", ge=1, le=10)

    # Stats during stint
    player_pts: int | None = Field(
        None,
        alias="PLAYER_PTS",
        ge=0,
        description="Points scored by player during stint",
    )
    pt_diff: int | None = Field(
        None,
        alias="PT_DIFF",
        description="Point differential during stint",
    )
    player_pts_off_tot: int | None = Field(None, alias="PLAYER_PTS_OFF_TOT", ge=0)
    player_pts_def_tot: int | None = Field(None, alias="PLAYER_PTS_DEF_TOT", ge=0)

    @model_validator(mode="after")
    def validate_times(self) -> "GameRotation":
        """Validate time fields are consistent."""
        if self.in_time_real > self.out_time_real:
            raise ValueError(
                f"in_time_real ({self.in_time_real}) cannot be greater than "
                f"out_time_real ({self.out_time_real})"
            )
        # Calculate stint duration if not provided
        if self.stint_duration is None:
            object.__setattr__(
                self,
                "stint_duration",
                self.out_time_real - self.in_time_real,
            )
        return self


# =============================================================================
# PLAYER SPLITS SCHEMAS
# =============================================================================


class PlayerSplits(NBABaseModel):
    """Player dashboard splits data."""

    # Required identifiers
    season_id: str = Field(
        ...,
        validation_alias=AliasChoices("SEASON_ID", "season_id"),
        description="Season identifier",
    )
    season_type: str = Field(
        ...,
        validation_alias=AliasChoices("SEASON_TYPE", "season_type"),
        description="Season type",
    )
    player_id: int = Field(..., alias="PLAYER_ID", ge=1)
    split_type: str = Field(
        ...,
        alias="SPLIT_TYPE",
        description="Type of split (Location, WinsLosses, Month, etc.)",
    )
    split_category: str = Field(
        ...,
        alias="SPLIT_CATEGORY",
        description="Category within split type",
    )
    split_value: str = Field(
        ...,
        alias="SPLIT_VALUE",
        description="Specific value of the split",
    )

    # Player info
    player_name: str | None = Field(None, alias="PLAYER_NAME")
    team_id: int | None = Field(None, alias="TEAM_ID", ge=1)
    team_abbreviation: str | None = Field(None, alias="TEAM_ABBREVIATION")

    # Game stats
    gp: int | None = Field(None, alias="GP", ge=0, description="Games played")
    wins: int | None = Field(None, alias="W", ge=0, description="Wins")
    losses: int | None = Field(None, alias="L", ge=0, description="Losses")
    w_pct: float | None = Field(None, alias="W_PCT", ge=0.0, le=1.0)
    minutes: float | None = Field(None, alias="MIN", ge=0, description="Minutes played")

    # Scoring stats
    pts: float | None = Field(None, alias="PTS", ge=0, description="Points per game")
    ast: float | None = Field(None, alias="AST", ge=0, description="Assists per game")
    reb: float | None = Field(None, alias="REB", ge=0, description="Rebounds per game")
    stl: float | None = Field(None, alias="STL", ge=0, description="Steals per game")
    blk: float | None = Field(None, alias="BLK", ge=0, description="Blocks per game")
    tov: float | None = Field(None, alias="TOV", ge=0, description="Turnovers per game")

    # Shooting percentages
    fg_pct: float | None = Field(
        None, alias="FG_PCT", ge=0.0, le=1.0, description="Field goal percentage"
    )
    fg3_pct: float | None = Field(
        None, alias="FG3_PCT", ge=0.0, le=1.0, description="3-point percentage"
    )
    ft_pct: float | None = Field(
        None, alias="FT_PCT", ge=0.0, le=1.0, description="Free throw percentage"
    )

    # Additional shooting stats
    fgm: float | None = Field(None, alias="FGM", ge=0)
    fga: float | None = Field(None, alias="FGA", ge=0)
    fg3m: float | None = Field(None, alias="FG3M", ge=0)
    fg3a: float | None = Field(None, alias="FG3A", ge=0)
    ftm: float | None = Field(None, alias="FTM", ge=0)
    fta: float | None = Field(None, alias="FTA", ge=0)
    oreb: float | None = Field(None, alias="OREB", ge=0)
    dreb: float | None = Field(None, alias="DREB", ge=0)
    pf: float | None = Field(None, alias="PF", ge=0)
    plus_minus: float | None = Field(None, alias="PLUS_MINUS")


# =============================================================================
# ESTIMATED METRICS SCHEMAS
# =============================================================================


class EstimatedMetrics(NBABaseModel):
    """Estimated player/team metrics."""

    # Required identifiers
    season_id: str = Field(
        ...,
        validation_alias=AliasChoices("SEASON_ID", "season_id"),
        description="Season identifier",
    )
    season_type: str = Field(
        ...,
        validation_alias=AliasChoices("SEASON_TYPE", "season_type"),
        description="Season type",
    )
    entity_type: str = Field(
        ...,
        alias="ENTITY_TYPE",
        description="Entity type (player or team)",
    )
    entity_id: int = Field(
        ...,
        validation_alias=AliasChoices("ENTITY_ID", "PLAYER_ID", "TEAM_ID"),
        ge=1,
        description="Player or team ID",
    )

    # Entity info
    entity_name: str | None = Field(
        None,
        validation_alias=AliasChoices("ENTITY_NAME", "PLAYER_NAME", "TEAM_NAME"),
    )
    team_id: int | None = Field(None, alias="TEAM_ID", ge=1)
    team_abbreviation: str | None = Field(None, alias="TEAM_ABBREVIATION")

    # Estimated metrics
    e_off_rating: float | None = Field(
        None,
        alias="E_OFF_RATING",
        ge=50.0,
        le=200.0,
        description="Estimated offensive rating",
    )
    e_def_rating: float | None = Field(
        None,
        alias="E_DEF_RATING",
        ge=50.0,
        le=200.0,
        description="Estimated defensive rating",
    )
    e_net_rating: float | None = Field(
        None,
        alias="E_NET_RATING",
        ge=-100.0,
        le=100.0,
        description="Estimated net rating",
    )
    e_pace: float | None = Field(
        None,
        alias="E_PACE",
        ge=80.0,
        le=130.0,
        description="Estimated pace",
    )
    e_pie: float | None = Field(
        None,
        alias="E_PIE",
        ge=0.0,
        le=1.0,
        description="Estimated Player Impact Estimate",
    )

    # Additional metrics
    e_ast_ratio: float | None = Field(None, alias="E_AST_RATIO", ge=0)
    e_oreb_pct: float | None = Field(None, alias="E_OREB_PCT", ge=0.0, le=1.0)
    e_dreb_pct: float | None = Field(None, alias="E_DREB_PCT", ge=0.0, le=1.0)
    e_reb_pct: float | None = Field(None, alias="E_REB_PCT", ge=0.0, le=1.0)
    e_tov_pct: float | None = Field(None, alias="E_TOV_PCT", ge=0.0, le=1.0)
    e_usg_pct: float | None = Field(None, alias="E_USG_PCT", ge=0.0, le=1.0)

    # Games info
    gp: int | None = Field(None, alias="GP", ge=0)
    w: int | None = Field(None, alias="W", ge=0)
    l: int | None = Field(None, alias="L", ge=0)
    w_pct: float | None = Field(None, alias="W_PCT", ge=0.0, le=1.0)
    min: float | None = Field(None, alias="MIN", ge=0)


# =============================================================================
# WIN PROBABILITY SCHEMAS
# =============================================================================


class WinProbability(NBABaseModel):
    """Win probability data."""

    # Required identifiers
    game_id: str = Field(..., alias="GAME_ID", min_length=10, max_length=10)
    event_num: int = Field(
        ...,
        alias="EVENT_NUM",
        ge=0,
        description="Event sequence number",
    )

    # Probabilities
    home_pct: float = Field(
        ...,
        alias="HOME_PCT",
        ge=0.0,
        le=1.0,
        description="Home team win probability",
    )
    visitor_pct: float = Field(
        ...,
        alias="VISITOR_PCT",
        ge=0.0,
        le=1.0,
        description="Visitor team win probability",
    )

    # Game state
    period: int | None = Field(None, alias="PERIOD", ge=1, le=10)
    seconds_remaining: int | None = Field(
        None,
        alias="SECONDS_REMAINING",
        ge=0,
        description="Seconds remaining in period",
    )
    home_pts: int | None = Field(None, alias="HOME_PTS", ge=0)
    visitor_pts: int | None = Field(None, alias="VISITOR_PTS", ge=0)

    # Team info
    home_team_id: int | None = Field(None, alias="HOME_TEAM_ID", ge=1)
    visitor_team_id: int | None = Field(None, alias="VISITOR_TEAM_ID", ge=1)
    home_team_abbreviation: str | None = Field(None, alias="HOME_TEAM_ABBREVIATION")
    visitor_team_abbreviation: str | None = Field(
        None, alias="VISITOR_TEAM_ABBREVIATION"
    )

    # Event info
    description: str | None = Field(None, alias="DESCRIPTION")
    location: str | None = Field(None, alias="LOCATION")

    @model_validator(mode="after")
    def validate_probabilities(self) -> "WinProbability":
        """Validate probabilities sum to approximately 1."""
        total = self.home_pct + self.visitor_pct
        if abs(total - 1.0) > 0.05:  # Allow 5% tolerance
            raise ValueError(f"Win probabilities should sum to ~1.0, got {total:.3f}")
        return self


# =============================================================================
# LEAGUE LEADERS SCHEMAS
# =============================================================================


class LeagueLeaders(NBABaseModel):
    """League statistical leaders."""

    # Required identifiers
    season_id: str = Field(
        ...,
        validation_alias=AliasChoices("SEASON_ID", "season_id"),
        description="Season identifier",
    )
    season_type: str = Field(
        ...,
        validation_alias=AliasChoices("SEASON_TYPE", "season_type"),
        description="Season type",
    )
    stat_category: str = Field(
        ...,
        alias="STAT_CATEGORY",
        description="Statistical category (PTS, AST, REB, etc.)",
    )
    player_id: int = Field(..., alias="PLAYER_ID", ge=1)

    # Player info
    player_name: str | None = Field(None, alias="PLAYER_NAME")
    team_id: int | None = Field(None, alias="TEAM_ID", ge=1)
    team_abbreviation: str | None = Field(None, alias="TEAM_ABBREVIATION")

    # Ranking info
    rank: int = Field(..., alias="RANK", ge=1, description="League rank")
    stat_value: float = Field(
        ...,
        alias="STAT_VALUE",
        description="Value of the statistic",
    )

    # Games info
    games_played: int | None = Field(None, alias="GP", ge=0, description="Games played")
    minutes: float | None = Field(None, alias="MIN", ge=0, description="Minutes played")

    # Additional context
    min_per_game: float | None = Field(None, alias="MIN_PG", ge=0)
    fgm: float | None = Field(None, alias="FGM", ge=0)
    fga: float | None = Field(None, alias="FGA", ge=0)
    fg_pct: float | None = Field(None, alias="FG_PCT", ge=0.0, le=1.0)
    fg3m: float | None = Field(None, alias="FG3M", ge=0)
    fg3a: float | None = Field(None, alias="FG3A", ge=0)
    fg3_pct: float | None = Field(None, alias="FG3_PCT", ge=0.0, le=1.0)
    ftm: float | None = Field(None, alias="FTM", ge=0)
    fta: float | None = Field(None, alias="FTA", ge=0)
    ft_pct: float | None = Field(None, alias="FT_PCT", ge=0.0, le=1.0)
    oreb: float | None = Field(None, alias="OREB", ge=0)
    dreb: float | None = Field(None, alias="DREB", ge=0)
    reb: float | None = Field(None, alias="REB", ge=0)
    ast: float | None = Field(None, alias="AST", ge=0)
    stl: float | None = Field(None, alias="STL", ge=0)
    blk: float | None = Field(None, alias="BLK", ge=0)
    tov: float | None = Field(None, alias="TOV", ge=0)
    pts: float | None = Field(None, alias="PTS", ge=0)
    eff: float | None = Field(None, alias="EFF")


# =============================================================================
# FRANCHISE HISTORY SCHEMAS
# =============================================================================


class FranchiseHistory(NBABaseModel):
    """Franchise history data."""

    # Required identifiers
    team_id: int = Field(..., alias="TEAM_ID", ge=1)
    start_year: int = Field(
        ...,
        alias="START_YEAR",
        ge=1946,
        description="First year of franchise",
    )

    # Franchise info
    team_city: str | None = Field(None, alias="TEAM_CITY")
    team_name: str | None = Field(None, alias="TEAM_NAME")
    league_id: str | None = Field(None, alias="LEAGUE_ID")

    # Years active
    end_year: int | None = Field(None, alias="END_YEAR", ge=1946)
    years_active: int | None = Field(
        None,
        alias="YEARS",
        ge=0,
        description="Number of years active",
    )

    # Historical stats
    games: int | None = Field(None, alias="GAMES", ge=0, description="Total games")
    wins: int | None = Field(None, alias="WINS", ge=0, description="Total wins")
    losses: int | None = Field(None, alias="LOSSES", ge=0, description="Total losses")
    w_pct: float | None = Field(None, alias="WIN_PCT", ge=0.0, le=1.0)

    # Playoff stats
    po_appearances: int | None = Field(
        None, alias="PO_APPEARANCES", ge=0, description="Playoff appearances"
    )
    div_titles: int | None = Field(
        None, alias="DIV_TITLES", ge=0, description="Division titles"
    )
    conf_titles: int | None = Field(
        None, alias="CONF_TITLES", ge=0, description="Conference titles"
    )
    championships: int | None = Field(
        None, alias="LEAGUE_TITLES", ge=0, description="Championships won"
    )

    @model_validator(mode="after")
    def validate_years(self) -> "FranchiseHistory":
        """Validate year fields are consistent."""
        if self.end_year is not None and self.end_year < self.start_year:
            raise ValueError(
                f"end_year ({self.end_year}) cannot be before "
                f"start_year ({self.start_year})"
            )
        return self

    @model_validator(mode="after")
    def validate_record(self) -> "FranchiseHistory":
        """Validate wins/losses match games."""
        if self.wins is not None and self.losses is not None and self.games is not None:
            total = self.wins + self.losses
            if abs(self.games - total) > 10:  # Allow some tolerance for ties/cancelled
                raise ValueError(
                    f"Games ({self.games}) should roughly equal "
                    f"wins ({self.wins}) + losses ({self.losses})"
                )
        return self


# =============================================================================
# BASKETBALL REFERENCE SCHEDULE SCHEMAS
# =============================================================================


class BRSchedule(NBABaseModel):
    """Basketball Reference schedule data."""

    # Required identifiers
    game_key: str = Field(
        ...,
        alias="game_key",
        description="Unique game identifier",
    )
    season_year: int = Field(
        ...,
        alias="season_year",
        ge=1946,
        description="Season year",
    )
    game_date: date = Field(
        ...,
        alias="game_date",
        description="Date of the game",
    )
    home_team: str = Field(
        ...,
        alias="home_team",
        min_length=3,
        max_length=3,
        description="Home team abbreviation",
    )
    away_team: str = Field(
        ...,
        alias="away_team",
        min_length=3,
        max_length=3,
        description="Away team abbreviation",
    )

    # Scores (optional - may be None for future games)
    home_team_score: int | None = Field(
        None,
        alias="home_team_score",
        ge=0,
        le=300,
        description="Home team final score",
    )
    away_team_score: int | None = Field(
        None,
        alias="away_team_score",
        ge=0,
        le=300,
        description="Away team final score",
    )

    # Additional info
    overtime: str | None = Field(
        None,
        alias="overtime",
        description="OT indicator if game went to overtime",
    )
    attendance: int | None = Field(None, alias="attendance", ge=0)
    arena: str | None = Field(None, alias="arena")
    game_remarks: str | None = Field(None, alias="game_remarks")

    # Game timing
    start_time: str | None = Field(None, alias="start_time")

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
            for fmt in ["%Y-%m-%d", "%b %d, %Y", "%m/%d/%Y", "%B %d, %Y"]:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue
        return None


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


def validate_dataframe(
    df: pd.DataFrame,
    schema: type[NBABaseModel],
    raise_on_error: bool = False,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Validate a DataFrame using a Pydantic schema."""
    return validate_with_schema(df, schema, raise_on_error=raise_on_error)


def get_schema_for_table(table_name: str) -> type[NBABaseModel] | None:
    """Get the appropriate schema for a table name.

    Args:
        table_name: Name of the database table

    Returns:
        Pydantic schema class or None if no schema defined
    """
    schema_map: dict[str, type[NBABaseModel]] = {
        # Player schemas
        "player": Player,
        "players": Player,
        "player_silver": Player,
        "common_player_info": CommonPlayerInfo,
        "common_player_info_silver": CommonPlayerInfo,
        # Game stats schemas
        "player_game_stats": PlayerGameStats,
        "player_game_stats_silver": PlayerGameStats,
        "player_game_stats_raw": PlayerGameStats,
        "team_game_stats": TeamGameStats,
        "team_game_stats_silver": TeamGameStats,
        "team_game_stats_raw": TeamGameStats,
        # Play by play schemas
        "play_by_play": PlayByPlayAction,
        "play_by_play_silver": PlayByPlayAction,
        "play_by_play_raw": PlayByPlayAction,
        # Draft schemas
        "draft_history": DraftHistory,
        "draft_history_silver": DraftHistory,
        "draft_history_raw": DraftHistory,
        "draft_combine_stats": DraftCombineStats,
        "draft_combine_stats_silver": DraftCombineStats,
        "draft_combine_stats_raw": DraftCombineStats,
        # Boxscore schemas
        "boxscores": BoxScorePlayer,
        "boxscores_raw": BoxScorePlayer,
        # Synergy play type schemas
        "synergy_playtypes": SynergyPlayTypeStats,
        "synergy_playtypes_raw": SynergyPlayTypeStats,
        "synergy_playtypes_silver": SynergyPlayTypeStats,
        "synergy_play_type_stats": SynergyPlayTypeStats,
        # Lineup schemas
        "lineups": LineupStats,
        "lineups_raw": LineupStats,
        "lineups_silver": LineupStats,
        "lineup_stats": LineupStats,
        # Shot chart schemas
        "shot_chart": ShotChartDetail,
        "shot_chart_raw": ShotChartDetail,
        "shot_chart_silver": ShotChartDetail,
        "shot_chart_detail": ShotChartDetail,
        # Game rotation schemas
        "game_rotations": GameRotation,
        "game_rotations_raw": GameRotation,
        "game_rotations_silver": GameRotation,
        "game_rotation": GameRotation,
        # Player splits schemas
        "player_splits": PlayerSplits,
        "player_splits_raw": PlayerSplits,
        "player_splits_silver": PlayerSplits,
        "player_dashboard_splits": PlayerSplits,
        # Estimated metrics schemas
        "estimated_metrics": EstimatedMetrics,
        "estimated_metrics_raw": EstimatedMetrics,
        "estimated_metrics_silver": EstimatedMetrics,
        # Win probability schemas
        "win_probability": WinProbability,
        "win_probability_raw": WinProbability,
        "win_probability_silver": WinProbability,
        # League leaders schemas
        "league_leaders": LeagueLeaders,
        "league_leaders_raw": LeagueLeaders,
        "league_leaders_silver": LeagueLeaders,
        # Franchise history schemas
        "franchise_history": FranchiseHistory,
        "franchise_history_raw": FranchiseHistory,
        "franchise_history_silver": FranchiseHistory,
        # Basketball Reference schedule schemas
        "br_schedule": BRSchedule,
        "br_schedule_raw": BRSchedule,
        "br_schedule_silver": BRSchedule,
        "basketball_reference_schedule": BRSchedule,
    }
    return schema_map.get(table_name.lower())
