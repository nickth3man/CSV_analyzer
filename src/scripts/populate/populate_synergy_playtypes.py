#!/usr/bin/env python3
"""Populate synergy_playtypes table from NBA API SynergyPlayTypes endpoint.

This script fetches Synergy Play Type efficiency data including:
- Isolation
- Transition
- Pick & Roll Ball Handler (PRBallHandler)
- Pick & Roll Roll Man (PRRollman)
- Post Up
- Spot Up
- Handoff
- Cut
- Off Screen
- Putbacks
- Misc

Each play type can be fetched for:
- Offensive or defensive stats (type_grouping_nullable)
- Player or team level (player_or_team_abbreviation)

Usage:
    # Full population (recent seasons)
    python scripts/populate/populate_synergy_playtypes.py

    # Specific seasons only
    python scripts/populate/populate_synergy_playtypes.py --seasons 2024-25 2023-24

    # Regular season only
    python scripts/populate/populate_synergy_playtypes.py --regular-season-only

    # Players only (no team stats)
    python scripts/populate/populate_synergy_playtypes.py --players-only

    # Offensive stats only
    python scripts/populate/populate_synergy_playtypes.py --offensive-only

    # Dry run
    python scripts/populate/populate_synergy_playtypes.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from enum import Enum
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.scripts.populate.api_client import get_client
from src.scripts.populate.base import BasePopulator, SeasonIteratorMixin
from src.scripts.populate.config import ALL_SEASONS, DEFAULT_SEASON_TYPES, get_db_path
from src.scripts.populate.helpers import configure_logging, resolve_season_types


configure_logging()
logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================


class PlayType(str, Enum):
    """Synergy play type categories."""

    ISOLATION = "Isolation"
    TRANSITION = "Transition"
    PR_BALL_HANDLER = "PRBallHandler"
    PR_ROLLMAN = "PRRollman"
    POSTUP = "Postup"
    SPOTUP = "Spotup"
    HANDOFF = "Handoff"
    CUT = "Cut"
    OFF_SCREEN = "OffScreen"
    PUTBACKS = "Putbacks"
    MISC = "Misc"


class TypeGrouping(str, Enum):
    """Offensive or defensive grouping."""

    OFFENSIVE = "offensive"
    DEFENSIVE = "defensive"


class PlayerOrTeam(str, Enum):
    """Player or team level stats."""

    PLAYER = "P"
    TEAM = "T"


# All play types to iterate
PLAY_TYPES: list[str] = [pt.value for pt in PlayType]

# Type groupings
TYPE_GROUPINGS: list[str] = [tg.value for tg in TypeGrouping]

# Player/Team options
PLAYER_OR_TEAM_OPTIONS: list[str] = [pot.value for pot in PlayerOrTeam]


# Expected columns for synergy_playtypes table
SYNERGY_PLAYTYPES_COLUMNS: list[str] = [
    # Key columns
    "season_id",
    "season_type",
    "player_id",
    "team_id",
    "play_type",
    "type_grouping",
    "player_or_team",
    # Identification
    "player_name",
    "team_abbreviation",
    "team_name",
    # Efficiency metrics
    "gp",
    "poss",
    "poss_pct",
    "pts",
    "fgm",
    "fga",
    "fg_pct",
    "efg_pct",
    "ft_poss_pct",
    "tov_poss_pct",
    "sf_poss_pct",
    "plusone_poss_pct",
    "score_poss_pct",
    "ppp",
    "percentile",
    # Metadata
    "filename",
]


# =============================================================================
# PYDANTIC SCHEMAS FOR VALIDATION
# =============================================================================


class SynergyPlayTypeStats(BaseModel):
    """Pydantic schema for Synergy Play Type statistics validation."""

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        coerce_numbers_to_str=False,
        str_strip_whitespace=True,
    )

    # Key identifiers - use generic since it can be player_id or team_id
    player_id: int | None = Field(
        None,
        description="Player ID (only for player-level stats)",
    )
    team_id: int | None = Field(
        None,
        description="Team ID",
    )
    player_name: str | None = Field(
        None,
        description="Player name (only for player-level stats)",
    )
    team_abbreviation: str | None = Field(
        None,
        description="Team abbreviation",
    )
    team_name: str | None = Field(
        None,
        description="Team name",
    )

    # Games and possessions
    gp: int | None = Field(
        None,
        ge=0,
        description="Games played",
    )
    poss: float | None = Field(
        None,
        ge=0,
        description="Total possessions",
    )
    poss_pct: float | None = Field(
        None,
        ge=0,
        le=100,
        description="Possession percentage",
    )

    # Scoring
    pts: float | None = Field(
        None,
        ge=0,
        description="Total points",
    )
    fgm: float | None = Field(
        None,
        ge=0,
        description="Field goals made",
    )
    fga: float | None = Field(
        None,
        ge=0,
        description="Field goals attempted",
    )
    fg_pct: float | None = Field(
        None,
        ge=0,
        le=1,
        description="Field goal percentage",
    )
    efg_pct: float | None = Field(
        None,
        ge=0,
        le=1,
        description="Effective field goal percentage",
    )

    # Possession outcomes
    ft_poss_pct: float | None = Field(
        None,
        ge=0,
        le=100,
        description="Free throw possession percentage",
    )
    tov_poss_pct: float | None = Field(
        None,
        ge=0,
        le=100,
        description="Turnover possession percentage",
    )
    sf_poss_pct: float | None = Field(
        None,
        ge=0,
        le=100,
        description="Shooting foul possession percentage",
    )
    plusone_poss_pct: float | None = Field(
        None,
        ge=0,
        le=100,
        description="And-one possession percentage",
    )
    score_poss_pct: float | None = Field(
        None,
        ge=0,
        le=100,
        description="Score possession percentage",
    )

    # Efficiency
    ppp: float | None = Field(
        None,
        ge=0,
        le=3,
        description="Points per possession",
    )
    percentile: float | None = Field(
        None,
        ge=0,
        le=100,
        description="Percentile ranking",
    )

    @field_validator("ppp", mode="before")
    @classmethod
    def validate_ppp(cls, v: Any) -> float | None:
        """Validate points per possession is reasonable."""
        if v is None or pd.isna(v):
            return None
        val = float(v)
        # PPP should typically be between 0 and 2, but allow up to 3 for edge cases
        if val < 0 or val > 3:
            return None
        return val

    @field_validator("fg_pct", "efg_pct", mode="before")
    @classmethod
    def validate_percentages(cls, v: Any) -> float | None:
        """Convert percentage to decimal if needed."""
        if v is None or pd.isna(v):
            return None
        val = float(v)
        # If > 1, assume it's in percentage form
        if val > 1:
            val = val / 100
        return val


# =============================================================================
# POPULATOR CLASS
# =============================================================================


class SynergyPlayTypesPopulator(BasePopulator, SeasonIteratorMixin):
    """Populator for synergy_playtypes table."""

    def __init__(self, **kwargs) -> None:
        """Initialize the SynergyPlayTypesPopulator.

        Args:
            **kwargs: Arguments passed to BasePopulator.
        """
        super().__init__(**kwargs)
        self._fetched_progress_keys: list[str] = []

        # Filter options (can be set before run)
        self.play_types: list[str] = PLAY_TYPES
        self.type_groupings: list[str] = TYPE_GROUPINGS
        self.player_or_team_options: list[str] = PLAYER_OR_TEAM_OPTIONS

    def get_table_name(self) -> str:
        """Return target table name."""
        return "synergy_playtypes"

    def get_key_columns(self) -> list[str]:
        """Return composite primary key columns."""
        return [
            "season_id",
            "season_type",
            "player_id",
            "team_id",
            "play_type",
            "type_grouping",
            "player_or_team",
        ]

    def get_expected_columns(self) -> list[str]:
        """Return expected column names for validation."""
        return SYNERGY_PLAYTYPES_COLUMNS

    def get_data_type(self) -> str:
        """Return data type for validation."""
        return "synergy_playtypes"

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        """Fetch Synergy Play Type data for all combinations.

        Args:
            **kwargs: Fetch parameters including:
                - seasons: List of seasons to fetch
                - season_types: List of season types
                - resume: Whether to skip completed items

        Returns:
            Combined DataFrame with all fetched data or None.
        """
        seasons: list[str] = kwargs.get("seasons") or ALL_SEASONS[:5]
        season_types: list[str] = kwargs.get("season_types") or DEFAULT_SEASON_TYPES
        resume = kwargs.get("resume", True)

        all_data: list[pd.DataFrame] = []

        # Calculate total combinations for progress tracking
        total_combinations = (
            len(seasons)
            * len(season_types)
            * len(self.play_types)
            * len(self.type_groupings)
            * len(self.player_or_team_options)
        )

        logger.info(
            "Fetching Synergy Play Type data for %d combinations "
            "(%d seasons x %d season types x %d play types x %d type groupings x %d entity types)",
            total_combinations,
            len(seasons),
            len(season_types),
            len(self.play_types),
            len(self.type_groupings),
            len(self.player_or_team_options),
        )

        processed = 0
        for season in seasons:
            for season_type in season_types:
                for play_type in self.play_types:
                    for type_grouping in self.type_groupings:
                        for player_or_team in self.player_or_team_options:
                            processed += 1
                            progress_key = self._build_progress_key(
                                season,
                                season_type,
                                play_type,
                                type_grouping,
                                player_or_team,
                            )

                            # Check if already completed
                            if resume and self.progress.is_completed(progress_key):
                                logger.debug(
                                    "[%d/%d] Skipping %s (already completed)",
                                    processed,
                                    total_combinations,
                                    progress_key,
                                )
                                continue

                            logger.info(
                                "[%d/%d] Fetching %s %s - %s (%s, %s)...",
                                processed,
                                total_combinations,
                                season,
                                season_type,
                                play_type,
                                type_grouping,
                                "Player" if player_or_team == "P" else "Team",
                            )

                            try:
                                df = self.client.get_synergy_playtypes(
                                    season=season,
                                    season_type=season_type,
                                    play_type=play_type,
                                    type_grouping=type_grouping,
                                    player_or_team=player_or_team,
                                )

                                if df is not None and not df.empty:
                                    # Add metadata columns
                                    df["_season"] = season
                                    df["_season_type"] = season_type
                                    df["_play_type"] = play_type
                                    df["_type_grouping"] = type_grouping
                                    df["_player_or_team"] = player_or_team

                                    all_data.append(df)
                                    logger.info(
                                        "  Found %d records for %s (%s)",
                                        len(df),
                                        play_type,
                                        type_grouping,
                                    )
                                    self.metrics.api_calls += 1
                                    self._fetched_progress_keys.append(progress_key)
                                else:
                                    logger.debug(
                                        "  No data for %s %s - %s (%s, %s)",
                                        season,
                                        season_type,
                                        play_type,
                                        type_grouping,
                                        player_or_team,
                                    )

                                # Respect rate limiting
                                time.sleep(self.client.config.request_delay)

                            except Exception as e:
                                logger.exception(
                                    "Error fetching %s: %s",
                                    progress_key,
                                    e,
                                )
                                self.progress.add_error(progress_key, str(e))
                                self.metrics.add_error(
                                    str(e),
                                    {
                                        "season": season,
                                        "season_type": season_type,
                                        "play_type": play_type,
                                        "type_grouping": type_grouping,
                                        "player_or_team": player_or_team,
                                    },
                                )

        if not all_data:
            logger.info("No data fetched")
            return None

        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info("Total records fetched: %d", len(combined_df))
        return combined_df

    def _build_progress_key(
        self,
        season: str,
        season_type: str,
        play_type: str,
        type_grouping: str,
        player_or_team: str,
    ) -> str:
        """Build a unique progress key for tracking."""
        return f"{season}_{season_type}_{play_type}_{type_grouping}_{player_or_team}"

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform Synergy Play Type data to match schema.

        Args:
            df: Raw DataFrame from API.
            **kwargs: Additional parameters.

        Returns:
            Transformed DataFrame matching expected schema.
        """
        if df.empty:
            return df

        output = pd.DataFrame()

        # Extract metadata from added columns
        output["season_id"] = df.get("SEASON_ID", df.get("_season"))
        output["season_type"] = df["_season_type"]
        output["play_type"] = df["_play_type"]
        output["type_grouping"] = df["_type_grouping"]
        output["player_or_team"] = df["_player_or_team"]

        # Player/Team identifiers
        # For player stats: PLAYER_ID, PLAYER_NAME
        # For team stats: TEAM_ID, TEAM_NAME, TEAM_ABBREVIATION
        output["player_id"] = pd.to_numeric(
            df.get("PLAYER_ID"), errors="coerce"
        ).astype("Int64")
        output["team_id"] = pd.to_numeric(df.get("TEAM_ID"), errors="coerce").astype(
            "Int64"
        )
        output["player_name"] = df.get("PLAYER_NAME", "")
        output["team_abbreviation"] = df.get("TEAM_ABBREVIATION", "")
        output["team_name"] = df.get("TEAM_NAME", "")

        # Games played
        output["gp"] = pd.to_numeric(df.get("GP"), errors="coerce").astype("Int64")

        # Possessions
        output["poss"] = pd.to_numeric(df.get("POSS"), errors="coerce")
        output["poss_pct"] = pd.to_numeric(df.get("POSS_PCT"), errors="coerce")

        # Scoring
        output["pts"] = pd.to_numeric(df.get("PTS"), errors="coerce")
        output["fgm"] = pd.to_numeric(df.get("FGM"), errors="coerce")
        output["fga"] = pd.to_numeric(df.get("FGA"), errors="coerce")
        output["fg_pct"] = pd.to_numeric(df.get("FG_PCT"), errors="coerce")
        output["efg_pct"] = pd.to_numeric(df.get("EFG_PCT"), errors="coerce")

        # Possession outcomes
        output["ft_poss_pct"] = pd.to_numeric(df.get("FT_POSS_PCT"), errors="coerce")
        output["tov_poss_pct"] = pd.to_numeric(df.get("TOV_POSS_PCT"), errors="coerce")
        output["sf_poss_pct"] = pd.to_numeric(df.get("SF_POSS_PCT"), errors="coerce")
        output["plusone_poss_pct"] = pd.to_numeric(
            df.get("PLUSONE_POSS_PCT"), errors="coerce"
        )
        output["score_poss_pct"] = pd.to_numeric(
            df.get("SCORE_POSS_PCT"), errors="coerce"
        )

        # Efficiency metrics
        output["ppp"] = pd.to_numeric(df.get("PPP"), errors="coerce")
        output["percentile"] = pd.to_numeric(df.get("PERCENTILE"), errors="coerce")

        # Metadata
        output["filename"] = "nba_api.synergyplaytypes"

        # Ensure all expected columns exist
        for col in SYNERGY_PLAYTYPES_COLUMNS:
            if col not in output.columns:
                output[col] = None

        # Reorder columns to match expected schema
        return output[SYNERGY_PLAYTYPES_COLUMNS]

    def pre_run_hook(self, **kwargs) -> None:
        """Reset fetched keys for this run."""
        self._fetched_progress_keys = []

    def post_run_hook(self, **kwargs) -> None:
        """Mark fetched items as completed after successful database writes."""
        dry_run = kwargs.get("dry_run", False)
        if dry_run:
            logger.info(
                "DRY RUN - not marking progress for fetched items (data was not written)"
            )
            return

        if self._fetched_progress_keys:
            for progress_key in self._fetched_progress_keys:
                self.progress.mark_completed(progress_key)
            self.progress.save()
            logger.info(
                "Marked %d combinations as completed",
                len(self._fetched_progress_keys),
            )


# =============================================================================
# MAIN FUNCTIONS
# =============================================================================


def populate_synergy_playtypes(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    season_types: list[str] | None = None,
    play_types: list[str] | None = None,
    type_groupings: list[str] | None = None,
    player_or_team_options: list[str] | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate synergy_playtypes table.

    Args:
        db_path: Path to DuckDB database.
        seasons: List of seasons to fetch (e.g., ["2024-25", "2023-24"]).
        season_types: List of season types (e.g., ["Regular Season", "Playoffs"]).
        play_types: List of play types to fetch (defaults to all).
        type_groupings: List of type groupings (e.g., ["offensive", "defensive"]).
        player_or_team_options: List of entity types (e.g., ["P", "T"]).
        delay: Delay between API requests in seconds.
        reset_progress: Reset progress tracking before starting.
        dry_run: If True, don't actually insert data.

    Returns:
        Dictionary with population statistics.
    """
    db_path = db_path or str(get_db_path())
    seasons = seasons or ALL_SEASONS[:5]  # Default: last 5 seasons
    season_types = season_types or DEFAULT_SEASON_TYPES

    # Create client with custom delay
    client = get_client()
    client.config.request_delay = delay

    logger.info("=" * 70)
    logger.info("NBA SYNERGY PLAY TYPES POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Seasons: {len(seasons)} ({seasons[0]} to {seasons[-1]})")
    logger.info(f"Season Types: {season_types}")
    logger.info(f"Play Types: {len(play_types or PLAY_TYPES)} types")
    logger.info(f"Type Groupings: {type_groupings or TYPE_GROUPINGS}")
    logger.info(f"Player/Team: {player_or_team_options or PLAYER_OR_TEAM_OPTIONS}")
    logger.info(f"Request Delay: {delay}s")

    # Create and configure populator
    populator = SynergyPlayTypesPopulator(
        db_path=db_path,
        client=client,
    )

    # Apply filter options
    if play_types:
        populator.play_types = play_types
    if type_groupings:
        populator.type_groupings = type_groupings
    if player_or_team_options:
        populator.player_or_team_options = player_or_team_options

    return populator.run(
        seasons=seasons,
        season_types=season_types,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def main() -> None:
    """Parse command-line arguments and run the Synergy Play Types population."""
    parser = argparse.ArgumentParser(
        description="Populate synergy_playtypes table from NBA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (last 5 seasons)
  python scripts/populate/populate_synergy_playtypes.py

  # Specific seasons only
  python scripts/populate/populate_synergy_playtypes.py --seasons 2024-25 2023-24

  # Regular season only
  python scripts/populate/populate_synergy_playtypes.py --regular-season-only

  # Players only (skip team stats)
  python scripts/populate/populate_synergy_playtypes.py --players-only

  # Teams only (skip player stats)
  python scripts/populate/populate_synergy_playtypes.py --teams-only

  # Offensive stats only
  python scripts/populate/populate_synergy_playtypes.py --offensive-only

  # Defensive stats only
  python scripts/populate/populate_synergy_playtypes.py --defensive-only

  # Specific play types only
  python scripts/populate/populate_synergy_playtypes.py --play-types Isolation Transition

  # Reset progress and start fresh
  python scripts/populate/populate_synergy_playtypes.py --reset-progress

  # Dry run (no database writes)
  python scripts/populate/populate_synergy_playtypes.py --dry-run
        """,
    )

    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database (default: src/backend/data/nba.duckdb)",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        help="Seasons to fetch (e.g., 2024-25 2023-24)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.6,
        help="Delay between API requests in seconds (default: 0.6)",
    )
    parser.add_argument(
        "--regular-season-only",
        action="store_true",
        help="Only fetch regular season stats",
    )
    parser.add_argument(
        "--playoffs-only",
        action="store_true",
        help="Only fetch playoff stats",
    )
    parser.add_argument(
        "--players-only",
        action="store_true",
        help="Only fetch player-level stats (skip team stats)",
    )
    parser.add_argument(
        "--teams-only",
        action="store_true",
        help="Only fetch team-level stats (skip player stats)",
    )
    parser.add_argument(
        "--offensive-only",
        action="store_true",
        help="Only fetch offensive stats",
    )
    parser.add_argument(
        "--defensive-only",
        action="store_true",
        help="Only fetch defensive stats",
    )
    parser.add_argument(
        "--play-types",
        nargs="+",
        choices=[pt.value for pt in PlayType],
        help="Specific play types to fetch",
    )
    parser.add_argument(
        "--reset-progress",
        action="store_true",
        help="Reset progress tracking before starting",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually write to database",
    )

    args = parser.parse_args()

    # Determine season types
    season_types = resolve_season_types(
        DEFAULT_SEASON_TYPES,
        regular_only=args.regular_season_only,
        playoffs_only=args.playoffs_only,
    )

    # Determine type groupings
    type_groupings: list[str] | None = None
    if args.offensive_only:
        type_groupings = [TypeGrouping.OFFENSIVE.value]
    elif args.defensive_only:
        type_groupings = [TypeGrouping.DEFENSIVE.value]

    # Determine player/team options
    player_or_team_options: list[str] | None = None
    if args.players_only:
        player_or_team_options = [PlayerOrTeam.PLAYER.value]
    elif args.teams_only:
        player_or_team_options = [PlayerOrTeam.TEAM.value]

    try:
        stats = populate_synergy_playtypes(
            db_path=args.db,
            seasons=args.seasons,
            season_types=season_types,
            play_types=args.play_types,
            type_groupings=type_groupings,
            player_or_team_options=player_or_team_options,
            delay=args.delay,
            reset_progress=args.reset_progress,
            dry_run=args.dry_run,
        )

        if stats.get("error_count", 0) > 0:
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
