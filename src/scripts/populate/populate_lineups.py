#!/usr/bin/env python3
"""Populate lineup_stats table from NBA API LeagueDashLineups endpoint.

This script fetches lineup combination statistics for different group sizes:
- 2-man lineups: Two-player combinations
- 3-man lineups: Three-player combinations
- 4-man lineups: Four-player combinations
- 5-man lineups: Full five-player lineups

Each lineup can be fetched with different measure types:
- Base: Basic counting stats
- Advanced: Advanced metrics (ratings, pace, etc.)
- Misc: Miscellaneous stats
- Four Factors: Four factors analysis
- Scoring: Scoring breakdown
- Opponent: Opponent stats
- Usage: Usage statistics
- Defense: Defensive stats

Usage:
    # Full population (recent seasons, all group quantities)
    python scripts/populate/populate_lineups.py

    # Specific seasons only
    python scripts/populate/populate_lineups.py --seasons 2024-25 2023-24

    # Only 5-man lineups
    python scripts/populate/populate_lineups.py --group-quantity 5

    # Multiple group quantities
    python scripts/populate/populate_lineups.py --group-quantity 4 5

    # Regular season only
    python scripts/populate/populate_lineups.py --regular-season-only

    # Specific measure types only
    python scripts/populate/populate_lineups.py --measure-types Base Advanced

    # Dry run
    python scripts/populate/populate_lineups.py --dry-run
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


class GroupQuantity(int, Enum):
    """Lineup group sizes."""

    TWO_MAN = 2
    THREE_MAN = 3
    FOUR_MAN = 4
    FIVE_MAN = 5


class MeasureType(str, Enum):
    """Measure type categories for lineup stats."""

    BASE = "Base"
    ADVANCED = "Advanced"
    MISC = "Misc"
    FOUR_FACTORS = "Four Factors"
    SCORING = "Scoring"
    OPPONENT = "Opponent"
    USAGE = "Usage"
    DEFENSE = "Defense"


# Default group quantities to iterate
GROUP_QUANTITIES: list[int] = [gq.value for gq in GroupQuantity]

# Default measure types to iterate
MEASURE_TYPES: list[str] = [mt.value for mt in MeasureType]


# Expected columns for lineup_stats table
LINEUP_STATS_COLUMNS: list[str] = [
    # Key columns
    "season_id",
    "season_type",
    "group_id",
    "team_id",
    # Group info
    "group_name",
    "team_abbreviation",
    "group_quantity",
    "measure_type",
    # Basic counting stats
    "games_played",
    "wins",
    "losses",
    "w_pct",
    "minutes",
    # Shooting stats
    "fgm",
    "fga",
    "fg_pct",
    "fg3m",
    "fg3a",
    "fg3_pct",
    "ftm",
    "fta",
    "ft_pct",
    # Rebounding
    "oreb",
    "dreb",
    "reb",
    # Other counting stats
    "ast",
    "tov",
    "stl",
    "blk",
    "blka",
    "pf",
    "pfd",
    "pts",
    # Plus/Minus
    "plus_minus",
    # Advanced stats
    "off_rating",
    "def_rating",
    "net_rating",
    "ast_pct",
    "ast_to",
    "ast_ratio",
    "oreb_pct",
    "dreb_pct",
    "reb_pct",
    "tov_pct",
    "efg_pct",
    "ts_pct",
    "pace",
    "pie",
    "poss",
    # Metadata
    "filename",
]


# =============================================================================
# PYDANTIC SCHEMAS FOR VALIDATION
# =============================================================================


class LineupStats(BaseModel):
    """Pydantic schema for lineup statistics validation."""

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        coerce_numbers_to_str=False,
        str_strip_whitespace=True,
    )

    # Key identifiers
    group_id: str = Field(
        ...,
        description="Unique lineup group identifier",
    )
    team_id: int = Field(
        ...,
        description="Team ID",
    )
    group_name: str | None = Field(
        None,
        description="Player names in the lineup",
    )
    team_abbreviation: str | None = Field(
        None,
        description="Team abbreviation",
    )
    group_quantity: int = Field(
        ...,
        ge=2,
        le=5,
        description="Number of players in lineup",
    )

    # Games and results
    games_played: int | None = Field(
        None,
        ge=0,
        description="Games played",
    )
    wins: int | None = Field(
        None,
        ge=0,
        description="Wins",
    )
    losses: int | None = Field(
        None,
        ge=0,
        description="Losses",
    )
    w_pct: float | None = Field(
        None,
        ge=0,
        le=1,
        description="Win percentage",
    )
    minutes: float | None = Field(
        None,
        ge=0,
        description="Minutes played",
    )

    # Shooting stats
    fgm: int | None = Field(None, ge=0, description="Field goals made")
    fga: int | None = Field(None, ge=0, description="Field goals attempted")
    fg_pct: float | None = Field(None, ge=0, le=1, description="Field goal percentage")
    fg3m: int | None = Field(None, ge=0, description="Three-pointers made")
    fg3a: int | None = Field(None, ge=0, description="Three-pointers attempted")
    fg3_pct: float | None = Field(
        None, ge=0, le=1, description="Three-point percentage"
    )
    ftm: int | None = Field(None, ge=0, description="Free throws made")
    fta: int | None = Field(None, ge=0, description="Free throws attempted")
    ft_pct: float | None = Field(None, ge=0, le=1, description="Free throw percentage")

    # Ratings
    off_rating: float | None = Field(
        None,
        description="Offensive rating (points per 100 possessions)",
    )
    def_rating: float | None = Field(
        None,
        description="Defensive rating (points allowed per 100 possessions)",
    )
    net_rating: float | None = Field(
        None,
        description="Net rating (off_rating - def_rating)",
    )
    plus_minus: float | None = Field(
        None,
        description="Plus/minus",
    )
    pace: float | None = Field(
        None,
        ge=0,
        description="Pace (possessions per 48 minutes)",
    )

    @field_validator("off_rating", "def_rating", "net_rating", mode="before")
    @classmethod
    def validate_rating(cls, v: Any) -> float | None:
        """Validate ratings are within reasonable bounds."""
        if v is None or pd.isna(v):
            return None
        val = float(v)
        # Ratings typically between 70 and 150
        if val < 0 or val > 200:
            return None
        return val

    @field_validator("fg_pct", "fg3_pct", "ft_pct", mode="before")
    @classmethod
    def validate_percentage(cls, v: Any) -> float | None:
        """Validate percentage fields."""
        if v is None or pd.isna(v):
            return None
        val = float(v)
        # If > 1, assume it's in percentage form (0-100)
        if val > 1:
            val = val / 100
        return val if 0 <= val <= 1 else None


# =============================================================================
# POPULATOR CLASS
# =============================================================================


class LineupStatsPopulator(BasePopulator, SeasonIteratorMixin):
    """Populator for lineup_stats table."""

    def __init__(self, **kwargs) -> None:
        """Initialize the LineupStatsPopulator.

        Args:
            **kwargs: Arguments passed to BasePopulator.
        """
        super().__init__(**kwargs)
        self._fetched_progress_keys: list[str] = []

        # Filter options (can be set before run)
        self.group_quantities: list[int] = GROUP_QUANTITIES
        self.measure_types: list[str] = MEASURE_TYPES

    def get_table_name(self) -> str:
        """Return target table name."""
        return "lineup_stats"

    def get_key_columns(self) -> list[str]:
        """Return composite primary key columns."""
        return [
            "season_id",
            "season_type",
            "group_id",
            "team_id",
            "measure_type",
        ]

    def get_expected_columns(self) -> list[str]:
        """Return expected column names for validation."""
        return LINEUP_STATS_COLUMNS

    def get_data_type(self) -> str:
        """Return data type for validation."""
        return "lineup_stats"

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        """Fetch lineup statistics for all combinations.

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
            * len(self.group_quantities)
            * len(self.measure_types)
        )

        logger.info(
            "Fetching lineup stats for %d combinations "
            "(%d seasons x %d season types x %d group quantities x %d measure types)",
            total_combinations,
            len(seasons),
            len(season_types),
            len(self.group_quantities),
            len(self.measure_types),
        )

        processed = 0
        for season in seasons:
            for season_type in season_types:
                for group_quantity in self.group_quantities:
                    for measure_type in self.measure_types:
                        processed += 1
                        progress_key = self._build_progress_key(
                            season,
                            season_type,
                            group_quantity,
                            measure_type,
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
                            "[%d/%d] Fetching %s %s - %d-man lineups (%s)...",
                            processed,
                            total_combinations,
                            season,
                            season_type,
                            group_quantity,
                            measure_type,
                        )

                        try:
                            df = self.client.get_lineup_stats(
                                season=season,
                                season_type=season_type,
                                group_quantity=group_quantity,
                                measure_type=measure_type,
                            )

                            if df is not None and not df.empty:
                                # Add metadata columns
                                df["_season"] = season
                                df["_season_type"] = season_type
                                df["_group_quantity"] = group_quantity
                                df["_measure_type"] = measure_type

                                all_data.append(df)
                                logger.info(
                                    "  Found %d lineup records for %d-man (%s)",
                                    len(df),
                                    group_quantity,
                                    measure_type,
                                )
                                self.metrics.api_calls += 1
                                self._fetched_progress_keys.append(progress_key)
                            else:
                                logger.debug(
                                    "  No data for %s %s - %d-man (%s)",
                                    season,
                                    season_type,
                                    group_quantity,
                                    measure_type,
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
                                    "group_quantity": group_quantity,
                                    "measure_type": measure_type,
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
        group_quantity: int,
        measure_type: str,
    ) -> str:
        """Build a unique progress key for tracking."""
        return f"{season}_{season_type}_{group_quantity}man_{measure_type}"

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform lineup stats data to match schema.

        Args:
            df: Raw DataFrame from API.
            **kwargs: Additional parameters.

        Returns:
            Transformed DataFrame matching expected schema.
        """
        if df.empty:
            return df

        output = pd.DataFrame()

        # Extract season_id from API or metadata
        # API returns SEASON_ID like "22024" for 2024-25
        output["season_id"] = df.get("SEASON_ID", df.get("_season"))
        output["season_type"] = df["_season_type"]

        # Group identifiers
        output["group_id"] = df["GROUP_ID"].astype(str)
        output["group_name"] = df.get("GROUP_NAME", "")
        output["team_id"] = pd.to_numeric(df.get("TEAM_ID"), errors="coerce").astype(
            "Int64"
        )
        output["team_abbreviation"] = df.get("TEAM_ABBREVIATION", "")

        # Metadata from fetch
        output["group_quantity"] = df["_group_quantity"].astype("Int64")
        output["measure_type"] = df["_measure_type"]

        # Games and results
        output["games_played"] = pd.to_numeric(df.get("GP"), errors="coerce").astype(
            "Int64"
        )
        output["wins"] = pd.to_numeric(df.get("W"), errors="coerce").astype("Int64")
        output["losses"] = pd.to_numeric(df.get("L"), errors="coerce").astype("Int64")
        output["w_pct"] = pd.to_numeric(df.get("W_PCT"), errors="coerce")
        output["minutes"] = pd.to_numeric(df.get("MIN"), errors="coerce")

        # Shooting stats
        output["fgm"] = pd.to_numeric(df.get("FGM"), errors="coerce").astype("Int64")
        output["fga"] = pd.to_numeric(df.get("FGA"), errors="coerce").astype("Int64")
        output["fg_pct"] = pd.to_numeric(df.get("FG_PCT"), errors="coerce")
        output["fg3m"] = pd.to_numeric(df.get("FG3M"), errors="coerce").astype("Int64")
        output["fg3a"] = pd.to_numeric(df.get("FG3A"), errors="coerce").astype("Int64")
        output["fg3_pct"] = pd.to_numeric(df.get("FG3_PCT"), errors="coerce")
        output["ftm"] = pd.to_numeric(df.get("FTM"), errors="coerce").astype("Int64")
        output["fta"] = pd.to_numeric(df.get("FTA"), errors="coerce").astype("Int64")
        output["ft_pct"] = pd.to_numeric(df.get("FT_PCT"), errors="coerce")

        # Rebounding
        output["oreb"] = pd.to_numeric(df.get("OREB"), errors="coerce").astype("Int64")
        output["dreb"] = pd.to_numeric(df.get("DREB"), errors="coerce").astype("Int64")
        output["reb"] = pd.to_numeric(df.get("REB"), errors="coerce").astype("Int64")

        # Other counting stats
        output["ast"] = pd.to_numeric(df.get("AST"), errors="coerce").astype("Int64")
        output["tov"] = pd.to_numeric(df.get("TOV"), errors="coerce").astype("Int64")
        output["stl"] = pd.to_numeric(df.get("STL"), errors="coerce").astype("Int64")
        output["blk"] = pd.to_numeric(df.get("BLK"), errors="coerce").astype("Int64")
        output["blka"] = pd.to_numeric(df.get("BLKA"), errors="coerce").astype("Int64")
        output["pf"] = pd.to_numeric(df.get("PF"), errors="coerce").astype("Int64")
        output["pfd"] = pd.to_numeric(df.get("PFD"), errors="coerce").astype("Int64")
        output["pts"] = pd.to_numeric(df.get("PTS"), errors="coerce").astype("Int64")

        # Plus/Minus
        output["plus_minus"] = pd.to_numeric(df.get("PLUS_MINUS"), errors="coerce")

        # Advanced stats
        output["off_rating"] = pd.to_numeric(df.get("OFF_RATING"), errors="coerce")
        output["def_rating"] = pd.to_numeric(df.get("DEF_RATING"), errors="coerce")
        output["net_rating"] = pd.to_numeric(df.get("NET_RATING"), errors="coerce")
        output["ast_pct"] = pd.to_numeric(df.get("AST_PCT"), errors="coerce")
        output["ast_to"] = pd.to_numeric(df.get("AST_TO"), errors="coerce")
        output["ast_ratio"] = pd.to_numeric(df.get("AST_RATIO"), errors="coerce")
        output["oreb_pct"] = pd.to_numeric(df.get("OREB_PCT"), errors="coerce")
        output["dreb_pct"] = pd.to_numeric(df.get("DREB_PCT"), errors="coerce")
        output["reb_pct"] = pd.to_numeric(df.get("REB_PCT"), errors="coerce")
        output["tov_pct"] = pd.to_numeric(df.get("TOV_PCT"), errors="coerce")
        output["efg_pct"] = pd.to_numeric(df.get("EFG_PCT"), errors="coerce")
        output["ts_pct"] = pd.to_numeric(df.get("TS_PCT"), errors="coerce")
        output["pace"] = pd.to_numeric(df.get("PACE"), errors="coerce")
        output["pie"] = pd.to_numeric(df.get("PIE"), errors="coerce")
        output["poss"] = pd.to_numeric(df.get("POSS"), errors="coerce")

        # Metadata
        output["filename"] = "nba_api.leaguedashlineups"

        # Ensure all expected columns exist
        for col in LINEUP_STATS_COLUMNS:
            if col not in output.columns:
                output[col] = None

        # Reorder columns to match expected schema
        return output[LINEUP_STATS_COLUMNS]

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


def populate_lineup_stats(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    season_types: list[str] | None = None,
    group_quantities: list[int] | None = None,
    measure_types: list[str] | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate lineup_stats table.

    Args:
        db_path: Path to DuckDB database.
        seasons: List of seasons to fetch (e.g., ["2024-25", "2023-24"]).
        season_types: List of season types (e.g., ["Regular Season", "Playoffs"]).
        group_quantities: List of lineup sizes (e.g., [2, 3, 4, 5]).
        measure_types: List of measure types (e.g., ["Base", "Advanced"]).
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
    logger.info("NBA LINEUP STATS POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Seasons: {len(seasons)} ({seasons[0]} to {seasons[-1]})")
    logger.info(f"Season Types: {season_types}")
    logger.info(f"Group Quantities: {group_quantities or GROUP_QUANTITIES}")
    logger.info(f"Measure Types: {measure_types or MEASURE_TYPES}")
    logger.info(f"Request Delay: {delay}s")

    # Create and configure populator
    populator = LineupStatsPopulator(
        db_path=db_path,
        client=client,
    )

    # Apply filter options
    if group_quantities:
        populator.group_quantities = group_quantities
    if measure_types:
        populator.measure_types = measure_types

    return populator.run(
        seasons=seasons,
        season_types=season_types,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def main() -> None:
    """Parse command-line arguments and run the lineup stats population."""
    parser = argparse.ArgumentParser(
        description="Populate lineup_stats table from NBA API LeagueDashLineups endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (last 5 seasons, all lineups)
  python scripts/populate/populate_lineups.py

  # Specific seasons only
  python scripts/populate/populate_lineups.py --seasons 2024-25 2023-24

  # Regular season only
  python scripts/populate/populate_lineups.py --regular-season-only

  # Only 5-man lineups
  python scripts/populate/populate_lineups.py --group-quantity 5

  # Multiple group quantities
  python scripts/populate/populate_lineups.py --group-quantity 4 5

  # Only Base and Advanced measure types
  python scripts/populate/populate_lineups.py --measure-types Base Advanced

  # Reset progress and start fresh
  python scripts/populate/populate_lineups.py --reset-progress

  # Dry run (no database writes)
  python scripts/populate/populate_lineups.py --dry-run
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
        "--group-quantity",
        nargs="+",
        type=int,
        choices=[2, 3, 4, 5],
        help="Lineup group sizes to fetch (2, 3, 4, 5)",
    )
    parser.add_argument(
        "--measure-types",
        nargs="+",
        choices=[mt.value for mt in MeasureType],
        help="Measure types to fetch",
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

    try:
        stats = populate_lineup_stats(
            db_path=args.db,
            seasons=args.seasons,
            season_types=season_types,
            group_quantities=args.group_quantity,
            measure_types=args.measure_types,
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
