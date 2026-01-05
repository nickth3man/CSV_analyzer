#!/usr/bin/env python3
"""Populate league_leaders and all_time_leaders tables from NBA API.

This script fetches league leaders statistics across multiple stat categories
and modes, plus all-time historical leaders across NBA history.

League Leaders categories:
- PTS, AST, REB, STL, BLK, FGM, FG3M, FTM, EFF, MIN

All-Time Leaders categories:
- GP, PTS, AST, REB, STL, BLK, FGM, FGA, FG3M, FG3A, FTM, FTA, OREB, DREB, TOV, PF

Usage:
    # Full population (recent seasons)
    python scripts/populate/populate_league_leaders.py

    # Specific seasons only
    python scripts/populate/populate_league_leaders.py --seasons 2024-25 2023-24

    # Regular season only
    python scripts/populate/populate_league_leaders.py --regular-season-only

    # Skip all-time leaders (season leaders only)
    python scripts/populate/populate_league_leaders.py --skip-all-time

    # Dry run
    python scripts/populate/populate_league_leaders.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Any

import pandas as pd

from src.scripts.populate.api_client import get_client
from src.scripts.populate.base import BasePopulator, SeasonIteratorMixin
from src.scripts.populate.config import ALL_SEASONS, DEFAULT_SEASON_TYPES, get_db_path
from src.scripts.populate.helpers import configure_logging, resolve_season_types


configure_logging()
logger = logging.getLogger(__name__)


# Stat categories to iterate through for league leaders
STAT_CATEGORIES = [
    "PTS",
    "AST",
    "REB",
    "STL",
    "BLK",
    "FGM",
    "FG3M",
    "FTM",
    "EFF",
    "MIN",
]

# Per-mode options for league leaders
PER_MODES = [
    "Totals",
    "PerGame",
]

# Player scopes for league leaders
SCOPES = [
    "S",  # All players (Season)
]

# Expected columns for the league_leaders table
LEAGUE_LEADERS_EXPECTED_COLUMNS = [
    "season_id",
    "season_type",
    "stat_category",
    "per_mode",
    "scope",
    "rank",
    "player_id",
    "player_name",
    "team_id",
    "team_abbreviation",
    "games_played",
    "minutes",
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
    "eff",
    "ast_tov",
    "stl_tov",
    "filename",
]

# Expected columns for the all_time_leaders table
ALL_TIME_LEADERS_EXPECTED_COLUMNS = [
    "stat_category",
    "per_mode",
    "rank",
    "player_id",
    "player_name",
    "team_id",
    "stat_value",
    "seasons_played",
    "is_active",
    "filename",
]


class LeagueLeadersPopulator(BasePopulator, SeasonIteratorMixin):
    """Populator for league_leaders table."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._fetched_season_keys: list[str] = []

    def get_table_name(self) -> str:
        return "league_leaders"

    def get_key_columns(self) -> list[str]:
        return ["season_id", "season_type", "stat_category", "per_mode", "player_id"]

    def get_expected_columns(self) -> list[str]:
        return LEAGUE_LEADERS_EXPECTED_COLUMNS

    def fetch_data(self, **kwargs: Any) -> pd.DataFrame | None:
        """Fetch league leaders data for all seasons, season types, and stat categories."""
        seasons: list[str] = kwargs.get("seasons") or ALL_SEASONS[:5]
        season_types: list[str] = kwargs.get("season_types") or DEFAULT_SEASON_TYPES
        resume = kwargs.get("resume", True)

        all_data: list[pd.DataFrame] = []
        total_combinations = (
            len(seasons) * len(season_types) * len(STAT_CATEGORIES) * len(PER_MODES)
        )

        logger.info(
            "Fetching league leaders data for %d combinations "
            "(%d seasons x %d season types x %d stat categories x %d per modes)",
            total_combinations,
            len(seasons),
            len(season_types),
            len(STAT_CATEGORIES),
            len(PER_MODES),
        )

        processed = 0
        for season in seasons:
            for season_type in season_types:
                for stat_category in STAT_CATEGORIES:
                    for per_mode in PER_MODES:
                        processed += 1
                        progress_key = (
                            f"{season}_{season_type}_{stat_category}_{per_mode}"
                        )

                        # Check if already completed
                        if resume and self.progress.is_completed(progress_key):
                            logger.info(
                                "[%d/%d] Skipping %s (already completed)",
                                processed,
                                total_combinations,
                                progress_key,
                            )
                            continue

                        logger.info(
                            "[%d/%d] Fetching %s %s - %s (%s)...",
                            processed,
                            total_combinations,
                            season,
                            season_type,
                            stat_category,
                            per_mode,
                        )

                        try:
                            df = self.client.get_league_leaders(
                                season=season,
                                season_type=season_type,
                                stat_category=stat_category,
                                per_mode=per_mode,
                                scope="S",
                            )

                            if df is not None and not df.empty:
                                # Add metadata columns
                                df["_season"] = season
                                df["_season_type"] = season_type
                                df["_stat_category"] = stat_category
                                df["_per_mode"] = per_mode
                                df["_scope"] = "S"
                                all_data.append(df)
                                logger.info(
                                    "  Found %d records for %s %s",
                                    len(df),
                                    stat_category,
                                    per_mode,
                                )
                                self.metrics.api_calls += 1
                                # Track for deferred progress marking
                                self._fetched_season_keys.append(progress_key)
                            else:
                                logger.info(
                                    "  No data for %s %s - %s (%s)",
                                    season,
                                    season_type,
                                    stat_category,
                                    per_mode,
                                )

                            # Respect rate limiting
                            time.sleep(self.client.config.request_delay)

                        except Exception as e:
                            logger.exception(
                                "Error fetching %s %s - %s (%s): %s",
                                season,
                                season_type,
                                stat_category,
                                per_mode,
                                e,
                            )
                            self.progress.add_error(progress_key, str(e))
                            self.metrics.add_error(
                                str(e),
                                {
                                    "season": season,
                                    "season_type": season_type,
                                    "stat_category": stat_category,
                                    "per_mode": per_mode,
                                },
                            )

        if not all_data:
            logger.info("No data fetched")
            return None

        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info("Total records fetched: %d", len(combined_df))
        return combined_df

    def transform_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """Transform league leaders data to match schema."""
        if df.empty:
            return df

        output = pd.DataFrame()

        # Extract metadata from added columns
        output["season_id"] = df["_season"]
        output["season_type"] = df["_season_type"]
        output["stat_category"] = df["_stat_category"]
        output["per_mode"] = df["_per_mode"]
        output["scope"] = df["_scope"]

        # Rank and identifiers
        output["rank"] = pd.to_numeric(df.get("RANK"), errors="coerce").astype("Int64")
        output["player_id"] = pd.to_numeric(
            df.get("PLAYER_ID"), errors="coerce"
        ).astype("Int64")
        output["player_name"] = df.get("PLAYER", df.get("PLAYER_NAME", ""))
        output["team_id"] = pd.to_numeric(df.get("TEAM_ID"), errors="coerce").astype(
            "Int64"
        )
        output["team_abbreviation"] = df.get("TEAM", df.get("TEAM_ABBREVIATION", ""))

        # Games and minutes
        output["games_played"] = pd.to_numeric(df.get("GP"), errors="coerce").astype(
            "Int64"
        )
        output["minutes"] = pd.to_numeric(df.get("MIN"), errors="coerce")

        # Field goals
        output["fgm"] = pd.to_numeric(df.get("FGM"), errors="coerce")
        output["fga"] = pd.to_numeric(df.get("FGA"), errors="coerce")
        output["fg_pct"] = pd.to_numeric(df.get("FG_PCT"), errors="coerce")

        # Three pointers
        output["fg3m"] = pd.to_numeric(df.get("FG3M"), errors="coerce")
        output["fg3a"] = pd.to_numeric(df.get("FG3A"), errors="coerce")
        output["fg3_pct"] = pd.to_numeric(df.get("FG3_PCT"), errors="coerce")

        # Free throws
        output["ftm"] = pd.to_numeric(df.get("FTM"), errors="coerce")
        output["fta"] = pd.to_numeric(df.get("FTA"), errors="coerce")
        output["ft_pct"] = pd.to_numeric(df.get("FT_PCT"), errors="coerce")

        # Rebounds
        output["oreb"] = pd.to_numeric(df.get("OREB"), errors="coerce")
        output["dreb"] = pd.to_numeric(df.get("DREB"), errors="coerce")
        output["reb"] = pd.to_numeric(df.get("REB"), errors="coerce")

        # Other stats
        output["ast"] = pd.to_numeric(df.get("AST"), errors="coerce")
        output["stl"] = pd.to_numeric(df.get("STL"), errors="coerce")
        output["blk"] = pd.to_numeric(df.get("BLK"), errors="coerce")
        output["tov"] = pd.to_numeric(df.get("TOV"), errors="coerce")
        output["pf"] = pd.to_numeric(df.get("PF"), errors="coerce")
        output["pts"] = pd.to_numeric(df.get("PTS"), errors="coerce")

        # Efficiency and ratios
        output["eff"] = pd.to_numeric(df.get("EFF"), errors="coerce")
        output["ast_tov"] = pd.to_numeric(df.get("AST_TOV"), errors="coerce")
        output["stl_tov"] = pd.to_numeric(df.get("STL_TOV"), errors="coerce")

        # Add filename
        output["filename"] = "nba_api.leagueleaders"

        # Ensure all expected columns exist
        for col in LEAGUE_LEADERS_EXPECTED_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return output[LEAGUE_LEADERS_EXPECTED_COLUMNS]

    def pre_run_hook(self, **kwargs: Any) -> None:
        """Reset fetched keys for this run."""
        self._fetched_season_keys = []

    def post_run_hook(self, **kwargs: Any) -> None:
        """Mark fetched seasons as completed after successful database writes."""
        dry_run = kwargs.get("dry_run", False)
        if dry_run:
            logger.info(
                "DRY RUN - not marking progress for fetched seasons (data was not written)"
            )
            return

        if self._fetched_season_keys:
            for progress_key in self._fetched_season_keys:
                self.progress.mark_completed(progress_key)
            self.progress.save()
            logger.info(
                "Marked %d season/stat-category combinations as completed",
                len(self._fetched_season_keys),
            )


class AllTimeLeadersPopulator(BasePopulator):
    """Populator for all_time_leaders table."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._fetched_keys: list[str] = []

    def get_table_name(self) -> str:
        return "all_time_leaders"

    def get_key_columns(self) -> list[str]:
        return ["stat_category", "per_mode", "rank", "player_id"]

    def get_expected_columns(self) -> list[str]:
        return ALL_TIME_LEADERS_EXPECTED_COLUMNS

    def fetch_data(self, **kwargs: Any) -> pd.DataFrame | None:
        """Fetch all-time leaders data for both per modes."""
        resume = kwargs.get("resume", True)
        top_x: int = kwargs.get("top_x", 50)  # Get top 50 players per category

        all_data: list[pd.DataFrame] = []
        per_modes = ["Totals", "PerGame"]
        total_combinations = len(per_modes)

        logger.info(
            "Fetching all-time leaders data for %d per modes (top %d per category)",
            total_combinations,
            top_x,
        )

        for idx, per_mode in enumerate(per_modes, start=1):
            progress_key = f"all_time_{per_mode}"

            # Check if already completed
            if resume and self.progress.is_completed(progress_key):
                logger.info(
                    "[%d/%d] Skipping %s (already completed)",
                    idx,
                    total_combinations,
                    progress_key,
                )
                continue

            logger.info(
                "[%d/%d] Fetching all-time leaders (%s)...",
                idx,
                total_combinations,
                per_mode,
            )

            try:
                df = self.client.get_all_time_leaders(
                    per_mode=per_mode,
                    top_x=top_x,
                )

                if df is not None and not df.empty:
                    # Add metadata
                    df["_per_mode"] = per_mode
                    all_data.append(df)
                    logger.info(
                        "  Found %d total records for %s",
                        len(df),
                        per_mode,
                    )
                    self.metrics.api_calls += 1
                    self._fetched_keys.append(progress_key)
                else:
                    logger.info("  No data for all-time leaders (%s)", per_mode)

                # Respect rate limiting
                time.sleep(self.client.config.request_delay)

            except Exception as e:
                logger.exception(
                    "Error fetching all-time leaders (%s): %s",
                    per_mode,
                    e,
                )
                self.progress.add_error(progress_key, str(e))
                self.metrics.add_error(
                    str(e),
                    {"per_mode": per_mode},
                )

        if not all_data:
            logger.info("No all-time leaders data fetched")
            return None

        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info("Total all-time leaders records fetched: %d", len(combined_df))
        return combined_df

    def transform_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """Transform all-time leaders data to match schema."""
        if df.empty:
            return df

        output = pd.DataFrame()

        # Stat category and mode
        output["stat_category"] = df.get("STAT_CATEGORY", "")
        output["per_mode"] = df["_per_mode"]

        # Generate rank based on position within each category/mode group
        # The API returns data sorted by the stat value
        output["rank"] = df.groupby(["STAT_CATEGORY", "_per_mode"]).cumcount() + 1

        # Player identifiers
        output["player_id"] = pd.to_numeric(
            df.get("PLAYER_ID"), errors="coerce"
        ).astype("Int64")
        output["player_name"] = df.get("PLAYER_NAME", "")
        output["team_id"] = pd.to_numeric(df.get("TEAM_ID"), errors="coerce").astype(
            "Int64"
        )

        # Get the stat value - look for common stat columns
        # The all-time leaders grid has specific columns based on category
        stat_value_cols = [
            "GP",
            "PTS",
            "AST",
            "REB",
            "STL",
            "BLK",
            "FGM",
            "FGA",
            "FG3M",
            "FG3A",
            "FTM",
            "FTA",
            "OREB",
            "DREB",
            "TOV",
            "PF",
        ]

        # Try to extract the stat value from whichever column is populated
        output["stat_value"] = None
        for col in stat_value_cols:
            if col in df.columns:
                mask = df[col].notna()
                output.loc[mask, "stat_value"] = pd.to_numeric(
                    df.loc[mask, col], errors="coerce"
                )

        # Seasons played
        output["seasons_played"] = pd.to_numeric(
            df.get("SEASON_COUNT", df.get("SEASONS_PLAYED")), errors="coerce"
        ).astype("Int64")

        # Is active (infer from ACTIVE_WITH field if available)
        output["is_active"] = df.get("ACTIVE_WITH", "").notna() & (
            df.get("ACTIVE_WITH", "") != ""
        )

        # Add filename
        output["filename"] = "nba_api.alltimeleadersgrids"

        # Ensure all expected columns exist
        for col in ALL_TIME_LEADERS_EXPECTED_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return output[ALL_TIME_LEADERS_EXPECTED_COLUMNS]

    def pre_run_hook(self, **kwargs: Any) -> None:
        """Reset fetched keys for this run."""
        self._fetched_keys = []

    def post_run_hook(self, **kwargs: Any) -> None:
        """Mark fetched items as completed after successful database writes."""
        dry_run = kwargs.get("dry_run", False)
        if dry_run:
            logger.info(
                "DRY RUN - not marking progress for fetched items (data was not written)"
            )
            return

        if self._fetched_keys:
            for progress_key in self._fetched_keys:
                self.progress.mark_completed(progress_key)
            self.progress.save()
            logger.info(
                "Marked %d all-time leaders per-modes as completed",
                len(self._fetched_keys),
            )


def populate_league_leaders(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    season_types: list[str] | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate league_leaders table.

    Args:
        db_path: Path to DuckDB database
        seasons: List of seasons to fetch (e.g., ["2024-25", "2023-24"])
        season_types: List of season types (e.g., ["Regular Season", "Playoffs"])
        delay: Delay between API requests in seconds
        reset_progress: Reset progress tracking before starting
        dry_run: If True, don't actually insert data

    Returns:
        Dictionary with population statistics
    """
    db_path = db_path or str(get_db_path())
    seasons = seasons or ALL_SEASONS[:5]  # Default: last 5 seasons
    season_types = season_types or DEFAULT_SEASON_TYPES

    # Create client with custom delay
    client = get_client()
    client.config.request_delay = delay

    logger.info("=" * 70)
    logger.info("NBA LEAGUE LEADERS POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Seasons: {len(seasons)} ({seasons[0]} to {seasons[-1]})")
    logger.info(f"Season Types: {season_types}")
    logger.info(f"Stat Categories: {len(STAT_CATEGORIES)} categories")
    logger.info(f"Per Modes: {PER_MODES}")
    logger.info(f"Request Delay: {delay}s")

    # Create and run populator
    populator = LeagueLeadersPopulator(
        db_path=db_path,
        client=client,
    )

    return populator.run(
        seasons=seasons,
        season_types=season_types,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def populate_all_time_leaders(
    db_path: str | None = None,
    top_x: int = 50,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate all_time_leaders table.

    Args:
        db_path: Path to DuckDB database
        top_x: Number of top players to fetch per category
        delay: Delay between API requests in seconds
        reset_progress: Reset progress tracking before starting
        dry_run: If True, don't actually insert data

    Returns:
        Dictionary with population statistics
    """
    db_path = db_path or str(get_db_path())

    # Create client with custom delay
    client = get_client()
    client.config.request_delay = delay

    logger.info("=" * 70)
    logger.info("NBA ALL-TIME LEADERS POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Top X per Category: {top_x}")
    logger.info(f"Request Delay: {delay}s")

    # Create and run populator
    populator = AllTimeLeadersPopulator(
        db_path=db_path,
        client=client,
    )

    return populator.run(
        top_x=top_x,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def main() -> None:
    """Parse command-line arguments and run the league leaders population process."""
    parser = argparse.ArgumentParser(
        description="Populate league_leaders and all_time_leaders tables from NBA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (last 5 seasons + all-time leaders)
  python scripts/populate/populate_league_leaders.py

  # Specific seasons only
  python scripts/populate/populate_league_leaders.py --seasons 2024-25 2023-24

  # Regular season only
  python scripts/populate/populate_league_leaders.py --regular-season-only

  # Skip all-time leaders (season leaders only)
  python scripts/populate/populate_league_leaders.py --skip-all-time

  # Only all-time leaders (skip season leaders)
  python scripts/populate/populate_league_leaders.py --all-time-only

  # Reset progress and start fresh
  python scripts/populate/populate_league_leaders.py --reset-progress

  # Dry run (no database writes)
  python scripts/populate/populate_league_leaders.py --dry-run
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
        help="Only fetch regular season data",
    )
    parser.add_argument(
        "--playoffs-only",
        action="store_true",
        help="Only fetch playoff data",
    )
    parser.add_argument(
        "--skip-all-time",
        action="store_true",
        help="Skip all-time leaders population",
    )
    parser.add_argument(
        "--all-time-only",
        action="store_true",
        help="Only populate all-time leaders (skip season leaders)",
    )
    parser.add_argument(
        "--top-x",
        type=int,
        default=50,
        help="Number of top players per category for all-time leaders (default: 50)",
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

    error_count = 0

    try:
        # Populate season league leaders (unless --all-time-only)
        if not args.all_time_only:
            logger.info("\n" + "=" * 70)
            logger.info("PHASE 1: SEASON LEAGUE LEADERS")
            logger.info("=" * 70 + "\n")

            stats = populate_league_leaders(
                db_path=args.db,
                seasons=args.seasons,
                season_types=season_types,
                delay=args.delay,
                reset_progress=args.reset_progress,
                dry_run=args.dry_run,
            )
            error_count += stats.get("error_count", 0)

        # Populate all-time leaders (unless --skip-all-time)
        if not args.skip_all_time:
            logger.info("\n" + "=" * 70)
            logger.info("PHASE 2: ALL-TIME LEADERS")
            logger.info("=" * 70 + "\n")

            stats = populate_all_time_leaders(
                db_path=args.db,
                top_x=args.top_x,
                delay=args.delay,
                reset_progress=args.reset_progress,
                dry_run=args.dry_run,
            )
            error_count += stats.get("error_count", 0)

        if error_count > 0:
            logger.warning(f"Completed with {error_count} errors")
            sys.exit(1)
        else:
            logger.info("Population completed successfully!")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
