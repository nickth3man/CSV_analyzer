#!/usr/bin/env python3
"""Populate player_tracking_stats table from NBA API.

This script fetches player tracking statistics across multiple measure types:
- SpeedDistance: Speed and distance traveled
- Rebounding: Rebounding tracking
- Possessions: Touches and time of possession
- CatchShoot: Catch and shoot stats
- PullUpShot: Pull-up shooting
- Defense: Defensive tracking
- Drives: Driving stats
- Passing: Passing stats

Usage:
    # Full population (recent seasons)
    python scripts/populate/populate_player_tracking.py

    # Specific seasons only
    python scripts/populate/populate_player_tracking.py --seasons 2024-25 2023-24

    # Regular season only
    python scripts/populate/populate_player_tracking.py --regular-season-only

    # Dry run
    python scripts/populate/populate_player_tracking.py --dry-run
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
from src.scripts.populate.constants import SeasonType
from src.scripts.populate.helpers import configure_logging, resolve_season_types


configure_logging()
logger = logging.getLogger(__name__)


# Tracking measure types to iterate through
MEASURE_TYPES = [
    "SpeedDistance",
    "Rebounding",
    "Possessions",
    "CatchShoot",
    "PullUpShot",
    "Defense",
    "Drives",
    "Passing",
]

# Expected columns for the player_tracking_stats table
EXPECTED_COLUMNS = [
    "season_id",
    "season_type",
    "player_id",
    "player_name",
    "team_id",
    "team_abbreviation",
    "measure_type",
    "age",
    "games_played",
    "wins",
    "losses",
    "minutes",
    # SpeedDistance fields
    "speed",
    "distance_miles",
    "distance_feet",
    "avg_speed",
    "avg_speed_off",
    "avg_speed_def",
    # Rebounding fields
    "reb_chances",
    "reb_chances_contested",
    "reb_chances_uncontested",
    "reb_chances_def",
    "reb_chances_off",
    "reb_contested",
    "reb_uncontested",
    "reb_def",
    "reb_off",
    "reb_adj",
    # Possessions fields
    "touches",
    "front_ct_touches",
    "time_of_poss",
    "avg_sec_per_touch",
    "avg_drib_per_touch",
    "pts_per_touch",
    "elb_touches",
    "post_touches",
    "paint_touches",
    "pts",
    "pts_per_elb_touch",
    "pts_per_post_touch",
    "pts_per_paint_touch",
    # CatchShoot fields
    "catch_shoot_fgm",
    "catch_shoot_fga",
    "catch_shoot_fg_pct",
    "catch_shoot_pts",
    "catch_shoot_fg3m",
    "catch_shoot_fg3a",
    "catch_shoot_fg3_pct",
    "catch_shoot_efg_pct",
    # PullUpShot fields
    "pull_up_fgm",
    "pull_up_fga",
    "pull_up_fg_pct",
    "pull_up_pts",
    "pull_up_fg3m",
    "pull_up_fg3a",
    "pull_up_fg3_pct",
    "pull_up_efg_pct",
    # Defense fields
    "def_rim_fgm",
    "def_rim_fga",
    "def_rim_fg_pct",
    "less_than_6ft_fgm",
    "less_than_6ft_fga",
    "less_than_6ft_fg_pct",
    "less_than_10ft_fgm",
    "less_than_10ft_fga",
    "less_than_10ft_fg_pct",
    "greater_than_15ft_fgm",
    "greater_than_15ft_fga",
    "greater_than_15ft_fg_pct",
    # Drives fields
    "drives",
    "drive_fgm",
    "drive_fga",
    "drive_fg_pct",
    "drive_ftm",
    "drive_fta",
    "drive_ft_pct",
    "drive_pts",
    "drive_pts_pct",
    "drive_passes",
    "drive_passes_pct",
    "drive_ast",
    "drive_ast_pct",
    "drive_tov",
    "drive_tov_pct",
    "drive_pf",
    "drive_pf_pct",
    # Passing fields
    "passes_made",
    "passes_received",
    "ast",
    "secondary_ast",
    "potential_ast",
    "ast_pts_created",
    "ast_adj",
    "ast_to_pass_pct",
    "ast_to_pass_pct_adj",
    "filename",
]


class PlayerTrackingPopulator(BasePopulator, SeasonIteratorMixin):
    """Populator for player_tracking_stats table."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._fetched_season_keys: list[str] = []

    def get_table_name(self) -> str:
        return "player_tracking_stats"

    def get_key_columns(self) -> list[str]:
        return ["season_id", "season_type", "player_id", "measure_type"]

    def get_expected_columns(self) -> list[str]:
        return EXPECTED_COLUMNS

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        """Fetch player tracking data for all seasons, season types, and measure types."""
        seasons: list[str] = kwargs.get("seasons") or ALL_SEASONS[:5]  # Last 5 seasons
        season_types: list[str] = kwargs.get("season_types") or DEFAULT_SEASON_TYPES
        resume = kwargs.get("resume", True)

        all_data = []
        total_combinations = len(seasons) * len(season_types) * len(MEASURE_TYPES)

        logger.info(
            "Fetching player tracking data for %d combinations "
            "(%d seasons x %d season types x %d measure types)",
            total_combinations,
            len(seasons),
            len(season_types),
            len(MEASURE_TYPES),
        )

        processed = 0
        for season in seasons:
            for season_type in season_types:
                for measure_type in MEASURE_TYPES:
                    processed += 1
                    progress_key = f"{season}_{season_type}_{measure_type}"

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
                        "[%d/%d] Fetching %s %s - %s...",
                        processed,
                        total_combinations,
                        season,
                        season_type,
                        measure_type,
                    )

                    try:
                        df = self.client.get_player_tracking_stats(
                            season=season,
                            season_type=season_type,
                            pt_measure_type=measure_type,
                        )

                        if df is not None and not df.empty:
                            # Add metadata columns
                            df["_season"] = season
                            df["_season_type"] = season_type
                            df["_measure_type"] = measure_type
                            all_data.append(df)
                            logger.info(
                                "  Found %d records for %s",
                                len(df),
                                measure_type,
                            )
                            self.metrics.api_calls += 1
                            # Track for deferred progress marking
                            self._fetched_season_keys.append(progress_key)
                        else:
                            logger.info(
                                "  No data for %s %s - %s",
                                season,
                                season_type,
                                measure_type,
                            )

                        # Respect rate limiting
                        time.sleep(self.client.config.request_delay)

                    except Exception as e:
                        logger.exception(
                            "Error fetching %s %s - %s: %s",
                            season,
                            season_type,
                            measure_type,
                            e,
                        )
                        self.progress.add_error(progress_key, str(e))
                        self.metrics.add_error(
                            str(e),
                            {
                                "season": season,
                                "season_type": season_type,
                                "measure_type": measure_type,
                            },
                        )

        if not all_data:
            logger.info("No data fetched")
            return None

        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info("Total records fetched: %d", len(combined_df))
        return combined_df

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform player tracking data to match schema."""
        if df.empty:
            return df

        output = pd.DataFrame()

        # Extract metadata from added columns
        output["season_id"] = df.get("SEASON_YEAR", df.get("_season"))
        output["season_type"] = df["_season_type"]
        output["measure_type"] = df["_measure_type"]

        # Player/team identifiers
        output["player_id"] = pd.to_numeric(df["PLAYER_ID"], errors="coerce").astype(
            "Int64"
        )
        output["player_name"] = df.get("PLAYER_NAME", "")
        output["team_id"] = pd.to_numeric(df.get("TEAM_ID"), errors="coerce").astype(
            "Int64"
        )
        output["team_abbreviation"] = df.get("TEAM_ABBREVIATION", "")

        # Common fields
        output["age"] = pd.to_numeric(df.get("AGE"), errors="coerce").astype("Int64")
        output["games_played"] = pd.to_numeric(df.get("GP"), errors="coerce").astype(
            "Int64"
        )
        output["wins"] = pd.to_numeric(df.get("W"), errors="coerce").astype("Int64")
        output["losses"] = pd.to_numeric(df.get("L"), errors="coerce").astype("Int64")
        output["minutes"] = pd.to_numeric(df.get("MIN"), errors="coerce")

        # Measure-type specific fields (initialize all as None)
        # SpeedDistance
        output["speed"] = pd.to_numeric(df.get("SPEED"), errors="coerce")
        output["distance_miles"] = pd.to_numeric(df.get("DIST_MILES"), errors="coerce")
        output["distance_feet"] = pd.to_numeric(df.get("DIST_FEET"), errors="coerce")
        output["avg_speed"] = pd.to_numeric(df.get("AVG_SPEED"), errors="coerce")
        output["avg_speed_off"] = pd.to_numeric(
            df.get("AVG_SPEED_OFF"), errors="coerce"
        )
        output["avg_speed_def"] = pd.to_numeric(
            df.get("AVG_SPEED_DEF"), errors="coerce"
        )

        # Rebounding
        output["reb_chances"] = pd.to_numeric(
            df.get("REB_CHANCES"), errors="coerce"
        ).astype("Int64")
        output["reb_chances_contested"] = pd.to_numeric(
            df.get("REB_CHANCES_CONTESTED"), errors="coerce"
        ).astype("Int64")
        output["reb_chances_uncontested"] = pd.to_numeric(
            df.get("REB_CHANCES_UNCONTESTED"), errors="coerce"
        ).astype("Int64")
        output["reb_chances_def"] = pd.to_numeric(
            df.get("REB_CHANCES_DEF"), errors="coerce"
        ).astype("Int64")
        output["reb_chances_off"] = pd.to_numeric(
            df.get("REB_CHANCES_OFF"), errors="coerce"
        ).astype("Int64")
        output["reb_contested"] = pd.to_numeric(
            df.get("REB_CONTESTED"), errors="coerce"
        ).astype("Int64")
        output["reb_uncontested"] = pd.to_numeric(
            df.get("REB_UNCONTESTED"), errors="coerce"
        ).astype("Int64")
        output["reb_def"] = pd.to_numeric(df.get("DREB"), errors="coerce").astype(
            "Int64"
        )
        output["reb_off"] = pd.to_numeric(df.get("OREB"), errors="coerce").astype(
            "Int64"
        )
        output["reb_adj"] = pd.to_numeric(df.get("REB_ADJ"), errors="coerce")

        # Possessions
        output["touches"] = pd.to_numeric(df.get("TOUCHES"), errors="coerce").astype(
            "Int64"
        )
        output["front_ct_touches"] = pd.to_numeric(
            df.get("FRONT_CT_TOUCHES"), errors="coerce"
        ).astype("Int64")
        output["time_of_poss"] = pd.to_numeric(
            df.get("TIME_OF_POSS"), errors="coerce"
        )
        output["avg_sec_per_touch"] = pd.to_numeric(
            df.get("AVG_SEC_PER_TOUCH"), errors="coerce"
        )
        output["avg_drib_per_touch"] = pd.to_numeric(
            df.get("AVG_DRIB_PER_TOUCH"), errors="coerce"
        )
        output["pts_per_touch"] = pd.to_numeric(
            df.get("PTS_PER_TOUCH"), errors="coerce"
        )
        output["elb_touches"] = pd.to_numeric(
            df.get("ELB_TOUCHES"), errors="coerce"
        ).astype("Int64")
        output["post_touches"] = pd.to_numeric(
            df.get("POST_TOUCHES"), errors="coerce"
        ).astype("Int64")
        output["paint_touches"] = pd.to_numeric(
            df.get("PAINT_TOUCHES"), errors="coerce"
        ).astype("Int64")
        output["pts"] = pd.to_numeric(df.get("PTS"), errors="coerce").astype("Int64")
        output["pts_per_elb_touch"] = pd.to_numeric(
            df.get("PTS_PER_ELB_TOUCH"), errors="coerce"
        )
        output["pts_per_post_touch"] = pd.to_numeric(
            df.get("PTS_PER_POST_TOUCH"), errors="coerce"
        )
        output["pts_per_paint_touch"] = pd.to_numeric(
            df.get("PTS_PER_PAINT_TOUCH"), errors="coerce"
        )

        # CatchShoot
        output["catch_shoot_fgm"] = pd.to_numeric(
            df.get("CATCH_SHOOT_FGM"), errors="coerce"
        ).astype("Int64")
        output["catch_shoot_fga"] = pd.to_numeric(
            df.get("CATCH_SHOOT_FGA"), errors="coerce"
        ).astype("Int64")
        output["catch_shoot_fg_pct"] = pd.to_numeric(
            df.get("CATCH_SHOOT_FG_PCT"), errors="coerce"
        )
        output["catch_shoot_pts"] = pd.to_numeric(
            df.get("CATCH_SHOOT_PTS"), errors="coerce"
        ).astype("Int64")
        output["catch_shoot_fg3m"] = pd.to_numeric(
            df.get("CATCH_SHOOT_FG3M"), errors="coerce"
        ).astype("Int64")
        output["catch_shoot_fg3a"] = pd.to_numeric(
            df.get("CATCH_SHOOT_FG3A"), errors="coerce"
        ).astype("Int64")
        output["catch_shoot_fg3_pct"] = pd.to_numeric(
            df.get("CATCH_SHOOT_FG3_PCT"), errors="coerce"
        )
        output["catch_shoot_efg_pct"] = pd.to_numeric(
            df.get("CATCH_SHOOT_EFG_PCT"), errors="coerce"
        )

        # PullUpShot
        output["pull_up_fgm"] = pd.to_numeric(
            df.get("PULL_UP_FGM"), errors="coerce"
        ).astype("Int64")
        output["pull_up_fga"] = pd.to_numeric(
            df.get("PULL_UP_FGA"), errors="coerce"
        ).astype("Int64")
        output["pull_up_fg_pct"] = pd.to_numeric(
            df.get("PULL_UP_FG_PCT"), errors="coerce"
        )
        output["pull_up_pts"] = pd.to_numeric(
            df.get("PULL_UP_PTS"), errors="coerce"
        ).astype("Int64")
        output["pull_up_fg3m"] = pd.to_numeric(
            df.get("PULL_UP_FG3M"), errors="coerce"
        ).astype("Int64")
        output["pull_up_fg3a"] = pd.to_numeric(
            df.get("PULL_UP_FG3A"), errors="coerce"
        ).astype("Int64")
        output["pull_up_fg3_pct"] = pd.to_numeric(
            df.get("PULL_UP_FG3_PCT"), errors="coerce"
        )
        output["pull_up_efg_pct"] = pd.to_numeric(
            df.get("PULL_UP_EFG_PCT"), errors="coerce"
        )

        # Defense
        output["def_rim_fgm"] = pd.to_numeric(
            df.get("DEF_RIM_FGM"), errors="coerce"
        ).astype("Int64")
        output["def_rim_fga"] = pd.to_numeric(
            df.get("DEF_RIM_FGA"), errors="coerce"
        ).astype("Int64")
        output["def_rim_fg_pct"] = pd.to_numeric(
            df.get("DEF_RIM_FG_PCT"), errors="coerce"
        )
        output["less_than_6ft_fgm"] = pd.to_numeric(
            df.get("LESS_THAN_6FT_FGM"), errors="coerce"
        ).astype("Int64")
        output["less_than_6ft_fga"] = pd.to_numeric(
            df.get("LESS_THAN_6FT_FGA"), errors="coerce"
        ).astype("Int64")
        output["less_than_6ft_fg_pct"] = pd.to_numeric(
            df.get("LESS_THAN_6FT_FG_PCT"), errors="coerce"
        )
        output["less_than_10ft_fgm"] = pd.to_numeric(
            df.get("LESS_THAN_10FT_FGM"), errors="coerce"
        ).astype("Int64")
        output["less_than_10ft_fga"] = pd.to_numeric(
            df.get("LESS_THAN_10FT_FGA"), errors="coerce"
        ).astype("Int64")
        output["less_than_10ft_fg_pct"] = pd.to_numeric(
            df.get("LESS_THAN_10FT_FG_PCT"), errors="coerce"
        )
        output["greater_than_15ft_fgm"] = pd.to_numeric(
            df.get("GREATER_THAN_15FT_FGM"), errors="coerce"
        ).astype("Int64")
        output["greater_than_15ft_fga"] = pd.to_numeric(
            df.get("GREATER_THAN_15FT_FGA"), errors="coerce"
        ).astype("Int64")
        output["greater_than_15ft_fg_pct"] = pd.to_numeric(
            df.get("GREATER_THAN_15FT_FG_PCT"), errors="coerce"
        )

        # Drives
        output["drives"] = pd.to_numeric(df.get("DRIVES"), errors="coerce").astype(
            "Int64"
        )
        output["drive_fgm"] = pd.to_numeric(
            df.get("DRIVE_FGM"), errors="coerce"
        ).astype("Int64")
        output["drive_fga"] = pd.to_numeric(
            df.get("DRIVE_FGA"), errors="coerce"
        ).astype("Int64")
        output["drive_fg_pct"] = pd.to_numeric(
            df.get("DRIVE_FG_PCT"), errors="coerce"
        )
        output["drive_ftm"] = pd.to_numeric(
            df.get("DRIVE_FTM"), errors="coerce"
        ).astype("Int64")
        output["drive_fta"] = pd.to_numeric(
            df.get("DRIVE_FTA"), errors="coerce"
        ).astype("Int64")
        output["drive_ft_pct"] = pd.to_numeric(
            df.get("DRIVE_FT_PCT"), errors="coerce"
        )
        output["drive_pts"] = pd.to_numeric(
            df.get("DRIVE_PTS"), errors="coerce"
        ).astype("Int64")
        output["drive_pts_pct"] = pd.to_numeric(
            df.get("DRIVE_PTS_PCT"), errors="coerce"
        )
        output["drive_passes"] = pd.to_numeric(
            df.get("DRIVE_PASSES"), errors="coerce"
        ).astype("Int64")
        output["drive_passes_pct"] = pd.to_numeric(
            df.get("DRIVE_PASSES_PCT"), errors="coerce"
        )
        output["drive_ast"] = pd.to_numeric(
            df.get("DRIVE_AST"), errors="coerce"
        ).astype("Int64")
        output["drive_ast_pct"] = pd.to_numeric(
            df.get("DRIVE_AST_PCT"), errors="coerce"
        )
        output["drive_tov"] = pd.to_numeric(
            df.get("DRIVE_TOV"), errors="coerce"
        ).astype("Int64")
        output["drive_tov_pct"] = pd.to_numeric(
            df.get("DRIVE_TOV_PCT"), errors="coerce"
        )
        output["drive_pf"] = pd.to_numeric(df.get("DRIVE_PF"), errors="coerce").astype(
            "Int64"
        )
        output["drive_pf_pct"] = pd.to_numeric(
            df.get("DRIVE_PF_PCT"), errors="coerce"
        )

        # Passing
        output["passes_made"] = pd.to_numeric(
            df.get("PASSES_MADE"), errors="coerce"
        ).astype("Int64")
        output["passes_received"] = pd.to_numeric(
            df.get("PASSES_RECEIVED"), errors="coerce"
        ).astype("Int64")
        output["ast"] = pd.to_numeric(df.get("AST"), errors="coerce").astype("Int64")
        output["secondary_ast"] = pd.to_numeric(
            df.get("SECONDARY_AST"), errors="coerce"
        ).astype("Int64")
        output["potential_ast"] = pd.to_numeric(
            df.get("POTENTIAL_AST"), errors="coerce"
        ).astype("Int64")
        output["ast_pts_created"] = pd.to_numeric(
            df.get("AST_PTS_CREATED"), errors="coerce"
        ).astype("Int64")
        output["ast_adj"] = pd.to_numeric(df.get("AST_ADJ"), errors="coerce")
        output["ast_to_pass_pct"] = pd.to_numeric(
            df.get("AST_TO_PASS_PCT"), errors="coerce"
        )
        output["ast_to_pass_pct_adj"] = pd.to_numeric(
            df.get("AST_TO_PASS_PCT_ADJ"), errors="coerce"
        )

        # Add filename
        output["filename"] = "nba_api.leaguedashptstats"

        # Ensure all expected columns exist
        for col in EXPECTED_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return output[EXPECTED_COLUMNS]

    def pre_run_hook(self, **kwargs) -> None:
        """Reset fetched keys for this run."""
        self._fetched_season_keys = []

    def post_run_hook(self, **kwargs) -> None:
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
                "Marked %d season/measure-type combinations as completed",
                len(self._fetched_season_keys),
            )


def populate_player_tracking(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    season_types: list[str] | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate player_tracking_stats table.

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
    logger.info("NBA PLAYER TRACKING STATS POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Seasons: {len(seasons)} ({seasons[0]} to {seasons[-1]})")
    logger.info(f"Season Types: {season_types}")
    logger.info(f"Measure Types: {len(MEASURE_TYPES)} types")
    logger.info(f"Request Delay: {delay}s")

    # Create and run populator
    populator = PlayerTrackingPopulator(
        db_path=db_path,
        client=client,
    )

    return populator.run(
        seasons=seasons,
        season_types=season_types,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def main() -> None:
    """Parse command-line arguments and run the player tracking stats population process."""
    parser = argparse.ArgumentParser(
        description="Populate player_tracking_stats table from NBA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (last 5 seasons)
  python scripts/populate/populate_player_tracking.py

  # Specific seasons only
  python scripts/populate/populate_player_tracking.py --seasons 2024-25 2023-24

  # Regular season only
  python scripts/populate/populate_player_tracking.py --regular-season-only

  # Reset progress and start fresh
  python scripts/populate/populate_player_tracking.py --reset-progress

  # Dry run (no database writes)
  python scripts/populate/populate_player_tracking.py --dry-run
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
        help="Only fetch regular season games",
    )
    parser.add_argument(
        "--playoffs-only",
        action="store_true",
        help="Only fetch playoff games",
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
        stats = populate_player_tracking(
            db_path=args.db,
            seasons=args.seasons,
            season_types=season_types,
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
