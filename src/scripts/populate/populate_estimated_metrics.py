#!/usr/bin/env python3
"""Populate estimated_metrics table from NBA API.

This script fetches estimated advanced metrics from the NBA API:
- PlayerEstimatedMetrics - player-level estimated OFF/DEF/NET ratings
- TeamEstimatedMetrics - team-level estimated OFF/DEF/NET ratings

These metrics are derived from play-by-play data and provide estimated
advanced statistics without requiring traditional box score calculations.

Usage:
    # Full population (recent seasons)
    python scripts/populate/populate_estimated_metrics.py

    # Specific seasons only
    python scripts/populate/populate_estimated_metrics.py --seasons 2024-25 2023-24

    # Regular season only
    python scripts/populate/populate_estimated_metrics.py --regular-season-only

    # Dry run
    python scripts/populate/populate_estimated_metrics.py --dry-run
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


# Entity types for combining player and team data
ENTITY_TYPES = ["player", "team"]

# Expected columns for the estimated_metrics table
EXPECTED_COLUMNS = [
    "season_id",
    "season_type",
    "entity_type",
    "entity_id",
    "entity_name",
    "team_id",
    "team_abbreviation",
    "games_played",
    "wins",
    "losses",
    "win_pct",
    "minutes",
    "e_off_rating",
    "e_def_rating",
    "e_net_rating",
    "e_ast_ratio",
    "e_oreb_pct",
    "e_dreb_pct",
    "e_reb_pct",
    "e_tov_pct",
    "e_efg_pct",
    "e_ts_pct",
    "e_usg_pct",
    "e_pace",
    "e_pie",
    "filename",
]


class EstimatedMetricsPopulator(BasePopulator, SeasonIteratorMixin):
    """Populator for estimated_metrics table."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._fetched_season_keys: list[str] = []

    def get_table_name(self) -> str:
        return "estimated_metrics"

    def get_key_columns(self) -> list[str]:
        return ["season_id", "season_type", "entity_type", "entity_id"]

    def get_expected_columns(self) -> list[str]:
        return EXPECTED_COLUMNS

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        """Fetch estimated metrics data for all seasons, season types, and entity types."""
        seasons: list[str] = kwargs.get("seasons") or ALL_SEASONS[:5]  # Last 5 seasons
        season_types: list[str] = kwargs.get("season_types") or DEFAULT_SEASON_TYPES
        resume = kwargs.get("resume", True)

        all_data = []
        total_combinations = len(seasons) * len(season_types) * len(ENTITY_TYPES)

        logger.info(
            "Fetching estimated metrics data for %d combinations "
            "(%d seasons x %d season types x %d entity types)",
            total_combinations,
            len(seasons),
            len(season_types),
            len(ENTITY_TYPES),
        )

        processed = 0
        for season in seasons:
            for season_type in season_types:
                for entity_type in ENTITY_TYPES:
                    processed += 1
                    progress_key = f"{season}_{season_type}_{entity_type}"

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
                        entity_type,
                    )

                    try:
                        if entity_type == "player":
                            df = self.client.get_player_estimated_metrics(
                                season=season,
                                season_type=season_type,
                            )
                        else:  # team
                            df = self.client.get_team_estimated_metrics(
                                season=season,
                                season_type=season_type,
                            )

                        if df is not None and not df.empty:
                            # Add metadata columns
                            df["_season"] = season
                            df["_season_type"] = season_type
                            df["_entity_type"] = entity_type
                            all_data.append(df)
                            logger.info(
                                "  Found %d records for %s",
                                len(df),
                                entity_type,
                            )
                            self.metrics.api_calls += 1
                            # Track for deferred progress marking
                            self._fetched_season_keys.append(progress_key)
                        else:
                            logger.info(
                                "  No data for %s %s - %s",
                                season,
                                season_type,
                                entity_type,
                            )

                        # Respect rate limiting
                        time.sleep(self.client.config.request_delay)

                    except Exception as e:
                        logger.exception(
                            "Error fetching %s %s - %s: %s",
                            season,
                            season_type,
                            entity_type,
                            e,
                        )
                        self.progress.add_error(progress_key, str(e))
                        self.metrics.add_error(
                            str(e),
                            {
                                "season": season,
                                "season_type": season_type,
                                "entity_type": entity_type,
                            },
                        )

        if not all_data:
            logger.info("No data fetched")
            return None

        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info("Total records fetched: %d", len(combined_df))
        return combined_df

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform estimated metrics data to match schema."""
        if df.empty:
            return df

        output = pd.DataFrame()

        # Extract metadata from added columns
        output["season_id"] = df["_season"]
        output["season_type"] = df["_season_type"]
        output["entity_type"] = df["_entity_type"]

        # Entity identifiers - handle both player and team data
        # For players: PLAYER_ID, PLAYER_NAME
        # For teams: TEAM_ID, TEAM_NAME
        output["entity_id"] = pd.to_numeric(
            df.apply(
                lambda row: row.get("PLAYER_ID")
                if row["_entity_type"] == "player"
                else row.get("TEAM_ID"),
                axis=1,
            ),
            errors="coerce",
        ).astype("Int64")

        output["entity_name"] = df.apply(
            lambda row: row.get("PLAYER_NAME", "")
            if row["_entity_type"] == "player"
            else row.get("TEAM_NAME", ""),
            axis=1,
        )

        # Team info (only for players, teams get their own ID in entity_id)
        output["team_id"] = pd.to_numeric(
            df.apply(
                lambda row: row.get("TEAM_ID")
                if row["_entity_type"] == "player"
                else None,
                axis=1,
            ),
            errors="coerce",
        ).astype("Int64")

        output["team_abbreviation"] = df.apply(
            lambda row: row.get("TEAM_ABBREVIATION", "")
            if row["_entity_type"] == "player"
            else "",
            axis=1,
        )

        # Common fields
        output["games_played"] = pd.to_numeric(df.get("GP"), errors="coerce").astype(
            "Int64"
        )
        output["wins"] = pd.to_numeric(df.get("W"), errors="coerce").astype("Int64")
        output["losses"] = pd.to_numeric(df.get("L"), errors="coerce").astype("Int64")
        output["win_pct"] = pd.to_numeric(df.get("W_PCT"), errors="coerce")
        output["minutes"] = pd.to_numeric(df.get("MIN"), errors="coerce")

        # Estimated metrics fields
        output["e_off_rating"] = pd.to_numeric(df.get("E_OFF_RATING"), errors="coerce")
        output["e_def_rating"] = pd.to_numeric(df.get("E_DEF_RATING"), errors="coerce")
        output["e_net_rating"] = pd.to_numeric(df.get("E_NET_RATING"), errors="coerce")
        output["e_ast_ratio"] = pd.to_numeric(df.get("E_AST_RATIO"), errors="coerce")
        output["e_oreb_pct"] = pd.to_numeric(df.get("E_OREB_PCT"), errors="coerce")
        output["e_dreb_pct"] = pd.to_numeric(df.get("E_DREB_PCT"), errors="coerce")
        output["e_reb_pct"] = pd.to_numeric(df.get("E_REB_PCT"), errors="coerce")
        output["e_tov_pct"] = pd.to_numeric(df.get("E_TOV_PCT"), errors="coerce")
        output["e_efg_pct"] = pd.to_numeric(df.get("E_EFG_PCT"), errors="coerce")
        output["e_ts_pct"] = pd.to_numeric(df.get("E_TS_PCT"), errors="coerce")

        # E_USG_PCT only exists for players
        output["e_usg_pct"] = pd.to_numeric(df.get("E_USG_PCT"), errors="coerce")

        output["e_pace"] = pd.to_numeric(df.get("E_PACE"), errors="coerce")
        output["e_pie"] = pd.to_numeric(df.get("E_PIE"), errors="coerce")

        # Add filename
        output["filename"] = df["_entity_type"].apply(
            lambda x: f"nba_api.{'playerestimatedmetrics' if x == 'player' else 'teamestimatedmetrics'}"
        )

        # Ensure all expected columns exist
        for col in EXPECTED_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return output[EXPECTED_COLUMNS]

    def validate_data(self, df: pd.DataFrame, **kwargs) -> bool:
        """Validate the estimated metrics data.

        Performs checks specific to estimated metrics data:
        - Required columns are present
        - Entity IDs are valid
        - Metric values are within reasonable ranges
        """
        if df.empty:
            logger.warning("DataFrame is empty, skipping validation")
            return True

        errors = []
        warnings = []

        # Check required columns
        required_cols = [
            "season_id",
            "season_type",
            "entity_type",
            "entity_id",
            "entity_name",
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")

        # Check entity_id is not null
        null_entity_ids = df["entity_id"].isna().sum()
        if null_entity_ids > 0:
            errors.append(f"Found {null_entity_ids} records with null entity_id")

        # Check entity_type values
        invalid_entity_types = df[~df["entity_type"].isin(ENTITY_TYPES)]
        if len(invalid_entity_types) > 0:
            errors.append(
                f"Found {len(invalid_entity_types)} records with invalid entity_type"
            )

        # Validate rating ranges (typically between 80 and 140)
        for col in ["e_off_rating", "e_def_rating", "e_net_rating"]:
            if col in df.columns:
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    min_val, max_val = col_data.min(), col_data.max()
                    # E_NET_RATING can be negative
                    if col == "e_net_rating":
                        if min_val < -50 or max_val > 50:
                            warnings.append(
                                f"{col} has unusual range: [{min_val:.1f}, {max_val:.1f}]"
                            )
                    elif min_val < 70 or max_val > 150:
                        warnings.append(
                            f"{col} has unusual range: [{min_val:.1f}, {max_val:.1f}]"
                        )

        # Validate percentage fields (should be between 0 and 1)
        pct_cols = [
            "win_pct",
            "e_oreb_pct",
            "e_dreb_pct",
            "e_reb_pct",
            "e_tov_pct",
            "e_efg_pct",
            "e_ts_pct",
            "e_usg_pct",
            "e_pie",
        ]
        for col in pct_cols:
            if col in df.columns:
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    min_val, max_val = col_data.min(), col_data.max()
                    if min_val < 0 or max_val > 1:
                        warnings.append(
                            f"{col} has values outside [0, 1]: [{min_val:.3f}, {max_val:.3f}]"
                        )

        # Check for duplicate keys
        key_cols = self.get_key_columns()
        duplicates = df.duplicated(subset=key_cols, keep=False).sum()
        if duplicates > 0:
            errors.append(f"Found {duplicates} duplicate key combinations")

        # Log warnings
        for warning in warnings:
            logger.warning("Validation warning: %s", warning)
            self.metrics.warnings.append(warning)

        # Log errors and return validation result
        if errors:
            for error in errors:
                logger.error("Validation error: %s", error)
                self.metrics.add_error(error)
            return False

        logger.info("Data validation passed")
        return True

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
                "Marked %d season/entity-type combinations as completed",
                len(self._fetched_season_keys),
            )


def populate_estimated_metrics(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    season_types: list[str] | None = None,
    delay: float = 0.6,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main function to populate estimated_metrics table.

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
    logger.info("NBA ESTIMATED METRICS POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Seasons: {len(seasons)} ({seasons[0]} to {seasons[-1]})")
    logger.info(f"Season Types: {season_types}")
    logger.info(f"Entity Types: {ENTITY_TYPES}")
    logger.info(f"Request Delay: {delay}s")

    # Create and run populator
    populator = EstimatedMetricsPopulator(
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
    """Parse command-line arguments and run the estimated metrics population process."""
    parser = argparse.ArgumentParser(
        description="Populate estimated_metrics table from NBA API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (last 5 seasons)
  python scripts/populate/populate_estimated_metrics.py

  # Specific seasons only
  python scripts/populate/populate_estimated_metrics.py --seasons 2024-25 2023-24

  # Regular season only
  python scripts/populate/populate_estimated_metrics.py --regular-season-only

  # Reset progress and start fresh
  python scripts/populate/populate_estimated_metrics.py --reset-progress

  # Dry run (no database writes)
  python scripts/populate/populate_estimated_metrics.py --dry-run
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
        stats = populate_estimated_metrics(
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
