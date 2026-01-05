"""Data reconciliation between NBA API and Basketball Reference sources.

This module provides utilities to:
- Compare player stats between different data sources
- Identify discrepancies with configurable thresholds
- Generate reconciliation reports
- Support data quality monitoring

The reconciliation process helps ensure data integrity by cross-referencing
statistics from multiple authoritative sources (NBA Stats API and Basketball
Reference) and flagging potential issues for review.

Usage:
    from src.scripts.populate.reconciliation import DataReconciler, Discrepancy

    # Initialize reconciler
    reconciler = DataReconciler(db_path)

    # Reconcile a specific player's season stats
    report = reconciler.reconcile_player_season_stats(player_id=201566, season="2024-25")

    # Reconcile all players for a season
    summary = reconciler.reconcile_all_players(season="2024-25", threshold=0.5)

    # Generate various report formats
    df_report = reconciler.generate_report(discrepancies, output_format='dataframe')
    json_report = reconciler.generate_report(discrepancies, output_format='json')

CLI Usage:
    python -m src.scripts.populate.reconciliation --season 2024-25
    python -m src.scripts.populate.reconciliation --player-id 201566 --season 2024-25
    python -m src.scripts.populate.reconciliation --season 2024-25 --threshold 0.5 --output report.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from src.scripts.populate.config import get_db_path
from src.scripts.populate.helpers import configure_logging


logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS AND DATA CLASSES
# =============================================================================


class Severity(str, Enum):
    """Severity levels for discrepancies."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class EntityType(str, Enum):
    """Types of entities that can be reconciled."""

    PLAYER = "player"
    TEAM = "team"
    GAME = "game"


class ReconciliationStatus(str, Enum):
    """Status of reconciliation operations."""

    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    NO_DATA = "no_data"


@dataclass
class Discrepancy:
    """Represents a data discrepancy between sources.

    Attributes:
        entity_type: Type of entity (player, team, game)
        entity_id: Unique identifier for the entity
        entity_name: Human-readable name for the entity
        stat_name: Name of the statistic with discrepancy
        nba_value: Value from NBA API source
        br_value: Value from Basketball Reference source
        difference: Absolute difference between values
        pct_difference: Percentage difference
        severity: Severity classification (low, medium, high, critical)
        season: Season identifier
        context: Additional context information
    """

    entity_type: str
    entity_id: int | str
    entity_name: str
    stat_name: str
    nba_value: float | int | None
    br_value: float | int | None
    difference: float
    pct_difference: float
    severity: str
    season: str | None = None
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return asdict(self)

    def __str__(self) -> str:
        """Human-readable string representation."""
        return (
            f"{self.entity_type.upper()} {self.entity_name} ({self.entity_id}): "
            f"{self.stat_name} - NBA: {self.nba_value}, BR: {self.br_value} "
            f"(diff: {self.difference:.2f}, {self.pct_difference:.1f}%) [{self.severity}]"
        )


@dataclass
class ReconciliationSummary:
    """Summary of a reconciliation run.

    Attributes:
        season: Season that was reconciled
        status: Overall status of reconciliation
        total_entities: Number of entities compared
        entities_matched: Entities with matching data
        entities_with_discrepancies: Entities with at least one discrepancy
        total_discrepancies: Total number of discrepancies found
        discrepancies_by_severity: Count by severity level
        discrepancies_by_stat: Count by statistic name
        duration_seconds: Time taken for reconciliation
        timestamp: When reconciliation was performed
    """

    season: str
    status: str
    total_entities: int = 0
    entities_matched: int = 0
    entities_with_discrepancies: int = 0
    total_discrepancies: int = 0
    discrepancies_by_severity: dict[str, int] = field(default_factory=dict)
    discrepancies_by_stat: dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return asdict(self)


# =============================================================================
# STAT MAPPINGS AND CONFIGURATION
# =============================================================================

# Mapping between NBA API column names and BR column names
# Format: {nba_column: br_column}
STAT_COLUMN_MAPPING: dict[str, str] = {
    # Counting stats
    "pts": "pts",
    "reb": "reb",
    "ast": "ast",
    "stl": "stl",
    "blk": "blk",
    "tov": "tov",
    "pf": "pf",
    # Shooting stats
    "fgm": "fgm",
    "fga": "fga",
    "fg3m": "fg3m",
    "fg3a": "fg3a",
    "ftm": "ftm",
    "fta": "fta",
    # Rebounds breakdown
    "oreb": "oreb",
    "dreb": "dreb",
    # Games
    "games_played": "games_played",
    "minutes_played": "minutes_played",
}

# Default thresholds for different stat types
# These define the maximum allowed difference before flagging as discrepancy
DEFAULT_THRESHOLDS: dict[str, float] = {
    # Counting stats (allow small rounding differences)
    "pts": 1.0,
    "reb": 1.0,
    "ast": 1.0,
    "stl": 0.5,
    "blk": 0.5,
    "tov": 0.5,
    "pf": 0.5,
    # Shooting stats
    "fgm": 1.0,
    "fga": 1.0,
    "fg3m": 1.0,
    "fg3a": 1.0,
    "ftm": 1.0,
    "fta": 1.0,
    # Rebounds breakdown
    "oreb": 0.5,
    "dreb": 0.5,
    # Games should match exactly
    "games_played": 0,
    # Minutes allow more variance
    "minutes_played": 5.0,
    # Percentages (as decimals, so 0.01 = 1%)
    "fg_pct": 0.01,
    "fg3_pct": 0.01,
    "ft_pct": 0.01,
    # Default for any unlisted stats
    "default": 1.0,
}

# Severity thresholds based on percentage difference
SEVERITY_PCT_THRESHOLDS: dict[str, float] = {
    Severity.LOW.value: 5.0,  # 0-5% difference
    Severity.MEDIUM.value: 10.0,  # 5-10% difference
    Severity.HIGH.value: 20.0,  # 10-20% difference
    # Anything above 20% is CRITICAL
}


# =============================================================================
# DATA RECONCILER CLASS
# =============================================================================


class DataReconciler:
    """Reconcile data between NBA API and Basketball Reference sources.

    This class provides methods to compare statistics from different data sources,
    identify discrepancies, and generate reports for data quality monitoring.

    Attributes:
        db_path: Path to the DuckDB database
        thresholds: Dictionary of stat-specific thresholds
        severity_thresholds: Thresholds for severity classification

    Example:
        >>> reconciler = DataReconciler("nba.duckdb")
        >>> discrepancies = reconciler.reconcile_player_season_stats(201566, "2024-25")
        >>> for d in discrepancies:
        ...     print(d)
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        thresholds: dict[str, float] | None = None,
    ) -> None:
        """Initialize the DataReconciler.

        Args:
            db_path: Path to DuckDB database. If None, uses default from config.
            thresholds: Custom thresholds for discrepancy detection.
                       Merged with defaults; keys are stat names, values are
                       maximum allowed absolute differences.
        """
        self.db_path = str(db_path) if db_path else str(get_db_path())
        self._conn: duckdb.DuckDBPyConnection | None = None

        # Merge custom thresholds with defaults
        self.thresholds = {**DEFAULT_THRESHOLDS}
        if thresholds:
            self.thresholds.update(thresholds)

        self.severity_thresholds = SEVERITY_PCT_THRESHOLDS.copy()

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection.

        Returns:
            Active DuckDB connection.
        """
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path, read_only=True)
            logger.debug(f"Connected to database: {self.db_path}")
        return self._conn

    def close(self) -> None:
        """Close database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> DataReconciler:
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    # =========================================================================
    # THRESHOLD AND SEVERITY HELPERS
    # =========================================================================

    def get_threshold(self, stat_name: str) -> float:
        """Get the threshold for a specific stat.

        Args:
            stat_name: Name of the statistic.

        Returns:
            Threshold value for the stat.
        """
        return self.thresholds.get(stat_name, self.thresholds.get("default", 1.0))

    def classify_severity(
        self,
        difference: float,
        pct_difference: float,
        stat_name: str,
    ) -> str:
        """Classify the severity of a discrepancy.

        Severity is based primarily on percentage difference, with consideration
        for the absolute difference and the type of statistic.

        Args:
            difference: Absolute difference between values.
            pct_difference: Percentage difference.
            stat_name: Name of the statistic.

        Returns:
            Severity level string (low, medium, high, critical).
        """
        # Games should always match - any difference is critical
        if stat_name == "games_played" and difference > 0:
            return Severity.CRITICAL.value

        # Use percentage-based severity
        if pct_difference <= self.severity_thresholds[Severity.LOW.value]:
            return Severity.LOW.value
        if pct_difference <= self.severity_thresholds[Severity.MEDIUM.value]:
            return Severity.MEDIUM.value
        if pct_difference <= self.severity_thresholds[Severity.HIGH.value]:
            return Severity.HIGH.value
        return Severity.CRITICAL.value

    def calculate_difference(
        self,
        nba_value: float | int | None,
        br_value: float | int | None,
    ) -> tuple[float, float]:
        """Calculate absolute and percentage difference between values.

        Args:
            nba_value: Value from NBA API.
            br_value: Value from Basketball Reference.

        Returns:
            Tuple of (absolute_difference, percentage_difference).
        """
        # Handle None values
        if nba_value is None and br_value is None:
            return 0.0, 0.0
        if nba_value is None:
            nba_value = 0
        if br_value is None:
            br_value = 0

        # Convert to float
        nba_val = float(nba_value)
        br_val = float(br_value)

        # Calculate absolute difference
        difference = abs(nba_val - br_val)

        # Calculate percentage difference
        # Use the larger value as the base to avoid division issues
        base = max(abs(nba_val), abs(br_val))
        if base == 0:
            pct_difference = 0.0
        else:
            pct_difference = (difference / base) * 100

        return difference, pct_difference

    # =========================================================================
    # DATA RETRIEVAL METHODS
    # =========================================================================

    def get_nba_player_season_stats(
        self,
        player_id: int,
        season: str,
    ) -> dict[str, Any] | None:
        """Get player season stats from NBA API data.

        Args:
            player_id: NBA player ID.
            season: Season identifier (e.g., "2024-25").

        Returns:
            Dictionary of stats or None if not found.
        """
        conn = self.connect()

        try:
            result = conn.execute(
                """
                SELECT
                    player_id,
                    player_name,
                    season_id,
                    games_played,
                    minutes_played,
                    pts,
                    reb,
                    ast,
                    stl,
                    blk,
                    tov,
                    pf,
                    fgm,
                    fga,
                    fg_pct,
                    fg3m,
                    fg3a,
                    fg3_pct,
                    ftm,
                    fta,
                    ft_pct,
                    oreb,
                    dreb
                FROM player_season_stats
                WHERE player_id = ?
                  AND season_id = ?
                  AND season_type = 'Regular Season'
                """,
                [player_id, season],
            ).fetchone()

            if result is None:
                return None

            columns = [
                "player_id",
                "player_name",
                "season_id",
                "games_played",
                "minutes_played",
                "pts",
                "reb",
                "ast",
                "stl",
                "blk",
                "tov",
                "pf",
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
            ]
            return dict(zip(columns, result))

        except duckdb.CatalogException:
            logger.warning("player_season_stats table not found")
            return None

    def get_br_player_season_stats(
        self,
        player_name: str,
        season: str,
    ) -> dict[str, Any] | None:
        """Get player season stats from Basketball Reference data.

        Args:
            player_name: Player's full name.
            season: Season identifier (e.g., "2024-25").

        Returns:
            Dictionary of stats or None if not found.
        """
        conn = self.connect()

        try:
            # BR uses season end year format
            # Convert "2024-25" to 2025
            season_year = int(season.split("-")[0]) + 1

            result = conn.execute(
                """
                SELECT
                    player_name,
                    season_id,
                    games_played,
                    minutes_played,
                    pts,
                    reb,
                    ast,
                    stl,
                    blk,
                    tov,
                    pf,
                    fgm,
                    fga,
                    fg_pct,
                    fg3m,
                    fg3a,
                    fg3_pct,
                    ftm,
                    fta,
                    ft_pct,
                    oreb,
                    dreb
                FROM br_player_season_totals
                WHERE LOWER(player_name) = LOWER(?)
                  AND season_end_year = ?
                """,
                [player_name, season_year],
            ).fetchone()

            if result is None:
                return None

            columns = [
                "player_name",
                "season_id",
                "games_played",
                "minutes_played",
                "pts",
                "reb",
                "ast",
                "stl",
                "blk",
                "tov",
                "pf",
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
            ]
            return dict(zip(columns, result))

        except duckdb.CatalogException:
            logger.warning("br_player_season_totals table not found")
            return None

    def get_player_mapping(self) -> pd.DataFrame:
        """Get mapping between NBA player IDs and names.

        Returns:
            DataFrame with player_id and player_name columns.
        """
        conn = self.connect()

        try:
            return conn.execute(
                """
                SELECT DISTINCT
                    player_id,
                    player_name
                FROM player_season_stats
                WHERE player_name IS NOT NULL
                ORDER BY player_name
                """
            ).df()
        except duckdb.CatalogException:
            logger.warning("player_season_stats table not found")
            return pd.DataFrame(columns=["player_id", "player_name"])

    def get_all_nba_players_for_season(self, season: str) -> pd.DataFrame:
        """Get all players with NBA API stats for a season.

        Args:
            season: Season identifier (e.g., "2024-25").

        Returns:
            DataFrame with player stats.
        """
        conn = self.connect()

        try:
            return conn.execute(
                """
                SELECT
                    player_id,
                    player_name,
                    games_played,
                    minutes_played,
                    pts,
                    reb,
                    ast,
                    stl,
                    blk,
                    tov,
                    pf,
                    fgm,
                    fga,
                    fg3m,
                    fg3a,
                    ftm,
                    fta,
                    oreb,
                    dreb
                FROM player_season_stats
                WHERE season_id = ?
                  AND season_type = 'Regular Season'
                ORDER BY pts DESC
                """,
                [season],
            ).df()
        except duckdb.CatalogException:
            logger.warning("player_season_stats table not found")
            return pd.DataFrame()

    def get_all_br_players_for_season(self, season: str) -> pd.DataFrame:
        """Get all players with BR stats for a season.

        Args:
            season: Season identifier (e.g., "2024-25").

        Returns:
            DataFrame with player stats.
        """
        conn = self.connect()

        try:
            season_year = int(season.split("-")[0]) + 1

            return conn.execute(
                """
                SELECT
                    player_name,
                    games_played,
                    minutes_played,
                    pts,
                    reb,
                    ast,
                    stl,
                    blk,
                    tov,
                    pf,
                    fgm,
                    fga,
                    fg3m,
                    fg3a,
                    ftm,
                    fta,
                    oreb,
                    dreb
                FROM br_player_season_totals
                WHERE season_end_year = ?
                ORDER BY pts DESC
                """,
                [season_year],
            ).df()
        except duckdb.CatalogException:
            logger.warning("br_player_season_totals table not found")
            return pd.DataFrame()

    # =========================================================================
    # RECONCILIATION METHODS
    # =========================================================================

    def reconcile_player_season_stats(
        self,
        player_id: int,
        season: str,
        stats_to_compare: list[str] | None = None,
    ) -> list[Discrepancy]:
        """Compare player season stats between NBA API and BR.

        Args:
            player_id: NBA player ID.
            season: Season identifier (e.g., "2024-25").
            stats_to_compare: List of stat names to compare. If None, uses all
                            available stats in STAT_COLUMN_MAPPING.

        Returns:
            List of Discrepancy objects for stats that exceed thresholds.
        """
        discrepancies: list[Discrepancy] = []

        # Get NBA API stats
        nba_stats = self.get_nba_player_season_stats(player_id, season)
        if nba_stats is None:
            logger.warning(f"No NBA API stats found for player {player_id} in {season}")
            return discrepancies

        player_name = nba_stats.get("player_name", str(player_id))

        # Get BR stats using player name
        br_stats = self.get_br_player_season_stats(player_name, season)
        if br_stats is None:
            logger.warning(f"No BR stats found for {player_name} in {season}")
            return discrepancies

        # Determine which stats to compare
        if stats_to_compare is None:
            stats_to_compare = list(STAT_COLUMN_MAPPING.keys())

        # Compare each stat
        for stat_name in stats_to_compare:
            nba_value = nba_stats.get(stat_name)
            br_value = br_stats.get(stat_name)

            # Skip if both are None
            if nba_value is None and br_value is None:
                continue

            # Calculate difference
            difference, pct_difference = self.calculate_difference(nba_value, br_value)

            # Check against threshold
            threshold = self.get_threshold(stat_name)
            if difference > threshold:
                severity = self.classify_severity(difference, pct_difference, stat_name)

                discrepancies.append(
                    Discrepancy(
                        entity_type=EntityType.PLAYER.value,
                        entity_id=player_id,
                        entity_name=player_name,
                        stat_name=stat_name,
                        nba_value=nba_value,
                        br_value=br_value,
                        difference=difference,
                        pct_difference=pct_difference,
                        severity=severity,
                        season=season,
                        context={"source": "season_stats"},
                    )
                )

        return discrepancies

    def reconcile_all_players(
        self,
        season: str,
        threshold: float | None = None,
        min_games: int = 10,
        stats_to_compare: list[str] | None = None,
    ) -> dict[str, Any]:
        """Reconcile stats for all players in a season.

        Args:
            season: Season identifier (e.g., "2024-25").
            threshold: Global threshold override. If provided, uses this for
                      all stats instead of stat-specific thresholds.
            min_games: Minimum games played to include player in reconciliation.
            stats_to_compare: List of stat names to compare.

        Returns:
            Dictionary with reconciliation summary and discrepancies.
        """
        start_time = datetime.now(UTC)
        all_discrepancies: list[Discrepancy] = []

        # Override thresholds if global threshold provided
        if threshold is not None:
            original_thresholds = self.thresholds.copy()
            self.thresholds = dict.fromkeys(self.thresholds, threshold)

        try:
            # Get all NBA players for the season
            nba_players = self.get_all_nba_players_for_season(season)
            if nba_players.empty:
                logger.warning(f"No NBA API data found for season {season}")
                return {
                    "status": ReconciliationStatus.NO_DATA.value,
                    "season": season,
                    "message": "No NBA API data found",
                }

            # Filter by minimum games
            nba_players = nba_players[nba_players["games_played"] >= min_games]
            logger.info(
                f"Reconciling {len(nba_players)} players with {min_games}+ games"
            )

            # Get BR players for name matching
            br_players = self.get_all_br_players_for_season(season)
            br_names = (
                set(br_players["player_name"].str.lower())
                if not br_players.empty
                else set()
            )

            entities_with_discrepancies = set()
            entities_matched = 0

            # Process each player
            for _, player_row in nba_players.iterrows():
                player_id = player_row["player_id"]
                player_name = player_row["player_name"]

                # Check if player exists in BR data
                if player_name.lower() not in br_names:
                    logger.debug(f"Player {player_name} not found in BR data")
                    continue

                # Reconcile this player
                player_discrepancies = self.reconcile_player_season_stats(
                    player_id=int(player_id),
                    season=season,
                    stats_to_compare=stats_to_compare,
                )

                if player_discrepancies:
                    all_discrepancies.extend(player_discrepancies)
                    entities_with_discrepancies.add(player_id)
                else:
                    entities_matched += 1

            # Build summary
            end_time = datetime.now(UTC)
            duration = (end_time - start_time).total_seconds()

            # Count discrepancies by severity and stat
            severity_counts: dict[str, int] = {}
            stat_counts: dict[str, int] = {}
            for d in all_discrepancies:
                severity_counts[d.severity] = severity_counts.get(d.severity, 0) + 1
                stat_counts[d.stat_name] = stat_counts.get(d.stat_name, 0) + 1

            summary = ReconciliationSummary(
                season=season,
                status=ReconciliationStatus.SUCCESS.value
                if not all_discrepancies
                else ReconciliationStatus.PARTIAL.value,
                total_entities=len(nba_players),
                entities_matched=entities_matched,
                entities_with_discrepancies=len(entities_with_discrepancies),
                total_discrepancies=len(all_discrepancies),
                discrepancies_by_severity=severity_counts,
                discrepancies_by_stat=stat_counts,
                duration_seconds=duration,
            )

            return {
                "summary": summary.to_dict(),
                "discrepancies": [d.to_dict() for d in all_discrepancies],
            }

        finally:
            # Restore original thresholds
            if threshold is not None:
                self.thresholds = original_thresholds

    def reconcile_game_stats(
        self,
        game_id: str,
    ) -> list[Discrepancy]:
        """Compare box score stats between sources for a game.

        This method compares game-level statistics between NBA API box scores
        and Basketball Reference game data.

        Args:
            game_id: NBA game ID (10-digit string).

        Returns:
            List of Discrepancy objects for stats that exceed thresholds.

        Note:
            This requires both br_player_box_scores and player_game_stats tables.
        """
        discrepancies: list[Discrepancy] = []
        conn = self.connect()

        try:
            # Get NBA API box score
            nba_boxscore = conn.execute(
                """
                SELECT
                    player_id,
                    player_name,
                    pts,
                    reb,
                    ast,
                    stl,
                    blk,
                    tov,
                    fgm,
                    fga,
                    fg3m,
                    fg3a,
                    ftm,
                    fta
                FROM player_game_stats
                WHERE game_id = ?
                """,
                [game_id],
            ).df()

            if nba_boxscore.empty:
                logger.warning(f"No NBA API box score found for game {game_id}")
                return discrepancies

            # Get game date for BR lookup
            game_info = conn.execute(
                """
                SELECT game_date
                FROM game_gold
                WHERE game_id = ?
                """,
                [game_id],
            ).fetchone()

            if not game_info:
                logger.warning(f"Game date not found for {game_id}")
                return discrepancies

            # Note: BR box score reconciliation requires br_player_box_scores table
            # which may not always be available. Log and return empty for now.
            logger.info(
                f"Game reconciliation for {game_id} - "
                f"found {len(nba_boxscore)} players in NBA API data"
            )

            # Future: Add BR box score comparison when table is available

        except duckdb.CatalogException as e:
            logger.warning(f"Required table not found: {e}")

        return discrepancies

    # =========================================================================
    # REPORT GENERATION
    # =========================================================================

    def generate_report(
        self,
        discrepancies: list[Discrepancy],
        output_format: str = "dict",
    ) -> dict[str, Any] | pd.DataFrame | str:
        """Generate a reconciliation report.

        Args:
            discrepancies: List of Discrepancy objects.
            output_format: Output format - 'dict', 'dataframe', 'json', or 'markdown'.

        Returns:
            Report in the specified format.

        Raises:
            ValueError: If output_format is not recognized.
        """
        if output_format == "dict":
            return self._report_as_dict(discrepancies)
        if output_format == "dataframe":
            return self._report_as_dataframe(discrepancies)
        if output_format == "json":
            return json.dumps(self._report_as_dict(discrepancies), indent=2)
        if output_format == "markdown":
            return self._report_as_markdown(discrepancies)
        raise ValueError(
            f"Unknown output format: {output_format}. "
            f"Use 'dict', 'dataframe', 'json', or 'markdown'."
        )

    def _report_as_dict(self, discrepancies: list[Discrepancy]) -> dict[str, Any]:
        """Generate report as dictionary."""
        severity_counts = {}
        stat_counts = {}
        entity_counts = {}

        for d in discrepancies:
            severity_counts[d.severity] = severity_counts.get(d.severity, 0) + 1
            stat_counts[d.stat_name] = stat_counts.get(d.stat_name, 0) + 1
            entity_counts[d.entity_id] = entity_counts.get(d.entity_id, 0) + 1

        return {
            "total_discrepancies": len(discrepancies),
            "unique_entities": len(entity_counts),
            "by_severity": severity_counts,
            "by_stat": stat_counts,
            "discrepancies": [d.to_dict() for d in discrepancies],
        }

    def _report_as_dataframe(self, discrepancies: list[Discrepancy]) -> pd.DataFrame:
        """Generate report as DataFrame."""
        if not discrepancies:
            return pd.DataFrame()

        return pd.DataFrame([d.to_dict() for d in discrepancies])

    def _report_as_markdown(self, discrepancies: list[Discrepancy]) -> str:
        """Generate report as Markdown string."""
        if not discrepancies:
            return "# Reconciliation Report\n\nNo discrepancies found."

        lines = [
            "# Reconciliation Report",
            "",
            f"**Total Discrepancies:** {len(discrepancies)}",
            "",
            "## Summary by Severity",
            "",
        ]

        # Group by severity
        severity_groups: dict[str, list[Discrepancy]] = {}
        for d in discrepancies:
            if d.severity not in severity_groups:
                severity_groups[d.severity] = []
            severity_groups[d.severity].append(d)

        for severity in [
            Severity.CRITICAL.value,
            Severity.HIGH.value,
            Severity.MEDIUM.value,
            Severity.LOW.value,
        ]:
            if severity in severity_groups:
                count = len(severity_groups[severity])
                lines.append(f"- **{severity.upper()}**: {count}")

        lines.extend(
            [
                "",
                "## Discrepancies",
                "",
                "| Entity | Stat | NBA | BR | Diff | % Diff | Severity |",
                "|--------|------|-----|----|----- |--------|----------|",
            ]
        )

        for d in sorted(
            discrepancies,
            key=lambda x: (
                [
                    Severity.CRITICAL.value,
                    Severity.HIGH.value,
                    Severity.MEDIUM.value,
                    Severity.LOW.value,
                ].index(x.severity),
                -x.pct_difference,
            ),
        ):
            lines.append(
                f"| {d.entity_name} | {d.stat_name} | {d.nba_value} | {d.br_value} | "
                f"{d.difference:.1f} | {d.pct_difference:.1f}% | {d.severity} |"
            )

        return "\n".join(lines)


# =============================================================================
# CLI
# =============================================================================


def main() -> int:
    """Run reconciliation from command line.

    Returns:
        Exit code (0 for success, 1 for errors).
    """
    parser = argparse.ArgumentParser(
        description="Reconcile NBA data between API and Basketball Reference sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Reconcile all players for a season
    python -m src.scripts.populate.reconciliation --season 2024-25

    # Reconcile specific player
    python -m src.scripts.populate.reconciliation --player-id 201566 --season 2024-25

    # Use custom threshold and save report
    python -m src.scripts.populate.reconciliation --season 2024-25 --threshold 0.5 --output report.json

    # Generate markdown report
    python -m src.scripts.populate.reconciliation --season 2024-25 --format markdown --output report.md
        """,
    )

    parser.add_argument(
        "--season",
        type=str,
        required=True,
        help="Season to reconcile (e.g., 2024-25)",
    )
    parser.add_argument(
        "--player-id",
        type=int,
        help="Specific player ID to reconcile",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        help="Global discrepancy threshold (overrides stat-specific thresholds)",
    )
    parser.add_argument(
        "--min-games",
        type=int,
        default=10,
        help="Minimum games played to include player (default: 10)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output file path for report",
    )
    parser.add_argument(
        "--format",
        "-f",
        type=str,
        choices=["json", "markdown", "csv"],
        default="json",
        help="Output format (default: json)",
    )
    parser.add_argument(
        "--db",
        type=str,
        help="Database path (default: from config)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Configure logging
    configure_logging()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 70)
    logger.info("NBA DATA RECONCILIATION")
    logger.info("=" * 70)

    try:
        with DataReconciler(db_path=args.db) as reconciler:
            if args.player_id:
                # Single player reconciliation
                logger.info(f"Reconciling player {args.player_id} for {args.season}")
                discrepancies = reconciler.reconcile_player_season_stats(
                    player_id=args.player_id,
                    season=args.season,
                )

                if discrepancies:
                    logger.info(f"Found {len(discrepancies)} discrepancies:")
                    for d in discrepancies:
                        logger.info(f"  {d}")
                else:
                    logger.info("No discrepancies found")

                # Generate report
                if args.format == "csv":
                    report = reconciler.generate_report(discrepancies, "dataframe")
                elif args.format == "markdown":
                    report = reconciler.generate_report(discrepancies, "markdown")
                else:
                    report = reconciler.generate_report(discrepancies, "json")

            else:
                # All players reconciliation
                logger.info(f"Reconciling all players for {args.season}")
                result = reconciler.reconcile_all_players(
                    season=args.season,
                    threshold=args.threshold,
                    min_games=args.min_games,
                )

                summary = result.get("summary", {})
                discrepancies_data = result.get("discrepancies", [])

                logger.info("=" * 70)
                logger.info("RECONCILIATION SUMMARY")
                logger.info("=" * 70)
                logger.info(f"Status: {summary.get('status')}")
                logger.info(f"Total entities: {summary.get('total_entities')}")
                logger.info(f"Entities matched: {summary.get('entities_matched')}")
                logger.info(
                    f"Entities with discrepancies: {summary.get('entities_with_discrepancies')}"
                )
                logger.info(
                    f"Total discrepancies: {summary.get('total_discrepancies')}"
                )
                logger.info(f"Duration: {summary.get('duration_seconds', 0):.2f}s")

                if summary.get("discrepancies_by_severity"):
                    logger.info("By severity:")
                    for sev, count in summary["discrepancies_by_severity"].items():
                        logger.info(f"  {sev}: {count}")

                # Prepare output
                if args.format == "csv":
                    # Convert discrepancies to DataFrame for CSV
                    report = pd.DataFrame(discrepancies_data)
                elif args.format == "markdown":
                    # Reconstruct Discrepancy objects for markdown generation
                    disc_objects = [
                        Discrepancy(**{k: v for k, v in d.items() if k != "context"})
                        for d in discrepancies_data
                    ]
                    report = reconciler._report_as_markdown(disc_objects)
                else:
                    report = json.dumps(result, indent=2)

            # Output report
            if args.output:
                output_path = Path(args.output)
                if args.format == "csv" and isinstance(report, pd.DataFrame):
                    report.to_csv(output_path, index=False)
                else:
                    output_path.write_text(str(report))
                logger.info(f"Report saved to: {output_path}")
            # Print to stdout if no output file
            elif isinstance(report, pd.DataFrame):
                print(report.to_string())
            else:
                print(report)

        return 0

    except Exception as e:
        logger.exception(f"Reconciliation failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
