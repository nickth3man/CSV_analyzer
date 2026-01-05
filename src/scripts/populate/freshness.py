"""Data freshness monitoring for NBA data pipeline.

This module provides utilities to:
- Track when data tables were last updated
- Define freshness thresholds per table
- Identify stale data needing refresh
- Generate freshness reports
- Support automatic refresh scheduling

The freshness monitoring system helps ensure data quality by tracking when
each table was last updated and flagging tables that have become stale based
on configurable thresholds.

Usage:
    from src.scripts.populate.freshness import FreshnessMonitor, TableFreshness

    # Initialize monitor
    monitor = FreshnessMonitor(db_path)

    # Get freshness status for all tables
    report = monitor.get_freshness_report()

    # Get stale tables
    stale = monitor.get_stale_tables()

    # Refresh stale tables (dry run)
    result = monitor.refresh_stale_tables(dry_run=True)

CLI Usage:
    python -m src.scripts.populate.freshness --report
    python -m src.scripts.populate.freshness --stale
    python -m src.scripts.populate.freshness --refresh --dry-run
    python -m src.scripts.populate.freshness --table player_game_stats
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import pandas as pd

from src.scripts.populate.config import get_db_path
from src.scripts.populate.helpers import configure_logging, format_duration


if TYPE_CHECKING:
    from collections.abc import Callable


logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS AND DATA CLASSES
# =============================================================================


class Priority(str, Enum):
    """Priority levels for data refresh."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class FreshnessStatus(str, Enum):
    """Status of table freshness."""

    FRESH = "fresh"
    STALE = "stale"
    UNKNOWN = "unknown"
    EMPTY = "empty"


@dataclass
class TableFreshness:
    """Freshness status for a single table.

    Attributes:
        table_name: Name of the database table
        last_updated: Timestamp of last update (None if unknown)
        freshness_threshold: Maximum age before considered stale
        is_stale: Whether the table is currently stale
        staleness_hours: Hours since last update (0 if fresh or unknown)
        record_count: Number of records in the table
        priority: Refresh priority level
        status: Freshness status (fresh, stale, unknown, empty)
        update_column: Column used to determine last update
    """

    table_name: str
    last_updated: datetime | None
    freshness_threshold: timedelta
    is_stale: bool
    staleness_hours: float
    record_count: int
    priority: str
    status: str = FreshnessStatus.UNKNOWN.value
    update_column: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        data = asdict(self)
        # Convert datetime to ISO string
        if data["last_updated"]:
            data["last_updated"] = data["last_updated"].isoformat()
        # Convert timedelta to hours
        data["freshness_threshold_hours"] = (
            data["freshness_threshold"].total_seconds() / 3600
        )
        del data["freshness_threshold"]
        return data

    def __str__(self) -> str:
        """Human-readable string representation."""
        status_emoji = "🔴" if self.is_stale else "🟢"
        last_update_str = (
            self.last_updated.strftime("%Y-%m-%d %H:%M")
            if self.last_updated
            else "Never"
        )
        return (
            f"{status_emoji} {self.table_name}: "
            f"Last updated {last_update_str}, "
            f"{self.staleness_hours:.1f}h old, "
            f"{self.record_count:,} records [{self.priority}]"
        )


@dataclass
class RefreshPlan:
    """Plan for refreshing stale tables.

    Attributes:
        tables: List of tables to refresh in order
        total_tables: Total number of tables to refresh
        estimated_duration: Estimated time to complete refresh
        by_priority: Tables grouped by priority
        dependencies: Table dependencies (if any)
    """

    tables: list[str]
    total_tables: int
    estimated_duration_minutes: float
    by_priority: dict[str, list[str]]
    dependencies: dict[str, list[str]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return asdict(self)


# =============================================================================
# TABLE CONFIGURATION
# =============================================================================

# Mapping from table names to update timestamp columns
# Tables may use different columns to track when data was added/updated
UPDATE_COLUMN_MAP: dict[str, str | None] = {
    # Tables with explicit updated_at columns
    "player": "updated_at",
    "team": "updated_at",
    "game_gold": "updated_at",
    # Tables using game_date as freshness indicator
    "player_game_stats": "game_date",
    "league_game_log": "game_date",
    "play_by_play": "game_date",
    "shot_chart_detail": "game_date",
    "win_probability": "game_date",
    "game_rotation": "game_date",
    "br_player_box_scores": "game_date",
    # Tables using season-based updates
    "player_season_stats": "season_id",
    "player_tracking_stats": "season_id",
    "synergy_playtypes": "season_id",
    "lineup_stats": "season_id",
    "matchup_stats": "season_id",
    "player_splits": "season_id",
    "estimated_metrics": "season_id",
    "league_leaders": "season_id",
    "br_season_stats": "season_end_year",
    # Tables using created_at or similar
    "common_player_info": "created_at",
    "team_details": "created_at",
    "franchise_history": "created_at",
    "franchise_leaders": "created_at",
    "all_time_leaders": "created_at",
    "draft_history": "created_at",
    "draft_combine_stats": "created_at",
    # BR tables
    "br_schedule": "game_date",
    "br_standings": "updated_at",
    # Default fallback - will be determined dynamically
    "default": None,
}

# Estimated refresh duration per table type (in minutes)
REFRESH_DURATION_ESTIMATES: dict[str, float] = {
    "player_game_stats": 15.0,
    "play_by_play": 30.0,
    "shot_chart_detail": 20.0,
    "league_game_log": 5.0,
    "player_tracking_stats": 10.0,
    "synergy_playtypes": 10.0,
    "lineup_stats": 15.0,
    "matchup_stats": 10.0,
    "player_season_stats": 5.0,
    "common_player_info": 10.0,
    "default": 5.0,
}

# Table dependencies - some tables should be refreshed before others
TABLE_DEPENDENCIES: dict[str, list[str]] = {
    "player_game_stats": ["player", "team", "league_game_log"],
    "player_season_stats": ["player", "team"],
    "play_by_play": ["league_game_log"],
    "shot_chart_detail": ["league_game_log", "player"],
    "lineup_stats": ["player", "team", "league_game_log"],
    "matchup_stats": ["player", "team"],
    "player_splits": ["player", "player_season_stats"],
}


# =============================================================================
# FRESHNESS MONITOR CLASS
# =============================================================================


class FreshnessMonitor:
    """Monitor data freshness and trigger updates.

    This class tracks when each table was last updated and identifies tables
    that have become stale based on configurable thresholds. It can generate
    reports and schedule refreshes in priority order.

    Attributes:
        db_path: Path to the DuckDB database
        freshness_thresholds: Table-specific freshness thresholds
        priority_map: Table-to-priority mappings

    Example:
        >>> monitor = FreshnessMonitor("nba.duckdb")
        >>> stale = monitor.get_stale_tables()
        >>> for table in stale:
        ...     print(f"{table.table_name}: {table.staleness_hours:.1f}h stale")
    """

    # Default freshness thresholds
    FRESHNESS_THRESHOLDS: dict[str, timedelta] = {
        # Live data - very fresh
        "live_scoreboard": timedelta(minutes=5),
        "live_boxscore": timedelta(minutes=5),
        # In-season game data - daily
        "player_game_stats": timedelta(hours=6),
        "league_game_log": timedelta(hours=6),
        "play_by_play": timedelta(hours=12),
        "shot_chart_detail": timedelta(hours=12),
        "win_probability": timedelta(hours=12),
        "game_rotation": timedelta(hours=12),
        # Aggregate stats - weekly
        "player_tracking_stats": timedelta(days=1),
        "synergy_playtypes": timedelta(days=1),
        "lineup_stats": timedelta(days=1),
        "matchup_stats": timedelta(days=1),
        "player_splits": timedelta(days=1),
        "estimated_metrics": timedelta(days=1),
        "league_leaders": timedelta(days=1),
        # Static/slow-changing - monthly or seasonal
        "player": timedelta(days=7),
        "team": timedelta(days=30),
        "common_player_info": timedelta(days=7),
        "team_details": timedelta(days=30),
        "franchise_history": timedelta(days=30),
        "franchise_leaders": timedelta(days=30),
        "all_time_leaders": timedelta(days=30),
        "draft_history": timedelta(days=90),
        "draft_combine_stats": timedelta(days=365),
        # BR data - daily during season
        "br_schedule": timedelta(days=1),
        "br_standings": timedelta(days=1),
        "br_player_box_scores": timedelta(days=1),
        "br_season_stats": timedelta(days=7),
        # Gold/derived tables
        "game_gold": timedelta(hours=6),
        "player_season_stats": timedelta(days=1),
        # Default
        "default": timedelta(days=1),
    }

    PRIORITY_MAP: dict[str, str] = {
        "live_": Priority.CRITICAL.value,
        "player_game_stats": Priority.HIGH.value,
        "league_game_log": Priority.HIGH.value,
        "game_gold": Priority.HIGH.value,
        "standings": Priority.HIGH.value,
        "schedule": Priority.HIGH.value,
        "tracking": Priority.MEDIUM.value,
        "synergy": Priority.MEDIUM.value,
        "lineup": Priority.MEDIUM.value,
        "matchup": Priority.MEDIUM.value,
        "splits": Priority.MEDIUM.value,
        "leaders": Priority.MEDIUM.value,
        "player_season": Priority.MEDIUM.value,
        "default": Priority.LOW.value,
    }

    def __init__(
        self,
        db_path: str | Path | None = None,
        freshness_thresholds: dict[str, timedelta] | None = None,
    ) -> None:
        """Initialize the FreshnessMonitor.

        Args:
            db_path: Path to DuckDB database. If None, uses default from config.
            freshness_thresholds: Custom freshness thresholds per table.
                                 Merged with defaults.
        """
        self.db_path = str(db_path) if db_path else str(get_db_path())
        self._conn: duckdb.DuckDBPyConnection | None = None

        # Merge custom thresholds with defaults
        self.freshness_thresholds = {**self.FRESHNESS_THRESHOLDS}
        if freshness_thresholds:
            self.freshness_thresholds.update(freshness_thresholds)

        # Refresh callbacks registry
        self._refresh_callbacks: dict[str, Callable[[], bool]] = {}

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection.

        Returns:
            Active DuckDB connection (read-only).
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

    def __enter__(self) -> FreshnessMonitor:
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _get_threshold(self, table_name: str) -> timedelta:
        """Get freshness threshold for a table.

        Args:
            table_name: Name of the table.

        Returns:
            Freshness threshold timedelta.
        """
        if table_name in self.freshness_thresholds:
            return self.freshness_thresholds[table_name]

        # Check for prefix matches
        for prefix, threshold in self.freshness_thresholds.items():
            if prefix != "default" and table_name.startswith(prefix):
                return threshold

        return self.freshness_thresholds.get("default", timedelta(days=1))

    def _get_priority(self, table_name: str) -> str:
        """Get priority level for a table.

        Args:
            table_name: Name of the table.

        Returns:
            Priority string (critical, high, medium, low).
        """
        # Check exact match first
        if table_name in self.PRIORITY_MAP:
            return self.PRIORITY_MAP[table_name]

        # Check for prefix/substring matches
        for key, priority in self.PRIORITY_MAP.items():
            if key != "default" and key in table_name:
                return priority

        return self.PRIORITY_MAP.get("default", Priority.LOW.value)

    def _get_update_column(self, table_name: str) -> str | None:
        """Get the column used to determine last update time.

        Args:
            table_name: Name of the table.

        Returns:
            Column name or None if not configured.
        """
        return UPDATE_COLUMN_MAP.get(table_name, UPDATE_COLUMN_MAP.get("default"))

    def _detect_update_column(self, table_name: str) -> str | None:
        """Auto-detect the update column for a table.

        Looks for common timestamp column patterns.

        Args:
            table_name: Name of the table.

        Returns:
            Detected column name or None.
        """
        conn = self.connect()

        try:
            # Get table columns
            result = conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = ?
                """,
                [table_name],
            ).fetchall()

            columns = [row[0].lower() for row in result]

            # Priority order for update columns
            candidates = [
                "updated_at",
                "update_date",
                "modified_at",
                "last_updated",
                "created_at",
                "create_date",
                "game_date",
                "date",
            ]

            for candidate in candidates:
                if candidate in columns:
                    return candidate

        except Exception as e:
            logger.debug(f"Could not detect update column for {table_name}: {e}")

        return None

    def _get_last_updated(self, table_name: str) -> tuple[datetime | None, str | None]:
        """Get the last update timestamp for a table.

        Args:
            table_name: Name of the table.

        Returns:
            Tuple of (timestamp, column_name) or (None, None) if unknown.
        """
        conn = self.connect()

        # Try configured column first
        update_col = self._get_update_column(table_name)
        if not update_col:
            update_col = self._detect_update_column(table_name)

        if not update_col:
            return None, None

        try:
            # Try to get max value from update column
            result = conn.execute(
                f"""
                SELECT MAX("{update_col}") as last_update
                FROM "{table_name}"
                """
            ).fetchone()

            if result and result[0]:
                last_update = result[0]

                # Handle different return types
                if isinstance(last_update, datetime):
                    # Ensure timezone-aware
                    if last_update.tzinfo is None:
                        last_update = last_update.replace(tzinfo=UTC)
                    return last_update, update_col

                if isinstance(last_update, str):
                    # Try to parse date/datetime strings
                    for fmt in [
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d",
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S.%f",
                    ]:
                        try:
                            parsed = datetime.strptime(
                                last_update[:19], fmt[:19]
                            ).replace(tzinfo=UTC)
                            return parsed, update_col
                        except ValueError:
                            continue

                    # Handle season format (e.g., "2024-25")
                    if "-" in last_update and len(last_update) == 7:
                        # Convert season to approximate date
                        year = int(last_update[:4])
                        # Assume current season data is fresh
                        return datetime(year, 10, 1, tzinfo=UTC), update_col

        except Exception as e:
            logger.debug(f"Could not get last update for {table_name}: {e}")

        return None, update_col

    # =========================================================================
    # MAIN API METHODS
    # =========================================================================

    def get_all_tables(self) -> list[str]:
        """Get all population tables in the database.

        Returns:
            List of table names, excluding system tables.
        """
        conn = self.connect()

        try:
            result = conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            ).fetchall()

            # Filter out internal/system tables
            excluded_prefixes = ("pg_", "sqlite_", "_")
            excluded_tables = {"information_schema"}

            return [
                row[0]
                for row in result
                if not row[0].startswith(excluded_prefixes)
                and row[0] not in excluded_tables
            ]

        except Exception as e:
            logger.exception(f"Failed to get tables: {e}")
            return []

    def get_table_freshness(self, table_name: str) -> TableFreshness:
        """Get freshness status for a single table.

        Args:
            table_name: Name of the table to check.

        Returns:
            TableFreshness object with status information.
        """
        conn = self.connect()
        now = datetime.now(UTC)

        # Get threshold and priority
        threshold = self._get_threshold(table_name)
        priority = self._get_priority(table_name)

        # Get record count
        try:
            count_result = conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            ).fetchone()
            record_count = count_result[0] if count_result else 0
        except Exception:
            record_count = 0

        # Handle empty tables
        if record_count == 0:
            return TableFreshness(
                table_name=table_name,
                last_updated=None,
                freshness_threshold=threshold,
                is_stale=True,
                staleness_hours=0.0,
                record_count=0,
                priority=priority,
                status=FreshnessStatus.EMPTY.value,
                update_column=None,
            )

        # Get last update time
        last_updated, update_col = self._get_last_updated(table_name)

        if last_updated is None:
            return TableFreshness(
                table_name=table_name,
                last_updated=None,
                freshness_threshold=threshold,
                is_stale=True,  # Assume stale if we can't determine
                staleness_hours=0.0,
                record_count=record_count,
                priority=priority,
                status=FreshnessStatus.UNKNOWN.value,
                update_column=update_col,
            )

        # Calculate staleness
        age = now - last_updated
        staleness_hours = age.total_seconds() / 3600
        is_stale = age > threshold

        status = (
            FreshnessStatus.STALE.value if is_stale else FreshnessStatus.FRESH.value
        )

        return TableFreshness(
            table_name=table_name,
            last_updated=last_updated,
            freshness_threshold=threshold,
            is_stale=is_stale,
            staleness_hours=staleness_hours,
            record_count=record_count,
            priority=priority,
            status=status,
            update_column=update_col,
        )

    def get_stale_tables(
        self,
        priority: str | None = None,
    ) -> list[TableFreshness]:
        """Get all tables that are stale and need refreshing.

        Args:
            priority: Filter by priority level (optional).

        Returns:
            List of TableFreshness objects for stale tables,
            sorted by priority and staleness.
        """
        all_tables = self.get_all_tables()
        stale_tables: list[TableFreshness] = []

        for table_name in all_tables:
            freshness = self.get_table_freshness(table_name)
            if freshness.is_stale and (
                priority is None or freshness.priority == priority
            ):
                stale_tables.append(freshness)

        # Sort by priority (critical first) then by staleness
        priority_order = {
            Priority.CRITICAL.value: 0,
            Priority.HIGH.value: 1,
            Priority.MEDIUM.value: 2,
            Priority.LOW.value: 3,
        }

        stale_tables.sort(
            key=lambda t: (
                priority_order.get(t.priority, 4),
                -t.staleness_hours,
            )
        )

        return stale_tables

    def get_freshness_report(
        self,
        include_fresh: bool = True,
    ) -> pd.DataFrame:
        """Generate a comprehensive freshness report.

        Args:
            include_fresh: Whether to include fresh tables (default: True).

        Returns:
            DataFrame with freshness status for all tables.
        """
        all_tables = self.get_all_tables()
        records: list[dict[str, Any]] = []

        for table_name in all_tables:
            freshness = self.get_table_freshness(table_name)

            if include_fresh or freshness.is_stale:
                record = {
                    "table_name": freshness.table_name,
                    "status": freshness.status,
                    "is_stale": freshness.is_stale,
                    "last_updated": freshness.last_updated,
                    "staleness_hours": round(freshness.staleness_hours, 1),
                    "threshold_hours": freshness.freshness_threshold.total_seconds()
                    / 3600,
                    "record_count": freshness.record_count,
                    "priority": freshness.priority,
                    "update_column": freshness.update_column,
                }
                records.append(record)

        df = pd.DataFrame(records)

        if not df.empty:
            # Sort by stale first, then priority, then staleness
            priority_order = {
                Priority.CRITICAL.value: 0,
                Priority.HIGH.value: 1,
                Priority.MEDIUM.value: 2,
                Priority.LOW.value: 3,
            }
            df["priority_order"] = df["priority"].map(priority_order)
            df = df.sort_values(
                by=["is_stale", "priority_order", "staleness_hours"],
                ascending=[False, True, False],
            )
            df = df.drop(columns=["priority_order"])

        return df

    def schedule_refresh(
        self,
        stale_tables: list[TableFreshness] | None = None,
    ) -> RefreshPlan:
        """Determine refresh order based on priority and dependencies.

        Args:
            stale_tables: List of stale tables to refresh.
                         If None, gets current stale tables.

        Returns:
            RefreshPlan with ordered tables and estimates.
        """
        if stale_tables is None:
            stale_tables = self.get_stale_tables()

        if not stale_tables:
            return RefreshPlan(
                tables=[],
                total_tables=0,
                estimated_duration_minutes=0.0,
                by_priority={},
                dependencies={},
            )

        # Group by priority
        by_priority: dict[str, list[str]] = {
            Priority.CRITICAL.value: [],
            Priority.HIGH.value: [],
            Priority.MEDIUM.value: [],
            Priority.LOW.value: [],
        }

        for table in stale_tables:
            if table.priority in by_priority:
                by_priority[table.priority].append(table.table_name)

        # Build dependency-aware refresh order
        ordered_tables: list[str] = []
        table_set = {t.table_name for t in stale_tables}

        # Process in priority order
        for priority in [
            Priority.CRITICAL.value,
            Priority.HIGH.value,
            Priority.MEDIUM.value,
            Priority.LOW.value,
        ]:
            for table_name in by_priority[priority]:
                # Add dependencies first (if they're also stale)
                deps = TABLE_DEPENDENCIES.get(table_name, [])
                for dep in deps:
                    if dep in table_set and dep not in ordered_tables:
                        ordered_tables.append(dep)

                if table_name not in ordered_tables:
                    ordered_tables.append(table_name)

        # Calculate estimated duration
        total_minutes = 0.0
        for table_name in ordered_tables:
            duration = REFRESH_DURATION_ESTIMATES.get(
                table_name, REFRESH_DURATION_ESTIMATES.get("default", 5.0)
            )
            total_minutes += duration

        # Get relevant dependencies
        dependencies = {
            table: TABLE_DEPENDENCIES.get(table, [])
            for table in ordered_tables
            if table in TABLE_DEPENDENCIES
        }

        return RefreshPlan(
            tables=ordered_tables,
            total_tables=len(ordered_tables),
            estimated_duration_minutes=total_minutes,
            by_priority={k: v for k, v in by_priority.items() if v},
            dependencies=dependencies,
        )

    def register_refresh_callback(
        self,
        table_name: str,
        callback: Callable[[], bool],
    ) -> None:
        """Register a callback function for refreshing a table.

        Args:
            table_name: Name of the table.
            callback: Function that performs the refresh.
                     Should return True on success, False on failure.
        """
        self._refresh_callbacks[table_name] = callback

    def refresh_stale_tables(
        self,
        dry_run: bool = True,
        tables: list[str] | None = None,
    ) -> dict[str, Any]:
        """Refresh all stale tables in priority order.

        Args:
            dry_run: If True, only report what would be refreshed.
            tables: Specific tables to refresh (optional).

        Returns:
            Dictionary with refresh results.
        """
        if tables:
            # Get freshness for specific tables
            stale_tables = [
                self.get_table_freshness(t)
                for t in tables
                if self.get_table_freshness(t).is_stale
            ]
        else:
            stale_tables = self.get_stale_tables()

        plan = self.schedule_refresh(stale_tables)

        result: dict[str, Any] = {
            "dry_run": dry_run,
            "plan": plan.to_dict(),
            "refreshed": [],
            "failed": [],
            "skipped": [],
        }

        if dry_run:
            logger.info("=" * 60)
            logger.info("REFRESH PLAN (DRY RUN)")
            logger.info("=" * 60)
            logger.info(f"Tables to refresh: {plan.total_tables}")
            logger.info(
                f"Estimated duration: {format_duration(plan.estimated_duration_minutes * 60)}"
            )

            if plan.by_priority:
                logger.info("\nBy priority:")
                for priority, tables_list in plan.by_priority.items():
                    logger.info(f"  {priority.upper()}: {', '.join(tables_list)}")

            if plan.dependencies:
                logger.info("\nDependencies:")
                for table, deps in plan.dependencies.items():
                    logger.info(f"  {table} depends on: {', '.join(deps)}")

            logger.info("\nRefresh order:")
            for i, table in enumerate(plan.tables, 1):
                logger.info(f"  {i}. {table}")

            return result

        # Actually perform refresh
        logger.info("=" * 60)
        logger.info("REFRESHING STALE TABLES")
        logger.info("=" * 60)

        for table_name in plan.tables:
            if table_name in self._refresh_callbacks:
                logger.info(f"Refreshing {table_name}...")
                try:
                    success = self._refresh_callbacks[table_name]()
                    if success:
                        result["refreshed"].append(table_name)
                        logger.info(f"  ✓ {table_name} refreshed successfully")
                    else:
                        result["failed"].append(table_name)
                        logger.warning(f"  ✗ {table_name} refresh failed")
                except Exception as e:
                    result["failed"].append(table_name)
                    logger.exception(f"  ✗ {table_name} refresh error: {e}")
            else:
                result["skipped"].append(table_name)
                logger.warning(f"  ⚠ {table_name} skipped (no refresh callback)")

        return result

    def generate_markdown_report(self) -> str:
        """Generate a markdown-formatted freshness report.

        Returns:
            Markdown string with freshness status.
        """
        df = self.get_freshness_report()

        if df.empty:
            return "# Data Freshness Report\n\nNo tables found."

        stale_count = df["is_stale"].sum()
        fresh_count = len(df) - stale_count

        lines = [
            "# Data Freshness Report",
            "",
            f"**Generated:** {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}",
            "",
            "## Summary",
            "",
            f"- **Total Tables:** {len(df)}",
            f"- **Fresh:** {fresh_count} 🟢",
            f"- **Stale:** {stale_count} 🔴",
            "",
        ]

        # Stale tables section
        stale_df = df[df["is_stale"]]
        if not stale_df.empty:
            lines.extend(
                [
                    "## Stale Tables",
                    "",
                    "| Table | Priority | Last Updated | Staleness (h) | Records |",
                    "|-------|----------|--------------|---------------|---------|",
                ]
            )

            for _, row in stale_df.iterrows():
                last_updated = (
                    row["last_updated"].strftime("%Y-%m-%d %H:%M")
                    if pd.notna(row["last_updated"])
                    else "Unknown"
                )
                lines.append(
                    f"| {row['table_name']} | {row['priority']} | "
                    f"{last_updated} | {row['staleness_hours']:.1f} | "
                    f"{row['record_count']:,} |"
                )

            lines.append("")

        # Fresh tables section
        fresh_df = df[~df["is_stale"]]
        if not fresh_df.empty:
            lines.extend(
                [
                    "## Fresh Tables",
                    "",
                    "| Table | Priority | Last Updated | Age (h) | Records |",
                    "|-------|----------|--------------|---------|---------|",
                ]
            )

            for _, row in fresh_df.iterrows():
                last_updated = (
                    row["last_updated"].strftime("%Y-%m-%d %H:%M")
                    if pd.notna(row["last_updated"])
                    else "Unknown"
                )
                lines.append(
                    f"| {row['table_name']} | {row['priority']} | "
                    f"{last_updated} | {row['staleness_hours']:.1f} | "
                    f"{row['record_count']:,} |"
                )

        return "\n".join(lines)


# =============================================================================
# CLI
# =============================================================================


def main() -> int:
    """Run freshness monitoring from command line.

    Returns:
        Exit code (0 for success, 1 for errors).
    """
    parser = argparse.ArgumentParser(
        description="Monitor NBA data freshness and identify stale tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Generate full freshness report
    python -m src.scripts.populate.freshness --report

    # Show only stale tables
    python -m src.scripts.populate.freshness --stale

    # Check specific table
    python -m src.scripts.populate.freshness --table player_game_stats

    # Plan refresh (dry run)
    python -m src.scripts.populate.freshness --refresh --dry-run

    # Output as JSON
    python -m src.scripts.populate.freshness --stale --format json

    # Save report to file
    python -m src.scripts.populate.freshness --report --output freshness_report.md
        """,
    )

    # Action arguments (mutually exclusive)
    action_group = parser.add_argument_group("actions")
    action_group.add_argument(
        "--report",
        action="store_true",
        help="Generate comprehensive freshness report",
    )
    action_group.add_argument(
        "--stale",
        action="store_true",
        help="Show only stale tables",
    )
    action_group.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh stale tables",
    )
    action_group.add_argument(
        "--table",
        type=str,
        help="Check freshness of a specific table",
    )

    # Options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually run refreshes (default behavior for --refresh)",
    )
    parser.add_argument(
        "--priority",
        type=str,
        choices=["critical", "high", "medium", "low"],
        help="Filter by priority level",
    )
    parser.add_argument(
        "--format",
        "-f",
        type=str,
        choices=["table", "json", "markdown"],
        default="table",
        help="Output format (default: table)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output file path for report",
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

    # Default to report if no action specified
    if not any([args.report, args.stale, args.refresh, args.table]):
        args.report = True

    try:
        with FreshnessMonitor(db_path=args.db) as monitor:
            output: str = ""

            if args.table:
                # Check specific table
                freshness = monitor.get_table_freshness(args.table)

                if args.format == "json":
                    output = json.dumps(freshness.to_dict(), indent=2)
                elif args.format == "markdown":
                    status_emoji = "🔴" if freshness.is_stale else "🟢"
                    output = f"## {args.table} {status_emoji}\n\n"
                    output += f"- **Status:** {freshness.status}\n"
                    output += f"- **Last Updated:** {freshness.last_updated}\n"
                    output += (
                        f"- **Staleness:** {freshness.staleness_hours:.1f} hours\n"
                    )
                    output += f"- **Threshold:** {freshness.freshness_threshold.total_seconds() / 3600:.1f} hours\n"
                    output += f"- **Records:** {freshness.record_count:,}\n"
                    output += f"- **Priority:** {freshness.priority}\n"
                else:
                    output = str(freshness)

            elif args.stale:
                # Show stale tables
                stale_tables = monitor.get_stale_tables(priority=args.priority)

                if args.format == "json":
                    output = json.dumps([t.to_dict() for t in stale_tables], indent=2)
                elif args.format == "markdown":
                    lines = ["# Stale Tables", ""]
                    if not stale_tables:
                        lines.append("No stale tables found! 🎉")
                    else:
                        lines.append(
                            f"**Total:** {len(stale_tables)} tables need refresh\n"
                        )
                        lines.extend(f"- {t}" for t in stale_tables)
                    output = "\n".join(lines)
                elif not stale_tables:
                    output = "No stale tables found! 🎉"
                else:
                    lines = [f"Found {len(stale_tables)} stale tables:", ""]
                    lines.extend(f"  {t}" for t in stale_tables)
                    output = "\n".join(lines)

            elif args.refresh:
                # Refresh stale tables
                result = monitor.refresh_stale_tables(dry_run=args.dry_run or True)

                if args.format == "json":
                    output = json.dumps(result, indent=2)
                elif args.dry_run:
                    # Output is already logged during refresh
                    output = "Dry run complete. Use without --dry-run to execute."
                else:
                    refreshed = len(result.get("refreshed", []))
                    failed = len(result.get("failed", []))
                    skipped = len(result.get("skipped", []))
                    output = f"Refresh complete: {refreshed} refreshed, {failed} failed, {skipped} skipped"

            elif args.report:
                # Generate full report
                if args.format == "json":
                    df = monitor.get_freshness_report()
                    # Convert timestamps to strings for JSON
                    df_dict = df.to_dict(orient="records")
                    for record in df_dict:
                        if record.get("last_updated"):
                            record["last_updated"] = record["last_updated"].isoformat()
                    output = json.dumps(df_dict, indent=2)
                elif args.format == "markdown":
                    output = monitor.generate_markdown_report()
                else:
                    df = monitor.get_freshness_report()
                    output = df.to_string(index=False)

            # Output result
            if args.output:
                output_path = Path(args.output)
                output_path.write_text(output, encoding="utf-8")
                logger.info(f"Report saved to: {output_path}")
            else:
                print(output)

        return 0

    except Exception as e:
        logger.exception(f"Freshness monitoring failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
