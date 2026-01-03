"""Base class for NBA data population scripts.

This module provides a reusable base class that encapsulates common functionality
for all population scripts including:

- Database connection management
- Progress tracking and resumability
- Batch insertion with upsert support
- Logging and metrics collection
- Rate limiting coordination
- Season/season-type iteration patterns

Usage:
    class MyPopulator(BasePopulator):
        def get_data(self, **kwargs):
            # Fetch data from API
            return dataframe

        def transform_data(self, df):
            # Transform to match schema
            return transformed_df

        def get_table_name(self):
            return "my_table"

        def get_key_columns(self):
            return ["id"]

    populator = MyPopulator()
    populator.run(seasons=["2025-26"])

    # Using SeasonIteratorMixin:
    class MySeasonPopulator(SeasonIteratorMixin, BasePopulator):
        def process_season(self, season, season_type, **kwargs):
            # Process a single season/season_type combination
            pass
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypedDict

import duckdb
import pandas as pd

from src.scripts.maintenance.check_integrity import check_integrity
from src.scripts.populate.api_client import NBAClient, get_client
from src.scripts.populate.config import (
    ALL_SEASONS,
    CACHE_DIR,
    CURRENT_SEASON,
    ensure_cache_dir,
    get_db_path,
)
from src.scripts.populate.constants import SEASON_TYPE_MAP, SeasonType
from src.scripts.populate.database import DatabaseManager
from src.scripts.populate.helpers import load_json_file, save_json_file
from src.scripts.populate.validation import DataValidator


logger = logging.getLogger(__name__)


class ProgressState(TypedDict):
    """Typed structure for progress tracking payloads."""

    completed_items: list[str]
    last_item: str | None
    last_run: str | None
    errors: list[dict[str, Any]]


class PopulationMetrics:
    """Collects and reports metrics for a population run."""

    def __init__(self) -> None:
        """Initialize a PopulationMetrics instance and reset all counters and collections used to track a population run.

        Attributes:
            start_time (Optional[datetime]): Timestamp when the run started, or None if not started.
            end_time (Optional[datetime]): Timestamp when the run ended, or None if not stopped.
            records_fetched (int): Number of records fetched from the source.
            records_inserted (int): Number of records inserted into the database.
            records_updated (int): Number of records updated in the database.
            records_skipped (int): Number of records skipped (e.g., deduplicated or unsupported).
            api_calls (int): Count of API requests made during the run.
            errors (List[Dict[str, Any]]): Collected error entries; each entry includes details and a timestamp.
            warnings (List[str]): Collected warning messages.
        """
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.records_fetched: int = 0
        self.records_inserted: int = 0
        self.records_updated: int = 0
        self.records_skipped: int = 0
        self.api_calls: int = 0
        self.errors: list[dict[str, Any]] = []
        self.warnings: list[str] = []

    def start(self) -> None:
        """Record the start time of the population run.

        Sets the instance's `start_time` to the current date and time.
        """
        self.start_time = datetime.now(tz=UTC)

    def stop(self) -> None:
        """Record the end time of the population run.

        Sets the object's `end_time` to the current datetime.
        """
        self.end_time = datetime.now(tz=UTC)

    @property
    def duration_seconds(self) -> float:
        """Compute the elapsed time between recorded start and end timestamps.

        Returns:
            duration_seconds (float): Elapsed time in seconds; returns 0.0 if either start_time or end_time is not set.
        """
        if not self.start_time or not self.end_time:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()

    def add_error(self, error: str, context: dict | None = None) -> None:
        """Append an error entry with timestamp and optional context to the instance's errors list.

        Parameters:
            error (str): Error message or identifier to record.
            context (Optional[Dict]): Additional structured context for the error; stored as an empty dict if omitted.
        """
        self.errors.append(
            {
                "error": error,
                "context": context or {},
                "timestamp": datetime.now(tz=UTC).isoformat(),
            },
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize collected population metrics into a JSON-serializable dictionary.

        The returned dictionary contains ISO 8601 strings for `start_time` and `end_time` or `None` if absent, numeric counts for duration and record/API metrics, the total `error_count`, and up to the first 10 `errors` and `warnings`.

        Returns:
            dict: {
                "start_time": str | None,
                "end_time": str | None,
                "duration_seconds": float,
                "records_fetched": int,
                "records_inserted": int,
                "records_updated": int,
                "records_skipped": int,
                "api_calls": int,
                "error_count": int,
                "errors": List[Dict[str, Any]],   # first 10 error entries
                "warnings": List[str]             # first 10 warnings
            }
        """
        return {
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "records_fetched": self.records_fetched,
            "records_inserted": self.records_inserted,
            "records_updated": self.records_updated,
            "records_skipped": self.records_skipped,
            "api_calls": self.api_calls,
            "error_count": len(self.errors),
            "errors": self.errors[:10],  # First 10 errors
            "warnings": self.warnings[:10],
        }

    def log_summary(self) -> None:
        """Log a formatted summary of collected population metrics including duration, API calls, record counts, and counts of errors and warnings."""
        logger.info("=" * 60)
        logger.info("POPULATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Duration: {self.duration_seconds:.1f}s")
        logger.info(f"API Calls: {self.api_calls}")
        logger.info(f"Records Fetched: {self.records_fetched:,}")
        logger.info(f"Records Inserted: {self.records_inserted:,}")
        logger.info(f"Records Updated: {self.records_updated:,}")
        logger.info(f"Records Skipped: {self.records_skipped:,}")
        if self.errors:
            logger.warning(f"Errors: {len(self.errors)}")
        if self.warnings:
            logger.warning(f"Warnings: {len(self.warnings)}")


class ProgressTracker:
    """Tracks and persists population progress for resumability."""

    def __init__(self, name: str) -> None:
        """Create a ProgressTracker for a named population task and load its persisted progress.

        Parameters:
            name (str): Unique identifier for the population task; used to name the progress JSON file.
        """
        self.name = name
        self.progress_file = CACHE_DIR / f"{name}_progress.json"
        self._progress: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        """Load the persisted progress for this tracker from its progress file, or return a default empty progress structure if loading fails.

        Returns:
            dict: A progress dictionary with keys:
                - "completed_items" (list): Completed item identifiers.
                - "last_item" (str | None): The most recently processed item, or `None`.
                - "last_run" (str | None): Timestamp of the last save/run (ISO string) or `None`.
                - "errors" (list): Recorded error entries.
        """
        default: ProgressState = {
            "completed_items": [],
            "last_item": None,
            "last_run": None,
            "errors": [],
        }
        return load_json_file(self.progress_file, default)

    def save(self) -> None:
        """Save progress to file."""
        ensure_cache_dir()
        self._progress["last_run"] = datetime.now(tz=UTC).isoformat()
        save_json_file(self.progress_file, self._progress)

    def mark_completed(self, item: str) -> None:
        """Mark the given item as completed and record it as the last processed item.

        Parameters:
            item (str): Identifier of the completed item; appended to the progress's completed_items list and set as last_item.
        """
        if item not in self._progress["completed_items"]:
            self._progress["completed_items"].append(item)
        self._progress["last_item"] = item

    def is_completed(self, item: str) -> bool:
        """Return whether a progress item has been marked completed.

        Parameters:
            item (str): Identifier of the progress item.

        Returns:
            bool: `true` if the item has been recorded as completed, `false` otherwise.
        """
        return item in self._progress["completed_items"]

    def get_completed(self) -> set[str]:
        """Get the set of item identifiers marked as completed.

        Returns:
            completed (Set[str]): A set of completed item identifiers from the in-memory progress.
        """
        return set(self._progress["completed_items"])

    def reset(self) -> None:
        """Reset the tracked progress to its initial empty state and persist the change.

        This clears in-memory progress (completed_items, last_item, last_run, errors) and saves the reset progress to disk.
        """
        self._progress = {
            "completed_items": [],
            "last_item": None,
            "last_run": None,
            "errors": [],
        }
        self.save()

    def add_error(self, item: str, error: str) -> None:
        """Record an error occurrence associated with a progress item.

        Appends an entry to the tracker's errors list containing the item identifier, the error message, and an ISO 8601 timestamp.

        Parameters:
            item (str): Identifier of the progress item that encountered the error.
            error (str): Human-readable error message or context.
        """
        self._progress["errors"].append(
            {
                "item": item,
                "error": error,
                "timestamp": datetime.now(tz=UTC).isoformat(),
            },
        )


# =============================================================================
# DATACLASSES FOR STRUCTURED ITERATION
# =============================================================================


@dataclass
class SeasonIterationContext:
    """Context for a single season/season_type iteration.

    Attributes:
        season: Season string (e.g., "2023-24").
        season_type: Season type enum value.
        progress_key: Unique key for progress tracking.
        is_completed: Whether this iteration was already completed.
        index: Zero-based index in the iteration sequence.
        total: Total number of iterations.
    """

    season: str
    season_type: SeasonType
    progress_key: str
    is_completed: bool = False
    index: int = 0
    total: int = 0

    @property
    def season_type_str(self) -> str:
        """Return the season type as API-compatible string."""
        return self.season_type.value


@dataclass
class IterationResult:
    """Result of processing a single iteration.

    Attributes:
        success: Whether the iteration succeeded.
        records_processed: Number of records processed.
        error: Error message if failed, None otherwise.
        data: Optional DataFrame result from the iteration.
    """

    success: bool
    records_processed: int = 0
    error: str | None = None
    data: pd.DataFrame | None = None


@dataclass
class BatchIterationConfig:
    """Configuration for batch iteration over items.

    Attributes:
        items: List of items to iterate over.
        key_func: Function to generate progress key from item.
        resume: Whether to skip completed items.
        save_interval: Save progress every N items.
        deferred_progress: If True, mark progress after successful processing.
    """

    items: list[Any]
    key_func: Any  # Callable[[Any], str]
    resume: bool = True
    save_interval: int = 10
    deferred_progress: bool = False


# =============================================================================
# MIXINS FOR COMMON PATTERNS
# =============================================================================


class ProgressMixin:
    """Mixin providing common progress tracking patterns.

    This mixin consolidates progress tracking patterns used across populators:
    - Checking if items are completed
    - Marking items as completed with optional deferral
    - Batch progress saving
    - Error recording

    Requires the class to have a `progress` attribute (ProgressTracker).
    """

    progress: ProgressTracker
    metrics: PopulationMetrics

    def should_skip_item(self, item_key: str, *, resume: bool = True) -> bool:
        """Check if an item should be skipped based on resume mode and completion status.

        Args:
            item_key: Unique identifier for the item.
            resume: Whether resume mode is enabled.

        Returns:
            True if the item should be skipped, False otherwise.
        """
        if not resume:
            return False
        return self.progress.is_completed(item_key)

    def mark_item_completed(
        self,
        item_key: str,
        *,
        save: bool = False,
        dry_run: bool = False,
    ) -> None:
        """Mark an item as completed in progress tracking.

        Args:
            item_key: Unique identifier for the item.
            save: Whether to immediately save progress to disk.
            dry_run: If True, skip marking (for dry-run mode).
        """
        if dry_run:
            return

        self.progress.mark_completed(item_key)
        if save:
            self.progress.save()

    def record_item_error(
        self,
        item_key: str,
        error: str | Exception,
        *,
        log_error: bool = True,
    ) -> None:
        """Record an error for an item.

        Args:
            item_key: Unique identifier for the item.
            error: Error message or exception.
            log_error: Whether to also log the error.
        """
        error_str = str(error)
        self.progress.add_error(item_key, error_str)
        self.metrics.add_error(error_str, {"item": item_key})

        if log_error:
            logger.error(f"Error processing {item_key}: {error_str}")

    def iter_with_progress(
        self,
        config: BatchIterationConfig,
    ) -> Generator[tuple[Any, str, bool], None, None]:
        """Iterate over items with progress tracking.

        Yields tuples of (item, progress_key, should_skip).

        Args:
            config: Configuration for the batch iteration.

        Yields:
            Tuple of (item, progress_key, should_skip_flag).
        """
        for idx, item in enumerate(config.items):
            key = config.key_func(item)
            should_skip = self.should_skip_item(key, resume=config.resume)

            yield item, key, should_skip

            # Periodic save
            if (idx + 1) % config.save_interval == 0:
                self.progress.save()


class SeasonIteratorMixin:
    """Mixin providing season/season_type iteration patterns.

    This mixin consolidates the common pattern of iterating over seasons
    and season types, which is repeated across many populators:
    - populate_league_game_logs.py
    - populate_player_game_stats_v2.py
    - populate_team_info_common.py
    - etc.

    Requires the class to have `progress` and `metrics` attributes.
    """

    progress: ProgressTracker
    metrics: PopulationMetrics

    @staticmethod
    def resolve_seasons(
        seasons: list[str] | None = None,
        *,
        default_current: bool = True,
        default_all: bool = False,
    ) -> list[str]:
        """Resolve season list from input or defaults.

        Args:
            seasons: Explicit list of seasons, or None for defaults.
            default_current: If True and seasons is None, use current season.
            default_all: If True and seasons is None, use all seasons.

        Returns:
            List of season strings.
        """
        if seasons:
            return seasons

        if default_all:
            return list(ALL_SEASONS)

        if default_current:
            return [CURRENT_SEASON]

        return []

    @staticmethod
    def resolve_season_types(
        season_types: list[str] | None = None,
        *,
        default_regular: bool = True,
        include_playoffs: bool = False,
    ) -> list[SeasonType]:
        """Resolve season types from input or defaults.

        Args:
            season_types: List of season type strings, or None for defaults.
            default_regular: If True and None, include Regular Season.
            include_playoffs: If True and None, also include Playoffs.

        Returns:
            List of SeasonType enum values.
        """
        if season_types:
            result = []
            for st in season_types:
                st_lower = st.lower()
                if st_lower in SEASON_TYPE_MAP:
                    # Map string to enum
                    st_value = SEASON_TYPE_MAP[st_lower]
                    result.append(SeasonType(st_value))
                else:
                    # Try direct enum lookup
                    try:
                        result.append(SeasonType(st))
                    except ValueError:
                        logger.warning(f"Unknown season type: {st}")
            return result

        result = []
        if default_regular:
            result.append(SeasonType.REGULAR)
        if include_playoffs:
            result.append(SeasonType.PLAYOFFS)
        return result

    def iter_seasons(
        self,
        seasons: list[str] | None = None,
        season_types: list[str] | None = None,
        *,
        resume: bool = True,
        key_format: str = "{season}_{season_type}",
    ) -> Generator[SeasonIterationContext, None, None]:
        """Iterate over season/season_type combinations with progress tracking.

        Args:
            seasons: List of seasons to iterate, or None for defaults.
            season_types: List of season types, or None for defaults.
            resume: Whether to check progress and skip completed items.
            key_format: Format string for progress key. Supports {season} and {season_type}.

        Yields:
            SeasonIterationContext for each combination.

        Example:
            for ctx in self.iter_seasons(seasons, season_types, resume=resume):
                if ctx.is_completed:
                    logger.info(f"Skipping {ctx.progress_key}")
                    continue
                # Process season/season_type
                self.mark_item_completed(ctx.progress_key)
        """
        resolved_seasons = self.resolve_seasons(seasons)
        resolved_types = self.resolve_season_types(season_types)

        # Calculate total iterations
        total = len(resolved_seasons) * len(resolved_types)
        index = 0

        for season in resolved_seasons:
            for season_type in resolved_types:
                # Generate progress key
                progress_key = key_format.format(
                    season=season,
                    season_type=season_type.value.replace(" ", "_"),
                )

                # Check if completed
                is_completed = resume and self.progress.is_completed(progress_key)

                yield SeasonIterationContext(
                    season=season,
                    season_type=season_type,
                    progress_key=progress_key,
                    is_completed=is_completed,
                    index=index,
                    total=total,
                )

                index += 1

    def process_seasons(
        self,
        seasons: list[str] | None = None,
        season_types: list[str] | None = None,
        *,
        resume: bool = True,
        dry_run: bool = False,
        process_func: Any = None,  # Callable[[SeasonIterationContext], IterationResult]
        **kwargs: Any,
    ) -> list[IterationResult]:
        """Process all season/season_type combinations using a callback.

        Args:
            seasons: List of seasons to process.
            season_types: List of season types to process.
            resume: Whether to skip completed items.
            dry_run: Whether this is a dry run.
            process_func: Callback function that processes each context.
            **kwargs: Additional arguments passed to process_func.

        Returns:
            List of IterationResult for each processed combination.
        """
        results = []

        for ctx in self.iter_seasons(seasons, season_types, resume=resume):
            if ctx.is_completed:
                logger.info(f"Skipping completed: {ctx.progress_key}")
                self.metrics.records_skipped += 1
                continue

            logger.info(f"Processing {ctx.progress_key} ({ctx.index + 1}/{ctx.total})")

            try:
                if process_func:
                    result = process_func(ctx, **kwargs)
                else:
                    # Default implementation calls process_season if defined
                    result = self.process_season(ctx, **kwargs)

                results.append(result)

                if result.success and not dry_run:
                    self.progress.mark_completed(ctx.progress_key)
                    self.progress.save()

            except Exception as e:
                logger.exception(f"Error processing {ctx.progress_key}: {e}")
                self.progress.add_error(ctx.progress_key, str(e))
                self.metrics.add_error(str(e), {"context": ctx.progress_key})
                results.append(IterationResult(success=False, error=str(e)))

        return results

    def process_season(
        self,
        ctx: SeasonIterationContext,
        **kwargs: Any,
    ) -> IterationResult:
        """Process a single season/season_type combination.

        Override this method in subclasses to implement season processing logic.

        Args:
            ctx: The iteration context containing season info.
            **kwargs: Additional processing arguments.

        Returns:
            IterationResult indicating success/failure.
        """
        raise NotImplementedError(
            "Subclasses must implement process_season() or provide process_func"
        )


class BasePopulator(ABC):
    """Abstract base class for NBA data population scripts.

    Subclasses must implement:
    - get_table_name(): Return the target table name
    - get_key_columns(): Return primary key column(s)
    - fetch_data(): Fetch data from the API
    - transform_data(): Transform API data to match schema

    Optional overrides:
    - get_expected_columns(): Return expected column list for validation
    - pre_run_hook(): Called before population starts
    - post_run_hook(): Called after population completes
    """

    def __init__(
        self,
        db_path: str | None = None,
        client: NBAClient | None = None,
        batch_size: int = 1000,
    ) -> None:
        """Initialize the populator and configure its runtime components.

        Parameters:
            db_path (Optional[str]): Path to the DuckDB database; when None the path is resolved via get_db_path().
            client (Optional[NBAClient]): NBA API client to use; when None a default client is provided by get_client().
            batch_size (int): Number of records to process per batch for upserts.

        This sets up:
        - self.db_path, self.client, and self.batch_size.
        - a PopulationMetrics instance at self.metrics.
        - a DataValidator instance at self.validator.
        - self._conn initialized to None.
        - a DatabaseManager instance at self._db_manager for bulk operations.
        - a ProgressTracker named after the class (lowercased) at self.progress.
        """
        self.db_path = db_path or str(get_db_path())
        self.client = client or get_client()
        self.batch_size = batch_size
        self.metrics = PopulationMetrics()
        self.validator = DataValidator()
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._db_manager: DatabaseManager | None = None

        # Initialize progress tracker with class name
        self.progress = ProgressTracker(self.__class__.__name__.lower())

    @abstractmethod
    def get_table_name(self) -> str:
        """Provide the target database table name for this populator.

        Returns:
            table_name (str): The name of the target table in the database.
        """

    @abstractmethod
    def get_key_columns(self) -> list[str]:
        """Primary key column name(s) for the target table.

        Returns:
            key_columns (List[str]): List of column names that uniquely identify a row in the target table.
        """

    def get_data_type(self) -> str:
        """Return the type of data being populated (e.g., 'players', 'games').

        Used for validation logic. Defaults to 'generic'.
        """
        return "generic"

    @abstractmethod
    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        """Retrieve raw data from the external API according to population parameters.

        Parameters:
            **kwargs: Population parameters such as seasons, date ranges, player or team filters, or other provider-specific options that control which data is fetched.

        Returns:
            A pandas DataFrame containing the raw API response records, or `None` if no data was returned.
        """

    @abstractmethod
    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Convert a raw API DataFrame into a DataFrame that conforms to the target table schema.

        Parameters:
            df (pd.DataFrame): Raw DataFrame returned by the data fetcher.
            **kwargs: Optional context or parameters used to guide transformation (e.g., season, team_id).

        Returns:
            pd.DataFrame: Transformed DataFrame with columns and types matching the target table schema, ready for validation and insertion.
        """

    def get_expected_columns(self) -> list[str] | None:
        """Return the list of expected column names for validation, or None to skip column-level checks.

        Returns:
            expected_columns (Optional[List[str]]): List of column names that transformed data is expected to contain, or `None` if the populator does not enforce expected columns.
        """
        return None

    def pre_run_hook(self, **kwargs) -> None:
        """Hook executed immediately before a population run begins; override to perform setup or initialization.

        Parameters:
            **kwargs: Optional runtime parameters forwarded from `run()` that subclasses may use for setup.
        """

    def post_run_hook(self, **kwargs) -> None:
        """Called after population completes. Override for cleanup logic."""

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Lazily initialize and return the DuckDB connection used by this populator.

        Returns:
            duckdb.DuckDBPyConnection: The active DuckDB connection instance.
        """
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")
        return self._conn

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
        if self._db_manager:
            self._db_manager.close()
            self._db_manager = None

    def _get_db_manager(self) -> DatabaseManager:
        """Lazily initialize and return the DatabaseManager for bulk operations.

        Returns:
            DatabaseManager: The database manager instance.
        """
        if self._db_manager is None:
            from pathlib import Path

            self._db_manager = DatabaseManager(db_path=Path(self.db_path))
        return self._db_manager

    def _iter_batches(self, df: pd.DataFrame) -> Iterable[pd.DataFrame]:
        """Yield DataFrame slices according to batch size."""
        for i in range(0, len(df), self.batch_size):
            yield df.iloc[i : i + self.batch_size]

    def get_raw_table_name(self) -> str:
        """Get the raw table name with '_raw' suffix.

        Returns:
            str: Table name with '_raw' suffix.
        """
        name = self.get_table_name()
        if not name.endswith("_raw"):
            return f"{name}_raw"
        return name

    def get_existing_keys(self) -> set[tuple]:
        """Return the set of existing primary key tuples for the populator's target table.

        Keys in each tuple follow the order returned by get_key_columns(). If the target table does not exist, an empty set is returned.

        Returns:
            existing_keys (Set[Tuple]): A set of tuples where each tuple contains the primary key values of an existing row.
        """
        conn = self.connect()
        table = self.get_raw_table_name()
        keys = self.get_key_columns()

        try:
            key_cols = ", ".join(keys)
            result = conn.execute(f"SELECT {key_cols} FROM {table}").fetchall()
            return {tuple(row) for row in result}
        except duckdb.CatalogException:
            # Table doesn't exist
            return set()

    def upsert_batch(self, df: pd.DataFrame) -> tuple[int, int]:
        """Insert new rows from the provided DataFrame into the target table and update existing ones using MERGE.

        Delegates to DatabaseManager.bulk_upsert() for the actual MERGE operation.

        Parameters:
            df: DataFrame containing rows to upsert; must include the columns returned by get_key_columns().

        Returns:
            (inserted_count, updated_count): Counts of rows inserted and updated.
        """
        if df.empty:
            return 0, 0

        table = self.get_raw_table_name()
        keys = self.get_key_columns()

        try:
            db_manager = self._get_db_manager()
            rows_affected = db_manager.bulk_upsert(df, table, keys)

            logger.info(f"Upserted {rows_affected} records into {table}")
            return (
                rows_affected,
                0,
            )  # Simplified return (bulk_upsert doesn't distinguish insert vs update)

        except Exception as e:
            logger.exception(f"Upsert error for {table}: {e}")
            self.metrics.add_error(str(e), {"operation": "upsert"})
            return 0, 0

    def validate_data(self, df: pd.DataFrame, **kwargs) -> bool:
        """Validate the DataFrame using the DataValidator.

        Parameters:
            df (pd.DataFrame): Data to validate.
            **kwargs: Additional validation parameters.

        Returns:
            True if the data is valid, False otherwise.
        """
        data_type = self.get_data_type()
        expected_cols = self.get_expected_columns()

        validation_kwargs = kwargs.copy()
        if expected_cols:
            validation_kwargs["expected_fields"] = expected_cols

        report = self.validator.generate_validation_report(
            data_type=data_type,
            df=df,
            **validation_kwargs,
        )

        if not report["overall_valid"]:
            errors = list(report.get("errors", []))
            completeness = report.get("completeness")
            if isinstance(completeness, dict):
                errors.extend(completeness.get("errors", []))
            for error in errors:
                self.metrics.add_error(f"Validation Error: {error}")
            return False

        for warning in report.get("warnings", []):
            self.metrics.warnings.append(f"Validation Warning: {warning}")

        return True

    def run(
        self,
        resume: bool = True,
        reset_progress: bool = False,
        dry_run: bool = False,
        **kwargs,
    ) -> dict[str, Any]:
        """Run the population process.

        Args:
            resume: Whether to skip already completed items
            reset_progress: Whether to reset progress before starting
            dry_run: If True, don't actually insert data
            **kwargs: Additional parameters passed to fetch_data

        Returns:
            Dictionary with population metrics
        """
        logger.info(f"Starting {self.__class__.__name__}")
        logger.info(f"Database: {self.db_path}")
        logger.info(f"Table: {self.get_raw_table_name()}")

        if reset_progress:
            self.progress.reset()
            logger.info("Progress reset")

        self.metrics.start()

        run_kwargs = {**kwargs, "resume": resume, "dry_run": dry_run}

        try:
            # Pre-run hook
            self.pre_run_hook(**run_kwargs)

            # Fetch data
            logger.info("Fetching data from API...")
            df = self.fetch_data(**run_kwargs)
            # API calls are counted within fetch_data for bulk operations

            if df is None or df.empty:
                logger.info("No data returned from API")
                return self.metrics.to_dict()

            self.metrics.records_fetched = len(df)
            logger.info(f"Fetched {len(df):,} records")

            # Transform data
            logger.info("Transforming data...")
            df = self.transform_data(df, **kwargs)
            logger.info(f"Transformed to {len(df):,} records")

            # Validate data
            if not self.validate_data(df, **run_kwargs):
                logger.error("Data validation failed")
                return self.metrics.to_dict()

            if dry_run:
                logger.info("DRY RUN - skipping database insertion")
                return self.metrics.to_dict()

            # Insert data in batches
            logger.info("Inserting data...")
            total_inserted = 0
            total_updated = 0

            from src.scripts.utils.ui import create_progress_bar

            with create_progress_bar() as progress:
                task = progress.add_task(
                    f"Inserting into {self.get_raw_table_name()}",
                    total=len(df),
                )

            for idx, batch in enumerate(self._iter_batches(df), start=1):
                inserted, updated = self.upsert_batch(batch)
                total_inserted += inserted
                total_updated += updated

                progress.update(task, advance=len(batch))

                if (idx * self.batch_size) % 5000 == 0:
                    self.connect().commit()
                    logger.info(
                        "Progress: %s/%s records",
                        min(idx * self.batch_size, len(df)),
                        len(df),
                    )

            # Final commit
            self.connect().commit()

            self.metrics.records_inserted = total_inserted
            self.metrics.records_updated = total_updated

            # Post-run hook
            self.post_run_hook(**run_kwargs)

            # Run integrity checks
            if not dry_run:
                logger.info("Running database integrity checks...")
                check_integrity(db_path=self.db_path)

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            self.progress.save()
            raise

        except Exception as e:
            logger.exception(f"Population failed: {e}")
            self.metrics.add_error(str(e))
            raise

        finally:
            self.metrics.stop()
            self.progress.save()
            self.close()
            self.metrics.log_summary()

        return self.metrics.to_dict()
