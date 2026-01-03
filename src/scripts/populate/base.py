"""Base class for NBA data population scripts.

This module provides a reusable base class that encapsulates common functionality
for all population scripts including:

- Database connection management
- Progress tracking and resumability
- Batch insertion with upsert support
- Logging and metrics collection
- Rate limiting coordination

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
"""

import json
import logging
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any

import duckdb
import pandas as pd

from src.scripts.maintenance.check_integrity import check_integrity
from src.scripts.populate.api_client import NBAClient, get_client
from src.scripts.populate.config import CACHE_DIR, ensure_cache_dir, get_db_path
from src.scripts.populate.database import DatabaseManager
from src.scripts.populate.validation import DataValidator


logger = logging.getLogger(__name__)


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
        if self.progress_file.exists():
            try:
                with open(self.progress_file) as f:
                    data: dict[str, Any] = json.load(f)
                    return data
            except (OSError, json.JSONDecodeError):
                logger.warning(f"Could not load progress file: {self.progress_file}")
        return {
            "completed_items": [],
            "last_item": None,
            "last_run": None,
            "errors": [],
        }

    def save(self) -> None:
        """Save progress to file."""
        ensure_cache_dir()
        self._progress["last_run"] = datetime.now(tz=UTC).isoformat()
        with open(self.progress_file, "w") as f:
            json.dump(self._progress, f, indent=2)

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

                for i in range(0, len(df), self.batch_size):
                    batch = df.iloc[i : i + self.batch_size]
                    inserted, updated = self.upsert_batch(batch)
                    total_inserted += inserted
                    total_updated += updated

                    progress.update(task, advance=len(batch))

                    if (i + self.batch_size) % 5000 == 0:
                        self.connect().commit()
                        logger.info(
                            f"Progress: {i + self.batch_size:,}/{len(df):,} records",
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
