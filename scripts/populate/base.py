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
    populator.run(seasons=["2023-24"])
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb
import pandas as pd

from .api_client import NBAClient, get_client
from .config import CACHE_DIR, ensure_cache_dir, get_db_path
from .validation import DataValidator

logger = logging.getLogger(__name__)


class PopulationMetrics:
    """Collects and reports metrics for a population run."""

    def __init__(self):
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.records_fetched: int = 0
        self.records_inserted: int = 0
        self.records_updated: int = 0
        self.records_skipped: int = 0
        self.api_calls: int = 0
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[str] = []

    def start(self):
        """Mark the start of a population run."""
        self.start_time = datetime.now()

    def stop(self):
        """Mark the end of a population run."""
        self.end_time = datetime.now()

    @property
    def duration_seconds(self) -> float:
        """Get the duration of the run in seconds."""
        if not self.start_time or not self.end_time:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()

    def add_error(self, error: str, context: Optional[Dict] = None):
        """Record an error."""
        self.errors.append({
            "error": error,
            "context": context or {},
            "timestamp": datetime.now().isoformat()
        })

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
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

    def log_summary(self):
        """Log a summary of the metrics."""
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

    def __init__(self, name: str):
        """
        Initialize progress tracker.

        Args:
            name: Unique name for this population task
        """
        self.name = name
        self.progress_file = CACHE_DIR / f"{name}_progress.json"
        self._progress: Dict[str, Any] = self._load()

    def _load(self) -> Dict[str, Any]:
        """Load progress from file."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                logger.warning(f"Could not load progress file: {self.progress_file}")
        return {
            "completed_items": [],
            "last_item": None,
            "last_run": None,
            "errors": []
        }

    def save(self):
        """Save progress to file."""
        ensure_cache_dir()
        self._progress["last_run"] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self._progress, f, indent=2)

    def mark_completed(self, item: str):
        """Mark an item as completed."""
        if item not in self._progress["completed_items"]:
            self._progress["completed_items"].append(item)
        self._progress["last_item"] = item

    def is_completed(self, item: str) -> bool:
        """Check if an item is completed."""
        return item in self._progress["completed_items"]

    def get_completed(self) -> Set[str]:
        """Get set of completed items."""
        return set(self._progress["completed_items"])

    def reset(self):
        """Reset all progress."""
        self._progress = {
            "completed_items": [],
            "last_item": None,
            "last_run": None,
            "errors": []
        }
        self.save()

    def add_error(self, item: str, error: str):
        """Record an error for an item."""
        self._progress["errors"].append({
            "item": item,
            "error": error,
            "timestamp": datetime.now().isoformat()
        })


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
        db_path: Optional[str] = None,
        client: Optional[NBAClient] = None,
        batch_size: int = 1000,
    ):
        """
        Initialize the populator.

        Args:
            db_path: Path to DuckDB database
            client: NBAClient instance
            batch_size: Number of records to insert per batch
        """
        self.db_path = db_path or str(get_db_path())
        self.client = client or get_client()
        self.batch_size = batch_size
        self.metrics = PopulationMetrics()
        self.validator = DataValidator()
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

        # Initialize progress tracker with class name
        self.progress = ProgressTracker(self.__class__.__name__.lower())

    @abstractmethod
    def get_table_name(self) -> str:
        """Return the target table name."""
        pass

    @abstractmethod
    def get_key_columns(self) -> List[str]:
        """Return the primary key column(s)."""
        pass

    @abstractmethod
    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """Fetch data from the API.

        Args:
            **kwargs: Population parameters (seasons, etc.)

        Returns:
            DataFrame with raw API data or None if no data
        """
        pass

    @abstractmethod
    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform API data to match the database schema.

        Args:
            df: Raw DataFrame from API
            **kwargs: Additional context

        Returns:
            Transformed DataFrame matching table schema
        """
        pass

    def get_expected_columns(self) -> Optional[List[str]]:
        """Return expected columns for validation. Override in subclass."""
        return None

    def pre_run_hook(self, **kwargs):
        """Called before population starts. Override for setup logic."""
        pass

    def post_run_hook(self, **kwargs):
        """Called after population completes. Override for cleanup logic."""
        pass

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = duckdb.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")
        return self._conn

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def get_existing_keys(self) -> Set[Tuple]:
        """Get existing primary key values from the table."""
        conn = self.connect()
        table = self.get_table_name()
        keys = self.get_key_columns()

        try:
            key_cols = ", ".join(keys)
            result = conn.execute(f"SELECT {key_cols} FROM {table}").fetchall()
            return {tuple(row) for row in result}
        except duckdb.CatalogException:
            # Table doesn't exist
            return set()

    def upsert_batch(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Insert or update a batch of records.

        Args:
            df: DataFrame to upsert

        Returns:
            Tuple of (inserted_count, updated_count)
        """
        if df.empty:
            return 0, 0

        conn = self.connect()
        table = self.get_table_name()
        keys = self.get_key_columns()

        # Get existing keys
        existing_keys = self.get_existing_keys()

        # Split into inserts and updates
        def get_key(row):
            return tuple(row[k] for k in keys)

        df['_is_update'] = df.apply(lambda r: get_key(r) in existing_keys, axis=1)

        inserts_df = df[~df['_is_update']].drop(columns=['_is_update'])
        updates_df = df[df['_is_update']].drop(columns=['_is_update'])

        inserted = 0
        updated = 0

        # Insert new records
        if not inserts_df.empty:
            try:
                conn.register("temp_inserts", inserts_df)
                conn.execute(f"INSERT INTO {table} SELECT * FROM temp_inserts")
                conn.unregister("temp_inserts")
                inserted = len(inserts_df)
            except Exception as e:
                logger.error(f"Insert error: {e}")
                self.metrics.add_error(str(e), {"operation": "insert"})

        # Update existing records (if needed)
        if not updates_df.empty:
            # For now, skip updates - could implement MERGE later
            self.metrics.records_skipped += len(updates_df)

        return inserted, updated

    def validate_data(self, df: pd.DataFrame, **kwargs) -> bool:
        """Validate data before insertion.

        Args:
            df: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        expected_cols = self.get_expected_columns()
        if expected_cols:
            result = self.validator.validate_data_completeness(df, expected_cols)
            if not result["valid"]:
                for error in result.get("errors", []):
                    self.metrics.add_error(error)
                return False
            for warning in result.get("warnings", []):
                self.metrics.warnings.append(warning)
        return True

    def run(
        self,
        resume: bool = True,
        reset_progress: bool = False,
        dry_run: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
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
        logger.info(f"Table: {self.get_table_name()}")

        if reset_progress:
            self.progress.reset()
            logger.info("Progress reset")

        self.metrics.start()

        try:
            # Pre-run hook
            self.pre_run_hook(**kwargs)

            # Fetch data
            logger.info("Fetching data from API...")
            df = self.fetch_data(**kwargs)
            self.metrics.api_calls += 1

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
            if not self.validate_data(df, **kwargs):
                logger.error("Data validation failed")
                return self.metrics.to_dict()

            if dry_run:
                logger.info("DRY RUN - skipping database insertion")
                return self.metrics.to_dict()

            # Insert data in batches
            logger.info("Inserting data...")
            total_inserted = 0
            total_updated = 0

            for i in range(0, len(df), self.batch_size):
                batch = df.iloc[i:i + self.batch_size]
                inserted, updated = self.upsert_batch(batch)
                total_inserted += inserted
                total_updated += updated

                if (i + self.batch_size) % 5000 == 0:
                    self.connect().commit()
                    logger.info(f"Progress: {i + self.batch_size:,}/{len(df):,} records")

            # Final commit
            self.connect().commit()

            self.metrics.records_inserted = total_inserted
            self.metrics.records_updated = total_updated

            # Post-run hook
            self.post_run_hook(**kwargs)

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            self.progress.save()
            raise

        except Exception as e:
            logger.error(f"Population failed: {e}")
            self.metrics.add_error(str(e))
            raise

        finally:
            self.metrics.stop()
            self.progress.save()
            self.close()
            self.metrics.log_summary()

        return self.metrics.to_dict()
