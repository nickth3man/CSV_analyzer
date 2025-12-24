"""Dataframe caching for efficient data loading."""

import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class DataFrameCache:
    """
    Cache for loaded dataframes to avoid re-reading CSV files on each command.
    Implements cache invalidation based on directory modification time.
    """
    def __init__(self, csv_dir="CSV"):
        self.csv_dir = csv_dir
        self._cache = {}
        self._last_mtime = None
        self._file_mtimes = {}

    def _get_dir_state(self):
        """Get the current state of the CSV directory (files and their mtimes)."""
        if not os.path.exists(self.csv_dir):
            return None, {}

        dir_mtime = os.path.getmtime(self.csv_dir)
        file_mtimes = {}
        for filename in os.listdir(self.csv_dir):
            if filename.endswith(".csv"):
                filepath = os.path.join(self.csv_dir, filename)
                try:
                    file_mtimes[filename] = os.path.getmtime(filepath)
                except OSError:
                    pass
        return dir_mtime, file_mtimes

    def _is_cache_valid(self):
        """Check if the cache is still valid based on directory state."""
        dir_mtime, file_mtimes = self._get_dir_state()

        # Invalid if directory doesn't exist or was modified
        if dir_mtime is None:
            return False
        if self._last_mtime is None or dir_mtime != self._last_mtime:
            return False

        # Invalid if any file was modified or files changed
        if set(file_mtimes.keys()) != set(self._file_mtimes.keys()):
            return False
        for filename, mtime in file_mtimes.items():
            if self._file_mtimes.get(filename) != mtime:
                return False

        return True

    def invalidate(self):
        """Force cache invalidation (e.g., after upload/delete)."""
        self._cache = {}
        self._last_mtime = None
        self._file_mtimes = {}

    def get_dataframes(self):
        """Get cached dataframes, reloading if cache is invalid."""
        if self._is_cache_valid() and self._cache:
            return self._cache

        # Reload dataframes
        dfs = {}
        if not os.path.exists(self.csv_dir):
            os.makedirs(self.csv_dir)

        for filename in os.listdir(self.csv_dir):
            if filename.endswith(".csv"):
                filepath = os.path.join(self.csv_dir, filename)
                try:
                    table_name = filename.replace(".csv", "")
                    dfs[table_name] = pd.read_csv(filepath)
                except (pd.errors.ParserError, UnicodeDecodeError) as e:
                    logger.warning(f"Warning: Could not parse {filename}: {e}")
                except Exception as e:
                    logger.warning(f"Warning: Unexpected error loading {filename}: {e}")

        # Update cache state
        self._cache = dfs
        self._last_mtime, self._file_mtimes = self._get_dir_state()

        return dfs


# Global dataframe cache instance
_df_cache = DataFrameCache()


def get_dataframe_cache():
    """Get the global dataframe cache instance."""
    return _df_cache
