"""Unit tests for src.scripts.populate.base module.

Tests BasePopulator, ProgressTracker, and PopulationMetrics.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.scripts.populate.base import BasePopulator, PopulationMetrics, ProgressTracker


class TestPopulationMetrics:
    """Tests for PopulationMetrics class."""

    def test_initialization(self):
        """Test metrics initialization."""
        metrics = PopulationMetrics()
        assert metrics.records_fetched == 0
        assert metrics.records_inserted == 0
        assert metrics.start_time is None
        assert metrics.errors == []

    def test_start_stop_duration(self):
        """Test start, stop and duration calculation."""
        metrics = PopulationMetrics()
        metrics.start()
        assert metrics.start_time is not None

        metrics.stop()
        assert metrics.end_time is not None
        assert metrics.duration_seconds >= 0

    def test_add_error(self):
        """Test adding errors with context."""
        metrics = PopulationMetrics()
        metrics.add_error("Test error", {"key": "value"})
        assert len(metrics.errors) == 1
        assert metrics.errors[0]["error"] == "Test error"
        assert metrics.errors[0]["context"] == {"key": "value"}

    def test_to_dict(self):
        """Test serialization to dictionary."""
        metrics = PopulationMetrics()
        metrics.start()
        metrics.records_fetched = 100
        metrics.stop()

        data = metrics.to_dict()
        assert data["records_fetched"] == 100
        assert "duration_seconds" in data
        assert data["error_count"] == 0


class TestProgressTracker:
    """Tests for ProgressTracker class."""

    @pytest.fixture
    def mock_cache_dir(self, tmp_path):
        """Mock CACHE_DIR for testing."""
        with patch("src.scripts.populate.base.CACHE_DIR", tmp_path):
            yield tmp_path

    def test_initialization_new(self, mock_cache_dir):
        """Test initialization when no progress file exists."""
        tracker = ProgressTracker("test_task")
        assert tracker.name == "test_task"
        assert tracker.get_completed() == set()
        assert tracker.progress_file.name == "test_task_progress.json"

    def test_mark_completed(self, mock_cache_dir):
        """Test marking items as completed."""
        tracker = ProgressTracker("test_task")
        tracker.mark_completed("item1")
        assert tracker.is_completed("item1")
        assert "item1" in tracker.get_completed()
        # Accessing private member for verification in test
        assert tracker._progress["last_item"] == "item1"

    def test_save_load(self, mock_cache_dir):
        """Test saving and loading progress."""
        tracker = ProgressTracker("test_task")
        tracker.mark_completed("item1")
        tracker.save()

        assert tracker.progress_file.exists()

        # New tracker instance should load saved progress
        new_tracker = ProgressTracker("test_task")
        assert new_tracker.is_completed("item1")

    def test_reset(self, mock_cache_dir):
        """Test resetting progress."""
        tracker = ProgressTracker("test_task")
        tracker.mark_completed("item1")
        tracker.reset()

        assert not tracker.is_completed("item1")
        assert tracker.get_completed() == set()


class MockPopulator(BasePopulator):
    """Concrete implementation of BasePopulator for testing."""

    def get_table_name(self) -> str:
        return "test_table"

    def get_key_columns(self) -> list[str]:
        return ["id"]

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        return kwargs.get("mock_df")

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return df


class TestBasePopulator:
    """Tests for BasePopulator class."""

    @pytest.fixture
    def populator(self, tmp_path):
        """Create a MockPopulator instance."""
        db_path = tmp_path / "test.duckdb"
        with patch("src.scripts.populate.base.get_db_path", return_value=db_path):
            pop = MockPopulator(db_path=str(db_path))
            yield pop

    def test_initialization(self, populator):
        """Test populator initialization."""
        assert populator.get_table_name() == "test_table"
        assert populator.get_key_columns() == ["id"]
        assert isinstance(populator.metrics, PopulationMetrics)
        assert isinstance(populator.progress, ProgressTracker)

    @patch("src.scripts.populate.base.duckdb.connect")
    def test_connect(self, mock_connect, populator):
        """Test database connection management."""
        populator.connect()
        mock_connect.assert_called_once_with(populator.db_path)
        assert populator._conn is not None

    @patch("src.scripts.populate.base.DatabaseManager")
    def test_upsert_batch(self, mock_db_manager_class, populator):
        """Test batch upsert logic."""
        mock_db_manager = mock_db_manager_class.return_value
        mock_db_manager.bulk_upsert.return_value = 10

        df = pd.DataFrame({"id": range(10), "val": range(10)})
        inserted, updated = populator.upsert_batch(df)

        assert inserted == 10
        assert updated == 0
        mock_db_manager.bulk_upsert.assert_called_once()

    def test_run_happy_path(self, populator):
        """Test a successful population run."""
        mock_df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

        with (
            patch.object(populator, "upsert_batch", return_value=(2, 0)) as mock_upsert,
            patch.object(populator, "connect") as mock_conn,
            patch("src.scripts.utils.ui.create_progress_bar") as mock_pb,
        ):
            # Mock progress bar
            mock_pb.return_value.__enter__.return_value = MagicMock()

            results = populator.run(mock_df=mock_df)

            assert results["records_fetched"] == 2
            assert results["records_inserted"] == 2
            assert results["error_count"] == 0
            mock_upsert.assert_called_once()
            mock_conn.return_value.commit.assert_called()

    def test_run_no_data(self, populator):
        """Test run when no data is returned."""
        with patch.object(populator, "fetch_data", return_value=None):
            results = populator.run()
            assert results["records_fetched"] == 0
            assert results["records_inserted"] == 0

    def test_run_validation_failure(self, populator):
        """Test run when validation fails."""
        mock_df = pd.DataFrame({"id": [1]})
        populator.get_expected_columns = MagicMock(return_value=["missing_col"])

        with patch.object(populator, "fetch_data", return_value=mock_df):
            results = populator.run()
            assert results["error_count"] > 0
            # Should return early before insertion
            assert results["records_inserted"] == 0

    def test_run_exception_handling(self, populator):
        """Test run handles exceptions gracefully."""
        with patch.object(populator, "fetch_data", side_effect=Exception("API Error")):
            with pytest.raises(Exception, match="API Error"):
                populator.run()

            assert len(populator.metrics.errors) == 1
            assert populator.metrics.errors[0]["error"] == "API Error"
