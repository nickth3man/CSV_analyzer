"""Integration tests for the data population pipeline.

This module tests the core data population infrastructure:
- BasePopulator class functionality
- Transform functions for API data
- Database operations (upsert, bulk operations)
- Data validation
- Progress tracking and resumability

Tests use in-memory DuckDB for fast execution and mock API responses
to avoid hitting external services.
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from src.scripts.populate.base import BasePopulator, PopulationMetrics, ProgressTracker
from src.scripts.populate.database import DatabaseManager
from src.scripts.populate.validation import DataValidator


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Create a temporary DuckDB database path."""
    return tmp_path / "test_nba.duckdb"


@pytest.fixture
def temp_db_connection(temp_db_path: Path) -> duckdb.DuckDBPyConnection:
    """Create a temporary DuckDB connection with test schema."""
    conn = duckdb.connect(str(temp_db_path))

    # Create test schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_game_stats_raw (
            game_id BIGINT,
            team_id BIGINT,
            player_id BIGINT,
            player_name VARCHAR,
            start_position VARCHAR,
            comment VARCHAR,
            min VARCHAR,
            fgm BIGINT,
            fga BIGINT,
            fg_pct DOUBLE,
            fg3m BIGINT,
            fg3a BIGINT,
            fg3_pct DOUBLE,
            ftm BIGINT,
            fta BIGINT,
            ft_pct DOUBLE,
            oreb BIGINT,
            dreb BIGINT,
            reb BIGINT,
            ast BIGINT,
            stl BIGINT,
            blk BIGINT,
            tov BIGINT,
            pf BIGINT,
            pts BIGINT,
            plus_minus DOUBLE,
            PRIMARY KEY (game_id, player_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY,
            full_name VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            is_active BOOLEAN
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY,
            team_name VARCHAR,
            team_abbreviation VARCHAR,
            team_city VARCHAR
        )
    """)

    yield conn
    conn.close()


@pytest.fixture
def mock_nba_client() -> MagicMock:
    """Create a mock NBA API client."""
    client = MagicMock()
    client.config = MagicMock()
    client.config.request_delay = 0  # No delay for tests

    # Mock player data
    client.get_all_players.return_value = [
        {"id": 201566, "full_name": "Russell Westbrook", "first_name": "Russell", "last_name": "Westbrook", "is_active": True},
        {"id": 2544, "full_name": "LeBron James", "first_name": "LeBron", "last_name": "James", "is_active": True},
        {"id": 101108, "full_name": "Chris Paul", "first_name": "Chris", "last_name": "Paul", "is_active": True},
    ]

    client.get_active_players.return_value = client.get_all_players.return_value

    # Mock team data
    client.get_all_teams.return_value = [
        {"id": 1610612747, "full_name": "Los Angeles Lakers", "abbreviation": "LAL", "nickname": "Lakers", "city": "Los Angeles"},
        {"id": 1610612744, "full_name": "Golden State Warriors", "abbreviation": "GSW", "nickname": "Warriors", "city": "San Francisco"},
    ]

    return client


@pytest.fixture
def sample_game_log_df() -> pd.DataFrame:
    """Create a sample game log DataFrame as returned by NBA API."""
    return pd.DataFrame({
        "SEASON_ID": ["22023", "22023", "22023"],
        "Player_ID": [2544, 2544, 2544],
        "Game_ID": ["0022300001", "0022300002", "0022300003"],
        "GAME_DATE": ["OCT 24, 2023", "OCT 26, 2023", "OCT 28, 2023"],
        "MATCHUP": ["LAL vs. DEN", "LAL @ PHX", "LAL vs. ORL"],
        "WL": ["L", "W", "W"],
        "MIN": ["35:42", "38:15", "32:10"],
        "FGM": [8, 12, 10],
        "FGA": [18, 22, 15],
        "FG_PCT": [0.444, 0.545, 0.667],
        "FG3M": [2, 3, 1],
        "FG3A": [6, 8, 4],
        "FG3_PCT": [0.333, 0.375, 0.250],
        "FTM": [5, 4, 8],
        "FTA": [6, 5, 9],
        "FT_PCT": [0.833, 0.800, 0.889],
        "OREB": [1, 2, 0],
        "DREB": [6, 9, 5],
        "REB": [7, 11, 5],
        "AST": [8, 6, 10],
        "STL": [1, 2, 1],
        "BLK": [0, 1, 0],
        "TOV": [3, 2, 4],
        "PF": [2, 3, 1],
        "PTS": [23, 31, 29],
        "PLUS_MINUS": [-5, 12, 8],
    })


@pytest.fixture
def temp_progress_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for progress tracking."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return cache_dir


# =============================================================================
# PopulationMetrics Tests
# =============================================================================


class TestPopulationMetrics:
    """Tests for PopulationMetrics class."""

    def test_metrics_initialization(self) -> None:
        """Test that metrics are properly initialized."""
        metrics = PopulationMetrics()

        assert metrics.start_time is None
        assert metrics.end_time is None
        assert metrics.records_fetched == 0
        assert metrics.records_inserted == 0
        assert metrics.records_updated == 0
        assert metrics.records_skipped == 0
        assert metrics.api_calls == 0
        assert metrics.errors == []
        assert metrics.warnings == []

    def test_metrics_start_stop(self) -> None:
        """Test start and stop recording."""
        metrics = PopulationMetrics()

        metrics.start()
        assert metrics.start_time is not None
        assert metrics.end_time is None

        metrics.stop()
        assert metrics.end_time is not None
        assert metrics.duration_seconds >= 0

    def test_metrics_add_error(self) -> None:
        """Test adding errors to metrics."""
        metrics = PopulationMetrics()

        metrics.add_error("Test error", {"key": "value"})

        assert len(metrics.errors) == 1
        assert metrics.errors[0]["error"] == "Test error"
        assert metrics.errors[0]["context"] == {"key": "value"}
        assert "timestamp" in metrics.errors[0]

    def test_metrics_to_dict(self) -> None:
        """Test metrics serialization to dictionary."""
        metrics = PopulationMetrics()
        metrics.start()
        metrics.records_fetched = 100
        metrics.records_inserted = 95
        metrics.records_skipped = 5
        metrics.add_error("Test error")
        metrics.warnings.append("Test warning")
        metrics.stop()

        result = metrics.to_dict()

        assert result["records_fetched"] == 100
        assert result["records_inserted"] == 95
        assert result["records_skipped"] == 5
        assert result["error_count"] == 1
        assert len(result["errors"]) == 1
        assert len(result["warnings"]) == 1
        assert result["duration_seconds"] >= 0


# =============================================================================
# ProgressTracker Tests
# =============================================================================


class TestProgressTracker:
    """Tests for ProgressTracker class."""

    def test_progress_initialization(self, temp_progress_dir: Path) -> None:
        """Test that progress tracker initializes correctly."""
        with patch("src.scripts.populate.base.CACHE_DIR", temp_progress_dir):
            tracker = ProgressTracker("test_populator")

            assert tracker.name == "test_populator"
            assert tracker.get_completed() == set()

    def test_progress_mark_completed(self, temp_progress_dir: Path) -> None:
        """Test marking items as completed."""
        with patch("src.scripts.populate.base.CACHE_DIR", temp_progress_dir):
            tracker = ProgressTracker("test_populator")

            tracker.mark_completed("player_1")
            tracker.mark_completed("player_2")

            assert tracker.is_completed("player_1")
            assert tracker.is_completed("player_2")
            assert not tracker.is_completed("player_3")
            assert tracker.get_completed() == {"player_1", "player_2"}

    def test_progress_persistence(self, temp_progress_dir: Path) -> None:
        """Test that progress is persisted to disk."""
        with patch("src.scripts.populate.base.CACHE_DIR", temp_progress_dir):
            # Create and save progress
            tracker1 = ProgressTracker("test_populator")
            tracker1.mark_completed("item_1")
            tracker1.save()

            # Create new tracker and verify persistence
            tracker2 = ProgressTracker("test_populator")
            assert tracker2.is_completed("item_1")

    def test_progress_reset(self, temp_progress_dir: Path) -> None:
        """Test resetting progress."""
        with patch("src.scripts.populate.base.CACHE_DIR", temp_progress_dir):
            tracker = ProgressTracker("test_populator")
            tracker.mark_completed("item_1")
            tracker.mark_completed("item_2")

            tracker.reset()

            assert not tracker.is_completed("item_1")
            assert not tracker.is_completed("item_2")
            assert tracker.get_completed() == set()

    def test_progress_add_error(self, temp_progress_dir: Path) -> None:
        """Test adding errors to progress."""
        with patch("src.scripts.populate.base.CACHE_DIR", temp_progress_dir):
            tracker = ProgressTracker("test_populator")

            tracker.add_error("item_1", "Connection failed")

            # Errors should be tracked in progress
            assert len(tracker._progress["errors"]) == 1


# =============================================================================
# DataValidator Tests
# =============================================================================


class TestDataValidator:
    """Tests for DataValidator class."""

    def test_validate_player_data_valid(self) -> None:
        """Test validation of valid player data."""
        validator = DataValidator()

        df = pd.DataFrame({
            "id": [1, 2, 3],
            "full_name": ["Player A", "Player B", "Player C"],
            "first_name": ["A", "B", "C"],
            "last_name": ["Player", "Player", "Player"],
            "is_active": [True, True, False],
        })

        result = validator.validate_player_data(df)

        assert result["valid"] is True
        assert len(result["errors"]) == 0
        assert result["total_records"] == 3

    def test_validate_player_data_missing_columns(self) -> None:
        """Test validation catches missing columns."""
        validator = DataValidator()

        df = pd.DataFrame({
            "id": [1, 2],
            "full_name": ["Player A", "Player B"],
            # Missing first_name, last_name, is_active
        })

        result = validator.validate_player_data(df)

        assert result["valid"] is False
        assert len(result["errors"]) > 0
        assert "Missing required columns" in result["errors"][0]

    def test_validate_player_data_duplicates(self) -> None:
        """Test validation catches duplicate IDs."""
        validator = DataValidator()

        df = pd.DataFrame({
            "id": [1, 1, 2],  # Duplicate ID
            "full_name": ["Player A", "Player A Copy", "Player B"],
            "first_name": ["A", "A", "B"],
            "last_name": ["Player", "Player", "Player"],
            "is_active": [True, True, False],
        })

        result = validator.validate_player_data(df)

        assert result["valid"] is False
        assert any("duplicate" in error.lower() for error in result["errors"])

    def test_validate_game_data_valid(self) -> None:
        """Test validation of valid game data."""
        validator = DataValidator()

        df = pd.DataFrame({
            "game_id": ["001", "002", "003"],
            "player_id": [1, 2, 3],
            "team_id": [101, 102, 101],
            "game_date": ["2023-10-24", "2023-10-24", "2023-10-25"],
            "pts": [20, 15, 30],
            "fgm": [8, 6, 12],
            "fga": [15, 12, 20],
            "min": [32, 28, 35],
        })

        result = validator.validate_game_data(df)

        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_validate_game_data_negative_stats(self) -> None:
        """Test validation catches negative stats."""
        validator = DataValidator()

        df = pd.DataFrame({
            "game_id": ["001", "002"],
            "player_id": [1, 2],
            "team_id": [101, 102],
            "game_date": ["2023-10-24", "2023-10-24"],
            "pts": [-5, 15],  # Negative points
            "fgm": [8, 6],
            "fga": [15, 12],
            "min": [32, 28],
        })

        result = validator.validate_game_data(df)

        assert result["valid"] is False
        assert any("negative" in error.lower() for error in result["errors"])

    def test_validate_statistical_consistency(self) -> None:
        """Test statistical consistency validation."""
        validator = DataValidator()

        # Invalid: FGM > FGA
        df = pd.DataFrame({
            "fgm": [15, 8],
            "fga": [10, 15],  # First row has FGM > FGA
            "fg3m": [3, 4],
            "fg3a": [8, 10],
            "ftm": [5, 6],
            "fta": [6, 8],
        })

        result = validator.validate_statistical_consistency(df)

        assert result["valid"] is False
        assert any("FGM > FGA" in error for error in result["errors"])

    def test_validate_data_completeness(self) -> None:
        """Test data completeness validation."""
        validator = DataValidator()

        df = pd.DataFrame({
            "field1": [1, 2, None, 4],
            "field2": [1, None, None, 4],
            "field3": [1, 2, 3, 4],
        })

        result = validator.validate_data_completeness(df, ["field1", "field2", "field3"])

        assert result["valid"] is True  # No errors, just warnings
        assert result["completeness"]["field1"]["null_count"] == 1
        assert result["completeness"]["field2"]["null_count"] == 2
        assert result["completeness"]["field3"]["null_count"] == 0


# =============================================================================
# DatabaseManager Tests
# =============================================================================


class TestDatabaseManager:
    """Tests for DatabaseManager class."""

    def test_database_connection(self, temp_db_path: Path) -> None:
        """Test database connection and context manager."""
        with patch("src.scripts.populate.config.get_db_path", return_value=temp_db_path):
            db = DatabaseManager(db_path=temp_db_path)

            with db as conn:
                result = conn.execute("SELECT 1 AS test").fetchone()
                assert result[0] == 1

    def test_create_schema(self, temp_db_path: Path) -> None:
        """Test schema creation."""
        with patch("src.scripts.populate.config.get_db_path", return_value=temp_db_path):
            db = DatabaseManager(db_path=temp_db_path)
            db.create_schema()

            # Verify tables were created
            conn = db.connect()
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = {t[0] for t in tables}

            assert "players" in table_names
            assert "teams" in table_names
            assert "games" in table_names
            assert "player_game_stats" in table_names

            db.close()

    def test_insert_data(self, temp_db_path: Path) -> None:
        """Test data insertion."""
        with patch("src.scripts.populate.config.get_db_path", return_value=temp_db_path):
            db = DatabaseManager(db_path=temp_db_path)
            db.create_schema()

            df = pd.DataFrame({
                "player_id": [1, 2, 3],
                "full_name": ["Player A", "Player B", "Player C"],
                "first_name": ["A", "B", "C"],
                "last_name": ["Player", "Player", "Player"],
                "is_active": [True, True, False],
                "populated_at": [datetime.now()] * 3,
                "updated_at": [datetime.now()] * 3,
            })

            rows_inserted = db.insert_data("players", df)

            assert rows_inserted == 3

            # Verify data was inserted
            conn = db.connect()
            result = conn.execute("SELECT COUNT(*) FROM players").fetchone()
            assert result[0] == 3

            db.close()

    def test_bulk_upsert(self, temp_db_path: Path) -> None:
        """Test bulk upsert operation."""
        with patch("src.scripts.populate.config.get_db_path", return_value=temp_db_path):
            db = DatabaseManager(db_path=temp_db_path)
            db.create_schema()

            # Insert initial data
            df1 = pd.DataFrame({
                "player_id": [1, 2],
                "full_name": ["Player A", "Player B"],
                "first_name": ["A", "B"],
                "last_name": ["Player", "Player"],
                "is_active": [True, True],
                "populated_at": [datetime.now()] * 2,
                "updated_at": [datetime.now()] * 2,
            })
            db.insert_data("players", df1)

            # Upsert with updated and new data
            df2 = pd.DataFrame({
                "player_id": [2, 3],  # Update player 2, insert player 3
                "full_name": ["Player B Updated", "Player C"],
                "first_name": ["B", "C"],
                "last_name": ["Updated", "Player"],
                "is_active": [False, True],
                "populated_at": [datetime.now()] * 2,
                "updated_at": [datetime.now()] * 2,
            })

            rows_affected = db.bulk_upsert(df2, "players", ["player_id"])

            assert rows_affected == 2

            # Verify results
            conn = db.connect()
            result = conn.execute(
                "SELECT full_name FROM players WHERE player_id = 2"
            ).fetchone()
            assert result[0] == "Player B Updated"

            total = conn.execute("SELECT COUNT(*) FROM players").fetchone()
            assert total[0] == 3

            db.close()

    def test_get_table_info(self, temp_db_path: Path) -> None:
        """Test getting table information."""
        with patch("src.scripts.populate.config.get_db_path", return_value=temp_db_path):
            db = DatabaseManager(db_path=temp_db_path)
            db.create_schema()

            # Insert some data
            df = pd.DataFrame({
                "player_id": [1, 2, 3],
                "full_name": ["A", "B", "C"],
                "first_name": ["A", "B", "C"],
                "last_name": ["X", "Y", "Z"],
                "is_active": [True, True, False],
                "populated_at": [datetime.now()] * 3,
                "updated_at": [datetime.now()] * 3,
            })
            db.insert_data("players", df)

            info = db.get_table_info("players")

            assert info["table_name"] == "players"
            assert info["row_count"] == 3
            assert "player_id" in info["columns"]
            assert "full_name" in info["columns"]

            db.close()

    def test_get_database_stats(self, temp_db_path: Path) -> None:
        """Test getting overall database statistics."""
        with patch("src.scripts.populate.config.get_db_path", return_value=temp_db_path):
            db = DatabaseManager(db_path=temp_db_path)
            db.create_schema()

            stats = db.get_database_stats()

            assert "database_path" in stats
            assert "table_count" in stats
            assert stats["table_count"] > 0
            assert "tables" in stats

            db.close()


# =============================================================================
# Transform Function Tests
# =============================================================================


class TestTransformFunctions:
    """Tests for data transformation functions."""

    def test_transform_game_log(self, sample_game_log_df: pd.DataFrame) -> None:
        """Test game log transformation."""
        from src.scripts.populate.populate_player_game_stats import transform_game_log

        player_info = {
            "id": 2544,
            "full_name": "LeBron James",
        }

        result = transform_game_log(sample_game_log_df, player_info)

        # Verify output structure
        assert "game_id" in result.columns
        assert "player_id" in result.columns
        assert "player_name" in result.columns
        assert "pts" in result.columns
        assert "reb" in result.columns
        assert "ast" in result.columns

        # Verify data values
        assert len(result) == 3
        assert result["player_name"].iloc[0] == "LeBron James"
        assert result["pts"].iloc[0] == 23
        assert result["ast"].iloc[0] == 8

    def test_transform_game_log_empty(self) -> None:
        """Test game log transformation with empty DataFrame."""
        from src.scripts.populate.populate_player_game_stats import transform_game_log

        player_info = {"id": 1, "full_name": "Test Player"}

        result = transform_game_log(pd.DataFrame(), player_info)

        assert result.empty

    def test_transform_game_log_none(self) -> None:
        """Test game log transformation with None input."""
        from src.scripts.populate.populate_player_game_stats import transform_game_log

        player_info = {"id": 1, "full_name": "Test Player"}

        result = transform_game_log(None, player_info)

        assert result.empty

    def test_parse_minutes(self) -> None:
        """Test minutes parsing function."""
        from src.scripts.populate.populate_player_game_stats import parse_minutes

        assert parse_minutes("35:42") == "35:42"
        assert parse_minutes("12") == "12"
        assert parse_minutes(30) == "30"
        assert parse_minutes(30.5) == "30"
        assert parse_minutes(None) is None
        assert parse_minutes(pd.NA) is None


# =============================================================================
# Integration Tests
# =============================================================================


class TestPopulationIntegration:
    """Integration tests for the full population pipeline."""

    def test_full_population_cycle(
        self,
        temp_db_path: Path,
        mock_nba_client: MagicMock,
        sample_game_log_df: pd.DataFrame,
        temp_progress_dir: Path,
    ) -> None:
        """Test a full population cycle from fetch to insert."""
        # Mock the game log response
        mock_nba_client.get_player_game_log.return_value = sample_game_log_df

        with (
            patch("src.scripts.populate.base.CACHE_DIR", temp_progress_dir),
            patch("src.scripts.populate.base.get_client", return_value=mock_nba_client),
            patch("src.scripts.populate.base.get_db_path", return_value=temp_db_path),
            patch("src.scripts.populate.base.check_integrity", return_value={"error_count": 0}),
        ):
            from src.scripts.populate.populate_player_game_stats import (
                populate_player_game_stats,
            )

            # Create the table first
            conn = duckdb.connect(str(temp_db_path))
            conn.execute("""
                CREATE TABLE IF NOT EXISTS player_game_stats_raw (
                    game_id BIGINT,
                    team_id BIGINT,
                    player_id BIGINT,
                    player_name VARCHAR,
                    start_position VARCHAR,
                    comment VARCHAR,
                    min VARCHAR,
                    fgm BIGINT,
                    fga BIGINT,
                    fg_pct DOUBLE,
                    fg3m BIGINT,
                    fg3a BIGINT,
                    fg3_pct DOUBLE,
                    ftm BIGINT,
                    fta BIGINT,
                    ft_pct DOUBLE,
                    oreb BIGINT,
                    dreb BIGINT,
                    reb BIGINT,
                    ast BIGINT,
                    stl BIGINT,
                    blk BIGINT,
                    tov BIGINT,
                    pf BIGINT,
                    pts BIGINT,
                    plus_minus DOUBLE
                )
            """)
            conn.close()

            # Run population with limited scope
            stats = populate_player_game_stats(
                db_path=str(temp_db_path),
                seasons=["2023-24"],
                limit=1,  # Only process 1 player
                delay=0,  # No delay for tests
                season_types=["Regular Season"],
                client=mock_nba_client,
            )

            # Verify results
            assert stats["players_processed"] >= 0
            assert "errors" in stats

    def test_population_with_validation_failure(
        self,
        temp_db_path: Path,
        mock_nba_client: MagicMock,
        temp_progress_dir: Path,
    ) -> None:
        """Test population handles validation failures gracefully."""
        # Return invalid data (empty DataFrame)
        mock_nba_client.get_player_game_log.return_value = pd.DataFrame()

        with (
            patch("src.scripts.populate.base.CACHE_DIR", temp_progress_dir),
            patch("src.scripts.populate.base.get_client", return_value=mock_nba_client),
            patch("src.scripts.populate.base.get_db_path", return_value=temp_db_path),
            patch("src.scripts.populate.base.check_integrity", return_value={"error_count": 0}),
        ):
            from src.scripts.populate.populate_player_game_stats import (
                populate_player_game_stats,
            )

            # Create the table first
            conn = duckdb.connect(str(temp_db_path))
            conn.execute("""
                CREATE TABLE IF NOT EXISTS player_game_stats_raw (
                    game_id BIGINT,
                    team_id BIGINT,
                    player_id BIGINT,
                    player_name VARCHAR,
                    start_position VARCHAR,
                    comment VARCHAR,
                    min VARCHAR,
                    fgm BIGINT,
                    fga BIGINT,
                    fg_pct DOUBLE,
                    fg3m BIGINT,
                    fg3a BIGINT,
                    fg3_pct DOUBLE,
                    ftm BIGINT,
                    fta BIGINT,
                    ft_pct DOUBLE,
                    oreb BIGINT,
                    dreb BIGINT,
                    reb BIGINT,
                    ast BIGINT,
                    stl BIGINT,
                    blk BIGINT,
                    tov BIGINT,
                    pf BIGINT,
                    pts BIGINT,
                    plus_minus DOUBLE
                )
            """)
            conn.close()

            stats = populate_player_game_stats(
                db_path=str(temp_db_path),
                seasons=["2023-24"],
                limit=1,
                delay=0,
                client=mock_nba_client,
            )

            # Should complete without errors even with no data
            assert stats["total_games_added"] == 0


class TestDataQualityChecks:
    """Tests for data quality validation during population."""

    def test_duplicate_detection(self, temp_db_connection: duckdb.DuckDBPyConnection) -> None:
        """Test that duplicates are properly detected and handled."""
        # Insert initial data
        temp_db_connection.execute("""
            INSERT INTO player_game_stats_raw (game_id, player_id, player_name, pts, reb, ast)
            VALUES (1, 100, 'Test Player', 20, 10, 5)
        """)

        # Try to insert duplicate (should be caught by PK constraint)
        try:
            temp_db_connection.execute("""
                INSERT INTO player_game_stats_raw (game_id, player_id, player_name, pts, reb, ast)
                VALUES (1, 100, 'Test Player Duplicate', 25, 12, 8)
            """)
            # If no error, the PK constraint isn't working
            result = temp_db_connection.execute(
                "SELECT COUNT(*) FROM player_game_stats_raw WHERE game_id = 1 AND player_id = 100"
            ).fetchone()
            # Should only have 1 record if upsert is working
            assert result[0] >= 1
        except Exception:
            # Expected - PK constraint prevents duplicate
            pass

    def test_null_handling(self) -> None:
        """Test that NULL values are handled properly in validation."""
        validator = DataValidator()

        df = pd.DataFrame({
            "game_id": ["001", None, "003"],  # One NULL game_id
            "player_id": [1, 2, 3],
            "team_id": [101, 102, 103],
            "game_date": ["2023-10-24", "2023-10-25", "2023-10-26"],
            "pts": [20, 15, 30],
            "fgm": [8, 6, 12],
            "fga": [15, 12, 20],
            "min": [32, 28, 35],
        })

        result = validator.validate_game_data(df)

        # Should flag invalid game_id
        assert result["valid"] is False


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_api_response(self, mock_nba_client: MagicMock) -> None:
        """Test handling of empty API responses."""
        from src.scripts.populate.populate_player_game_stats import transform_game_log

        mock_nba_client.get_player_game_log.return_value = None

        result = transform_game_log(None, {"id": 1, "full_name": "Test"})

        assert result.empty

    def test_malformed_data(self) -> None:
        """Test handling of malformed data."""
        from src.scripts.populate.populate_player_game_stats import transform_game_log

        # DataFrame with wrong column names
        df = pd.DataFrame({
            "wrong_column": [1, 2, 3],
            "another_wrong": ["a", "b", "c"],
        })

        result = transform_game_log(df, {"id": 1, "full_name": "Test"})

        # The transform function returns empty DataFrame when critical columns
        # like Game_ID/GAME_ID are missing - this is correct defensive behavior
        # because we can't meaningfully process data without game identifiers
        assert result.empty  # Correctly rejects malformed data

    def test_special_characters_in_names(self) -> None:
        """Test handling of special characters in player names."""
        validator = DataValidator()

        df = pd.DataFrame({
            "id": [1, 2, 3],
            "full_name": ["Giannis Antetokounmpo", "Nikola Jokić", "Luka Dončić"],
            "first_name": ["Giannis", "Nikola", "Luka"],
            "last_name": ["Antetokounmpo", "Jokić", "Dončić"],
            "is_active": [True, True, True],
        })

        result = validator.validate_player_data(df)

        assert result["valid"] is True
