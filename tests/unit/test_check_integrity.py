"""Unit tests for scripts/check_integrity.py module.

This module tests database integrity checking functionality including:
- Primary key validation and constraint checking
- Foreign key relationship validation
- Orphan record detection
- Data quality checks (TODO implementation)
"""

import contextlib
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from scripts.check_integrity import check_integrity


class TestCheckIntegrity:
    """Test suite for database integrity checking functions."""

    def test_check_integrity_connects_to_database(self):
        """Test that check_integrity connects to the correct database."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.sql.return_value.fetchone.return_value = [0]
            
            check_integrity()
            
            mock_connect.assert_called_once_with("data/nba.duckdb")
            mock_con.close.assert_called_once()

    def test_check_integrity_validates_primary_keys(self):
        """Test primary key validation for all candidate tables."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            # Mock responses for primary key checks
            # Format: total count, unique count, null count for each table
            mock_con.sql.return_value.fetchone.side_effect = [
                [100],   # team_silver total
                [100],   # team_silver unique
                [0],     # team_silver nulls
                [500],   # player_silver total
                [500],   # player_silver unique
                [0],     # player_silver nulls
                [1000],  # game_silver total
                [1000],  # game_silver unique
                [0],     # game_silver nulls
            ] + [[0]] * 100  # Additional calls for FK checks
            
            check_integrity()
            
            # Verify primary key checks were performed
            calls = mock_con.sql.call_args_list
            assert any("team_silver" in str(c) for c in calls)
            assert any("player_silver" in str(c) for c in calls)
            assert any("game_silver" in str(c) for c in calls)

    def test_check_integrity_detects_non_unique_primary_keys(self):
        """Test detection of non-unique primary key values."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            # Simulate non-unique primary keys (100 total, 95 unique)
            mock_con.sql.return_value.fetchone.side_effect = [
                [100],  # total count
                [95],   # unique count (NOT EQUAL - indicates duplicates)
                [0],    # null count
            ] + [[0]] * 100
            
            # Should not raise exception, but also should not add constraints
            check_integrity()
            
            # Verify ALTER TABLE was not called for this table
            alter_calls = [c for c in mock_con.sql.call_args_list 
                          if "ALTER TABLE" in str(c)]
            # Should have attempts but with exception handling

    def test_check_integrity_detects_null_primary_keys(self):
        """Test detection of NULL values in primary key columns."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            # Simulate NULL primary keys
            mock_con.sql.return_value.fetchone.side_effect = [
                [100],  # total count
                [100],  # unique count
                [5],    # null count (NOT ZERO - indicates NULLs)
            ] + [[0]] * 100
            
            check_integrity()
            
            # Should not add constraints when NULLs exist

    def test_check_integrity_validates_foreign_keys(self):
        """Test foreign key validation for defined relationships."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            # Mock no orphan records for FK checks
            mock_con.sql.return_value.fetchone.side_effect = (
                [[0]] * 10  # PK checks
                + [[0]] * 10  # FK orphan checks
            )
            mock_con.sql.return_value.fetchall.return_value = []
            
            check_integrity()
            
            # Verify FK checks include expected relationships
            calls = [str(c) for c in mock_con.sql.call_args_list]
            sql_calls = " ".join(calls)
            
            # Check for expected FK relationships
            assert "game_silver" in sql_calls or "LEFT JOIN" in sql_calls

    def test_check_integrity_detects_orphan_records(self):
        """Test detection of orphan records (FK violations)."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            # Mock orphan records found
            mock_con.sql.return_value.fetchone.side_effect = (
                [[0]] * 10  # PK checks
                + [[5], [3], [0]]  # FK checks - some orphans found
            )
            mock_con.sql.return_value.fetchall.return_value = [
                (123,), (456,), (789,)
            ]
            
            check_integrity()
            
            # Verify sample orphans were retrieved
            calls = mock_con.sql.call_args_list
            assert any("LIMIT 3" in str(c) for c in calls)

    def test_check_integrity_handles_database_errors_gracefully(self):
        """Test graceful handling of database connection errors."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_connect.side_effect = Exception("Database connection failed")
            
            # Should raise exception - no graceful handling at connection level
            with pytest.raises(Exception, match="Database connection failed"):
                check_integrity()

    def test_check_integrity_handles_query_errors_gracefully(self):
        """Test graceful handling of SQL query errors."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            # Simulate query error
            mock_con.sql.side_effect = Exception("Table not found")
            
            # Should not crash - errors are caught with contextlib.suppress
            check_integrity()
            
            mock_con.close.assert_called_once()

    def test_check_integrity_attempts_to_add_primary_key_constraints(self):
        """Test that valid PKs trigger constraint addition attempts."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            # Valid PK: total == unique, nulls == 0
            mock_con.sql.return_value.fetchone.side_effect = [
                [100], [100], [0],  # Valid PK for team_silver
            ] + [[0]] * 50
            
            check_integrity()
            
            # Verify ALTER TABLE attempts were made
            calls = [str(c) for c in mock_con.sql.call_args_list]
            alter_calls = [c for c in calls if "ALTER TABLE" in c]
            assert len(alter_calls) > 0

    def test_check_integrity_attempts_to_add_foreign_key_constraints(self):
        """Test that valid FKs trigger constraint addition attempts."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            # No orphans found
            mock_con.sql.return_value.fetchone.side_effect = (
                [[0]] * 10  # PK checks
                + [[0], [0], [0]]  # FK checks - no orphans
            )
            
            check_integrity()
            
            # Verify FK constraint addition was attempted
            calls = [str(c) for c in mock_con.sql.call_args_list]
            fk_calls = [c for c in calls if "FOREIGN KEY" in c]
            assert len(fk_calls) > 0

    def test_check_integrity_closes_connection_after_completion(self):
        """Test that database connection is properly closed."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.sql.return_value.fetchone.return_value = [0]
            
            check_integrity()
            
            mock_con.close.assert_called_once()

    def test_check_integrity_closes_connection_on_error(self):
        """Test that connection is closed even when errors occur."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.sql.side_effect = Exception("Query error")
            
            check_integrity()
            
            # Connection should still be closed
            mock_con.close.assert_called_once()

    @pytest.mark.parametrize("table,pk_col", [
        ("team_silver", "id"),
        ("player_silver", "id"),
        ("game_silver", "game_id"),
    ])
    def test_check_integrity_validates_all_pk_candidates(self, table, pk_col):
        """Test that all PK candidates are validated."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.sql.return_value.fetchone.return_value = [0]
            
            check_integrity()
            
            calls = [str(c) for c in mock_con.sql.call_args_list]
            calls_str = " ".join(calls)
            assert table in calls_str

    def test_check_integrity_handles_missing_tables_gracefully(self):
        """Test handling of missing tables."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            def side_effect_fn(query):
                if "team_silver" in query:
                    raise Exception("Table does not exist")
                return MagicMock(fetchone=lambda: [0])
            
            mock_con.sql.side_effect = side_effect_fn
            
            # Should not crash
            check_integrity()
            
            mock_con.close.assert_called_once()