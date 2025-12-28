"""Unit tests for scripts/check_integrity.py module.

This module tests database integrity checking functionality including:
- Primary key validation and constraint checking
- Foreign key relationship validation
- Orphan record detection
- Data quality checks (TODO implementation)
"""

from unittest.mock import MagicMock, patch

import pytest

from scripts.check_integrity import check_integrity


class TestCheckIntegrity:
    """Test suite for database integrity checking functions."""

    def test_check_integrity_connects_to_database(self):
        """
        Verify check_integrity opens the expected DuckDB database file and closes the connection.
        
        Asserts that duckdb.connect is called with "data/nba.duckdb" and that the returned connection's close method is invoked.
        """
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
        """
        Ensure check_integrity detects non-unique primary key values and refrains from adding PRIMARY KEY constraints when duplicates exist.
        
        Verifies that when a table's total primary key count differs from its unique count (indicating duplicates), check_integrity does not execute ALTER TABLE ... ADD PRIMARY KEY for that table.
        """
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

            # Verify ALTER TABLE was not called for table with duplicates
            alter_calls = [c for c in mock_con.sql.call_args_list 
                          if "ALTER TABLE" in str(c)]
            # When duplicates exist, no PK constraint should be added
            assert len(alter_calls) == 0 or all("team_silver" not in str(c) for c in alter_calls), \
                "ALTER TABLE should not be called when duplicates exist"

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

            # Should not add PK constraints when NULLs exist
            calls = [str(c) for c in mock_con.sql.call_args_list]
            pk_constraint_calls = [c for c in calls if "PRIMARY KEY" in c and "team_silver" in c]
            assert len(pk_constraint_calls) == 0, \
                "PRIMARY KEY constraint should not be added when NULLs exist"

    def test_check_integrity_validates_foreign_keys(self):
        """
        Verifies that check_integrity validates foreign key relationships and runs orphan checks.
        
        Asserts that when no orphan records are reported, the SQL executed against the database includes evidence of foreign-key relationship checks (e.g., referencing the related table names or using LEFT JOIN).
        """
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
        """
        Verify that check_integrity identifies foreign-key orphan records and retrieves a sample of orphan IDs.
        
        Simulates FK checks producing orphan counts and confirms the function issues a query with a LIMIT clause to fetch example orphan records.
        """
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
        """
        Ensure a database connection error raised during connect propagates.
        
        Verifies that if duckdb.connect raises an Exception, check_integrity raises the same error instead of suppressing it.
        """
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
        """
        Verifies that when a table has a valid primary key (total equals unique and zero nulls), the integrity checker attempts to add a PRIMARY KEY constraint.
        
        Mocks a database connection returning total, unique, and null counts for a candidate primary key and asserts that at least one ALTER TABLE statement was executed.
        """
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
        """
        Verifies that check_integrity closes the database connection after successful completion.
        
        Asserts that a mocked database connection's close() method is called exactly once.
        """
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

    @pytest.mark.parametrize(("table", "pk_col"), [
        ("team_silver", "id"),
        ("player_silver", "id"),
        ("game_silver", "game_id"),
    ])
    def test_check_integrity_validates_all_pk_candidates(self, table, pk_col):
        """
        Verifies that the integrity check queries include each primary-key candidate table.
        
        Parameters:
            table (str): The table name expected to be validated as a primary-key candidate.
            pk_col (str): The primary-key column name candidate for the given table.
        """
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.sql.return_value.fetchone.return_value = [0]

            check_integrity()

            calls = [str(c) for c in mock_con.sql.call_args_list]
            calls_str = " ".join(calls)
            assert table in calls_str

    def test_check_integrity_handles_missing_tables_gracefully(self):
        """
        Verifies that check_integrity does not raise when a referenced table is missing and that the database connection is closed.
        """
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            def side_effect_fn(query):
                """
                Simulate a database query side effect for tests: raise for a missing table name or return a single-row result.
                
                Parameters:
                    query (str): SQL query string inspected to decide behavior.
                
                Returns:
                    MagicMock: A mock whose `fetchone()` returns `[0]` when `query` does not mention "team_silver".
                
                Raises:
                    Exception: If the `query` string contains "team_silver", raises Exception("Table does not exist").
                """
                if "team_silver" in query:
                    raise Exception("Table does not exist")
                return MagicMock(fetchone=lambda: [0])

            mock_con.sql.side_effect = side_effect_fn

            # Should not crash
            check_integrity()

            mock_con.close.assert_called_once()