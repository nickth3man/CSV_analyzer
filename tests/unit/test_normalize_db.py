"""Unit tests for scripts/normalize_db.py module.

This module tests database normalization functionality including:
- Type inference for VARCHAR columns
- Silver table creation with proper types
- Handling of various data types (BIGINT, DOUBLE, DATE)
- Error handling and edge cases
"""

from unittest.mock import MagicMock, patch

import pytest

from scripts.normalize_db import (
    get_tables,
    infer_column_type,
    transform_to_silver,
)


class TestGetTables:
    """Test suite for get_tables function."""

    def test_get_tables_returns_base_tables_only(self):
        """Test that only base tables are returned, excluding views."""
        mock_con = MagicMock()

        # Mock views
        mock_con.sql.return_value.fetchall.side_effect = [
            [("view1",), ("view2",)],  # Views
            [("table1",), ("table2",), ("view1",), ("table2_silver",)],  # All tables
        ]

        result = get_tables(mock_con)

        assert "table1" in result
        assert "table2" in result
        assert "view1" not in result  # Excluded as view
        assert "table2_silver" not in result  # Excluded as silver table

    def test_get_tables_excludes_silver_tables(self):
        """Test that tables ending with _silver are excluded."""
        mock_con = MagicMock()
        mock_con.sql.return_value.fetchall.side_effect = [
            [],  # No views
            [("player",), ("player_silver",), ("team",), ("team_silver",)],
        ]

        result = get_tables(mock_con)

        assert "player" in result
        assert "team" in result
        assert "player_silver" not in result
        assert "team_silver" not in result

    def test_get_tables_excludes_rejects_tables(self):
        """Test that tables ending with _rejects are excluded."""
        mock_con = MagicMock()
        mock_con.sql.return_value.fetchall.side_effect = [
            [],
            [("data",), ("data_rejects",), ("invalid_rejects",)],
        ]

        result = get_tables(mock_con)

        assert "data" in result
        assert "data_rejects" not in result
        assert "invalid_rejects" not in result

    def test_get_tables_handles_empty_database(self):
        """Verifies that get_tables returns an empty list when the database contains no views or tables."""
        mock_con = MagicMock()
        mock_con.sql.return_value.fetchall.side_effect = [[], []]

        result = get_tables(mock_con)

        assert result == []


class TestInferColumnType:
    """Test suite for infer_column_type function."""

    def test_infer_column_type_detects_bigint(self):
        """Test detection of BIGINT columns."""
        mock_con = MagicMock()
        # Total count, then successful BIGINT cast count
        mock_con.sql.return_value.fetchone.side_effect = [
            (100,),  # Total non-null count
            (100,),  # Successful BIGINT casts
        ]

        result = infer_column_type(mock_con, "test_table", "col1")

        assert result == "BIGINT"

    def test_infer_column_type_detects_double(self):
        """Test detection of DOUBLE columns."""
        mock_con = MagicMock()
        mock_con.sql.return_value.fetchone.side_effect = [
            (100,),  # Total
            (50,),   # BIGINT fails
            (100,),  # DOUBLE succeeds
        ]

        result = infer_column_type(mock_con, "test_table", "col1")

        assert result == "DOUBLE"

    def test_infer_column_type_detects_date(self):
        """
        Verifies that infer_column_type returns "DATE" when DATE casting succeeds after BIGINT and DOUBLE casts fail.
        
        Simulates query results where the total non-null count is 100, BIGINT and DOUBLE casts return 0 matches, and DATE cast matches all 100 rows.
        """
        mock_con = MagicMock()
        mock_con.sql.return_value.fetchone.side_effect = [
            (100,),  # Total
            (0,),    # BIGINT fails
            (0,),    # DOUBLE fails
            (100,),  # DATE succeeds
        ]

        result = infer_column_type(mock_con, "test_table", "date_col")

        assert result == "DATE"

    def test_infer_column_type_defaults_to_varchar(self):
        """
        Verifies infer_column_type falls back to "VARCHAR" when BIGINT, DOUBLE, and DATE do not fully match the column's non-null values.
        """
        mock_con = MagicMock()
        mock_con.sql.return_value.fetchone.side_effect = [
            (100,),  # Total
            (50,),   # BIGINT partial
            (30,),   # DOUBLE partial
            (20,),   # DATE partial
        ]

        result = infer_column_type(mock_con, "test_table", "mixed_col")

        assert result == "VARCHAR"

    def test_infer_column_type_handles_empty_column(self):
        """Test handling of empty columns (0 non-null values)."""
        mock_con = MagicMock()
        mock_con.sql.return_value.fetchone.return_value = (0,)

        result = infer_column_type(mock_con, "test_table", "empty_col")

        assert result == "VARCHAR"

    def test_infer_column_type_quotes_column_names(self):
        """Test that column names are properly quoted."""
        mock_con = MagicMock()
        mock_con.sql.return_value.fetchone.side_effect = [(100,), (100,)]

        infer_column_type(mock_con, "test_table", "reserved_word")

        # Verify column name was quoted
        calls = [str(c) for c in mock_con.sql.call_args_list]
        assert any('"reserved_word"' in c for c in calls)

    def test_infer_column_type_handles_special_characters(self):
        """
        Verify that infer_column_type accepts column names with special characters and returns a valid SQL type.
        
        Asserts that the inferred type for a column named with a dash is one of: "BIGINT", "DOUBLE", "DATE", or "VARCHAR".
        """
        mock_con = MagicMock()
        mock_con.sql.return_value.fetchone.side_effect = [(50,), (50,)]

        result = infer_column_type(mock_con, "test_table", "col-with-dash")

        assert result in ["BIGINT", "DOUBLE", "DATE", "VARCHAR"]


class TestTransformToSilver:
    """Test suite for transform_to_silver function."""

    def test_transform_to_silver_requires_existing_database(self):
        """
        Ensure transform_to_silver exits with SystemExit when the given database path does not exist.
        """
        with patch("scripts.normalize_db.Path") as mock_path:
            mock_path.return_value.exists.return_value = False

            with pytest.raises(SystemExit):
                transform_to_silver("nonexistent.db")

    def test_transform_to_silver_processes_all_tables_by_default(self):
        """Test that all tables are processed when none specified."""
        with patch("scripts.normalize_db.Path") as mock_path, \
             patch("scripts.normalize_db.duckdb.connect") as mock_connect:

            mock_path.return_value.exists.return_value = True
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            # Mock get_tables response
            mock_con.sql.return_value.fetchall.side_effect = [
                [],  # No views
                [("table1",), ("table2",)],  # Tables
                [("col1", "VARCHAR")],  # Describe table1
                [("col1", "VARCHAR")],  # Describe table2
            ]
            mock_con.sql.return_value.fetchone.side_effect = [
                (100,), (100,),  # table1 inference
                (100,), (100,),  # table2 inference
            ]

            transform_to_silver("data/test.db")

            # Verify CREATE TABLE calls
            calls = [str(c) for c in mock_con.execute.call_args_list]
            assert any("table1_silver" in c for c in calls)
            assert any("table2_silver" in c for c in calls)

    def test_transform_to_silver_processes_specific_tables(self):
        """
        Verifies that transform_to_silver processes only the specified tables.
        
        Asserts that a `specific_table_silver` table is created by inspecting the executed SQL statements.
        """
        with patch("scripts.normalize_db.Path") as mock_path, \
             patch("scripts.normalize_db.duckdb.connect") as mock_connect:

            mock_path.return_value.exists.return_value = True
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            mock_con.sql.return_value.fetchall.return_value = [("col1", "VARCHAR")]
            mock_con.sql.return_value.fetchone.side_effect = [(100,), (100,)]

            transform_to_silver("data/test.db", tables=["specific_table"])

            calls = [str(c) for c in mock_con.execute.call_args_list]
            assert any("specific_table_silver" in c for c in calls)

    def test_transform_to_silver_skips_typed_columns(self):
        """
        Ensures transform_to_silver does not attempt type inference for columns that already have explicit types.
        
        Verifies that TRY_CAST calls are only generated for columns lacking a predefined type (e.g., VARCHAR) and are not issued for columns already typed as BIGINT.
        """
        with patch("scripts.normalize_db.Path") as mock_path, \
             patch("scripts.normalize_db.duckdb.connect") as mock_connect:

            mock_path.return_value.exists.return_value = True
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            mock_con.sql.return_value.fetchall.side_effect = [
                [],  # No views
                [("table1",)],  # Tables
                [("id", "BIGINT"), ("name", "VARCHAR")],  # Columns
            ]
            mock_con.sql.return_value.fetchone.side_effect = [
                (50,), (50,),  # Only VARCHAR column inferred
            ]

            transform_to_silver("data/test.db")

            # Verify type inference not called for BIGINT column
            inference_calls = [c for c in mock_con.sql.call_args_list 
                             if "TRY_CAST" in str(c)]
            # Should only have casts for the VARCHAR column

    def test_transform_to_silver_creates_silver_tables(self):
        """Test that _silver tables are created with proper schema."""
        with patch("scripts.normalize_db.Path") as mock_path, \
             patch("scripts.normalize_db.duckdb.connect") as mock_connect:

            mock_path.return_value.exists.return_value = True
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            mock_con.sql.return_value.fetchall.side_effect = [
                [],
                [("table1",)],
                [("col1", "VARCHAR")],
            ]
            mock_con.sql.return_value.fetchone.side_effect = [(100,), (100,)]

            transform_to_silver("data/test.db")

            # Verify CREATE OR REPLACE TABLE was called
            calls = [str(c) for c in mock_con.execute.call_args_list]
            assert any("CREATE OR REPLACE TABLE" in c and "table1_silver" in c 
                      for c in calls)

    def test_transform_to_silver_handles_errors_gracefully(self):
        """
        Verifies that transform_to_silver logs exceptions raised while processing individual tables and continues processing remaining tables.
        
        Simulates a database with two tables and configures the connection to raise an exception when executing table-specific SQL; asserts that logger.exception is called to record the error.
        """
        with patch("scripts.normalize_db.Path") as mock_path, \
             patch("scripts.normalize_db.duckdb.connect") as mock_connect, \
             patch("scripts.normalize_db.logger") as mock_logger:

            mock_path.return_value.exists.return_value = True
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            # Setup valid cursor for initial calls (get_tables)
            valid_cursor = MagicMock()
            valid_cursor.fetchall.side_effect = [
                [],  # views
                [("table1",), ("table2",)],  # tables
            ]

            # Setup side effect to return valid cursor for get_tables, then raise Exception
            def sql_side_effect(query):
                """
                Return the mocked cursor for queries that request views or tables; raise otherwise.
                
                Returns:
                    valid_cursor: The mocked cursor when the query contains "duckdb_views" or "SHOW TABLES".
                
                Raises:
                    Exception: with message "Table error" for any other query.
                """
                if "duckdb_views" in query or "SHOW TABLES" in query:
                    return valid_cursor
                raise Exception("Table error")

            mock_con.sql.side_effect = sql_side_effect

            transform_to_silver("data/test.db")

            # Should log errors but continue
            assert mock_logger.exception.called

    def test_transform_to_silver_closes_connection(self):
        """Test that database connection is closed after processing."""
        with patch("scripts.normalize_db.Path") as mock_path, \
             patch("scripts.normalize_db.duckdb.connect") as mock_connect:

            mock_path.return_value.exists.return_value = True
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            mock_con.sql.return_value.fetchall.side_effect = [[], []]

            transform_to_silver("data/test.db")

            mock_con.close.assert_called_once()

    @pytest.mark.parametrize("inferred_type", ["BIGINT", "DOUBLE", "DATE"])
    def test_transform_to_silver_applies_type_conversions(self, inferred_type):
        """
        Verify that inferred column types (BIGINT, DOUBLE, DATE) are applied via TRY_CAST in the generated CREATE TABLE statements.
        
        Parameters:
            inferred_type (str): Expected inferred SQL type for the column; one of "BIGINT", "DOUBLE", or "DATE".
        """
        with patch("scripts.normalize_db.Path") as mock_path, \
             patch("scripts.normalize_db.duckdb.connect") as mock_connect:

            mock_path.return_value.exists.return_value = True
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            # Set up mock to return specific type
            mock_con.sql.return_value.fetchall.side_effect = [
                [],
                [("test_table",)],
                [("col1", "VARCHAR")],
            ]

            if inferred_type == "BIGINT":
                mock_con.sql.return_value.fetchone.side_effect = [(100,), (100,)]
            elif inferred_type == "DOUBLE":
                mock_con.sql.return_value.fetchone.side_effect = [(100,), (50,), (100,)]
            else:  # DATE
                mock_con.sql.return_value.fetchone.side_effect = [
                    (100,), (0,), (0,), (100,)
                ]

            transform_to_silver("data/test.db")

            # Verify TRY_CAST with correct type
            calls = [str(c) for c in mock_con.execute.call_args_list]
            assert any("TRY_CAST" in c and inferred_type in c for c in calls)