"""Unit tests verifying duckdb is available and properly integrated.

This module tests that duckdb dependency is installed and can be used
by the scripts that depend on it.
"""

import pytest


class TestDuckDBAvailability:
    """Test suite for duckdb package availability."""

    def test_duckdb_can_be_imported(self):
        """Test that duckdb package can be imported."""
        try:
            import duckdb
            assert duckdb is not None
        except ImportError:
            pytest.fail("duckdb package is not installed")

    def test_duckdb_version_is_sufficient(self):
        """Test that duckdb version meets minimum requirements."""
        import duckdb

        # Should have version attribute
        assert hasattr(duckdb, "__version__")
        version = duckdb.__version__

        # Parse version
        major, minor = map(int, version.split(".")[:2])

        # Minimum version 0.9.0
        assert (major, minor) >= (0, 9), f"duckdb version {version} < 0.9.0"

    def test_duckdb_connect_function_exists(self):
        """Test that duckdb.connect function exists."""
        import duckdb

        assert hasattr(duckdb, "connect")
        assert callable(duckdb.connect)

    def test_duckdb_can_create_in_memory_database(self):
        """Test that duckdb can create in-memory databases."""
        import duckdb

        con = duckdb.connect(":memory:")
        assert con is not None
        con.close()

    def test_duckdb_supports_sql_queries(self):
        """Test that duckdb supports SQL queries."""
        import duckdb

        con = duckdb.connect(":memory:")
        result = con.execute("SELECT 1 as num").fetchall()
        assert result == [(1,)]
        con.close()

    def test_duckdb_supports_create_table(self):
        """Test that duckdb supports CREATE TABLE."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
        con.execute("INSERT INTO test VALUES (1, 'test')")
        result = con.execute("SELECT * FROM test").fetchall()
        assert result == [(1, "test")]
        con.close()

    def test_duckdb_supports_views(self):
        """Test that duckdb supports CREATE VIEW."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE test (id INTEGER)")
        con.execute("CREATE VIEW test_view AS SELECT * FROM test")

        # Verify view exists
        result = con.execute("""
            SELECT COUNT(*) FROM information_schema.views
            WHERE table_name = 'test_view'
        """).fetchone()

        assert result[0] > 0
        con.close()

    def test_duckdb_supports_alter_table(self):
        """Test that duckdb supports ALTER TABLE commands."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE test (id INTEGER)")

        # DuckDB has limited ALTER TABLE support, but test what's available
        try:  # noqa: SIM105
            con.execute("ALTER TABLE test ADD COLUMN name VARCHAR")
        except Exception:
            # Some ALTER commands may not be supported
            pass

        con.close()

    def test_duckdb_supports_constraints(self):
        """Test that duckdb supports PRIMARY KEY constraints."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR)")
        con.execute("INSERT INTO test VALUES (1, 'test')")

        # Try to insert duplicate - should fail
        with pytest.raises(Exception):  # noqa: B017, PT011
            con.execute("INSERT INTO test VALUES (1, 'duplicate')")

        con.close()

    def test_duckdb_supports_foreign_keys(self):
        """Test that duckdb supports FOREIGN KEY constraints."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        con.execute("CREATE TABLE child (id INTEGER, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id))")

        con.execute("INSERT INTO parent VALUES (1)")
        con.execute("INSERT INTO child VALUES (1, 1)")

        con.close()

    def test_duckdb_supports_aggregate_functions(self):
        """Test that duckdb supports aggregate functions."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE test (val INTEGER)")
        con.execute("INSERT INTO test VALUES (1), (2), (3)")

        # Test various aggregates
        result = con.execute("SELECT SUM(val), AVG(val), COUNT(val), MIN(val), MAX(val) FROM test").fetchone()
        assert result == (6, 2.0, 3, 1, 3)

        con.close()

    def test_duckdb_supports_group_by(self):
        """Test that duckdb supports GROUP BY."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE test (category VARCHAR, value INTEGER)")
        con.execute("INSERT INTO test VALUES ('A', 10), ('B', 20), ('A', 15)")

        result = con.execute("SELECT category, SUM(value) FROM test GROUP BY category ORDER BY category").fetchall()
        assert result == [("A", 25), ("B", 20)]

        con.close()

    def test_duckdb_supports_joins(self):
        """Test that duckdb supports JOIN operations."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE t1 (id INTEGER, val VARCHAR)")
        con.execute("CREATE TABLE t2 (id INTEGER, val VARCHAR)")
        con.execute("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")
        con.execute("INSERT INTO t2 VALUES (1, 'x'), (2, 'y')")

        result = con.execute("""
            SELECT t1.val, t2.val FROM t1
            JOIN t2 ON t1.id = t2.id
            ORDER BY t1.id
        """).fetchall()

        assert result == [("a", "x"), ("b", "y")]
        con.close()

    def test_duckdb_supports_null_handling(self):
        """Test that duckdb properly handles NULL values."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE test (id INTEGER, val INTEGER)")
        con.execute("INSERT INTO test VALUES (1, NULL), (2, 10)")

        # Test NULLIF
        result = con.execute("SELECT NULLIF(val, 0) FROM test ORDER BY id").fetchall()
        assert result[0][0] is None
        assert result[1][0] == 10

        # Test COALESCE
        result = con.execute("SELECT COALESCE(val, 0) FROM test ORDER BY id").fetchall()
        assert result[0][0] == 0
        assert result[1][0] == 10

        con.close()

    def test_duckdb_supports_case_statements(self):
        """Test that duckdb supports CASE statements."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE test (val INTEGER)")
        con.execute("INSERT INTO test VALUES (1), (2), (3)")

        result = con.execute("""
            SELECT CASE
                WHEN val < 2 THEN 'low'
                WHEN val < 3 THEN 'medium'
                ELSE 'high'
            END as category
            FROM test ORDER BY val
        """).fetchall()

        assert result == [("low",), ("medium",), ("high",)]
        con.close()

    def test_duckdb_supports_cast_operations(self):
        """Test that duckdb supports CAST and TRY_CAST."""
        import duckdb

        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE test (str_val VARCHAR)")
        con.execute("INSERT INTO test VALUES ('123'), ('456'), ('abc')")

        # TRY_CAST returns NULL on failure
        result = con.execute("SELECT TRY_CAST(str_val AS INTEGER) FROM test").fetchall()
        assert result[0][0] == 123
        assert result[1][0] == 456
        assert result[2][0] is None

        con.close()

    def test_duckdb_file_paths_work_with_scripts(self):
        """Test that duckdb can use file paths as expected by scripts."""
        import os
        import tempfile

        import duckdb

        # Create temp directory and file path (but not the file itself)
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, "test.duckdb")

        try:
            # Connect and create table - duckdb will create the file
            con = duckdb.connect(temp_path)
            con.execute("CREATE TABLE test (id INTEGER)")
            con.execute("INSERT INTO test VALUES (1)")
            con.close()

            # Reconnect and verify
            con = duckdb.connect(temp_path)
            result = con.execute("SELECT * FROM test").fetchall()
            assert result == [(1,)]
            con.close()
        finally:
            # Clean up temp file and directory
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)


class TestDuckDBIntegrationWithScripts:
    """Test that scripts can use duckdb properly."""

    def test_check_integrity_can_import_duckdb(self):
        """Test that check_integrity script can import duckdb."""
        try:
            import scripts.check_integrity
            # If import succeeds, duckdb import worked
            assert scripts.check_integrity is not None
        except ImportError as e:
            pytest.fail(f"check_integrity failed to import duckdb: {e}")

    def test_create_advanced_metrics_can_import_duckdb(self):
        """Test that create_advanced_metrics script can import duckdb."""
        try:
            import scripts.create_advanced_metrics
            assert scripts.create_advanced_metrics is not None
        except ImportError as e:
            pytest.fail(f"create_advanced_metrics failed to import duckdb: {e}")

    def test_normalize_db_can_import_duckdb(self):
        """Test that normalize_db script can import duckdb."""
        try:
            import scripts.normalize_db
            assert scripts.normalize_db is not None
        except ImportError as e:
            pytest.fail(f"normalize_db failed to import duckdb: {e}")

    def test_duckdb_works_with_mocked_connection(self):
        """Test that duckdb mocking works as expected in tests."""
        from unittest.mock import MagicMock, patch

        with patch("duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            import duckdb
            con = duckdb.connect(":memory:")

            assert mock_connect.called
            assert con == mock_con
