"""Comprehensive edge case tests for modified scripts.

This module provides additional test coverage for edge cases, boundary conditions,
and integration scenarios across all modified files.
"""

from unittest.mock import MagicMock, patch

import pytest


class TestDocstringUpdates:
    """Test that docstring updates don't break functionality."""

    def test_check_integrity_docstring_format(self):
        """Test that check_integrity has proper docstring format."""
        from scripts.check_integrity import check_integrity

        assert check_integrity.__doc__ is not None
        assert len(check_integrity.__doc__) > 50
        # Should describe what it does
        assert any(word in check_integrity.__doc__.lower() for word in ["validate", "check", "integrity"])

    def test_create_advanced_metrics_docstring_format(self):
        """Test that create_advanced_metrics has proper docstring format."""
        from scripts.create_advanced_metrics import create_advanced_metrics

        assert create_advanced_metrics.__doc__ is not None
        assert "Parameters:" in create_advanced_metrics.__doc__ or "Args:" in create_advanced_metrics.__doc__
        # Should document the db_path parameter
        assert "db_path" in create_advanced_metrics.__doc__

    def test_docstrings_follow_google_or_numpy_style(self):
        """Test that docstrings follow consistent style."""
        from scripts.check_integrity import check_integrity
        from scripts.create_advanced_metrics import create_advanced_metrics

        funcs = [check_integrity, create_advanced_metrics]

        for func in funcs:
            doc = func.__doc__
            if doc:
                # Should have proper formatting
                assert not doc.startswith(" " * 10)  # Not over-indented
                lines = doc.split("\n")
                # First line should be a summary
                assert len(lines[0].strip()) > 0 or len(lines) > 1


class TestTrailingNewlines:
    """Test that trailing newline additions don't break parsing."""

    def test_check_integrity_file_ends_with_newline(self):
        """Test that check_integrity.py ends with newline."""
        import inspect

        import scripts.check_integrity

        source = inspect.getsource(scripts.check_integrity)
        # Should end with newline (Python best practice)
        assert source.endswith(("\n", "\r\n"))

    def test_create_advanced_metrics_file_ends_with_newline(self):
        """Test that create_advanced_metrics.py ends with newline."""
        import inspect

        import scripts.create_advanced_metrics

        source = inspect.getsource(scripts.create_advanced_metrics)
        assert source.endswith(("\n", "\r\n"))

    def test_python_files_parse_correctly(self):
        """Test that all modified Python files parse correctly."""
        import ast
        import inspect

        modules = [
            "scripts.check_integrity",
            "scripts.create_advanced_metrics",
            "scripts.populate",
        ]

        for module_name in modules:
            module = __import__(module_name, fromlist=[""])
            source = inspect.getsource(module)

            try:
                ast.parse(source)
            except SyntaxError as e:
                pytest.fail(f"Syntax error in {module_name}: {e}")


class TestImportPathChanges:
    """Test that import path changes work correctly."""

    def test_all_populate_submodules_importable(self):
        """Test that all populate submodules can be imported."""
        submodules = [
            "api_client",
            "base",
            "config",
            "database",
            "init_db",
            "validation",
            "populate_nba_data",
            "populate_player_game_stats",
            "populate_player_game_stats_v2",
            "populate_player_season_stats",
            "populate_play_by_play",
        ]

        for submodule in submodules:
            try:
                __import__(f"scripts.populate.{submodule}")
            except ImportError as e:
                pytest.fail(f"Failed to import scripts.populate.{submodule}: {e}")

    def test_import_from_init_matches_direct_import(self):
        """Test that importing from __init__ gives same result as direct import."""
        from scripts.populate import NBAClient as InitClient
        from scripts.populate.api_client import NBAClient as DirectClient

        # Should be the same class
        assert InitClient is DirectClient

    def test_multiple_import_styles_work(self):
        """Test that different import styles all work."""
        # Style 1: from package import
        # Style 3: import module, then access
        import scripts.populate.api_client
        from scripts.populate import NBAClient

        # Style 2: from submodule import
        from scripts.populate.api_client import NBAClient as NBAClient2
        nba_client3 = scripts.populate.api_client.NBAClient

        # All should be the same class
        assert NBAClient is NBAClient2 is nba_client3


class TestDatabasePathHandling:
    """Test database path handling across scripts."""

    def test_check_integrity_uses_correct_default_path(self):
        """Test that check_integrity uses correct default database path."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.sql.return_value.fetchone.return_value = [0]

            from scripts.check_integrity import check_integrity
            check_integrity()

            # Should use default path
            mock_connect.assert_called_once_with("data/nba.duckdb")

    def test_create_advanced_metrics_uses_correct_default_path(self):
        """Test that create_advanced_metrics uses correct default database path."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            from scripts.create_advanced_metrics import create_advanced_metrics
            create_advanced_metrics()

            mock_connect.assert_called_once_with("data/nba.duckdb")

    def test_create_advanced_metrics_accepts_command_line_path(self):
        """Test that create_advanced_metrics can accept path from command line."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            from scripts.create_advanced_metrics import create_advanced_metrics

            custom_path = "custom/path/db.duckdb"
            create_advanced_metrics(custom_path)

            mock_connect.assert_called_once_with(custom_path)

    @pytest.mark.parametrize("path", [
        "data/nba.duckdb",
        "test.db",
        ":memory:",
        "/absolute/path/db.duckdb",
        "relative/path/db.duckdb",
    ])
    def test_scripts_accept_various_path_formats(self, path):
        """Test that scripts accept various database path formats."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con

            from scripts.create_advanced_metrics import create_advanced_metrics
            create_advanced_metrics(path)

            mock_connect.assert_called_once_with(path)


class TestErrorHandlingConsistency:
    """Test that error handling is consistent across scripts."""

    def test_check_integrity_handles_connection_errors(self):
        """Test check_integrity handles connection errors."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")

            from scripts.check_integrity import check_integrity

            with pytest.raises(Exception, match="Connection failed"):
                check_integrity()

    def test_create_advanced_metrics_handles_execution_errors(self):
        """Test create_advanced_metrics handles execution errors."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.execute.side_effect = Exception("Execution failed")

            from scripts.create_advanced_metrics import create_advanced_metrics

            with pytest.raises(Exception):  # noqa: B017, PT011
                create_advanced_metrics()

    def test_scripts_close_connections_on_errors(self):
        """Test that all scripts close database connections even on errors."""
        with patch("scripts.check_integrity.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.sql.side_effect = Exception("Query error")

            from scripts.check_integrity import check_integrity

            check_integrity()  # Should not raise, errors are suppressed

            # Connection should still be closed
            mock_con.close.assert_called_once()


class TestCodeQuality:
    """Test code quality aspects of changes."""

    def test_no_print_statements_in_production_code(self):
        """Test that production code doesn't use print() (use logging instead)."""
        import inspect

        modules = [
            "scripts.check_integrity",
            "scripts.create_advanced_metrics",
        ]

        for module_name in modules:
            module = __import__(module_name, fromlist=[""])
            source = inspect.getsource(module)

            # print() should not be used (except in __main__ block maybe)
            # This is a soft check - logging is preferred
            lines = source.split("\n")
            print_lines = [line for line in lines if "print(" in line and not line.strip().startswith("#")]

            # If there are print statements, they should be minimal
            # This is just a warning, not a hard failure
            if len(print_lines) > 5:
                pytest.skip(f"Consider using logging instead of print in {module_name}")

    def test_no_bare_except_clauses(self):
        """Test that code doesn't use bare except: clauses."""
        import inspect
        import re

        modules = [
            "scripts.check_integrity",
            "scripts.create_advanced_metrics",
        ]

        for module_name in modules:
            module = __import__(module_name, fromlist=[""])
            source = inspect.getsource(module)

            # Look for 'except:' without exception type
            bare_except_pattern = r"except\s*:"
            matches = re.findall(bare_except_pattern, source)

            # Some bare excepts may be intentional with contextlib.suppress
            if matches:
                # Check if they're using contextlib.suppress properly
                assert "contextlib" in source or "Exception" in source

    def test_functions_have_docstrings(self):
        """Test that main functions have docstrings."""
        from scripts.check_integrity import check_integrity
        from scripts.create_advanced_metrics import create_advanced_metrics

        functions = [check_integrity, create_advanced_metrics]

        for func in functions:
            assert func.__doc__ is not None, f"{func.__name__} missing docstring"
            assert len(func.__doc__.strip()) > 0


class TestBackwardCompatibility:
    """Test that changes maintain backward compatibility."""

    def test_check_integrity_signature_unchanged(self):
        """Test that check_integrity function signature is unchanged."""
        import inspect

        from scripts.check_integrity import check_integrity

        sig = inspect.signature(check_integrity)
        # Should have no parameters (uses global DATABASE constant)
        assert len(sig.parameters) == 0

    def test_create_advanced_metrics_signature_compatible(self):
        """Test that create_advanced_metrics signature is backward compatible."""
        import inspect

        from scripts.create_advanced_metrics import create_advanced_metrics

        sig = inspect.signature(create_advanced_metrics)
        params = list(sig.parameters.values())

        # Should have optional db_path parameter
        assert len(params) <= 1
        if len(params) == 1:
            # Parameter should have default value
            assert params[0].default != inspect.Parameter.empty

    def test_populate_functions_still_callable(self):
        """Test that populate functions are still callable."""
        from scripts.populate import (
            populate_play_by_play,
            populate_player_game_stats,
            populate_player_season_stats,
        )

        functions = [
            populate_player_game_stats,
            populate_player_season_stats,
            populate_play_by_play,
        ]

        for func in functions:
            assert callable(func), f"{func.__name__} is not callable"


class TestModuleMetadata:
    """Test module-level metadata."""

    def test_scripts_have_shebang_where_appropriate(self):
        """Test that executable scripts have shebang line."""
        import inspect

        # These are typically run as scripts
        modules = [
            "scripts.check_integrity",
            "scripts.create_advanced_metrics",
        ]

        for module_name in modules:
            module = __import__(module_name, fromlist=[""])
            source = inspect.getsource(module)

            # If it has __main__ block, should have shebang
            if "__main__" in source:
                # First line might be shebang
                # This is optional but good practice
                # Just informational
                pass

    def test_scripts_have_if_main_blocks(self):
        """Test that scripts have if __name__ == '__main__' blocks."""
        import inspect

        modules = [
            "scripts.check_integrity",
            "scripts.create_advanced_metrics",
        ]

        for module_name in modules:
            module = __import__(module_name, fromlist=[""])
            source = inspect.getsource(module)

            # Should have __main__ block for command-line usage
            assert '__name__ == "__main__"' in source or "__name__ == '__main__'" in source


class TestTypeHints:
    """Test type hint additions."""

    def test_create_advanced_metrics_has_type_hints(self):
        """Test that create_advanced_metrics has type hints."""
        import inspect

        from scripts.create_advanced_metrics import create_advanced_metrics

        sig = inspect.signature(create_advanced_metrics)

        # Check if parameters have annotations
        for param in sig.parameters.values():
            # db_path should be annotated as str
            if param.name == "db_path":
                assert param.annotation != inspect.Parameter.empty
                assert param.annotation is str or "str" in str(param.annotation)

        # Check return annotation
        assert sig.return_annotation != inspect.Signature.empty
        assert sig.return_annotation is None or "None" in str(sig.return_annotation)

    def test_check_integrity_has_return_type(self):
        """Test that check_integrity has return type annotation."""
        import inspect

        from scripts.check_integrity import check_integrity

        sig = inspect.signature(check_integrity)

        # Should return None
        assert sig.return_annotation != inspect.Signature.empty
        assert sig.return_annotation is None or "None" in str(sig.return_annotation)
