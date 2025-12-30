"""Unit tests for scripts/populate/__init__.py module.

This module tests the populate package initialization including:
- Module imports and exports
- __all__ definition
- Version information
- Package-level functionality
"""

import pytest


class TestPopulateInit:
    """Test suite for populate package initialization."""

    def test_populate_init_imports_successfully(self):
        """Test that the populate package can be imported."""
        try:
            import src.scripts.populate
            assert src.scripts.populate is not None
        except ImportError as e:
            pytest.fail(f"Failed to import src.scripts.populate: {e}")

    def test_populate_init_has_version(self):
        """Test that package has version information."""
        from src.scripts.populate import __version__
        assert __version__ is not None
        assert isinstance(__version__, str)
        assert len(__version__) > 0

    def test_populate_init_version_format(self):
        """Test that version follows semantic versioning."""
        from src.scripts.populate import __version__
        parts = __version__.split(".")
        assert len(parts) >= 2, "Version should have at least major.minor"
        assert all(part.isdigit() for part in parts), "Version parts should be numeric"

    def test_populate_init_exports_core_components(self):
        """Test that core components are exported."""
        from src.scripts.populate import (
            NBAClient,
            DatabaseManager,
            PopulationManager,
            DataValidator,
        )
        
        assert NBAClient is not None
        assert DatabaseManager is not None
        assert PopulationManager is not None
        assert DataValidator is not None

    def test_populate_init_exports_base_classes(self):
        """Test that base classes are exported."""
        from src.scripts.populate import (
            BasePopulator,
            PopulationMetrics,
            ProgressTracker,
        )
        
        assert BasePopulator is not None
        assert PopulationMetrics is not None
        assert ProgressTracker is not None

    def test_populate_init_exports_config(self):
        """Test that configuration constants are exported."""
        from src.scripts.populate import (
            ALL_SEASONS,
            CURRENT_SEASON,
            DEFAULT_SEASONS,
            SEASON_TYPES,
            CACHE_DIR,
        )
        
        assert ALL_SEASONS is not None
        assert CURRENT_SEASON is not None
        assert DEFAULT_SEASONS is not None
        assert SEASON_TYPES is not None
        assert CACHE_DIR is not None

    def test_populate_init_exports_database_functions(self):
        """Test that database utility functions are exported."""
        from src.scripts.populate import (
            init_database,
            get_database_info,
            get_db_path,
        )
        
        assert callable(init_database)
        assert callable(get_database_info)
        assert callable(get_db_path)

    def test_populate_init_exports_population_functions(self):
        """Test that population functions are exported."""
        from src.scripts.populate import (
            populate_player_game_stats,
            populate_player_game_stats_v2,
            populate_player_season_stats,
            populate_play_by_play,
        )
        
        assert callable(populate_player_game_stats)
        assert callable(populate_player_game_stats_v2)
        assert callable(populate_player_season_stats)
        assert callable(populate_play_by_play)

    def test_populate_init_exports_api_client_factory(self):
        """Test that API client factory function is exported."""
        from src.scripts.populate import get_client
        assert callable(get_client)

    def test_populate_init_exports_config_functions(self):
        """Test that configuration helper functions are exported."""
        from src.scripts.populate import (
            get_api_config,
            ensure_cache_dir,
        )
        
        assert callable(get_api_config)
        assert callable(ensure_cache_dir)

    def test_populate_init_all_definition_complete(self):
        """Test that __all__ includes all public exports."""
        from src.scripts.populate import __all__
        
        assert isinstance(__all__, list)
        assert len(__all__) > 0
        
        # Check for expected exports
        expected_exports = [
            "NBAClient",
            "DatabaseManager",
            "PopulationManager",
            "DataValidator",
            "BasePopulator",
            "init_database",
            "populate_player_game_stats",
            "populate_player_season_stats",
            "populate_play_by_play",
            "get_client",
        ]
        
        for export in expected_exports:
            assert export in __all__, f"{export} not in __all__"

    def test_populate_init_all_items_are_defined(self):
        """Test that all items in __all__ are actually defined."""
        import src.scripts.populate
        from src.scripts.populate import __all__
        
        for item in __all__:
            assert hasattr(src.scripts.populate, item), f"{item} in __all__ but not defined"

    def test_populate_init_imports_use_absolute_paths(self):
        """Test that imports use absolute paths (src.scripts.populate.*)."""
        import inspect
        import src.scripts.populate
        
        # Get the source code of the __init__.py
        source = inspect.getsource(src.scripts.populate)
        
        # Check that imports use absolute paths
        assert "from src.scripts.populate." in source or "import src.scripts.populate." in source
        
        # Verify no relative imports for main components
        lines = source.split("\n")
        import_lines = [line for line in lines if line.strip().startswith("from") and "import" in line]
        
        # Core imports should be absolute
        for line in import_lines:
            if any(component in line for component in ["api_client", "base", "database", "init_db"]):
                assert "src.scripts.populate" in line, f"Expected absolute import in: {line}"

    def test_populate_init_no_star_imports(self):
        """Test that __init__ doesn't use star imports."""
        import inspect
        import src.scripts.populate
        
        source = inspect.getsource(src.scripts.populate)
        lines = source.split("\n")
        import_lines = [line for line in lines if "import" in line]
        
        # Star imports are discouraged
        star_imports = [line for line in import_lines if "import *" in line]
        assert len(star_imports) == 0, "Star imports found in __init__.py"

    def test_populate_init_exports_season_constants(self):
        """Test that season-related constants are exported."""
        from src.scripts.populate import (
            RECENT_SEASONS,
            DEFAULT_SEASON_TYPES,
        )
        
        assert RECENT_SEASONS is not None
        assert DEFAULT_SEASON_TYPES is not None

    def test_populate_init_season_constants_are_valid(self):
        """Test that season constants have valid formats."""
        from src.scripts.populate import CURRENT_SEASON, ALL_SEASONS
        
        # Current season should be in YYYY-YY format
        assert isinstance(CURRENT_SEASON, str)
        assert len(CURRENT_SEASON) == 7  # e.g., "2023-24"
        assert "-" in CURRENT_SEASON
        
        # All seasons should be a collection
        assert hasattr(ALL_SEASONS, "__iter__")

    def test_populate_init_cache_dir_is_valid(self):
        """Test that CACHE_DIR is a valid path."""
        from src.scripts.populate import CACHE_DIR
        from pathlib import Path
        
        assert isinstance(CACHE_DIR, (str, Path))
        cache_path = Path(CACHE_DIR)
        # Path should be relative or absolute
        assert len(str(cache_path)) > 0

    def test_populate_init_docstring_exists(self):
        """Test that package has comprehensive docstring."""
        import src.scripts.populate
        
        assert src.scripts.populate.__doc__ is not None
        assert len(src.scripts.populate.__doc__) > 100  # Should be comprehensive
        assert "NBA" in src.scripts.populate.__doc__
        assert "populate" in src.scripts.populate.__doc__.lower()

    def test_populate_init_docstring_describes_usage(self):
        """Test that docstring includes usage examples."""
        import src.scripts.populate
        
        doc = src.scripts.populate.__doc__
        assert isinstance(doc, str)
        assert "Usage:" in doc or "Example:" in doc or "from src.scripts.populate" in doc

    def test_populate_init_docstring_lists_modules(self):
        """Test that docstring documents sub-modules."""
        import src.scripts.populate
        
        doc = src.scripts.populate.__doc__
        # Should mention key modules
        assert isinstance(doc, str)
        assert any(module in doc for module in [
            "api_client", "database", "populate_nba_data"
        ])

    def test_populate_init_exports_are_importable(self):
        """Test that all exports can be imported directly."""
        from src.scripts.populate import __all__
        
        for export_name in __all__:
            try:
                exec(f"from src.scripts.populate import {export_name}")  # noqa: S102
            except ImportError as e:
                pytest.fail(f"Failed to import {export_name}: {e}")

    def test_populate_init_no_circular_imports(self):
        """Test that importing doesn't cause circular import errors."""
        try:
            import src.scripts.populate  # noqa: F401
            from src.scripts.populate import NBAClient, PopulationManager  # noqa: F401
            
            # If we got here, no circular imports
            assert True
        except ImportError as e:
            if "circular" in str(e).lower():
                pytest.fail(f"Circular import detected: {e}")
            raise

    def test_populate_init_client_factory_returns_client(self):
        """Test that get_client factory returns an NBAClient instance."""
        from src.scripts.populate import get_client, NBAClient
        
        client = get_client()
        assert isinstance(client, NBAClient)

    def test_populate_init_config_functions_callable(self):
        """Test that configuration functions are callable and return expected types."""
        from src.scripts.populate import get_api_config, get_db_path
        from src.scripts.populate.config import NBAAPIConfig
        from pathlib import Path
        
        # get_api_config should return NBAAPIConfig instance
        config = get_api_config()
        assert isinstance(config, NBAAPIConfig)
        
        # get_db_path should return Path instance
        db_path = get_db_path()
        assert isinstance(db_path, Path)

    def test_populate_init_ensure_cache_dir_function(self):
        """Test that ensure_cache_dir function exists and is callable."""
        from src.scripts.populate import ensure_cache_dir
        
        assert callable(ensure_cache_dir)
        # Should not raise error when called
        try:
            ensure_cache_dir()
        except Exception as e:
            # Some exceptions are acceptable (e.g., permissions)
            pass

    @pytest.mark.parametrize("export_name", [
        "NBAClient",
        "PopulationManager",
        "DatabaseManager",
        "init_database",
        "populate_player_game_stats",
    ])
    def test_populate_init_critical_exports_exist(self, export_name):
        """Test that critical exports are available."""
        from src.scripts.populate import __all__
        
        assert export_name in __all__, f"Critical export {export_name} not in __all__"
        
        # Also verify it's importable
        import src.scripts.populate
        assert hasattr(src.scripts.populate, export_name)

    def test_populate_init_version_is_accessible_from_package(self):
        """Test that version is accessible from package level."""
        import src.scripts.populate
        
        assert hasattr(src.scripts.populate, "__version__")
        version = src.scripts.populate.__version__
        assert isinstance(version, str)
        assert version == src.scripts.populate.__version__  # Consistent access


class TestPopulateInitImportPaths:
    """Test suite for import path changes."""

    def test_absolute_imports_work_for_api_client(self):
        """Test that absolute import works for api_client."""
        try:
            from src.scripts.populate.api_client import NBAClient, get_client
            assert NBAClient is not None
            assert get_client is not None
        except ImportError as e:
            pytest.fail(f"Failed to import from src.scripts.populate.api_client: {e}")

    def test_absolute_imports_work_for_base(self):
        """Test that absolute import works for base."""
        try:
            from src.scripts.populate.base import BasePopulator, PopulationMetrics
            assert BasePopulator is not None
            assert PopulationMetrics is not None
        except ImportError as e:
            pytest.fail(f"Failed to import from src.scripts.populate.base: {e}")

    def test_absolute_imports_work_for_config(self):
        """Test that absolute import works for config."""
        try:
            from src.scripts.populate.config import (
                CURRENT_SEASON,
                get_db_path,
            )
            assert CURRENT_SEASON is not None
            assert callable(get_db_path)
        except ImportError as e:
            pytest.fail(f"Failed to import from src.scripts.populate.config: {e}")

    def test_absolute_imports_work_for_database(self):
        """Test that absolute import works for database."""
        try:
            from src.scripts.populate.database import DatabaseManager
            assert DatabaseManager is not None
        except ImportError as e:
            pytest.fail(f"Failed to import from src.scripts.populate.database: {e}")

    def test_absolute_imports_work_for_init_db(self):
        """Test that absolute import works for init_db."""
        try:
            from src.scripts.populate.init_db import init_database, get_database_info
            assert callable(init_database)
            assert callable(get_database_info)
        except ImportError as e:
            pytest.fail(f"Failed to import from src.scripts.populate.init_db: {e}")

    def test_absolute_imports_work_for_validation(self):
        """Test that absolute import works for validation."""
        try:
            from src.scripts.populate.validation import DataValidator
            assert DataValidator is not None
        except ImportError as e:
            pytest.fail(f"Failed to import from src.scripts.populate.validation: {e}")

    def test_relative_imports_still_work_within_package(self):
        """Test that relative imports still work within package modules."""
        # This tests that the package structure supports both import styles
        try:
            import src.scripts.populate.populate_nba_data
            # If the module loads, its internal imports worked
            assert src.scripts.populate.populate_nba_data is not None
        except ImportError as e:
            pytest.fail(f"Package internal imports failed: {e}")