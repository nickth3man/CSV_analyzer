"""Unit tests for src.scripts.populate.config module.

Tests pydantic-settings configuration, environment variable parsing,
and the NBAAPIConfig/DatabaseConfig classes.
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest


class TestNBAAPIConfig:
    """Tests for NBAAPIConfig pydantic-settings class."""

    def test_default_values(self, monkeypatch):
        """Test default configuration values without environment variables."""
        from src.scripts.populate.config import NBAAPIConfig

        # Clear all NBA_API_ env vars
        for key in list(os.environ.keys()):
            if key.startswith("NBA_API_"):
                monkeypatch.delenv(key, raising=False)

        # Also clear potential .env file influence by setting env vars explicitly to None
        monkeypatch.setenv("NBA_API_TIMEOUT", "30.0")
        monkeypatch.setenv("NBA_API_REQUEST_DELAY", "0.6")
        monkeypatch.setenv("NBA_API_MAX_RETRIES", "3")

        config = NBAAPIConfig()

        assert config.timeout == 30.0
        assert config.request_delay == 0.6
        assert config.max_retries == 3
        assert config.retry_backoff_factor == 2.0

    def test_timeout_parsing_single_value(self):
        """Test timeout parsing with single numeric value."""
        from src.scripts.populate.config import NBAAPIConfig

        with patch.dict(os.environ, {"NBA_API_TIMEOUT": "60"}, clear=False):
            config = NBAAPIConfig()
            assert config.timeout == 60.0

    def test_timeout_parsing_connect_read_format(self):
        """Test timeout parsing with 'connect,read' format uses first value."""
        from src.scripts.populate.config import NBAAPIConfig

        with patch.dict(os.environ, {"NBA_API_TIMEOUT": "10,60"}, clear=False):
            config = NBAAPIConfig()
            assert config.timeout == 10.0

    def test_timeout_parsing_with_spaces(self):
        """Test timeout parsing handles whitespace in comma-separated format."""
        from src.scripts.populate.config import NBAAPIConfig

        with patch.dict(os.environ, {"NBA_API_TIMEOUT": "15, 90"}, clear=False):
            config = NBAAPIConfig()
            assert config.timeout == 15.0

    def test_env_override_request_delay(self):
        """Test NBA_API_REQUEST_DELAY environment variable override."""
        from src.scripts.populate.config import NBAAPIConfig

        with patch.dict(os.environ, {"NBA_API_REQUEST_DELAY": "1.5"}, clear=False):
            config = NBAAPIConfig()
            assert config.request_delay == 1.5

    def test_env_override_max_retries(self):
        """Test NBA_API_MAX_RETRIES environment variable override."""
        from src.scripts.populate.config import NBAAPIConfig

        with patch.dict(os.environ, {"NBA_API_MAX_RETRIES": "5"}, clear=False):
            config = NBAAPIConfig()
            assert config.max_retries == 5

    def test_env_override_proxy(self):
        """Test NBA_API_PROXY environment variable override."""
        from src.scripts.populate.config import NBAAPIConfig

        proxy_url = "http://proxy.example.com:8080"
        with patch.dict(os.environ, {"NBA_API_PROXY": proxy_url}, clear=False):
            config = NBAAPIConfig()
            assert config.proxy == proxy_url

    def test_env_override_user_agent(self):
        """Test NBA_API_USER_AGENT environment variable override."""
        from src.scripts.populate.config import NBAAPIConfig

        custom_ua = "CustomBot/1.0"
        with patch.dict(os.environ, {"NBA_API_USER_AGENT": custom_ua}, clear=False):
            config = NBAAPIConfig()
            assert config.user_agent == custom_ua

    def test_headers_property_default(self):
        """Test headers property returns expected default headers."""
        from src.scripts.populate.config import NBAAPIConfig

        config = NBAAPIConfig()
        headers = config.headers

        assert headers["Host"] == "stats.nba.com"
        assert "User-Agent" in headers
        assert headers["Referer"] == "https://stats.nba.com/"
        assert headers["x-nba-stats-origin"] == "stats"
        assert "Sec-Ch-Ua" in headers

    def test_headers_property_user_agent_override(self):
        """Test headers property applies user_agent override."""
        from src.scripts.populate.config import NBAAPIConfig

        custom_ua = "TestAgent/2.0"
        with patch.dict(os.environ, {"NBA_API_USER_AGENT": custom_ua}, clear=False):
            config = NBAAPIConfig()
            headers = config.headers
            assert headers["User-Agent"] == custom_ua

    def test_headers_property_json_merge(self):
        """Test headers property merges headers_json correctly."""
        from src.scripts.populate.config import NBAAPIConfig

        extra_headers = '{"X-Custom-Header": "test-value", "X-Another": "value2"}'
        with patch.dict(
            os.environ, {"NBA_API_HEADERS_JSON": extra_headers}, clear=False
        ):
            config = NBAAPIConfig()
            headers = config.headers
            assert headers["X-Custom-Header"] == "test-value"
            assert headers["X-Another"] == "value2"
            # Original headers should still be present
            assert headers["Host"] == "stats.nba.com"

    def test_headers_property_invalid_json(self):
        """Test headers property handles invalid JSON gracefully."""
        from src.scripts.populate.config import NBAAPIConfig

        with patch.dict(
            os.environ, {"NBA_API_HEADERS_JSON": "not-valid-json"}, clear=False
        ):
            config = NBAAPIConfig()
            # Should not raise, just log warning
            headers = config.headers
            assert "Host" in headers  # Default headers still work

    def test_max_retries_validation_bounds(self):
        """Test max_retries respects ge=1, le=10 bounds."""
        from pydantic import ValidationError

        from src.scripts.populate.config import NBAAPIConfig

        # Valid bounds
        with patch.dict(os.environ, {"NBA_API_MAX_RETRIES": "1"}, clear=False):
            config = NBAAPIConfig()
            assert config.max_retries == 1

        with patch.dict(os.environ, {"NBA_API_MAX_RETRIES": "10"}, clear=False):
            config = NBAAPIConfig()
            assert config.max_retries == 10

        # Invalid: below minimum
        with (
            patch.dict(os.environ, {"NBA_API_MAX_RETRIES": "0"}, clear=False),
            pytest.raises(ValidationError),
        ):
            NBAAPIConfig()

        # Invalid: above maximum
        with (
            patch.dict(os.environ, {"NBA_API_MAX_RETRIES": "11"}, clear=False),
            pytest.raises(ValidationError),
        ):
            NBAAPIConfig()

    def test_request_delay_non_negative(self):
        """Test request_delay must be non-negative (ge=0.0)."""
        from pydantic import ValidationError

        from src.scripts.populate.config import NBAAPIConfig

        with (
            patch.dict(os.environ, {"NBA_API_REQUEST_DELAY": "-1"}, clear=False),
            pytest.raises(ValidationError),
        ):
            NBAAPIConfig()


class TestDatabaseConfig:
    """Tests for DatabaseConfig pydantic-settings class."""

    def test_default_db_path(self):
        """Test default database path is set correctly."""
        from src.scripts.populate.config import DatabaseConfig

        config = DatabaseConfig()
        assert config.db_path.name == "nba.duckdb"
        assert "backend" in str(config.db_path) or "data" in str(config.db_path)

    def test_env_override_db_path(self, tmp_path):
        """Test NBA_DB_PATH environment variable override."""
        from src.scripts.populate.config import DatabaseConfig

        custom_path = str(tmp_path / "test_nba.duckdb")
        with patch.dict(os.environ, {"NBA_DB_PATH": custom_path}, clear=False):
            config = DatabaseConfig()
            assert config.db_path == Path(custom_path)

    def test_db_path_string_conversion(self):
        """Test db_path validator converts strings to Path objects."""
        from src.scripts.populate.config import DatabaseConfig

        test_path = "C:/Users/test/nba.duckdb"
        with patch.dict(os.environ, {"NBA_DB_PATH": test_path}, clear=False):
            config = DatabaseConfig()
            assert isinstance(config.db_path, Path)


class TestConfigUtilityFunctions:
    """Tests for module-level utility functions."""

    def test_get_api_config(self):
        """Test get_api_config returns NBAAPIConfig instance."""
        from src.scripts.populate.config import NBAAPIConfig, get_api_config

        config = get_api_config()
        assert isinstance(config, NBAAPIConfig)

    def test_get_db_path(self):
        """Test get_db_path returns Path instance."""
        from src.scripts.populate.config import get_db_path

        db_path = get_db_path()
        assert isinstance(db_path, Path)
        assert db_path.suffix == ".duckdb"

    def test_ensure_cache_dir(self, tmp_path):
        """Test ensure_cache_dir creates directory if needed."""
        from src.scripts.populate.config import CACHE_DIR, ensure_cache_dir

        # ensure_cache_dir should not raise
        ensure_cache_dir()
        assert CACHE_DIR.exists()


class TestSeasonConfiguration:
    """Tests for season-related configuration constants."""

    def test_all_seasons_format(self):
        """Test ALL_SEASONS contains valid season strings."""
        from src.scripts.populate.config import ALL_SEASONS

        assert len(ALL_SEASONS) > 0
        for season in ALL_SEASONS:
            # Format should be YYYY-YY
            assert len(season) == 7
            assert season[4] == "-"
            start_year = int(season[:4])
            end_year = int(season[5:])
            # End year should be start year + 1 (mod 100)
            assert end_year == (start_year + 1) % 100

    def test_current_season(self):
        """Test CURRENT_SEASON is in ALL_SEASONS."""
        from src.scripts.populate.config import ALL_SEASONS, CURRENT_SEASON

        assert CURRENT_SEASON in ALL_SEASONS
        assert ALL_SEASONS[0] == CURRENT_SEASON  # Should be first (newest)

    def test_default_seasons_subset(self):
        """Test DEFAULT_SEASONS is subset of ALL_SEASONS."""
        from src.scripts.populate.config import ALL_SEASONS, DEFAULT_SEASONS

        assert len(DEFAULT_SEASONS) <= len(ALL_SEASONS)
        for season in DEFAULT_SEASONS:
            assert season in ALL_SEASONS

    def test_season_types(self):
        """Test SEASON_TYPES contains expected values."""
        from src.scripts.populate.config import SEASON_TYPES

        assert "regular" in SEASON_TYPES
        assert "playoffs" in SEASON_TYPES
        assert SEASON_TYPES["regular"] == "Regular Season"
        assert SEASON_TYPES["playoffs"] == "Playoffs"
