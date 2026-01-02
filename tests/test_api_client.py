"""Unit tests for src.scripts.populate.api_client module.

Tests tenacity retry decorator, rate limiting, and NBAClient methods.
"""

import pytest
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout


class TestRetryDecorator:
    """Tests for the tenacity-based retry decorator."""

    def test_create_retry_decorator_default_params(self):
        """Test create_retry_decorator with default parameters."""
        from src.scripts.populate.api_client import create_retry_decorator

        decorator = create_retry_decorator()
        assert decorator is not None

    def test_create_retry_decorator_custom_params(self):
        """Test create_retry_decorator with custom parameters."""
        from src.scripts.populate.api_client import create_retry_decorator

        decorator = create_retry_decorator(
            max_retries=5,
            backoff_multiplier=2.0,
            min_wait=1.0,
            max_wait=60.0,
        )
        assert decorator is not None

    def test_retry_on_timeout(self):
        """Test that Timeout exceptions trigger retry."""
        from src.scripts.populate.api_client import create_retry_decorator

        call_count = 0

        @create_retry_decorator(max_retries=3, min_wait=0.1, max_wait=0.2)
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Timeout("Connection timed out")
            return "success"

        result = flaky_function()
        assert result == "success"
        assert call_count == 3  # Called 3 times (2 failures + 1 success)

    def test_retry_on_connection_error(self):
        """Test that ConnectionError exceptions trigger retry."""
        from src.scripts.populate.api_client import create_retry_decorator

        call_count = 0

        @create_retry_decorator(max_retries=3, min_wait=0.1, max_wait=0.2)
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RequestsConnectionError("Connection failed")
            return "success"

        result = flaky_function()
        assert result == "success"
        assert call_count == 2

    def test_retry_on_http_error(self):
        """Test that HTTPError exceptions trigger retry."""
        from src.scripts.populate.api_client import create_retry_decorator

        call_count = 0

        @create_retry_decorator(max_retries=3, min_wait=0.1, max_wait=0.2)
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise HTTPError("500 Server Error")
            return "success"

        result = flaky_function()
        assert result == "success"
        assert call_count == 2

    def test_retry_exhausted_raises(self):
        """Test that the original exception is raised after max retries exhausted."""
        from src.scripts.populate.api_client import create_retry_decorator

        @create_retry_decorator(max_retries=2, min_wait=0.1, max_wait=0.2)
        def always_fails():
            raise Timeout("Always times out")

        # With reraise=True (default), tenacity re-raises the original exception
        with pytest.raises(Timeout, match="Always times out"):
            always_fails()

    def test_non_retryable_exception_not_retried(self):
        """Test that non-retryable exceptions are raised immediately."""
        from src.scripts.populate.api_client import create_retry_decorator

        call_count = 0

        @create_retry_decorator(max_retries=3, min_wait=0.1, max_wait=0.2)
        def raises_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("Not a network error")

        with pytest.raises(ValueError, match="Not a network error"):
            raises_value_error()

        assert call_count == 1  # Only called once, not retried


class TestWithRetryDecorator:
    """Tests for the pre-configured with_retry decorator."""

    def test_with_retry_exists(self):
        """Test that with_retry decorator is exported."""
        from src.scripts.populate.api_client import with_retry

        assert with_retry is not None

    def test_with_retry_applies_to_function(self):
        """Test that with_retry can be applied to a function."""
        from src.scripts.populate.api_client import with_retry

        @with_retry
        def sample_function():
            return "result"

        result = sample_function()
        assert result == "result"


class TestNBAClient:
    """Tests for the NBAClient class."""

    def test_client_initialization(self):
        """Test NBAClient initializes with config."""
        from src.scripts.populate.api_client import NBAClient

        client = NBAClient()
        assert client.config is not None

    def test_client_initialization_with_custom_config(self):
        """Test NBAClient accepts custom config."""
        from src.scripts.populate.api_client import NBAClient
        from src.scripts.populate.config import NBAAPIConfig

        custom_config = NBAAPIConfig(timeout=60.0, request_delay=1.0)
        client = NBAClient(config=custom_config)
        assert client.config.timeout == 60.0
        assert client.config.request_delay == 1.0

    def test_get_all_players_returns_list(self):
        """Test get_all_players returns a list."""
        from src.scripts.populate.api_client import NBAClient

        client = NBAClient()
        players = client.get_all_players()
        assert isinstance(players, list)
        # nba_api static data should have players
        assert len(players) > 0

    def test_get_active_players_returns_list(self):
        """Test get_active_players returns list of active players."""
        from src.scripts.populate.api_client import NBAClient

        client = NBAClient()
        active_players = client.get_active_players()
        assert isinstance(active_players, list)
        # All returned players should be active
        for player in active_players:
            assert player.get("is_active", True)  # Default to True if not present

    def test_get_all_teams_returns_list(self):
        """Test get_all_teams returns list of NBA teams."""
        from src.scripts.populate.api_client import NBAClient

        client = NBAClient()
        teams = client.get_all_teams()
        assert isinstance(teams, list)
        assert len(teams) == 30  # 30 NBA teams

    def test_find_player_by_id(self):
        """Test find_player_by_id locates player by ID."""
        from src.scripts.populate.api_client import NBAClient

        client = NBAClient()
        # Use LeBron James' player ID
        player = client.find_player_by_id(2544)
        assert player is not None
        assert player.get("id") == 2544 or player.get("player_id") == 2544

    def test_find_player_by_id_not_found_returns_none(self):
        """Test find_player_by_id returns None for non-existent player."""
        from src.scripts.populate.api_client import NBAClient

        client = NBAClient()
        player = client.find_player_by_id(999999999)
        assert player is None

    def test_find_players_by_name(self):
        """Test find_players_by_name locates players by name."""
        from src.scripts.populate.api_client import NBAClient

        client = NBAClient()
        # Use a well-known player name
        players = client.find_players_by_name("LeBron")
        assert isinstance(players, list)
        assert len(players) > 0

    def test_find_team_by_abbreviation(self):
        """Test find_team_by_abbreviation locates team."""
        from src.scripts.populate.api_client import NBAClient

        client = NBAClient()
        team = client.find_team_by_abbreviation("LAL")
        assert team is not None
        assert team.get("abbreviation") == "LAL"

    def test_find_team_by_abbreviation_not_found_returns_none(self):
        """Test find_team_by_abbreviation returns None for non-existent team."""
        from src.scripts.populate.api_client import NBAClient

        client = NBAClient()
        team = client.find_team_by_abbreviation("XYZ")
        assert team is None


class TestGetClient:
    """Tests for the get_client singleton function."""

    def test_get_client_returns_client(self):
        """Test get_client returns NBAClient instance."""
        from src.scripts.populate.api_client import NBAClient, get_client

        client = get_client()
        assert isinstance(client, NBAClient)

    def test_get_client_singleton(self):
        """Test get_client returns same instance on multiple calls."""
        from src.scripts.populate.api_client import get_client

        client1 = get_client()
        client2 = get_client()
        assert client1 is client2


class TestRateLimiting:
    """Tests for rate limiting functionality."""

    def test_config_has_request_delay(self):
        """Test that config includes request_delay setting."""
        from src.scripts.populate.config import NBAAPIConfig

        config = NBAAPIConfig(request_delay=0.5)
        assert config.request_delay == 0.5

    def test_default_request_delay(self):
        """Test default request_delay value."""
        from src.scripts.populate.config import NBAAPIConfig

        config = NBAAPIConfig()
        assert config.request_delay == 0.6  # Default is 0.6 seconds


class TestRetryableExceptions:
    """Tests verifying RETRYABLE_EXCEPTIONS configuration."""

    def test_retryable_exceptions_defined(self):
        """Test RETRYABLE_EXCEPTIONS tuple is defined."""
        from src.scripts.populate.api_client import RETRYABLE_EXCEPTIONS

        assert isinstance(RETRYABLE_EXCEPTIONS, tuple)
        assert Timeout in RETRYABLE_EXCEPTIONS
        assert RequestsConnectionError in RETRYABLE_EXCEPTIONS
        assert HTTPError in RETRYABLE_EXCEPTIONS

    def test_oserror_in_retryable(self):
        """Test OSError is included in retryable exceptions."""
        from src.scripts.populate.api_client import RETRYABLE_EXCEPTIONS

        assert OSError in RETRYABLE_EXCEPTIONS
