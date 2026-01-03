"""Tests for the populate module exception hierarchy and resilience patterns.

Tests cover:
- Exception classification and retry logic
- Circuit breaker state transitions
- Adaptive rate limiter behavior
- Pydantic schema validation
"""

import time

import pandas as pd
import pytest

from src.scripts.populate.exceptions import (
    APITimeoutError,
    CircuitBreakerError,
    DataNotFoundError,
    PermanentError,
    RateLimitError,
    SchemaError,
    ServiceUnavailableError,
    TransientError,
    ValidationError,
    classify_exception,
    get_retry_delay,
    is_retriable,
)
from src.scripts.populate.resilience import (
    AdaptiveRateLimiter,
    CircuitBreaker,
    CircuitBreakerRegistry,
    CircuitState,
)
from src.scripts.populate.schemas import (
    CommonPlayerInfo,
    DraftHistory,
    Player,
    PlayerGameStats,
    TeamGameStats,
    validate_dataframe,
)


class TestExceptionHierarchy:
    """Tests for the exception hierarchy."""

    def test_transient_error_is_retriable(self):
        """Transient errors should be retriable by default."""
        error = TransientError("Test transient error")
        assert error.retriable is True

    def test_permanent_error_not_retriable(self):
        """Permanent errors should not be retriable."""
        error = PermanentError("Test permanent error")
        assert error.retriable is False

    def test_rate_limit_error_has_retry_after(self):
        """Rate limit errors should have retry_after."""
        error = RateLimitError("Rate limited", retry_after=60.0)
        assert error.retry_after == 60.0
        assert error.retriable is True

    def test_api_timeout_error(self):
        """API timeout errors should be retriable."""
        error = APITimeoutError("Timeout", timeout_seconds=30.0)
        assert error.timeout_seconds == 30.0
        assert error.retriable is True

    def test_service_unavailable_error(self):
        """Service unavailable errors should be retriable."""
        error = ServiceUnavailableError("Service down")
        assert error.retriable is True

    def test_data_not_found_error(self):
        """Data not found errors should not be retriable."""
        error = DataNotFoundError(
            "Not found",
            resource_type="player",
            resource_id="12345",
        )
        assert error.retriable is False
        assert error.resource_type == "player"
        assert error.resource_id == "12345"

    def test_validation_error(self):
        """Validation errors should not be retriable."""
        error = ValidationError(
            "Validation failed",
            field="name",
            value="invalid",
            expected_type="str",
        )
        assert error.retriable is False
        assert error.field == "name"

    def test_schema_error(self):
        """Schema errors should not be retriable."""
        error = SchemaError(
            "Schema mismatch",
            expected_columns=["a", "b"],
            actual_columns=["a", "c"],
        )
        assert error.retriable is False

    def test_circuit_breaker_error(self):
        """Circuit breaker errors should have endpoint info."""
        error = CircuitBreakerError(
            "Circuit open",
            endpoint="test_endpoint",
            failure_count=5,
            reset_time=time.time() + 60,
        )
        assert error.endpoint == "test_endpoint"
        assert error.failure_count == 5


class TestClassifyException:
    """Tests for exception classification."""

    def test_classify_rate_limit(self):
        """Rate limit exceptions should be classified correctly."""
        exc = Exception("Rate limit exceeded")
        result = classify_exception(exc)
        assert isinstance(result, RateLimitError)

    def test_classify_timeout(self):
        """Timeout exceptions should be classified correctly."""
        exc = TimeoutError("Connection timed out")
        result = classify_exception(exc)
        assert isinstance(result, APITimeoutError)

    def test_classify_connection_error(self):
        """Connection errors should be classified as service unavailable."""
        exc = ConnectionError("Connection refused")
        result = classify_exception(exc)
        assert isinstance(result, ServiceUnavailableError)

    def test_classify_404_not_found(self):
        """404 errors should be classified as data not found."""
        exc = Exception("404: Not Found")
        result = classify_exception(exc)
        assert isinstance(result, DataNotFoundError)

    def test_classify_unknown_error(self):
        """Unknown errors should be classified as permanent."""
        exc = ValueError("Unknown error type")
        result = classify_exception(exc)
        assert isinstance(result, PermanentError)

    def test_is_retriable_transient(self):
        """Transient errors should be retriable."""
        error = TransientError("Test")
        assert is_retriable(error) is True

    def test_is_retriable_permanent(self):
        """Permanent errors should not be retriable."""
        error = PermanentError("Test")
        assert is_retriable(error) is False

    def test_get_retry_delay_rate_limit(self):
        """Rate limit errors should use retry_after as delay."""
        error = RateLimitError("Test", retry_after=120.0)
        delay = get_retry_delay(error)
        assert delay == 120.0

    def test_get_retry_delay_service_unavailable(self):
        """Service unavailable errors should have reasonable delay."""
        error = ServiceUnavailableError("Test")
        delay = get_retry_delay(error)
        assert delay >= 5.0  # Minimum delay


class TestCircuitBreaker:
    """Tests for the circuit breaker pattern."""

    def test_initial_state_closed(self):
        """Circuit breaker should start in closed state."""
        cb = CircuitBreaker(name="test", failure_threshold=3)
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold(self):
        """Circuit should open after reaching failure threshold."""
        cb = CircuitBreaker(name="test", failure_threshold=3)

        for _ in range(3):
            cb.record_failure()

        assert cb.state == CircuitState.OPEN

    def test_allows_request_when_closed(self):
        """Should allow requests when circuit is closed."""
        cb = CircuitBreaker(name="test", failure_threshold=3)
        assert cb.allow_request() is True

    def test_blocks_request_when_open(self):
        """Should block requests when circuit is open."""
        cb = CircuitBreaker(name="test", failure_threshold=3, reset_timeout=60.0)

        for _ in range(3):
            cb.record_failure()

        assert cb.allow_request() is False

    def test_half_open_after_timeout(self):
        """Circuit should go to half-open after reset timeout."""
        cb = CircuitBreaker(name="test", failure_threshold=3, reset_timeout=0.1)

        for _ in range(3):
            cb.record_failure()

        assert cb.state == CircuitState.OPEN

        # Wait for reset timeout
        time.sleep(0.15)

        # Should transition to half-open on next request check
        assert cb.allow_request() is True
        assert cb.state == CircuitState.HALF_OPEN

    def test_closes_after_success_in_half_open(self):
        """Circuit should close after success in half-open state."""
        cb = CircuitBreaker(
            name="test",
            failure_threshold=3,
            reset_timeout=0.1,
            success_threshold=1,
        )

        for _ in range(3):
            cb.record_failure()

        time.sleep(0.15)
        cb.allow_request()  # Triggers transition to half-open

        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    def test_reopens_on_failure_in_half_open(self):
        """Circuit should reopen on failure in half-open state."""
        cb = CircuitBreaker(name="test", failure_threshold=3, reset_timeout=0.1)

        for _ in range(3):
            cb.record_failure()

        time.sleep(0.15)
        cb.allow_request()  # Triggers transition to half-open

        cb.record_failure()
        assert cb.state == CircuitState.OPEN


class TestCircuitBreakerRegistry:
    """Tests for the circuit breaker registry."""

    def test_singleton_pattern(self):
        """Registry should be a singleton."""
        registry1 = CircuitBreakerRegistry()
        registry2 = CircuitBreakerRegistry()
        assert registry1 is registry2

    def test_get_or_create_circuit(self):
        """Should create and return circuit breakers."""
        registry = CircuitBreakerRegistry()
        cb1 = registry.get_or_create("test_endpoint_1")
        cb2 = registry.get_or_create("test_endpoint_1")
        assert cb1 is cb2

    def test_different_endpoints_different_circuits(self):
        """Different endpoints should have different circuits."""
        registry = CircuitBreakerRegistry()
        cb1 = registry.get_or_create("endpoint_a")
        cb2 = registry.get_or_create("endpoint_b")
        assert cb1 is not cb2


class TestAdaptiveRateLimiter:
    """Tests for the adaptive rate limiter."""

    def test_initial_rate(self):
        """Should start with initial rate."""
        limiter = AdaptiveRateLimiter(
            initial_rate=10.0,
            min_rate=1.0,
            max_rate=100.0,
        )
        assert limiter.current_rate == 10.0

    def test_decrease_on_rate_limit(self):
        """Should decrease rate when rate limited."""
        limiter = AdaptiveRateLimiter(
            initial_rate=10.0,
            min_rate=1.0,
            max_rate=100.0,
            decrease_factor=0.5,
        )
        limiter.on_rate_limited()
        assert limiter.current_rate == 5.0

    def test_increase_on_success(self):
        """Should increase rate on success."""
        limiter = AdaptiveRateLimiter(
            initial_rate=10.0,
            min_rate=1.0,
            max_rate=100.0,
            increase_factor=1.1,
        )
        limiter.on_success()
        assert limiter.current_rate == 11.0

    def test_respects_min_rate(self):
        """Should not go below minimum rate."""
        limiter = AdaptiveRateLimiter(
            initial_rate=2.0,
            min_rate=1.0,
            max_rate=100.0,
            decrease_factor=0.1,
        )
        limiter.on_rate_limited()
        assert limiter.current_rate >= 1.0

    def test_respects_max_rate(self):
        """Should not exceed maximum rate."""
        limiter = AdaptiveRateLimiter(
            initial_rate=90.0,
            min_rate=1.0,
            max_rate=100.0,
            increase_factor=1.5,
        )
        limiter.on_success()
        limiter.on_success()
        assert limiter.current_rate <= 100.0


class TestPydanticSchemas:
    """Tests for Pydantic validation schemas."""

    def test_player_model_valid(self):
        """Valid player data should pass validation."""
        data = {
            "person_id": 12345,
            "first_name": "LeBron",
            "last_name": "James",
            "is_active": True,
        }
        player = Player(**data)
        assert player.person_id == 12345
        assert player.first_name == "LeBron"

    def test_player_model_full_name(self):
        """Player model should compute full name."""
        data = {
            "person_id": 12345,
            "first_name": "LeBron",
            "last_name": "James",
            "is_active": True,
        }
        player = Player(**data)
        assert player.full_name == "LeBron James"

    def test_player_game_stats_valid(self):
        """Valid player game stats should pass validation."""
        data = {
            "player_id": 12345,
            "game_id": "0022400001",
            "team_id": 1610612747,
            "pts": 25,
            "ast": 10,
            "reb": 7,
        }
        stats = PlayerGameStats(**data)
        assert stats.player_id == 12345
        assert stats.pts == 25

    def test_player_game_stats_negative_points_fails(self):
        """Negative points should fail validation."""
        data = {
            "player_id": 12345,
            "game_id": "0022400001",
            "team_id": 1610612747,
            "pts": -5,  # Invalid
        }
        with pytest.raises(ValueError, match="greater than or equal to 0"):
            PlayerGameStats(**data)

    def test_team_game_stats_valid(self):
        """Valid team game stats should pass validation."""
        data = {
            "team_id": 1610612747,
            "game_id": "0022400001",
            "pts": 110,
        }
        stats = TeamGameStats(**data)
        assert stats.team_id == 1610612747

    def test_common_player_info_cross_validation(self):
        """Common player info should validate draft fields together."""
        # Valid: both draft year and round
        data = {
            "person_id": 12345,
            "first_name": "Test",
            "last_name": "Player",
            "draft_year": 2020,
            "draft_round": 1,
        }
        info = CommonPlayerInfo(**data)
        assert info.draft_year == 2020

        # Valid: undrafted (no fields)
        data = {
            "person_id": 12345,
            "first_name": "Test",
            "last_name": "Player",
        }
        info = CommonPlayerInfo(**data)
        assert info.draft_year is None


class TestDataFrameValidation:
    """Tests for DataFrame validation utility."""

    def test_validate_dataframe_valid(self):
        """Valid DataFrame should pass validation."""
        df = pd.DataFrame(
            {
                "player_id": [1, 2, 3],
                "game_id": ["0000000001", "0000000002", "0000000003"],
                "team_id": [100, 100, 200],
                "pts": [10, 20, 15],
            }
        )

        valid_df, errors = validate_dataframe(df, PlayerGameStats)
        assert len(errors) == 0
        assert len(valid_df) == 3

    def test_validate_dataframe_with_errors(self):
        """Invalid rows should be captured in errors."""
        df = pd.DataFrame(
            {
                "player_id": [1, 2, 3],
                "game_id": ["0000000001", "0000000002", "0000000003"],
                "team_id": [100, 100, 200],
                "pts": [10, -5, 15],  # Row 1 has invalid negative points
            }
        )

        valid_df, errors = validate_dataframe(df, PlayerGameStats)
        assert len(errors) == 1  # One invalid row
        assert len(valid_df) == 2  # Two valid rows

    def test_validate_dataframe_all_invalid(self):
        """All invalid rows should be captured."""
        df = pd.DataFrame(
            {
                "player_id": [None, None],
                "game_id": ["0000000001", "0000000002"],
                "team_id": [100, 200],
            }
        )

        valid_df, errors = validate_dataframe(df, PlayerGameStats)
        assert len(valid_df) == 0
        assert len(errors) >= 1


class TestDraftHistory:
    """Tests for DraftHistory schema."""

    def test_draft_history_valid(self):
        """Valid draft history should pass validation."""
        data = {
            "person_id": 12345,
            "player_name": "Test Player",
            "season": "2020",
            "round_number": 1,
            "round_pick": 5,
            "overall_pick": 5,
            "team_id": 1610612747,
        }
        history = DraftHistory(**data)
        assert history.overall_pick == 5

    def test_draft_history_pick_validation(self):
        """Pick numbers should be positive."""
        data = {
            "person_id": 12345,
            "player_name": "Test Player",
            "season": "2020",
            "round_number": 1,
            "round_pick": -1,  # Invalid
            "overall_pick": 5,
            "team_id": 1610612747,
        }
        with pytest.raises(ValueError, match="greater than 0"):
            DraftHistory(**data)
