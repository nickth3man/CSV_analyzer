"""Circuit breaker pattern implementation for API resilience.

The circuit breaker prevents cascading failures by:
1. Monitoring for failures (CLOSED state - normal operation)
2. Opening when failure threshold exceeded (OPEN state - fast fail)
3. Periodically testing if service recovered (HALF_OPEN state)

Usage:
    circuit = CircuitBreaker(name="nba_api", failure_threshold=5)

    @circuit
    def call_api():
        return requests.get(...)

    # Or use as context manager
    with circuit:
        response = requests.get(...)
"""

import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, TypeVar

from src.scripts.populate.exceptions import CircuitBreakerError


logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """States of the circuit breaker."""

    CLOSED = "closed"  # Normal operation, requests pass through
    OPEN = "open"  # Failing, requests blocked
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker monitoring."""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rejected_requests: int = 0  # Requests rejected while open
    state_changes: int = 0
    last_failure_time: datetime | None = None
    last_success_time: datetime | None = None
    current_state: CircuitState = CircuitState.CLOSED
    opened_at: datetime | None = None
    failure_rate: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize stats to dictionary."""
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "rejected_requests": self.rejected_requests,
            "state_changes": self.state_changes,
            "last_failure_time": (
                self.last_failure_time.isoformat() if self.last_failure_time else None
            ),
            "last_success_time": (
                self.last_success_time.isoformat() if self.last_success_time else None
            ),
            "current_state": self.current_state.value,
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
            "failure_rate": self.failure_rate,
        }


@dataclass
class FailureRecord:
    """Record of a failure for sliding window tracking."""

    timestamp: datetime
    exception: Exception
    context: dict[str, Any] = field(default_factory=dict)


class CircuitBreaker:
    """Circuit breaker for protecting against cascading failures.

    Configuration:
        failure_threshold: Number of failures to open circuit (default: 5)
        success_threshold: Successes in half-open to close circuit (default: 3)
        timeout: Seconds to wait before testing again (default: 60)
        failure_window: Window in seconds to count failures (default: 60)
        excluded_exceptions: Exceptions that don't count as failures

    States:
        CLOSED: Normal operation, all requests pass through
        OPEN: Service is failing, requests are rejected immediately
        HALF_OPEN: Testing recovery, limited requests allowed

    Example:
        >>> breaker = CircuitBreaker("nba_api", failure_threshold=5)
        >>> @breaker
        ... def fetch_data():
        ...     return api.get_data()
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 3,
        timeout: float = 60.0,
        failure_window: float = 60.0,
        excluded_exceptions: tuple[type[Exception], ...] | None = None,
    ) -> None:
        """Initialize circuit breaker.

        Args:
            name: Name for logging/identification
            failure_threshold: Failures to trigger open state
            success_threshold: Successes in half-open to close
            timeout: Seconds before transitioning from open to half-open
            failure_window: Window in seconds to count failures
            excluded_exceptions: Exceptions that don't trigger failure count
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout = timeout
        self.failure_window = failure_window
        self.excluded_exceptions = excluded_exceptions or ()

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: datetime | None = None
        self._opened_at: datetime | None = None

        # Sliding window for failure tracking
        self._failures: deque[FailureRecord] = deque()

        # Thread safety
        self._lock = threading.RLock()

        # Statistics
        self._stats = CircuitBreakerStats()

        logger.info(
            f"CircuitBreaker '{name}' initialized: "
            f"threshold={failure_threshold}, timeout={timeout}s"
        )

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            return self._state

    @property
    def stats(self) -> CircuitBreakerStats:
        """Get current statistics."""
        with self._lock:
            self._stats.current_state = self._state
            self._stats.opened_at = self._opened_at
            total = self._stats.total_requests
            if total > 0:
                self._stats.failure_rate = self._stats.failed_requests / total
            return self._stats

    def _is_excluded_exception(self, exc: Exception) -> bool:
        """Check if exception should not trigger failure count."""
        return isinstance(exc, self.excluded_exceptions)

    def _clean_old_failures(self) -> None:
        """Remove failures outside the sliding window."""
        cutoff = datetime.now(tz=UTC).timestamp() - self.failure_window
        while self._failures and self._failures[0].timestamp.timestamp() < cutoff:
            self._failures.popleft()

    def _should_trip(self) -> bool:
        """Check if circuit should trip to open state."""
        self._clean_old_failures()
        return len(self._failures) >= self.failure_threshold

    def _can_attempt(self) -> bool:
        """Check if a request can be attempted.

        Returns:
            True if request should proceed, False if should reject
        """
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                # Check if timeout has passed
                if self._opened_at:
                    elapsed = (datetime.now(tz=UTC) - self._opened_at).total_seconds()
                    if elapsed >= self.timeout:
                        self._transition_to_half_open()
                        return True
                return False

            # HALF_OPEN - allow limited requests
            return True

    def _record_success(self) -> None:
        """Record a successful request."""
        with self._lock:
            self._stats.total_requests += 1
            self._stats.successful_requests += 1
            self._stats.last_success_time = datetime.now(tz=UTC)

            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._transition_to_closed()

    def _record_failure(
        self, exc: Exception, context: dict[str, Any] | None = None
    ) -> None:
        """Record a failed request."""
        with self._lock:
            self._stats.total_requests += 1
            self._stats.failed_requests += 1
            self._stats.last_failure_time = datetime.now(tz=UTC)
            self._last_failure_time = datetime.now(tz=UTC)

            # Don't count excluded exceptions
            if self._is_excluded_exception(exc):
                return

            # Add to sliding window
            self._failures.append(
                FailureRecord(
                    timestamp=datetime.now(tz=UTC),
                    exception=exc,
                    context=context or {},
                )
            )

            if self._state == CircuitState.CLOSED:
                if self._should_trip():
                    self._transition_to_open()
            elif self._state == CircuitState.HALF_OPEN:
                # Any failure in half-open reopens circuit
                self._transition_to_open()

    def _record_rejected(self) -> None:
        """Record a rejected request (circuit open)."""
        with self._lock:
            self._stats.total_requests += 1
            self._stats.rejected_requests += 1

    def _transition_to_open(self) -> None:
        """Transition to OPEN state."""
        logger.warning(
            f"CircuitBreaker '{self.name}' OPENING - "
            f"{len(self._failures)} failures in {self.failure_window}s window"
        )
        self._state = CircuitState.OPEN
        self._opened_at = datetime.now(tz=UTC)
        self._success_count = 0
        self._stats.state_changes += 1

    def _transition_to_half_open(self) -> None:
        """Transition to HALF_OPEN state."""
        logger.info(f"CircuitBreaker '{self.name}' transitioning to HALF_OPEN")
        self._state = CircuitState.HALF_OPEN
        self._success_count = 0
        self._stats.state_changes += 1

    def _transition_to_closed(self) -> None:
        """Transition to CLOSED state."""
        logger.info(f"CircuitBreaker '{self.name}' CLOSING - service recovered")
        self._state = CircuitState.CLOSED
        self._failures.clear()
        self._success_count = 0
        self._opened_at = None
        self._stats.state_changes += 1

    def reset(self) -> None:
        """Manually reset circuit breaker to closed state."""
        with self._lock:
            logger.info(f"CircuitBreaker '{self.name}' manually reset")
            self._state = CircuitState.CLOSED
            self._failures.clear()
            self._success_count = 0
            self._opened_at = None

    def execute(
        self,
        func: Callable[..., T],
        *args,
        context: dict[str, Any] | None = None,
        **kwargs,
    ) -> T:
        """Execute a function with circuit breaker protection.

        Args:
            func: Function to execute
            *args: Positional arguments for func
            context: Context for error tracking
            **kwargs: Keyword arguments for func

        Returns:
            Result of func execution

        Raises:
            CircuitBreakerError: If circuit is open
            Exception: Original exception from func
        """
        if not self._can_attempt():
            self._record_rejected()
            raise CircuitBreakerError(
                message=f"Circuit '{self.name}' is OPEN",
                context=context,
                failure_count=len(self._failures),
                reset_at=datetime.fromtimestamp(
                    (self._opened_at.timestamp() + self.timeout)
                    if self._opened_at
                    else 0,
                    tz=UTC,
                ),
            )

        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as exc:
            self._record_failure(exc, context)
            raise

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to wrap a function with circuit breaker.

        Example:
            >>> @circuit_breaker
            ... def fetch_data():
            ...     return api.get_data()
        """

        def wrapper(*args, **kwargs) -> T:
            return self.execute(func, *args, **kwargs)

        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        return wrapper

    def __enter__(self) -> "CircuitBreaker":
        """Context manager entry - check if can proceed."""
        if not self._can_attempt():
            self._record_rejected()
            raise CircuitBreakerError(
                message=f"Circuit '{self.name}' is OPEN",
                failure_count=len(self._failures),
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit - record success or failure."""
        if exc_val is None:
            self._record_success()
        else:
            self._record_failure(exc_val)
        return False  # Don't suppress exceptions


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers.

    Provides centralized access to circuit breakers for different services.
    """

    _instance: "CircuitBreakerRegistry | None" = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        """Initialize the registry (called only once due to singleton)."""
        self._breakers: dict[str, CircuitBreaker] = {}
        self._breaker_lock = threading.RLock()

    def __new__(cls) -> "CircuitBreakerRegistry":
        """Singleton pattern."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def get_or_create(
        self,
        name: str,
        **kwargs,
    ) -> CircuitBreaker:
        """Get existing circuit breaker or create new one.

        Args:
            name: Circuit breaker name
            **kwargs: Arguments for CircuitBreaker if creating new

        Returns:
            CircuitBreaker instance
        """
        with self._breaker_lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(name, **kwargs)
            return self._breakers[name]

    def get(self, name: str) -> CircuitBreaker | None:
        """Get circuit breaker by name."""
        with self._breaker_lock:
            return self._breakers.get(name)

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        with self._breaker_lock:
            for breaker in self._breakers.values():
                breaker.reset()

    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all circuit breakers."""
        with self._breaker_lock:
            return {
                name: breaker.stats.to_dict()
                for name, breaker in self._breakers.items()
            }


# Global registry instance
circuit_registry = CircuitBreakerRegistry()


def get_circuit_breaker(name: str, **kwargs) -> CircuitBreaker:
    """Get or create a circuit breaker from the global registry.

    Args:
        name: Name of the circuit breaker
        **kwargs: Configuration for new circuit breaker

    Returns:
        CircuitBreaker instance
    """
    return circuit_registry.get_or_create(name, **kwargs)


# =============================================================================
# ADAPTIVE RATE LIMITER
# =============================================================================


class AdaptiveRateLimiter:
    """Rate limiter that adapts based on API response behavior.

    Features:
    - Increases delay on rate limit errors
    - Decreases delay on consecutive successes
    - Configurable min/max bounds
    - Thread-safe

    Usage:
        limiter = AdaptiveRateLimiter(base_delay=0.6)

        for item in items:
            limiter.wait()
            try:
                result = api.call(item)
                limiter.record_success()
            except RateLimitError:
                limiter.record_rate_limit()
    """

    def __init__(
        self,
        base_delay: float = 0.6,
        min_delay: float = 0.2,
        max_delay: float = 30.0,
        decrease_factor: float = 0.95,
        increase_factor: float = 2.0,
        success_threshold: int = 10,
    ) -> None:
        """Initialize adaptive rate limiter.

        Args:
            base_delay: Starting delay in seconds
            min_delay: Minimum delay (won't go below this)
            max_delay: Maximum delay (won't exceed this)
            decrease_factor: Multiply delay by this on success streak
            increase_factor: Multiply delay by this on rate limit
            success_threshold: Successes needed to decrease delay
        """
        self.base_delay = base_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.decrease_factor = decrease_factor
        self.increase_factor = increase_factor
        self.success_threshold = success_threshold

        self._current_delay = base_delay
        self._success_count = 0
        self._lock = threading.Lock()

    @property
    def current_delay(self) -> float:
        """Get current delay value."""
        with self._lock:
            return self._current_delay

    def wait(self) -> None:
        """Wait for the current delay period."""
        delay = self.current_delay
        if delay > 0:
            time.sleep(delay)

    def record_success(self) -> None:
        """Record a successful request, potentially decreasing delay."""
        with self._lock:
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                new_delay = self._current_delay * self.decrease_factor
                self._current_delay = max(self.min_delay, new_delay)
                self._success_count = 0
                logger.debug(
                    f"Rate limiter delay decreased to {self._current_delay:.2f}s"
                )

    def record_rate_limit(self, retry_after: float | None = None) -> None:
        """Record a rate limit error, increasing delay.

        Args:
            retry_after: Suggested delay from API (uses this if larger)
        """
        with self._lock:
            if retry_after and retry_after > self._current_delay:
                self._current_delay = min(self.max_delay, retry_after)
            else:
                new_delay = self._current_delay * self.increase_factor
                self._current_delay = min(self.max_delay, new_delay)
            self._success_count = 0
            logger.warning(
                f"Rate limit hit, delay increased to {self._current_delay:.2f}s"
            )

    def record_failure(self) -> None:
        """Record a non-rate-limit failure (slight delay increase)."""
        with self._lock:
            self._success_count = 0
            # Smaller increase for general failures
            new_delay = self._current_delay * 1.2
            self._current_delay = min(self.max_delay, new_delay)

    def reset(self) -> None:
        """Reset to base delay."""
        with self._lock:
            self._current_delay = self.base_delay
            self._success_count = 0
