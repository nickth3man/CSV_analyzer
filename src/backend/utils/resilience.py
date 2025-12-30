"""Resilience patterns for external service calls.

This module provides decorators for circuit breaking, rate limiting, and timeouts
as specified in design.md Section 7.
"""

from __future__ import annotations

import functools
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ParamSpec, TypeVar

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


class CircuitState(Enum):
    """States for the circuit breaker."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, rejecting calls
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerState:
    """Tracks state for a circuit breaker instance."""

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: float = 0.0
    half_open_calls: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""

    pass


def circuit_breaker(
    threshold: int = 5,
    recovery: int = 60,
    half_open_max: int = 1,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Circuit breaker decorator to prevent cascade failures.

    Args:
        threshold: Number of failures before opening circuit.
        recovery: Seconds before attempting recovery (half-open state).
        half_open_max: Maximum test requests in half-open state.

    Returns:
        Decorated function with circuit breaker protection.

    Example:
        @circuit_breaker(threshold=3, recovery=60)
        def call_external_api():
            ...
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        state = CircuitBreakerState()

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with state.lock:
                current_time = time.time()

                if state.state == CircuitState.OPEN:
                    if current_time - state.last_failure_time >= recovery:
                        state.state = CircuitState.HALF_OPEN
                        state.half_open_calls = 0
                        logger.info(
                            f"Circuit breaker for {func.__name__} entering half-open state"
                        )
                    else:
                        raise CircuitBreakerError(
                            f"Circuit breaker is open for {func.__name__}. "
                            f"Recovery in {recovery - (current_time - state.last_failure_time):.1f}s"
                        )

                if (
                    state.state == CircuitState.HALF_OPEN
                    and state.half_open_calls >= half_open_max
                ):
                    raise CircuitBreakerError(
                        f"Circuit breaker for {func.__name__} is in half-open state, "
                        "waiting for test result"
                    )

                if state.state == CircuitState.HALF_OPEN:
                    state.half_open_calls += 1

            try:
                result = func(*args, **kwargs)
                with state.lock:
                    if state.state == CircuitState.HALF_OPEN:
                        state.state = CircuitState.CLOSED
                        state.failure_count = 0
                        logger.info(
                            f"Circuit breaker for {func.__name__} closed (recovered)"
                        )
                    elif state.failure_count > 0:
                        state.failure_count = 0
                return result
            except Exception as e:
                with state.lock:
                    state.failure_count += 1
                    state.last_failure_time = current_time

                    if state.state == CircuitState.HALF_OPEN:
                        state.state = CircuitState.OPEN
                        logger.warning(
                            f"Circuit breaker for {func.__name__} re-opened after failed recovery"
                        )
                    elif state.failure_count >= threshold:
                        state.state = CircuitState.OPEN
                        logger.warning(
                            f"Circuit breaker for {func.__name__} opened after "
                            f"{state.failure_count} failures"
                        )
                raise e

        return wrapper

    return decorator


@dataclass
class RateLimiterState:
    """Tracks state for a rate limiter instance."""

    request_times: list[float] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)


def rate_limit(
    rpm: int = 60,
    backoff: str = "exponential",
    max_backoff: int = 60,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Rate limiting decorator with backoff.

    Args:
        rpm: Maximum requests per minute.
        backoff: Backoff strategy ("exponential" or "linear").
        max_backoff: Maximum wait time in seconds.

    Returns:
        Decorated function with rate limiting.

    Example:
        @rate_limit(rpm=60, backoff="exponential")
        def call_api():
            ...
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        state = RateLimiterState()
        base_wait = 1.0  # Base wait time in seconds

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            attempt = 0
            while True:
                with state.lock:
                    current_time = time.time()
                    window_start = current_time - 60

                    state.request_times = [
                        t for t in state.request_times if t > window_start
                    ]

                    if len(state.request_times) < rpm:
                        state.request_times.append(current_time)
                        break

                attempt += 1
                if backoff == "exponential":
                    wait_time = min(base_wait * (2**attempt), max_backoff)
                else:
                    wait_time = min(base_wait * attempt, max_backoff)

                logger.debug(
                    f"Rate limit reached for {func.__name__}, "
                    f"waiting {wait_time:.1f}s (attempt {attempt})"
                )
                time.sleep(wait_time)

            return func(*args, **kwargs)

        return wrapper

    return decorator


def timeout(seconds: int = 30) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Timeout decorator to prevent runaway operations.

    Args:
        seconds: Maximum execution time in seconds.

    Returns:
        Decorated function with timeout protection.

    Raises:
        TimeoutError: If the function exceeds the timeout.

    Example:
        @timeout(seconds=30)
        def long_running_query():
            ...
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except FuturesTimeoutError:
                    raise TimeoutError(
                        f"Function {func.__name__} timed out after {seconds}s"
                    ) from None

        return wrapper

    return decorator


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_multiplier: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts.
        delay: Initial delay between retries in seconds.
        backoff_multiplier: Multiplier for delay after each attempt.
        exceptions: Tuple of exceptions to retry on.

    Returns:
        Decorated function with retry logic.

    Example:
        @retry(max_attempts=3, exceptions=(ConnectionError,))
        def flaky_api_call():
            ...
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            current_delay = delay
            last_exception: Exception | None = None

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed for "
                            f"{func.__name__}: {e}. Retrying in {current_delay:.1f}s"
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff_multiplier
                    else:
                        logger.error(
                            f"All {max_attempts} attempts failed for {func.__name__}"
                        )

            if last_exception:
                raise last_exception
            raise RuntimeError("Unexpected state in retry decorator")

        return wrapper

    return decorator


def combine_resilience(
    circuit_threshold: int = 5,
    circuit_recovery: int = 60,
    rate_rpm: int = 60,
    timeout_seconds: int = 30,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Combine multiple resilience patterns into a single decorator.

    Applies: timeout -> rate_limit -> circuit_breaker (outer to inner).

    Args:
        circuit_threshold: Circuit breaker failure threshold.
        circuit_recovery: Circuit breaker recovery time in seconds.
        rate_rpm: Rate limit requests per minute.
        timeout_seconds: Timeout in seconds.

    Returns:
        Decorated function with combined resilience patterns.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @circuit_breaker(threshold=circuit_threshold, recovery=circuit_recovery)
        @rate_limit(rpm=rate_rpm)
        @timeout(seconds=timeout_seconds)
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return func(*args, **kwargs)

        return wrapper

    return decorator
