"""Custom exceptions for NBA data population with categorized error handling.

This module provides a hierarchy of exceptions that enable:
- Categorized error handling (transient vs permanent failures)
- Circuit breaker pattern support
- Retry decision making based on error type
- Quarantine system for failed records

Exception Hierarchy:
    PopulationError (base)
    ├── TransientError (retriable)
    │   ├── RateLimitError
    │   ├── TimeoutError
    │   └── ServiceUnavailableError
    ├── PermanentError (non-retriable)
    │   ├── DataNotFoundError
    │   ├── ValidationError
    │   └── SchemaError
    └── CircuitBreakerError (circuit open)
"""

from datetime import UTC, datetime
from enum import Enum
from typing import Any


class ErrorCategory(Enum):
    """Categories of errors for retry decision making."""

    TRANSIENT = "transient"  # Temporary, should retry
    PERMANENT = "permanent"  # Won't recover, don't retry
    RATE_LIMIT = "rate_limit"  # Rate limited, wait longer
    CIRCUIT_OPEN = "circuit_open"  # Circuit breaker tripped


class PopulationError(Exception):
    """Base exception for all population-related errors.

    Attributes:
        message: Human-readable error message
        context: Additional context about the error (e.g., player_id, game_id)
        category: Error category for retry decision
        timestamp: When the error occurred
        retriable: Whether this error type can be retried
    """

    category: ErrorCategory = ErrorCategory.PERMANENT
    retriable: bool = False

    def __init__(
        self,
        message: str,
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            message: Error message
            context: Additional context (player_id, game_id, season, etc.)
            cause: Original exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}
        self.cause = cause
        self.timestamp = datetime.now(tz=UTC)

    def to_dict(self) -> dict[str, Any]:
        """Serialize exception to dictionary for logging/storage.

        Returns:
            Dictionary representation of the error
        """
        return {
            "type": self.__class__.__name__,
            "message": self.message,
            "category": self.category.value,
            "retriable": self.retriable,
            "context": self.context,
            "timestamp": self.timestamp.isoformat(),
            "cause": str(self.cause) if self.cause else None,
        }

    def __str__(self) -> str:
        """String representation with context."""
        ctx = ", ".join(f"{k}={v}" for k, v in self.context.items())
        if ctx:
            return f"{self.message} [{ctx}]"
        return self.message


# =============================================================================
# TRANSIENT ERRORS (Retriable)
# =============================================================================


class TransientError(PopulationError):
    """Base class for temporary errors that may succeed on retry.

    These errors indicate temporary issues like network problems,
    server overload, or rate limiting that may resolve with time.
    """

    category = ErrorCategory.TRANSIENT
    retriable = True

    def __init__(
        self,
        message: str,
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
        retry_after: float | None = None,
    ) -> None:
        """Initialize transient error.

        Args:
            message: Error message
            context: Additional context
            cause: Original exception
            retry_after: Suggested wait time before retry (seconds)
        """
        super().__init__(message, context, cause)
        self.retry_after = retry_after


class RateLimitError(TransientError):
    """Error when API rate limit is exceeded (HTTP 429).

    Attributes:
        retry_after: Time to wait before retrying (from Retry-After header)
        limit_type: Type of limit hit (requests_per_minute, concurrent, etc.)
    """

    category = ErrorCategory.RATE_LIMIT

    def __init__(
        self,
        message: str = "API rate limit exceeded",
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
        retry_after: float = 60.0,
        limit_type: str = "requests_per_minute",
    ) -> None:
        """Initialize rate limit error.

        Args:
            message: Error message
            context: Additional context
            cause: Original exception
            retry_after: Seconds to wait (default 60s)
            limit_type: Type of rate limit hit
        """
        super().__init__(message, context, cause, retry_after)
        self.limit_type = limit_type


class APITimeoutError(TransientError):
    """Error when API request times out.

    Attributes:
        timeout_seconds: The timeout value that was exceeded
        endpoint: The API endpoint that timed out
    """

    def __init__(
        self,
        message: str = "API request timed out",
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
        timeout_seconds: float = 30.0,
        endpoint: str | None = None,
    ) -> None:
        """Initialize timeout error.

        Args:
            message: Error message
            context: Additional context
            cause: Original exception
            timeout_seconds: The timeout that was exceeded
            endpoint: API endpoint that timed out
        """
        super().__init__(message, context, cause, retry_after=5.0)
        self.timeout_seconds = timeout_seconds
        self.endpoint = endpoint
        if endpoint:
            self.context["endpoint"] = endpoint


class ServiceUnavailableError(TransientError):
    """Error when API service is temporarily unavailable (HTTP 500, 502, 503, 504).

    Attributes:
        status_code: HTTP status code received
    """

    def __init__(
        self,
        message: str = "API service unavailable",
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
        status_code: int | None = None,
        retry_after: float = 30.0,
    ) -> None:
        """Initialize service unavailable error.

        Args:
            message: Error message
            context: Additional context
            cause: Original exception
            status_code: HTTP status code
            retry_after: Suggested wait time
        """
        super().__init__(message, context, cause, retry_after)
        self.status_code = status_code
        if status_code:
            self.context["status_code"] = status_code


class ConnectionError(TransientError):
    """Error when network connection fails."""

    def __init__(
        self,
        message: str = "Network connection failed",
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize connection error."""
        super().__init__(message, context, cause, retry_after=10.0)


# =============================================================================
# PERMANENT ERRORS (Non-Retriable)
# =============================================================================


class PermanentError(PopulationError):
    """Base class for errors that won't recover on retry.

    These errors indicate issues like missing data, invalid parameters,
    or schema mismatches that require manual intervention.
    """

    category = ErrorCategory.PERMANENT
    retriable = False


class DataNotFoundError(PermanentError):
    """Error when requested data does not exist (HTTP 404).

    This is expected for some queries (e.g., player not in league yet,
    game not played). Should not retry, but may not be a critical failure.

    Attributes:
        resource_type: Type of resource not found (player, game, season)
        resource_id: Identifier of missing resource
    """

    def __init__(
        self,
        message: str = "Requested data not found",
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
        resource_type: str | None = None,
        resource_id: str | int | None = None,
    ) -> None:
        """Initialize data not found error.

        Args:
            message: Error message
            context: Additional context
            cause: Original exception
            resource_type: Type of resource (player, game, etc.)
            resource_id: ID of missing resource
        """
        super().__init__(message, context, cause)
        self.resource_type = resource_type
        self.resource_id = resource_id
        if resource_type:
            self.context["resource_type"] = resource_type
        if resource_id:
            self.context["resource_id"] = resource_id


class ValidationError(PermanentError):
    """Error when data fails validation checks.

    Attributes:
        validation_errors: List of specific validation failures
        data_sample: Sample of invalid data for debugging
    """

    def __init__(
        self,
        message: str = "Data validation failed",
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
        validation_errors: list[str] | None = None,
        data_sample: dict[str, Any] | None = None,
    ) -> None:
        """Initialize validation error.

        Args:
            message: Error message
            context: Additional context
            cause: Original exception
            validation_errors: List of validation error messages
            data_sample: Sample of the invalid data
        """
        super().__init__(message, context, cause)
        self.validation_errors = validation_errors or []
        self.data_sample = data_sample
        if validation_errors:
            self.context["validation_errors"] = validation_errors[:5]  # First 5


class SchemaError(PermanentError):
    """Error when API response schema doesn't match expected structure.

    Attributes:
        expected_columns: Columns we expected
        actual_columns: Columns we received
        missing_columns: Columns that are missing
    """

    def __init__(
        self,
        message: str = "API response schema mismatch",
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
        expected_columns: list[str] | None = None,
        actual_columns: list[str] | None = None,
    ) -> None:
        """Initialize schema error.

        Args:
            message: Error message
            context: Additional context
            cause: Original exception
            expected_columns: Expected column names
            actual_columns: Actual column names received
        """
        super().__init__(message, context, cause)
        self.expected_columns = expected_columns or []
        self.actual_columns = actual_columns or []
        self.missing_columns = (
            list(set(self.expected_columns) - set(self.actual_columns))
            if expected_columns and actual_columns
            else []
        )
        if self.missing_columns:
            self.context["missing_columns"] = self.missing_columns


class AuthenticationError(PermanentError):
    """Error when API authentication fails (HTTP 401, 403)."""

    def __init__(
        self,
        message: str = "API authentication failed",
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize authentication error."""
        super().__init__(message, context, cause)


class InvalidParameterError(PermanentError):
    """Error when API parameters are invalid (HTTP 400).

    Attributes:
        parameter_name: Name of invalid parameter
        parameter_value: Value that was invalid
    """

    def __init__(
        self,
        message: str = "Invalid API parameter",
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
        parameter_name: str | None = None,
        parameter_value: Any = None,
    ) -> None:
        """Initialize invalid parameter error.

        Args:
            message: Error message
            context: Additional context
            cause: Original exception
            parameter_name: Name of the invalid parameter
            parameter_value: The invalid value
        """
        super().__init__(message, context, cause)
        self.parameter_name = parameter_name
        self.parameter_value = parameter_value
        if parameter_name:
            self.context["parameter_name"] = parameter_name
        if parameter_value is not None:
            self.context["parameter_value"] = str(parameter_value)


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================


class CircuitBreakerError(PopulationError):
    """Error when circuit breaker is open, preventing requests.

    Attributes:
        opened_at: When the circuit was opened
        failure_count: Number of failures that triggered opening
        reset_at: When the circuit will attempt to close
    """

    category = ErrorCategory.CIRCUIT_OPEN
    retriable = False

    def __init__(
        self,
        message: str = "Circuit breaker is open",
        context: dict[str, Any] | None = None,
        failure_count: int = 0,
        reset_at: datetime | None = None,
    ) -> None:
        """Initialize circuit breaker error.

        Args:
            message: Error message
            context: Additional context
            failure_count: Number of failures
            reset_at: When circuit will try to reset
        """
        super().__init__(message, context)
        self.opened_at = datetime.now(tz=UTC)
        self.failure_count = failure_count
        self.reset_at = reset_at
        self.context["failure_count"] = failure_count
        if reset_at:
            self.context["reset_at"] = reset_at.isoformat()


# =============================================================================
# DATABASE ERRORS
# =============================================================================


class DatabaseError(PopulationError):
    """Base class for database-related errors."""

    category = ErrorCategory.PERMANENT
    retriable = False


class DatabaseConnectionError(DatabaseError, TransientError):
    """Error when database connection fails (may be transient)."""

    category = ErrorCategory.TRANSIENT
    retriable = True

    def __init__(
        self,
        message: str = "Database connection failed",
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize database connection error."""
        # Call PopulationError.__init__ directly
        PopulationError.__init__(self, message, context, cause)
        self.retry_after = 5.0


class DatabaseIntegrityError(DatabaseError):
    """Error when database integrity constraint is violated.

    Attributes:
        constraint: Name of violated constraint
        table: Table where violation occurred
    """

    def __init__(
        self,
        message: str = "Database integrity constraint violated",
        context: dict[str, Any] | None = None,
        cause: Exception | None = None,
        constraint: str | None = None,
        table: str | None = None,
    ) -> None:
        """Initialize integrity error."""
        super().__init__(message, context, cause)
        self.constraint = constraint
        self.table = table
        if constraint:
            self.context["constraint"] = constraint
        if table:
            self.context["table"] = table


# =============================================================================
# ERROR CLASSIFICATION UTILITY
# =============================================================================


def classify_exception(exc: Exception) -> PopulationError:
    """Classify a generic exception into a specific PopulationError.

    This function analyzes the exception message and type to determine
    the appropriate PopulationError subclass for proper retry handling.

    Args:
        exc: Original exception to classify

    Returns:
        Appropriate PopulationError subclass instance
    """
    if isinstance(exc, PopulationError):
        return exc

    error_str = str(exc).lower()
    exc_type = type(exc).__name__.lower()

    # Check for rate limiting indicators
    if any(x in error_str for x in ["rate", "429", "too many requests", "throttl"]):
        return RateLimitError(
            message=f"Rate limit: {exc}",
            cause=exc,
            retry_after=60.0,
        )

    # Check for timeout indicators
    if any(x in error_str for x in ["timeout", "timed out"]) or "timeout" in exc_type:
        return APITimeoutError(
            message=f"Timeout: {exc}",
            cause=exc,
        )

    # Check for not found indicators
    if any(x in error_str for x in ["404", "not found", "no data"]):
        return DataNotFoundError(
            message=f"Not found: {exc}",
            cause=exc,
        )

    # Check for service unavailable (5xx errors)
    if any(x in error_str for x in ["500", "502", "503", "504", "service unavailable"]):
        # Extract status code if present
        status_code = None
        for code in [500, 502, 503, 504]:
            if str(code) in error_str:
                status_code = code
                break
        return ServiceUnavailableError(
            message=f"Service error: {exc}",
            cause=exc,
            status_code=status_code,
        )

    # Check for connection errors
    if any(
        x in error_str for x in ["connection", "network", "refused", "unreachable"]
    ) or any(x in exc_type for x in ["connection", "network"]):
        return ConnectionError(
            message=f"Connection error: {exc}",
            cause=exc,
        )

    # Check for authentication errors
    if any(x in error_str for x in ["401", "403", "unauthorized", "forbidden", "auth"]):
        return AuthenticationError(
            message=f"Auth error: {exc}",
            cause=exc,
        )

    # Check for validation errors
    if any(x in error_str for x in ["invalid", "validation", "400", "bad request"]):
        return InvalidParameterError(
            message=f"Invalid parameter: {exc}",
            cause=exc,
        )

    # Default to transient error (safer to retry unknown errors)
    return TransientError(
        message=f"Unknown error: {exc}",
        cause=exc,
    )


def is_retriable(exc: Exception) -> bool:
    """Check if an exception is retriable.

    Args:
        exc: Exception to check

    Returns:
        True if the error should be retried
    """
    if isinstance(exc, PopulationError):
        return exc.retriable
    return classify_exception(exc).retriable


def get_retry_delay(exc: Exception, attempt: int, base_delay: float = 1.0) -> float:
    """Calculate appropriate retry delay based on error type.

    Args:
        exc: Exception that caused the retry
        attempt: Current attempt number (0-indexed)
        base_delay: Base delay in seconds

    Returns:
        Delay in seconds before next retry
    """
    classified = (
        classify_exception(exc) if not isinstance(exc, PopulationError) else exc
    )

    # Rate limit errors use suggested retry_after
    if isinstance(classified, RateLimitError) and classified.retry_after:
        return classified.retry_after * (1.5**attempt)  # Increase on subsequent retries

    # Transient errors with retry_after
    if isinstance(classified, TransientError) and classified.retry_after:
        return classified.retry_after * (2**attempt)

    # Default exponential backoff
    return base_delay * (2**attempt)
