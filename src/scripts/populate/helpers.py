"""Shared helper utilities for population scripts.

This module provides common helper functions used across NBA data population
scripts, including:

- Logging configuration
- JSON file I/O
- Duration formatting
- Season type resolution

Note: DataFrame transformation helpers are in transform_utils.py.
Column mappings and constants are in constants.py.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar, overload

from src.scripts.populate.constants import SEASON_TYPE_MAP, SeasonType


if TYPE_CHECKING:
    from collections.abc import Mapping


logger = logging.getLogger(__name__)

# Type aliases
T = TypeVar("T")
JSONDict = dict[str, Any]

# Default log format
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


# =============================================================================
# LOGGING UTILITIES
# =============================================================================


def configure_logging(
    level: int | str = logging.INFO,
    fmt: str = DEFAULT_LOG_FORMAT,
    *,
    log_file: Path | str | None = None,
) -> None:
    """Configure standard logging for population scripts.

    Args:
        level: Logging level (int) or level name (str like "DEBUG", "INFO").
        fmt: Logging format string.
        log_file: Optional file path to also write logs to.

    Example:
        >>> configure_logging(level="DEBUG")
        >>> configure_logging(level=logging.WARNING, log_file="populate.log")
    """
    resolved_level: int
    if isinstance(level, str):
        resolved_level = logging._nameToLevel.get(level.upper(), logging.INFO)
    else:
        resolved_level = level

    handlers: list[logging.Handler] = [logging.StreamHandler()]

    if log_file:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(file_path, encoding="utf-8"))

    logging.basicConfig(
        level=resolved_level,
        format=fmt,
        handlers=handlers,
        force=True,  # Override any existing configuration
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name.

    Args:
        name: Logger name (typically __name__).

    Returns:
        Configured logger instance.
    """
    return logging.getLogger(name)


# =============================================================================
# TIME AND DURATION UTILITIES
# =============================================================================


def format_duration(seconds: float) -> str:
    """Format a duration in seconds to a human-readable compact string.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string like "5s", "3m45s", or "2h15m30s".

    Examples:
        >>> format_duration(45)
        '45s'
        >>> format_duration(195)
        '3m15s'
        >>> format_duration(3723)
        '1h02m03s'
    """
    total_seconds = max(0, int(seconds))
    minutes, secs = divmod(total_seconds, 60)

    if minutes < 60:
        if minutes == 0:
            return f"{secs}s"
        return f"{minutes}m{secs:02d}s"

    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{secs:02d}s"


def get_timestamp() -> str:
    """Get current UTC timestamp as ISO format string.

    Returns:
        ISO 8601 formatted timestamp string.
    """
    return datetime.utcnow().isoformat()


# =============================================================================
# JSON FILE UTILITIES
# =============================================================================


@overload
def load_json_file(path: Path, default: None = None) -> JSONDict | None: ...


@overload
def load_json_file(path: Path, default: JSONDict) -> JSONDict: ...


def load_json_file(
    path: Path,
    default: JSONDict | None = None,
) -> JSONDict | None:
    """Load a JSON file into a dictionary, returning default on failure.

    Args:
        path: Path to the JSON file.
        default: Default value if file doesn't exist or can't be parsed.

    Returns:
        Parsed JSON dictionary, or default value on failure.

    Raises:
        No exceptions are raised; failures return the default.
    """
    if not path.exists():
        return default

    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
            if isinstance(data, dict):
                return data
            logger.warning("JSON file %s did not contain a dictionary", path)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not load JSON file %s: %s", path, exc)

    return default


def save_json_file(
    path: Path,
    payload: Mapping[str, Any],
    *,
    indent: int = 2,
    ensure_ascii: bool = False,
) -> bool:
    """Persist a dictionary payload to a JSON file.

    Creates parent directories if they don't exist.

    Args:
        path: Path to save the JSON file.
        payload: Dictionary to serialize.
        indent: JSON indentation level.
        ensure_ascii: If True, escape non-ASCII characters.

    Returns:
        True if save succeeded, False otherwise.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=indent, ensure_ascii=ensure_ascii)
        return True
    except (OSError, TypeError) as exc:
        logger.error("Could not save JSON file %s: %s", path, exc)
        return False


# =============================================================================
# SEASON TYPE UTILITIES
# =============================================================================


def resolve_season_types(
    default: list[str] | None = None,
    *,
    regular_only: bool = False,
    playoffs_only: bool = False,
) -> list[str]:
    """Resolve season types based on regular/playoffs flags.

    Args:
        default: Default season types if neither flag is set.
        regular_only: If True, return only Regular Season.
        playoffs_only: If True, return only Playoffs.

    Returns:
        List of season type strings for API calls.

    Examples:
        >>> resolve_season_types(regular_only=True)
        ['Regular Season']
        >>> resolve_season_types(playoffs_only=True)
        ['Playoffs']
        >>> resolve_season_types(default=["Regular Season", "Playoffs"])
        ['Regular Season', 'Playoffs']
    """
    if regular_only:
        return [SeasonType.REGULAR.value]
    if playoffs_only:
        return [SeasonType.PLAYOFFS.value]

    if default:
        return default

    return [SeasonType.REGULAR.value]


def normalize_season_type(season_type: str) -> str:
    """Normalize a season type string to the canonical API format.

    Args:
        season_type: Season type string (may be abbreviated or lowercase).

    Returns:
        Canonical season type string (e.g., "Regular Season").

    Examples:
        >>> normalize_season_type("regular")
        'Regular Season'
        >>> normalize_season_type("playoffs")
        'Playoffs'
    """
    normalized = season_type.lower().strip()

    # Check mapping first
    if normalized in SEASON_TYPE_MAP:
        return SEASON_TYPE_MAP[normalized]

    # Try direct enum lookup
    try:
        return SeasonType(season_type).value
    except ValueError:
        pass

    # Return as-is if not found
    logger.warning("Unknown season type: %s", season_type)
    return season_type


# =============================================================================
# MISC UTILITIES
# =============================================================================


def chunk_list(items: list[T], chunk_size: int) -> list[list[T]]:
    """Split a list into chunks of specified size.

    Args:
        items: List to chunk.
        chunk_size: Maximum size of each chunk.

    Returns:
        List of chunks.

    Example:
        >>> chunk_list([1, 2, 3, 4, 5], 2)
        [[1, 2], [3, 4], [5]]
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def safe_get(
    data: Mapping[str, Any] | None,
    key: str,
    default: T | None = None,
) -> T | Any | None:
    """Safely get a value from a dictionary.

    Args:
        data: Dictionary to get value from (can be None).
        key: Key to look up.
        default: Default value if key not found or data is None.

    Returns:
        Value for key, or default.
    """
    if data is None:
        return default
    return data.get(key, default)


def pluralize(count: int, singular: str, plural: str | None = None) -> str:
    """Return singular or plural form based on count.

    Args:
        count: Number to check.
        singular: Singular form of the word.
        plural: Plural form (defaults to singular + 's').

    Returns:
        Appropriate form with count.

    Examples:
        >>> pluralize(1, "record")
        '1 record'
        >>> pluralize(5, "record")
        '5 records'
        >>> pluralize(0, "player", "players")
        '0 players'
    """
    plural_form = plural or f"{singular}s"
    word = singular if count == 1 else plural_form
    return f"{count:,} {word}"
