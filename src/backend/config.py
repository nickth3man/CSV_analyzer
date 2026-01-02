"""Shared configuration for the NBA Data Analyst Agent.

This module provides centralized configuration loading from:
1. config.yaml file (primary source)
2. Environment variables (overrides)
3. Hardcoded defaults (fallback)

See design.md Section 11 for full specification.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

import yaml  # type: ignore[import-untyped]


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_FILE = PROJECT_ROOT / "config.yaml"


def get_current_nba_season() -> str:
    """Auto-detect current NBA season based on date.

    NBA season starts in October, so:
    - Oct 2024 - Sep 2025 = "2024-25"
    """
    now = datetime.now(tz=UTC)
    year = now.year
    if now.month >= 10:
        return f"{year}-{str(year + 1)[-2:]}"
    return f"{year - 1}-{str(year)[-2:]}"


@dataclass
class DatabaseConfig:
    """Database configuration."""

    path: str = "src/backend/data/nba.duckdb"
    timeout_seconds: int = 30


@dataclass
class LLMConfig:
    """LLM configuration."""

    model: str = "gpt-4o"
    temperature: float = 0.1
    max_tokens: int = 2000
    rate_limit_rpm: int = 60


@dataclass
class ResilienceConfig:
    """Resilience patterns configuration."""

    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_seconds: int = 60
    max_retries: int = 3
    backoff_base_seconds: int = 2


@dataclass
class CacheConfig:
    """Cache configuration."""

    semantic_threshold: float = 0.95
    ttl_hours: int = 24
    max_entries: int = 10000


@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str = "INFO"
    structured: bool = True
    include_prompts: bool = False


@dataclass
class AppConfig:
    """Complete application configuration."""

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    resilience: ResilienceConfig = field(default_factory=ResilienceConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


def load_config(config_path: Path | None = None) -> AppConfig:
    """Load configuration from YAML file with environment overrides.

    Args:
        config_path: Optional path to config file. Defaults to PROJECT_ROOT/config.yaml.

    Returns:
        AppConfig with loaded settings.
    """
    config_path = config_path or CONFIG_FILE
    config = AppConfig()

    if config_path.exists():
        try:
            with open(config_path) as f:
                data = yaml.safe_load(f) or {}

            if "database" in data:
                config.database = DatabaseConfig(**data["database"])
            if "llm" in data:
                config.llm = LLMConfig(**data["llm"])
            if "resilience" in data:
                config.resilience = ResilienceConfig(**data["resilience"])
            if "cache" in data:
                config.cache = CacheConfig(**data["cache"])
            if "logging" in data:
                config.logging = LoggingConfig(**data["logging"])

            logger.debug(f"Loaded config from {config_path}")
        except (yaml.YAMLError, TypeError) as e:
            logger.warning(f"Failed to load config from {config_path}: {e}")

    if db_path := os.environ.get("NBA_DB_PATH"):
        config.database.path = db_path
    if db_timeout := os.environ.get("NBA_DB_TIMEOUT"):
        config.database.timeout_seconds = int(db_timeout)

    if llm_model := os.environ.get("LLM_MODEL"):
        config.llm.model = llm_model
    if llm_temp := os.environ.get("LLM_TEMPERATURE"):
        config.llm.temperature = float(llm_temp)
    if llm_tokens := os.environ.get("LLM_MAX_TOKENS"):
        config.llm.max_tokens = int(llm_tokens)

    if log_level := os.environ.get("LOG_LEVEL"):
        config.logging.level = log_level

    return config


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    """Get the cached configuration instance.

    Returns:
        Application configuration.
    """
    return load_config()


DEFAULT_DATA_DIR = "src/backend/data/raw/csv"
NBA_DEFAULT_SEASON = os.environ.get("NBA_API_DEFAULT_SEASON", get_current_nba_season())
ENTITY_SAMPLE_SIZE = 1000
SEARCH_SAMPLE_SIZE = 1000
CSV_EXECUTION_TIMEOUT = 30
API_EXECUTION_TIMEOUT = 60
CHART_HISTORY_LIMIT = 10
CHART_ROW_LIMIT = 10
RAW_RESULT_TRUNCATION = 5000
ANALYSIS_TRUNCATION = 2000
EXEC_RESULT_TRUNCATION = 3000
"""
Constants governing prompt construction.
"""
MAX_PLAN_STEPS = 6
MIN_PLAN_STEPS = 4
SUCCESSFUL_PATTERN_LIMIT = 10

# Season format pattern (e.g., "2023-24")
_SEASON_PATTERN = re.compile(r"^\d{4}-\d{2}$")

# Configuration values that must be positive integers
_POSITIVE_INT_CONFIGS: dict[str, int] = {
    "CSV_EXECUTION_TIMEOUT": CSV_EXECUTION_TIMEOUT,
    "API_EXECUTION_TIMEOUT": API_EXECUTION_TIMEOUT,
    "ENTITY_SAMPLE_SIZE": ENTITY_SAMPLE_SIZE,
    "SEARCH_SAMPLE_SIZE": SEARCH_SAMPLE_SIZE,
    "CHART_HISTORY_LIMIT": CHART_HISTORY_LIMIT,
    "CHART_ROW_LIMIT": CHART_ROW_LIMIT,
    "RAW_RESULT_TRUNCATION": RAW_RESULT_TRUNCATION,
    "ANALYSIS_TRUNCATION": ANALYSIS_TRUNCATION,
    "EXEC_RESULT_TRUNCATION": EXEC_RESULT_TRUNCATION,
    "MAX_PLAN_STEPS": MAX_PLAN_STEPS,
    "MIN_PLAN_STEPS": MIN_PLAN_STEPS,
    "SUCCESSFUL_PATTERN_LIMIT": SUCCESSFUL_PATTERN_LIMIT,
}


class ConfigurationError(Exception):
    """Raised when configuration validation fails."""


def validate_config() -> list[str]:
    """Validate all configuration values.

    Returns:
        List of validation error messages. Empty list if all validations pass.

    Raises:
        ConfigurationError: If any validation fails and raise_on_error is True
            (called via validate_config_or_raise).
    """
    errors: list[str] = []

    # Validate all positive integer configs
    for name, value in _POSITIVE_INT_CONFIGS.items():
        if value <= 0:
            errors.append(f"{name} must be positive, got {value}")

    # Validate season format if provided
    if NBA_DEFAULT_SEASON and not _SEASON_PATTERN.match(NBA_DEFAULT_SEASON):
        errors.append(
            f"NBA_DEFAULT_SEASON must match format 'YYYY-YY' (e.g., '2023-24'), "
            f"got '{NBA_DEFAULT_SEASON}'",
        )

    # Validate plan step constraints
    if MIN_PLAN_STEPS > MAX_PLAN_STEPS:
        errors.append(
            f"MIN_PLAN_STEPS ({MIN_PLAN_STEPS}) cannot be greater than "
            f"MAX_PLAN_STEPS ({MAX_PLAN_STEPS})",
        )

    return errors


def validate_config_or_raise() -> None:
    """Validate configuration and raise an exception if validation fails.

    Raises:
        ConfigurationError: If any configuration value is invalid.
    """
    errors = validate_config()
    if errors:
        raise ConfigurationError(
            "Configuration validation failed:\n  - " + "\n  - ".join(errors),
        )
