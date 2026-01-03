"""Data validation and quality checks for NBA data.

This module provides validation functions to ensure data integrity
and quality when populating NBA data from the API. It includes:

- Schema-aware field validation
- Statistical consistency checks
- Data completeness validation
- Type-specific validators for different data types

Usage:
    from src.scripts.populate.validation import DataValidator, ValidationConfig

    validator = DataValidator()
    report = validator.generate_validation_report(
        data_type="games",
        df=game_data,
        expected_fields=PLAYER_GAME_STATS_COLUMNS,
    )
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

import pandas as pd

from src.scripts.populate.constants import (
    INTEGER_STAT_COLUMNS,
    PERCENTAGE_COLUMNS,
    ValidationThresholds,
)


if TYPE_CHECKING:
    from collections.abc import Sequence


logger = logging.getLogger(__name__)


# =============================================================================
# VALIDATION TYPES AND CONFIGURATION
# =============================================================================


class ValidationSeverity(str, Enum):
    """Severity level of validation issues."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """Represents a single validation issue.

    Attributes:
        message: Human-readable description of the issue.
        severity: Severity level (error, warning, info).
        field: Optional field name that caused the issue.
        count: Number of affected records.
        sample_ids: Sample of affected record IDs for debugging.
    """

    message: str
    severity: ValidationSeverity
    field_name: str | None = None
    count: int = 0
    sample_ids: list[Any] = dataclass_field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "message": self.message,
            "severity": self.severity.value,
            "field": self.field_name,
            "count": self.count,
            "sample_ids": self.sample_ids[:5],  # Limit sample size
        }


@dataclass
class ValidationConfig:
    """Configuration for data validation.

    Attributes:
        max_minutes: Maximum valid minutes per game.
        max_points: Maximum valid points per game.
        max_rebounds: Maximum valid rebounds per game.
        max_assists: Maximum valid assists per game.
        high_null_threshold: Percentage threshold for null warnings.
        allow_future_dates: Whether to allow future game dates.
        strict_mode: If True, treat warnings as errors.
    """

    max_minutes: int = ValidationThresholds.MAX_MINUTES_PER_GAME
    max_points: int = ValidationThresholds.MAX_POINTS_PER_GAME
    max_rebounds: int = ValidationThresholds.MAX_REBOUNDS_PER_GAME
    max_assists: int = ValidationThresholds.MAX_ASSISTS_PER_GAME
    high_null_threshold: float = ValidationThresholds.HIGH_NULL_PERCENTAGE
    allow_future_dates: bool = False
    strict_mode: bool = False


@dataclass
class ValidationResult:
    """Result of a validation operation.

    Attributes:
        valid: Overall validation status.
        issues: List of validation issues found.
        record_count: Total records validated.
        metadata: Additional metadata about the validation.
    """

    valid: bool = True
    issues: list[ValidationIssue] = dataclass_field(default_factory=list)
    record_count: int = 0
    metadata: dict[str, Any] = dataclass_field(default_factory=dict)

    @property
    def errors(self) -> list[str]:
        """Return list of error messages."""
        return [
            i.message for i in self.issues if i.severity == ValidationSeverity.ERROR
        ]

    @property
    def warnings(self) -> list[str]:
        """Return list of warning messages."""
        return [
            i.message for i in self.issues if i.severity == ValidationSeverity.WARNING
        ]

    def add_issue(
        self,
        message: str,
        severity: ValidationSeverity,
        *,
        field_name: str | None = None,
        count: int = 0,
        sample_ids: list[Any] | None = None,
    ) -> None:
        """Add a validation issue."""
        self.issues.append(
            ValidationIssue(
                message=message,
                severity=severity,
                field_name=field_name,
                count=count,
                sample_ids=sample_ids or [],
            )
        )
        if severity == ValidationSeverity.ERROR:
            self.valid = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format for compatibility."""
        return {
            "valid": self.valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "total_records": self.record_count,
            "issues": [i.to_dict() for i in self.issues],
            "metadata": self.metadata,
        }


# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================


def validate_required_columns(
    df: pd.DataFrame,
    required: Sequence[str],
) -> ValidationResult:
    """Validate that required columns exist in DataFrame.

    Args:
        df: DataFrame to validate.
        required: List of required column names.

    Returns:
        ValidationResult with any missing column errors.
    """
    result = ValidationResult(record_count=len(df))
    missing = [col for col in required if col not in df.columns]

    if missing:
        result.add_issue(
            f"Missing required columns: {missing}",
            ValidationSeverity.ERROR,
            count=len(missing),
        )

    return result


def validate_no_duplicates(
    df: pd.DataFrame,
    key_columns: Sequence[str],
) -> ValidationResult:
    """Validate that there are no duplicate rows based on key columns.

    Args:
        df: DataFrame to validate.
        key_columns: Columns that should be unique together.

    Returns:
        ValidationResult with duplicate errors if found.
    """
    result = ValidationResult(record_count=len(df))

    # Check if key columns exist
    existing_keys = [col for col in key_columns if col in df.columns]
    if not existing_keys:
        return result

    duplicates = df[df.duplicated(subset=existing_keys, keep=False)]
    if not duplicates.empty:
        # Get sample of duplicate IDs
        sample_ids = (
            duplicates[existing_keys[0]].head(5).tolist() if existing_keys else []
        )
        result.add_issue(
            f"Found {len(duplicates)} duplicate records based on {existing_keys}",
            ValidationSeverity.ERROR,
            count=len(duplicates),
            sample_ids=sample_ids,
        )

    return result


def validate_non_negative(
    df: pd.DataFrame,
    columns: Sequence[str] | None = None,
) -> ValidationResult:
    """Validate that numeric columns are non-negative.

    Args:
        df: DataFrame to validate.
        columns: Columns to check, or None to check INTEGER_STAT_COLUMNS.

    Returns:
        ValidationResult with any negative value errors.
    """
    result = ValidationResult(record_count=len(df))

    # Default to integer stat columns
    check_cols = columns if columns is not None else list(INTEGER_STAT_COLUMNS)

    for col in check_cols:
        if col not in df.columns:
            continue

        # Convert to numeric for comparison
        numeric_col = pd.to_numeric(df[col], errors="coerce")
        negative_mask = numeric_col < 0

        if negative_mask.any():
            count = negative_mask.sum()
            result.add_issue(
                f"Found {count} negative values in '{col}'",
                ValidationSeverity.ERROR,
                field_name=col,
                count=count,
            )

    return result


def validate_percentage_range(
    df: pd.DataFrame,
    columns: Sequence[str] | None = None,
) -> ValidationResult:
    """Validate that percentage columns are in valid range [0, 1] or [0, 100].

    Args:
        df: DataFrame to validate.
        columns: Columns to check, or None to check PERCENTAGE_COLUMNS.

    Returns:
        ValidationResult with any out-of-range errors.
    """
    result = ValidationResult(record_count=len(df))

    check_cols = columns if columns is not None else list(PERCENTAGE_COLUMNS)

    for col in check_cols:
        if col not in df.columns:
            continue

        numeric_col = pd.to_numeric(df[col], errors="coerce")

        # Check if values are > 100 (could be 0-1 or 0-100 format)
        # Allow 0-1 range or 0-100 range
        invalid_mask = (numeric_col < 0) | (numeric_col > 100)

        if invalid_mask.any():
            count = invalid_mask.sum()
            result.add_issue(
                f"Found {count} out-of-range percentage values in '{col}'",
                ValidationSeverity.ERROR,
                field_name=col,
                count=count,
            )

    return result


def validate_made_vs_attempted(
    df: pd.DataFrame,
    made_col: str,
    attempted_col: str,
) -> ValidationResult:
    """Validate that made <= attempted for shooting stats.

    Args:
        df: DataFrame to validate.
        made_col: Column name for made shots.
        attempted_col: Column name for attempted shots.

    Returns:
        ValidationResult with any inconsistency errors.
    """
    result = ValidationResult(record_count=len(df))

    if made_col not in df.columns or attempted_col not in df.columns:
        return result

    made = pd.to_numeric(df[made_col], errors="coerce")
    attempted = pd.to_numeric(df[attempted_col], errors="coerce")

    invalid_mask = made > attempted

    if invalid_mask.any():
        count = invalid_mask.sum()
        result.add_issue(
            f"Found {count} records with {made_col} > {attempted_col}",
            ValidationSeverity.ERROR,
            count=count,
        )

    return result


# =============================================================================
# DATA VALIDATOR CLASS
# =============================================================================


class DataValidator:
    """Validates NBA data for quality and consistency.

    This class provides a unified interface for validating different types
    of NBA data with configurable thresholds and validation rules.
    """

    def __init__(self, config: ValidationConfig | None = None) -> None:
        """Initialize the data validator.

        Args:
            config: Optional validation configuration. Uses defaults if None.
        """
        self.config = config or ValidationConfig()
        self.validation_errors: list[str] = []

    def validate_player_data(self, df: pd.DataFrame) -> dict[str, Any]:
        """Validate player data.

        Args:
            df: DataFrame with player data

        Returns:
            Dictionary with validation results
        """
        result = ValidationResult(record_count=len(df))

        # Check required columns
        required_cols = ["id", "full_name", "first_name", "last_name", "is_active"]
        req_result = validate_required_columns(df, required_cols)
        result.issues.extend(req_result.issues)

        # Check for duplicates
        dup_result = validate_no_duplicates(df, ["id"])
        result.issues.extend(dup_result.issues)

        # Check for invalid IDs
        if "id" in df.columns:
            invalid_ids = df[df["id"].isna() | (df["id"] <= 0)]
            if not invalid_ids.empty:
                result.add_issue(
                    f"Found {len(invalid_ids)} invalid player IDs",
                    ValidationSeverity.ERROR,
                    field_name="id",
                    count=len(invalid_ids),
                )

        # Check for missing names
        if "full_name" in df.columns:
            missing_names = df[df["full_name"].isna() | (df["full_name"] == "")]
            if not missing_names.empty:
                result.add_issue(
                    f"Found {len(missing_names)} players with missing names",
                    ValidationSeverity.WARNING,
                    field_name="full_name",
                    count=len(missing_names),
                )

        result.valid = len(result.errors) == 0
        return result.to_dict()

    def validate_game_data(self, df: pd.DataFrame) -> dict[str, Any]:
        """Validate game log data.

        Args:
            df: DataFrame with game log data

        Returns:
            Dictionary with validation results
        """
        result = ValidationResult(record_count=len(df))

        # Check required columns
        required_cols = ["game_id", "player_id", "team_id", "game_date", "pts"]
        req_result = validate_required_columns(df, required_cols)
        result.issues.extend(req_result.issues)

        # Check for invalid game IDs
        if "game_id" in df.columns:
            invalid_game_ids = df[df["game_id"].isna() | (df["game_id"] == "")]
            if not invalid_game_ids.empty:
                result.add_issue(
                    f"Found {len(invalid_game_ids)} invalid game IDs",
                    ValidationSeverity.ERROR,
                    field_name="game_id",
                    count=len(invalid_game_ids),
                )

        # Check for negative stats using centralized function
        neg_result = validate_non_negative(df, ["pts", "fgm", "fga", "min"])
        result.issues.extend(neg_result.issues)

        # Check for unrealistic minutes
        if "min" in df.columns:
            df_min = df.copy()
            df_min["min_numeric"] = pd.to_numeric(df_min["min"], errors="coerce")
            unrealistic_min = df_min[df_min["min_numeric"] > self.config.max_minutes]
            if not unrealistic_min.empty:
                result.add_issue(
                    f"Found {len(unrealistic_min)} records with >{self.config.max_minutes} minutes",
                    ValidationSeverity.WARNING,
                    field_name="min",
                    count=len(unrealistic_min),
                )

        # Check for games in the future
        if "game_date" in df.columns and not self.config.allow_future_dates:
            today = datetime.now().date()
            future_games = df[pd.to_datetime(df["game_date"]).dt.date > today]
            if not future_games.empty:
                result.add_issue(
                    f"Found {len(future_games)} games in the future",
                    ValidationSeverity.WARNING,
                    field_name="game_date",
                    count=len(future_games),
                )

        result.valid = len(result.errors) == 0
        return result.to_dict()

    def validate_boxscore_data(self, df: pd.DataFrame) -> dict[str, Any]:
        """Validate box score data.

        Args:
            df: DataFrame with box score data

        Returns:
            Dictionary with validation results
        """
        result = ValidationResult(record_count=len(df))

        # Check required columns
        required_cols = ["game_id", "team_id", "person_id", "points"]
        req_result = validate_required_columns(df, required_cols)
        result.issues.extend(req_result.issues)

        # Check field goal percentages
        pct_result = validate_percentage_range(
            df,
            [
                "field_goals_percentage",
                "three_pointers_percentage",
                "free_throws_percentage",
            ],
        )
        result.issues.extend(pct_result.issues)

        result.valid = len(result.errors) == 0
        return result.to_dict()

    def validate_season_data(self, season: str, df: pd.DataFrame) -> dict[str, Any]:
        """Validate that data belongs to the expected season.

        Args:
            season: Expected season (e.g., "2023-24")
            df: DataFrame with data

        Returns:
            Dictionary with validation results
        """
        result = ValidationResult(record_count=len(df))

        # Extract year from season
        try:
            start_year = int(season.split("-")[0])
            expected_years = [start_year, start_year + 1]
        except (ValueError, IndexError):
            result.add_issue(
                f"Invalid season format: {season}",
                ValidationSeverity.ERROR,
            )
            return result.to_dict()

        # Check game dates if available
        if "game_date" in df.columns:
            df_dates = pd.to_datetime(df["game_date"], errors="coerce")
            game_years = df_dates.dt.year.dropna().unique()

            unexpected_years = [
                year for year in game_years if year not in expected_years
            ]
            if unexpected_years:
                result.add_issue(
                    f"Found games in unexpected years: {unexpected_years}",
                    ValidationSeverity.WARNING,
                )

        # Check season ID if available
        if "season_id" in df.columns:
            season_ids = df["season_id"].dropna().unique()
            expected_season_id = f"{start_year}"

            unexpected_seasons = [
                sid for sid in season_ids if not str(sid).startswith(expected_season_id)
            ]
            if unexpected_seasons:
                result.add_issue(
                    f"Found unexpected season IDs: {unexpected_seasons}",
                    ValidationSeverity.WARNING,
                )

        result.valid = len(result.errors) == 0
        return result.to_dict()

    def validate_data_completeness(
        self,
        df: pd.DataFrame,
        expected_fields: list[str],
    ) -> dict[str, Any]:
        """Validate data completeness by checking for missing values.

        Args:
            df: DataFrame to validate
            expected_fields: List of expected field names

        Returns:
            Dictionary with validation results
        """
        result = ValidationResult(record_count=len(df))

        # Check for missing fields
        missing_fields = [field for field in expected_fields if field not in df.columns]
        if missing_fields:
            result.add_issue(
                f"Missing expected fields: {missing_fields}",
                ValidationSeverity.ERROR,
                count=len(missing_fields),
            )

        # Check for null values in critical fields
        critical_fields = [field for field in expected_fields if field in df.columns]
        if not critical_fields:
            base_result = result.to_dict()
            base_result.update(
                {
                    "completeness": {},
                    "overall_completeness": 1.0,
                }
            )
            return base_result

        null_counts = df[critical_fields].isna().sum()

        completeness_report = {}
        for field_name in critical_fields:
            null_count = null_counts[field_name]
            null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
            completeness_report[field_name] = {
                "null_count": int(null_count),
                "null_percentage": round(null_pct, 2),
            }

            # Flag fields with high null percentages
            if null_pct > self.config.high_null_threshold:
                result.add_issue(
                    f"Field '{field_name}' has {null_pct:.1f}% null values",
                    ValidationSeverity.WARNING,
                    field_name=field_name,
                    count=int(null_count),
                )

        overall_completeness = 1 - (
            null_counts.sum() / (len(df) * len(critical_fields))
        )

        final_result = result.to_dict()
        final_result.update(
            {
                "completeness": completeness_report,
                "overall_completeness": round(overall_completeness, 4),
            }
        )
        return final_result

    def validate_statistical_consistency(self, df: pd.DataFrame) -> dict[str, Any]:
        """Validate statistical consistency across related fields.

        Args:
            df: DataFrame with statistical data

        Returns:
            Dictionary with validation results
        """
        result = ValidationResult(record_count=len(df))

        # Field goals made vs attempted
        fg_result = validate_made_vs_attempted(df, "fgm", "fga")
        result.issues.extend(fg_result.issues)

        # Three pointers made vs attempted
        fg3_result = validate_made_vs_attempted(df, "fg3m", "fg3a")
        result.issues.extend(fg3_result.issues)

        # Free throws made vs attempted
        ft_result = validate_made_vs_attempted(df, "ftm", "fta")
        result.issues.extend(ft_result.issues)

        # Check rebound consistency
        if all(col in df.columns for col in ["oreb", "dreb", "reb"]):
            oreb = pd.to_numeric(df["oreb"], errors="coerce")
            dreb = pd.to_numeric(df["dreb"], errors="coerce")
            reb = pd.to_numeric(df["reb"], errors="coerce")
            reb_diff = abs(reb - (oreb + dreb))
            inconsistent_reb = df[reb_diff > 2]  # Allow 2 rebound difference
            if not inconsistent_reb.empty:
                result.add_issue(
                    f"Found {len(inconsistent_reb)} records with inconsistent rebound totals",
                    ValidationSeverity.WARNING,
                    count=len(inconsistent_reb),
                )

        # Check for zero-minute players with stats
        if "min" in df.columns:
            min_numeric = pd.to_numeric(df["min"], errors="coerce")
            has_pts = pd.to_numeric(df.get("pts", 0), errors="coerce") > 0
            has_reb = pd.to_numeric(df.get("reb", 0), errors="coerce") > 0
            has_ast = pd.to_numeric(df.get("ast", 0), errors="coerce") > 0

            zero_min_with_stats = df[(min_numeric == 0) & (has_pts | has_reb | has_ast)]
            if not zero_min_with_stats.empty:
                result.add_issue(
                    f"Found {len(zero_min_with_stats)} records with 0 minutes but positive stats",
                    ValidationSeverity.WARNING,
                    count=len(zero_min_with_stats),
                )

        result.valid = len(result.errors) == 0
        return result.to_dict()

    def generate_validation_report(
        self,
        data_type: str,
        df: pd.DataFrame,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Generate a comprehensive validation report.

        Args:
            data_type: Type of data ('players', 'games', 'boxscores', etc.)
            df: DataFrame to validate
            **kwargs: Additional validation parameters

        Returns:
            Comprehensive validation report
        """
        logger.info(f"Generating validation report for {data_type} data")

        report: dict[str, Any] = {
            "data_type": data_type,
            "timestamp": datetime.now().isoformat(),
            "record_count": len(df),
            "column_count": len(df.columns),
        }

        # Run appropriate validations based on data type
        if data_type == "players":
            validation_result = self.validate_player_data(df)
        elif data_type == "games":
            validation_result = self.validate_game_data(df)
        elif data_type == "boxscores":
            validation_result = self.validate_boxscore_data(df)
        else:
            validation_result = {"valid": True, "errors": [], "warnings": []}

        report.update(validation_result)

        # Additional validations
        if "expected_fields" in kwargs:
            completeness_result = self.validate_data_completeness(
                df,
                kwargs["expected_fields"],
            )
            report["completeness"] = completeness_result

        if "season" in kwargs:
            season_result = self.validate_season_data(kwargs["season"], df)
            report["season_validation"] = season_result

        # Statistical consistency check
        consistency_result = self.validate_statistical_consistency(df)
        report["statistical_consistency"] = consistency_result

        # Overall validation status
        all_validations = [
            validation_result.get("valid", True),
            consistency_result.get("valid", True),
        ]

        completeness = report.get("completeness")
        if completeness is not None and isinstance(completeness, dict):
            all_validations.append(completeness.get("valid", True))

        report["overall_valid"] = all(all_validations)

        # Log summary
        if report["overall_valid"]:
            logger.info(f"Validation passed for {data_type}: {len(df)} records")
        else:
            logger.error(
                f"Validation failed for {data_type}: {len(validation_result.get('errors', []))} errors",
            )

        return report


def validate_player_stats_consistency(player_games: pd.DataFrame) -> dict[str, Any]:
    """Validate consistency of player statistics across games.

    Args:
        player_games: DataFrame with player game statistics

    Returns:
        Dictionary with consistency check results
    """
    result = ValidationResult(record_count=len(player_games))

    # Group by player and calculate per-game averages
    player_stats = (
        player_games.groupby("player_id")
        .agg(
            {
                "pts": ["mean", "std", "count"],
                "reb": ["mean", "std"],
                "ast": ["mean", "std"],
                "min": ["mean", "std"],
            },
        )
        .round(2)
    )

    # Flatten column names
    player_stats.columns = ["_".join(col).strip() for col in player_stats.columns]

    # Check for players with high standard deviations (potential outliers)
    high_variation_players = player_stats[
        (player_stats["pts_std"] > ValidationThresholds.HIGH_POINTS_STD)
        | (player_stats["reb_std"] > ValidationThresholds.HIGH_REBOUNDS_STD)
        | (player_stats["ast_std"] > ValidationThresholds.HIGH_ASSISTS_STD)
    ]

    if len(high_variation_players) > 0:
        result.add_issue(
            f"Found {len(high_variation_players)} players with high statistical variation",
            ValidationSeverity.WARNING,
            count=len(high_variation_players),
        )

    # Check for players with very few games but high stats (potential data errors)
    low_game_high_stats = player_stats[
        (player_stats["pts_count"] < ValidationThresholds.MIN_GAMES_FOR_SEASON_STATS)
        & (player_stats["pts_mean"] > 25)
    ]

    if len(low_game_high_stats) > 0:
        result.add_issue(
            f"Found {len(low_game_high_stats)} players with few games but high scoring averages",
            ValidationSeverity.WARNING,
            count=len(low_game_high_stats),
        )

    final_result = result.to_dict()
    final_result.update(
        {
            "player_count": len(player_stats),
            "high_variation_count": len(high_variation_players),
        }
    )
    return final_result
