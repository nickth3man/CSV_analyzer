"""Data validation and quality checks for NBA data.

This module provides validation functions to ensure data integrity
and quality when populating NBA data from the API.
"""

import logging
from datetime import datetime
from typing import Any

import pandas as pd


logger = logging.getLogger(__name__)


class DataValidator:
    """Validates NBA data for quality and consistency."""

    def __init__(self) -> None:
        """Initialize the data validator."""
        self.validation_errors: list[str] = []

    def validate_player_data(self, df: pd.DataFrame) -> dict[str, Any]:
        """Validate player data.

        Args:
            df: DataFrame with player data

        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []

        # Check required columns
        required_cols = ["id", "full_name", "first_name", "last_name", "is_active"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")

        # Check for duplicates
        duplicates = df[df.duplicated(subset=["id"], keep=False)]
        if not duplicates.empty:
            errors.append(f"Found {len(duplicates)} duplicate player IDs")

        # Check for invalid IDs
        invalid_ids = df[df["id"].isna() | (df["id"] <= 0)]
        if not invalid_ids.empty:
            errors.append(f"Found {len(invalid_ids)} invalid player IDs")

        # Check for missing names
        missing_names = df[df["full_name"].isna() | (df["full_name"] == "")]
        if not missing_names.empty:
            warnings.append(f"Found {len(missing_names)} players with missing names")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "total_records": len(df),
        }

    def validate_game_data(self, df: pd.DataFrame) -> dict[str, Any]:
        """Validate game log data.

        Args:
            df: DataFrame with game log data

        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []

        # Check required columns
        required_cols = ["game_id", "player_id", "team_id", "game_date", "pts"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")

        # Check for invalid game IDs
        invalid_game_ids = df[df["game_id"].isna() | (df["game_id"] == "")]
        if not invalid_game_ids.empty:
            errors.append(f"Found {len(invalid_game_ids)} invalid game IDs")

        # Check for impossible stats
        negative_stats = df[
            (df["pts"] < 0) | (df["fgm"] < 0) | (df["fga"] < 0) | (df["min"] < 0)
        ]
        if not negative_stats.empty:
            errors.append(f"Found {len(negative_stats)} records with negative stats")

        # Check for unrealistic minutes
        if "min" in df.columns:
            # Convert minutes to numeric for comparison
            df_min = df.copy()
            df_min["min_numeric"] = pd.to_numeric(df_min["min"], errors="coerce")
            unrealistic_min = df_min[df_min["min_numeric"] > 60]  # More than 60 minutes
            if not unrealistic_min.empty:
                warnings.append(
                    f"Found {len(unrealistic_min)} records with >60 minutes",
                )

        # Check for games in the future
        today = datetime.now().date()
        future_games = df[pd.to_datetime(df["game_date"]).dt.date > today]
        if not future_games.empty:
            warnings.append(f"Found {len(future_games)} games in the future")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "total_records": len(df),
        }

    def validate_boxscore_data(self, df: pd.DataFrame) -> dict[str, Any]:
        """Validate box score data.

        Args:
            df: DataFrame with box score data

        Returns:
            Dictionary with validation results
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Check required columns
        required_cols = ["game_id", "team_id", "person_id", "points"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")

        # Check field goal percentages
        if all(
            col in df.columns
            for col in ["field_goals_percentage", "field_goals_attempted"]
        ):
            # Only check when attempts > 0
            attempted_shots = df[df["field_goals_attempted"] > 0]
            invalid_fg_pct = attempted_shots[
                (attempted_shots["field_goals_percentage"] < 0)
                | (attempted_shots["field_goals_percentage"] > 100)
            ]
            if not invalid_fg_pct.empty:
                errors.append(
                    f"Found {len(invalid_fg_pct)} invalid field goal percentages",
                )

        # Check three point percentages
        if all(
            col in df.columns
            for col in ["three_pointers_percentage", "three_pointers_attempted"]
        ):
            attempted_3pt = df[df["three_pointers_attempted"] > 0]
            invalid_3pt_pct = attempted_3pt[
                (attempted_3pt["three_pointers_percentage"] < 0)
                | (attempted_3pt["three_pointers_percentage"] > 100)
            ]
            if not invalid_3pt_pct.empty:
                errors.append(f"Found {len(invalid_3pt_pct)} invalid 3PT percentages")

        # Check free throw percentages
        if all(
            col in df.columns
            for col in ["free_throws_percentage", "free_throws_attempted"]
        ):
            attempted_ft = df[df["free_throws_attempted"] > 0]
            invalid_ft_pct = attempted_ft[
                (attempted_ft["free_throws_percentage"] < 0)
                | (attempted_ft["free_throws_percentage"] > 100)
            ]
            if not invalid_ft_pct.empty:
                errors.append(
                    f"Found {len(invalid_ft_pct)} invalid free throw percentages",
                )

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "total_records": len(df),
        }

    def validate_season_data(self, season: str, df: pd.DataFrame) -> dict[str, Any]:
        """Validate that data belongs to the expected season.

        Args:
            season: Expected season (e.g., "2023-24")
            df: DataFrame with data

        Returns:
            Dictionary with validation results
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Extract year from season
        try:
            start_year = int(season.split("-")[0])
            expected_years = [start_year, start_year + 1]
        except (ValueError, IndexError):
            errors.append(f"Invalid season format: {season}")
            return {"valid": False, "errors": errors, "warnings": warnings}

        # Check game dates if available
        if "game_date" in df.columns:
            df_dates = pd.to_datetime(df["game_date"], errors="coerce")
            game_years = df_dates.dt.year.dropna().unique()

            unexpected_years = [
                year for year in game_years if year not in expected_years
            ]
            if unexpected_years:
                warnings.append(f"Found games in unexpected years: {unexpected_years}")

        # Check season ID if available
        if "season_id" in df.columns:
            season_ids = df["season_id"].dropna().unique()
            expected_season_id = f"{start_year}"

            unexpected_seasons = [
                sid for sid in season_ids if not str(sid).startswith(expected_season_id)
            ]
            if unexpected_seasons:
                warnings.append(f"Found unexpected season IDs: {unexpected_seasons}")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
        }

    def validate_data_completeness(
        self, df: pd.DataFrame, expected_fields: list[str],
    ) -> dict[str, Any]:
        """Validate data completeness by checking for missing values.

        Args:
            df: DataFrame to validate
            expected_fields: List of expected field names

        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []

        # Check for missing fields
        missing_fields = [field for field in expected_fields if field not in df.columns]
        if missing_fields:
            errors.append(f"Missing expected fields: {missing_fields}")

        # Check for null values in critical fields
        critical_fields = [field for field in expected_fields if field in df.columns]
        null_counts = df[critical_fields].isna().sum()

        completeness_report = {}
        for field in critical_fields:
            null_count = null_counts[field]
            null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
            completeness_report[field] = {
                "null_count": null_count,
                "null_percentage": null_pct,
            }

            # Flag fields with high null percentages
            if null_pct > 50:
                warnings.append(f"Field '{field}' has {null_pct:.1f}% null values")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "completeness": completeness_report,
            "overall_completeness": 1
            - (null_counts.sum() / (len(df) * len(critical_fields))),
        }

    def validate_statistical_consistency(self, df: pd.DataFrame) -> dict[str, Any]:
        """Validate statistical consistency across related fields.

        Args:
            df: DataFrame with statistical data

        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []

        # Check field goals made vs attempted
        if all(col in df.columns for col in ["fgm", "fga"]):
            invalid_fg = df[df["fgm"] > df["fga"]]
            if not invalid_fg.empty:
                errors.append(f"Found {len(invalid_fg)} records with FGM > FGA")

        # Check three pointers made vs attempted
        if all(col in df.columns for col in ["fg3m", "fg3a"]):
            invalid_fg3 = df[df["fg3m"] > df["fg3a"]]
            if not invalid_fg3.empty:
                errors.append(f"Found {len(invalid_fg3)} records with FG3M > FG3A")

        # Check free throws made vs attempted
        if all(col in df.columns for col in ["ftm", "fta"]):
            invalid_ft = df[df["ftm"] > df["fta"]]
            if not invalid_ft.empty:
                errors.append(f"Found {len(invalid_ft)} records with FTM > FTA")

        # Check rebound consistency
        if all(col in df.columns for col in ["oreb", "dreb", "reb"]):
            # REB should generally equal OREB + DREB (allowing for small discrepancies)
            reb_diff = abs(df["reb"] - (df["oreb"] + df["dreb"]))
            inconsistent_reb = df[reb_diff > 2]  # Allow 2 rebound difference
            if not inconsistent_reb.empty:
                warnings.append(
                    f"Found {len(inconsistent_reb)} records with inconsistent rebound totals",
                )

        # Check for zero-minute players with stats
        if "min" in df.columns:
            zero_min_with_stats = df[
                (df["min"] == 0)
                & (
                    (df.get("pts", 0) > 0)
                    | (df.get("reb", 0) > 0)
                    | (df.get("ast", 0) > 0)
                )
            ]
            if not zero_min_with_stats.empty:
                warnings.append(
                    f"Found {len(zero_min_with_stats)} records with 0 minutes but positive stats",
                )

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
        }

    def generate_validation_report(
        self, data_type: str, df: pd.DataFrame, **kwargs,
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

        report = {
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
                df, kwargs["expected_fields"],
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
    errors: list[str] = []
    warnings: list[str] = []

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
        (player_stats["pts_std"] > 20)  # High point variation
        | (player_stats["reb_std"] > 10)  # High rebound variation
        | (player_stats["ast_std"] > 8)  # High assist variation
    ]

    if not high_variation_players.empty:
        warnings.append(
            f"Found {len(high_variation_players)} players with high statistical variation",
        )

    # Check for players with very few games but high stats (potential data errors)
    low_game_high_stats = player_stats[
        (player_stats["pts_count"] < 5) & (player_stats["pts_mean"] > 25)
    ]

    if not low_game_high_stats.empty:
        warnings.append(
            f"Found {len(low_game_high_stats)} players with few games but high scoring averages",
        )

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "player_count": len(player_stats),
        "high_variation_count": len(high_variation_players),
    }
