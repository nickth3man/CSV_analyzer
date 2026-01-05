"""Populate NBA standings from Basketball Reference.

This module fetches NBA standings from Basketball Reference using the
basketball_reference_web_scraper library.

Usage:
    from src.scripts.populate.populate_br_standings import populate_br_standings

    # Populate current season
    result = populate_br_standings(seasons=[2025])

    # Populate multiple seasons
    result = populate_br_standings(seasons=[2024, 2023, 2022])

    # Populate a range of historical seasons
    result = populate_br_standings(start_year=2015, end_year=2024)
"""

import argparse
import logging
import time
from typing import Any

import duckdb
import pandas as pd
from basketball_reference_web_scraper import client as br_client

from src.scripts.populate.base import PopulationMetrics, ProgressTracker
from src.scripts.populate.config import CURRENT_SEASON, get_db_path
from src.scripts.populate.helpers import configure_logging


logger = logging.getLogger(__name__)


# Column mapping from BR standings data to our schema
BR_STANDINGS_COLUMNS = {
    "team": "team",
    "wins": "wins",
    "losses": "losses",
    "win_percentage": "win_percentage",
    "games_behind": "games_behind",
    "playoff_seed": "playoff_seed",
    "conference": "conference",
    "division": "division",
    "points_for": "points_for",
    "points_against": "points_against",
    "point_differential": "point_differential",
    "home_wins": "home_wins",
    "home_losses": "home_losses",
    "away_wins": "away_wins",
    "away_losses": "away_losses",
    "streak": "streak",
    "last_10_record": "last_10_record",
}


def convert_team_enum(team_value: Any) -> str:
    """Convert Basketball Reference Team enum to string.

    Args:
        team_value: Team enum value or string from BR.

    Returns:
        Team name as string (e.g., 'BOSTON_CELTICS').
    """
    if team_value is None:
        return ""

    # Handle enum objects
    if hasattr(team_value, "value"):
        return str(team_value.value)
    if hasattr(team_value, "name"):
        return str(team_value.name)

    return str(team_value)


def get_team_display_name(team_value: Any) -> str:
    """Get a human-readable team name from BR Team enum.

    Args:
        team_value: Team enum value or string from BR.

    Returns:
        Human-readable team name (e.g., 'Boston Celtics').
    """
    team_str = convert_team_enum(team_value)
    if not team_str:
        return ""

    # Convert BOSTON_CELTICS to Boston Celtics
    return team_str.replace("_", " ").title()


def extract_conference(team_value: Any) -> str | None:
    """Extract conference from team enum if available.

    Basketball Reference standings endpoint may include conference info
    in the team data or return separate conference standings.

    Args:
        team_value: Team data from BR.

    Returns:
        Conference name ('Eastern' or 'Western') or None.
    """
    # Map teams to conferences (this is a static mapping since BR doesn't always provide it)
    eastern_teams = {
        "ATLANTA_HAWKS",
        "BOSTON_CELTICS",
        "BROOKLYN_NETS",
        "CHARLOTTE_HORNETS",
        "CHICAGO_BULLS",
        "CLEVELAND_CAVALIERS",
        "DETROIT_PISTONS",
        "INDIANA_PACERS",
        "MIAMI_HEAT",
        "MILWAUKEE_BUCKS",
        "NEW_YORK_KNICKS",
        "ORLANDO_MAGIC",
        "PHILADELPHIA_76ERS",
        "TORONTO_RAPTORS",
        "WASHINGTON_WIZARDS",
    }

    western_teams = {
        "DALLAS_MAVERICKS",
        "DENVER_NUGGETS",
        "GOLDEN_STATE_WARRIORS",
        "HOUSTON_ROCKETS",
        "LOS_ANGELES_CLIPPERS",
        "LOS_ANGELES_LAKERS",
        "MEMPHIS_GRIZZLIES",
        "MINNESOTA_TIMBERWOLVES",
        "NEW_ORLEANS_PELICANS",
        "OKLAHOMA_CITY_THUNDER",
        "PHOENIX_SUNS",
        "PORTLAND_TRAIL_BLAZERS",
        "SACRAMENTO_KINGS",
        "SAN_ANTONIO_SPURS",
        "UTAH_JAZZ",
    }

    team_str = convert_team_enum(team_value)

    if team_str in eastern_teams:
        return "Eastern"
    if team_str in western_teams:
        return "Western"

    return None


def extract_division(team_value: Any) -> str | None:
    """Extract division from team enum.

    Args:
        team_value: Team data from BR.

    Returns:
        Division name or None.
    """
    # Map teams to divisions
    divisions = {
        # Atlantic
        "BOSTON_CELTICS": "Atlantic",
        "BROOKLYN_NETS": "Atlantic",
        "NEW_YORK_KNICKS": "Atlantic",
        "PHILADELPHIA_76ERS": "Atlantic",
        "TORONTO_RAPTORS": "Atlantic",
        # Central
        "CHICAGO_BULLS": "Central",
        "CLEVELAND_CAVALIERS": "Central",
        "DETROIT_PISTONS": "Central",
        "INDIANA_PACERS": "Central",
        "MILWAUKEE_BUCKS": "Central",
        # Southeast
        "ATLANTA_HAWKS": "Southeast",
        "CHARLOTTE_HORNETS": "Southeast",
        "MIAMI_HEAT": "Southeast",
        "ORLANDO_MAGIC": "Southeast",
        "WASHINGTON_WIZARDS": "Southeast",
        # Northwest
        "DENVER_NUGGETS": "Northwest",
        "MINNESOTA_TIMBERWOLVES": "Northwest",
        "OKLAHOMA_CITY_THUNDER": "Northwest",
        "PORTLAND_TRAIL_BLAZERS": "Northwest",
        "UTAH_JAZZ": "Northwest",
        # Pacific
        "GOLDEN_STATE_WARRIORS": "Pacific",
        "LOS_ANGELES_CLIPPERS": "Pacific",
        "LOS_ANGELES_LAKERS": "Pacific",
        "PHOENIX_SUNS": "Pacific",
        "SACRAMENTO_KINGS": "Pacific",
        # Southwest
        "DALLAS_MAVERICKS": "Southwest",
        "HOUSTON_ROCKETS": "Southwest",
        "MEMPHIS_GRIZZLIES": "Southwest",
        "NEW_ORLEANS_PELICANS": "Southwest",
        "SAN_ANTONIO_SPURS": "Southwest",
    }

    team_str = convert_team_enum(team_value)
    return divisions.get(team_str)


def transform_br_standings(df: pd.DataFrame, season_year: int) -> pd.DataFrame:
    """Transform BR standings data to match our schema.

    Args:
        df: Raw DataFrame from BR client (list of dicts converted to DataFrame).
        season_year: The season end year (e.g., 2024 for 2023-24 season).

    Returns:
        Transformed DataFrame ready for insertion.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # Create a copy to avoid modifying original
    result = df.copy()

    # Add season info
    result["season_year"] = season_year
    result["season_id"] = f"{season_year - 1}-{str(season_year)[2:]}"

    # Convert team enum to string and create display name
    if "team" in result.columns:
        result["team_name"] = result["team"].apply(get_team_display_name)
        result["team"] = result["team"].apply(convert_team_enum)

    # Add conference if not present
    if "conference" not in result.columns:
        result["conference"] = result["team"].apply(
            lambda x: extract_conference(x) if pd.notna(x) else None
        )

    # Add division if not present
    if "division" not in result.columns:
        result["division"] = result["team"].apply(
            lambda x: extract_division(x) if pd.notna(x) else None
        )

    # Handle numeric columns - convert to proper types
    numeric_cols = [
        "wins",
        "losses",
        "win_percentage",
        "games_behind",
        "points_for",
        "points_against",
        "point_differential",
        "home_wins",
        "home_losses",
        "away_wins",
        "away_losses",
    ]

    for col in numeric_cols:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")
        else:
            result[col] = None

    # Handle playoff_seed - might be None for teams not in playoffs
    if "playoff_seed" in result.columns:
        result["playoff_seed"] = pd.to_numeric(result["playoff_seed"], errors="coerce")
    else:
        result["playoff_seed"] = None

    # Handle streak column (might be string like "W3" or "L2")
    if "streak" not in result.columns:
        result["streak"] = None

    # Handle last_10_record (might be string like "7-3")
    if "last_10_record" not in result.columns:
        result["last_10_record"] = None

    # Calculate point differential if not present but we have pts_for/against
    has_pts_for = "points_for" in result.columns
    has_pts_against = "points_against" in result.columns
    needs_diff = (
        "point_differential" not in result.columns
        or result["point_differential"].isna().all()
    )
    if needs_diff and has_pts_for and has_pts_against:
        result["point_differential"] = result["points_for"].fillna(0) - result[
            "points_against"
        ].fillna(0)

    # Generate a unique key for deduplication
    # Format: BR_YYYY_TEAM
    def generate_standings_key(row):
        team = (row.get("team", "") or "").replace(" ", "_")[:25]
        return f"BR_{season_year}_{team}"

    result["standings_key"] = result.apply(generate_standings_key, axis=1)

    # Select and order columns for output
    output_cols = [
        "standings_key",
        "season_year",
        "season_id",
        "team",
        "team_name",
        "conference",
        "division",
        "wins",
        "losses",
        "win_percentage",
        "games_behind",
        "playoff_seed",
        "points_for",
        "points_against",
        "point_differential",
        "home_wins",
        "home_losses",
        "away_wins",
        "away_losses",
        "streak",
        "last_10_record",
    ]

    # Ensure all output columns exist
    for col in output_cols:
        if col not in result.columns:
            result[col] = None

    return result[output_cols]


def validate_standings_data(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """Validate the standings DataFrame.

    Args:
        df: Standings DataFrame to validate.

    Returns:
        Tuple of (is_valid, list of error messages).
    """
    errors = []

    if df is None or df.empty:
        errors.append("DataFrame is empty or None")
        return False, errors

    # Check required columns
    required_cols = ["season_year", "team", "wins", "losses"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")

    # Check for missing team values
    if "team" in df.columns:
        null_teams = df["team"].isna().sum()
        empty_teams = (df["team"] == "").sum()
        if null_teams + empty_teams > 0:
            errors.append(f"{null_teams + empty_teams} records have missing team")

    # Check for duplicate standings keys
    if "standings_key" in df.columns:
        dup_count = df.duplicated(subset=["standings_key"]).sum()
        if dup_count > 0:
            errors.append(f"{dup_count} duplicate standings_key values found")

    # Validate wins/losses are non-negative
    for col in ["wins", "losses"]:
        if col in df.columns:
            negative_values = (df[col].dropna() < 0).sum()
            if negative_values > 0:
                errors.append(f"{negative_values} records have negative {col}")

    # Validate win_percentage is between 0 and 1
    if "win_percentage" in df.columns:
        invalid_pct = (
            (df["win_percentage"].dropna() < 0) | (df["win_percentage"].dropna() > 1)
        ).sum()
        if invalid_pct > 0:
            errors.append(
                f"{invalid_pct} records have invalid win_percentage (not 0-1)"
            )

    # Check expected number of teams (should be 30 for modern NBA)
    if len(df) > 0 and len(df) < 20:
        errors.append(f"Only {len(df)} teams found, expected ~30 teams")

    # Warn if more than 30 teams (might indicate duplicates)
    if len(df) > 35:
        errors.append(
            f"{len(df)} teams found, expected ~30 teams (possible duplicates)"
        )

    return len(errors) == 0, errors


def create_br_standings_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the br_standings table if it doesn't exist.

    Args:
        conn: DuckDB connection.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS br_standings (
            standings_key VARCHAR PRIMARY KEY,
            season_year INTEGER NOT NULL,
            season_id VARCHAR NOT NULL,
            team VARCHAR NOT NULL,
            team_name VARCHAR,
            conference VARCHAR,
            division VARCHAR,
            wins INTEGER,
            losses INTEGER,
            win_percentage DOUBLE,
            games_behind DOUBLE,
            playoff_seed INTEGER,
            points_for DOUBLE,
            points_against DOUBLE,
            point_differential DOUBLE,
            home_wins INTEGER,
            home_losses INTEGER,
            away_wins INTEGER,
            away_losses INTEGER,
            streak VARCHAR,
            last_10_record VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indexes for common queries
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_br_standings_season
        ON br_standings(season_year)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_br_standings_team
        ON br_standings(team)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_br_standings_conference
        ON br_standings(conference)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_br_standings_division
        ON br_standings(division)
    """)

    conn.commit()
    logger.info("Created br_standings table and indexes")


def get_season_end_year_from_string(season_str: str) -> int:
    """Convert season string like '2024-25' to end year (2025).

    Args:
        season_str: Season string in format 'YYYY-YY'.

    Returns:
        Season end year as integer.
    """
    if "-" in season_str:
        parts = season_str.split("-")
        start_year = int(parts[0])
        return start_year + 1
    return int(season_str)


def populate_br_standings(
    db_path: str | None = None,
    seasons: list[int] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    delay: float | None = None,
    reset_progress: bool = False,
    dry_run: bool = False,
    validate: bool = True,
) -> dict[str, Any]:
    """Populate NBA standings from Basketball Reference.

    Args:
        db_path: Path to DuckDB database. Defaults to configured path.
        seasons: Specific season end years to fetch (e.g., [2024, 2023]).
            Takes precedence over start_year/end_year.
        start_year: Start year for automatic season range.
        end_year: End year for automatic season range.
        delay: Delay in seconds between API calls. Default: 3.0 for rate limiting.
        reset_progress: Reset progress tracking before starting.
        dry_run: Don't write to database, only fetch and transform.
        validate: Run data validation before insertion.

    Returns:
        Dictionary with population metrics including:
        - records_fetched: Total records retrieved from BR
        - records_inserted: Records written to database
        - api_calls: Number of API requests made
        - errors: List of any errors encountered

    Example:
        >>> result = populate_br_standings(seasons=[2024, 2023])
        >>> print(f"Inserted {result['records_inserted']} standings")
    """
    db_path = db_path or str(get_db_path())
    metrics = PopulationMetrics()
    progress = ProgressTracker("br_standings")

    if reset_progress:
        progress.reset()
        logger.info("Progress tracking reset")

    logger.info("=" * 70)
    logger.info("BASKETBALL REFERENCE STANDINGS POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Dry run: {dry_run}")

    metrics.start()
    conn = None

    # Set default delay for rate limiting (Basketball Reference is sensitive)
    if delay is None:
        delay = 3.0  # 3 seconds between requests to avoid 403 errors

    try:
        conn = duckdb.connect(db_path)

        # Create table if needed
        if not dry_run:
            create_br_standings_table(conn)

        # Determine seasons to process
        if seasons:
            seasons_to_process = sorted(seasons, reverse=True)
        elif start_year and end_year:
            seasons_to_process = list(range(end_year, start_year - 1, -1))
        else:
            # Default: current season
            current_end_year = get_season_end_year_from_string(CURRENT_SEASON)
            seasons_to_process = [current_end_year]

        logger.info(
            f"Processing {len(seasons_to_process)} seasons: {seasons_to_process}"
        )

        total_records = 0
        seasons_processed = 0
        seasons_skipped = 0

        for season_end_year in seasons_to_process:
            season_key = f"season_{season_end_year}"

            # Check if already processed
            if progress.is_completed(season_key):
                logger.debug(f"Skipping season {season_end_year} (already done)")
                seasons_skipped += 1
                continue

            try:
                logger.info(
                    f"Fetching standings for {season_end_year - 1}-{str(season_end_year)[2:]} season..."
                )

                # Rate limiting delay before API call (except first call)
                if metrics.api_calls > 0:
                    logger.debug(f"Rate limiting: sleeping {delay}s")
                    time.sleep(delay)

                metrics.api_calls += 1

                # Fetch standings from Basketball Reference
                standings_data = br_client.standings(season_end_year=season_end_year)

                if standings_data is None or len(standings_data) == 0:
                    logger.warning(
                        f"No standings data returned for season {season_end_year}"
                    )
                    progress.mark_completed(season_key)
                    continue

                # Convert list of dicts to DataFrame
                if isinstance(standings_data, list):
                    df = pd.DataFrame(standings_data)
                else:
                    df = standings_data

                logger.info(f"  Retrieved {len(df)} team standings from BR")

                # Transform data to match our schema
                transformed = transform_br_standings(df, season_end_year)

                if transformed.empty:
                    logger.warning(
                        f"  No data after transformation for season {season_end_year}"
                    )
                    progress.mark_completed(season_key)
                    continue

                records = len(transformed)
                metrics.records_fetched += records

                # Validate data if requested
                if validate:
                    is_valid, validation_errors = validate_standings_data(transformed)
                    if not is_valid:
                        for err in validation_errors:
                            logger.warning(f"  Validation warning: {err}")
                            metrics.warnings.append(f"Season {season_end_year}: {err}")

                if dry_run:
                    logger.info(f"  DRY RUN: Would insert {records} standings")
                    logger.info(f"  Sample data:\n{transformed.head(3).to_string()}")
                    total_records += records
                else:
                    # Insert into database using UPSERT
                    conn.register("standings_data", transformed)
                    conn.execute("""
                        INSERT OR REPLACE INTO br_standings (
                            standings_key, season_year, season_id, team, team_name,
                            conference, division, wins, losses, win_percentage,
                            games_behind, playoff_seed, points_for, points_against,
                            point_differential, home_wins, home_losses, away_wins,
                            away_losses, streak, last_10_record, updated_at
                        )
                        SELECT
                            standings_key, season_year, season_id, team, team_name,
                            conference, division, wins, losses, win_percentage,
                            games_behind, playoff_seed, points_for, points_against,
                            point_differential, home_wins, home_losses, away_wins,
                            away_losses, streak, last_10_record, CURRENT_TIMESTAMP
                        FROM standings_data
                    """)
                    conn.unregister("standings_data")
                    conn.commit()

                    metrics.records_inserted += records
                    total_records += records
                    logger.info(
                        f"  Inserted {records} standings for season {season_end_year}"
                    )

                progress.mark_completed(season_key)
                seasons_processed += 1

                # Periodic progress save
                if seasons_processed % 5 == 0:
                    progress.save()

            except Exception as e:
                error_msg = f"Error processing season {season_end_year}: {e}"
                logger.exception(error_msg)
                progress.add_error(season_key, str(e))
                metrics.add_error(str(e), {"season": season_end_year})

        # Final progress save
        progress.save()

        logger.info("=" * 70)
        logger.info("BR STANDINGS POPULATION COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Seasons processed: {seasons_processed}")
        logger.info(f"Seasons skipped (already done): {seasons_skipped}")
        logger.info(f"Total team standings: {total_records:,}")
        logger.info(f"API calls: {metrics.api_calls}")

        if metrics.errors:
            logger.warning(f"Errors encountered: {len(metrics.errors)}")

    except Exception as e:
        logger.exception(f"Population failed: {e}")
        metrics.add_error(str(e))
        raise

    finally:
        metrics.stop()
        if conn:
            conn.close()

    return metrics.to_dict()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Populate NBA standings from Basketball Reference",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Populate current season
    python -m src.scripts.populate.populate_br_standings

    # Populate specific seasons
    python -m src.scripts.populate.populate_br_standings --seasons 2024 2023 2022

    # Populate a range of seasons
    python -m src.scripts.populate.populate_br_standings --start-year 2015 --end-year 2024

    # Dry run (don't write to database)
    python -m src.scripts.populate.populate_br_standings --seasons 2024 --dry-run

    # Reset progress and re-run
    python -m src.scripts.populate.populate_br_standings --reset-progress
        """,
    )

    parser.add_argument(
        "--seasons",
        type=int,
        nargs="+",
        help="Specific season end years to fetch (e.g., 2024 2023)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help="Start year for season range (used with --end-year)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        help="End year for season range (used with --start-year)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=3.0,
        help="Delay between API calls in seconds (default: 3.0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to database, only fetch and validate",
    )
    parser.add_argument(
        "--reset-progress",
        action="store_true",
        help="Reset progress tracking and start fresh",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip data validation before insertion",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point for CLI execution."""
    args = parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    configure_logging(level=log_level)

    # Run population
    result = populate_br_standings(
        db_path=args.db_path,
        seasons=args.seasons,
        start_year=args.start_year,
        end_year=args.end_year,
        delay=args.delay,
        reset_progress=args.reset_progress,
        dry_run=args.dry_run,
        validate=not args.skip_validation,
    )

    # Print summary
    print("\n" + "=" * 50)
    print("POPULATION SUMMARY")
    print("=" * 50)
    print(f"Duration: {result.get('duration_seconds', 0):.1f}s")
    print(f"Records fetched: {result.get('records_fetched', 0):,}")
    print(f"Records inserted: {result.get('records_inserted', 0):,}")
    print(f"API calls: {result.get('api_calls', 0)}")
    print(f"Errors: {result.get('error_count', 0)}")


if __name__ == "__main__":
    main()
