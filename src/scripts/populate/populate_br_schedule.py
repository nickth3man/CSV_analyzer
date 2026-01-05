"""Populate NBA season schedules from Basketball Reference.

This module fetches complete NBA season schedules from Basketball Reference
using the basketball_reference_web_scraper library.

Usage:
    from src.scripts.populate.populate_br_schedule import populate_br_schedule

    # Populate current season
    result = populate_br_schedule(seasons=[2025])

    # Populate multiple seasons
    result = populate_br_schedule(seasons=[2024, 2023, 2022])

    # Populate a range of historical seasons
    result = populate_br_schedule(start_year=2015, end_year=2024)
"""

import argparse
import logging
import random
import time
from datetime import datetime
from typing import Any

import duckdb
import pandas as pd
from bs4 import BeautifulSoup

# from basketball_reference_web_scraper import client as br_client  # Deprecated due to 403 blocking
from curl_cffi import requests as cffi_requests

from src.scripts.populate.base import PopulationMetrics, ProgressTracker
from src.scripts.populate.config import CURRENT_SEASON, get_db_path
from src.scripts.populate.helpers import configure_logging


logger = logging.getLogger(__name__)


# Column mapping from BR schedule data to our schema
BR_SCHEDULE_COLUMNS = {
    "start_time": "start_time",
    "away_team": "away_team",
    "away_team_score": "away_team_score",
    "home_team": "home_team",
    "home_team_score": "home_team_score",
    "overtime_periods": "overtime_periods",
    "attendance": "attendance",
    "arena": "arena",
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


def extract_game_date(start_time: Any) -> str | None:
    """Extract game date from start_time datetime.

    Args:
        start_time: Datetime object or string from BR.

    Returns:
        Date string in YYYY-MM-DD format or None.
    """
    if start_time is None:
        return None

    if isinstance(start_time, datetime):
        return start_time.strftime("%Y-%m-%d")

    if isinstance(start_time, str):
        try:
            # Try parsing common datetime formats
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                try:
                    # Parse and extract just the date portion
                    parsed = datetime.strptime(start_time, fmt)  # noqa: DTZ007
                    return parsed.strftime("%Y-%m-%d")
                except ValueError:
                    continue
        except Exception:
            pass

    return None


def extract_start_time_str(start_time: Any) -> str | None:
    """Extract start time as string from datetime.

    Args:
        start_time: Datetime object or string from BR.

    Returns:
        Time string in HH:MM format or None.
    """
    if start_time is None:
        return None

    if isinstance(start_time, datetime):
        return start_time.strftime("%H:%M")

    return None


# -----------------------------------------------------------------------------
# CUSTOM SCRAPER WITH CURL_CFFI (BYPASS 403)
# -----------------------------------------------------------------------------


def parse_schedule_row(row, year: int) -> dict[str, Any] | None:
    """Parse a single row from the BR schedule table."""
    try:
        cols = row.find_all(["th", "td"])
        if not cols or len(cols) < 6:
            return None

        # Date
        date_text = cols[0].get_text().strip()
        if date_text == "Playoffs":
            return None

        # Try parse date (e.g., "Tue, Oct 24, 2023")
        try:
            game_date = datetime.strptime(date_text, "%a, %b %d, %Y")
        except ValueError:
            return None

        # Start Time (e.g., "7:30p")
        time_text = cols[1].get_text().strip()
        if time_text:
            time_text = time_text.replace("p", " PM").replace("a", " AM")
            # Combine date and time
            full_dt_str = f"{game_date.strftime('%Y-%m-%d')} {time_text}"
            try:
                start_time = datetime.strptime(full_dt_str, "%Y-%m-%d %I:%M %p")
            except ValueError:
                start_time = game_date
        else:
            start_time = game_date

        # Visitor Team
        visitor_curr = cols[2].get_text(strip=True)

        # Visitor Pts
        visitor_pts = cols[3].get_text(strip=True)
        visitor_pts = int(visitor_pts) if visitor_pts.isdigit() else None

        # Home Team
        home_curr = cols[4].get_text(strip=True)

        # Home Pts
        home_pts = cols[5].get_text(strip=True)
        home_pts = int(home_pts) if home_pts.isdigit() else None

        # OT
        ot_text = cols[7].get_text(strip=True) if len(cols) > 7 else ""
        ot_periods = 0
        if ot_text == "OT":
            ot_periods = 1
        elif "OT" in ot_text:
            try:
                ot_periods = int(ot_text.replace("OT", ""))
            except:
                pass

        # Attendance
        attend_idx = 8
        if len(cols) > attend_idx:
            attend_text = cols[attend_idx].get_text(strip=True).replace(",", "")
            attendance = int(attend_text) if attend_text.isdigit() else None
        else:
            attendance = None

        # Arena
        arena_idx = 9
        arena = cols[arena_idx].get_text(strip=True) if len(cols) > arena_idx else None

        return {
            "start_time": start_time,
            "away_team": visitor_curr.upper().replace(
                " ", "_"
            ),  # Match enum-like format roughly
            "away_team_score": visitor_pts,
            "home_team": home_curr.upper().replace(" ", "_"),
            "home_team_score": home_pts,
            "overtime_periods": ot_periods,
            "attendance": attendance,
            "arena": arena,
        }

    except Exception as e:
        logger.warning(f"Failed to parse row: {e}")
        return None


def fetch_season_schedule_cffi(season_end_year: int) -> list[dict[str, Any]]:
    """Fetch season schedule using curl_cffi to bypass blocking."""
    months = [
        "october",
        "november",
        "december",
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
    ]
    all_games = []

    # Base URL
    base_url = (
        f"https://www.basketball-reference.com/leagues/NBA_{season_end_year}_games-"
    )

    # Random user agents
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    for month in months:
        url = f"{base_url}{month}.html"
        logger.info(f"  Fetching {url}...")

        try:
            # Add jitter
            time.sleep(random.uniform(2.0, 5.0))

            response = cffi_requests.get(
                url,
                impersonate="chrome",
                headers={
                    "User-Agent": random.choice(user_agents),
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.basketball-reference.com/",
                },
                timeout=30,
            )

            if response.status_code == 404:
                logger.debug(
                    f"  Month {month} not found (might be future or lockout). skipping."
                )
                continue

            if response.status_code != 200:
                logger.error(f"  Failed for {month}: Status {response.status_code}")
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", {"id": "schedule"})

            if not table:
                logger.warning(f"  No schedule table found for {month}")
                continue

            rows = table.find("tbody").find_all("tr")
            for row in rows:
                if "thead" in row.get("class", []):
                    continue

                game_data = parse_schedule_row(row, season_end_year)
                if game_data:
                    all_games.append(game_data)

        except Exception as e:
            logger.error(f"  Error fetching {month}: {e}")

    return all_games


def transform_br_schedule(df: pd.DataFrame, season_year: int) -> pd.DataFrame:
    """Transform BR schedule data to match our schema.

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

    # Extract game_date from start_time
    result["game_date"] = result["start_time"].apply(extract_game_date)

    # Extract time portion as string
    result["start_time_str"] = result["start_time"].apply(extract_start_time_str)

    # Keep original start_time as datetime if possible, convert to string for storage
    if "start_time" in result.columns:
        result["start_time"] = result["start_time"].apply(
            lambda x: x.isoformat()
            if isinstance(x, datetime)
            else str(x)
            if x
            else None
        )

    # Convert team enums to strings
    if "away_team" in result.columns:
        result["away_team"] = result["away_team"].apply(convert_team_enum)

    if "home_team" in result.columns:
        result["home_team"] = result["home_team"].apply(convert_team_enum)

    # Add season info
    result["season_year"] = season_year
    result["season_id"] = f"{season_year - 1}-{str(season_year)[2:]}"

    # Handle optional columns that might not exist
    if "overtime_periods" not in result.columns:
        result["overtime_periods"] = 0

    # Convert overtime_periods to int, handling None
    result["overtime_periods"] = (
        pd.to_numeric(result["overtime_periods"], errors="coerce").fillna(0).astype(int)
    )

    # Convert scores to nullable integers
    for col in ["away_team_score", "home_team_score"]:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")
        else:
            result[col] = None

    # Handle attendance - might be None for unplayed games
    if "attendance" in result.columns:
        result["attendance"] = pd.to_numeric(result["attendance"], errors="coerce")
    else:
        result["attendance"] = None

    # Handle arena - might be None
    if "arena" not in result.columns:
        result["arena"] = None

    # Generate a unique game key for deduplication
    # Format: BR_YYYYMMDD_AWAY_HOME
    def generate_game_key(row):
        date_str = row.get("game_date", "")
        if date_str:
            date_str = date_str.replace("-", "")
        away = (row.get("away_team", "") or "").replace(" ", "_")[:15]
        home = (row.get("home_team", "") or "").replace(" ", "_")[:15]
        return f"BR_{date_str}_{away}_{home}"

    result["game_key"] = result.apply(generate_game_key, axis=1)

    # Select and order columns for output
    output_cols = [
        "game_key",
        "season_year",
        "season_id",
        "game_date",
        "start_time",
        "start_time_str",
        "away_team",
        "away_team_score",
        "home_team",
        "home_team_score",
        "overtime_periods",
        "attendance",
        "arena",
    ]

    # Ensure all output columns exist
    for col in output_cols:
        if col not in result.columns:
            result[col] = None

    return result[output_cols]


def validate_schedule_data(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """Validate the schedule DataFrame.

    Args:
        df: Schedule DataFrame to validate.

    Returns:
        Tuple of (is_valid, list of error messages).
    """
    errors = []

    if df is None or df.empty:
        errors.append("DataFrame is empty or None")
        return False, errors

    # Check required columns
    required_cols = ["game_date", "home_team", "away_team", "season_year"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")

    # Check for missing game dates
    if "game_date" in df.columns:
        null_dates = df["game_date"].isna().sum()
        if null_dates > 0:
            errors.append(f"{null_dates} records have missing game_date")

    # Check for missing teams
    if "home_team" in df.columns:
        null_home = df["home_team"].isna().sum()
        empty_home = (df["home_team"] == "").sum()
        if null_home + empty_home > 0:
            errors.append(f"{null_home + empty_home} records have missing home_team")

    if "away_team" in df.columns:
        null_away = df["away_team"].isna().sum()
        empty_away = (df["away_team"] == "").sum()
        if null_away + empty_away > 0:
            errors.append(f"{null_away + empty_away} records have missing away_team")

    # Check for duplicate game keys
    if "game_key" in df.columns:
        dup_count = df.duplicated(subset=["game_key"]).sum()
        if dup_count > 0:
            errors.append(f"{dup_count} duplicate game_key values found")

    # Validate scores make sense (non-negative when present)
    for col in ["home_team_score", "away_team_score"]:
        if col in df.columns:
            negative_scores = (df[col].dropna() < 0).sum()
            if negative_scores > 0:
                errors.append(f"{negative_scores} records have negative {col}")

    return len(errors) == 0, errors


def create_br_schedule_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the br_schedule table if it doesn't exist.

    Args:
        conn: DuckDB connection.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS br_schedule (
            game_key VARCHAR PRIMARY KEY,
            season_year INTEGER NOT NULL,
            season_id VARCHAR NOT NULL,
            game_date DATE,
            start_time VARCHAR,
            start_time_str VARCHAR,
            away_team VARCHAR NOT NULL,
            away_team_score INTEGER,
            home_team VARCHAR NOT NULL,
            home_team_score INTEGER,
            overtime_periods INTEGER DEFAULT 0,
            attendance INTEGER,
            arena VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indexes for common queries
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_br_schedule_season
        ON br_schedule(season_year)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_br_schedule_game_date
        ON br_schedule(game_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_br_schedule_teams
        ON br_schedule(home_team, away_team)
    """)

    conn.commit()
    logger.info("Created br_schedule table and indexes")


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


def populate_br_schedule(
    db_path: str | None = None,
    seasons: list[int] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    delay: float | None = None,
    reset_progress: bool = False,
    dry_run: bool = False,
    validate: bool = True,
) -> dict[str, Any]:
    """Populate NBA season schedules from Basketball Reference.

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
        >>> result = populate_br_schedule(seasons=[2024, 2023])
        >>> print(f"Inserted {result['records_inserted']} games")
    """
    db_path = db_path or str(get_db_path())
    metrics = PopulationMetrics()
    progress = ProgressTracker("br_schedule")

    if reset_progress:
        progress.reset()
        logger.info("Progress tracking reset")

    logger.info("=" * 70)
    logger.info("BASKETBALL REFERENCE SCHEDULE POPULATION")
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
            create_br_schedule_table(conn)

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
                    f"Fetching schedule for {season_end_year - 1}-{str(season_end_year)[2:]} season..."
                )

                # Rate limiting delay before API call (except first call)
                if metrics.api_calls > 0:
                    logger.debug(f"Rate limiting: sleeping {delay}s")
                    time.sleep(delay)

                metrics.api_calls += 1

                # Fetch schedule from Basketball Reference (CUSTOM FETCH)
                schedule_data = fetch_season_schedule_cffi(season_end_year)

                if schedule_data is None or len(schedule_data) == 0:
                    logger.warning(
                        f"No schedule data returned for season {season_end_year}"
                    )
                    progress.mark_completed(season_key)
                    continue

                # Convert list of dicts to DataFrame
                if isinstance(schedule_data, list):
                    df = pd.DataFrame(schedule_data)
                else:
                    df = schedule_data

                logger.info(f"  Retrieved {len(df)} games from BR")

                # Transform data to match our schema
                transformed = transform_br_schedule(df, season_end_year)

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
                    is_valid, validation_errors = validate_schedule_data(transformed)
                    if not is_valid:
                        for err in validation_errors:
                            logger.warning(f"  Validation warning: {err}")
                            metrics.warnings.append(f"Season {season_end_year}: {err}")

                if dry_run:
                    logger.info(f"  DRY RUN: Would insert {records} games")
                    total_records += records
                else:
                    # Insert into database using UPSERT
                    conn.register("schedule_data", transformed)
                    conn.execute("""
                        INSERT OR REPLACE INTO br_schedule (
                            game_key, season_year, season_id, game_date,
                            start_time, start_time_str, away_team, away_team_score,
                            home_team, home_team_score, overtime_periods,
                            attendance, arena, updated_at
                        )
                        SELECT
                            game_key, season_year, season_id, game_date,
                            start_time, start_time_str, away_team, away_team_score,
                            home_team, home_team_score, overtime_periods,
                            attendance, arena, CURRENT_TIMESTAMP
                        FROM schedule_data
                    """)
                    conn.unregister("schedule_data")
                    conn.commit()

                    metrics.records_inserted += records
                    total_records += records
                    logger.info(
                        f"  Inserted {records} games for season {season_end_year}"
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
        logger.info("BR SCHEDULE POPULATION COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Seasons processed: {seasons_processed}")
        logger.info(f"Seasons skipped (already done): {seasons_skipped}")
        logger.info(f"Total games: {total_records:,}")
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
        description="Populate NBA season schedules from Basketball Reference",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Populate current season
    python -m src.scripts.populate.populate_br_schedule

    # Populate specific seasons
    python -m src.scripts.populate.populate_br_schedule --seasons 2024 2023 2022

    # Populate a range of seasons
    python -m src.scripts.populate.populate_br_schedule --start-year 2015 --end-year 2024

    # Dry run (don't write to database)
    python -m src.scripts.populate.populate_br_schedule --seasons 2024 --dry-run

    # Reset progress and re-run
    python -m src.scripts.populate.populate_br_schedule --reset-progress
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
    result = populate_br_schedule(
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
