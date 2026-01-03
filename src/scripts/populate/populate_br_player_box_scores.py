"""Populate player game stats from Basketball Reference.

This module fetches player box scores from Basketball Reference to backfill
historical data that isn't available from the NBA Stats API (pre-1996 games).

Usage:
    from src.scripts.populate.populate_br_player_box_scores import populate_br_player_box_scores

    # Backfill all missing dates
    result = populate_br_player_box_scores()

    # Backfill specific date range
    result = populate_br_player_box_scores(
        start_date="1990-01-01",
        end_date="1995-12-31"
    )
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any

import duckdb
import pandas as pd

from src.scripts.populate.base import PopulationMetrics, ProgressTracker
from src.scripts.populate.br_client import get_br_client
from src.scripts.populate.config import get_db_path

logger = logging.getLogger(__name__)


# Column mapping from BR to our schema
BR_TO_DB_COLUMNS = {
    "name": "player_name",
    "team": "team_abbreviation",
    "location": "location",  # HOME or AWAY
    "opponent": "opponent",
    "outcome": "wl",  # Win or Loss enum
    "seconds_played": "seconds_played",
    "made_field_goals": "fgm",
    "attempted_field_goals": "fga",
    "made_three_point_field_goals": "fg3m",
    "attempted_three_point_field_goals": "fg3a",
    "made_free_throws": "ftm",
    "attempted_free_throws": "fta",
    "offensive_rebounds": "oreb",
    "defensive_rebounds": "dreb",
    "assists": "ast",
    "steals": "stl",
    "blocks": "blk",
    "turnovers": "tov",
    "personal_fouls": "pf",
    "points_scored": "pts",
    "game_score": "game_score",
    "plus_minus": "plus_minus",
    "Player": "player_name",
    "MP": "min",
    "FG": "fgm",
    "FGA": "fga",
    "FG%": "fg_pct",
    "3P": "fg3m",
    "3PA": "fg3a",
    "3P%": "fg3_pct",
    "FT": "ftm",
    "FTA": "fta",
    "FT%": "ft_pct",
    "ORB": "oreb",
    "DRB": "dreb",
    "TRB": "reb",
    "AST": "ast",
    "STL": "stl",
    "BLK": "blk",
    "TOV": "tov",
    "PF": "pf",
    "PTS": "pts",
    "GmSc": "game_score",
    "+/-": "plus_minus",
}


def transform_br_box_scores(df: pd.DataFrame, game_date: date) -> pd.DataFrame:
    """Transform BR box score data to match our schema.

    Args:
        df: Raw DataFrame from BR client
        game_date: Date of the games

    Returns:
        Transformed DataFrame ready for insertion
    """
    if df is None or df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        flat_cols = []
        for col in df.columns:
            label = None
            for part in reversed(col):
                if part and not str(part).startswith("Unnamed"):
                    label = part
                    break
            flat_cols.append(label or col[0])
        df = df.copy()
        df.columns = flat_cols

    # Rename columns
    df = df.rename(columns=BR_TO_DB_COLUMNS)

    # Add game date
    df["game_date"] = game_date

    if "player_name" in df.columns:
        df = df[df["player_name"].astype(str).str.lower() != "team totals"]

    # Convert seconds to minutes string (MM:SS format)
    if "min" not in df.columns and "seconds_played" in df.columns:
        df["min"] = df["seconds_played"].apply(
            lambda s: f"{int(s // 60)}:{int(s % 60):02d}" if pd.notna(s) else None
        )

    for col in ["fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb"]:
        if col not in df.columns:
            df[col] = 0

    for col in ["fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Calculate percentages
    df["fg_pct"] = df.apply(
        lambda r: r["fgm"] / r["fga"] if r["fga"] > 0 else 0.0, axis=1
    )
    df["fg3_pct"] = df.apply(
        lambda r: r["fg3m"] / r["fg3a"] if r["fg3a"] > 0 else 0.0, axis=1
    )
    df["ft_pct"] = df.apply(
        lambda r: r["ftm"] / r["fta"] if r["fta"] > 0 else 0.0, axis=1
    )

    # Calculate total rebounds
    if "reb" not in df.columns:
        df["reb"] = df["oreb"].fillna(0) + df["dreb"].fillna(0)

    # Convert team enum to string if needed
    if "team_abbreviation" in df.columns:
        df["team_abbreviation"] = df["team_abbreviation"].apply(
            lambda t: t.value if hasattr(t, "value") else str(t)
        )
    if "opponent" in df.columns:
        df["opponent"] = df["opponent"].apply(
            lambda t: t.value if hasattr(t, "value") else str(t)
        )
    if "wl" in df.columns:
        df["wl"] = df["wl"].apply(
            lambda o: "W" if hasattr(o, "value") and "WIN" in str(o.value).upper()
            else ("L" if hasattr(o, "value") else str(o)[:1].upper())
        )
    if "location" in df.columns:
        df["location"] = df["location"].apply(
            lambda l: l.value if hasattr(l, "value") else str(l)
        )

    # Generate a synthetic game_id based on date and teams
    # Format: BRYYYYMMDDTTT where TTT is home team abbreviation
    def generate_game_id(row):
        date_str = game_date.strftime("%Y%m%d")
        team = row.get("team_abbreviation", "UNK")[:3].upper()
        opp = row.get("opponent", "UNK")[:3].upper()
        loc = row.get("location", "")
        # Home team first in game ID
        if "HOME" in str(loc).upper():
            return f"BR{date_str}{team}{opp}"
        else:
            return f"BR{date_str}{opp}{team}"

    df["game_id"] = df.apply(generate_game_id, axis=1)

    # Select and order columns for the table
    output_cols = [
        "game_id",
        "game_date",
        "player_name",
        "team_abbreviation",
        "opponent",
        "location",
        "wl",
        "min",
        "fgm",
        "fga",
        "fg_pct",
        "fg3m",
        "fg3a",
        "fg3_pct",
        "ftm",
        "fta",
        "ft_pct",
        "oreb",
        "dreb",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "pf",
        "pts",
        "plus_minus",
        "game_score",
    ]

    for col in output_cols:
        if col not in df.columns:
            df[col] = None
    return df[output_cols]


def get_missing_game_dates(
    conn: duckdb.DuckDBPyConnection,
    start_year: int = 1947,
    end_year: int = 1996,
) -> list[date]:
    """Get dates of games that are missing player stats.

    Args:
        conn: DuckDB connection
        start_year: Start year for search
        end_year: End year for search (exclusive for NBA API coverage)

    Returns:
        List of dates that need player stats
    """
    game_table = None
    for table_name in ("game_gold", "game_silver", "game_raw", "game"):
        try:
            conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            game_table = table_name
            break
        except Exception:
            continue

    if game_table is None:
        return []

    player_stats_table = None
    for table_name in (
        "player_game_stats",
        "player_game_stats_silver",
        "player_game_stats_raw",
    ):
        try:
            conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            player_stats_table = table_name
            break
        except Exception:
            continue

    stats_filter = ""
    if player_stats_table:
        stats_filter = f"""
        AND CAST(game_id AS VARCHAR) NOT IN (
            SELECT DISTINCT CAST(game_id AS VARCHAR) FROM {player_stats_table}
        )
        """

    query = f"""
        SELECT DISTINCT CAST(game_date AS DATE) as game_date
        FROM {game_table}
        WHERE CAST(game_date AS DATE) >= ? AND CAST(game_date AS DATE) < ?
        {stats_filter}
        AND CAST(game_id AS VARCHAR) NOT IN (
            SELECT DISTINCT CAST(game_id AS VARCHAR) FROM br_player_box_scores
        )
        ORDER BY game_date
    """

    start = date(start_year, 1, 1)
    end = date(end_year, 10, 1)  # Before 1996-97 season

    try:
        result = conn.execute(query, [start, end]).fetchall()
        return [row[0] for row in result if row[0] is not None]
    except duckdb.CatalogException:
        # br_player_box_scores table doesn't exist yet
        query_simple = f"""
            SELECT DISTINCT CAST(game_date AS DATE) as game_date
            FROM {game_table}
            WHERE CAST(game_date AS DATE) >= ? AND CAST(game_date AS DATE) < ?
            {stats_filter}
            ORDER BY game_date
        """
        result = conn.execute(query_simple, [start, end]).fetchall()
        return [row[0] for row in result if row[0] is not None]


def create_br_table_if_not_exists(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the br_player_box_scores table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS br_player_box_scores (
            game_id VARCHAR,
            game_date DATE,
            player_name VARCHAR,
            team_abbreviation VARCHAR,
            opponent VARCHAR,
            location VARCHAR,
            wl VARCHAR,
            min VARCHAR,
            fgm INTEGER,
            fga INTEGER,
            fg_pct DOUBLE,
            fg3m INTEGER,
            fg3a INTEGER,
            fg3_pct DOUBLE,
            ftm INTEGER,
            fta INTEGER,
            ft_pct DOUBLE,
            oreb INTEGER,
            dreb INTEGER,
            reb INTEGER,
            ast INTEGER,
            stl INTEGER,
            blk INTEGER,
            tov INTEGER,
            pf INTEGER,
            pts INTEGER,
            plus_minus INTEGER,
            game_score DOUBLE,
            PRIMARY KEY (game_id, player_name)
        )
    """)
    conn.commit()


def populate_br_player_box_scores(
    db_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    start_year: int = 1980,  # BR data is more reliable from 1980+
    end_year: int = 1996,
    delay: float | None = None,
    limit: int | None = None,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Populate player box scores from Basketball Reference.

    Args:
        db_path: Path to DuckDB database
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
        start_year: Start year for automatic date detection
        end_year: End year for automatic date detection
        limit: Maximum number of dates to process
        reset_progress: Reset progress tracking
        dry_run: Don't write to database

    Returns:
        Dictionary with population metrics
    """
    db_path = db_path or str(get_db_path())
    metrics = PopulationMetrics()
    progress = ProgressTracker("br_player_box_scores")

    if reset_progress:
        progress.reset()

    logger.info("=" * 70)
    logger.info("BASKETBALL REFERENCE PLAYER BOX SCORES POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")

    metrics.start()

    try:
        conn = duckdb.connect(db_path)
        client = get_br_client()
        if delay is not None:
            client.config.request_delay = delay

        # Create table if needed
        if not dry_run:
            create_br_table_if_not_exists(conn)

        # Determine dates to process
        if start_date and end_date:
            # Use provided date range
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
            dates_to_process = []
            current = start
            while current <= end:
                dates_to_process.append(current)
                current += timedelta(days=1)
        else:
            # Find missing dates from database
            logger.info(f"Finding missing game dates ({start_year}-{end_year})...")
            dates_to_process = get_missing_game_dates(conn, start_year, end_year)

        if not dates_to_process:
            logger.info("No dates need processing")
            return metrics.to_dict()

        logger.info(f"Found {len(dates_to_process)} dates to process")

        if limit:
            dates_to_process = dates_to_process[:limit]
            logger.info(f"Limited to {limit} dates")

        # Process each date
        total_records = 0
        dates_processed = 0
        dates_skipped = 0

        for game_date in dates_to_process:
            date_key = game_date.strftime("%Y-%m-%d")

            # Check if already processed
            if progress.is_completed(date_key):
                dates_skipped += 1
                continue

            try:
                logger.info(f"Fetching box scores for {date_key}...")
                metrics.api_calls += 1

                df = client.get_player_box_scores(
                    day=game_date.day,
                    month=game_date.month,
                    year=game_date.year,
                )

                if df is None or df.empty:
                    logger.debug(f"No games on {date_key}")
                    progress.mark_completed(date_key)
                    continue

                # Transform data
                transformed = transform_br_box_scores(df, game_date)
                if transformed.empty:
                    progress.mark_completed(date_key)
                    continue

                records = len(transformed)
                total_records += records
                metrics.records_fetched += records

                if not dry_run:
                    # Insert into database
                    conn.register("br_data", transformed)
                    conn.execute("""
                        INSERT OR REPLACE INTO br_player_box_scores
                        SELECT * FROM br_data
                    """)
                    conn.unregister("br_data")
                    conn.commit()
                    metrics.records_inserted += records

                progress.mark_completed(date_key)
                dates_processed += 1

                if dates_processed % 10 == 0:
                    logger.info(
                        f"Progress: {dates_processed}/{len(dates_to_process)} dates, "
                        f"{total_records:,} records"
                    )
                    progress.save()

            except Exception as e:
                logger.error(f"Error processing {date_key}: {e}")
                progress.add_error(date_key, str(e))
                metrics.add_error(str(e), {"date": date_key})

        # Final save
        progress.save()

        logger.info("=" * 70)
        logger.info("BR BOX SCORES POPULATION COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Dates processed: {dates_processed}")
        logger.info(f"Dates skipped (already done): {dates_skipped}")
        logger.info(f"Total records: {total_records:,}")

    except Exception as e:
        logger.exception(f"Population failed: {e}")
        metrics.add_error(str(e))
        raise

    finally:
        metrics.stop()
        if "conn" in locals():
            conn.close()

    return metrics.to_dict()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    populate_br_player_box_scores(limit=5)  # Test with 5 dates
