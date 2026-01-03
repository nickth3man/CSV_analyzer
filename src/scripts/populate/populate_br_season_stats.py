"""Populate player season stats from Basketball Reference.

This module fetches player season totals from Basketball Reference to backfill
historical data that isn't available from the NBA Stats API.

Usage:
    from src.scripts.populate.populate_br_season_stats import populate_br_season_stats

    # Backfill all historical seasons
    result = populate_br_season_stats()

    # Backfill specific seasons
    result = populate_br_season_stats(seasons=[1995, 1994, 1993])
"""

import logging
from typing import Any

import duckdb
import pandas as pd

from src.scripts.populate.base import PopulationMetrics, ProgressTracker
from src.scripts.populate.br_client import get_br_client
from src.scripts.populate.config import get_db_path
from src.scripts.populate.helpers import configure_logging


logger = logging.getLogger(__name__)


# Column mapping from BR HTML tables to our schema
# BR uses these column names in their HTML tables
BR_SEASON_COLUMNS = {
    "Player": "player_name",
    "Team": "team_abbreviation",
    "Age": "age",
    "Pos": "position",
    "G": "games_played",
    "GS": "games_started",
    "MP": "minutes_played",
    "FG": "fgm",
    "FGA": "fga",
    "FG%": "fg_pct",
    "3P": "fg3m",
    "3PA": "fg3a",
    "3P%": "fg3_pct",
    "2P": "fg2m",
    "2PA": "fg2a",
    "2P%": "fg2_pct",
    "eFG%": "efg_pct",
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
    "Trp-Dbl": "triple_doubles",
    "Awards": "awards",
}

BR_ADVANCED_COLUMNS = {
    "Player": "player_name",
    "Team": "team_abbreviation",
    "Age": "age",
    "Pos": "position",
    "G": "games_played",
    "MP": "minutes_played",
    "PER": "per",
    "TS%": "ts_pct",
    "3PAr": "fg3a_rate",
    "FTr": "fta_rate",
    "ORB%": "oreb_pct",
    "DRB%": "dreb_pct",
    "TRB%": "reb_pct",
    "AST%": "ast_pct",
    "STL%": "stl_pct",
    "BLK%": "blk_pct",
    "TOV%": "tov_pct",
    "USG%": "usg_pct",
    "OWS": "ows",
    "DWS": "dws",
    "WS": "ws",
    "WS/48": "ws_48",
    "OBPM": "obpm",
    "DBPM": "dbpm",
    "BPM": "bpm",
    "VORP": "vorp",
}


def transform_br_season_totals(df: pd.DataFrame, season_end_year: int) -> pd.DataFrame:
    """Transform BR season totals to match our schema."""
    if df is None or df.empty:
        return pd.DataFrame()

    # Rename columns using the mapping
    df = df.rename(columns=BR_SEASON_COLUMNS)

    # Add season info
    df["season_end_year"] = season_end_year
    df["season_id"] = f"{season_end_year - 1}-{str(season_end_year)[2:]}"

    # Ensure team_abbreviation is a string
    if "team_abbreviation" in df.columns:
        df["team_abbreviation"] = df["team_abbreviation"].astype(str)

    # Drop the Rk column if it exists (row number from BR)
    if "Rk" in df.columns:
        df = df.drop(columns=["Rk"])

    return df


def transform_br_advanced_totals(
    df: pd.DataFrame, season_end_year: int
) -> pd.DataFrame:
    """Transform BR advanced season totals to match our schema."""
    if df is None or df.empty:
        return pd.DataFrame()

    # Rename columns using the mapping
    df = df.rename(columns=BR_ADVANCED_COLUMNS)

    # Add season info
    df["season_end_year"] = season_end_year
    df["season_id"] = f"{season_end_year - 1}-{str(season_end_year)[2:]}"

    # Ensure team_abbreviation is a string
    if "team_abbreviation" in df.columns:
        df["team_abbreviation"] = df["team_abbreviation"].astype(str)

    # Drop the Rk column if it exists
    if "Rk" in df.columns:
        df = df.drop(columns=["Rk"])

    return df


def create_br_season_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create BR season stats tables if they don't exist."""
    # Basic season totals - matches BR column structure
    conn.execute("""
        CREATE TABLE IF NOT EXISTS br_player_season_totals (
            season_id VARCHAR,
            season_end_year INTEGER,
            player_name VARCHAR,
            team_abbreviation VARCHAR,
            position VARCHAR,
            age DOUBLE,
            games_played DOUBLE,
            games_started DOUBLE,
            minutes_played DOUBLE,
            fgm DOUBLE,
            fga DOUBLE,
            fg_pct DOUBLE,
            fg3m DOUBLE,
            fg3a DOUBLE,
            fg3_pct DOUBLE,
            fg2m DOUBLE,
            fg2a DOUBLE,
            fg2_pct DOUBLE,
            efg_pct DOUBLE,
            ftm DOUBLE,
            fta DOUBLE,
            ft_pct DOUBLE,
            oreb DOUBLE,
            dreb DOUBLE,
            reb DOUBLE,
            ast DOUBLE,
            stl DOUBLE,
            blk DOUBLE,
            tov DOUBLE,
            pf DOUBLE,
            pts DOUBLE,
            triple_doubles DOUBLE,
            awards VARCHAR,
            PRIMARY KEY (season_id, player_name, team_abbreviation)
        )
    """)

    # Advanced season stats - matches BR column structure
    conn.execute("""
        CREATE TABLE IF NOT EXISTS br_player_season_advanced (
            season_id VARCHAR,
            season_end_year INTEGER,
            player_name VARCHAR,
            team_abbreviation VARCHAR,
            position VARCHAR,
            age DOUBLE,
            games_played DOUBLE,
            minutes_played DOUBLE,
            per DOUBLE,
            ts_pct DOUBLE,
            fg3a_rate DOUBLE,
            fta_rate DOUBLE,
            oreb_pct DOUBLE,
            dreb_pct DOUBLE,
            reb_pct DOUBLE,
            ast_pct DOUBLE,
            stl_pct DOUBLE,
            blk_pct DOUBLE,
            tov_pct DOUBLE,
            usg_pct DOUBLE,
            ows DOUBLE,
            dws DOUBLE,
            ws DOUBLE,
            ws_48 DOUBLE,
            obpm DOUBLE,
            dbpm DOUBLE,
            bpm DOUBLE,
            vorp DOUBLE,
            PRIMARY KEY (season_id, player_name, team_abbreviation)
        )
    """)
    conn.commit()


def populate_br_season_stats(
    db_path: str | None = None,
    seasons: list[int] | None = None,
    start_year: int = 1980,
    end_year: int = 1996,
    include_advanced: bool = True,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Populate player season stats from Basketball Reference.

    Args:
        db_path: Path to DuckDB database
        seasons: Specific seasons to fetch (end years, e.g., [1995, 1994])
        start_year: Start year for automatic season range
        end_year: End year for automatic season range
        include_advanced: Also fetch advanced stats
        reset_progress: Reset progress tracking
        dry_run: Don't write to database

    Returns:
        Dictionary with population metrics
    """
    db_path = db_path or str(get_db_path())
    metrics = PopulationMetrics()
    progress = ProgressTracker("br_season_stats")

    if reset_progress:
        progress.reset()

    logger.info("=" * 70)
    logger.info("BASKETBALL REFERENCE SEASON STATS POPULATION")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")

    metrics.start()
    conn = None

    try:
        conn = duckdb.connect(db_path)
        client = get_br_client()

        # Create tables if needed
        if not dry_run:
            create_br_season_tables(conn)

        # Determine seasons to process
        # Process historical seasons (BR data available from ~1950) if seasons not provided
        seasons_to_process = seasons or list(range(end_year, start_year - 1, -1))

        logger.info(
            f"Processing {len(seasons_to_process)} seasons: {seasons_to_process[0]} to {seasons_to_process[-1]}"
        )

        total_basic_records = 0
        total_advanced_records = 0
        seasons_processed = 0

        for season_end_year in seasons_to_process:
            season_key = str(season_end_year)

            # Check if already processed
            if progress.is_completed(season_key):
                logger.debug(f"Skipping {season_end_year} (already done)")
                continue

            try:
                logger.info(f"Fetching season totals for {season_end_year}...")

                # Fetch basic totals
                metrics.api_calls += 1
                basic_df = client.get_players_season_totals(season_end_year)

                if basic_df is not None and not basic_df.empty:
                    transformed = transform_br_season_totals(basic_df, season_end_year)
                    records = len(transformed)
                    total_basic_records += records
                    metrics.records_fetched += records

                    if not dry_run:
                        # Get only columns that exist in both DataFrame and table
                        table_cols = [
                            "season_id",
                            "season_end_year",
                            "player_name",
                            "team_abbreviation",
                            "position",
                            "age",
                            "games_played",
                            "games_started",
                            "minutes_played",
                            "fgm",
                            "fga",
                            "fg_pct",
                            "fg3m",
                            "fg3a",
                            "fg3_pct",
                            "fg2m",
                            "fg2a",
                            "fg2_pct",
                            "efg_pct",
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
                            "triple_doubles",
                            "awards",
                        ]
                        # Only include columns that exist in DataFrame
                        available_cols = [
                            c for c in table_cols if c in transformed.columns
                        ]
                        insert_df = transformed[available_cols].copy()

                        conn.register("br_season_data", insert_df)
                        cols_str = ", ".join(available_cols)
                        conn.execute(f"""
                            INSERT OR REPLACE INTO br_player_season_totals ({cols_str})
                            SELECT {cols_str} FROM br_season_data
                        """)
                        conn.unregister("br_season_data")
                        conn.commit()
                        metrics.records_inserted += records

                    logger.info(f"  Basic totals: {records} players")

                # Fetch advanced stats
                if include_advanced:
                    metrics.api_calls += 1
                    advanced_df = client.get_players_advanced_season_totals(
                        season_end_year
                    )

                    if advanced_df is not None and not advanced_df.empty:
                        transformed = transform_br_advanced_totals(
                            advanced_df, season_end_year
                        )
                        records = len(transformed)
                        total_advanced_records += records
                        metrics.records_fetched += records

                        if not dry_run:
                            # Get only columns that exist in both DataFrame and table
                            adv_table_cols = [
                                "season_id",
                                "season_end_year",
                                "player_name",
                                "team_abbreviation",
                                "position",
                                "age",
                                "games_played",
                                "minutes_played",
                                "per",
                                "ts_pct",
                                "fg3a_rate",
                                "fta_rate",
                                "oreb_pct",
                                "dreb_pct",
                                "reb_pct",
                                "ast_pct",
                                "stl_pct",
                                "blk_pct",
                                "tov_pct",
                                "usg_pct",
                                "ows",
                                "dws",
                                "ws",
                                "ws_48",
                                "obpm",
                                "dbpm",
                                "bpm",
                                "vorp",
                            ]
                            available_cols = [
                                c for c in adv_table_cols if c in transformed.columns
                            ]
                            insert_df = transformed[available_cols].copy()

                            conn.register("br_adv_data", insert_df)
                            cols_str = ", ".join(available_cols)
                            conn.execute(f"""
                                INSERT OR REPLACE INTO br_player_season_advanced ({cols_str})
                                SELECT {cols_str} FROM br_adv_data
                            """)
                            conn.unregister("br_adv_data")
                            conn.commit()
                            metrics.records_inserted += records

                        logger.info(f"  Advanced stats: {records} players")

                progress.mark_completed(season_key)
                seasons_processed += 1

            except Exception as e:
                logger.exception(f"Error processing season {season_end_year}: {e}")
                progress.add_error(season_key, str(e))
                metrics.add_error(str(e), {"season": season_end_year})

        progress.save()

        logger.info("=" * 70)
        logger.info("BR SEASON STATS POPULATION COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Seasons processed: {seasons_processed}")
        logger.info(f"Basic season records: {total_basic_records:,}")
        logger.info(f"Advanced season records: {total_advanced_records:,}")

    except Exception as e:
        logger.exception(f"Population failed: {e}")
        metrics.add_error(str(e))
        raise

    finally:
        metrics.stop()
        if conn:
            conn.close()

    return metrics.to_dict()


if __name__ == "__main__":
    configure_logging()
    # Test with 2 seasons
    populate_br_season_stats(seasons=[1995, 1994])
