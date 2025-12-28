#!/usr/bin/env python3
"""Initialize the NBA DuckDB database with schema.

This script creates all required tables for the NBA analytics database.
It can be run to set up a fresh database or to add missing tables.

Tables created:
- player: Player master data
- team: Team master data
- game: Game records with home/away stats
- player_game_stats: Player statistics per game
- play_by_play: Play-by-play event data
- player_season_stats: Aggregated season statistics (view)

Usage:
    # Initialize database with all tables
    python scripts/populate/init_db.py

    # Specify custom database path
    python scripts/populate/init_db.py --db /path/to/nba.duckdb

    # Force recreate all tables (WARNING: deletes existing data)
    python scripts/populate/init_db.py --force
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import duckdb

from scripts.populate.config import get_db_path


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Schema definitions
SCHEMA_DEFINITIONS = {
    "player": """
        CREATE TABLE IF NOT EXISTS player (
            id BIGINT PRIMARY KEY,
            full_name VARCHAR NOT NULL,
            first_name VARCHAR,
            last_name VARCHAR NOT NULL,
            is_active BOOLEAN DEFAULT FALSE
        )
    """,
    "player_silver": """
        CREATE TABLE IF NOT EXISTS player_silver (
            id BIGINT PRIMARY KEY,
            full_name VARCHAR NOT NULL,
            first_name VARCHAR,
            last_name VARCHAR NOT NULL,
            is_active BOOLEAN DEFAULT FALSE
        )
    """,
    "team": """
        CREATE TABLE IF NOT EXISTS team (
            id BIGINT PRIMARY KEY,
            full_name VARCHAR NOT NULL,
            abbreviation VARCHAR(3) NOT NULL,
            nickname VARCHAR,
            city VARCHAR,
            state VARCHAR,
            year_founded INTEGER
        )
    """,
    "team_silver": """
        CREATE TABLE IF NOT EXISTS team_silver (
            id BIGINT PRIMARY KEY,
            full_name VARCHAR NOT NULL,
            abbreviation VARCHAR(3) NOT NULL,
            nickname VARCHAR,
            city VARCHAR,
            state VARCHAR,
            year_founded INTEGER
        )
    """,
    "team_details": """
        CREATE TABLE IF NOT EXISTS team_details (
            team_id BIGINT PRIMARY KEY,
            abbreviation VARCHAR(3),
            nickname VARCHAR,
            year_founded INTEGER,
            city VARCHAR,
            arena VARCHAR,
            arena_capacity INTEGER,
            owner VARCHAR,
            general_manager VARCHAR,
            head_coach VARCHAR,
            dleague_affiliation VARCHAR,
            facebook VARCHAR,
            instagram VARCHAR,
            twitter VARCHAR
        )
    """,
    "game": """
        CREATE TABLE IF NOT EXISTS game (
            game_id BIGINT PRIMARY KEY,
            season_id VARCHAR,
            game_date DATE,
            team_id_home BIGINT,
            team_abbreviation_home VARCHAR(3),
            team_name_home VARCHAR,
            matchup_home VARCHAR,
            wl_home VARCHAR(1),
            pts_home INTEGER,
            fg_pct_home DOUBLE,
            ft_pct_home DOUBLE,
            fg3_pct_home DOUBLE,
            ast_home INTEGER,
            reb_home INTEGER,
            team_id_away BIGINT,
            team_abbreviation_away VARCHAR(3),
            team_name_away VARCHAR,
            matchup_away VARCHAR,
            wl_away VARCHAR(1),
            pts_away INTEGER,
            fg_pct_away DOUBLE,
            ft_pct_away DOUBLE,
            fg3_pct_away DOUBLE,
            ast_away INTEGER,
            reb_away INTEGER,
            season_type VARCHAR
        )
    """,
    "game_gold": """
        CREATE TABLE IF NOT EXISTS game_gold (
            game_id BIGINT PRIMARY KEY,
            season_id VARCHAR,
            game_date DATE,
            home_team_id BIGINT,
            away_team_id BIGINT,
            home_pts INTEGER,
            away_pts INTEGER,
            home_win BOOLEAN,
            season_type VARCHAR
        )
    """,
    "player_game_stats": """
        CREATE TABLE IF NOT EXISTS player_game_stats (
            game_id BIGINT NOT NULL,
            team_id BIGINT,
            player_id BIGINT NOT NULL,
            player_name VARCHAR,
            start_position VARCHAR,
            comment VARCHAR,
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
            plus_minus DOUBLE,
            PRIMARY KEY (game_id, player_id)
        )
    """,
    "play_by_play": """
        CREATE TABLE IF NOT EXISTS play_by_play (
            game_id BIGINT NOT NULL,
            event_num INTEGER NOT NULL,
            event_msg_type INTEGER,
            event_msg_action_type INTEGER,
            period INTEGER,
            wctimestring VARCHAR,
            pctimestring VARCHAR,
            home_description VARCHAR,
            neutral_description VARCHAR,
            visitor_description VARCHAR,
            score VARCHAR,
            score_margin VARCHAR,
            person1type INTEGER,
            player1_id BIGINT,
            player1_name VARCHAR,
            player1_team_id BIGINT,
            person2type INTEGER,
            player2_id BIGINT,
            player2_name VARCHAR,
            player2_team_id BIGINT,
            person3type INTEGER,
            player3_id BIGINT,
            player3_name VARCHAR,
            player3_team_id BIGINT,
            PRIMARY KEY (game_id, event_num)
        )
    """,
    "common_player_info": """
        CREATE TABLE IF NOT EXISTS common_player_info (
            person_id BIGINT PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            display_first_last VARCHAR,
            display_last_comma_first VARCHAR,
            display_fi_last VARCHAR,
            player_slug VARCHAR,
            birthdate DATE,
            school VARCHAR,
            country VARCHAR,
            last_affiliation VARCHAR,
            height VARCHAR,
            weight INTEGER,
            season_exp INTEGER,
            jersey VARCHAR,
            position VARCHAR,
            roster_status VARCHAR,
            team_id BIGINT,
            team_name VARCHAR,
            team_abbreviation VARCHAR(3),
            team_city VARCHAR,
            from_year INTEGER,
            to_year INTEGER,
            draft_year INTEGER,
            draft_round INTEGER,
            draft_number INTEGER,
            greatest_75_flag BOOLEAN
        )
    """,
    "draft_history": """
        CREATE TABLE IF NOT EXISTS draft_history (
            person_id BIGINT,
            player_name VARCHAR,
            season INTEGER,
            round_number INTEGER,
            round_pick INTEGER,
            overall_pick INTEGER,
            draft_type VARCHAR,
            team_id BIGINT,
            team_city VARCHAR,
            team_name VARCHAR,
            team_abbreviation VARCHAR(3),
            organization VARCHAR,
            organization_type VARCHAR,
            PRIMARY KEY (person_id, season)
        )
    """,
    "draft_combine_stats": """
        CREATE TABLE IF NOT EXISTS draft_combine_stats (
            season INTEGER,
            player_id BIGINT,
            first_name VARCHAR,
            last_name VARCHAR,
            player_name VARCHAR,
            position VARCHAR,
            height_wo_shoes DOUBLE,
            height_w_shoes DOUBLE,
            weight DOUBLE,
            wingspan DOUBLE,
            standing_reach DOUBLE,
            body_fat_pct DOUBLE,
            hand_length DOUBLE,
            hand_width DOUBLE,
            standing_vertical_leap DOUBLE,
            max_vertical_leap DOUBLE,
            lane_agility_time DOUBLE,
            three_quarter_sprint DOUBLE,
            bench_press INTEGER,
            PRIMARY KEY (season, player_id)
        )
    """,
    "player_season_stats": """
        CREATE TABLE IF NOT EXISTS player_season_stats (
            player_id BIGINT NOT NULL,
            player_name VARCHAR,
            team_id BIGINT,
            team_abbreviation VARCHAR(3),
            season_id VARCHAR,
            season_type VARCHAR,
            games_played INTEGER,
            minutes_played DOUBLE,
            fgm DOUBLE,
            fga DOUBLE,
            fg_pct DOUBLE,
            fg3m DOUBLE,
            fg3a DOUBLE,
            fg3_pct DOUBLE,
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
            plus_minus DOUBLE,
            ts_pct DOUBLE,
            efg_pct DOUBLE,
            PRIMARY KEY (player_id, season_id, team_id)
        )
    """,
}


# Indexes to create for performance
INDEX_DEFINITIONS = [
    "CREATE INDEX IF NOT EXISTS idx_player_game_stats_player ON player_game_stats(player_id)",
    "CREATE INDEX IF NOT EXISTS idx_player_game_stats_game ON player_game_stats(game_id)",
    "CREATE INDEX IF NOT EXISTS idx_player_game_stats_team ON player_game_stats(team_id)",
    "CREATE INDEX IF NOT EXISTS idx_play_by_play_game ON play_by_play(game_id)",
    "CREATE INDEX IF NOT EXISTS idx_play_by_play_player1 ON play_by_play(player1_id)",
    "CREATE INDEX IF NOT EXISTS idx_game_date ON game(game_date)",
    "CREATE INDEX IF NOT EXISTS idx_game_home_team ON game(team_id_home)",
    "CREATE INDEX IF NOT EXISTS idx_game_away_team ON game(team_id_away)",
    "CREATE INDEX IF NOT EXISTS idx_player_season_stats_player ON player_season_stats(player_id)",
    "CREATE INDEX IF NOT EXISTS idx_player_season_stats_season ON player_season_stats(season_id)",
    "CREATE INDEX IF NOT EXISTS idx_player_season_stats_team ON player_season_stats(team_id)",
    "CREATE INDEX IF NOT EXISTS idx_common_player_info_team ON common_player_info(team_id)",
    "CREATE INDEX IF NOT EXISTS idx_draft_history_team ON draft_history(team_id)",
]


def init_database(
    db_path: str | None = None,
    force: bool = False,
    tables: list[str] | None = None,
) -> dict[str, str]:
    """Initialize the NBA database with schema.

    Args:
        db_path: Path to DuckDB database file
        force: If True, drop and recreate tables
        tables: List of specific tables to create (default: all)

    Returns:
        Dictionary with table name -> status
    """
    db_path = db_path or str(get_db_path())

    # Ensure parent directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("NBA DATABASE INITIALIZATION")
    logger.info("=" * 60)
    logger.info(f"Database: {db_path}")
    logger.info(f"Force recreate: {force}")

    conn = duckdb.connect(db_path)
    results = {}

    # Determine which tables to create
    tables_to_create = tables or list(SCHEMA_DEFINITIONS.keys())

    for table_name in tables_to_create:
        if table_name not in SCHEMA_DEFINITIONS:
            logger.warning(f"Unknown table: {table_name}")
            results[table_name] = "unknown"
            continue

        try:
            # Check if table exists
            table_exists = False
            try:
                conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                table_exists = True
            except duckdb.CatalogException:
                pass

            if table_exists and force:
                logger.info(f"Dropping table: {table_name}")
                conn.execute(f"DROP TABLE {table_name}")
                table_exists = False

            if table_exists:
                row_count = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}",
                ).fetchone()[0]
                logger.info(f"Table {table_name} exists ({row_count:,} rows)")
                results[table_name] = f"exists ({row_count:,} rows)"
            else:
                logger.info(f"Creating table: {table_name}")
                conn.execute(SCHEMA_DEFINITIONS[table_name])
                results[table_name] = "created"

        except Exception as e:
            logger.exception(f"Error with table {table_name}: {e}")
            results[table_name] = f"error: {e}"

    # Create indexes
    logger.info("\nCreating indexes...")
    for idx_sql in INDEX_DEFINITIONS:
        try:
            conn.execute(idx_sql)
        except Exception as e:
            logger.warning(f"Index creation failed: {e}")

    conn.close()

    logger.info("\n" + "=" * 60)
    logger.info("INITIALIZATION COMPLETE")
    logger.info("=" * 60)

    for table, status in results.items():
        logger.info(f"  {table}: {status}")

    return results


def get_database_info(db_path: str | None = None) -> dict[str, Any]:
    """Retrieve metadata and row counts for tables in the specified DuckDB database.

    Parameters:
        db_path (str | None): Path to the DuckDB database file. If omitted, the default path from get_db_path() is used.

    Returns:
        info (dict): Dictionary with database information.
            - exists (bool): `True` if the database file exists and was opened, `False` otherwise.
            - path (str): The resolved database path.
            - tables (dict): Mapping of table name -> `{"rows": <int>}` when the row count was obtained, or `{"error": "<message>"}` if counting failed. Present only when `exists` is `True`.
            - table_count (int): Number of tables discovered in the main schema. Present only when `exists` is `True`.
    """
    db_path = db_path or str(get_db_path())

    if not Path(db_path).exists():
        return {"exists": False, "path": db_path}

    conn = duckdb.connect(db_path, read_only=True)

    # Get all tables
    tables = conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()

    table_info = {}
    for (table_name,) in tables:
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            table_info[table_name] = {"rows": row_count}
        except Exception as e:
            table_info[table_name] = {"error": str(e)}

    conn.close()

    return {
        "exists": True,
        "path": db_path,
        "tables": table_info,
        "table_count": len(tables),
    }


def main() -> None:
    """Command-line entry point to initialize or inspect the NBA DuckDB database.

    Parses CLI arguments, supports listing available table definitions, printing database info, or creating the schema. Recognized options:
      --db       Path to the DuckDB database (optional).
      --force    Drop and recreate existing tables (deletes data).
      --tables   One or more specific table names to create.
      --info     Print database path, existence, table count, and per-table row counts or errors.
      --list-tables
                 Print names of available table definitions.

    On normal completion the function returns to the caller; if initialization fails it logs the error and exits the process with status code 1.
    """
    parser = argparse.ArgumentParser(
        description="Initialize NBA DuckDB database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize database with all tables
  python scripts/populate/init_db.py

  # Show database info
  python scripts/populate/init_db.py --info

  # Force recreate all tables (WARNING: deletes data)
  python scripts/populate/init_db.py --force

  # Create specific tables only
  python scripts/populate/init_db.py --tables player team game
        """,
    )

    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Drop and recreate existing tables (WARNING: deletes data)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific tables to create",
    )
    parser.add_argument(
        "--info",
        action="store_true",
        help="Show database information",
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List available table definitions",
    )

    args = parser.parse_args()

    if args.list_tables:
        for _table_name in sorted(SCHEMA_DEFINITIONS.keys()):
            pass
        return

    if args.info:
        info = get_database_info(args.db)
        if info["exists"]:
            tables_dict: dict[str, Any] = info.get("tables", {})
            for _table, data in sorted(tables_dict.items()):
                if "rows" in data:
                    pass
                else:
                    pass
        return

    try:
        init_database(
            db_path=args.db,
            force=args.force,
            tables=args.tables,
        )
    except Exception as e:
        logger.exception(f"Initialization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
