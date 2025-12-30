"""Database utilities for NBA data population.

This module provides utilities for DuckDB database operations,
including schema management, data insertion, and query utilities.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages DuckDB database operations for NBA data."""

    def __init__(self, db_path: Path | None = None) -> None:
        """Initialize the DatabaseManager and set the DuckDB database path.

        Parameters:
            db_path (Optional[Path]): Path to the DuckDB database file. If omitted, the path is obtained from the application's configuration.
        """
        from src.scripts.populate.config import get_db_path

        self.db_path = db_path or get_db_path()
        self.connection: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Connect to the database.

        Returns:
            DuckDB connection object
        """
        if self.connection is None:
            self.connection = duckdb.connect(str(self.db_path))
            logger.info(f"Connected to database: {self.db_path}")
        return self.connection

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def create_schema(self) -> None:
        """Create the NBA database schema if it doesn't exist."""
        conn = self.connect()

        # Create tables
        self._create_players_table(conn)
        self._create_teams_table(conn)
        self._create_games_table(conn)
        self._create_player_game_stats_table(conn)
        self._create_team_game_stats_table(conn)
        self._create_boxscores_table(conn)
        self._create_play_by_play_table(conn)
        self._create_shot_charts_table(conn)
        self._create_standings_table(conn)
        self._create_tracking_stats_table(conn)
        self._create_hustle_stats_table(conn)

        logger.info("Database schema created/verified")

    def _create_players_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create players table."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS players (
                player_id INTEGER PRIMARY KEY,
                full_name VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                is_active BOOLEAN,
                populated_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def _create_teams_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create teams table."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                team_id INTEGER PRIMARY KEY,
                team_name VARCHAR,
                team_abbreviation VARCHAR,
                team_city VARCHAR,
                team_state VARCHAR,
                year_founded INTEGER,
                populated_at TIMESTAMP
            )
        """)

    def _create_games_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create games table."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS games (
                game_id VARCHAR PRIMARY KEY,
                season_year VARCHAR,
                game_date DATE,
                home_team_id INTEGER,
                away_team_id INTEGER,
                home_team_score INTEGER,
                away_team_score INTEGER,
                game_status VARCHAR,
                game_time VARCHAR,
                populated_at TIMESTAMP
            )
        """)

    def _create_player_game_stats_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create player game statistics table."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS player_game_stats (
                game_id VARCHAR,
                player_id INTEGER,
                team_id INTEGER,
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
                plus_minus INTEGER,
                fantasy_pts DOUBLE,
                double_double BOOLEAN,
                triple_double BOOLEAN,
                populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (game_id, player_id)
            )
        """)

    def _create_team_game_stats_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create team game statistics table."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS team_game_stats (
                game_id VARCHAR,
                team_id INTEGER,
                team_abbreviation VARCHAR,
                team_name VARCHAR,
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
                populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (game_id, team_id)
            )
        """)

    def _create_boxscores_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create box scores table for detailed game statistics."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS boxscores (
                game_id VARCHAR,
                team_id INTEGER,
                player_id INTEGER,
                player_name VARCHAR,
                position VARCHAR,
                minutes VARCHAR,
                field_goals_made INTEGER,
                field_goals_attempted INTEGER,
                field_goals_percentage DOUBLE,
                three_pointers_made INTEGER,
                three_pointers_attempted INTEGER,
                three_pointers_percentage DOUBLE,
                free_throws_made INTEGER,
                free_throws_attempted INTEGER,
                free_throws_percentage DOUBLE,
                rebounds_offensive INTEGER,
                rebounds_defensive INTEGER,
                rebounds_total INTEGER,
                assists INTEGER,
                steals INTEGER,
                blocks INTEGER,
                turnovers INTEGER,
                fouls_personal INTEGER,
                points INTEGER,
                plus_minus INTEGER,
                off_rating DOUBLE,
                def_rating DOUBLE,
                net_rating DOUBLE,
                ast_pct DOUBLE,
                ast_to DOUBLE,
                usg_pct DOUBLE,
                ts_pct DOUBLE,
                efg_pct DOUBLE,
                populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (game_id, team_id, player_id)
            )
        """)

    def _create_play_by_play_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create play-by-play table."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS play_by_play (
                game_id VARCHAR,
                action_number INTEGER,
                clock VARCHAR,
                period INTEGER,
                player_id INTEGER,
                team_id INTEGER,
                team_tricode VARCHAR,
                action_type VARCHAR,
                sub_type VARCHAR,
                descriptor VARCHAR,
                area VARCHAR,
                loc_x INTEGER,
                loc_y INTEGER,
                shot_distance INTEGER,
                option1 VARCHAR,
                option2 VARCHAR,
                option3 VARCHAR,
                option4 VARCHAR,
                description VARCHAR,
                home_description VARCHAR,
                neutral_description VARCHAR,
                visitor_description VARCHAR,
                score VARCHAR,
                score_margin VARCHAR,
                populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (game_id, action_number)
            )
        """)

    def _create_shot_charts_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create shot charts table."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS shot_charts (
                game_id VARCHAR,
                grid_type VARCHAR,
                shot_zone_basic VARCHAR,
                shot_zone_area VARCHAR,
                shot_zone_range VARCHAR,
                shot_distance INTEGER,
                loc_x INTEGER,
                loc_y INTEGER,
                shot_made_flag BOOLEAN,
                player_id INTEGER,
                team_id INTEGER,
                team_name VARCHAR,
                period INTEGER,
                minutes_remaining INTEGER,
                seconds_remaining INTEGER,
                event_type VARCHAR,
                action_type VARCHAR,
                shot_type VARCHAR,
                populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (game_id, player_id, loc_x, loc_y, period, minutes_remaining, seconds_remaining)
            )
        """)

    def _create_standings_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create standings table."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS standings (
                team_id INTEGER,
                league_id VARCHAR,
                season_year VARCHAR,
                team VARCHAR,
                team_city VARCHAR,
                team_name VARCHAR,
                team_abbreviation VARCHAR,
                conference VARCHAR,
                conference_record VARCHAR,
                playoff_rank INTEGER,
                clinch_indicator VARCHAR,
                division VARCHAR,
                division_record VARCHAR,
                division_rank INTEGER,
                wins INTEGER,
                losses INTEGER,
                win_pct DOUBLE,
                league_rank INTEGER,
                record VARCHAR,
                home_record VARCHAR,
                road_record VARCHAR,
                return_to_play_eligibility_flag BOOLEAN,
                populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (team_id, season_year)
            )
        """)

    def _create_tracking_stats_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create player tracking statistics table."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tracking_stats (
                player_id INTEGER,
                player_name VARCHAR,
                team_id INTEGER,
                team_abbreviation VARCHAR,
                season_year VARCHAR,
                season_type VARCHAR,
                age DOUBLE,
                games_played INTEGER,
                wins INTEGER,
                losses INTEGER,
                minutes DOUBLE,
                speed DOUBLE,
                distance DOUBLE,
                touches DOUBLE,
                secondary_assists DOUBLE,
                free_throw_assists DOUBLE,
                passes DOUBLE,
                assist_points_created DOUBLE,
                time_of_possession DOUBLE,
                drives DOUBLE,
                points_per_drive DOUBLE,
                pass_pct DOUBLE,
                shoot_pct DOUBLE,
                turnover_pct DOUBLE,
                foul_pct DOUBLE,
                populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (player_id, team_id, season_year, season_type)
            )
        """)

    def _create_hustle_stats_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create hustle statistics table."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hustle_stats (
                player_id INTEGER,
                player_name VARCHAR,
                team_id INTEGER,
                team_abbreviation VARCHAR,
                season_year VARCHAR,
                season_type VARCHAR,
                age DOUBLE,
                games_played INTEGER,
                minutes DOUBLE,
                deflections DOUBLE,
                deflections_per_game DOUBLE,
                loose_balls_recovered DOUBLE,
                loose_balls_recovered_per_game DOUBLE,
                charges_drawn DOUBLE,
                charges_drawn_per_game DOUBLE,
                screen_assists DOUBLE,
                screen_assist_points DOUBLE,
                screen_assists_per_game DOUBLE,
                screen_assist_points_per_game DOUBLE,
                populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (player_id, team_id, season_year, season_type)
            )
        """)

    def insert_data(
        self,
        table_name: str,
        df: pd.DataFrame,
        mode: str = "append",
    ) -> int:
        """Insert data into a table.

        Args:
            table_name: Name of the table
            df: DataFrame to insert
            mode: Insert mode ("append" or "replace")

        Returns:
            Number of rows inserted
        """
        conn = self.connect()

        if df.empty:
            logger.warning(f"Attempted to insert empty DataFrame into {table_name}")
            return 0

        try:
            # Use DuckDB's native DataFrame insertion
            if mode == "replace":
                conn.execute(f"DELETE FROM {table_name}")

            conn.register("temp_df", df)
            conn.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM temp_df
            """).fetchall()

            conn.unregister("temp_df")

            logger.info(f"Inserted {len(df)} rows into {table_name}")
            return len(df)

        except Exception as e:
            logger.exception(f"Error inserting data into {table_name}: {e}")
            raise

    def upsert_data(
        self,
        table_name: str,
        df: pd.DataFrame,
        key_columns: list[str],
    ) -> int:
        """Upsert data into a table (insert or update if exists).

        Args:
            table_name: Name of the table
            df: DataFrame to upsert
            key_columns: List of columns that form the primary key

        Returns:
            Number of rows affected
        """
        conn = self.connect()

        if df.empty:
            logger.warning(f"Attempted to upsert empty DataFrame into {table_name}")
            return 0

        try:
            # Create temporary table
            temp_table = f"temp_{table_name}_{int(datetime.now().timestamp())}"
            conn.register(temp_table, df)

            # Build WHERE clause for key matching
            where_clause = " AND ".join([f"t.{col} = s.{col}" for col in key_columns])

            # Get column names from DataFrame
            all_columns = list(df.columns)
            update_columns = [col for col in all_columns if col not in key_columns]

            if not update_columns:
                # Only key columns, do simple insert
                conn.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM {temp_table}
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {table_name} t
                        WHERE {where_clause}
                    )
                """).fetchall()
            else:
                # Build UPDATE clause
                update_clause = ", ".join(
                    [f"{col} = s.{col}" for col in update_columns],
                )

                # Perform upsert
                conn.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM {temp_table}
                    ON CONFLICT ({", ".join(key_columns)})
                    DO UPDATE SET {update_clause}
                """).fetchall()

            conn.unregister(temp_table)

            logger.info(f"Upserted {len(df)} rows into {table_name}")
            return len(df)

        except Exception as e:
            logger.exception(f"Error upserting data into {table_name}: {e}")
            raise

    def get_table_info(self, table_name: str) -> dict[str, Any]:
        """Get information about a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table information
        """
        conn = self.connect()

        try:
            # Get column information
            columns = conn.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchall()

            # Get row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

            # Get min/max dates if applicable
            date_info = {}
            for col_info in columns:
                col_name, data_type, _ = col_info
                if "date" in data_type.lower() or "timestamp" in data_type.lower():
                    try:
                        date_range = conn.execute(f"""
                            SELECT MIN({col_name}), MAX({col_name})
                            FROM {table_name}
                            WHERE {col_name} IS NOT NULL
                        """).fetchone()
                        if date_range and date_range[0]:
                            date_info[col_name] = {
                                "min": str(date_range[0]),
                                "max": str(date_range[1]),
                            }
                    except Exception:
                        pass

            return {
                "table_name": table_name,
                "row_count": row_count,
                "columns": [col[0] for col in columns],
                "column_details": [
                    {"name": col[0], "type": col[1], "nullable": col[2]}
                    for col in columns
                ],
                "date_ranges": date_info,
            }

        except Exception as e:
            logger.exception(f"Error getting table info for {table_name}: {e}")
            raise

    def get_missing_data(self, table_name: str, season: str) -> list[str]:
        """Get list of missing data for a season.

        Args:
            table_name: Name of the table
            season: Season to check

        Returns:
            List of missing identifiers (games, players, etc.)
        """
        conn = self.connect()

        try:
            # This is a generic implementation - customize based on table structure
            if table_name == "player_game_stats":
                # Get games that don't have player stats
                missing_games = conn.execute(f"""
                    SELECT DISTINCT g.game_id
                    FROM games g
                    LEFT JOIN player_game_stats pgs ON g.game_id = pgs.game_id
                    WHERE g.season_year = '{season}'
                    AND pgs.game_id IS NULL
                """).fetchall()
                return [row[0] for row in missing_games]

            if table_name == "boxscores":
                # Get games that don't have box scores
                missing_games = conn.execute(f"""
                    SELECT DISTINCT g.game_id
                    FROM games g
                    LEFT JOIN boxscores b ON g.game_id = b.game_id
                    WHERE g.season_year = '{season}'
                    AND b.game_id IS NULL
                """).fetchall()
                return [row[0] for row in missing_games]

            logger.warning(f"Missing data check not implemented for {table_name}")
            return []

        except Exception as e:
            logger.exception(f"Error checking missing data for {table_name}: {e}")
            return []

    def cleanup_old_data(self, table_name: str, days_old: int = 30) -> int:
        """Clean up old data from a table.

        Args:
            table_name: Name of the table
            days_old: Age in days to consider for cleanup

        Returns:
            Number of rows deleted
        """
        conn = self.connect()

        try:
            # Use pandas to calculate the cutoff safely
            cutoff_date = datetime.now() - pd.Timedelta(days=days_old)

            # Use parameterized query to avoid isoformat() and SQL injection
            conn.execute(
                f"DELETE FROM {table_name} WHERE populated_at < ?", [cutoff_date]
            )

            deleted_count: int = conn.execute("SELECT changes()").fetchone()[0]
            logger.info(f"Deleted {deleted_count} old records from {table_name}")
            return deleted_count

        except Exception as e:
            logger.exception(f"Error cleaning up old data from {table_name}: {e}")
            return 0

    def get_database_stats(self) -> dict[str, Any]:
        """Get overall database statistics.

        Returns:
            Dictionary with database statistics
        """
        conn = self.connect()

        try:
            # Get all tables
            tables = conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
            """).fetchall()

            table_stats = {}
            total_rows = 0

            for table_row in tables:
                table_name = table_row[0]
                try:
                    # Get row count
                    row_count = conn.execute(
                        f"SELECT COUNT(*) FROM {table_name}",
                    ).fetchone()[0]
                    total_rows += row_count

                    # Get table info
                    table_info = self.get_table_info(table_name)
                    table_stats[table_name] = {
                        "row_count": row_count,
                        "columns": len(table_info["columns"]),
                        "date_range": table_info.get("date_ranges", {}),
                    }
                except Exception as e:
                    logger.warning(f"Could not get stats for {table_name}: {e}")

            return {
                "database_path": str(self.db_path),
                "table_count": len(tables),
                "total_rows": total_rows,
                "tables": table_stats,
                "generated_at": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.exception(f"Error getting database stats: {e}")
            return {}


def create_database_backup(db_path: Path, backup_dir: Path | None = None) -> Path:
    """Create a backup of the database.

    Args:
        db_path: Path to the database file
        backup_dir: Directory for backup (creates timestamped subdirectory)

    Returns:
        Path to the backup file
    """
    import shutil
    from datetime import datetime

    if backup_dir is None:
        backup_dir = db_path.parent / "backups"

    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"nba_backup_{timestamp}.duckdb"

    try:
        shutil.copy2(db_path, backup_path)
        logger.info(f"Database backed up to: {backup_path}")
        return backup_path
    except Exception as e:
        logger.exception(f"Failed to create database backup: {e}")
        raise


def restore_database_backup(backup_path: Path, target_path: Path | None = None) -> Path:
    """Restore a database from backup.

    Args:
        backup_path: Path to the backup file
        target_path: Target path (defaults to original location)

    Returns:
        Path to the restored database
    """
    import shutil

    if target_path is None:
        # Assume original location (remove backup timestamp)
        target_path = backup_path.parent.parent / "nba.duckdb"

    try:
        # Create backup of current database if it exists
        if target_path.exists():
            current_backup = (
                target_path.parent
                / f"current_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
            )
            shutil.copy2(target_path, current_backup)
            logger.info(f"Current database backed up to: {current_backup}")

        # Restore from backup
        shutil.copy2(backup_path, target_path)
        logger.info(f"Database restored from: {backup_path}")
        return target_path

    except Exception as e:
        logger.exception(f"Failed to restore database backup: {e}")
        raise
