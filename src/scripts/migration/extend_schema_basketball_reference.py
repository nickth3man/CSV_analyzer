#!/usr/bin/env python3
"""Database schema extensions for basketball reference data.

This script creates new tables for:
1. Team standings data (historical standings)
2. Player box scores (detailed game performance)
3. Team schedules (complete season schedules)
4. Injury reports (player injury tracking)
"""

import logging

import duckdb

from src.scripts.populate.config import get_db_path


logger = logging.getLogger(__name__)


def create_team_standings_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create team standings table for historical standings data."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS team_standings (
            season_id TEXT NOT NULL,
            team_id INTEGER NOT NULL,
            team_name TEXT NOT NULL,
            team_abbreviation TEXT NOT NULL,
            conference TEXT NOT NULL,
            division TEXT,
            wins INTEGER NOT NULL,
            losses INTEGER NOT NULL,
            win_percentage FLOAT,
            games_back FLOAT,
            home_record TEXT,
            away_record TEXT,
            division_record TEXT,
            conference_record TEXT,
            streak TEXT,
            last_10_record TEXT,
            playoff_seed INTEGER,
            clinched_playoffs BOOLEAN,
            clinched_division BOOLEAN,
            clinched_conference BOOLEAN,
            clinched_play_in BOOLEAN,
            eliminated BOOLEAN,
            points_per_game FLOAT,
            opponent_points_per_game FLOAT,
            pace FLOAT,
            offensive_rating FLOAT,
            defensive_rating FLOAT,
            net_rating FLOAT,
            strength_of_schedule FLOAT,
            simple_rating_system FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (season_id, team_id)
        )
    """)
    logger.info("Created team_standings table")


def create_player_box_scores_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create player box scores table for detailed game performance."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_box_scores (
            game_id TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            team_abbreviation TEXT NOT NULL,
            opponent_team_id INTEGER NOT NULL,
            opponent_team_abbreviation TEXT NOT NULL,
            game_date DATE NOT NULL,
            season_id TEXT NOT NULL,
            is_home_game BOOLEAN NOT NULL,
            is_starter BOOLEAN,
            minutes_played INTEGER,
            field_goals_made INTEGER,
            field_goals_attempted INTEGER,
            field_goal_percentage FLOAT,
            three_pointers_made INTEGER,
            three_pointers_attempted INTEGER,
            three_point_percentage FLOAT,
            free_throws_made INTEGER,
            free_throws_attempted INTEGER,
            free_throw_percentage FLOAT,
            offensive_rebounds INTEGER,
            defensive_rebounds INTEGER,
            total_rebounds INTEGER,
            assists INTEGER,
            steals INTEGER,
            blocks INTEGER,
            turnovers INTEGER,
            personal_fouls INTEGER,
            points INTEGER,
            plus_minus INTEGER,
            double_doubles INTEGER,
            triple_doubles INTEGER,
            technical_fouls INTEGER,
            flagrant_fouls INTEGER,
            ejections INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (game_id, player_id)
        )
    """)
    logger.info("Created player_box_scores table")


def create_team_schedules_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create team schedules table for complete season schedules."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS team_schedules (
            game_id TEXT NOT NULL,
            season_id TEXT NOT NULL,
            game_date DATE NOT NULL,
            home_team_id INTEGER NOT NULL,
            home_team_abbreviation TEXT NOT NULL,
            away_team_id INTEGER NOT NULL,
            away_team_abbreviation TEXT NOT NULL,
            home_team_score INTEGER,
            away_team_score INTEGER,
            game_status TEXT NOT NULL,
            game_time TEXT,
            attendance INTEGER,
            arena_name TEXT,
            arena_city TEXT,
            arena_state TEXT,
            arena_country TEXT,
            arena_capacity INTEGER,
            broadcast_network TEXT,
            broadcast_region TEXT,
            overtime_periods INTEGER,
            playoff_series_id TEXT,
            playoff_series_game_number INTEGER,
            playoff_series_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (game_id)
        )
    """)
    logger.info("Created team_schedules table")


def create_injury_reports_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create injury reports table for player injury tracking."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS injury_reports (
            injury_id TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            team_id INTEGER NOT NULL,
            team_abbreviation TEXT NOT NULL,
            injury_status TEXT NOT NULL,
            injury_type TEXT,
            injury_location TEXT,
            injury_description TEXT,
            injury_date DATE,
            expected_return_date DATE,
            games_missed INTEGER,
            season_id TEXT NOT NULL,
            is_active BOOLEAN NOT NULL,
            notes TEXT,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (injury_id)
        )
    """)
    logger.info("Created injury_reports table")


def create_basketball_reference_tables() -> None:
    """Create all basketball reference tables."""
    db_path = get_db_path()
    logger.info(f"Connecting to database at {db_path}")

    conn = duckdb.connect(str(db_path))

    try:
        # Create tables
        create_team_standings_table(conn)
        create_player_box_scores_table(conn)
        create_team_schedules_table(conn)
        create_injury_reports_table(conn)

        # Create indexes for performance
        logger.info("Creating indexes for performance...")

        # Team standings indexes
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_team_standings_season ON team_standings(season_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_team_standings_team ON team_standings(team_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_team_standings_conference ON team_standings(conference)"
        )

        # Player box scores indexes
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_player_box_scores_game ON player_box_scores(game_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_player_box_scores_player ON player_box_scores(player_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_player_box_scores_team ON player_box_scores(team_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_player_box_scores_date ON player_box_scores(game_date)"
        )

        # Team schedules indexes
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_team_schedules_season ON team_schedules(season_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_team_schedules_date ON team_schedules(game_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_team_schedules_home_team ON team_schedules(home_team_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_team_schedules_away_team ON team_schedules(away_team_id)"
        )

        # Injury reports indexes
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_injury_reports_player ON injury_reports(player_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_injury_reports_team ON injury_reports(team_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_injury_reports_status ON injury_reports(injury_status)"
        )

        conn.commit()
        logger.info("Successfully created all basketball reference tables and indexes")

    except Exception as e:
        conn.rollback()
        logger.exception(f"Error creating basketball reference tables: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    create_basketball_reference_tables()
