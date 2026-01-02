#!/usr/bin/env python3
"""Create advanced basketball metrics views and tables in the NBA DuckDB database.

This script creates SQL views and tables for computing advanced NBA statistics,
rolling averages, and standings.
"""

import sys
import duckdb
import logging

logger = logging.getLogger(__name__)

def create_advanced_metrics(db_path: str = "src/backend/data/nba.duckdb") -> None:
    """Create advanced NBA metrics views and tables.
    
    Args:
        db_path: Path to the DuckDB database file.
    """
    conn = duckdb.connect(db_path)
    
    try:
        # 1. PLAYER GAME ADVANCED METRICS VIEW
        conn.execute("""
            CREATE OR REPLACE VIEW player_game_advanced AS
            SELECT 
                pgs.*,
                -- True Shooting Percentage (TS%)
                CASE 
                    WHEN (pgs.fga + 0.44 * pgs.fta) > 0 
                    THEN pgs.pts / (2.0 * (pgs.fga + 0.44 * pgs.fta)) 
                    ELSE 0 
                END as ts_pct,
                -- Effective Field Goal Percentage (eFG%)
                CASE 
                    WHEN pgs.fga > 0 
                    THEN (pgs.fgm + 0.5 * pgs.fg3m) / pgs.fga 
                    ELSE 0 
                END as efg_pct,
                -- Game Score
                pgs.pts + 0.4 * pgs.fgm - 0.7 * pgs.fga - 0.4 * (pgs.fta - pgs.ftm) + 0.7 * pgs.oreb + 0.3 * pgs.dreb + pgs.stl + 0.7 * pgs.ast + 0.7 * pgs.blk - 0.4 * pgs.pf - pgs.tov as game_score
            FROM player_game_stats pgs
        """)

        # 2. TEAM GAME ADVANCED METRICS VIEW
        conn.execute("""
            CREATE OR REPLACE VIEW team_game_advanced AS
            SELECT 
                tgs.*,
                -- Points Allowed (calculated from games table)
                CASE 
                    WHEN tgs.is_home THEN g.visitor_pts 
                    ELSE g.home_pts 
                END as pts_allowed,
                -- Effective Field Goal Percentage (eFG%)
                CASE 
                    WHEN tgs.fga > 0 
                    THEN (tgs.fgm + 0.5 * tgs.fg3m) / tgs.fga 
                    ELSE 0 
                END as efg_pct,
                -- True Shooting Percentage (TS%)
                CASE 
                    WHEN (tgs.fga + 0.44 * tgs.fta) > 0 
                    THEN tgs.pts / (2.0 * (tgs.fga + 0.44 * tgs.fta)) 
                    ELSE 0 
                END as ts_pct
            FROM team_game_stats tgs
            JOIN games g ON tgs.game_id = g.game_id
        """)

        # 3. TEAM ROLLING METRICS (Table for performance)
        conn.execute("DROP TABLE IF EXISTS team_rolling_metrics")
        conn.execute("""
            CREATE TABLE team_rolling_metrics AS
            WITH team_stats AS (
                SELECT 
                    tgs.*,
                    CASE 
                        WHEN tgs.is_home THEN g.visitor_pts 
                        ELSE g.home_pts 
                    END as pts_allowed,
                    CASE WHEN tgs.plus_minus > 0 THEN 1 ELSE 0 END as is_win
                FROM team_game_stats tgs
                JOIN games g ON tgs.game_id = g.game_id
            )
            SELECT 
                team_id,
                game_id,
                game_date,
                season_id,
                pts,
                pts_allowed,
                is_win,
                AVG(pts) OVER (PARTITION BY team_id, season_id ORDER BY game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as rolling_pts_avg,
                AVG(pts_allowed) OVER (PARTITION BY team_id, season_id ORDER BY game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as rolling_pts_allowed_avg,
                AVG(is_win) OVER (PARTITION BY team_id, season_id ORDER BY game_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as rolling_win_pct
            FROM team_stats
        """)

        # 4. PLAYER SEASON AVERAGES (Table)
        conn.execute("DROP TABLE IF EXISTS player_season_averages")
        conn.execute("""
            CREATE TABLE player_season_averages AS
            SELECT 
                pgs.player_id,
                pgs.player_name,
                g.season_id,
                COUNT(*) as games_played,
                AVG(pgs.pts) as ppg,
                AVG(pgs.reb) as rpg,
                AVG(pgs.ast) as apg,
                AVG(pgs.stl) as spg,
                AVG(pgs.blk) as bpg,
                AVG(pgs.tov) as topg,
                SUM(pgs.fgm) / NULLIF(SUM(pgs.fga), 0) as fg_pct,
                SUM(pgs.fg3m) / NULLIF(SUM(pgs.fg3a), 0) as fg3_pct,
                SUM(pgs.ftm) / NULLIF(SUM(pgs.fta), 0) as ft_pct,
                AVG(pgs.pts + 0.4 * pgs.fgm - 0.7 * pgs.fga - 0.4 * (pgs.fta - pgs.ftm) + 0.7 * pgs.oreb + 0.3 * pgs.dreb + pgs.stl + 0.7 * pgs.ast + 0.7 * pgs.blk - 0.4 * pgs.pf - pgs.tov) as avg_game_score
            FROM player_game_stats pgs
            JOIN games g ON pgs.game_id = g.game_id
            GROUP BY pgs.player_id, pgs.player_name, g.season_id
        """)

        # 5. TEAM STANDINGS (Table)
        conn.execute("DROP TABLE IF EXISTS team_standings")
        conn.execute("""
            CREATE TABLE team_standings AS
            WITH team_results AS (
                SELECT 
                    tgs.team_id,
                    tg.full_name as team_name,
                    tgs.season_id,
                    CASE WHEN tgs.plus_minus > 0 THEN 1 ELSE 0 END as is_win
                FROM team_game_stats tgs
                JOIN team_gold tg ON tgs.team_id = tg.id
            )
            SELECT 
                team_id,
                team_name,
                season_id,
                COUNT(*) as games_played,
                SUM(is_win) as wins,
                COUNT(*) - SUM(is_win) as losses,
                CAST(SUM(is_win) AS DOUBLE) / COUNT(*) as win_pct
            FROM team_results
            GROUP BY team_id, team_name, season_id
            ORDER BY season_id DESC, win_pct DESC
        """)

        conn.commit()
        logger.info("Advanced metrics creation complete.")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating advanced metrics: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db_path = sys.argv[1] if len(sys.argv) > 1 else "src/backend/data/nba.duckdb"
    create_advanced_metrics(db_path)
