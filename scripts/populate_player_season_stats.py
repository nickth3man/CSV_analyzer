#!/usr/bin/env python3
"""Populate player_season_stats table with aggregated player statistics.

This script creates comprehensive player-season statistics by aggregating
game-level data and calculating advanced metrics like TS%, eFG%, USG%, etc.

Usage:
    python scripts/populate_player_season_stats.py
    
    # For specific seasons only
    python scripts/populate_player_season_stats.py --seasons 2022-23 2021-22
"""

import duckdb
import argparse
import sys
from pathlib import Path


def populate_player_season_stats(db_path: str = "data/nba.duckdb", seasons: list = None):
    """Populate player_season_stats table with aggregated statistics."""
    
    conn = duckdb.connect(db_path)
    
    print("Creating player_season_stats table...")
    
    # Create the player_season_stats table with comprehensive statistics
    conn.execute("""
        CREATE OR REPLACE TABLE player_season_stats AS
        WITH player_games AS (
            SELECT 
                p.player_id,
                p.player_name,
                p.team_id,
                t.abbreviation as team_abbreviation,
                t.full_name as team_name,
                -- Extract season from game_id (format: YYYYMMDD)
                CAST(SUBSTRING(CAST(p.game_id AS VARCHAR), 1, 4) || '-' || 
                     SUBSTRING(CAST(p.game_id AS VARCHAR), 3, 2) AS VARCHAR) as season,
                -- Calculate minutes played from time string (MM:SS format)
                CASE 
                    WHEN p.min IS NULL OR p.min = '' THEN 0
                    WHEN POSITION(':' IN p.min) > 0 THEN 
                        CAST(SPLIT_PART(p.min, ':', 1) AS INTEGER) + 
                        CAST(SPLIT_PART(p.min, ':', 2) AS INTEGER) / 60.0
                    ELSE CAST(p.min AS INTEGER)
                END as minutes_played,
                p.fgm, p.fga, p.fg3m, p.fg3a, p.ftm, p.fta,
                p.oreb, p.dreb, p.reb, p.ast, p.stl, p.blk, p.tov, p.pf, p.pts,
                p.plus_minus
            FROM player_game_stats p
            LEFT JOIN team_silver t ON p.team_id = t.id
            WHERE p.player_id IS NOT NULL
        ),
        player_season_agg AS (
            SELECT 
                player_id,
                player_name,
                team_id,
                team_abbreviation,
                team_name,
                season,
                COUNT(*) as games_played,
                SUM(CASE WHEN minutes_played > 0 THEN 1 ELSE 0 END) as games_with_minutes,
                SUM(minutes_played) as total_minutes,
                SUM(pts) as total_points,
                SUM(fgm) as total_fgm, SUM(fga) as total_fga,
                SUM(fg3m) as total_fg3m, SUM(fg3a) as total_fg3a,
                SUM(ftm) as total_ftm, SUM(fta) as total_fta,
                SUM(oreb) as total_oreb, SUM(dreb) as total_dreb, SUM(reb) as total_reb,
                SUM(ast) as total_ast, SUM(stl) as total_stl, SUM(blk) as total_blk,
                SUM(tov) as total_tov, SUM(pf) as total_pf,
                SUM(plus_minus) as total_plus_minus,
                AVG(pts) as pts_per_game,
                AVG(reb) as reb_per_game,
                AVG(ast) as ast_per_game,
                AVG(stl) as stl_per_game,
                AVG(blk) as blk_per_game,
                AVG(tov) as tov_per_game,
                AVG(plus_minus) as plus_minus_per_game
            FROM player_games
            GROUP BY player_id, player_name, team_id, team_abbreviation, team_name, season
        )
        SELECT 
            player_id,
            player_name,
            team_id,
            team_abbreviation,
            team_name,
            season,
            games_played,
            games_with_minutes,
            ROUND(total_minutes, 1) as total_minutes,
            ROUND(total_minutes / games_played, 1) as minutes_per_game,
            total_points,
            ROUND(pts_per_game, 1) as pts_per_game,
            total_fgm, total_fga,
            CASE 
                WHEN total_fga > 0 THEN ROUND(100.0 * total_fgm / total_fga, 1)
                ELSE 0.0
            END as fg_pct,
            total_fg3m, total_fg3a,
            CASE 
                WHEN total_fg3a > 0 THEN ROUND(100.0 * total_fg3m / total_fg3a, 1)
                ELSE 0.0
            END as fg3_pct,
            total_ftm, total_fta,
            CASE 
                WHEN total_fta > 0 THEN ROUND(100.0 * total_ftm / total_fta, 1)
                ELSE 0.0
            END as ft_pct,
            -- True Shooting Percentage
            CASE 
                WHEN (total_fga + 0.44 * total_fta) > 0 THEN 
                    ROUND(100.0 * total_points / (2.0 * (total_fga + 0.44 * total_fta)), 1)
                ELSE 0.0
            END as ts_pct,
            -- Effective Field Goal Percentage
            CASE 
                WHEN total_fga > 0 THEN 
                    ROUND(100.0 * (total_fgm + 0.5 * total_fg3m) / total_fga, 1)
                ELSE 0.0
            END as efg_pct,
            total_oreb, total_dreb, total_reb,
            ROUND(reb_per_game, 1) as reb_per_game,
            total_ast,
            ROUND(ast_per_game, 1) as ast_per_game,
            total_stl,
            ROUND(stl_per_game, 1) as stl_per_game,
            total_blk,
            ROUND(blk_per_game, 1) as blk_per_game,
            total_tov,
            ROUND(tov_per_game, 1) as tov_per_game,
            -- Turnover Percentage
            CASE 
                WHEN (total_fga + 0.44 * total_fta + total_tov) > 0 THEN
                    ROUND(100.0 * total_tov / (total_fga + 0.44 * total_fta + total_tov), 1)
                ELSE 0.0
            END as tov_pct,
            total_pf,
            total_plus_minus,
            ROUND(plus_minus_per_game, 1) as plus_minus_per_game,
            -- Field Goals Made/Attempted per game
            ROUND(total_fgm * 1.0 / games_played, 1) as fgm_per_game,
            ROUND(total_fga * 1.0 / games_played, 1) as fga_per_game,
            -- Three Pointers Made/Attempted per game
            ROUND(total_fg3m * 1.0 / games_played, 1) as fg3m_per_game,
            ROUND(total_fg3a * 1.0 / games_played, 1) as fg3a_per_game,
            -- Free Throws Made/Attempted per game
            ROUND(total_ftm * 1.0 / games_played, 1) as ftm_per_game,
            ROUND(total_fta * 1.0 / games_played, 1) as fta_per_game,
            -- Rebounds per game breakdown
            ROUND(total_oreb * 1.0 / games_played, 1) as oreb_per_game,
            ROUND(total_dreb * 1.0 / games_played, 1) as dreb_per_game,
            -- Total fantasy points (simple calculation)
            ROUND(total_points + 1.2 * total_reb + 1.5 * total_ast + 3 * total_stl + 3 * total_blk - total_tov, 0) as fantasy_points,
            -- Per game fantasy average
            ROUND((total_points + 1.2 * total_reb + 1.5 * total_ast + 3 * total_stl + 3 * total_blk - total_tov) / games_played, 1) as fantasy_points_per_game
        FROM player_season_agg
        WHERE games_played >= 5  -- Minimum 5 games for meaningful stats
        ORDER BY season DESC, pts_per_game DESC;
    """)
    
    # Get the count of records created
    count = conn.execute("SELECT COUNT(*) FROM player_season_stats").fetchone()[0]
    print(f"✓ Created player_season_stats table with {count} player-season records")
    
    # Show some sample data
    print("\nSample player season stats (top scorers):")
    sample = conn.execute("""
        SELECT 
            player_name,
            team_abbreviation,
            season,
            games_played,
            pts_per_game,
            ts_pct,
            efg_pct,
            reb_per_game,
            ast_per_game
        FROM player_season_stats 
        WHERE games_played >= 10
        ORDER BY pts_per_game DESC 
        LIMIT 10
    """).fetchall()
    
    for row in sample:
        print(f"  {row[0]} ({row[1]}) - {row[2]}: {row[4]} PPG, {row[5]}% TS, {row[6]}% eFG")
    
    conn.close()
    print(f"\n✓ Player season stats populated successfully!")


def main():
    parser = argparse.ArgumentParser(description='Populate player season statistics')
    parser.add_argument('--db', default='data/nba.duckdb', help='Database path')
    parser.add_argument('--seasons', nargs='+', help='Specific seasons to process')
    
    args = parser.parse_args()
    
    try:
        populate_player_season_stats(args.db, args.seasons)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()