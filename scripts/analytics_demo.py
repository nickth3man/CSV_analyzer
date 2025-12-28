#!/usr/bin/env python3
"""NBA Database Analytics Demo - Showcase the transformed database capabilities.

This script demonstrates the advanced analytics now possible with our
comprehensive NBA DuckDB database.
"""

import duckdb
import pandas as pd
from pathlib import Path


def run_analytics_demo(db_path: str = "data/nba.duckdb"):
    """Run comprehensive analytics demonstrations."""
    
    print("🏀 NBA DATABASE ANALYTICS DEMO")
    print("=" * 50)
    
    conn = duckdb.connect(db_path)
    
    # 1. Database Overview
    print("\n📊 DATABASE OVERVIEW")
    print("-" * 30)
    
    tables = conn.execute("""
        SELECT table_name, 
               COUNT(*) as row_count
        FROM information_schema.tables 
        JOIN (SELECT table_name as tn, COUNT(*) as cnt FROM information_schema.columns GROUP BY table_name) 
        ON table_name = tn
        WHERE table_schema = 'main'
        GROUP BY table_name
        ORDER BY row_count DESC
        LIMIT 10
    """).fetchall()
    
    for table, count in tables:
        actual_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if actual_count > 0:
            print(f"  {table}: {actual_count:,} records")
    
    # 2. Top Players by Advanced Metrics
    print("\n🌟 TOP PLAYERS BY ADVANCED METRICS")
    print("-" * 40)
    
    top_players = conn.execute("""
        SELECT 
            player_name,
            season,
            games_played,
            ROUND(pts_per_game, 1) as ppg,
            ROUND(ts_pct, 1) as ts_pct,
            ROUND(efg_pct, 1) as efg_pct,
            ROUND(reb_per_game, 1) as rpg,
            ROUND(ast_per_game, 1) as apg
        FROM player_season_stats 
        WHERE games_played >= 15
        ORDER BY pts_per_game DESC 
        LIMIT 8
    """).fetchall()
    
    print(f"{'Player':<20} {'Season':<8} {'GP':<3} {'PPG':<5} {'TS%':<5} {'EFG%':<5} {'RPG':<5} {'APG':<5}")
    print("-" * 60)
    for player in top_players:
        print(f"{player[0]:<20} {player[1]:<8} {player[2]:<3} {player[3]:<5} {player[4]:<5} {player[5]:<5} {player[6]:<5} {player[7]:<5}")
    
    # 3. Shooting Efficiency Leaders
    print("\n🎯 SHOOTING EFFICIENCY LEADERS")
    print("-" * 35)
    
    efficient_players = conn.execute("""
        SELECT 
            player_name,
            season,
            games_played,
            ROUND(pts_per_game, 1) as ppg,
            ROUND(ts_pct, 1) as ts_pct,
            ROUND(ft_pct, 1) as ft_pct,
            ROUND(fg3_pct, 1) as fg3_pct
        FROM player_season_stats 
        WHERE games_played >= 15 AND pts_per_game >= 15
        ORDER BY ts_pct DESC 
        LIMIT 8
    """).fetchall()
    
    print(f"{'Player':<20} {'Season':<8} {'GP':<3} {'PPG':<5} {'TS%':<5} {'FT%':<5} {'3P%':<5}")
    print("-" * 55)
    for player in efficient_players:
        print(f"{player[0]:<20} {player[1]:<8} {player[2]:<3} {player[3]:<5} {player[4]:<5} {player[5]:<5} {player[6]:<5}")
    
    # 4. Game-Level Advanced Stats
    print("\n📈 GAME-LEVEL ADVANCED STATS")
    print("-" * 32)
    
    advanced_stats = conn.execute("""
        SELECT 
            player_name,
            COUNT(*) as games,
            ROUND(AVG(ts_pct), 1) as avg_ts_pct,
            ROUND(AVG(efg_pct), 1) as avg_efg_pct,
            ROUND(AVG(tov_pct), 1) as avg_tov_pct,
            SUM(is_triple_double) as triple_doubles
        FROM player_game_advanced 
        GROUP BY player_name
        HAVING COUNT(*) >= 10
        ORDER BY avg_ts_pct DESC 
        LIMIT 8
    """).fetchall()
    
    print(f"{'Player':<20} {'Games':<6} {'TS%':<5} {'EFG%':<5} {'TOV%':<6} {'TD':<3}")
    print("-" * 50)
    for player in advanced_stats:
        print(f"{player[0]:<20} {player[1]:<6} {player[2]:<5} {player[3]:<5} {player[4]:<6} {player[5]:<3}")
    
    # 5. Team Performance
    print("\n🏆 TEAM PERFORMANCE")
    print("-" * 22)
    
    team_stats = conn.execute("""
        SELECT 
            t.abbreviation as team,
            COUNT(*) as games,
            ROUND(AVG(tgs.pts), 1) as avg_points,
            ROUND(AVG(tgs.fg_pct), 3) as avg_fg_pct,
            ROUND(AVG(tgs.fg3_pct), 3) as avg_fg3_pct,
            ROUND(AVG(tgs.plus_minus), 1) as avg_point_diff
        FROM team_game_stats tgs
        JOIN team_silver t ON tgs.team_id = t.id
        GROUP BY t.abbreviation
        ORDER BY avg_point_diff DESC
        LIMIT 10
    """).fetchall()
    
    print(f"{'Team':<5} {'GP':<4} {'PPG':<6} {'FG%':<6} {'3P%':<6} {'+/-':<6}")
    print("-" * 35)
    for team in team_stats:
        print(f"{team[0]:<5} {team[1]:<4} {team[2]:<6} {team[3]:<6} {team[4]:<6} {team[5]:<6}")
    
    # 6. Database Summary Statistics
    print("\n📊 DATABASE SUMMARY")
    print("-" * 25)
    
    summary_stats = {
        'Total Games': conn.execute("SELECT COUNT(DISTINCT game_id) FROM player_game_stats").fetchone()[0],
        'Total Players': conn.execute("SELECT COUNT(DISTINCT player_id) FROM player_game_stats").fetchone()[0],
        'Total Player Seasons': conn.execute("SELECT COUNT(*) FROM player_season_stats").fetchone()[0],
        'Seasons Covered': conn.execute("SELECT COUNT(DISTINCT season) FROM player_season_stats").fetchone()[0],
        'Avg Points/Game': round(conn.execute("SELECT AVG(pts) FROM player_game_stats").fetchone()[0], 1),
        'Triple Doubles': conn.execute("SELECT SUM(is_triple_double) FROM player_game_advanced").fetchone()[0],
    }
    
    for stat, value in summary_stats.items():
        print(f"  {stat}: {value:,}")
    
    # 7. Sample Complex Query - Player Comparison
    print("\n🔍 PLAYER COMPARISON EXAMPLE")
    print("-" * 35)
    
    comparison = conn.execute("""
        SELECT 
            'LeBron James' as player,
            COUNT(*) as games,
            ROUND(AVG(pts), 1) as avg_pts,
            ROUND(AVG(ts_pct), 1) as avg_ts,
            ROUND(AVG(reb), 1) as avg_reb,
            ROUND(AVG(ast), 1) as avg_ast
        FROM player_game_advanced 
        WHERE player_name = 'LeBron James'
        
        UNION ALL
        
        SELECT 
            'Stephen Curry' as player,
            COUNT(*) as games,
            ROUND(AVG(pts), 1) as avg_pts,
            ROUND(AVG(ts_pct), 1) as avg_ts,
            ROUND(AVG(reb), 1) as avg_reb,
            ROUND(AVG(ast), 1) as avg_ast
        FROM player_game_advanced 
        WHERE player_name = 'Stephen Curry'
        
        UNION ALL
        
        SELECT 
            'Giannis Antetokounmpo' as player,
            COUNT(*) as games,
            ROUND(AVG(pts), 1) as avg_pts,
            ROUND(AVG(ts_pct), 1) as avg_ts,
            ROUND(AVG(reb), 1) as avg_reb,
            ROUND(AVG(ast), 1) as avg_ast
        FROM player_game_advanced 
        WHERE player_name = 'Giannis Antetokounmpo'
    """).fetchall()
    
    print(f"{'Player':<20} {'Games':<6} {'PPG':<5} {'TS%':<5} {'RPG':<5} {'APG':<5}")
    print("-" * 50)
    for player in comparison:
        print(f"{player[0]:<20} {player[1]:<6} {player[2]:<5} {player[3]:<5} {player[4]:<5} {player[5]:<5}")
    
    conn.close()
    
    print(f"\n{'='*50}")
    print("✅ NBA DATABASE ANALYTICS DEMO COMPLETE!")
    print(f"{'='*50}")
    print("\nThe database is now ready for:")
    print("• Player performance analysis")
    print("• Team statistics and comparisons")
    print("• Advanced metrics calculations")
    print("• Season-over-season trends")
    print("• Fantasy sports analytics")
    print("• Machine learning model training")


def main():
    """Run the analytics demo."""
    import argparse
    
    parser = argparse.ArgumentParser(description='NBA Database Analytics Demo')
    parser.add_argument('--db', default='data/nba.duckdb', help='Database path')
    
    args = parser.parse_args()
    
    try:
        run_analytics_demo(args.db)
    except Exception as e:
        print(f"Error running demo: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())