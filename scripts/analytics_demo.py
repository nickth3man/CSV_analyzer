#!/usr/bin/env python3
"""NBA Database Analytics Demo - Showcase the transformed database capabilities.

This script demonstrates the advanced analytics now possible with our
comprehensive NBA DuckDB database.
"""

import sys

import duckdb


def run_analytics_demo(db_path: str = "data/nba.duckdb") -> None:
    """Run comprehensive analytics demonstrations."""
    conn = duckdb.connect(db_path)

    # 1. Database Overview

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

    for table, _count in tables:
        actual_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if actual_count > 0:
            pass

    # 2. Top Players by Advanced Metrics

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

    for _player in top_players:
        pass

    # 3. Shooting Efficiency Leaders

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

    for _player in efficient_players:
        pass

    # 4. Game-Level Advanced Stats

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

    for _player in advanced_stats:
        pass

    # 5. Team Performance

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

    for _team in team_stats:
        pass

    # 6. Database Summary Statistics

    summary_stats = {
        "Total Games": conn.execute(
            "SELECT COUNT(DISTINCT game_id) FROM player_game_stats",
        ).fetchone()[0],
        "Total Players": conn.execute(
            "SELECT COUNT(DISTINCT player_id) FROM player_game_stats",
        ).fetchone()[0],
        "Total Player Seasons": conn.execute(
            "SELECT COUNT(*) FROM player_season_stats",
        ).fetchone()[0],
        "Seasons Covered": conn.execute(
            "SELECT COUNT(DISTINCT season) FROM player_season_stats",
        ).fetchone()[0],
        "Avg Points/Game": round(
            conn.execute("SELECT AVG(pts) FROM player_game_stats").fetchone()[0], 1,
        ),
        "Triple Doubles": conn.execute(
            "SELECT SUM(is_triple_double) FROM player_game_advanced",
        ).fetchone()[0],
    }

    for _stat, _value in summary_stats.items():
        pass

    # 7. Sample Complex Query - Player Comparison

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

    for _player in comparison:
        pass

    conn.close()


def main() -> int:
    """Run the analytics demo."""
    import argparse

    parser = argparse.ArgumentParser(description="NBA Database Analytics Demo")
    parser.add_argument("--db", default="data/nba.duckdb", help="Database path")

    args = parser.parse_args()

    try:
        run_analytics_demo(args.db)
    except Exception:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
