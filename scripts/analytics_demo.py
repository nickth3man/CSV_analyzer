#!/usr/bin/env python3
"""NBA Database Analytics Demo - Showcase the transformed database capabilities.

This script demonstrates the advanced analytics now possible with our
comprehensive NBA DuckDB database.
"""

import logging
import sys

import duckdb

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def run_analytics_demo(db_path: str = "src/backend/data/nba.duckdb") -> None:
    """Run comprehensive analytics demonstrations."""
    logging.info(f"Connecting to database: {db_path}")
    conn = duckdb.connect(db_path)

    # 1. Database Overview
    logging.info("\n=== DATABASE OVERVIEW ===")
    tables = conn.execute(
        """
        SELECT table_name,
               COUNT(*) as row_count
        FROM information_schema.tables
        JOIN (SELECT table_name as tn, COUNT(*) as cnt FROM information_schema.columns GROUP BY table_name)
        ON table_name = tn
        WHERE table_schema = 'main'
        GROUP BY table_name
        ORDER BY row_count DESC
        LIMIT 10
    """
    ).fetchall()

    for table, _count in tables:
        actual_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if actual_count > 0:
            logging.info(f"Table: {table:30s} - Rows: {actual_count:,}")

    # 2. Top Players by Advanced Metrics
    logging.info("\n=== TOP PLAYERS BY POINTS PER GAME ===")
    top_players = conn.execute(
        """
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
    """
    ).fetchall()

    for player in top_players:
        logging.info(
            f"{player[0]:25s} ({player[1]}) - PPG: {player[3]}, TS%: {player[4]}, "
            f"EFG%: {player[5]}, RPG: {player[6]}, APG: {player[7]}",
        )

    # 3. Shooting Efficiency Leaders
    logging.info("\n=== SHOOTING EFFICIENCY LEADERS (15+ PPG) ===")
    efficient_players = conn.execute(
        """
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
    """
    ).fetchall()

    for player in efficient_players:
        logging.info(
            f"{player[0]:25s} ({player[1]}) - TS%: {player[4]}, FT%: {player[5]}, "
            f"3P%: {player[6]}, PPG: {player[3]}",
        )

    # 4. Game-Level Advanced Stats
    logging.info("\n=== TOP PLAYERS BY GAME-LEVEL EFFICIENCY (10+ games) ===")
    advanced_stats = conn.execute(
        """
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
    """
    ).fetchall()

    for player in advanced_stats:
        logging.info(
            f"{player[0]:25s} - Games: {player[1]:4d}, Avg TS%: {player[2]}, "
            f"Avg EFG%: {player[3]}, Triple Doubles: {player[5]}",
        )

    # 5. Team Performance
    logging.info("\n=== TEAM PERFORMANCE (by Point Differential) ===")
    team_stats = conn.execute(
        """
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
    """
    ).fetchall()

    for team in team_stats:
        logging.info(
            f"{team[0]:5s} - Games: {team[1]:4d}, Avg Points: {team[2]:5.1f}, "
            f"FG%: {team[3]:.3f}, 3P%: {team[4]:.3f}, +/-: {team[5]:+5.1f}",
        )

    # 6. Database Summary Statistics
    logging.info("\n=== DATABASE SUMMARY STATISTICS ===")
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
            conn.execute("SELECT AVG(pts) FROM player_game_stats").fetchone()[0],
            1,
        ),
        "Triple Doubles": conn.execute(
            "SELECT SUM(is_triple_double) FROM player_game_advanced",
        ).fetchone()[0],
    }

    for stat, value in summary_stats.items():
        logging.info(
            f"{stat:25s}: {value:,}"
            if isinstance(value, int)
            else f"{stat:25s}: {value}"
        )

    # 7. Sample Complex Query - Player Comparison
    logging.info("\n=== PLAYER COMPARISON (Career Game Averages) ===")
    comparison = conn.execute(
        """
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
    """
    ).fetchall()

    for player in comparison:
        logging.info(
            f"{player[0]:25s} - Games: {player[1]:4d}, PPG: {player[2]:5.1f}, "
            f"TS%: {player[3]:5.1f}, RPG: {player[4]:4.1f}, APG: {player[5]:4.1f}",
        )

    conn.close()
    logging.info("\n=== Analytics Demo Complete ===")


def main() -> int:
    """Run the analytics demo."""
    import argparse

    parser = argparse.ArgumentParser(description="NBA Database Analytics Demo")
    parser.add_argument("--db", default="src/backend/data/nba.duckdb", help="Database path")

    args = parser.parse_args()

    try:
        run_analytics_demo(args.db)
    except Exception as e:
        logging.error(f"Error running analytics demo: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
