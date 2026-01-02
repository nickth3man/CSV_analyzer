#!/usr/bin/env python3
"""Create canonical Gold layer tables from Silver and deduplicated tables.

This script transforms the deduplicated and normalized data into the final
canonical schema used by the application and LLM.
"""

import logging

import duckdb


logger = logging.getLogger(__name__)


def create_gold_tables(db_path: str = "src/backend/data/nba.duckdb") -> None:
    """Create canonical gold tables."""
    con = duckdb.connect(db_path)

    try:
        logger.info("Creating canonical 'games' table...")
        con.execute("DROP TABLE IF EXISTS games")
        con.execute("""
            CREATE TABLE games AS
            SELECT
                game_id,
                season_id,
                game_date,
                team_id_home AS home_team_id,
                team_id_away AS visitor_team_id,
                pts_home AS home_pts,
                pts_away AS visitor_pts,
                wl_home AS home_wl,
                wl_away AS visitor_wl,
                season_type
            FROM game_gold
        """)

        logger.info("Creating canonical 'team_game_stats' table...")
        con.execute("DROP TABLE IF EXISTS team_game_stats")

        # Home stats
        q_home = """
            SELECT
                game_id,
                team_id_home AS team_id,
                season_id,
                game_date,
                TRUE AS is_home,
                pts_home AS pts,
                fgm_home AS fgm, fga_home AS fga, fg_pct_home AS fg_pct,
                fg3m_home AS fg3m, fg3a_home AS fg3a, fg3_pct_home AS fg3_pct,
                ftm_home AS ftm, fta_home AS fta, ft_pct_home AS ft_pct,
                oreb_home AS oreb, dreb_home AS dreb, reb_home AS reb,
                ast_home AS ast, stl_home AS stl, blk_home AS blk, tov_home AS tov, pf_home AS pf,
                plus_minus_home AS plus_minus
            FROM game_gold
        """

        # Visitor stats
        q_away = """
            SELECT
                game_id,
                team_id_away AS team_id,
                season_id,
                game_date,
                FALSE AS is_home,
                pts_away AS pts,
                fgm_away AS fgm, fga_away AS fga, fg_pct_away AS fg_pct,
                fg3m_away AS fg3m, fg3a_away AS fg3a, fg3_pct_away AS fg3_pct,
                ftm_away AS ftm, fta_away AS fta, ft_pct_away AS ft_pct,
                oreb_away AS oreb, dreb_away AS dreb, reb_away AS reb,
                ast_away AS ast, stl_away AS stl, blk_away AS blk, tov_away AS tov, pf_away AS pf,
                plus_minus_away AS plus_minus
            FROM game_gold
        """

        con.execute(f"CREATE TABLE team_game_stats AS {q_home} UNION ALL {q_away}")

        logger.info("Ensuring 'player_game_stats' is canonical...")
        # If player_game_stats_silver exists, make it the canonical player_game_stats
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        if "player_game_stats_silver" in tables:
            con.execute("DROP TABLE IF EXISTS player_game_stats")
            con.execute(
                "CREATE TABLE player_game_stats AS SELECT * FROM player_game_stats_silver"
            )
            logger.info("  Created 'player_game_stats' from 'player_game_stats_silver'")

        logger.info("Gold tables creation complete.")
        con.commit()
    except Exception as e:
        logger.exception(f"Error creating gold tables: {e}")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_gold_tables()
