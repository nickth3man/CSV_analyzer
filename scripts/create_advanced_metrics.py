#!/usr/bin/env python3
"""Create advanced basketball metrics views in the NBA DuckDB database.

This script creates SQL views and tables for computing advanced NBA statistics
based on Basketball Reference and industry-standard formulas.

TODO: ROADMAP Phase 2.3 - Complete advanced metrics implementation
- Current Status: Basic metrics (TS%, eFG%, TOV%, GmSc) implemented
- Missing: USG%, ORB%, DRB%, TRB%, STL%, BLK%, AST% (requires possessions data)
- Missing: PER (complex formula requiring league averages)
- Missing: BPM, VORP (require additional regression models)
- Missing: ORtg, DRtg (require possessions and team context)
Reference: docs/roadmap.md Phase 2.3

TODO: ROADMAP Phase 2.4 - Add possessions/pace data
- Need to calculate team possessions per game for advanced metrics
- Formula: Poss = 0.5 * ((Tm_FGA + 0.4*Tm_FTA - 1.07*(Tm_ORB/(Tm_ORB+Opp_DRB))*(Tm_FGA-Tm_FGM) + Tm_TOV) + (Opp_FGA + 0.4*Opp_FTA - 1.07*(Opp_ORB/(Opp_ORB+Tm_DRB))*(Opp_FGA-Opp_FGM) + Opp_TOV))
- Required for: USG%, ORtg, DRtg, Pace calculation
- Blocked by: Need opponent team stats joined per game
Reference: docs/roadmap.md Phase 2.4

Metrics Implemented:
====================

SHOOTING EFFICIENCY:
- TS% (True Shooting Percentage): PTS / (2 * TSA), where TSA = FGA + 0.44 * FTA
- eFG% (Effective FG%): (FGM + 0.5 * FG3M) / FGA
- 2P%, 3P%, FT% (already in raw data)

USAGE & VOLUME:
- USG% (Usage Rate): ((FGA + 0.44 * FTA + TOV) * (Tm_MP / 5)) / (MP * Tm_Poss)
- Pace: 48 * ((Tm_Poss + Opp_Poss) / (2 * (Tm_MP / 5)))

PLAYMAKING:
- AST% (Assist %): 100 * AST / (((MP / (Tm_MP / 5)) * Tm_FGM) - FGM)
- AST/TO (Assist to Turnover Ratio): AST / TOV
- TOV% (Turnover %): 100 * TOV / (FGA + 0.44 * FTA + TOV)

REBOUNDING:
- ORB% (Offensive Rebound %): 100 * (ORB * (Tm_MP / 5)) / (MP * (Tm_ORB + Opp_DRB))
- DRB% (Defensive Rebound %): 100 * (DRB * (Tm_MP / 5)) / (MP * (Tm_DRB + Opp_ORB))
- TRB% (Total Rebound %): 100 * (TRB * (Tm_MP / 5)) / (MP * (Tm_TRB + Opp_TRB))

DEFENSE:
- STL% (Steal %): 100 * (STL * (Tm_MP / 5)) / (MP * Opp_Poss)
- BLK% (Block %): 100 * (BLK * (Tm_MP / 5)) / (MP * (Opp_FGA - Opp_3PA))

OVERALL:
- GmSc (Game Score): PTS + 0.4*FGM - 0.7*FGA - 0.4*(FTA-FTM) + 0.7*ORB + 0.3*DRB + STL + 0.7*AST + 0.7*BLK - 0.4*PF - TOV
- PER (Player Efficiency Rating): Complex formula, simplified version provided
- Fantasy Points (NBA Fantasy): Various fantasy scoring systems

POSSESSIONS (Team Level):
- Poss = 0.5 * ((Tm_FGA + 0.4 * Tm_FTA - 1.07 * (Tm_ORB / (Tm_ORB + Opp_DRB)) * (Tm_FGA - Tm_FGM) + Tm_TOV)
         + (Opp_FGA + 0.4 * Opp_FTA - 1.07 * (Opp_ORB / (Opp_ORB + Tm_DRB)) * (Opp_FGA - Opp_FGM) + Opp_TOV))

Run with: python scripts/create_advanced_metrics.py
"""

import sys

import duckdb


def create_advanced_metrics(db_path: str = "src/backend/data/nba.duckdb") -> None:
    """Create advanced NBA metrics views and a player season summary table in the DuckDB database.

    Parameters:
        db_path (str): Filesystem path to the DuckDB database file (default: "src/backend/data/nba.duckdb").
            The function will create or replace views and a season-summary table within this
            database and commit the changes.
    """
    conn = duckdb.connect(db_path)

    try:
        # =====================================================================
        # 1. PLAYER GAME ADVANCED METRICS VIEW
        # =====================================================================

        # This view computes per-game advanced metrics for individual players
        # Note: This will only work once player_game_stats is populated
        conn.execute("""
            CREATE OR REPLACE VIEW player_game_advanced AS
            SELECT
                pgs.game_id,
                pgs.team_id,
                pgs.player_id,
                pgs.player_name,

                -- Raw stats (for reference)
                pgs.pts,
                pgs.fgm,
                pgs.fga,
                pgs.fg3m,
                pgs.fg3a,
                pgs.ftm,
                pgs.fta,
                pgs.oreb,
                pgs.dreb,
                pgs.reb,
                pgs.ast,
                pgs.stl,
                pgs.blk,
                pgs.tov,
                pgs.pf,
                pgs.plus_minus,

                -- True Shooting Attempts (TSA)
                CASE
                    WHEN pgs.fga IS NOT NULL THEN pgs.fga + 0.44 * COALESCE(pgs.fta, 0)
                    ELSE NULL
                END AS tsa,

                -- True Shooting Percentage (TS%)
                -- Formula: PTS / (2 * TSA)
                CASE
                    WHEN pgs.fga + 0.44 * COALESCE(pgs.fta, 0) > 0
                    THEN pgs.pts / (2.0 * (pgs.fga + 0.44 * COALESCE(pgs.fta, 0)))
                    ELSE NULL
                END AS ts_pct,

                -- Effective Field Goal Percentage (eFG%)
                -- Formula: (FGM + 0.5 * FG3M) / FGA
                CASE
                    WHEN pgs.fga > 0
                    THEN (pgs.fgm + 0.5 * COALESCE(pgs.fg3m, 0)) / CAST(pgs.fga AS DOUBLE)
                    ELSE NULL
                END AS efg_pct,

                -- 2-Point Field Goal Attempts and Percentage
                pgs.fga - COALESCE(pgs.fg3a, 0) AS fg2a,
                pgs.fgm - COALESCE(pgs.fg3m, 0) AS fg2m,
                CASE
                    WHEN (pgs.fga - COALESCE(pgs.fg3a, 0)) > 0
                    THEN (pgs.fgm - COALESCE(pgs.fg3m, 0)) / CAST((pgs.fga - COALESCE(pgs.fg3a, 0)) AS DOUBLE)
                    ELSE NULL
                END AS fg2_pct,

                -- Turnover Percentage (TOV%)
                -- Formula: 100 * TOV / (FGA + 0.44 * FTA + TOV)
                CASE
                    WHEN pgs.fga + 0.44 * COALESCE(pgs.fta, 0) + COALESCE(pgs.tov, 0) > 0
                    THEN 100.0 * COALESCE(pgs.tov, 0) / (pgs.fga + 0.44 * COALESCE(pgs.fta, 0) + COALESCE(pgs.tov, 0))
                    ELSE NULL
                END AS tov_pct,

                -- Assist to Turnover Ratio
                CASE
                    WHEN COALESCE(pgs.tov, 0) > 0
                    THEN CAST(pgs.ast AS DOUBLE) / pgs.tov
                    ELSE NULL
                END AS ast_to_ratio,

                -- Game Score (GmSc) - John Hollinger's metric
                -- Formula: PTS + 0.4*FGM - 0.7*FGA - 0.4*(FTA-FTM) + 0.7*ORB + 0.3*DRB + STL + 0.7*AST + 0.7*BLK - 0.4*PF - TOV
                pgs.pts
                    + 0.4 * pgs.fgm
                    - 0.7 * pgs.fga
                    - 0.4 * (COALESCE(pgs.fta, 0) - COALESCE(pgs.ftm, 0))
                    + 0.7 * COALESCE(pgs.oreb, 0)
                    + 0.3 * COALESCE(pgs.dreb, 0)
                    + COALESCE(pgs.stl, 0)
                    + 0.7 * COALESCE(pgs.ast, 0)
                    + 0.7 * COALESCE(pgs.blk, 0)
                    - 0.4 * COALESCE(pgs.pf, 0)
                    - COALESCE(pgs.tov, 0) AS game_score,

                -- Points per Shot (PPS)
                CASE
                    WHEN pgs.fga > 0
                    THEN CAST(pgs.pts AS DOUBLE) / pgs.fga
                    ELSE NULL
                END AS pts_per_shot,

                -- Free Throw Rate (FTr)
                -- Formula: FTA / FGA
                CASE
                    WHEN pgs.fga > 0
                    THEN CAST(COALESCE(pgs.fta, 0) AS DOUBLE) / pgs.fga
                    ELSE NULL
                END AS ft_rate,

                -- 3-Point Attempt Rate (3PAr)
                -- Formula: 3PA / FGA
                CASE
                    WHEN pgs.fga > 0
                    THEN CAST(COALESCE(pgs.fg3a, 0) AS DOUBLE) / pgs.fga
                    ELSE NULL
                END AS fg3a_rate,

                -- NBA Fantasy Points (DraftKings scoring)
                -- PTS + 0.5*FG3M + 1.25*REB + 1.5*AST + 2*STL + 2*BLK - 0.5*TOV
                pgs.pts
                    + 0.5 * COALESCE(pgs.fg3m, 0)
                    + 1.25 * COALESCE(pgs.reb, 0)
                    + 1.5 * COALESCE(pgs.ast, 0)
                    + 2.0 * COALESCE(pgs.stl, 0)
                    + 2.0 * COALESCE(pgs.blk, 0)
                    - 0.5 * COALESCE(pgs.tov, 0) AS fantasy_pts_dk,

                -- Double-Double indicator
                CASE
                    WHEN (
                        (CASE WHEN pgs.pts >= 10 THEN 1 ELSE 0 END) +
                        (CASE WHEN COALESCE(pgs.reb, 0) >= 10 THEN 1 ELSE 0 END) +
                        (CASE WHEN COALESCE(pgs.ast, 0) >= 10 THEN 1 ELSE 0 END) +
                        (CASE WHEN COALESCE(pgs.stl, 0) >= 10 THEN 1 ELSE 0 END) +
                        (CASE WHEN COALESCE(pgs.blk, 0) >= 10 THEN 1 ELSE 0 END)
                    ) >= 2 THEN 1 ELSE 0
                END AS is_double_double,

                -- Triple-Double indicator
                CASE
                    WHEN (
                        (CASE WHEN pgs.pts >= 10 THEN 1 ELSE 0 END) +
                        (CASE WHEN COALESCE(pgs.reb, 0) >= 10 THEN 1 ELSE 0 END) +
                        (CASE WHEN COALESCE(pgs.ast, 0) >= 10 THEN 1 ELSE 0 END) +
                        (CASE WHEN COALESCE(pgs.stl, 0) >= 10 THEN 1 ELSE 0 END) +
                        (CASE WHEN COALESCE(pgs.blk, 0) >= 10 THEN 1 ELSE 0 END)
                    ) >= 3 THEN 1 ELSE 0
                END AS is_triple_double

            FROM player_game_stats pgs
        """)

        # =====================================================================
        # 2. TEAM GAME ADVANCED METRICS VIEW
        # =====================================================================

        conn.execute("""
            CREATE OR REPLACE VIEW team_game_advanced AS
            SELECT
                tgs.game_id,
                tgs.team_id,
                tgs.season_id,
                tgs.game_date,
                tgs.is_home,

                -- Raw stats
                tgs.pts,
                tgs.fgm,
                tgs.fga,
                tgs.fg3m,
                tgs.fg3a,
                tgs.ftm,
                tgs.fta,
                tgs.oreb,
                tgs.dreb,
                tgs.reb,
                tgs.ast,
                tgs.stl,
                tgs.blk,
                tgs.tov,
                tgs.pf,
                tgs.plus_minus,

                -- Effective Field Goal Percentage (eFG%)
                CASE
                    WHEN tgs.fga > 0
                    THEN (tgs.fgm + 0.5 * COALESCE(tgs.fg3m, 0)) / CAST(tgs.fga AS DOUBLE)
                    ELSE NULL
                END AS efg_pct,

                -- True Shooting Percentage (TS%)
                CASE
                    WHEN tgs.fga + 0.44 * COALESCE(tgs.fta, 0) > 0
                    THEN tgs.pts / (2.0 * (tgs.fga + 0.44 * COALESCE(tgs.fta, 0)))
                    ELSE NULL
                END AS ts_pct,

                -- Turnover Percentage (TOV%)
                CASE
                    WHEN tgs.fga + 0.44 * COALESCE(tgs.fta, 0) + COALESCE(tgs.tov, 0) > 0
                    THEN 100.0 * COALESCE(tgs.tov, 0) / (tgs.fga + 0.44 * COALESCE(tgs.fta, 0) + COALESCE(tgs.tov, 0))
                    ELSE NULL
                END AS tov_pct,

                -- Offensive Rebound Percentage (estimate - needs opponent data for accuracy)
                -- Simplified: ORB / (ORB + DRB)
                CASE
                    WHEN COALESCE(tgs.oreb, 0) + COALESCE(tgs.dreb, 0) > 0
                    THEN 100.0 * COALESCE(tgs.oreb, 0) / (COALESCE(tgs.oreb, 0) + COALESCE(tgs.dreb, 0))
                    ELSE NULL
                END AS orb_pct_simple,

                -- Free Throw Rate (FTr)
                CASE
                    WHEN tgs.fga > 0
                    THEN CAST(COALESCE(tgs.fta, 0) AS DOUBLE) / tgs.fga
                    ELSE NULL
                END AS ft_rate,

                -- 3-Point Attempt Rate (3PAr)
                CASE
                    WHEN tgs.fga > 0
                    THEN CAST(COALESCE(tgs.fg3a, 0) AS DOUBLE) / tgs.fga
                    ELSE NULL
                END AS fg3a_rate,

                -- Assist Ratio (AST / FGM)
                CASE
                    WHEN tgs.fgm > 0
                    THEN CAST(COALESCE(tgs.ast, 0) AS DOUBLE) / tgs.fgm
                    ELSE NULL
                END AS ast_ratio,

                -- Points per Shot
                CASE
                    WHEN tgs.fga > 0
                    THEN CAST(tgs.pts AS DOUBLE) / tgs.fga
                    ELSE NULL
                END AS pts_per_shot

            FROM team_game_stats tgs
        """)

        # =====================================================================
        # 3. PLAYER SEASON SUMMARY TABLE
        # =====================================================================

        # TODO: ROADMAP Phase 2.3 - Enhance player_season_stats with advanced metrics
        # Consider adding to this table:
        # - USG% (usage rate)
        # - ORB%, DRB%, TRB% (rebounding percentages)
        # - AST% (assist percentage)
        # - STL%, BLK% (defensive activity rates)
        # - PER (player efficiency rating)
        # - WS, WS/48 (win shares)
        # - BPM, VORP (box plus/minus, value over replacement)
        # Reference: docs/roadmap.md Phase 2.3

        # Create table to store aggregated season stats per player
        conn.execute("""
            CREATE TABLE IF NOT EXISTS player_season_stats (
                player_id BIGINT,
                player_name VARCHAR,
                team_id BIGINT,
                season_id BIGINT,

                -- Counting stats
                gp INTEGER,
                gs INTEGER,
                total_min DOUBLE,

                -- Totals
                pts_total BIGINT,
                fgm_total BIGINT,
                fga_total BIGINT,
                fg3m_total BIGINT,
                fg3a_total BIGINT,
                ftm_total BIGINT,
                fta_total BIGINT,
                oreb_total BIGINT,
                dreb_total BIGINT,
                reb_total BIGINT,
                ast_total BIGINT,
                stl_total BIGINT,
                blk_total BIGINT,
                tov_total BIGINT,
                pf_total BIGINT,
                plus_minus_total DOUBLE,

                -- Per-game averages
                ppg DOUBLE,
                rpg DOUBLE,
                apg DOUBLE,
                spg DOUBLE,
                bpg DOUBLE,
                topg DOUBLE,

                -- Shooting percentages
                fg_pct DOUBLE,
                fg3_pct DOUBLE,
                ft_pct DOUBLE,
                ts_pct DOUBLE,
                efg_pct DOUBLE,

                -- Advanced
                game_score_avg DOUBLE,
                fantasy_pts_avg DOUBLE,
                double_doubles INTEGER,
                triple_doubles INTEGER,

                PRIMARY KEY (player_id, season_id, team_id)
            )
        """)

        # =====================================================================
        # 4. FOUR FACTORS VIEW (Team Level)
        # =====================================================================

        # Dean Oliver's Four Factors of Basketball Success
        # 1. eFG% - Shooting
        # 2. TOV% - Turnovers
        # 3. ORB% - Offensive Rebounding
        # 4. FT/FGA - Free Throw Rate
        conn.execute("""
            CREATE OR REPLACE VIEW team_four_factors AS
            SELECT
                tgs.game_id,
                tgs.team_id,
                tgs.season_id,
                tgs.game_date,
                tgs.is_home,

                -- Factor 1: Effective Field Goal Percentage (eFG%)
                CASE
                    WHEN tgs.fga > 0
                    THEN (tgs.fgm + 0.5 * COALESCE(tgs.fg3m, 0)) / CAST(tgs.fga AS DOUBLE)
                    ELSE NULL
                END AS efg_pct,

                -- Factor 2: Turnover Percentage (TOV%)
                CASE
                    WHEN tgs.fga + 0.44 * COALESCE(tgs.fta, 0) + COALESCE(tgs.tov, 0) > 0
                    THEN 100.0 * COALESCE(tgs.tov, 0) / (tgs.fga + 0.44 * COALESCE(tgs.fta, 0) + COALESCE(tgs.tov, 0))
                    ELSE NULL
                END AS tov_pct,

                -- Factor 3: Offensive Rebound Percentage (simplified)
                CASE
                    WHEN COALESCE(tgs.oreb, 0) + COALESCE(tgs.dreb, 0) > 0
                    THEN 100.0 * COALESCE(tgs.oreb, 0) / (COALESCE(tgs.oreb, 0) + COALESCE(tgs.dreb, 0))
                    ELSE NULL
                END AS orb_pct,

                -- Factor 4: Free Throw Rate (FT/FGA)
                CASE
                    WHEN tgs.fga > 0
                    THEN CAST(COALESCE(tgs.ftm, 0) AS DOUBLE) / tgs.fga
                    ELSE NULL
                END AS ft_factor,

                -- Result
                CASE WHEN tgs.plus_minus > 0 THEN 1 ELSE 0 END AS is_win,
                tgs.pts,
                tgs.plus_minus

            FROM team_game_stats tgs
        """)

        # =====================================================================
        # 5. LEAGUE AVERAGES VIEW (for advanced metric normalization)
        # =====================================================================

        conn.execute("""
            CREATE OR REPLACE VIEW league_season_averages AS
            SELECT
                season_id,
                COUNT(DISTINCT game_id) / 2 AS total_games,
                COUNT(DISTINCT team_id) AS total_teams,

                -- Scoring
                AVG(pts) AS avg_pts,
                SUM(pts) AS total_pts,

                -- Shooting
                AVG(fgm) AS avg_fgm,
                AVG(fga) AS avg_fga,
                SUM(fgm) / NULLIF(SUM(fga), 0) AS league_fg_pct,
                SUM(fg3m) / NULLIF(SUM(fg3a), 0) AS league_fg3_pct,
                SUM(ftm) / NULLIF(SUM(fta), 0) AS league_ft_pct,

                -- Efficiency
                (SUM(fgm) + 0.5 * SUM(fg3m)) / NULLIF(SUM(fga), 0) AS league_efg_pct,
                SUM(pts) / NULLIF(2.0 * (SUM(fga) + 0.44 * SUM(fta)), 0) AS league_ts_pct,

                -- Other
                AVG(ast) AS avg_ast,
                AVG(reb) AS avg_reb,
                AVG(tov) AS avg_tov,

                -- Pace estimation (simplified)
                AVG(fga) + 0.44 * AVG(fta) + AVG(tov) AS avg_poss_estimate

            FROM team_game_stats
            GROUP BY season_id
        """)

        # =====================================================================
        # 6. PLAYER CAREER SUMMARY VIEW
        # =====================================================================

        conn.execute("""
            CREATE OR REPLACE VIEW player_career_summary AS
            SELECT
                ps.id AS player_id,
                ps.full_name,
                ps.first_name,
                ps.last_name,
                ps.is_active,

                -- Career totals from common_player_info if available
                cpi.height,
                cpi.weight,
                cpi.position,
                cpi.jersey,
                cpi.draft_year,
                cpi.draft_round,
                cpi.draft_number,
                cpi.greatest_75_flag

            FROM player_silver ps
            LEFT JOIN common_player_info cpi
                ON ps.id = cpi.person_id
        """)

        # =====================================================================
        # 7. Commit and verify
        # =====================================================================
        conn.commit()

        # List all views created
        views = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_type = 'VIEW'
            AND table_schema = 'main'
            ORDER BY table_name
        """).fetchall()

        for v in views:
            # Get column count
            conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = '{v[0]}'
            """).fetchone()[0]

        # Check tables
        tables = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            AND table_schema = 'main'
            AND table_name LIKE '%season_stats%'
        """).fetchall()

        if tables:
            for t in tables:
                conn.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]

    finally:
        conn.close()


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "src/backend/data/nba.duckdb"
    create_advanced_metrics(db_path)
