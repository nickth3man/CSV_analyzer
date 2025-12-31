#!/usr/bin/env python3
"""Populate player_season_stats table with aggregated player statistics.

This script creates comprehensive player-season statistics by aggregating
game-level data and calculating advanced metrics like TS%, eFG%, USG%, etc.

TODO: ROADMAP Phase 2.5 - Verify and utilize bridge_player_team_season table
- Current Status: Partial verification needed
- Tasks:
  1. Verify bridge_player_team_season table exists and is populated
  2. Check data quality and completeness
  3. Consider using this table for player-team-season relationships
  4. May simplify season aggregation logic if properly maintained
- Reference: docs/roadmap.md Phase 2.5

Features:
- Aggregates player game stats into season totals
- Calculates advanced metrics (TS%, eFG%, fantasy points)
- Supports specific seasons or all available data
- Uses shared configuration from the populate package

Usage:
    # Full population (all seasons in database)
    python scripts/populate/populate_player_season_stats.py

    # For specific seasons only
    python scripts/populate/populate_player_season_stats.py --seasons 2022-23 2021-22

Based on nba_api documentation:
- reference/nba_api/src/nba_api/stats/endpoints/playercareerstats.py
- reference/nba_api/src/nba_api/stats/endpoints/leaguedashplayerstats.py
"""

import argparse
import logging
import sys
from datetime import datetime
from typing import Any

import duckdb

# Import shared modules from the populate package
from src.scripts.populate.config import get_db_path


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# SQL QUERIES FOR AGGREGATION
# =============================================================================

CREATE_PLAYER_SEASON_STATS_SQL = """
CREATE OR REPLACE TABLE player_season_stats AS
WITH player_games AS (
    SELECT
        p.player_id,
        p.player_name,
        p.team_id,
        g.season_id,
        g.season_type,
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
    INNER JOIN game_gold g ON p.game_id = g.game_id
    WHERE p.player_id IS NOT NULL
),
player_season_agg AS (
    SELECT
        player_id,
        player_name,
        team_id,
        season_id,
        season_type,
        COUNT(*) as games_played,
        SUM(minutes_played) as minutes_played,
        SUM(fgm) as fgm,
        SUM(fga) as fga,
        SUM(fg3m) as fg3m,
        SUM(fg3a) as fg3a,
        SUM(ftm) as ftm,
        SUM(fta) as fta,
        SUM(oreb) as oreb,
        SUM(dreb) as dreb,
        SUM(reb) as reb,
        SUM(ast) as ast,
        SUM(stl) as stl,
        SUM(blk) as blk,
        SUM(tov) as tov,
        SUM(pf) as pf,
        SUM(pts) as pts,
        SUM(plus_minus) as plus_minus
    FROM player_games
    GROUP BY player_id, player_name, team_id, season_id, season_type
)
SELECT
    p.player_id,
    p.player_name,
    p.team_id,
    t.abbreviation as team_abbreviation,
    p.season_id,
    p.season_type,
    p.games_played,
    p.minutes_played,
    p.fgm,
    p.fga,
    CASE WHEN p.fga > 0 THEN p.fgm * 1.0 / p.fga ELSE 0 END as fg_pct,
    p.fg3m,
    p.fg3a,
    CASE WHEN p.fg3a > 0 THEN p.fg3m * 1.0 / p.fg3a ELSE 0 END as fg3_pct,
    p.ftm,
    p.fta,
    CASE WHEN p.fta > 0 THEN p.ftm * 1.0 / p.fta ELSE 0 END as ft_pct,
    p.oreb,
    p.dreb,
    p.reb,
    p.ast,
    p.stl,
    p.blk,
    p.tov,
    p.pf,
    p.pts,
    p.plus_minus,
    CASE
        WHEN (p.fga + 0.44 * p.fta) > 0 THEN
            p.pts * 1.0 / (2.0 * (p.fga + 0.44 * p.fta))
        ELSE 0.0
    END as ts_pct,
    CASE
        WHEN p.fga > 0 THEN
            (p.fgm + 0.5 * p.fg3m) * 1.0 / p.fga
        ELSE 0.0
    END as efg_pct
FROM player_season_agg p
LEFT JOIN team_silver t ON p.team_id = t.id
WHERE p.games_played >= 5;
"""


# =============================================================================
# MAIN POPULATION FUNCTION
# =============================================================================


def populate_player_season_stats(
    db_path: str | None = None,
    seasons: list[str] | None = None,
) -> dict[str, Any]:
    """Populate the player_season_stats table by aggregating per-game player statistics into season-level metrics.

    Parameters:
        db_path (Optional[str]): Path to the DuckDB database file; when omitted the default from get_db_path() is used.
        seasons (Optional[List[str]]): Optional list of season identifiers to target; currently logged but not applied to the SQL population query.

    Returns:
        Dict[str, Any]: Summary of the population run containing:
            - start_time (str): ISO timestamp when the run started.
            - end_time (str): ISO timestamp when the run completed.
            - records_created (int): Number of player-season records written to the table.
            - errors (List[str]): List of error messages encountered during the run (empty if none).
    """
    db_path = db_path or str(get_db_path())

    logger.info("=" * 70)
    logger.info("NBA PLAYER SEASON STATS POPULATION SCRIPT")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")

    if seasons:
        logger.info(f"Seasons: {seasons}")

    # Connect to database
    logger.info("Connecting to database...")
    conn = duckdb.connect(db_path)

    stats: dict[str, Any] = {
        "start_time": datetime.now().isoformat(),
        "records_created": 0,
        "errors": [],
    }

    try:
        logger.info("Creating player_season_stats table...")
        conn.execute(CREATE_PLAYER_SEASON_STATS_SQL)

        # Get the count of records created
        count = conn.execute("SELECT COUNT(*) FROM player_season_stats").fetchone()[0]
        stats["records_created"] = count

        logger.info(
            f"Created player_season_stats table with {count} player-season records",
        )

        # Show some sample data
        logger.info("Sample player season stats (top scorers):")
        sample = conn.execute("""
            SELECT
                player_name,
                team_abbreviation,
                season_id,
                games_played,
                pts,
                ts_pct,
                efg_pct
            FROM player_season_stats
            WHERE games_played >= 10
            ORDER BY pts DESC
            LIMIT 10
        """).fetchall()

        for row in sample:
            logger.info(
                "  %s (%s) - %s: %s PTS, %.3f TS, %.3f eFG",
                row[0],
                row[1],
                row[2],
                row[4],
                row[5] or 0,
                row[6] or 0,
            )

    except Exception as e:
        logger.exception(f"Error creating player_season_stats: {e}")
        stats["errors"].append(str(e))

    finally:
        conn.close()

    stats["end_time"] = datetime.now().isoformat()

    logger.info("=" * 70)
    logger.info("PLAYER SEASON STATS POPULATION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Records created: {stats['records_created']}")

    if stats["errors"]:
        logger.error(f"Errors encountered: {len(stats['errors'])}")

    return stats


# =============================================================================
# CLI
# =============================================================================


def main() -> None:
    """Parse command-line arguments and run the player season stats population process.

    Parses optional `--db` (database path) and `--seasons` (one or more seasons) arguments, calls populate_player_season_stats with the parsed values, and exits with status 1 if the population reports errors or an exception occurs.
    """
    parser = argparse.ArgumentParser(
        description="Populate player season statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full population (all seasons)
  python scripts/populate/populate_player_season_stats.py

  # For specific seasons only
  python scripts/populate/populate_player_season_stats.py --seasons 2022-23 2021-22
        """,
    )

    parser.add_argument("--db", default=None, help="Database path")
    parser.add_argument("--seasons", nargs="+", help="Specific seasons to process")

    args = parser.parse_args()

    try:
        result = populate_player_season_stats(args.db, args.seasons)

        if result.get("errors"):
            sys.exit(1)

    except Exception as e:
        logger.exception(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
