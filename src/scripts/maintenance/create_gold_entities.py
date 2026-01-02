import logging

import duckdb


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_gold_entities(db_path: str = "src/backend/data/nba.duckdb"):
    """Create unified gold entities (teams, players) that include all IDs found in the DB."""
    conn = duckdb.connect(db_path)
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    player_stats_table = next(
        (
            name
            for name in (
                "player_game_stats_silver",
                "player_game_stats",
                "player_game_stats_raw",
            )
            if name in tables
        ),
        None,
    )
    common_player_table = next(
        (
            name
            for name in ("common_player_info_silver", "common_player_info_raw")
            if name in tables
        ),
        None,
    )
    player_stats_team_union = (
        f"SELECT team_id FROM {player_stats_table}"
        if player_stats_table
        else "SELECT NULL AS team_id WHERE FALSE"
    )
    player_stats_player_union = (
        f"SELECT player_id FROM {player_stats_table}"
        if player_stats_table
        else "SELECT NULL AS player_id WHERE FALSE"
    )
    player_stats_name_union = (
        f"SELECT player_id, player_name as name FROM {player_stats_table}"
        if player_stats_table
        else "SELECT NULL AS player_id, NULL AS name WHERE FALSE"
    )
    common_player_union = (
        f"SELECT person_id FROM {common_player_table}"
        if common_player_table
        else "SELECT NULL AS person_id WHERE FALSE"
    )
    player_name_union = (
        f"SELECT person_id, display_first_last FROM {common_player_table}"
        if common_player_table
        else "SELECT NULL AS player_id, NULL AS name WHERE FALSE"
    )

    # 1. Create team_gold
    logger.info("Creating team_gold...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE team_gold AS
        WITH all_team_ids AS (
            SELECT id as team_id FROM team_silver
            UNION
            SELECT team_id_home FROM game_silver
            UNION
            SELECT team_id_away FROM game_silver
            UNION
            {player_stats_team_union}
        ),
        team_names AS (
            -- Try to find the best name for each team ID from games
            SELECT team_id_home as team_id, team_name_home as name, team_abbreviation_home as abbrev
            FROM game_silver
            UNION
            SELECT team_id_away, team_name_away, team_abbreviation_away
            FROM game_silver
        ),
        best_names AS (
            SELECT team_id, name, abbrev,
                   row_number() OVER (PARTITION BY team_id ORDER BY name DESC) as rn
            FROM team_names
            WHERE name IS NOT NULL
        )
        SELECT 
            t.team_id as id,
            COALESCE(s.full_name, b.name, 'Unknown Team ' || t.team_id) as full_name,
            COALESCE(s.abbreviation, b.abbrev, 'UNK') as abbreviation,
            s.nickname,
            s.city,
            s.state,
            s.year_founded
        FROM all_team_ids t
        LEFT JOIN team_silver s ON t.team_id = s.id
        LEFT JOIN best_names b ON t.team_id = b.team_id AND b.rn = 1
    """)

    # 2. Create player_gold
    logger.info("Creating player_gold...")
    conn.execute(f"""
        CREATE OR REPLACE TABLE player_gold AS
        WITH all_player_ids AS (
            SELECT id as player_id FROM player_silver
            UNION
            {player_stats_player_union}
            UNION
            {common_player_union}
        ),
        player_names AS (
            {player_stats_name_union}
            UNION
            {player_name_union}
        ),
        best_names AS (
            SELECT player_id, name,
                   row_number() OVER (PARTITION BY player_id ORDER BY name DESC) as rn
            FROM player_names
            WHERE name IS NOT NULL
        )
        SELECT 
            p.player_id as id,
            COALESCE(s.full_name, b.name, 'Unknown Player ' || p.player_id) as full_name,
            s.first_name,
            s.last_name,
            s.is_active
        FROM all_player_ids p
        LEFT JOIN player_silver s ON p.player_id = s.id
        LEFT JOIN best_names b ON p.player_id = b.player_id AND b.rn = 1
    """)

    # 3. Update game_gold to include stubs for games found in player_game_stats but missing from games
    logger.info("Updating game_gold with stubs...")
    if player_stats_table:
        conn.execute(f"""
            INSERT INTO game_gold (game_id, season_id, team_id_home, team_id_away, game_date)
            SELECT DISTINCT 
                pgs.game_id,
                NULL as season_id,
                NULL as team_id_home,
                NULL as team_id_away,
                NULL as game_date
            FROM {player_stats_table} pgs
            WHERE pgs.game_id NOT IN (SELECT game_id FROM game_gold)
        """)

    conn.close()
    logger.info("Gold entities creation complete.")


if __name__ == "__main__":
    create_gold_entities()
