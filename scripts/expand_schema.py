import duckdb

DATABASE = 'data/nba.duckdb'

def expand_schema():
    con = duckdb.connect(DATABASE)
    print("Expanding Database Schema...")

    # 1. Create Normalized 'games' Table (Metadata only)
    print("Creating 'games' table (normalized metadata)...")
    con.sql("DROP TABLE IF EXISTS games")
    con.sql("""
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
            wl_away AS visitor_wl
        FROM game_gold
    """)
    
    # 2. Create 'team_game_stats' (Unpivoted Stats)
    # This transforms wide format (home_pts, away_pts) into long format (one row per team per game)
    print("Creating 'team_game_stats' (unpivoted)...")
    con.sql("DROP TABLE IF EXISTS team_game_stats")
    
    # We select Home stats first
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
    
    # Then Visitor stats
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
    
    con.sql(f"CREATE TABLE team_game_stats AS {q_home} UNION ALL {q_away}")
    
    # 3. Create 'player_team_history'
    # Extracting relationship between players and teams from common_player_info
    print("Creating 'player_team_history'...")
    con.sql("DROP TABLE IF EXISTS player_team_history")
    con.sql("""
        CREATE TABLE player_team_history AS
        SELECT
            person_id AS player_id,
            team_id,
            team_name,
            team_city AS city,
            from_year AS start_year,
            to_year AS end_year,
            season_exp
        FROM common_player_info_silver
        WHERE team_id IS NOT NULL
    """)
    
    # 4. Create Empty Tables for Future Data (Standard Schema)
    print("Creating empty placeholders for future expansion...")
    
    # Play-by-Play
    con.sql("""
        CREATE TABLE IF NOT EXISTS play_by_play (
            game_id BIGINT,
            event_num BIGINT,
            event_msg_type INTEGER,
            event_msg_action_type INTEGER,
            period INTEGER,
            wc_time VARCHAR,
            pc_time VARCHAR,
            homedescription VARCHAR,
            neutraldescription VARCHAR,
            visitordescription VARCHAR,
            score VARCHAR,
            scoremargin VARCHAR,
            person1type INTEGER,
            player1_id BIGINT,
            player1_name VARCHAR,
            player1_team_id BIGINT,
            player2_id BIGINT,
            player2_name VARCHAR,
            player2_team_id BIGINT,
            player3_id BIGINT,
            player3_name VARCHAR,
            player3_team_id BIGINT
        )
    """)
    
    # Player Game Stats (Box Scores)
    con.sql("""
        CREATE TABLE IF NOT EXISTS player_game_stats (
            game_id BIGINT,
            team_id BIGINT,
            player_id BIGINT,
            player_name VARCHAR,
            start_position VARCHAR,
            comment VARCHAR,
            min VARCHAR,
            fgm BIGINT, fga BIGINT, fg_pct DOUBLE,
            fg3m BIGINT, fg3a BIGINT, fg3_pct DOUBLE,
            ftm BIGINT, fta BIGINT, ft_pct DOUBLE,
            oreb BIGINT, dreb BIGINT, reb BIGINT,
            ast BIGINT, stl BIGINT, blk BIGINT, tov BIGINT, pf BIGINT,
            pts BIGINT,
            plus_minus BIGINT
        )
    """)
    
    # Salaries
    con.sql("""
        CREATE TABLE IF NOT EXISTS salaries (
            player_id BIGINT,
            season VARCHAR,
            amount DOUBLE,
            team_id BIGINT,
            rank INTEGER
        )
    """)

    # 5. Verification
    print("\n--- Expansion Summary ---")
    tables = ['games', 'team_game_stats', 'player_team_history', 'play_by_play', 'player_game_stats', 'salaries']
    for t in tables:
        count = con.sql(f"SELECT count(*) FROM {t}").fetchone()[0]
        cols = len(con.sql(f"DESCRIBE {t}").fetchall())
        print(f"Table '{t}': {count} rows, {cols} columns")

    con.close()

if __name__ == "__main__":
    expand_schema()
