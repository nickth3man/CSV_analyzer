import duckdb

DATABASE = 'project_data.db'

def create_advanced_schema():
    con = duckdb.connect(DATABASE)
    print("Initializing Advanced Relational Schema Shells...")

    # --- 1. Lookup Tables (Static Data) ---
    
    # Game Types
    print("Creating 'game_types'...")
    con.sql("DROP TABLE IF EXISTS game_types")
    con.sql("""
        CREATE TABLE game_types (
            type_id VARCHAR PRIMARY KEY,
            description VARCHAR
        )
    """)
    # Seed standard values
    con.sql("""
        INSERT INTO game_types VALUES 
        ('Regular Season', 'Regular Season Games'),
        ('Playoffs', 'Post-Season Playoff Games'),
        ('Pre Season', 'Pre-Season Exhibition Games'),
        ('All-Star', 'All-Star Weekend Games')
    """)

    # Player Positions
    print("Creating 'positions'...")
    con.sql("DROP TABLE IF EXISTS positions")
    con.sql("""
        CREATE TABLE positions (
            position_code VARCHAR PRIMARY KEY,
            description VARCHAR
        )
    """)
    con.sql("""
        INSERT INTO positions VALUES 
        ('G', 'Guard'),
        ('F', 'Forward'),
        ('C', 'Center'),
        ('G-F', 'Guard-Forward'),
        ('F-G', 'Forward-Guard'),
        ('F-C', 'Forward-Center'),
        ('C-F', 'Center-Forward')
    """)

    # --- 2. Entity Tables (Shells) ---

    # Seasons
    print("Creating 'seasons'...")
    con.sql("DROP TABLE IF EXISTS seasons")
    con.sql("""
        CREATE TABLE seasons (
            season_id BIGINT PRIMARY KEY,
            season_name VARCHAR, -- e.g. "2023-24"
            start_year INTEGER,
            end_year INTEGER,
            start_date DATE,
            end_date DATE
        )
    """)

    # Arenas / Venues
    print("Creating 'arenas'...")
    con.sql("DROP TABLE IF EXISTS arenas")
    con.sql("""
        CREATE TABLE arenas (
            arena_id BIGINT PRIMARY KEY, -- Generate or use external ID
            arena_name VARCHAR,
            city VARCHAR,
            state VARCHAR,
            country VARCHAR DEFAULT 'USA',
            capacity INTEGER,
            opened_year INTEGER
        )
    """)

    # Franchises (Linking history)
    print("Creating 'franchises'...")
    con.sql("DROP TABLE IF EXISTS franchises")
    con.sql("""
        CREATE TABLE franchises (
            franchise_id BIGINT PRIMARY KEY,
            current_team_id BIGINT, -- Link to current team row
            original_name VARCHAR,
            original_city VARCHAR,
            established_year INTEGER
        )
    """)
    
    # Officials Directory (Referees)
    print("Creating 'officials_directory'...")
    con.sql("DROP TABLE IF EXISTS officials_directory")
    con.sql("""
        CREATE TABLE officials_directory (
            official_id BIGINT PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            jersey_number INTEGER,
            birthdate DATE,
            college VARCHAR
        )
    """)

    # --- 3. Transactional / History Tables (Shells) ---

    # Transactions (Trades, Signings)
    print("Creating 'transactions'...")
    con.sql("DROP TABLE IF EXISTS transactions")
    con.sql("""
        CREATE TABLE transactions (
            transaction_id VARCHAR PRIMARY KEY, -- UUID or source ID
            transaction_date DATE,
            player_id BIGINT,
            from_team_id BIGINT,
            to_team_id BIGINT,
            transaction_type VARCHAR, -- 'Trade', 'Sign', 'Waive', 'Draft'
            notes VARCHAR
        )
    """)

    # Awards
    print("Creating 'awards'...")
    con.sql("DROP TABLE IF EXISTS awards")
    con.sql("""
        CREATE TABLE awards (
            award_id VARCHAR PRIMARY KEY, -- UUID or composite
            award_name VARCHAR, -- 'MVP', 'Rookie of the Year'
            season_id BIGINT,
            player_id BIGINT,
            team_id BIGINT,
            date_awarded DATE,
            description VARCHAR
        )
    """)

    # Draft Combines (Normalized)
    print("Creating 'draft_combines'...")
    con.sql("DROP TABLE IF EXISTS draft_combines")
    con.sql("""
        CREATE TABLE draft_combines (
            combine_id BIGINT PRIMARY KEY, -- sequence
            player_id BIGINT,
            season BIGINT,
            height_wo_shoes DOUBLE,
            height_w_shoes DOUBLE,
            weight DOUBLE,
            wingspan DOUBLE,
            standing_reach DOUBLE,
            body_fat_pct DOUBLE,
            standing_vertical_leap DOUBLE,
            max_vertical_leap DOUBLE,
            lane_agility_time DOUBLE,
            three_quarter_sprint DOUBLE,
            bench_press INTEGER
        )
    """)

    # --- 4. Validation ---
    print("\n--- Advanced Schema Summary ---")
    tables = [
        'game_types', 'positions', 'seasons', 'arenas', 
        'franchises', 'officials_directory', 'transactions', 
        'awards', 'draft_combines'
    ]
    
    for t in tables:
        try:
            cols = con.sql(f"DESCRIBE {t}").fetchall()
            row_count = con.sql(f"SELECT count(*) FROM {t}").fetchone()[0]
            print(f"Table '{t}': {row_count} rows, {len(cols)} columns")
            if row_count > 0:
                 # Show sample for lookups
                 print(f"  Sample: {con.sql(f'SELECT * FROM {t} LIMIT 3').fetchall()}")
        except Exception as e:
            print(f"Error checking {t}: {e}")

    con.close()

if __name__ == "__main__":
    create_advanced_schema()
