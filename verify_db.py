import duckdb

DATABASE_FILE = 'project_data.db'

def verify_database():
    print(f"Connecting to {DATABASE_FILE} for verification...\n")
    con = duckdb.connect(DATABASE_FILE)
    
    # Query 1: Check teams
    print("--- Query 1: Sample rows from 'team' table ---")
    try:
        result = con.sql("SELECT id, full_name, abbreviation, nickname, city, state, year_founded FROM team LIMIT 5")
        result.show()
    except Exception as e:
        print(f"Error querying team: {e}")

    # Query 2: Aggregation on games
    print("\n--- Query 2: Count total games ---")
    try:
        count = con.sql("SELECT count(*) as total_games FROM game").fetchone()[0]
        print(f"Total games recorded: {count}")
    except Exception as e:
        print(f"Error querying game: {e}")

    # Query 3: Join example (Player info)
    print("\n--- Query 3: Sample player names from 'player' table ---")
    try:
        con.sql("SELECT full_name, is_active FROM player LIMIT 5").show()
    except Exception as e:
        print(f"Error querying player: {e}")

    # Query 4: Verify the empty table
    print("\n--- Query 4: Verify 'team_info_common' is empty (but schema exists) ---")
    try:
        count = con.sql("SELECT count(*) FROM team_info_common").fetchone()[0]
        print(f"Row count for team_info_common: {count}")
        columns = con.sql("DESCRIBE team_info_common").fetchall()
        print(f"Column count: {len(columns)}")
    except Exception as e:
        print(f"Error querying team_info_common: {e}")
        
    con.close()

if __name__ == "__main__":
    verify_database()
