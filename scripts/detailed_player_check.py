import duckdb

DATABASE_FILE = 'data/nba.duckdb'

def detailed_check():
    print(f"Connecting to {DATABASE_FILE}...\n")
    con = duckdb.connect(DATABASE_FILE)
    
    # IDs found from previous step
    players = [
        {"name": "Pete Maravich", "id": "77459"},
        {"name": "Tracy McGrady", "id": "1503"}
    ]
    
    for p in players:
        p_id = p["id"]
        p_name = p["name"]
        print(f"=== Details for {p_name} (ID: {p_id}) ===")
        
        # 1. Check common_player_info by person_id
        # Note: Columns in common_player_info are all VARCHAR
        print(f"--- Checking 'common_player_info' by person_id ---")
        try:
            res = con.sql(f"SELECT * FROM common_player_info WHERE person_id = '{p_id}'")
            if len(res.fetchall()) > 0:
                con.sql(f"SELECT person_id, display_first_last, birthdate, school, height, weight, jersey, position, team_name, from_year, to_year FROM common_player_info WHERE person_id = '{p_id}'").show()
            else:
                print("No record found in 'common_player_info'.")
        except Exception as e:
            print(f"Error checking common_player_info: {e}")
            
        # 2. Check draft_history
        # We don't know exact column name for player id in draft_history, usually person_id or player_id. 
        # Or search by name.
        print(f"--- Checking 'draft_history' ---")
        try:
            # Let's try searching by name since IDs might vary or column name unknown
            # Using ILIKE on likely name columns
            res = con.sql(f"SELECT * FROM draft_history WHERE player_name ILIKE '%{p_name}%'")
            if len(res.fetchall()) > 0:
                 con.sql(f"SELECT player_name, season, round_number, round_pick, overall_pick, team_city, team_name, organization, organization_type FROM draft_history WHERE player_name ILIKE '%{p_name}%'").show()
            else:
                print("No record found in 'draft_history'.")
        except Exception as e:
            # Maybe column is not player_name
            print(f"Error checking draft_history: {e}")
            # Let's peek at columns if it failed
            try:
                print("Columns in draft_history:", [col[0] for col in con.sql("DESCRIBE draft_history").fetchall()])
            except:
                pass

    con.close()

if __name__ == "__main__":
    detailed_check()
