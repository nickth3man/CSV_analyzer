import duckdb

DATABASE_FILE = 'project_data.db'

def query_players():
    print(f"Connecting to {DATABASE_FILE}...\n")
    con = duckdb.connect(DATABASE_FILE)
    
    players = ["Pete Maravich", "Tracy McGrady"]
    
    for player_name in players:
        print(f"--- Searching for {player_name} ---")
        
        # Search in common_player_info which seems to have bio info
        # We use ILIKE for case-insensitive search
        query = f"""
            SELECT 
                person_id, 
                display_first_last, 
                school, 
                country, 
                height, 
                weight, 
                birthdate, 
                season_exp,
                position,
                team_name,
                from_year,
                to_year
            FROM common_player_info 
            WHERE display_first_last ILIKE '%{player_name}%'
        """
        
        try:
            # First check if columns exist by selecting * first in a safe way or just try-catch
            # But based on report we know some columns. Let's try select * first to be safe if I guessed column names wrong,
            # but I saw the report.
            # actually, let's just select * for the matching row to see everything available.
            
            result = con.sql(f"SELECT * FROM common_player_info WHERE display_first_last ILIKE '%{player_name}%'")
            
            if len(result.fetchall()) > 0:
                print(f"Found match in 'common_player_info':")
                # showing the result
                con.sql(f"SELECT person_id, display_first_last, birthdate, school, country, height, weight, season_exp, jersey, position, team_name, from_year, to_year FROM common_player_info WHERE display_first_last ILIKE '%{player_name}%'").show()
            else:
                print(f"No direct match in 'common_player_info' for {player_name}")
                
                # Fallback: Search in 'player' table
                print(f"Checking 'player' table...")
                player_res = con.sql(f"SELECT * FROM player WHERE full_name ILIKE '%{player_name}%'")
                if len(player_res.fetchall()) > 0:
                    player_res.show()
                else:
                    print("No match found.")
                    
        except Exception as e:
            print(f"Error querying for {player_name}: {e}")
        print("\n")

    con.close()

if __name__ == "__main__":
    query_players()
