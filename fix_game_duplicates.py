import duckdb

DATABASE = 'project_data.db'

def fix_duplicates():
    con = duckdb.connect(DATABASE)
    print("Attempting to fix duplicates in 'game_silver'...")
    
    # Check duplicate counts
    total = con.sql("SELECT count(*) FROM game_silver").fetchone()[0]
    unique = con.sql("SELECT count(DISTINCT game_id) FROM game_silver").fetchone()[0]
    print(f"Current: {total} rows, {unique} unique game_ids.")
    
    if total > unique:
        print("Deduplicating...")
        # Simple deduplication: keep one row per game_id. 
        # If rows are identical, DISTINCT works. If they differ, we need arbitrary choice.
        # Let's check if they are identical using DISTINCT *
        distinct_rows = con.sql("SELECT count(*) FROM (SELECT DISTINCT * FROM game_silver)").fetchone()[0]
        
        if distinct_rows == unique:
             print("Rows are identical duplicates. Using DISTINCT.")
             con.sql("CREATE OR REPLACE TABLE game_gold AS SELECT DISTINCT * FROM game_silver")
        else:
             print(f"Rows differ ({distinct_rows} distinct rows vs {unique} ids). Using QUALIFY to pick first.")
             # Pick the one with most data? or just arbitrary. Let's use arbitrary row_number.
             con.sql("""
                CREATE OR REPLACE TABLE game_gold AS 
                SELECT * FROM game_silver 
                QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY game_date DESC) = 1
             """)
             
        # Verify
        new_total = con.sql("SELECT count(*) FROM game_gold").fetchone()[0]
        print(f"New 'game_gold' count: {new_total}")
        
        # Verify PK
        new_unique = con.sql("SELECT count(DISTINCT game_id) FROM game_gold").fetchone()[0]
        if new_total == new_unique:
            print("SUCCESS: 'game_gold' has a valid Primary Key (game_id).")
            # Create View for cleaner access? Or just leave as table.
        else:
            print("WARNING: Still have duplicates?")
            
    con.close()

if __name__ == "__main__":
    fix_duplicates()
