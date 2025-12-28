import duckdb

DATABASE = 'project_data.db'

def final_report():
    con = duckdb.connect(DATABASE)
    print("FINAL DATABASE SCHEMA REPORT")
    print("============================")
    
    tables = ['team_silver', 'player_silver', 'game_gold']
    
    for t in tables:
        print(f"\nTable: {t}")
        print("-" * (len(t) + 7))
        count = con.sql(f"SELECT count(*) FROM {t}").fetchone()[0]
        print(f"Row Count: {count}")
        print("Columns:")
        desc = con.sql(f"DESCRIBE {t}").fetchall()
        for col in desc:
            print(f"  - {col[0]:<25} {col[1]}")

    con.close()

if __name__ == "__main__":
    final_report()
