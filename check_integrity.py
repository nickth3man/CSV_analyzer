import duckdb

DATABASE = 'project_data.db'

def check_integrity():
    print(f"Connecting to {DATABASE} to verify relational integrity...")
    con = duckdb.connect(DATABASE)
    
    # 1. Check Primary Keys
    print("\n--- Checking Primary Key Candidates ---")
    pk_candidates = [
        ('team_silver', 'id'),
        ('player_silver', 'id'),
        ('game_silver', 'game_id')
    ]
    
    for table, pk in pk_candidates:
        try:
            total = con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]
            unique = con.sql(f"SELECT count(DISTINCT {pk}) FROM {table}").fetchone()[0]
            nulls = con.sql(f"SELECT count(*) FROM {table} WHERE {pk} IS NULL").fetchone()[0]
            
            print(f"Table '{table}' PK '{pk}':")
            print(f"  Total Rows: {total}")
            print(f"  Unique PKs: {unique}")
            print(f"  Null PKs:   {nulls}")
            
            if total == unique and nulls == 0:
                print("  -> VALID Primary Key")
                # We can explicitly add the constraint in DuckDB
                try:
                    con.sql(f"ALTER TABLE {table} ALTER {pk} SET NOT NULL")
                    con.sql(f"CREATE UNIQUE INDEX idx_{table}_{pk} ON {table} ({pk})") # DuckDB PK syntax via index usually or just PK constraint
                    # DuckDB support for adding PK to existing table is limited, index works for performance.
                    # Standard SQL: ALTER TABLE t ADD PRIMARY KEY (id)
                    con.sql(f"ALTER TABLE {table} ADD PRIMARY KEY ({pk})")
                    print("  -> Primary Key Constraint ADDED")
                except Exception as e:
                    print(f"  -> Could not add PK constraint (might already exist or not supported in this version on existing data): {e}")
            else:
                print("  -> INVALID Primary Key (duplicates or nulls found)")
        except Exception as e:
            print(f"  Error checking {table}: {e}")

    # 2. Check Foreign Keys
    print("\n--- Checking Foreign Key Integrity ---")
    fk_checks = [
        ('game_silver', 'team_id_home', 'team_silver', 'id'),
        ('game_silver', 'team_id_away', 'team_silver', 'id'),
        ('common_player_info_silver', 'person_id', 'player_silver', 'id') 
    ]
    
    for child_table, child_col, parent_table, parent_col in fk_checks:
        try:
            # count orphans
            query = f"""
                SELECT count(DISTINCT c.{child_col})
                FROM {child_table} c
                LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL
            """
            orphan_count = con.sql(query).fetchone()[0]
            
            print(f"FK Check: {child_table}.{child_col} -> {parent_table}.{parent_col}")
            if orphan_count == 0:
                print("  -> VALID Foreign Key (No orphans)")
                # Adding FK constraint
                try:
                     con.sql(f"ALTER TABLE {child_table} ADD FOREIGN KEY ({child_col}) REFERENCES {parent_table}({parent_col})")
                     print("  -> Foreign Key Constraint ADDED")
                except Exception as e:
                     print(f"  -> Could not add FK constraint: {e}")
            else:
                print(f"  -> INVALID Foreign Key. Found {orphan_count} orphaned IDs.")
                # Show sample orphans
                sample = con.sql(f"""
                    SELECT DISTINCT c.{child_col}
                    FROM {child_table} c
                    LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                    WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL
                    LIMIT 3
                """).fetchall()
                print(f"     Samples: {[s[0] for s in sample]}")

        except Exception as e:
            print(f"  Skipping FK check {child_table}.{child_col}: {e}")

    con.close()

if __name__ == "__main__":
    check_integrity()
