#!/usr/bin/env python3
"""Fix data type issues in the NBA DuckDB database.

This script addresses the following issues:
1. plus_minus columns stored as BIGINT should be DOUBLE (can be fractional in per-game averages)
2. team_game_stats has fg_pct, fg3_pct, ft_pct as BIGINT (should be DOUBLE)

Run with: python scripts/fix_data_types.py
"""

import duckdb
import sys
from pathlib import Path


def fix_data_types(db_path: str = "data/nba.duckdb") -> None:
    """Fix data type issues in the database."""
    
    print(f"Connecting to database: {db_path}")
    conn = duckdb.connect(db_path)
    
    try:
        # Get list of all tables
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        
        print(f"\nFound {len(table_names)} tables")
        
        # Track fixes
        fixes_applied = []
        
        # Fix 1: plus_minus should be DOUBLE in all tables
        print("\n=== Checking plus_minus columns ===")
        plus_minus_tables = conn.execute("""
            SELECT table_name, data_type 
            FROM information_schema.columns 
            WHERE column_name = 'plus_minus'
        """).fetchall()
        
        for table, dtype in plus_minus_tables:
            if dtype == 'BIGINT':
                print(f"  Fixing {table}.plus_minus: BIGINT -> DOUBLE")
                try:
                    # DuckDB doesn't support ALTER COLUMN TYPE directly on all tables
                    # We need to recreate the table or use CAST
                    # For now, create a new column and swap
                    
                    # Check if table has data
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    
                    if count == 0:
                        # Empty table - can alter directly by recreating
                        # Get all columns
                        cols = conn.execute(f"""
                            SELECT column_name, data_type 
                            FROM information_schema.columns 
                            WHERE table_name = '{table}'
                            ORDER BY ordinal_position
                        """).fetchall()
                        
                        # Build column definitions with fixed type
                        col_defs = []
                        for col_name, col_type in cols:
                            if col_name == 'plus_minus':
                                col_defs.append(f"{col_name} DOUBLE")
                            else:
                                col_defs.append(f"{col_name} {col_type}")
                        
                        # Drop and recreate
                        conn.execute(f"DROP TABLE IF EXISTS {table}_backup")
                        conn.execute(f"ALTER TABLE {table} RENAME TO {table}_backup")
                        conn.execute(f"CREATE TABLE {table} ({', '.join(col_defs)})")
                        conn.execute(f"DROP TABLE {table}_backup")
                        
                        fixes_applied.append(f"{table}.plus_minus: BIGINT -> DOUBLE (empty table)")
                    else:
                        # Table has data - need to recreate with data migration
                        cols = conn.execute(f"""
                            SELECT column_name, data_type 
                            FROM information_schema.columns 
                            WHERE table_name = '{table}'
                            ORDER BY ordinal_position
                        """).fetchall()
                        
                        # Build SELECT with CAST for plus_minus
                        select_cols = []
                        col_defs = []
                        for col_name, col_type in cols:
                            if col_name == 'plus_minus':
                                select_cols.append(f"CAST({col_name} AS DOUBLE) AS {col_name}")
                                col_defs.append(f"{col_name} DOUBLE")
                            else:
                                select_cols.append(col_name)
                                col_defs.append(f"{col_name} {col_type}")
                        
                        # Create backup, recreate, copy data
                        conn.execute(f"DROP TABLE IF EXISTS {table}_new")
                        conn.execute(f"CREATE TABLE {table}_new AS SELECT {', '.join(select_cols)} FROM {table}")
                        conn.execute(f"DROP TABLE {table}")
                        conn.execute(f"ALTER TABLE {table}_new RENAME TO {table}")
                        
                        fixes_applied.append(f"{table}.plus_minus: BIGINT -> DOUBLE ({count} rows migrated)")
                except Exception as e:
                    print(f"    ERROR: {e}")
            else:
                print(f"  {table}.plus_minus: Already {dtype} (OK)")
        
        # Fix 2: Percentage columns in team_game_stats should be DOUBLE
        print("\n=== Checking percentage columns in team_game_stats ===")
        pct_cols = ['fg_pct', 'fg3_pct', 'ft_pct']
        
        for col in pct_cols:
            result = conn.execute(f"""
                SELECT data_type 
                FROM information_schema.columns 
                WHERE table_name = 'team_game_stats' AND column_name = '{col}'
            """).fetchone()
            
            if result and result[0] == 'BIGINT':
                print(f"  Fixing team_game_stats.{col}: BIGINT -> DOUBLE")
                try:
                    count = conn.execute("SELECT COUNT(*) FROM team_game_stats").fetchone()[0]
                    
                    # Get all columns
                    cols = conn.execute("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = 'team_game_stats'
                        ORDER BY ordinal_position
                    """).fetchall()
                    
                    # Build SELECT with CAST for percentage columns
                    select_cols = []
                    for col_name, col_type in cols:
                        if col_name in pct_cols:
                            # Convert from stored integer (e.g., 45 for 45%) to decimal (0.45)
                            # Or if stored as 0/1, divide by 100
                            select_cols.append(f"CAST({col_name} AS DOUBLE) / 100.0 AS {col_name}")
                        else:
                            select_cols.append(col_name)
                    
                    # Recreate table with correct types
                    conn.execute("DROP TABLE IF EXISTS team_game_stats_new")
                    conn.execute(f"CREATE TABLE team_game_stats_new AS SELECT {', '.join(select_cols)} FROM team_game_stats")
                    conn.execute("DROP TABLE team_game_stats")
                    conn.execute("ALTER TABLE team_game_stats_new RENAME TO team_game_stats")
                    
                    fixes_applied.append(f"team_game_stats.{col}: BIGINT -> DOUBLE ({count} rows)")
                except Exception as e:
                    print(f"    ERROR: {e}")
            elif result:
                print(f"  team_game_stats.{col}: Already {result[0]} (OK)")
        
        # Commit changes
        conn.commit()
        
        # Print summary
        print("\n" + "=" * 50)
        print("SUMMARY OF FIXES APPLIED")
        print("=" * 50)
        
        if fixes_applied:
            for fix in fixes_applied:
                print(f"  ✓ {fix}")
        else:
            print("  No fixes needed - all data types are correct!")
        
        # Verify fixes
        print("\n=== Verification ===")
        for table in ['team_game_stats', 'player_game_stats']:
            if table in table_names:
                cols = conn.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table}' 
                    AND column_name IN ('plus_minus', 'fg_pct', 'fg3_pct', 'ft_pct')
                """).fetchall()
                print(f"\n{table}:")
                for col, dtype in cols:
                    status = "✓" if dtype == "DOUBLE" else "✗"
                    print(f"  {status} {col}: {dtype}")
        
    finally:
        conn.close()


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/nba.duckdb"
    fix_data_types(db_path)
