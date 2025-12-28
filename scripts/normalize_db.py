import duckdb
import re

DATABASE = 'data/nba.duckdb'
SILVER_SUFFIX = '_silver'

def get_tables(con):
    return [r[0] for r in con.sql("SHOW TABLES").fetchall() if not r[0].endswith('_silver') and not r[0].endswith('_rejects')]

def infer_column_type(con, table, col):
    """
    Determines the best data type for a column by testing casts.
    Hierarchy: BIGINT -> DOUBLE -> DATE -> BOOLEAN -> VARCHAR
    """
    # Get total non-null count
    total_count = con.sql(f"SELECT count({col}) FROM {table}").fetchone()[0]
    if total_count == 0:
        return 'VARCHAR' # Empty column, stay safe

    # 1. Try BIGINT
    # We check if count(try_cast(...)) == total_count
    match_count = con.sql(f"SELECT count(TRY_CAST({col} AS BIGINT)) FROM {table}").fetchone()[0]
    if match_count == total_count:
        return 'BIGINT'

    # 2. Try DOUBLE
    match_count = con.sql(f"SELECT count(TRY_CAST({col} AS DOUBLE)) FROM {table}").fetchone()[0]
    if match_count == total_count:
        return 'DOUBLE'

    # 3. Try DATE
    match_count = con.sql(f"SELECT count(TRY_CAST({col} AS DATE)) FROM {table}").fetchone()[0]
    if match_count == total_count:
        return 'DATE'

    # 4. Try BOOLEAN (True/False, 0/1 is covered by BigInt but maybe we want boolean?)
    # DuckDB's boolean cast is smart. Let's stick to explicit if text is 'true'/'false'.
    # For now, BIGINT usually covers 0/1. Let's skip boolean inference to avoid over-optimizing 0/1 flags as bools unless requested.
    
    return 'VARCHAR'

def transform_to_silver():
    con = duckdb.connect(DATABASE)
    tables = get_tables(con)
    print(f"Found {len(tables)} tables to process.")

    for table in tables:
        print(f"Analyzing table '{table}'...")
        cols = con.sql(f"DESCRIBE {table}").fetchall()
        # col structure: name, type, null, key, default, extra
        
        select_parts = []
        
        for col_info in cols:
            col_name = col_info[0]
            current_type = col_info[1]
            
            # Skip checking if it's already typed (though our source is all varchar)
            if current_type != 'VARCHAR':
                select_parts.append(f"{col_name}")
                continue
                
            new_type = infer_column_type(con, table, col_name)
            
            if new_type != 'VARCHAR':
                print(f"  - {col_name}: inferred {new_type}")
                select_parts.append(f"TRY_CAST({col_name} AS {new_type}) AS {col_name}")
            else:
                select_parts.append(f"{col_name}")

        # Create Silver Table
        silver_table = f"{table}{SILVER_SUFFIX}"
        print(f"  Creating '{silver_table}' with corrected types...")
        
        query = f"CREATE OR REPLACE TABLE {silver_table} AS SELECT {', '.join(select_parts)} FROM {table}"
        con.execute(query)

    print("\nTransformation complete.")
    con.close()

if __name__ == "__main__":
    transform_to_silver()
