import duckdb
import glob
import os
import sys

# Configuration
DATABASE_FILE = 'data/nba.duckdb'
DATA_DIRECTORY = './data/raw/csv/'
# We will iterate through files, so we don't use a single source pattern for read_csv

def run_ingestion_pipeline():
    print(f"--- Starting Ingestion Pipeline ---")
    print(f"Initializing persistent connection to '{DATABASE_FILE}'...")
    
    # Connect to DuckDB
    con = duckdb.connect(DATABASE_FILE)
    
    # Find CSV files
    csv_files = glob.glob(os.path.join(DATA_DIRECTORY, '*.csv'))
    if not csv_files:
        print(f"Error: No CSV files found in {DATA_DIRECTORY}")
        return

    print(f"Found {len(csv_files)} CSV files.")
    
    tables_created = []
    
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        table_name = os.path.splitext(filename)[0]
        
        print(f"\nProcessing {filename} -> Table: {table_name}")
        
        try:
            # Drop the table if it exists to allow re-running
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Using store_rejects=True to capture errors
            # Using all_varchar=True for preservation
            # Using sample_size=-1 for exhaustive scanning
            query = f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_csv(
                    '{file_path}',
                    sample_size=-1,
                    all_varchar=true,
                    header=true,
                    store_rejects=true,
                    filename=true
                )
            """
            con.execute(query)
            tables_created.append(table_name)
            
            # Check for rejects
            # The reject_errors table is created/updated by store_rejects=true
            try:
                # We check if reject_errors exists and has entries for this specific file
                # The reject_errors table contains a file_id, but easier to just check count if we assume sequential processing
                # However, read_csv might append to existing reject tables if they exist in temp? 
                # Let's check the count.
                
                # Check if reject_errors table exists (it might not if no errors ever occurred in the session yet)
                has_rejects_table = con.sql("SELECT count(*) FROM information_schema.tables WHERE table_name = 'reject_errors'").fetchone()[0]
                
                if has_rejects_table > 0:
                     # Check for errors specifically from this load (safest to check if table is not empty)
                     # Since we are running sequentially, we can capture and clear, or just query.
                     # But reject_errors persists for the session.
                     # We can select errors associated with the current file path if possible.
                     # reject_scans has file_path.
                     
                     reject_count = con.sql(f"""
                        SELECT count(*) 
                        FROM reject_errors 
                        JOIN reject_scans ON reject_errors.scan_id = reject_scans.scan_id
                        WHERE reject_scans.file_path LIKE '%{filename}%'
                     """).fetchone()[0]
                     
                     if reject_count > 0:
                         print(f"  WARNING: {reject_count} rows rejected.")
                         # Persist rejects for this table
                         reject_table_name = f"{table_name}_rejects"
                         con.execute(f"""
                            CREATE OR REPLACE TABLE {reject_table_name} AS
                            SELECT * 
                            FROM reject_errors
                            WHERE scan_id IN (SELECT scan_id FROM reject_scans WHERE file_path LIKE '%{filename}%')
                         """)
                         print(f"  Rejected rows saved to table: {reject_table_name}")
            except Exception as e:
                # If reject_errors doesn't exist, it means no errors so far
                pass
                
        except Exception as e:
            print(f"  FAILED to process {filename}: {e}")

    print("\n--- Verification Summary ---")
    report_file = open("scripts/migration_report.txt", "w")
    report_file.write("MIGRATION REPORT\n")
    report_file.write("================\n\n")
    
    for table in tables_created:
        row_count = con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]
        col_count = len(con.sql(f"DESCRIBE {table}").fetchall())
        
        summary_line = f"Table '{table}': {row_count} rows, {col_count} columns"
        print(summary_line)
        report_file.write(f"{summary_line}\n")
        
        # detailed summary
        report_file.write(f"--- Schema & Stats for {table} ---\n")
        try:
            summary_df = con.sql(f"SUMMARIZE {table}").df()
            # Select relevant columns for the report
            # SUMMARIZE returns: column_name, column_type, min, max, approx_unique, avg, std, q25, q50, q75, count, null_percentage
            # We want: column_name, column_type, null_percentage
            
            # Formatting the dataframe as string to write to file
            report_file.write(summary_df[['column_name', 'column_type', 'null_percentage']].to_string(index=False))
            report_file.write("\n\n")
        except Exception as e:
             report_file.write(f"Could not generate summary: {e}\n\n")

    report_file.close()
    con.close()
    print(f"\nMigration complete. Database saved to {DATABASE_FILE}")
    print(f"Detailed report saved to scripts/migration_report.txt")

if __name__ == "__main__":
    run_ingestion_pipeline()
