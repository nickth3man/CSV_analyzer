import glob
import os

import duckdb


# Configuration
DATABASE_FILE = "src/backend/data/nba.duckdb"
DATA_DIRECTORY = "./src/backend/data/raw/csv/"
# We will iterate through files, so we don't use a single source pattern for read_csv


def run_ingestion_pipeline(db_path: str | None = None) -> None:
    # Connect to DuckDB
    database_file = db_path or DATABASE_FILE
    con = duckdb.connect(database_file)

    # Find CSV files
    csv_files = glob.glob(os.path.join(DATA_DIRECTORY, "*.csv"))
    if not csv_files:
        return

    tables_created = []

    for file_path in csv_files:
        filename = os.path.basename(file_path)
        base_table = os.path.splitext(filename)[0]
        table_name = f"{base_table}_raw"

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
                row = con.sql(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'reject_errors'",
                ).fetchone()
                has_rejects_table = row[0] if row else 0

                if has_rejects_table > 0:
                    # Check for errors specifically from this load (safest to check if table is not empty)
                    # Since we are running sequentially, we can capture and clear, or just query.
                    # But reject_errors persists for the session.
                    # We can select errors associated with the current file path if possible.
                    # reject_scans has file_path.

                    row = con.sql(f"""
                        SELECT count(*)
                        FROM reject_errors
                        JOIN reject_scans ON reject_errors.scan_id = reject_scans.scan_id
                        WHERE reject_scans.file_path LIKE '%{filename}%'
                     """).fetchone()
                    reject_count = row[0] if row else 0

                    if reject_count > 0:
                        # Persist rejects for this table
                        reject_table_name = f"{table_name}_rejects"
                        con.execute(f"""
                            CREATE OR REPLACE TABLE {reject_table_name} AS
                            SELECT *
                            FROM reject_errors
                            WHERE scan_id IN (SELECT scan_id FROM reject_scans WHERE file_path LIKE '%{filename}%')
                         """)
            except Exception:
                # If reject_errors doesn't exist, it means no errors so far
                pass

        except Exception:
            pass

    with open("scripts/migration_report.txt", "w") as report_file:
        report_file.write("MIGRATION REPORT\n")
        report_file.write("================\n\n")

        for table in tables_created:
            row = con.sql(f"SELECT count(*) FROM {table}").fetchone()
            row_count = row[0] if row else 0
            col_count = len(con.sql(f"DESCRIBE {table}").fetchall())

            summary_line = f"Table '{table}': {row_count} rows, {col_count} columns"
            report_file.write(f"{summary_line}\n")

            # detailed summary
            report_file.write(f"--- Schema & Stats for {table} ---\n")
            try:
                summary_df = con.sql(f"SUMMARIZE {table}").df()
                # Select relevant columns for the report
                # SUMMARIZE returns: column_name, column_type, min, max, approx_unique, avg, std, q25, q50, q75, count, null_percentage
                # We want: column_name, column_type, null_percentage

                # Formatting the dataframe as string to write to file
                report_file.write(
                    summary_df[
                        ["column_name", "column_type", "null_percentage"]
                    ].to_string(
                        index=False,
                    ),
                )
                report_file.write("\n\n")
            except Exception as e:
                report_file.write(f"Could not generate summary: {e}\n\n")
    con.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Load CSV files into the DuckDB raw tables"
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database (default: src/backend/data/nba.duckdb)",
    )
    args = parser.parse_args()
    run_ingestion_pipeline(db_path=args.db)
