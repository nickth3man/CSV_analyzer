import logging

import duckdb


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def deduplicate_silver_tables(db_path: str = "src/backend/data/nba.duckdb"):
    """Deduplicate silver tables that have natural primary keys."""
    conn = duckdb.connect(db_path)

    # List of tables and their primary keys to deduplicate
    tables_to_fix = [
        ("game_silver", "game_id"),
        ("team_silver", "id"),
        ("player_silver", "id"),
        ("common_player_info_silver", "person_id"),
    ]

    for table, pk in tables_to_fix:
        try:
            # Check if table exists
            exists = conn.execute(
                f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table}'"
            ).fetchone()[0]
            if not exists:
                continue

            # Check for duplicates
            dupes = conn.execute(
                f"SELECT count(*) - count(DISTINCT {pk}) FROM {table}"
            ).fetchone()[0]
            if dupes > 0:
                logger.info(f"Found {dupes} duplicates in {table}. Deduplicating...")

                # Create a temporary table with unique records
                # We use arg_max to keep the 'latest' record if there are multiple (based on filename or populated_at if exists)
                # DuckDB doesn't have DISTINCT ON like Postgres, but we can use row_number
                conn.execute(f"""
                    CREATE OR REPLACE TABLE {table} AS
                    SELECT * EXCLUDE (row_num) FROM (
                        SELECT *, row_number() OVER (PARTITION BY {pk} ORDER BY filename DESC) as row_num
                        FROM {table}
                    ) WHERE row_num = 1
                """)
                logger.info(f"Successfully deduplicated {table}")
            else:
                logger.info(f"No duplicates found in {table}")
        except Exception:
            logger.exception(f"Error deduplicating {table}")

    conn.close()


if __name__ == "__main__":
    deduplicate_silver_tables()
