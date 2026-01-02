#!/usr/bin/env python3
"""Normalize NBA database tables by inferring and applying proper data types.

TODO: ROADMAP Phase 1.2 - Document canonical tables vs raw text tables
- Current Status: Partial documentation exists
- Need to clearly document:
  1. Which tables are canonical (silver/gold) vs raw text
  2. Relationships between raw and canonical tables
  3. Transformation logic from raw -> silver -> gold
- Reference: docs/roadmap.md Phase 1.2

TODO: ROADMAP Phase 1.5 - Quarantine raw text tables
- Current Status: Raw tables use the '_raw' suffix and are normalized into silver.
- Recommended actions:
  1. Consider moving raw tables to a separate schema/database (e.g., 'raw' schema)
  2. Update documentation to clarify canonical vs raw
  3. Prevent accidental use of raw tables in queries
- Priority: MEDIUM (Phase 1.5)
- Canonical tables to prefer:
  - Use player_silver (not player_raw)
  - Use team_silver (not team_raw)
  - Use game_gold (not game_raw)
- Reference: docs/roadmap.md Phase 1.5

This script analyzes VARCHAR columns in the database and converts them to
appropriate data types (BIGINT, DOUBLE, DATE) based on content analysis.
Creates "_silver" versions of each table with proper types.

The normalization process:
1. Scans all tables (excluding views and existing _silver tables)
2. For each VARCHAR column, tests which data type fits best
3. Creates a new table with suffix "_silver" containing typed data

Type inference hierarchy:
- BIGINT: If all values can cast to integer
- DOUBLE: If all values can cast to floating point
- DATE: If all values can cast to date
- VARCHAR: Default fallback

Usage:
    # Run normalization
    python scripts/maintenance/normalize_db.py

    # Or via CLI
    python -m scripts.populate.cli normalize
"""

import argparse
import logging
import sys
from pathlib import Path

import duckdb


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Default database path
DATABASE = "src/backend/data/nba.duckdb"
SILVER_SUFFIX = "_silver"


def get_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Get actual raw tables (not views) that end with _raw.

    Args:
        con: DuckDB connection

    Returns:
        List of table names to process
    """
    # Get list of views to exclude
    views = {
        r[0]
        for r in con.sql(
            "SELECT view_name FROM duckdb_views() WHERE internal = false",
        ).fetchall()
    }

    # Get all tables and filter out views, keeping only raw tables
    all_tables = [r[0] for r in con.sql("SHOW TABLES").fetchall()]
    return [
        t
        for t in all_tables
        if t not in views and t.endswith("_raw")
    ]


def infer_column_type(con: duckdb.DuckDBPyConnection, table: str, col: str) -> str:
    """Determine the best data type for a column by testing casts.

    Tests type casts in order of specificity:
    1. BIGINT - for integer values
    2. DOUBLE - for floating point values
    3. DATE - for date values
    4. VARCHAR - fallback for text

    Args:
        con: DuckDB connection
        table: Table name
        col: Column name

    Returns:
        Inferred type name (BIGINT, DOUBLE, DATE, or VARCHAR)
    """
    # Quote the column name to handle reserved words and special characters
    quoted_col = f'"{col}"'

    # Get total non-null count
    total_count = con.sql(f"SELECT count({quoted_col}) FROM {table}").fetchone()[0]
    if total_count == 0:
        return "VARCHAR"  # Empty column, stay safe

    # 1. Try BIGINT
    match_count = con.sql(
        f"SELECT count(TRY_CAST({quoted_col} AS BIGINT)) FROM {table}",
    ).fetchone()[0]
    if match_count == total_count:
        return "BIGINT"

    # 2. Try DOUBLE
    match_count = con.sql(
        f"SELECT count(TRY_CAST({quoted_col} AS DOUBLE)) FROM {table}",
    ).fetchone()[0]
    if match_count == total_count:
        return "DOUBLE"

    # 3. Try DATE
    match_count = con.sql(
        f"SELECT count(TRY_CAST({quoted_col} AS DATE)) FROM {table}",
    ).fetchone()[0]
    if match_count == total_count:
        return "DATE"

    # Default to VARCHAR
    return "VARCHAR"


def transform_to_silver(
    db_path: str | None = None,
    tables: list[str] | None = None,
) -> None:
    """Transform tables to silver layer with proper data types.

    Args:
        db_path: Path to DuckDB database (default: src/backend/data/nba.duckdb)
        tables: Specific tables to process (default: all)
    """
    db_path = db_path or DATABASE

    if not Path(db_path).exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)

    con = duckdb.connect(db_path)

    # Get tables to process
    tables_to_process = tables or get_tables(con)

    logger.info(f"Found {len(tables_to_process)} tables to process.")

    processed = 0
    errors = 0

    for table in tables_to_process:
        try:
            logger.info(f"Analyzing table '{table}'...")

            # Get column info
            cols = con.sql(f"DESCRIBE {table}").fetchall()

            select_parts = []
            type_changes = []

            for col_info in cols:
                col_name = col_info[0]
                current_type = col_info[1]
                quoted_col = f'"{col_name}"'

                # Skip checking if it's already typed
                if current_type != "VARCHAR":
                    select_parts.append(quoted_col)
                    continue

                new_type = infer_column_type(con, table, col_name)

                if new_type != "VARCHAR":
                    type_changes.append((col_name, new_type))
                    select_parts.append(
                        f"TRY_CAST({quoted_col} AS {new_type}) AS {quoted_col}",
                    )
                else:
                    select_parts.append(quoted_col)

            # Log type changes
            for col_name, new_type in type_changes:
                logger.info(f"  - {col_name}: inferred {new_type}")

            # Create Silver Table
            if table.endswith("_raw"):
                silver_table = f"{table[:-4]}{SILVER_SUFFIX}"
            else:
                silver_table = f"{table}{SILVER_SUFFIX}"

            logger.info(f"  Creating '{silver_table}' with corrected types...")

            query = f"""
                CREATE OR REPLACE TABLE {silver_table} AS
                SELECT {", ".join(select_parts)} FROM {table}
            """
            con.execute(query)
            processed += 1

        except Exception as e:
            logger.exception(f"Error processing table '{table}': {e}")
            errors += 1

    con.close()

    logger.info("")
    logger.info("=" * 50)
    logger.info("NORMALIZATION COMPLETE")
    logger.info("=" * 50)
    logger.info(f"Tables processed: {processed}")
    if errors:
        logger.warning(f"Errors: {errors}")


def main() -> None:
    """Command-line entry point for database normalization."""
    parser = argparse.ArgumentParser(
        description="Normalize NBA database tables with proper data types",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Normalize all tables
    python scripts/maintenance/normalize_db.py

    # Normalize specific tables
    python scripts/maintenance/normalize_db.py --tables player_raw team_raw game_raw

    # Specify custom database
    python scripts/maintenance/normalize_db.py --db /path/to/nba.duckdb
        """,
    )

    parser.add_argument(
        "--db",
        default=DATABASE,
        help="Path to DuckDB database (default: src/backend/data/nba.duckdb)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Specific tables to normalize",
    )

    args = parser.parse_args()

    transform_to_silver(db_path=args.db, tables=args.tables)


if __name__ == "__main__":
    main()
