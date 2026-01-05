#!/usr/bin/env python3
"""Restore database from local CSV files."""

import argparse
import logging
import time
from pathlib import Path

import duckdb
import pandas as pd

from src.scripts.populate.config import get_db_path
from src.scripts.populate.helpers import configure_logging


configure_logging()
logger = logging.getLogger(__name__)

# Map CSV filenames to table names if they differ significantly
# or if we want to enforce specific order
CSV_TO_TABLE_MAP = {
    "team_details.csv": "team_details",
    "common_player_info.csv": "common_player_info",
    "team_info_common.csv": "team_info_common",
    "player.csv": "player",
    "team.csv": "team",
    "game.csv": "game",
    "game_info.csv": "game_info",
    "game_summary.csv": "game_summary",
    "line_score.csv": "line_score",
    "officials.csv": "officials",
    "other_stats.csv": "other_stats",
    "inactive_players.csv": "inactive_players",
    "draft_history.csv": "draft_history",
    "draft_combine_stats.csv": "draft_combine_stats",
    "team_history.csv": "team_history",
}


def get_csv_dir() -> Path:
    """Get the directory containing the CSV files."""
    # Assuming script is run from project root or src/scripts/populate
    # Path: src/backend/data/raw/csv
    current_file = Path(__file__).resolve()
    # Go up 4 levels from src/scripts/populate/restore_from_csv.py to project root
    project_root = current_file.parent.parent.parent.parent
    csv_dir = project_root / "src" / "backend" / "data" / "raw" / "csv"

    if not csv_dir.exists():
        # Fallback for different execution contexts
        logger.warning(
            f"Primary CSV path {csv_dir} does not exist. Trying relative path search..."
        )
        candidates = list(Path.cwd().rglob("src/backend/data/raw/csv"))
        if candidates:
            csv_dir = candidates[0]

    return csv_dir


def clean_column_names(
    conn: duckdb.DuckDBPyConnection, table_name: str, df: pd.DataFrame
) -> pd.DataFrame:
    """Ensure DataFrame columns match the table schema names."""
    try:
        # Get table schema
        schema_info = conn.execute(f"DESCRIBE {table_name}").df()
        table_cols = set(schema_info["column_name"].tolist())

        # Rename common mismatches if any (expand as needed)
        rename_map = {}
        for col in df.columns:
            if (
                col not in table_cols and col.upper() in table_cols
            ):  # Case insensitivity check
                rename_map[col] = col.upper()
            elif col not in table_cols and col.lower() in table_cols:
                rename_map[col] = col.lower()

        if rename_map:
            df = df.rename(columns=rename_map)

        # Add missing columns as None
        for col in table_cols:
            if col not in df.columns:
                df[col] = None

        # Keep only relevant columns
        cols_to_keep = [c for c in df.columns if c in table_cols]
        return df[cols_to_keep]
    except Exception as e:
        logger.warning(f"Could not validate schema for {table_name}: {e}")
        return df


def restore_from_csv(
    db_path: str | None = None, dry_run: bool = False, force: bool = False
) -> None:
    """Restore database from CSV files."""
    db_path = db_path or str(get_db_path())
    csv_dir = get_csv_dir()

    if not csv_dir.exists():
        logger.error(f"CSV directory not found: {csv_dir}")
        return

    logger.info(f"Restoring database: {db_path}")
    logger.info(f"Source CSV directory: {csv_dir}")

    conn = duckdb.connect(db_path)

    # Get list of CSV files
    csv_files = list(csv_dir.glob("*.csv"))
    if not csv_files:
        logger.warning("No CSV files found.")
        return

    start_time = time.time()
    total_records = 0
    failures = 0

    for csv_file in csv_files:
        table_name = CSV_TO_TABLE_MAP.get(csv_file.name, csv_file.stem)
        logger.info(f"Processing {csv_file.name} -> Table: {table_name}")

        try:
            # Check if table exists
            table_exists = (
                conn.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                    [table_name],
                ).fetchone()[0]
                > 0
            )

            if not table_exists:
                if force:
                    logger.info(
                        f"Table {table_name} does not exist. Creating from CSV schema."
                    )
                    if not dry_run:
                        conn.execute(
                            f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}') LIMIT 0"
                        )
                else:
                    logger.warning(
                        f"Table {table_name} does not exist. Skipping (use --force to auto-create)."
                    )
                    continue

            # Read CSV
            df = pd.read_csv(csv_file)
            if df.empty:
                logger.info(f"Skipping empty file {csv_file.name}")
                continue

            # Basic transformation/cleaning if needed
            if (
                not force
            ):  # If we are inserting into existing schema, try to align columns
                df = clean_column_names(conn, table_name, df)

            records = len(df)

            if dry_run:
                logger.info(
                    f"DRY RUN: Would insert {records} records into {table_name}"
                )
            else:
                # Use DuckDB appender for speed
                # Or register df and insert
                conn.register("temp_load_view", df)
                # INSERT OR IGNORE or INSERT INTO
                conn.execute(
                    f"INSERT OR IGNORE INTO {table_name} SELECT * FROM temp_load_view"
                )
                conn.unregister("temp_load_view")

                # Verify count
                # count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                logger.info(f"Inserted {records} records into {table_name}")
                total_records += records

        except Exception as e:
            logger.error(f"Failed to restore {csv_file.name}: {e}")
            failures += 1
            if force:  # If forcing, maybe we try strict CSV read directly
                try:
                    logger.info("Attempting direct SQL CSV import as fallback...")
                    conn.execute(
                        f"INSERT INTO {table_name} SELECT * FROM read_csv_auto('{csv_file}', union_by_name=True)"
                    )
                    logger.info("Direct SQL import successful.")
                except Exception as e2:
                    logger.error(f"Direct import also failed: {e2}")

    conn.close()
    duration = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Restoration Complete in {duration:.2f}s")
    logger.info(f"Total Records Inserted: {total_records}")
    logger.info(f"Failures: {failures}")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Restore NBA database from local CSVs")
    parser.add_argument("--db", help="Path to DuckDB database")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument(
        "--force", action="store_true", help="Force creation of tables if missing"
    )
    args = parser.parse_args()

    restore_from_csv(db_path=args.db, dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    main()
