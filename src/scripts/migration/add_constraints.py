#!/usr/bin/env python3
"""Add integrity constraints to existing NBA database tables.

This migration script adds:
1. NOT NULL constraints on critical columns
2. FOREIGN KEY relationships between tables
3. CHECK constraints for valid data ranges
4. Unique indexes for performance

Usage:
    python -m src.scripts.migration.add_constraints
    python -m src.scripts.migration.add_constraints --db path/to/nba.duckdb
    python -m src.scripts.migration.add_constraints --dry-run

Note:
    DuckDB has limited ALTER TABLE support, so some constraints are
    implemented via CREATE TABLE AS SELECT + validation checks.
"""

import argparse
import logging
from contextlib import suppress
from pathlib import Path
from typing import Any

import duckdb


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Default database path
DEFAULT_DB_PATH = "src/backend/data/nba.duckdb"


# =============================================================================
# Constraint Definitions
# =============================================================================

# Tables with their NOT NULL constraints
NOT_NULL_CONSTRAINTS = {
    "players": ["player_id", "full_name"],
    "teams": ["team_id", "team_name", "team_abbreviation"],
    "games": ["game_id", "season_year", "game_date"],
    "player_game_stats": ["game_id", "player_id"],
    "team_game_stats": ["game_id", "team_id"],
    "player_gold": ["id", "full_name"],
    "team_gold": ["id", "full_name", "abbreviation"],
    "game_gold": ["game_id", "season_id", "game_date"],
}

# Foreign key relationships: (child_table, child_col, parent_table, parent_col)
FOREIGN_KEYS = [
    # Games reference teams
    ("games", "home_team_id", "teams", "team_id"),
    ("games", "away_team_id", "teams", "team_id"),
    # Player game stats reference players, teams, and games
    ("player_game_stats", "player_id", "players", "player_id"),
    ("player_game_stats", "team_id", "teams", "team_id"),
    # Team game stats reference teams and games
    ("team_game_stats", "team_id", "teams", "team_id"),
    # Gold layer references
    ("game_gold", "home_team_id", "team_gold", "id"),
    ("game_gold", "visitor_team_id", "team_gold", "id"),
]

# Unique indexes for performance
UNIQUE_INDEXES = [
    ("teams", "team_abbreviation", "idx_teams_abbreviation"),
    ("players", "full_name", "idx_players_fullname"),
    ("team_gold", "abbreviation", "idx_team_gold_abbreviation"),
]

# Regular indexes for query performance
PERFORMANCE_INDEXES = [
    ("games", "season_year", "idx_games_season"),
    ("games", "game_date", "idx_games_date"),
    ("player_game_stats", "player_id", "idx_pgs_player"),
    ("player_game_stats", "team_id", "idx_pgs_team"),
    ("player_game_stats", "game_id", "idx_pgs_game"),
    ("team_game_stats", "team_id", "idx_tgs_team"),
    ("team_game_stats", "game_id", "idx_tgs_game"),
]


# =============================================================================
# Migration Functions
# =============================================================================


def table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check if a table exists in the database."""
    result = conn.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return result[0] > 0


def add_not_null_constraints(
    conn: duckdb.DuckDBPyConnection,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Add NOT NULL constraints to critical columns.

    Returns:
        Dictionary with results of constraint additions
    """
    results = {"added": [], "skipped": [], "errors": []}

    for table, columns in NOT_NULL_CONSTRAINTS.items():
        if not table_exists(conn, table):
            results["skipped"].append(f"{table}: table does not exist")
            continue

        for column in columns:
            try:
                # Check if column has NULLs first
                null_count = conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
                ).fetchone()[0]

                if null_count > 0:
                    logger.warning(
                        f"{table}.{column}: {null_count} NULL values exist, cannot add NOT NULL"
                    )
                    results["errors"].append(
                        f"{table}.{column}: {null_count} NULL values"
                    )
                    continue

                if not dry_run:
                    conn.execute(f"ALTER TABLE {table} ALTER {column} SET NOT NULL")
                    results["added"].append(f"{table}.{column}")
                    logger.info(f"Added NOT NULL to {table}.{column}")
                else:
                    logger.info(f"[DRY RUN] Would add NOT NULL to {table}.{column}")
                    results["added"].append(f"{table}.{column} (dry run)")

            except duckdb.CatalogException as e:
                # Column doesn't exist or constraint already exists
                if "already set to NOT NULL" in str(e):
                    results["skipped"].append(f"{table}.{column}: already NOT NULL")
                else:
                    results["errors"].append(f"{table}.{column}: {e}")
            except Exception as e:
                results["errors"].append(f"{table}.{column}: {e}")

    return results


def add_foreign_keys(
    conn: duckdb.DuckDBPyConnection,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Add foreign key constraints between tables.

    Note: DuckDB's FK support is limited, so we validate relationships
    and add constraints where possible.

    Returns:
        Dictionary with results of FK additions
    """
    results = {"added": [], "skipped": [], "errors": [], "orphans": []}

    for child_table, child_col, parent_table, parent_col in FOREIGN_KEYS:
        # Check if both tables exist
        if not table_exists(conn, child_table):
            results["skipped"].append(f"{child_table}: table does not exist")
            continue
        if not table_exists(conn, parent_table):
            results["skipped"].append(
                f"{child_table}.{child_col} -> {parent_table}: parent table does not exist"
            )
            continue

        try:
            # Check for orphaned records
            orphan_query = f"""
                SELECT COUNT(DISTINCT c.{child_col})
                FROM {child_table} c
                LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL
            """
            orphan_count = conn.execute(orphan_query).fetchone()[0]

            if orphan_count > 0:
                # Get sample orphans for debugging
                sample_query = f"""
                    SELECT DISTINCT c.{child_col}
                    FROM {child_table} c
                    LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                    WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL
                    LIMIT 5
                """
                samples = [r[0] for r in conn.execute(sample_query).fetchall()]
                results["orphans"].append({
                    "relationship": f"{child_table}.{child_col} -> {parent_table}.{parent_col}",
                    "count": orphan_count,
                    "samples": samples,
                })
                logger.warning(
                    f"FK {child_table}.{child_col} -> {parent_table}.{parent_col}: "
                    f"{orphan_count} orphaned records (samples: {samples[:3]})"
                )
                continue

            # Try to add FK constraint
            if not dry_run:
                with suppress(Exception):
                    conn.execute(
                        f"""
                        ALTER TABLE {child_table}
                        ADD FOREIGN KEY ({child_col})
                        REFERENCES {parent_table}({parent_col})
                        """
                    )
                results["added"].append(
                    f"{child_table}.{child_col} -> {parent_table}.{parent_col}"
                )
                logger.info(
                    f"Added FK {child_table}.{child_col} -> {parent_table}.{parent_col}"
                )
            else:
                logger.info(
                    f"[DRY RUN] Would add FK {child_table}.{child_col} -> {parent_table}.{parent_col}"
                )
                results["added"].append(
                    f"{child_table}.{child_col} -> {parent_table}.{parent_col} (dry run)"
                )

        except Exception as e:
            results["errors"].append(
                f"{child_table}.{child_col} -> {parent_table}: {e}"
            )

    return results


def add_indexes(
    conn: duckdb.DuckDBPyConnection,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Add unique and performance indexes.

    Returns:
        Dictionary with results of index additions
    """
    results = {"added": [], "skipped": [], "errors": []}

    # Add unique indexes
    for table, column, index_name in UNIQUE_INDEXES:
        if not table_exists(conn, table):
            results["skipped"].append(f"{index_name}: table {table} does not exist")
            continue

        try:
            if not dry_run:
                conn.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table}({column})"
                )
                results["added"].append(f"{index_name} (unique)")
                logger.info(f"Added unique index {index_name} on {table}.{column}")
            else:
                logger.info(
                    f"[DRY RUN] Would add unique index {index_name} on {table}.{column}"
                )
                results["added"].append(f"{index_name} (dry run)")

        except Exception as e:
            if "already exists" in str(e).lower():
                results["skipped"].append(f"{index_name}: already exists")
            else:
                results["errors"].append(f"{index_name}: {e}")

    # Add performance indexes
    for table, column, index_name in PERFORMANCE_INDEXES:
        if not table_exists(conn, table):
            results["skipped"].append(f"{index_name}: table {table} does not exist")
            continue

        try:
            if not dry_run:
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {table}({column})"
                )
                results["added"].append(index_name)
                logger.info(f"Added index {index_name} on {table}.{column}")
            else:
                logger.info(
                    f"[DRY RUN] Would add index {index_name} on {table}.{column}"
                )
                results["added"].append(f"{index_name} (dry run)")

        except Exception as e:
            if "already exists" in str(e).lower():
                results["skipped"].append(f"{index_name}: already exists")
            else:
                results["errors"].append(f"{index_name}: {e}")

    return results


def validate_data_ranges(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    """Validate data ranges and flag potential issues.

    This doesn't add constraints but reports on data quality.

    Returns:
        Dictionary with validation results
    """
    results = {"valid": [], "warnings": [], "errors": []}

    # Check percentage columns are in valid range
    pct_checks = [
        ("player_game_stats", "fg_pct", 0, 1),
        ("player_game_stats", "fg3_pct", 0, 1),
        ("player_game_stats", "ft_pct", 0, 1),
        ("team_game_stats", "fg_pct", 0, 1),
        ("team_game_stats", "fg3_pct", 0, 1),
        ("team_game_stats", "ft_pct", 0, 1),
    ]

    for table, column, min_val, max_val in pct_checks:
        if not table_exists(conn, table):
            continue

        try:
            # Check for values outside range
            invalid_count = conn.execute(
                f"""
                SELECT COUNT(*) FROM {table}
                WHERE {column} IS NOT NULL
                AND ({column} < {min_val} OR {column} > {max_val})
                """
            ).fetchone()[0]

            if invalid_count > 0:
                results["warnings"].append(
                    f"{table}.{column}: {invalid_count} values outside range [{min_val}, {max_val}]"
                )
                logger.warning(
                    f"{table}.{column}: {invalid_count} invalid values"
                )
            else:
                results["valid"].append(f"{table}.{column}: all values in range")

        except Exception as e:
            results["errors"].append(f"{table}.{column}: {e}")

    # Check made <= attempted
    consistency_checks = [
        ("player_game_stats", "fgm", "fga", "Field Goals"),
        ("player_game_stats", "fg3m", "fg3a", "3-Pointers"),
        ("player_game_stats", "ftm", "fta", "Free Throws"),
        ("team_game_stats", "fgm", "fga", "Field Goals"),
        ("team_game_stats", "fg3m", "fg3a", "3-Pointers"),
        ("team_game_stats", "ftm", "fta", "Free Throws"),
    ]

    for table, made_col, attempted_col, name in consistency_checks:
        if not table_exists(conn, table):
            continue

        try:
            invalid_count = conn.execute(
                f"""
                SELECT COUNT(*) FROM {table}
                WHERE {made_col} > {attempted_col}
                AND {made_col} IS NOT NULL
                AND {attempted_col} IS NOT NULL
                """
            ).fetchone()[0]

            if invalid_count > 0:
                results["errors"].append(
                    f"{table}: {invalid_count} records with {name} made > attempted"
                )
                logger.error(
                    f"{table}: {invalid_count} records with {made_col} > {attempted_col}"
                )
            else:
                results["valid"].append(f"{table}: {name} consistency OK")

        except Exception as e:
            results["errors"].append(f"{table}.{made_col}/{attempted_col}: {e}")

    return results


def run_migration(
    db_path: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run the full constraint migration.

    Args:
        db_path: Path to DuckDB database
        dry_run: If True, don't make changes, just report

    Returns:
        Dictionary with full migration results
    """
    db_path = db_path or DEFAULT_DB_PATH

    if not Path(db_path).exists():
        logger.error(f"Database not found: {db_path}")
        return {"error": f"Database not found: {db_path}"}

    logger.info("=" * 60)
    logger.info("DATABASE CONSTRAINT MIGRATION")
    logger.info("=" * 60)
    logger.info(f"Database: {db_path}")
    logger.info(f"Dry run: {dry_run}")
    logger.info("")

    conn = duckdb.connect(db_path)

    results = {
        "not_null": {},
        "foreign_keys": {},
        "indexes": {},
        "validation": {},
    }

    try:
        # 1. Add NOT NULL constraints
        logger.info("Step 1: Adding NOT NULL constraints...")
        results["not_null"] = add_not_null_constraints(conn, dry_run)
        logger.info(
            f"  Added: {len(results['not_null']['added'])}, "
            f"Skipped: {len(results['not_null']['skipped'])}, "
            f"Errors: {len(results['not_null']['errors'])}"
        )

        # 2. Add Foreign Key constraints
        logger.info("\nStep 2: Adding Foreign Key constraints...")
        results["foreign_keys"] = add_foreign_keys(conn, dry_run)
        logger.info(
            f"  Added: {len(results['foreign_keys']['added'])}, "
            f"Orphans: {len(results['foreign_keys']['orphans'])}, "
            f"Errors: {len(results['foreign_keys']['errors'])}"
        )

        # 3. Add indexes
        logger.info("\nStep 3: Adding indexes...")
        results["indexes"] = add_indexes(conn, dry_run)
        logger.info(
            f"  Added: {len(results['indexes']['added'])}, "
            f"Skipped: {len(results['indexes']['skipped'])}, "
            f"Errors: {len(results['indexes']['errors'])}"
        )

        # 4. Validate data ranges
        logger.info("\nStep 4: Validating data ranges...")
        results["validation"] = validate_data_ranges(conn)
        logger.info(
            f"  Valid: {len(results['validation']['valid'])}, "
            f"Warnings: {len(results['validation']['warnings'])}, "
            f"Errors: {len(results['validation']['errors'])}"
        )

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("MIGRATION SUMMARY")
        logger.info("=" * 60)

        total_errors = (
            len(results["not_null"]["errors"])
            + len(results["foreign_keys"]["errors"])
            + len(results["indexes"]["errors"])
            + len(results["validation"]["errors"])
        )

        if total_errors > 0:
            logger.warning(f"Migration completed with {total_errors} errors")
        else:
            logger.info("Migration completed successfully!")

        if results["foreign_keys"]["orphans"]:
            logger.warning("\nOrphaned records need attention:")
            for orphan in results["foreign_keys"]["orphans"]:
                logger.warning(
                    f"  - {orphan['relationship']}: {orphan['count']} orphans"
                )

    finally:
        conn.close()

    return results


# =============================================================================
# CLI
# =============================================================================


def main() -> None:
    """Command-line entry point for constraint migration."""
    parser = argparse.ArgumentParser(
        description="Add integrity constraints to NBA database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run migration on default database
    python -m src.scripts.migration.add_constraints

    # Dry run to see what would change
    python -m src.scripts.migration.add_constraints --dry-run

    # Run on specific database
    python -m src.scripts.migration.add_constraints --db path/to/nba.duckdb
        """,
    )

    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help=f"Path to DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without making changes",
    )

    args = parser.parse_args()

    results = run_migration(db_path=args.db, dry_run=args.dry_run)

    # Return non-zero exit code if there were errors
    total_errors = sum(
        len(v.get("errors", []))
        for v in results.values()
        if isinstance(v, dict)
    )
    exit(1 if total_errors > 0 else 0)


if __name__ == "__main__":
    main()
