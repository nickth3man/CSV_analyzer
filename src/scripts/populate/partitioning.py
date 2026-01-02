"""Database partitioning utilities for large NBA tables.

This module provides utilities for managing partitioned tables in DuckDB,
enabling efficient storage and querying of large datasets like:
- player_game_stats (769K+ rows)
- team_game_stats (140K+ rows)
- play_by_play (will grow to millions)
- shot_chart (will grow significantly)

Partitioning Strategy:
- Season-based partitioning for game-related data
- This allows efficient pruning when querying specific seasons

Usage:
    from src.scripts.populate.partitioning import PartitionManager

    manager = PartitionManager(db_path="path/to/db.duckdb")
    manager.create_partitioned_table("player_game_stats", partition_column="season")
    manager.migrate_data("player_game_stats_raw", "player_game_stats")
"""

import contextlib
import logging
from pathlib import Path

import duckdb


logger = logging.getLogger(__name__)


class PartitionConfig:
    """Configuration for table partitioning."""

    # Tables that benefit from partitioning
    PARTITIONABLE_TABLES = {
        "player_game_stats_raw": {
            "partition_column": "season_id",
            "expected_partitions": 20,  # ~20 seasons of data
        },
        "team_game_stats_raw": {
            "partition_column": "season_id",
            "expected_partitions": 20,
        },
        "play_by_play_raw": {
            "partition_column": "game_id",  # Partition by game prefix for season
            "expected_partitions": 20,
        },
        "shot_chart_raw": {
            "partition_column": "game_id",
            "expected_partitions": 20,
        },
        "games_raw": {
            "partition_column": "season_id",
            "expected_partitions": 20,
        },
    }

    # Minimum rows to consider partitioning
    MIN_ROWS_FOR_PARTITION = 100_000


class PartitionManager:
    """Manages partitioned tables in DuckDB.

    DuckDB doesn't have traditional partitioning like PostgreSQL,
    but we can achieve similar benefits through:
    1. Hive-style partitioning for Parquet exports
    2. Clustered indexes on partition columns
    3. View-based partitioning with UNION ALL
    """

    def __init__(self, db_path: str | Path):
        """Initialize partition manager.

        Args:
            db_path: Path to DuckDB database
        """
        self.db_path = Path(db_path)
        self.conn = duckdb.connect(str(self.db_path))

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()

    def get_table_stats(self, table_name: str) -> dict:
        """Get statistics for a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with row count, columns, and size info
        """
        try:
            row_count = self.conn.execute(
                f"SELECT count(*) FROM {table_name}"
            ).fetchone()[0]

            columns = self.conn.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
            """).fetchall()

            return {
                "table_name": table_name,
                "row_count": row_count,
                "columns": {col[0]: col[1] for col in columns},
            }
        except Exception as e:
            logger.exception(f"Error getting stats for {table_name}: {e}")
            return {"table_name": table_name, "error": str(e)}

    def analyze_partition_candidates(self) -> list[dict]:
        """Analyze tables to identify partitioning candidates.

        Returns:
            List of tables with partitioning recommendations
        """
        candidates = []

        for table_name, config in PartitionConfig.PARTITIONABLE_TABLES.items():
            stats = self.get_table_stats(table_name)

            if "error" in stats:
                continue

            row_count = stats.get("row_count", 0)
            partition_col = config["partition_column"]

            # Check if table has the partition column
            has_partition_col = partition_col in stats.get("columns", {})

            recommendation = {
                "table_name": table_name,
                "row_count": row_count,
                "partition_column": partition_col,
                "has_partition_column": has_partition_col,
                "should_partition": (
                    row_count >= PartitionConfig.MIN_ROWS_FOR_PARTITION
                    and has_partition_col
                ),
            }

            if has_partition_col:
                # Get partition distribution
                try:
                    distribution = self.conn.execute(f"""
                        SELECT {partition_col}, count(*) as cnt
                        FROM {table_name}
                        GROUP BY {partition_col}
                        ORDER BY {partition_col}
                    """).fetchall()
                    recommendation["partition_distribution"] = {
                        str(d[0]): d[1] for d in distribution
                    }
                except Exception as e:
                    logger.warning(f"Could not get distribution for {table_name}: {e}")

            candidates.append(recommendation)

        return candidates

    def create_clustered_index(
        self,
        table_name: str,
        columns: list[str],
        index_name: str | None = None,
    ) -> bool:
        """Create a clustered index for efficient partition-like access.

        DuckDB automatically clusters data during inserts, but we can
        optimize existing tables by rewriting them in sorted order.

        Args:
            table_name: Name of the table
            columns: Columns to cluster on
            index_name: Optional name for the index

        Returns:
            True if successful
        """
        if index_name is None:
            index_name = f"idx_{table_name}_{'_'.join(columns)}"

        # Define temp_table outside try block so it's accessible in except
        temp_table = f"{table_name}_sorted_temp"

        try:
            # Create a new table with sorted data
            column_list = ", ".join(columns)

            logger.info(f"Creating clustered table for {table_name} on {columns}")

            self.conn.execute(f"""
                CREATE TABLE {temp_table} AS
                SELECT * FROM {table_name}
                ORDER BY {column_list}
            """)

            # Drop original and rename
            self.conn.execute(f"DROP TABLE {table_name}")
            self.conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")

            logger.info(f"Successfully clustered {table_name} on {columns}")
            return True

        except Exception as e:
            logger.exception(f"Error creating clustered index on {table_name}: {e}")
            # Try to clean up temp table if it exists
            with contextlib.suppress(Exception):
                self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            return False

    def export_partitioned_parquet(
        self,
        table_name: str,
        output_dir: str | Path,
        partition_columns: list[str],
    ) -> bool:
        """Export table to Hive-style partitioned Parquet files.

        This enables efficient querying with partition pruning
        when reading back with DuckDB.

        Args:
            table_name: Source table name
            output_dir: Directory for Parquet files
            partition_columns: Columns to partition by

        Returns:
            True if successful
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        partition_by = ", ".join(partition_columns)

        try:
            logger.info(
                f"Exporting {table_name} to partitioned Parquet at {output_dir}"
            )

            self.conn.execute(f"""
                COPY {table_name} TO '{output_dir}'
                (FORMAT PARQUET, PARTITION_BY ({partition_by}), OVERWRITE_OR_IGNORE)
            """)

            logger.info(f"Successfully exported {table_name} to partitioned Parquet")
            return True

        except Exception as e:
            logger.exception(
                f"Error exporting {table_name} to partitioned Parquet: {e}"
            )
            return False

    def create_partitioned_view(
        self,
        view_name: str,
        parquet_dir: str | Path,
    ) -> bool:
        """Create a view over partitioned Parquet files.

        This allows querying partitioned data with automatic pruning.

        Args:
            view_name: Name for the view
            parquet_dir: Directory containing partitioned Parquet

        Returns:
            True if successful
        """
        parquet_dir = Path(parquet_dir)

        try:
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM read_parquet('{parquet_dir}/**/*.parquet', hive_partitioning=true)
            """)

            logger.info(f"Created partitioned view {view_name}")
            return True

        except Exception as e:
            logger.exception(f"Error creating partitioned view {view_name}: {e}")
            return False

    def migrate_to_partitioned(
        self,
        table_name: str,
        partition_column: str,
        parquet_base_dir: str | Path,
    ) -> dict:
        """Migrate a table to partitioned storage.

        This creates:
        1. Parquet files partitioned by the specified column
        2. A view that reads from the partitioned files
        3. Optionally backs up and drops the original table

        Args:
            table_name: Source table name
            partition_column: Column to partition by
            parquet_base_dir: Base directory for Parquet storage

        Returns:
            Migration result with statistics
        """
        parquet_base_dir = Path(parquet_base_dir)
        table_dir = parquet_base_dir / table_name

        result = {
            "table_name": table_name,
            "partition_column": partition_column,
            "success": False,
        }

        # Get before stats
        before_stats = self.get_table_stats(table_name)
        result["before_row_count"] = before_stats.get("row_count", 0)

        # Export to partitioned Parquet
        if not self.export_partitioned_parquet(
            table_name, table_dir, [partition_column]
        ):
            result["error"] = "Failed to export to Parquet"
            return result

        # Create view over partitioned data
        view_name = f"{table_name}_partitioned"
        if not self.create_partitioned_view(view_name, table_dir):
            result["error"] = "Failed to create partitioned view"
            return result

        # Verify row counts match
        view_count = self.conn.execute(f"SELECT count(*) FROM {view_name}").fetchone()[
            0
        ]

        if view_count != result["before_row_count"]:
            result["error"] = (
                f"Row count mismatch: table={result['before_row_count']}, "
                f"view={view_count}"
            )
            return result

        result["success"] = True
        result["view_name"] = view_name
        result["parquet_dir"] = str(table_dir)
        result["after_row_count"] = view_count

        logger.info(f"Successfully migrated {table_name} to partitioned storage")
        return result


def analyze_and_recommend(db_path: str | Path) -> None:
    """Analyze database and print partitioning recommendations.

    Args:
        db_path: Path to DuckDB database
    """
    manager = PartitionManager(db_path)

    try:
        print("\n=== Database Partitioning Analysis ===\n")

        candidates = manager.analyze_partition_candidates()

        for candidate in candidates:
            print(f"Table: {candidate['table_name']}")
            print(f"  Row Count: {candidate['row_count']:,}")
            print(f"  Partition Column: {candidate['partition_column']}")
            print(f"  Has Partition Column: {candidate['has_partition_column']}")
            print(f"  Should Partition: {candidate['should_partition']}")

            if "partition_distribution" in candidate:
                print("  Partition Distribution:")
                for partition, count in list(
                    candidate["partition_distribution"].items()
                )[:5]:
                    print(f"    {partition}: {count:,} rows")
                if len(candidate["partition_distribution"]) > 5:
                    print(
                        f"    ... and {len(candidate['partition_distribution']) - 5} more partitions"
                    )

            print()

    finally:
        manager.close()


# CLI entry point
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Database partitioning utilities")
    parser.add_argument(
        "action",
        choices=["analyze", "migrate", "cluster"],
        help="Action to perform",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="src/backend/data/nba.duckdb",
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--table",
        type=str,
        help="Table name (for migrate/cluster)",
    )
    parser.add_argument(
        "--partition-column",
        type=str,
        help="Column to partition by",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="src/backend/data/parquet",
        help="Output directory for Parquet files",
    )

    args = parser.parse_args()

    if args.action == "analyze":
        analyze_and_recommend(args.db_path)
    elif args.action == "migrate":
        if not args.table or not args.partition_column:
            parser.error("migrate requires --table and --partition-column")
        manager = PartitionManager(args.db_path)
        result = manager.migrate_to_partitioned(
            args.table, args.partition_column, args.output_dir
        )
        print(f"Migration result: {result}")
        manager.close()
    elif args.action == "cluster":
        if not args.table or not args.partition_column:
            parser.error("cluster requires --table and --partition-column")
        manager = PartitionManager(args.db_path)
        success = manager.create_clustered_index(args.table, [args.partition_column])
        print(f"Clustering {'successful' if success else 'failed'}")
        manager.close()
