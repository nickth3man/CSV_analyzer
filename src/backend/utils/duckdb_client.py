"""DuckDB client for the NBA Data Analyst Agent.

This module provides all database interactions with resilience built-in,
as specified in design.md Section 4.1.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pandas as pd

from src.backend.models import TableMeta, ValidationResult
from src.backend.utils.logger import get_logger
from src.backend.utils.resilience import circuit_breaker, timeout

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "nba.duckdb"


class DuckDBClient:
    """DuckDB client with resilience patterns.

    Provides read-only database access with timeout protection
    and circuit breaker for repeated failures.
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        query_timeout: int = 30,
    ) -> None:
        """Initialize the DuckDB client.

        Args:
            db_path: Path to the DuckDB database file.
            query_timeout: Query timeout in seconds.
        """
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.query_timeout = query_timeout
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._structured_logger = get_logger()

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create a database connection.

        Returns:
            DuckDB connection.

        Raises:
            FileNotFoundError: If database file doesn't exist.
        """
        if self._connection is None:
            if not self.db_path.exists():
                raise FileNotFoundError(f"Database not found: {self.db_path}")
            self._connection = duckdb.connect(str(self.db_path), read_only=True)
        return self._connection

    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def get_all_tables(self) -> list[TableMeta]:
        """Get all tables in the database with metadata.

        Returns:
            List of TableMeta objects with table names and descriptions.
        """
        conn = self._get_connection()

        tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        tables_df = conn.execute(tables_query).fetchdf()

        result = []
        for table_name in tables_df["table_name"]:
            count_result = conn.execute(
                f'SELECT COUNT(*) as cnt FROM "{table_name}"'
            ).fetchone()
            row_count = count_result[0] if count_result else 0

            columns_query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """
            columns_df = conn.execute(columns_query).fetchdf()
            columns = columns_df["column_name"].tolist()

            description = self._generate_table_description(table_name, columns)

            result.append(
                TableMeta(
                    name=table_name,
                    description=description,
                    row_count=row_count,
                    columns=columns,
                )
            )

        return result

    def _generate_table_description(
        self, table_name: str, columns: list[str]
    ) -> str:
        """Generate a description for a table based on its name and columns.

        Args:
            table_name: Name of the table.
            columns: List of column names.

        Returns:
            Human-readable description.
        """
        descriptions = {
            "player": "Player basic information including name and active status",
            "team": "Team information including name, abbreviation, city, and founding year",
            "game": "Game statistics for home and away teams including scores and shooting stats",
            "common_player_info": "Detailed player info including birthdate, school, position, draft info",
            "draft_history": "Historical draft data including picks and players",
            "draft_combine_stats": "Physical measurements and athletic tests from draft combine",
            "game_info": "Game metadata including arena, attendance, and officials",
            "game_summary": "Game summary including date, season, and matchup",
            "line_score": "Quarter-by-quarter scoring breakdown",
            "officials": "Game officials (referees) information",
            "other_stats": "Additional game statistics",
            "inactive_players": "Players inactive for specific games",
            "team_details": "Detailed team information including arena and history",
            "team_history": "Historical team records and championships",
            "team_info_common": "Common team information and current season stats",
        }

        if table_name in descriptions:
            return descriptions[table_name]

        col_sample = ", ".join(columns[:5])
        if len(columns) > 5:
            col_sample += f" (+{len(columns) - 5} more)"
        return f"Table with columns: {col_sample}"

    def get_table_schema(self, tables: list[str]) -> str:
        """Get CREATE TABLE statements for specified tables.

        Args:
            tables: List of table names.

        Returns:
            DDL string with CREATE TABLE statements.
        """
        conn = self._get_connection()
        ddl_parts = []

        for table_name in tables:
            columns_query = f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """
            columns_df = conn.execute(columns_query).fetchdf()

            if columns_df.empty:
                continue

            column_defs = []
            for _, row in columns_df.iterrows():
                nullable = "" if row["is_nullable"] == "YES" else " NOT NULL"
                column_defs.append(f"    {row['column_name']} {row['data_type']}{nullable}")

            ddl = f"CREATE TABLE {table_name} (\n"
            ddl += ",\n".join(column_defs)
            ddl += "\n);"

            count_result = conn.execute(
                f'SELECT COUNT(*) as cnt FROM "{table_name}"'
            ).fetchone()
            row_count = count_result[0] if count_result else 0
            ddl += f"\n-- {row_count:,} rows"

            ddl_parts.append(ddl)

        return "\n\n".join(ddl_parts)

    @circuit_breaker(threshold=3, recovery=60)
    @timeout(seconds=30)
    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute a read-only SQL query with timeout protection.

        Args:
            sql: SQL query to execute.

        Returns:
            DataFrame with query results.

        Raises:
            TimeoutError: If query exceeds timeout.
            duckdb.Error: If query fails.
        """
        conn = self._get_connection()
        start_time = time.time()

        try:
            result = conn.execute(sql).fetchdf()
            latency_ms = int((time.time() - start_time) * 1000)

            self._structured_logger.log_sql_execution(
                sql=sql,
                row_count=len(result),
                latency_ms=latency_ms,
            )

            return result

        except Exception as e:
            latency_ms = int((time.time() - start_time) * 1000)
            self._structured_logger.log_sql_execution(
                sql=sql,
                row_count=0,
                latency_ms=latency_ms,
                error=str(e),
            )
            raise

    def validate_sql_syntax(self, sql: str) -> ValidationResult:
        """Validate SQL syntax without executing.

        Uses DuckDB's EXPLAIN to check syntax without running the query.

        Args:
            sql: SQL query to validate.

        Returns:
            ValidationResult with validation status and any errors.
        """
        conn = self._get_connection()
        errors = []
        warnings = []

        try:
            conn.execute(f"EXPLAIN {sql}")
        except duckdb.Error as e:
            error_msg = str(e)
            errors.append(error_msg)

            if "does not exist" in error_msg.lower():
                if "column" in error_msg.lower():
                    warnings.append("Check column names against the table schema")
                elif "table" in error_msg.lower():
                    warnings.append("Check table names against available tables")

        except Exception as e:
            errors.append(f"Unexpected error: {e}")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def get_sample_data(
        self, table_name: str, limit: int = 5
    ) -> pd.DataFrame:
        """Get sample rows from a table.

        Args:
            table_name: Name of the table.
            limit: Maximum rows to return.

        Returns:
            DataFrame with sample data.
        """
        sql = f'SELECT * FROM "{table_name}" LIMIT {limit}'
        return self.execute_query(sql)


_client_instance: DuckDBClient | None = None


def get_duckdb_client(
    db_path: str | Path | None = None,
) -> DuckDBClient:
    """Get the global DuckDB client instance.

    Args:
        db_path: Optional path to override default database location.

    Returns:
        DuckDB client instance.
    """
    global _client_instance
    if _client_instance is None or db_path is not None:
        _client_instance = DuckDBClient(db_path=db_path)
    return _client_instance


def initialize_database_from_csvs(
    csv_dir: str | Path,
    db_path: str | Path | None = None,
) -> Path:
    """Initialize DuckDB database from CSV files.

    Args:
        csv_dir: Directory containing CSV files.
        db_path: Path for the DuckDB database file.

    Returns:
        Path to the created database.
    """
    csv_dir = Path(csv_dir)
    db_path = Path(db_path) if db_path else DEFAULT_DB_PATH

    db_path.parent.mkdir(parents=True, exist_ok=True)

    if db_path.exists():
        os.remove(db_path)

    conn = duckdb.connect(str(db_path))

    csv_files = list(csv_dir.glob("*.csv"))
    logger.info(f"Found {len(csv_files)} CSV files to import")

    for csv_file in csv_files:
        table_name = csv_file.stem

        try:
            conn.execute(
                f"""
                CREATE TABLE "{table_name}" AS
                SELECT * FROM read_csv_auto('{csv_file}', header=true)
                """
            )

            count = conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            ).fetchone()[0]
            logger.info(f"Created table '{table_name}' with {count:,} rows")

        except Exception as e:
            logger.error(f"Failed to import {csv_file.name}: {e}")

    conn.close()
    logger.info(f"Database created at: {db_path}")

    return db_path
