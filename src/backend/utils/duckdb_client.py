"""DuckDB client for NBA Data Analyst Agent.

This module provides all database interactions with resilience built-in,
as specified in design.md Section 4.1.

SECURITY NOTICE:
All table and column names are validated against a whitelist before use in queries.
All user-provided SQL is checked for injection patterns before execution.
Parameterized queries are used wherever possible to prevent SQL injection.
"""

from __future__ import annotations

import logging
import os
import re
import time
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from src.backend.models import TableMeta, ValidationResult
from src.backend.utils.logger import get_logger
from src.backend.utils.resilience import circuit_breaker, timeout


logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "nba.duckdb"

if TYPE_CHECKING:
    import pandas as pd


# ============================================================================
# SQL INJECTION PROTECTION
# ============================================================================

# Valid identifier pattern: starts with letter or underscore, contains only alphanumeric and underscores
VALID_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Reserved SQL keywords that cannot be used as identifiers
_RESERVED_WORDS = {
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "CREATE",
    "ALTER",
    "TRUNCATE",
    "UNION",
    "WHERE",
    "AND",
    "OR",
    "HAVING",
    "GROUP",
    "ORDER",
    "LIMIT",
    "OFFSET",
    "JOIN",
    "INNER",
    "OUTER",
    "LEFT",
    "RIGHT",
    "FULL",
    "ON",
    "AS",
    "FROM",
    "INTO",
    "VALUES",
    "EXEC",
    "EXECUTE",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "IF",
    "NULL",
}

# Dangerous SQL patterns to detect in user-provided queries
_DANGEROUS_SQL_PATTERNS = [
    (r";\s*DROP\b", "DROP statement detected"),
    (r";\s*TRUNCATE\b", "TRUNCATE statement detected"),
    (r";\s*DELETE\b", "DELETE statement detected"),
    (r";\s*INSERT\b", "INSERT statement detected"),
    (r";\s*UPDATE\b", "UPDATE statement detected"),
    (r";\s*ALTER\b", "ALTER statement detected"),
    (r"--", "SQL comment syntax detected"),
    (r"/\*", "SQL comment syntax detected"),
    (r"\*/", "SQL comment syntax detected"),
]

# Read-only statement enforcement to prevent unsafe operations.
_FORBIDDEN_SQL_KEYWORDS = re.compile(
    r"\b("
    r"INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|COPY|"
    r"PRAGMA|ATTACH|DETACH|EXPORT|IMPORT|CALL|EXECUTE"
    r")\b",
    re.IGNORECASE,
)


def _is_valid_identifier(identifier: str) -> bool:
    """Validate that an identifier is safe for SQL use.

    This function checks that an identifier (table name, column name, etc.)
    follows SQL identifier naming conventions and is not a reserved keyword.

    Args:
        identifier: The identifier to validate.

    Returns:
        True if the identifier is valid.

    Raises:
        ValueError: If the identifier is invalid or contains malicious patterns.
    """
    if not identifier or not identifier.strip():
        raise ValueError("Identifier cannot be empty or whitespace")

    if len(identifier) > 128:
        raise ValueError("Identifier exceeds maximum length of 128 characters")

    # Check if identifier matches the valid pattern
    if not VALID_IDENTIFIER_PATTERN.match(identifier):
        raise ValueError(
            f"Invalid identifier '{identifier}'. Identifiers must start with a letter "
            "or underscore and contain only letters, numbers, and underscores."
        )

    # Check if identifier is a reserved keyword
    if identifier.upper() in _RESERVED_WORDS:
        raise ValueError(
            f"Identifier '{identifier}' is a reserved SQL keyword and cannot be used."
        )

    return True


def _quote_identifier(identifier: str) -> str:
    """Safely quote a DuckDB identifier with validation.

    This function validates the identifier before quoting to prevent
    SQL injection attacks through identifier manipulation.

    Args:
        identifier: The identifier to quote.

    Returns:
        The properly quoted identifier for DuckDB.

    Raises:
        ValueError: If the identifier is invalid.
    """
    # Validate the identifier first
    _is_valid_identifier(identifier)

    # DuckDB uses double quotes for identifiers - escape existing quotes
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _check_for_sql_injection(sql: str) -> None:
    """Check SQL query for injection patterns.

    Args:
        sql: The SQL query to check.

    Raises:
        ValueError: If dangerous SQL patterns are detected.
    """
    sql_upper = sql.upper()

    for pattern, description in _DANGEROUS_SQL_PATTERNS:
        if re.search(pattern, sql_upper, re.IGNORECASE):
            raise ValueError(f"SQL injection risk detected: {description}")

    _validate_read_only_sql(sql)


def _validate_read_only_sql(sql: str) -> None:
    """Ensure only safe, read-only SQL statements are executed.

    Args:
        sql: The SQL query to validate.

    Raises:
        ValueError: If the query is empty, contains multiple statements, or uses
            non-read-only keywords.
    """
    stripped = sql.strip()
    if not stripped:
        raise ValueError("SQL query cannot be empty")

    # Allow a single trailing semicolon but reject multiple statements.
    if ";" in stripped[:-1]:
        raise ValueError("Multiple SQL statements are not allowed")

    if stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()

    first_keyword = re.match(r"^[A-Za-z]+", stripped)
    if not first_keyword:
        raise ValueError("SQL query must start with a valid keyword")

    if first_keyword.group(0).upper() not in {"SELECT", "WITH", "EXPLAIN"}:
        raise ValueError("Only read-only SELECT/WITH/EXPLAIN queries are allowed")

    scrubbed = re.sub(r"'(?:''|[^'])*'", "''", stripped)
    if _FORBIDDEN_SQL_KEYWORDS.search(scrubbed):
        raise ValueError("Read-only queries cannot include write operations")


def _validate_table_name(table_name: str, conn: duckdb.DuckDBPyConnection) -> str:
    """Validate a table name against the actual database schema.

    This ensures that only legitimate table names that exist in the database
    can be used in queries, preventing SQL injection through table name manipulation.

    Args:
        table_name: The table name to validate.
        conn: DuckDB connection to check against.

    Returns:
        The validated table name.

    Raises:
        ValueError: If the table name is not found in the database.
    """
    # First validate the identifier format
    _is_valid_identifier(table_name)

    # Check against actual database tables
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
    """

    result = conn.execute(query, [table_name]).fetchone()

    if not result or result[0] != table_name:
        # Get list of valid tables for better error message
        all_tables = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchdf()
        valid_tables = all_tables["table_name"].tolist() if not all_tables.empty else []

        raise ValueError(
            f"Table '{table_name}' not found in database. "
            f"Valid tables: {', '.join(sorted(valid_tables))}"
        )

    return table_name


def _validate_column_name(
    column_name: str, table_name: str, conn: duckdb.DuckDBPyConnection
) -> str:
    """Validate a column name against the actual database schema.

    Args:
        column_name: The column name to validate.
        table_name: The table name to check against.
        conn: DuckDB connection to check against.

    Returns:
        The validated column name.

    Raises:
        ValueError: If the column name is not found in the table.
    """
    # First validate the identifier format
    _is_valid_identifier(column_name)

    # Check against actual database columns
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ? AND column_name = ?
    """

    result = conn.execute(query, [table_name, column_name]).fetchone()

    if not result or result[0] != column_name:
        # Get list of valid columns for better error message
        all_columns = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
        """,
            [table_name],
        ).fetchdf()
        valid_columns = (
            all_columns["column_name"].tolist() if not all_columns.empty else []
        )

        raise ValueError(
            f"Column '{column_name}' not found in table '{table_name}'. "
            f"Valid columns: {', '.join(sorted(valid_columns))}"
        )

    return column_name


# ============================================================================
# DUCKDB CLIENT CLASS
# ============================================================================


class DuckDBClient:
    """DuckDB client with resilience patterns and SQL injection protection.

    Provides read-only database access with timeout protection,
    circuit breaker for repeated failures, and comprehensive SQL injection prevention.
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        query_timeout: int = 30,
    ) -> None:
        """Initialize a DuckDB client.

        Args:
            db_path: Path to DuckDB database file.
            query_timeout: Query timeout in seconds.
        """
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
        self.query_timeout = query_timeout
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._structured_logger = get_logger()
        # Cache of valid table names for validation
        self._valid_tables: set[str] | None = None

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

    def _get_valid_tables(self) -> set[str]:
        """Get the set of valid table names from the database.

        Returns:
            Set of valid table names.
        """
        if self._valid_tables is None:
            conn = self._get_connection()
            result = conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
            """).fetchdf()
            self._valid_tables = (
                set(result["table_name"].tolist()) if not result.empty else set()
            )
        return self._valid_tables

    def _is_valid_table(self, table_name: str) -> bool:
        """Check if a table name is valid in the database.

        Args:
            table_name: The table name to check.

        Returns:
            True if the table exists, False otherwise.
        """
        valid_tables = self._get_valid_tables()
        return table_name in valid_tables

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
            AND table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY 
                CASE 
                    WHEN table_name LIKE '%_gold' THEN 1
                    WHEN table_name LIKE '%_silver' THEN 2
                    WHEN table_name LIKE '%_raw' THEN 4
                    ELSE 3
                END,
                table_name
        """
        tables_df = conn.execute(tables_query).fetchdf()

        result = []
        for table_name in tables_df["table_name"]:
            # Validate and quote table name
            table_identifier = _quote_identifier(table_name)

            # Build COUNT query safely
            count_query = f"SELECT COUNT(*) as cnt FROM {table_identifier}"
            count_result = conn.execute(count_query).fetchone()
            row_count = count_result[0] if count_result else 0

            # Parameterized query for columns
            columns_query = """
                SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = ?
                    ORDER BY ordinal_position
            """
            columns_df = conn.execute(columns_query, [table_name]).fetchdf()
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

    def _generate_table_description(self, table_name: str, columns: list[str]) -> str:
        """Generate a description for a table based on its name and columns.

        Args:
            table_name: Name of the table.
            columns: List of column names.

        Returns:
            Human-readable description.
        """
        descriptions = {
            # Analytics Layer
            "team_rolling_metrics": "Rolling 10-game averages for team performance (PPG, Opp PPG, Win %)",
            "player_season_averages": "Season-level averages for players (PPG, RPG, APG, etc.)",
            "team_standings": "Current and historical team standings and win percentages",
            # Gold Layer (Canonical)
            "games": "Canonical game records with scores and dates",
            "team_game_stats": "Team-level statistics for every game (points, rebounds, assists, etc.)",
            "player_game_stats": "Player-level statistics for every game (points, rebounds, assists, etc.)",
            "game_gold": "Canonical game statistics (deduplicated and cleaned)",
            "player_gold": "Canonical player information (deduplicated and cleaned)",
            "team_gold": "Canonical team information (deduplicated and cleaned)",
            # Silver Layer (Typed & Normalized)
            "player_silver": "Normalized player basic information with correct types",
            "team_silver": "Normalized team information with correct types",
            "game_silver": "Normalized game statistics with correct types",
            "common_player_info_silver": "Detailed player info (birthdate, school, position, draft)",
            "draft_history_silver": "Historical draft data with correct types",
            "draft_combine_stats_silver": "Physical measurements and athletic tests from draft combine",
            # Legacy / Other
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

        if table_name.endswith("_raw"):
            return f"Raw landing table for {table_name[:-4]} (untyped)"

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

        Raises:
            ValueError: If any table name is invalid.
        """
        conn = self._get_connection()
        ddl_parts = []

        for table_name in tables:
            # Validate table name against database
            _validate_table_name(table_name, conn)

            table_identifier = _quote_identifier(table_name)

            # Parameterized query for column information
            columns_query = """
                    SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_name = ?
                        ORDER BY ordinal_position
                """
            columns_df = conn.execute(columns_query, [table_name]).fetchdf()

            if columns_df.empty:
                continue

            column_defs = []
            for _, row in columns_df.iterrows():
                # Validate and quote column name
                column_identifier = _quote_identifier(row["column_name"])
                nullable = "" if row["is_nullable"] == "YES" else " NOT NULL"
                column_defs.append(
                    f"    {column_identifier} {row['data_type']}{nullable}"
                )

            ddl = f"CREATE TABLE {table_identifier} (\n"
            ddl += ",\n".join(column_defs)
            ddl += "\n);"

            # Build COUNT query safely
            count_query = f"SELECT COUNT(*) as cnt FROM {table_identifier}"
            count_result = conn.execute(count_query).fetchone()
            row_count = count_result[0] if count_result else 0
            ddl += f"\n-- {row_count:,} rows"

            ddl_parts.append(ddl)

        return "\n\n".join(ddl_parts)

    @circuit_breaker(threshold=3, recovery=60)
    @timeout(seconds=30)
    def execute_query(self, sql: str, params: list[Any] | None = None) -> pd.DataFrame:
        """Execute a read-only SQL query with timeout protection.

        Args:
            sql: SQL query to execute.
            params: Optional parameter list for parameterized queries.

        Returns:
            DataFrame with query results.

        Raises:
            TimeoutError: If query exceeds timeout.
            duckdb.Error: If query fails.
            ValueError: If SQL injection patterns are detected.
        """
        # Check for SQL injection patterns
        _check_for_sql_injection(sql)

        conn = self._get_connection()
        start_time = time.time()

        try:
            if params:
                result = conn.execute(sql, params).fetchdf()
            else:
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
        This function also performs basic SQL injection prevention checks.

        Args:
            sql: SQL query to validate.

        Returns:
            ValidationResult with validation status and any errors.
        """
        conn = self._get_connection()
        errors = []
        warnings = []

        try:
            # Check for SQL injection patterns first
            _check_for_sql_injection(sql)

            # Use parameterized query for EXPLAIN to prevent injection
            conn.execute("EXPLAIN ?", [sql])
        except duckdb.Error as e:
            error_msg = str(e)
            errors.append(error_msg)

            if "does not exist" in error_msg.lower():
                if "column" in error_msg.lower():
                    warnings.append("Check column names against table schema")
                elif "table" in error_msg.lower():
                    warnings.append("Check table names against available tables")

        except Exception as e:
            errors.append(f"Unexpected error: {e}")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Get sample rows from a table.

        Args:
            table_name: Name of the table.
            limit: Maximum rows to return.

        Returns:
            DataFrame with sample data.

        Raises:
            ValueError: If table_name is invalid or not in database.
        """
        conn = self._get_connection()

        # Validate table name against database
        _validate_table_name(table_name, conn)

        # Build safe query
        table_identifier = _quote_identifier(table_name)
        sql = f"SELECT * FROM {table_identifier} LIMIT ?"

        return self.execute_query(sql, params=[int(limit)])


@cache
def _get_duckdb_client_cached(db_path: str | None) -> DuckDBClient:
    return DuckDBClient(db_path=db_path)


def _resolve_db_path(db_path: str | Path | None) -> str | None:
    if db_path is not None:
        return str(db_path)

    if env_path := os.environ.get("NBA_DB_PATH"):
        return env_path

    try:
        from src.backend.config import get_config

        return get_config().database.path
    except Exception:
        return str(DEFAULT_DB_PATH)


def get_duckdb_client(
    db_path: str | Path | None = None,
) -> DuckDBClient:
    """Get a global DuckDB client instance.

    Args:
        db_path: Optional path to override default database location.

    Returns:
        DuckDB client instance.
    """
    normalized_path = _resolve_db_path(db_path)
    return _get_duckdb_client_cached(normalized_path)


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

        # Validate table name
        _is_valid_identifier(table_name)

        try:
            # Use parameterized query for table creation
            table_identifier = _quote_identifier(table_name)

            conn.execute(
                f"""
                    CREATE TABLE {table_identifier} AS
                    SELECT * FROM read_csv_auto(?, header=true)
                """,
                [str(csv_file)],
            )

            # Build safe COUNT query
            count_query = f"SELECT COUNT(*) FROM {table_identifier}"
            count = conn.execute(count_query).fetchone()[0]
            logger.info(f"Created table '{table_name}' with {count:,} rows")

        except Exception as e:
            logger.exception(f"Failed to import {csv_file.name}: {e}")

    conn.close()
    logger.info(f"Database created at: {db_path}")

    return db_path
