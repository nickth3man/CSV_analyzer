"""SQLExecutor node for the NBA Data Analyst Agent.

This module executes SQL safely with resilience,
as specified in design.md Section 6.6.
"""

from __future__ import annotations

import logging
from typing import Any

from pocketflow import Node

from src.backend.utils.duckdb_client import get_duckdb_client
from src.backend.utils.logger import get_logger


logger = logging.getLogger(__name__)


class SQLExecutor(Node):
    """Execute SQL safely with resilience.

    This node:
    - Executes SQL via duckdb_client.execute_query()
    - Circuit breaker handles repeated failures
    - Timeout prevents runaway queries
    """

    def prep(self, shared: dict[str, Any]) -> str:
        """Read SQL query from shared store.

        Args:
            shared: The shared store.

        Returns:
            SQL query string.
        """
        sql_query = shared.get("sql_query")
        sql_query_str = str(sql_query) if sql_query is not None else ""
        sub_query_id = self.params.get("sub_query_id")

        get_logger().log_node_start(
            "SQLExecutor",
            {"sql_length": len(sql_query_str), "sub_query_id": sub_query_id},
        )

        return sql_query_str

    def exec(self, prep_res: str) -> dict[str, Any]:
        """Execute the SQL query.

        Args:
            prep_res: SQL query to execute.

        Returns:
            Dictionary with either 'result' (DataFrame) or 'error'.
        """
        sql = prep_res
        if not sql:
            return {
                "success": False,
                "error": "No SQL query provided",
                "result": None,
            }

        db_client = get_duckdb_client()

        try:
            result = db_client.execute_query(sql)
            return {
                "success": True,
                "result": result,
                "error": None,
                "row_count": len(result) if result is not None else 0,
            }

        except TimeoutError as e:
            logger.exception("SQL execution timeout: %s", e)
            return {
                "success": False,
                "error": "Query took too long. Try a simpler question.",
                "result": None,
            }

        except Exception as e:
            error_msg = str(e)
            logger.exception("SQL execution error: %s", error_msg)

            user_friendly_error = self._format_error_message(error_msg)

            return {
                "success": False,
                "error": user_friendly_error,
                "result": None,
                "raw_error": error_msg,
            }

    def post(
        self,
        shared: dict[str, Any],
        prep_res: str,
        exec_res: dict[str, Any],
    ) -> str:
        """Store execution results in shared store.

        Args:
            shared: The shared store.
            prep_res: SQL query from prep().
            exec_res: Execution result from exec().

        Returns:
            Action string: "success" or "error".
        """
        sub_query_id = self.params.get("sub_query_id")

        if exec_res["success"]:
            if sub_query_id:
                shared.setdefault("sub_query_results", {})[sub_query_id] = exec_res[
                    "result"
                ]
                sub_errors = shared.setdefault("sub_query_errors", {})
                sub_errors.pop(sub_query_id, None)
            else:
                shared["query_result"] = exec_res["result"]
                shared["execution_error"] = None

            get_logger().log_node_end(
                "SQLExecutor",
                {"row_count": exec_res.get("row_count", 0)},
                "success",
            )

            logger.info(
                "SQL executed successfully, returned %d rows",
                exec_res.get("row_count", 0),
            )
            return "success"

        if sub_query_id:
            shared.setdefault("sub_query_errors", {})[sub_query_id] = exec_res["error"]
        else:
            shared["execution_error"] = exec_res["error"]
            shared["total_retries"] = shared.get("total_retries", 0) + 1

        get_logger().log_node_end(
            "SQLExecutor",
            {"error": exec_res["error"]},
            "error",
        )

        if sub_query_id:
            logger.warning(
                "SQL execution failed for sub-query %s: %s",
                sub_query_id,
                exec_res["error"],
            )
        else:
            logger.warning(
                "SQL execution failed (retry %d): %s",
                shared["total_retries"],
                exec_res["error"],
            )
        return "error"

    def _format_error_message(self, raw_error: str) -> str:
        """Format error message for user and SQLGenerator feedback.

        Args:
            raw_error: Raw error message from DuckDB.

        Returns:
            User-friendly error message.
        """
        error_lower = raw_error.lower()

        if "timeout" in error_lower:
            return (
                "Query took too long. Try a simpler question or limit the date range."
            )

        if "column" in error_lower and "not found" in error_lower:
            return f"Column not found: {raw_error}"

        if "table" in error_lower and "not found" in error_lower:
            return f"Table not found: {raw_error}"

        if "syntax error" in error_lower:
            return f"SQL syntax error: {raw_error}"

        if "division by zero" in error_lower:
            return (
                "Division by zero error - some calculations produced invalid results."
            )

        if "out of memory" in error_lower:
            return "Query too complex - try limiting the data range or simplifying."

        return raw_error


class SQLExecutorWithRetry(SQLExecutor):
    """SQLExecutor with configurable retry behavior.

    This variant allows setting max_retries at the Node level
    for the flow to handle retries automatically.
    """

    def __init__(self, max_retries: int = 3) -> None:
        """Initialize with retry configuration.

        Args:
            max_retries: Maximum retry attempts.
        """
        super().__init__(max_retries=max_retries)
