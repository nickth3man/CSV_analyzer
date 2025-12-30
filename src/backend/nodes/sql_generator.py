"""SQLGenerator node for the NBA Data Analyst Agent.

This module generates valid DuckDB SQL with built-in self-correction,
as specified in design.md Section 6.5.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import yaml
from pocketflow import Node

from src.backend.models import SQLGenerationAttempt, ValidationResult
from src.backend.utils.call_llm import call_llm
from src.backend.utils.duckdb_client import get_duckdb_client
from src.backend.utils.logger import get_logger

logger = logging.getLogger(__name__)


SQL_GENERATION_PROMPT = """You are a DuckDB SQL expert. Generate a query to answer the user's question.

Question: {rewritten_query}

Available Schema:
{table_schemas}

{previous_attempt_context}

Rules:
- Use only columns that exist in the schema above
- Use DuckDB SQL syntax (not PostgreSQL or MySQL)
- Include appropriate JOINs based on foreign key relationships
- Use aliases for clarity
- For string comparisons, use ILIKE for case-insensitive matching
- Limit results to 100 rows unless aggregating
- Always include ORDER BY for consistent results

Output as YAML:
```yaml
thinking: |
  <your reasoning about table relationships and approach>
sql: |
  <your SQL query>
```
"""


PREVIOUS_ATTEMPT_TEMPLATE = """
Your previous attempt had issues:
- SQL: {previous_sql}
- Errors: {validation_errors}
{grader_feedback}

Fix these issues in your new attempt.
"""


class SQLGenerator(Node):
    """Generate valid DuckDB SQL with built-in self-correction.

    This node includes an internal reflection loop that:
    1. Generates SQL candidate
    2. Validates syntax via duckdb_client.validate_sql_syntax()
    3. Checks that referenced columns exist in the schema
    4. If invalid and retries remaining: incorporates errors and regenerates
    """

    def __init__(self, max_retries: int = 3) -> None:
        """Initialize the SQL generator.

        Args:
            max_retries: Maximum internal retries for validation.
        """
        super().__init__(max_retries=max_retries)
        self.internal_retries = 0
        self.max_internal_retries = max_retries

    def prep(self, shared: dict[str, Any]) -> dict[str, Any]:
        """Read query, schemas, and any previous feedback.

        Args:
            shared: The shared store.

        Returns:
            Dictionary with inputs for SQL generation.
        """
        sub_query_id = self.params.get("sub_query_id")
        sub_query_description = self.params.get("sub_query_description")
        rewritten_query = sub_query_description or shared.get(
            "rewritten_query", shared.get("question", "")
        )
        table_schemas = shared.get("table_schemas", "")
        if sub_query_id:
            previous_attempts = (
                shared.get("sub_query_attempts", {}).get(sub_query_id, [])
            )
            grader_feedback = None
            execution_error = shared.get("sub_query_errors", {}).get(sub_query_id)
        else:
            previous_attempts = shared.get("generation_attempts", [])
            grader_feedback = shared.get("grader_feedback")
            execution_error = shared.get("execution_error")

        self.internal_retries = 0

        get_logger().log_node_start(
            "SQLGenerator",
            {
                "query": rewritten_query,
                "has_feedback": grader_feedback is not None,
                "has_error": execution_error is not None,
                "previous_attempts": len(previous_attempts),
            },
        )

        return {
            "rewritten_query": rewritten_query,
            "table_schemas": table_schemas,
            "grader_feedback": grader_feedback,
            "execution_error": execution_error,
            "previous_attempts": previous_attempts,
            "sub_query_id": sub_query_id,
        }

    def exec(self, prep_res: dict[str, Any]) -> dict[str, Any]:
        """Generate SQL with internal validation loop.

        Args:
            prep_res: Dictionary with query and schema context.

        Returns:
            Dictionary with sql, is_valid, and attempts list.
        """
        rewritten_query = prep_res["rewritten_query"]
        table_schemas = prep_res["table_schemas"]
        grader_feedback = prep_res["grader_feedback"]
        execution_error = prep_res["execution_error"]
        previous_attempts = prep_res["previous_attempts"]
        sub_query_id = prep_res.get("sub_query_id")

        db_client = get_duckdb_client()
        attempts = []
        current_sql = ""
        is_valid = False
        last_errors: list[str] = []

        for attempt_num in range(self.max_internal_retries):
            self.internal_retries = attempt_num

            previous_context = self._build_previous_context(
                previous_attempts,
                attempts,
                grader_feedback,
                execution_error,
                last_errors,
            )

            prompt = SQL_GENERATION_PROMPT.format(
                rewritten_query=rewritten_query,
                table_schemas=table_schemas,
                previous_attempt_context=previous_context,
            )

            response = call_llm(prompt)
            parsed = self._parse_sql_response(response)
            current_sql = parsed.get("sql", "")

            if not current_sql:
                last_errors = ["Failed to parse SQL from LLM response"]
                attempts.append(
                    SQLGenerationAttempt(
                        attempt_number=attempt_num + 1,
                        sql="",
                        validation=ValidationResult(
                            is_valid=False,
                            errors=last_errors,
                        ),
                    )
                )
                continue

            validation = db_client.validate_sql_syntax(current_sql)

            schema_errors = self._validate_schema_references(
                current_sql, table_schemas
            )
            if schema_errors:
                validation = ValidationResult(
                    is_valid=False,
                    errors=validation.errors + schema_errors,
                    warnings=validation.warnings,
                )

            attempts.append(
                SQLGenerationAttempt(
                    attempt_number=attempt_num + 1,
                    sql=current_sql,
                    validation=validation,
                )
            )

            if validation.is_valid:
                is_valid = True
                break

            last_errors = validation.errors
            logger.warning(
                "SQL validation failed (attempt %d/%d): %s",
                attempt_num + 1,
                self.max_internal_retries,
                last_errors,
            )

        return {
            "sql": current_sql,
            "is_valid": is_valid,
            "attempts": attempts,
            "thinking": parsed.get("thinking", ""),
            "sub_query_id": sub_query_id,
        }

    def post(
        self,
        shared: dict[str, Any],
        prep_res: dict[str, Any],
        exec_res: dict[str, Any],
    ) -> str:
        """Store SQL and validation results in shared store.

        Args:
            shared: The shared store.
            prep_res: Result from prep().
            exec_res: Result from exec().

        Returns:
            Action string: "valid" or "fallback".
        """
        sub_query_id = exec_res.get("sub_query_id")
        shared["sql_is_valid"] = exec_res["is_valid"]

        if sub_query_id:
            shared.setdefault("sub_query_sqls", {})[sub_query_id] = exec_res["sql"]
            sub_attempts = shared.setdefault("sub_query_attempts", {})
            existing_attempts = sub_attempts.get(sub_query_id, [])
            sub_attempts[sub_query_id] = existing_attempts + exec_res["attempts"]
        else:
            shared["sql_query"] = exec_res["sql"]
            existing_attempts = shared.get("generation_attempts", [])
            shared["generation_attempts"] = existing_attempts + exec_res["attempts"]

        get_logger().log_node_end(
            "SQLGenerator",
            {
                "sql": exec_res["sql"][:200] + "..." if len(exec_res["sql"]) > 200 else exec_res["sql"],
                "is_valid": exec_res["is_valid"],
                "internal_attempts": len(exec_res["attempts"]),
            },
            "success" if exec_res["is_valid"] else "validation_failed",
        )

        if exec_res["is_valid"]:
            logger.info("Generated valid SQL after %d attempts", len(exec_res["attempts"]))
            return "valid"

        logger.warning("SQL generation failed after max retries")
        return "fallback"

    def _build_previous_context(
        self,
        previous_attempts: list[SQLGenerationAttempt],
        current_attempts: list[SQLGenerationAttempt],
        grader_feedback: Any,
        execution_error: str | None,
        last_errors: list[str],
    ) -> str:
        """Build context about previous attempts for the prompt.

        Args:
            previous_attempts: Attempts from previous node runs.
            current_attempts: Attempts from current exec() loop.
            grader_feedback: Feedback from ResponseGrader if any.
            execution_error: Error from SQLExecutor if any.
            last_errors: Errors from last attempt.

        Returns:
            Formatted context string for the prompt.
        """
        all_attempts = previous_attempts + current_attempts

        if not all_attempts and not grader_feedback and not execution_error:
            return ""

        context_parts = []

        if all_attempts:
            last_attempt = all_attempts[-1]
            errors = last_attempt.validation.errors + last_errors
            if last_attempt.execution_error:
                errors.append(last_attempt.execution_error)

            if errors:
                context_parts.append(
                    PREVIOUS_ATTEMPT_TEMPLATE.format(
                        previous_sql=last_attempt.sql[:500],
                        validation_errors="\n  - ".join(errors),
                        grader_feedback=self._format_grader_feedback(grader_feedback),
                    )
                )

        if execution_error and not context_parts:
            context_parts.append(f"\nExecution error from previous attempt: {execution_error}")

        return "\n".join(context_parts)

    def _format_grader_feedback(self, feedback: Any) -> str:
        """Format grader feedback for the prompt.

        Args:
            feedback: GraderFeedback object or None.

        Returns:
            Formatted feedback string.
        """
        if feedback is None:
            return ""

        parts = []
        if hasattr(feedback, "issues") and feedback.issues:
            parts.append("Issues identified by quality check:")
            for issue in feedback.issues:
                parts.append(f"  - {issue}")

        if hasattr(feedback, "suggestions") and feedback.suggestions:
            parts.append("Suggestions for improvement:")
            for suggestion in feedback.suggestions:
                parts.append(f"  - {suggestion}")

        return "\n".join(parts) if parts else ""

    def _parse_sql_response(self, response: str) -> dict[str, str]:
        """Parse YAML response from LLM.

        Args:
            response: Raw LLM response.

        Returns:
            Dictionary with 'sql' and optionally 'thinking'.
        """
        try:
            yaml_match = re.search(r"```yaml\s*(.*?)\s*```", response, re.DOTALL)
            if yaml_match:
                yaml_str = yaml_match.group(1)
            else:
                yaml_str = response.strip()

            result = yaml.safe_load(yaml_str)

            if isinstance(result, dict):
                sql = result.get("sql", "")
                if isinstance(sql, str):
                    sql = sql.strip()
                return {
                    "sql": sql,
                    "thinking": result.get("thinking", ""),
                }

        except yaml.YAMLError as e:
            logger.warning("Failed to parse YAML: %s", e)

        sql_match = re.search(
            r"(?:SELECT|INSERT|UPDATE|DELETE|WITH)\s+.*?(?:;|$)",
            response,
            re.IGNORECASE | re.DOTALL,
        )
        if sql_match:
            return {"sql": sql_match.group(0).strip(), "thinking": ""}

        return {"sql": "", "thinking": ""}

    def _validate_schema_references(
        self, sql: str, table_schemas: str
    ) -> list[str]:
        """Validate that SQL references only tables/columns in the schema."""
        errors: list[str] = []

        table_pattern = r"CREATE TABLE\s+([A-Za-z_][\\w]*)\s*\\("
        available_tables = set(re.findall(table_pattern, table_schemas, re.IGNORECASE))

        from_pattern = (
            r"FROM\s+([\"']?)([A-Za-z_][\\w]*)\1"
            r"(?:\s+(?:AS\s+)?([A-Za-z_][\\w]*))?"
        )
        join_pattern = (
            r"JOIN\s+([\"']?)([A-Za-z_][\\w]*)\1"
            r"(?:\s+(?:AS\s+)?([A-Za-z_][\\w]*))?"
        )
        reserved_aliases = {
            "where",
            "join",
            "on",
            "group",
            "order",
            "limit",
            "having",
            "union",
            "left",
            "right",
            "inner",
            "outer",
        }

        alias_map: dict[str, str] = {}
        for _, table, alias in re.findall(from_pattern, sql, re.IGNORECASE):
            if alias and alias.lower() not in reserved_aliases:
                alias_map[alias] = table
        for _, table, alias in re.findall(join_pattern, sql, re.IGNORECASE):
            if alias and alias.lower() not in reserved_aliases:
                alias_map[alias] = table

        referenced_tables = {
            table for _, table, _ in re.findall(from_pattern, sql, re.IGNORECASE)
        }
        referenced_tables.update(
            table for _, table, _ in re.findall(join_pattern, sql, re.IGNORECASE)
        )

        available_tables_lower = {t.lower() for t in available_tables}
        for table in referenced_tables:
            if table.lower() not in available_tables_lower:
                errors.append(f"Table '{table}' is not in the available schema")

        schema_columns = self._parse_schema_columns(table_schemas)
        column_refs = re.findall(
            r"([A-Za-z_][\\w]*)\s*\\.\\s*([A-Za-z_][\\w]*)",
            sql,
        )
        for table_or_alias, column in column_refs:
            table_name = alias_map.get(table_or_alias, table_or_alias)
            columns = schema_columns.get(table_name.lower())
            if columns is None:
                continue
            if column.lower() not in columns:
                errors.append(
                    f"Column '{column}' not found in table '{table_name}'"
                )

        return errors

    def _parse_schema_columns(self, table_schemas: str) -> dict[str, set[str]]:
        """Parse table schemas into a mapping of table -> columns."""
        table_columns: dict[str, set[str]] = {}
        pattern = re.compile(
            r"CREATE TABLE\s+([A-Za-z_][\\w]*)\s*\\((.*?)\\);",
            re.IGNORECASE | re.DOTALL,
        )
        for match in pattern.finditer(table_schemas):
            table_name = match.group(1)
            column_block = match.group(2)
            columns: set[str] = set()
            for line in column_block.splitlines():
                line = line.strip().rstrip(",")
                if not line or line.startswith("--"):
                    continue
                col_name = line.split()[0].strip("\"'")
                if col_name:
                    columns.add(col_name.lower())
            table_columns[table_name.lower()] = columns

        return table_columns
