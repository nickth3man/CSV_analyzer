"""Data analysis node for the NBA Data Analyst Agent.

This module synthesizes SQL results into natural language answers,
as specified in design.md Section 6.7.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import pandas as pd
import yaml
from pocketflow import Node

from src.backend.utils.call_llm import call_llm
from src.backend.utils.logger import get_logger
from src.backend.utils.memory import get_memory


logger = logging.getLogger(__name__)

MAX_RESULT_CHARS = 3500


DATA_ANALYZER_PROMPT = """You are an NBA data analyst explaining query results to a fan.

Original Question: {question}
Interpreted As: {rewritten_query}

SQL Query Used:
{sql_query}

Results:
{query_result_formatted}

Provide:
1. A direct answer to the question in conversational language
2. A brief "How I found this" explanation (1-2 sentences)

Output as YAML:
```yaml
answer: |
  <natural language answer>
transparency_note: |
  <brief explanation of methodology>
```
"""


class DataAnalyzer(Node):
    """Synthesize SQL results into natural language with transparency notes."""

    def prep(self, shared: dict[str, Any]) -> dict[str, Any]:
        """Read inputs for synthesis from shared store."""
        get_logger().log_node_start(
            "DataAnalyzer",
            {
                "has_result": shared.get("query_result") is not None,
                "has_sql": bool(shared.get("sql_query")),
            },
        )

        return {
            "question": shared.get("question", ""),
            "rewritten_query": shared.get("rewritten_query", ""),
            "sql_query": shared.get("sql_query", ""),
            "query_result": shared.get("query_result"),
            "execution_error": shared.get("execution_error"),
            "sub_query_sqls": shared.get("sub_query_sqls", {}),
            "query_plan": shared.get("query_plan"),
            "selected_tables": shared.get("selected_tables", []),
            "sub_query_tables": shared.get("sub_query_tables", {}),
        }

    def exec(self, prep_res: dict[str, Any]) -> dict[str, str]:
        """Generate final answer and transparency note."""
        question = prep_res["question"]
        rewritten_query = prep_res["rewritten_query"]
        sql_query = prep_res["sql_query"]
        query_result = prep_res["query_result"]
        execution_error = prep_res["execution_error"]
        sub_query_sqls = prep_res["sub_query_sqls"]

        if query_result is None:
            return self._fallback_response(question, execution_error)

        sql_block = self._format_sql_block(sql_query, sub_query_sqls)
        result_str = self._format_query_result(query_result)

        prompt = DATA_ANALYZER_PROMPT.format(
            question=question,
            rewritten_query=rewritten_query,
            sql_query=sql_block,
            query_result_formatted=result_str,
        )

        response = call_llm(prompt)
        parsed = self._parse_response(response)

        if not parsed.get("answer"):
            parsed["answer"] = (
                "I was able to retrieve the data, but couldn't summarize it clearly."
            )
        if not parsed.get("transparency_note"):
            parsed["transparency_note"] = (
                "I queried the NBA database and summarized the results."
            )

        return parsed

    def post(
        self,
        shared: dict[str, Any],
        prep_res: dict[str, Any],
        exec_res: dict[str, str],
    ) -> str:
        """Store final answer and transparency note."""
        answer = exec_res.get("answer", "")
        transparency_note = exec_res.get("transparency_note", "")

        shared["final_answer"] = answer
        shared["transparency_note"] = transparency_note

        if transparency_note:
            shared["final_text"] = f"{answer}\n\nHow I found this:\n{transparency_note}"
        else:
            shared["final_text"] = answer

        self._update_memory(shared, answer)

        get_logger().log_node_end(
            "DataAnalyzer",
            {"answer_length": len(answer)},
            "success",
        )

        logger.info("Generated final response (%d chars)", len(answer))
        if prep_res.get("query_result") is None:
            return "fallback"
        return "default"

    def _format_query_result(self, result: Any) -> str:
        """Format query result for prompt consumption."""
        if result is None:
            return "No results."

        if isinstance(result, pd.DataFrame):
            if result.empty:
                return "Empty result set."
            preview = result.head(20).to_string(index=False)
            if len(result) > 20:
                preview += f"\n... ({len(result) - 20} more rows)"
            return self._truncate(preview)

        try:
            result_str = json.dumps(result, indent=2, default=str)
        except (TypeError, ValueError):
            result_str = str(result)

        return self._truncate(result_str)

    def _format_sql_block(
        self, sql_query: str, sub_query_sqls: dict[str, str]
    ) -> str:
        """Format SQL for prompt, handling multi-query plans."""
        if sub_query_sqls:
            lines = []
            for sub_id, sql in sub_query_sqls.items():
                lines.append(f"-- {sub_id}")
                lines.append(sql.strip())
            return "\n".join(lines)
        return sql_query.strip() if sql_query else "No SQL query available."

    def _parse_response(self, response: str) -> dict[str, str]:
        """Parse YAML response from LLM."""
        try:
            match = re.search(r"```yaml\s*(.*?)\s*```", response, re.DOTALL)
            yaml_str = match.group(1) if match else response.strip()
            parsed = yaml.safe_load(yaml_str)
            if isinstance(parsed, dict):
                return {
                    "answer": str(parsed.get("answer", "")).strip(),
                    "transparency_note": str(parsed.get("transparency_note", "")).strip(),
                }
        except yaml.YAMLError as exc:
            logger.warning("Failed to parse analyzer YAML: %s", exc)
        except Exception as exc:
            logger.warning("Unexpected analyzer parse error: %s", exc)

        return {"answer": response.strip(), "transparency_note": ""}

    def _fallback_response(self, question: str, error: str | None) -> dict[str, str]:
        """Return a fallback response when results are unavailable."""
        reason = f" (error: {error})" if error else ""
        return {
            "answer": (
                "I couldn't retrieve results for that question. "
                "Please try rephrasing or narrowing the time range."
            ),
            "transparency_note": f"No results were returned from the database{reason}.",
        }

    def _truncate(self, text: str) -> str:
        """Truncate long strings for prompt safety."""
        if len(text) > MAX_RESULT_CHARS:
            return text[:MAX_RESULT_CHARS] + "... [truncated]"
        return text

    def _update_memory(self, shared: dict[str, Any], answer: str) -> None:
        """Update conversation memory with the latest turn."""
        memory = get_memory()

        tables_used = shared.get("selected_tables", [])
        sub_query_tables = shared.get("sub_query_tables", {})
        if sub_query_tables:
            tables_used = sorted(
                {table for tables in sub_query_tables.values() for table in tables}
            )

        memory.add_turn(
            question=shared.get("question", ""),
            answer=answer,
            sql=shared.get("sql_query"),
            rewritten_query=shared.get("rewritten_query"),
            tables_used=tables_used or None,
        )
