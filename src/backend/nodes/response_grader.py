"""ResponseGrader node for the NBA Data Analyst Agent.

This module performs quality check ensuring the response answers the question,
as specified in design.md Section 6.8.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import yaml
from pocketflow import Node

from src.backend.models import GradeStatus, GraderFeedback
from src.backend.utils.call_llm import call_llm
from src.backend.utils.logger import get_logger

logger = logging.getLogger(__name__)


GRADER_PROMPT = """You are a quality assurance reviewer for an NBA data analysis system.

Original Question: {question}
Interpreted As: {rewritten_query}

SQL Query:
{sql_query}

Query Results (first 20 rows):
{query_result}

Generated Answer:
{final_answer}

Evaluate:
1. CORRECTNESS: Does the SQL correctly answer the question?
2. INTERPRETATION: Is the answer mathematically accurate given the data?
3. COMPLETENESS: Does the answer fully address what was asked?
4. HALLUCINATION: Does the answer make claims not supported by the data?

Output as YAML:
```yaml
status: pass | fail
confidence: <0.0 to 1.0>
issues:  # Only if fail
  - <issue 1>
  - <issue 2>
suggestions:  # Specific fixes for SQLGenerator
  - <suggestion 1>
```
"""


class ResponseGrader(Node):
    """Quality check ensuring the response answers the question.

    This node acts as a critic, checking:
    1. Does the SQL actually answer the specific question asked?
    2. Is the data interpretation mathematically/logically correct?
    3. Does the final answer make sense given the data?
    4. Are there any hallucinations or unsupported claims?
    """

    def __init__(self, max_retries_allowed: int = 2) -> None:
        """Initialize the grader.

        Args:
            max_retries_allowed: Max times to retry SQL generation on fail.
        """
        super().__init__()
        self.max_retries_allowed = max_retries_allowed

    def prep(self, shared: dict[str, Any]) -> dict[str, Any]:
        """Read all relevant data for grading.

        Args:
            shared: The shared store.

        Returns:
            Dictionary with question, query, results, and answer.
        """
        get_logger().log_node_start(
            "ResponseGrader",
            {"has_result": shared.get("query_result") is not None},
        )

        return {
            "question": shared.get("question", ""),
            "rewritten_query": shared.get("rewritten_query", ""),
            "sql_query": shared.get("sql_query", ""),
            "sub_query_sqls": shared.get("sub_query_sqls", {}),
            "query_result": shared.get("query_result"),
            "final_answer": shared.get("final_answer", ""),
            "query_plan": shared.get("query_plan"),
            "grader_retries": shared.get("grader_retries", 0),
        }

    def exec(self, prep_res: dict[str, Any]) -> GraderFeedback:
        """Evaluate the response quality using LLM.

        Args:
            prep_res: Dictionary with all grading inputs.

        Returns:
            GraderFeedback with status, issues, and suggestions.
        """
        question = prep_res["question"]
        rewritten_query = prep_res["rewritten_query"]
        sql_query = prep_res["sql_query"]
        sub_query_sqls = prep_res["sub_query_sqls"]
        query_result = prep_res["query_result"]
        final_answer = prep_res["final_answer"]

        result_str = self._format_query_result(query_result)
        sql_block = self._format_sql_block(sql_query, sub_query_sqls)

        prompt = GRADER_PROMPT.format(
            question=question,
            rewritten_query=rewritten_query,
            sql_query=sql_block,
            query_result=result_str,
            final_answer=final_answer,
        )

        response = call_llm(prompt)
        return self._parse_grader_response(response)

    def post(
        self,
        shared: dict[str, Any],
        prep_res: dict[str, Any],
        exec_res: GraderFeedback,
    ) -> str:
        """Store grader feedback and determine next action.

        Args:
            shared: The shared store.
            prep_res: Result from prep().
            exec_res: GraderFeedback from exec().

        Returns:
            Action string: "pass", "fail", or "pass_with_warning".
        """
        shared["grader_feedback"] = exec_res

        get_logger().log_node_end(
            "ResponseGrader",
            {
                "status": exec_res.status.value,
                "confidence": exec_res.confidence,
                "issues": len(exec_res.issues),
            },
            exec_res.status.value,
        )

        if exec_res.status == GradeStatus.PASS:
            logger.info(
                "Response passed quality check (confidence: %.2f)",
                exec_res.confidence,
            )
            return "pass"

        grader_retries = prep_res.get("grader_retries", 0) + 1
        shared["grader_retries"] = grader_retries

        if grader_retries <= self.max_retries_allowed:
            complexity = self._get_complexity(prep_res.get("query_plan"))
            logger.warning(
                "Response failed quality check, triggering retry. Issues: %s",
                exec_res.issues,
            )
            if complexity == "complex":
                return "fail_complex"
            return "fail"

        logger.warning(
            "Response failed but max retries exceeded. Passing with warning. Issues: %s",
            exec_res.issues,
        )
        return "pass_with_warning"

    def _format_query_result(self, result: Any) -> str:
        """Format query result for the prompt.

        Args:
            result: DataFrame or None.

        Returns:
            Formatted string representation.
        """
        if result is None:
            return "No results (query returned empty or failed)"

        try:
            import pandas as pd

            if isinstance(result, pd.DataFrame):
                if result.empty:
                    return "Empty result set"

                if len(result) > 20:
                    result_str = result.head(20).to_string()
                    result_str += f"\n... ({len(result) - 20} more rows)"
                else:
                    result_str = result.to_string()

                return result_str

        except Exception as e:
            logger.warning("Failed to format result: %s", e)

        return str(result)[:2000]

    def _format_sql_block(
        self, sql_query: str, sub_query_sqls: dict[str, str]
    ) -> str:
        """Format SQL block for grading."""
        if sub_query_sqls:
            lines = []
            for sub_id, sql in sub_query_sqls.items():
                lines.append(f"-- {sub_id}")
                lines.append(sql.strip())
            return "\n".join(lines)
        return sql_query.strip() if sql_query else "No SQL query available."

    def _get_complexity(self, query_plan: Any) -> str:
        """Extract complexity from query plan."""
        if query_plan is None:
            return "simple"
        if isinstance(query_plan, dict):
            return str(query_plan.get("complexity", "simple")).lower()
        if hasattr(query_plan, "complexity"):
            return str(query_plan.complexity.value).lower()
        return "simple"

    def _parse_grader_response(self, response: str) -> GraderFeedback:
        """Parse YAML response from LLM.

        Args:
            response: Raw LLM response.

        Returns:
            GraderFeedback object.
        """
        try:
            yaml_match = re.search(r"```yaml\s*(.*?)\s*```", response, re.DOTALL)
            if yaml_match:
                yaml_str = yaml_match.group(1)
            else:
                yaml_str = response.strip()

            result = yaml.safe_load(yaml_str)

            if not isinstance(result, dict):
                return self._default_feedback()

            status_str = result.get("status", "pass").lower()
            status = GradeStatus.PASS if status_str == "pass" else GradeStatus.FAIL

            confidence = result.get("confidence", 1.0)
            if not isinstance(confidence, (int, float)):
                confidence = 1.0
            confidence = max(0.0, min(1.0, float(confidence)))

            issues = result.get("issues", [])
            if not isinstance(issues, list):
                issues = [str(issues)] if issues else []

            suggestions = result.get("suggestions", [])
            if not isinstance(suggestions, list):
                suggestions = [str(suggestions)] if suggestions else []

            return GraderFeedback(
                status=status,
                confidence=confidence,
                issues=issues,
                suggestions=suggestions,
            )

        except yaml.YAMLError as e:
            logger.warning("Failed to parse grader YAML: %s", e)
            return self._default_feedback()

    def _default_feedback(self) -> GraderFeedback:
        """Return default passing feedback when parsing fails.

        Returns:
            Default GraderFeedback.
        """
        return GraderFeedback(
            status=GradeStatus.PASS,
            confidence=0.7,
            issues=[],
            suggestions=[],
        )
