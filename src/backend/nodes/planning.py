"""Query planning node for the NBA Data Analyst Agent.

This module decides whether a query is simple or needs decomposition,
as specified in design.md Section 6.3.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import yaml
from pocketflow import Node

from src.backend.models import QueryComplexity, QueryPlan, SubQuery
from src.backend.utils.call_llm import call_llm
from src.backend.utils.logger import get_logger


logger = logging.getLogger(__name__)


PLANNER_PROMPT = """You are an NBA data analyst planning how to answer a user's question.

Determine if the question is SIMPLE (single SQL query) or COMPLEX (needs multiple sub-queries).

Complexity indicators:
- Multiple distinct aggregations across different dimensions
- Explicit comparisons ("compare X and Y")
- Sequential analysis ("first find X, then calculate Y")
- Conditional logic ("if X then show Y")

Question: {rewritten_query}

Output as YAML:
```yaml
complexity: simple | complex
combination_strategy: synthesize | synthesize_comparison | chain | merge
sub_queries:
  - id: <short_id>
    description: <what this sub-query should retrieve>
    depends_on: [<ids this depends on>]
```

If SIMPLE, set sub_queries to [].
"""


class QueryPlanner(Node):
    """Decide if the query needs decomposition into sub-queries."""

    def prep(self, shared: dict[str, Any]) -> dict[str, Any]:
        """Read rewritten query from shared store."""
        rewritten_query = shared.get("rewritten_query", shared.get("question", ""))

        get_logger().log_node_start(
            "QueryPlanner",
            {"query": rewritten_query},
        )

        return {"rewritten_query": rewritten_query}

    def exec(self, prep_res: dict[str, Any]) -> dict[str, Any]:
        """Analyze query complexity and produce a plan."""
        rewritten_query = prep_res["rewritten_query"]

        prompt = PLANNER_PROMPT.format(rewritten_query=rewritten_query)
        response = call_llm(prompt)

        return self._parse_plan(response, rewritten_query)

    def post(
        self,
        shared: dict[str, Any],
        prep_res: dict[str, Any],
        exec_res: dict[str, Any],
    ) -> str:
        """Store query plan and return action."""
        query_plan = exec_res["query_plan"]
        shared["query_plan"] = query_plan

        if query_plan.complexity == QueryComplexity.COMPLEX:
            shared.setdefault("sub_query_results", {})
            shared.setdefault("sub_query_sqls", {})
            shared.setdefault("sub_query_tables", {})
            shared.setdefault("sub_query_errors", {})

        get_logger().log_node_end(
            "QueryPlanner",
            {
                "complexity": query_plan.complexity.value,
                "sub_queries": len(query_plan.sub_queries),
                "strategy": query_plan.combination_strategy,
            },
            "success",
        )

        return query_plan.complexity.value

    def _parse_plan(self, response: str, query: str) -> dict[str, Any]:
        """Parse YAML response into a QueryPlan."""
        parsed = None
        try:
            match = re.search(r"```yaml\s*(.*?)\s*```", response, re.DOTALL)
            yaml_str = match.group(1) if match else response.strip()
            parsed = yaml.safe_load(yaml_str)
        except yaml.YAMLError as exc:
            logger.warning("Failed to parse planner YAML: %s", exc)

        if not isinstance(parsed, dict):
            return {"query_plan": self._default_plan(query, QueryComplexity.SIMPLE)}

        complexity_str = str(parsed.get("complexity", "simple")).lower()
        complexity = (
            QueryComplexity.COMPLEX
            if complexity_str == "complex"
            else QueryComplexity.SIMPLE
        )

        combination_strategy = parsed.get("combination_strategy") or self._infer_strategy(
            query, complexity
        )

        sub_queries_data = parsed.get("sub_queries", [])
        sub_queries: list[SubQuery] = []

        if isinstance(sub_queries_data, list):
            for idx, item in enumerate(sub_queries_data, 1):
                if not isinstance(item, dict):
                    continue
                sub_id = item.get("id") or f"sub_{idx}"
                description = item.get("description") or ""
                depends_on = item.get("depends_on") or []
                if not isinstance(depends_on, list):
                    depends_on = [str(depends_on)]
                if description:
                    sub_queries.append(
                        SubQuery(
                            id=str(sub_id),
                            description=str(description),
                            depends_on=[str(dep) for dep in depends_on],
                        )
                    )

        if complexity == QueryComplexity.COMPLEX and not sub_queries:
            complexity = QueryComplexity.SIMPLE
            combination_strategy = "synthesize"

        query_plan = QueryPlan(
            complexity=complexity,
            sub_queries=sub_queries,
            combination_strategy=str(combination_strategy),
        )

        return {"query_plan": query_plan}

    def _infer_strategy(self, query: str, complexity: QueryComplexity) -> str:
        """Infer a reasonable combination strategy if missing."""
        if complexity == QueryComplexity.SIMPLE:
            return "synthesize"

        query_lower = query.lower()
        if any(term in query_lower for term in ["compare", "versus", "vs"]):
            return "synthesize_comparison"
        if any(term in query_lower for term in ["then", "after", "followed by"]):
            return "chain"
        return "synthesize"

    def _default_plan(self, query: str, complexity: QueryComplexity) -> QueryPlan:
        """Build a fallback plan when parsing fails."""
        return QueryPlan(
            complexity=complexity,
            sub_queries=[],
            combination_strategy=self._infer_strategy(query, complexity),
        )
