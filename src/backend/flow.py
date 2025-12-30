"""Flow definition for the NBA Data Analyst Agent."""

from __future__ import annotations

import logging
from typing import Any

from pocketflow import BatchFlow, Flow

from src.backend.nodes import (
    AskUser,
    ClarifyQuery,
    CombineResults,
    DataAnalyzer,
    QueryPlanner,
    QueryRewriter,
    ResponseGrader,
    SQLExecutor,
    SQLGenerator,
    TableSelector,
)


logger = logging.getLogger(__name__)


class SubQueryBatchFlow(BatchFlow):
    """Batch flow for executing decomposed sub-queries."""

    def prep(self, shared: dict[str, Any]) -> list[dict[str, Any]]:
        query_plan = shared.get("query_plan")
        if query_plan is None:
            return []

        if isinstance(query_plan, dict):
            sub_queries = query_plan.get("sub_queries", [])
        else:
            sub_queries = query_plan.sub_queries

        shared["sub_query_results"] = {}
        shared["sub_query_sqls"] = {}
        shared["sub_query_tables"] = {}
        shared["sub_query_errors"] = {}

        params_list: list[dict[str, Any]] = []
        for idx, sub_query in enumerate(sub_queries, 1):
            if isinstance(sub_query, dict):
                sub_id = sub_query.get("id") or f"sub_{idx}"
                description = sub_query.get("description") or ""
            else:
                sub_id = sub_query.id or f"sub_{idx}"
                description = sub_query.description or ""
            params_list.append(
                {
                    "sub_query_id": str(sub_id),
                    "sub_query_description": str(description),
                }
            )

        return params_list


def create_analyst_flow() -> Flow:
    """Create the NBA Data Analyst Flow aligned with design.md."""
    clarify = ClarifyQuery()
    ask_user = AskUser()
    rewriter = QueryRewriter()
    planner = QueryPlanner()

    selector = TableSelector()
    sql_gen = SQLGenerator()
    sql_exec = SQLExecutor()
    analyzer = DataAnalyzer()
    grader = ResponseGrader()
    combiner = CombineResults()

    sub_selector = TableSelector()
    sub_sql_gen = SQLGenerator()
    sub_sql_exec = SQLExecutor()

    sub_selector >> sub_sql_gen
    sub_sql_gen - "valid" >> sub_sql_exec
    sub_flow = SubQueryBatchFlow(start=sub_selector)

    clarify - "ambiguous" >> ask_user
    clarify - "clear" >> rewriter

    ask_user - "clarified" >> rewriter

    rewriter >> planner
    planner - "simple" >> selector
    planner - "complex" >> sub_flow

    selector >> sql_gen
    sql_gen - "valid" >> sql_exec
    sql_gen - "fallback" >> analyzer
    sql_exec - "success" >> analyzer
    sql_exec - "error" >> sql_gen

    sub_flow >> combiner >> analyzer

    analyzer - "default" >> grader
    grader - "fail" >> sql_gen
    grader - "fail_complex" >> sub_flow

    return Flow(start=clarify)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow = create_analyst_flow()
    logger.info("Flow created successfully: %s", flow)
