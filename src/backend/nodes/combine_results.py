"""CombineResults node for the NBA Data Analyst Agent.

This module merges results from multiple sub-queries into unified context,
as specified in design.md Section 6.9.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from pocketflow import Node

from src.backend.models import QueryPlan, SubQuery
from src.backend.utils.logger import get_logger

logger = logging.getLogger(__name__)


class CombineResults(Node):
    """Merge results from multiple sub-queries into unified context.

    This node is used for complex queries that are decomposed by QueryPlanner.
    It combines partial results based on the combination_strategy specified
    in the query plan.
    """

    def prep(self, shared: dict[str, Any]) -> dict[str, Any]:
        """Read query plan and collect sub-query results.

        Args:
            shared: The shared store.

        Returns:
            Dictionary with query_plan and sub_query_results.
        """
        query_plan = shared.get("query_plan")
        sub_query_results = shared.get("sub_query_results", {})

        get_logger().log_node_start(
            "CombineResults",
            {
                "has_plan": query_plan is not None,
                "results_count": len(sub_query_results),
            },
        )

        return {
            "query_plan": query_plan,
            "sub_query_results": sub_query_results,
        }

    def exec(self, prep_res: dict[str, Any]) -> dict[str, Any]:
        """Combine sub-query results based on strategy.

        Args:
            prep_res: Dictionary with query_plan and sub_query_results.

        Returns:
            Dictionary with combined_result and metadata.
        """
        query_plan = prep_res["query_plan"]
        sub_query_results = prep_res["sub_query_results"]

        if query_plan is None:
            return {
                "combined_result": None,
                "combination_strategy": "none",
                "error": "No query plan available",
            }

        if isinstance(query_plan, dict):
            strategy = query_plan.get("combination_strategy", "synthesize")
            sub_queries = query_plan.get("sub_queries", [])
        else:
            strategy = query_plan.combination_strategy
            sub_queries = query_plan.sub_queries

        if strategy == "synthesize_comparison":
            combined = self._combine_comparison(sub_queries, sub_query_results)
        elif strategy == "chain":
            combined = self._combine_chain(sub_queries, sub_query_results)
        elif strategy == "merge":
            combined = self._combine_merge(sub_queries, sub_query_results)
        else:
            combined = self._combine_synthesize(sub_queries, sub_query_results)

        return {
            "combined_result": combined,
            "combination_strategy": strategy,
            "sub_query_count": len(sub_queries),
        }

    def post(
        self,
        shared: dict[str, Any],
        prep_res: dict[str, Any],
        exec_res: dict[str, Any],
    ) -> str:
        """Store combined results for DataAnalyzer.

        Args:
            shared: The shared store.
            prep_res: Result from prep().
            exec_res: Result from exec().

        Returns:
            Action string "default" to continue.
        """
        shared["combined_result"] = exec_res["combined_result"]
        shared["query_result"] = exec_res["combined_result"]

        get_logger().log_node_end(
            "CombineResults",
            {
                "strategy": exec_res["combination_strategy"],
                "sub_queries": exec_res.get("sub_query_count", 0),
            },
            "success",
        )

        logger.info(
            "Combined %d sub-query results using strategy '%s'",
            exec_res.get("sub_query_count", 0),
            exec_res["combination_strategy"],
        )

        return "default"

    def _combine_comparison(
        self,
        sub_queries: list[Any],
        results: dict[str, Any],
    ) -> pd.DataFrame | dict[str, Any]:
        """Combine results for comparison queries.

        Structures data side-by-side for easy comparison.

        Args:
            sub_queries: List of sub-query definitions.
            results: Dictionary of sub-query results.

        Returns:
            Combined comparison data.
        """
        comparison_data = {}

        for sq in sub_queries:
            sq_id = sq.id if hasattr(sq, "id") else sq.get("id", "")
            sq_desc = sq.description if hasattr(sq, "description") else sq.get("description", "")

            if sq_id in results:
                result = results[sq_id]

                if isinstance(result, pd.DataFrame):
                    comparison_data[sq_id] = {
                        "description": sq_desc,
                        "data": result.to_dict("records"),
                        "summary": self._summarize_dataframe(result),
                    }
                else:
                    comparison_data[sq_id] = {
                        "description": sq_desc,
                        "data": result,
                    }

        if len(comparison_data) == 2:
            try:
                keys = list(comparison_data.keys())
                df1_data = comparison_data[keys[0]].get("data", [])
                df2_data = comparison_data[keys[1]].get("data", [])

                if isinstance(df1_data, list) and isinstance(df2_data, list):
                    df1 = pd.DataFrame(df1_data)
                    df2 = pd.DataFrame(df2_data)

                    df1["_source"] = keys[0]
                    df2["_source"] = keys[1]

                    return pd.concat([df1, df2], ignore_index=True)

            except Exception as e:
                logger.warning("Failed to create comparison DataFrame: %s", e)

        return comparison_data

    def _combine_chain(
        self,
        sub_queries: list[Any],
        results: dict[str, Any],
    ) -> dict[str, Any]:
        """Combine results for sequential/chained queries.

        Chains results logically based on dependencies.

        Args:
            sub_queries: List of sub-query definitions.
            results: Dictionary of sub-query results.

        Returns:
            Combined chained data.
        """
        sorted_queries = self._topological_sort(sub_queries)

        chain = []
        for sq in sorted_queries:
            sq_id = sq.id if hasattr(sq, "id") else sq.get("id", "")
            sq_desc = sq.description if hasattr(sq, "description") else sq.get("description", "")

            if sq_id in results:
                result = results[sq_id]

                chain.append({
                    "step": sq_id,
                    "description": sq_desc,
                    "result": result.to_dict("records") if isinstance(result, pd.DataFrame) else result,
                })

        return {"chain": chain, "final_step": chain[-1] if chain else None}

    def _combine_merge(
        self,
        sub_queries: list[Any],
        results: dict[str, Any],
    ) -> pd.DataFrame | None:
        """Combine results by merging DataFrames.

        Args:
            sub_queries: List of sub-query definitions.
            results: Dictionary of sub-query results.

        Returns:
            Merged DataFrame or None.
        """
        dataframes = []

        for sq in sub_queries:
            sq_id = sq.id if hasattr(sq, "id") else sq.get("id", "")

            if sq_id in results:
                result = results[sq_id]
                if isinstance(result, pd.DataFrame):
                    dataframes.append(result)

        if not dataframes:
            return None

        if len(dataframes) == 1:
            return dataframes[0]

        merged = dataframes[0]
        for df in dataframes[1:]:
            common_cols = list(set(merged.columns) & set(df.columns))
            if common_cols:
                merged = pd.merge(merged, df, on=common_cols, how="outer")
            else:
                merged = pd.concat([merged, df], ignore_index=True)

        return merged

    def _combine_synthesize(
        self,
        sub_queries: list[Any],
        results: dict[str, Any],
    ) -> dict[str, Any]:
        """Default synthesis of all results.

        Args:
            sub_queries: List of sub-query definitions.
            results: Dictionary of sub-query results.

        Returns:
            Dictionary with all synthesized data.
        """
        synthesized = {"sub_queries": [], "all_data": []}

        for sq in sub_queries:
            sq_id = sq.id if hasattr(sq, "id") else sq.get("id", "")
            sq_desc = sq.description if hasattr(sq, "description") else sq.get("description", "")

            entry = {
                "id": sq_id,
                "description": sq_desc,
            }

            if sq_id in results:
                result = results[sq_id]
                if isinstance(result, pd.DataFrame):
                    entry["row_count"] = len(result)
                    entry["columns"] = list(result.columns)
                    entry["data"] = result.to_dict("records")
                else:
                    entry["data"] = result

            synthesized["sub_queries"].append(entry)

        return synthesized

    def _summarize_dataframe(self, df: pd.DataFrame) -> dict[str, Any]:
        """Create a summary of a DataFrame.

        Args:
            df: DataFrame to summarize.

        Returns:
            Summary dictionary.
        """
        summary = {
            "rows": len(df),
            "columns": list(df.columns),
        }

        numeric_cols = df.select_dtypes(include=["number"]).columns
        if len(numeric_cols) > 0:
            summary["numeric_stats"] = {}
            for col in numeric_cols[:5]:
                summary["numeric_stats"][col] = {
                    "mean": float(df[col].mean()) if not df[col].isna().all() else None,
                    "min": float(df[col].min()) if not df[col].isna().all() else None,
                    "max": float(df[col].max()) if not df[col].isna().all() else None,
                }

        return summary

    def _topological_sort(self, sub_queries: list[Any]) -> list[Any]:
        """Sort sub-queries by dependencies.

        Args:
            sub_queries: List of sub-query definitions.

        Returns:
            Topologically sorted list.
        """
        id_to_query = {}
        for sq in sub_queries:
            sq_id = sq.id if hasattr(sq, "id") else sq.get("id", "")
            id_to_query[sq_id] = sq

        visited = set()
        result = []

        def visit(sq_id: str) -> None:
            if sq_id in visited:
                return
            visited.add(sq_id)

            sq = id_to_query.get(sq_id)
            if sq:
                deps = sq.depends_on if hasattr(sq, "depends_on") else sq.get("depends_on", [])
                for dep in deps:
                    visit(dep)
                result.append(sq)

        for sq_id in id_to_query:
            visit(sq_id)

        return result
