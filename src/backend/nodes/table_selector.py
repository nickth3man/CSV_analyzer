"""TableSelector node for the NBA Data Analyst Agent.

This module identifies relevant tables using hybrid search (embeddings + LLM),
as specified in design.md Section 6.4.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import yaml
from pocketflow import Node

from src.backend.models import TableMeta
from src.backend.utils.call_llm import call_llm
from src.backend.utils.duckdb_client import get_duckdb_client
from src.backend.utils.embeddings import embed_text, find_similar
from src.backend.utils.logger import get_logger

logger = logging.getLogger(__name__)


TABLE_SELECTION_PROMPT = """Given a user question about NBA data, select the most relevant tables.

Question: {rewritten_query}

Candidate tables (pre-filtered by relevance):
{candidate_tables_with_descriptions}

Select 3-5 tables that would be needed to answer this question.
Consider:
- Which tables contain the metrics mentioned?
- Which tables would need to be JOINed?
- Are there lookup/dimension tables needed?

Output as YAML:
```yaml
selected_tables:
  - table_name: <name>
    reason: <why needed>
  - table_name: <name>
    reason: <why needed>
```
"""


class TableSelector(Node):
    """Identify relevant tables using hybrid search.

    Uses a two-stage approach:
    1. Embedding pre-filter: Find top-10 tables by semantic similarity
    2. LLM selection: From candidates, select 3-5 most relevant tables
    3. Schema fetch: Get DDL for selected tables
    """

    def __init__(self, top_k_candidates: int = 10, max_selected: int = 5) -> None:
        """Initialize the table selector.

        Args:
            top_k_candidates: Number of tables to pre-filter with embeddings.
            max_selected: Maximum tables to select for schema.
        """
        super().__init__()
        self.top_k_candidates = top_k_candidates
        self.max_selected = max_selected

    def prep(self, shared: dict[str, Any]) -> dict[str, Any]:
        """Read query and fetch available tables from database.

        Args:
            shared: The shared store.

        Returns:
            Dictionary with rewritten_query and table metadata.
        """
        sub_query_description = self.params.get("sub_query_description")
        rewritten_query = sub_query_description or shared.get(
            "rewritten_query", shared.get("question", "")
        )

        db_client = get_duckdb_client()
        available_tables = shared.get("available_tables")
        if available_tables is None:
            available_tables = db_client.get_all_tables()
            shared["available_tables"] = available_tables

        table_embeddings = shared.get("table_embeddings")
        if table_embeddings is None:
            table_embeddings = self._compute_table_embeddings(available_tables)
            shared["table_embeddings"] = table_embeddings

        get_logger().log_node_start(
            "TableSelector",
            {
                "query": rewritten_query,
                "available_tables": len(available_tables),
            },
        )

        return {
            "rewritten_query": rewritten_query,
            "available_tables": available_tables,
            "table_embeddings": table_embeddings,
        }

    def exec(self, prep_res: dict[str, Any]) -> dict[str, Any]:
        """Execute hybrid table selection.

        Args:
            prep_res: Dictionary with query and table data.

        Returns:
            Dictionary with candidates, selected tables, and schemas.
        """
        rewritten_query = prep_res["rewritten_query"]
        available_tables = prep_res["available_tables"]
        table_embeddings = prep_res["table_embeddings"]

        query_embedding = embed_text(rewritten_query)
        candidate_names = find_similar(
            query_embedding,
            table_embeddings,
            top_k=self.top_k_candidates,
        )

        candidates = [t for t in available_tables if t.name in candidate_names]

        candidates.sort(key=lambda t: candidate_names.index(t.name))

        selected_names = self._llm_select_tables(rewritten_query, candidates)

        if not selected_names:
            selected_names = candidate_names[: self.max_selected]

        db_client = get_duckdb_client()
        table_schemas = db_client.get_table_schema(selected_names)

        return {
            "candidate_tables": candidate_names,
            "selected_tables": selected_names,
            "table_schemas": table_schemas,
        }

    def post(
        self,
        shared: dict[str, Any],
        prep_res: dict[str, Any],
        exec_res: dict[str, Any],
    ) -> str:
        """Store table selection results in shared store.

        Args:
            shared: The shared store.
            prep_res: Result from prep().
            exec_res: Result from exec().

        Returns:
            Action string "default" to continue.
        """
        shared["candidate_tables"] = exec_res["candidate_tables"]
        shared["selected_tables"] = exec_res["selected_tables"]
        shared["table_schemas"] = exec_res["table_schemas"]

        sub_query_id = self.params.get("sub_query_id")
        if sub_query_id:
            shared.setdefault("sub_query_tables", {})[sub_query_id] = exec_res[
                "selected_tables"
            ]

        get_logger().log_node_end(
            "TableSelector",
            {
                "candidates": len(exec_res["candidate_tables"]),
                "selected": exec_res["selected_tables"],
            },
            "success",
        )

        logger.info(
            "Selected tables: %s (from %d candidates)",
            exec_res["selected_tables"],
            len(exec_res["candidate_tables"]),
        )

        return "default"

    def _compute_table_embeddings(
        self, tables: list[TableMeta]
    ) -> dict[str, list[float]]:
        """Compute embeddings for all tables.

        Args:
            tables: List of table metadata.

        Returns:
            Dictionary mapping table names to embedding vectors.
        """
        embeddings = {}

        for table in tables:
            text = f"{table.name}: {table.description}"
            if table.columns:
                cols = ", ".join(table.columns[:10])
                text += f" (columns: {cols})"

            embeddings[table.name] = embed_text(text)

        return embeddings

    def _llm_select_tables(
        self, query: str, candidates: list[TableMeta]
    ) -> list[str]:
        """Use LLM to select most relevant tables from candidates.

        Args:
            query: The rewritten user query.
            candidates: Pre-filtered candidate tables.

        Returns:
            List of selected table names.
        """
        if not candidates:
            return []

        candidate_descriptions = []
        for table in candidates:
            cols = ", ".join(table.columns[:8]) if table.columns else "unknown"
            row_info = f"({table.row_count:,} rows)" if table.row_count else ""
            candidate_descriptions.append(
                f"- {table.name} {row_info}: {table.description}\n  Columns: {cols}"
            )

        prompt = TABLE_SELECTION_PROMPT.format(
            rewritten_query=query,
            candidate_tables_with_descriptions="\n".join(candidate_descriptions),
        )

        response = call_llm(prompt)
        return self._parse_selection_response(response, candidates)

    def _parse_selection_response(
        self, response: str, candidates: list[TableMeta]
    ) -> list[str]:
        """Parse LLM response to extract selected table names.

        Args:
            response: Raw LLM response.
            candidates: List of valid candidate tables.

        Returns:
            List of selected table names.
        """
        try:
            yaml_match = re.search(r"```yaml\s*(.*?)\s*```", response, re.DOTALL)
            if yaml_match:
                yaml_str = yaml_match.group(1)
            else:
                yaml_str = response.strip()

            result = yaml.safe_load(yaml_str)

            if not isinstance(result, dict):
                return []

            selected = result.get("selected_tables", [])
            if not isinstance(selected, list):
                return []

            valid_names = {t.name for t in candidates}
            selected_names = []

            for item in selected:
                if isinstance(item, dict):
                    table_name = item.get("table_name", "")
                elif isinstance(item, str):
                    table_name = item
                else:
                    continue

                if table_name in valid_names:
                    selected_names.append(table_name)

            return selected_names[: self.max_selected]

        except yaml.YAMLError as e:
            logger.warning("Failed to parse YAML response: %s", e)
            return []
