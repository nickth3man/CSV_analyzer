"""
Planning and context aggregation nodes.
"""

import json
import logging
from pocketflow import Node

logger = logging.getLogger(__name__)

from backend.config import MAX_PLAN_STEPS, MIN_PLAN_STEPS
from backend.utils.call_llm import call_llm
from backend.utils.knowledge_store import knowledge_store


class Planner(Node):
    """Generate an analysis plan that leverages both CSV and NBA API data."""

    def prep(self, shared):
        """
        Build a prepared payload extracted from the shared runtime state for plan generation.
        
        Parameters:
            shared (dict): Runtime shared state containing inputs and intermediate results. Expected keys used:
                - "question": user question string
                - "schema_str": database schema representation
                - "entity_map" (optional): mapping of entity names to their table/column locations
                - "knowledge_hints" (optional): hints or join patterns from the knowledge store
                - "aggregated_context" (optional): previously aggregated context for the query
                - "entity_ids" (optional): explicit identifiers for entities
        
        Returns:
            dict: A payload with the following keys:
                - "question": copied from shared["question"]
                - "schema": copied from shared["schema_str"]
                - "entity_map": shared.get("entity_map", {})
                - "knowledge_hints": shared.get("knowledge_hints", {})
                - "aggregated_context": shared.get("aggregated_context", {})
                - "entity_ids": shared.get("entity_ids", {})
        """
        return {
            "question": shared["question"],
            "schema": shared["schema_str"],
            "entity_map": shared.get("entity_map", {}),
            "knowledge_hints": shared.get("knowledge_hints", {}),
            "aggregated_context": shared.get("aggregated_context", {}),
            "entity_ids": shared.get("entity_ids", {}),
        }

    def exec(self, prep_res):
        """
        Generate a multi-step analysis plan that combines CSV and NBA API data based on the provided preparation results.
        
        Parameters:
            prep_res (dict): Prepared input dictionary containing:
                - question (str): The user's question.
                - schema (str): Unified database schema string.
                - entity_map (dict): Mapping of entity names to tables and columns where they were found.
                - knowledge_hints (dict): Hints such as join patterns from previous queries.
                - aggregated_context (dict, optional): Previously aggregated contextual data.
                - entity_ids (dict, optional): Official NBA IDs for entities to be used in API calls.
        
        Returns:
            plan (str): A detailed, step-by-step analysis plan covering CSV table queries and joins, NBA API endpoints and fields to call (using entity IDs), filters, cross-source aggregations and comparisons, cross-validation steps, discrepancy handling, and lineup optimization guidance.
        
        Raises:
            ValueError: If the language model returns an empty plan.
        """
        question = prep_res["question"]
        schema = prep_res["schema"]
        entity_map = prep_res["entity_map"]
        knowledge_hints = prep_res["knowledge_hints"]
        aggregated_context = prep_res.get("aggregated_context", {})
        entity_ids = prep_res.get("entity_ids", {})

        entity_info = ""
        if entity_map:
            entity_info = "\n\nENTITY LOCATIONS (where entities were found in the data):\n"
            for entity, tables in entity_map.items():
                if tables:
                    for table, cols in tables.items():
                        entity_info += f"  - '{entity}' found in table '{table}' columns: {cols}\n"

        hints_info = ""
        if knowledge_hints.get("join_patterns"):
            hints_info = "\n\nHINTS FROM PREVIOUS QUERIES (use as guidance):\n"
            for pattern in knowledge_hints["join_patterns"][:3]:
                hints_info += f"  - Tables {pattern['tables']} can be joined on {pattern['keys']}\n"

        prompt = f"""You are a data analyst. Given the unified schema, user question, entity locations, and official NBA IDs, create a comprehensive analysis plan that uses BOTH CSV and NBA API data.

DATABASE SCHEMA:
{schema}
{entity_info}
{hints_info}
ENTITY IDS (for nba_api calls): {json.dumps(entity_ids, indent=2)}
DATA CONTEXT: {json.dumps(aggregated_context, indent=2)}
USER QUESTION: <user_question>{question}</user_question>

Create a detailed step-by-step plan ({MIN_PLAN_STEPS}-{MAX_PLAN_STEPS} steps) to thoroughly answer the question. Include:
1. Which CSV tables to query and how to join them
2. Which NBA API endpoints to call (using entity IDs) and what fields to extract
3. What filters to apply for the specific entities
4. What aggregations or comparisons to perform across BOTH data sources
5. How to cross-validate results and highlight discrepancies
6. For lineup optimization, outline candidate selection, metrics, constraints, and selection steps

Be thorough - this is for deep analysis, not just a simple lookup."""

        plan = call_llm(prompt)
        if not plan:
            raise ValueError("LLM returned empty plan - retrying")
        return plan

    def exec_fallback(self, prep_res, exc):
        """
        Provide a fallback plan message when plan generation fails.
        
        Parameters:
            prep_res (dict): The prepared execution payload passed to the node.
            exc (Exception): The exception that caused the failure.
        
        Returns:
            str: A generic fallback message instructing caution while proceeding.
        """
        logger.error(f"Planner failed: {exc}")
        return "Plan generation failed. Please proceed with caution."

    def post(self, shared, prep_res, exec_res):
        """
        Persist the generated plan into the shared execution state and mark the node as complete.
        
        Parameters:
            shared (dict): Shared state dictionary used by nodes; the plan will be stored under the "plan_steps" key.
            exec_res: The plan produced by the node's execution, stored into shared["plan_steps"].
        
        Returns:
            str: The next node transition label, `"default"`.
        """
        shared["plan_steps"] = exec_res
        logger.info("Plan generated.")
        return "default"


class ContextAggregator(Node):
    """
    Collect insights from previous nodes and create enriched context for code generation.
    """

    def prep(self, shared):
        """
        Builds the execution payload for ContextAggregator by extracting required values from the shared execution state.
        
        Parameters:
        	shared (dict): Shared node state containing prior results and configuration. Expected keys (if present) include:
        		- "question": the user's original question
        		- "schema_str": database schema string
        		- "entity_map": mapping of entities to their table/column locations
        		- "entities": list of entity names
        		- "data_profile": table profiling information (e.g., row counts, id_columns)
        		- "cross_references": cross-reference metadata between sources
        		- "plan_steps": previously generated plan steps
        		- "data_sources": metadata about available data sources
        		- "entity_ids": explicit entity identifier mappings
        
        Returns:
        	dict: Payload with the following keys populated from `shared` (or defaults):
        		- "question": user question
        		- "schema": schema string
        		- "entity_map": entity location map (default {})
        		- "entities": list of entities (default [])
        		- "data_profile": profiling info (default {})
        		- "cross_references": cross-reference info (default {})
        		- "plan_steps": plan steps string (default "")
        		- "knowledge_hints": hints retrieved from the knowledge store
        		- "data_sources": data source metadata (default {})
        		- "entity_ids": entity identifier mappings (default {})
        """
        return {
            "question": shared["question"],
            "schema": shared["schema_str"],
            "entity_map": shared.get("entity_map", {}),
            "entities": shared.get("entities", []),
            "data_profile": shared.get("data_profile", {}),
            "cross_references": shared.get("cross_references", {}),
            "plan_steps": shared.get("plan_steps", ""),
            "knowledge_hints": knowledge_store.get_all_hints(),
            "data_sources": shared.get("data_sources", {}),
            "entity_ids": shared.get("entity_ids", {}),
        }

    def exec(self, prep_res):
        """
        Builds an aggregated context object and a human-readable summary from prepared inputs about entities and data profiles.
        
        Parameters:
            prep_res (dict): Prepared inputs containing:
                - "entities" (list): Entity names involved in the query.
                - "entity_map" (dict): Mapping from entity to tables and column info.
                - "data_profile" (dict): Table profiles (e.g., "id_columns", "row_count").
                - "cross_references" (optional dict): Cross-reference metadata.
                - "data_sources" (optional dict): Source descriptors for tables.
                - "entity_ids" (optional dict): Specific IDs for entities.
        
        Returns:
            dict: A dictionary with:
                - "context" (dict): Aggregated context including:
                    - "query_type": "comparison" if multiple entities, otherwise "lookup".
                    - "entities": the input entity list.
                    - "entity_locations": tables and primary table per entity.
                    - "recommended_tables": list of tables recommended for querying.
                    - "join_keys": inferred join key descriptors from profiles.
                    - "data_quality_notes": notes such as empty tables.
                    - Optional keys: "cross_references", "data_sources", "entity_ids" when present.
                - "summary" (str): A formatted textual summary of the aggregated context.
        """
        context = {
            "query_type": "comparison" if len(prep_res["entities"]) > 1 else "lookup",
            "entities": prep_res["entities"],
            "entity_locations": {},
            "recommended_tables": set(),
            "join_keys": [],
            "data_quality_notes": [],
        }

        for entity, tables in prep_res["entity_map"].items():
            context["entity_locations"][entity] = {
                "tables": list(tables.keys()),
                "primary_table": list(tables.keys())[0] if tables else None,
            }
            context["recommended_tables"].update(tables.keys())

        profile = prep_res["data_profile"]
        for table_name in context["recommended_tables"]:
            if table_name in profile:
                id_cols = profile[table_name].get("id_columns", [])
                if id_cols:
                    context["join_keys"].append(f"{table_name}: {id_cols}")

                row_count = profile[table_name].get("row_count", 0)
                if row_count == 0:
                    context["data_quality_notes"].append(f"{table_name} is empty")

        if prep_res["cross_references"]:
            context["cross_references"] = prep_res["cross_references"]
        if prep_res.get("data_sources"):
            context["data_sources"] = prep_res["data_sources"]
        if prep_res.get("entity_ids"):
            context["entity_ids"] = prep_res["entity_ids"]

        context["recommended_tables"] = list(context["recommended_tables"])

        context_summary = f"""
AGGREGATED CONTEXT:
- Query Type: {context['query_type']}
- Entities: {', '.join(context['entities'])}
- Recommended Tables: {', '.join(context['recommended_tables'])}
- Join Keys: {'; '.join(context['join_keys']) if context['join_keys'] else 'None identified'}
- Entity Locations: {json.dumps(context['entity_locations'], indent=2)}
"""
        if context.get("cross_references"):
            context_summary += f"- Cross-References: {json.dumps(context['cross_references'], indent=2)}\n"
        if context.get("data_sources"):
            context_summary += f"- Data Sources: {json.dumps(context['data_sources'], indent=2)}\n"
        if context.get("entity_ids"):
            context_summary += f"- Entity IDs: {json.dumps(context['entity_ids'], indent=2)}\n"
        if context["data_quality_notes"]:
            context_summary += f"- Data Notes: {'; '.join(context['data_quality_notes'])}\n"

        return {"context": context, "summary": context_summary}

    def post(self, shared, prep_res, exec_res):
        """
        Store the aggregated context and its textual summary into the shared state.
        
        Parameters:
            shared (dict): Mutable shared state for the node pipeline; will be updated with aggregated context and summary.
            prep_res (dict): Preparation output (unused by this method).
            exec_res (dict): Execution result containing:
                - context (dict): Aggregated context object; must include `query_type` and `recommended_tables`.
                - summary (str): Human-readable summary of the aggregated context.
        
        Returns:
            str: Execution token "default".
        """
        shared["aggregated_context"] = exec_res["context"]
        shared["context_summary"] = exec_res["summary"]
        logger.info(
            "Context aggregated: "
            f"{exec_res['context']['query_type']} query with "
            f"{len(exec_res['context']['recommended_tables'])} tables"
        )
        return "default"