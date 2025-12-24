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
        return {
            "question": shared["question"],
            "schema": shared["schema_str"],
            "entity_map": shared.get("entity_map", {}),
            "knowledge_hints": shared.get("knowledge_hints", {}),
            "aggregated_context": shared.get("aggregated_context", {}),
            "entity_ids": shared.get("entity_ids", {}),
        }

    def exec(self, prep_res):
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
        logger.error(f"Planner failed: {exc}")
        return "Plan generation failed. Please proceed with caution."

    def post(self, shared, prep_res, exec_res):
        shared["plan_steps"] = exec_res
        logger.info("Plan generated.")
        return "default"


class ContextAggregator(Node):
    """
    Collect insights from previous nodes and create enriched context for code generation.
    """

    def prep(self, shared):
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
        shared["aggregated_context"] = exec_res["context"]
        shared["context_summary"] = exec_res["summary"]
        logger.info(
            "Context aggregated: "
            f"{exec_res['context']['query_type']} query with "
            f"{len(exec_res['context']['recommended_tables'])} tables"
        )
        return "default"
