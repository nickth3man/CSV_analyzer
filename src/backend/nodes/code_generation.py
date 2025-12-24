"""
Code generation nodes for CSV analysis and NBA API interactions.
"""

import json
import logging
from pocketflow import Node

logger = logging.getLogger(__name__)

from backend.utils.call_llm import call_llm


class CodeGenerator(Node):
    """Generate Python code to analyze CSV data based on the planned steps."""

    DYNAMIC_GUIDANCE = """
CRITICAL CODE GENERATION RULES:
1. ALWAYS convert values to str() before comparing or storing to avoid type errors
2. DO NOT try to convert string values like 'Y', 'N', flags to int
3. Query each table SEPARATELY - avoid complex multi-table merges
4. Use .astype(str) on columns before string matching
5. When extracting row values, wrap with str(row.get('col', 'N/A')) to prevent type issues
6. For comparisons: loop through each entity, query each table separately, combine results

DYNAMIC QUERY PATTERN (adapt to actual tables in schema):
```python
final_result = {}
entities_to_compare = ['Entity1', 'Entity2']  # from user question

for entity_name in entities_to_compare:
    entity_data = {'name': entity_name, 'found_in_tables': []}
    parts = entity_name.lower().split()
    
    # For each table where entity was found (from ENTITY LOCATIONS above):
    for table_name in dfs.keys():
        df = dfs[table_name]
        # Check if table has name columns and search
        name_cols = [c for c in df.columns if 'name' in c.lower() or 'first' in c.lower() or 'last' in c.lower()]
        if name_cols:
            # Build dynamic mask based on available columns
            # Extract relevant data from matches
            pass
    
    final_result[entity_name] = entity_data
```

IMPORTANT: Use the ENTITY LOCATIONS section to know exactly which tables contain each entity.
Only query tables listed there - don't assume tables exist.
"""

    def prep(self, shared):
        return {
            "plan": shared["plan_steps"],
            "schema": shared["schema_str"],
            "question": shared.get("question", ""),
            "entity_map": shared.get("entity_map", {}),
            "entities": shared.get("entities", []),
            "error": shared.get("exec_error"),
            "previous_code": shared.get("csv_code_snippet"),
            "context_summary": shared.get("context_summary", ""),
            "cross_references": shared.get("cross_references", {}),
            "data_sources": shared.get("data_sources", {}),
            "aggregated_context": shared.get("aggregated_context", {}),
        }

    def exec(self, prep_res):
        entity_info = ""
        if prep_res.get("entity_map"):
            entity_info = "\n\nENTITY LOCATIONS:\n"
            for entity, tables in prep_res["entity_map"].items():
                for table, cols in tables.items():
                    entity_info += f"  - '{entity}' is in table '{table}', columns: {cols}\n"

        context_summary = prep_res.get("context_summary", "")
        cross_refs = prep_res.get("cross_references", {})
        cross_ref_info = ""
        if cross_refs:
            cross_ref_info = "\n\nCROSS-REFERENCES (entity IDs found):\n"
            for entity, refs in cross_refs.items():
                for ref_key, ref_val in refs.items():
                    cross_ref_info += f"  - {entity}: {ref_key} = {ref_val}\n"

        is_comparison = len(prep_res.get("entities", [])) > 1
        comparison_hint = ""
        if is_comparison:
            entities = prep_res["entities"]
            comparison_hint = f"""
COMPARISON QUERY: You are comparing these entities: {entities}
For each entity, query ONLY the tables listed in ENTITY LOCATIONS above.
Loop through each entity, gather data from their respective tables, then combine into final_result.
"""

        if prep_res.get("error"):
            logger.info("Fixing code based on error...")
            error_fix_hint = ""
            error = prep_res["error"]
            if "merge" in error.lower() or "key" in error.lower() or "dtype" in error.lower():
                error_fix_hint = """
FIX APPROACH: The error is likely due to incompatible dtypes or merge keys.
- AVOID complex multi-table merges. Instead, query tables separately.
- If you must merge, convert keys to same dtype first: df['key'] = df['key'].astype(str)
- Loop through entities and gather data from each table separately.
"""

            prompt = f"""You are a Python data analyst. Your previous code had an error. Fix it.
{self.DYNAMIC_GUIDANCE}
{error_fix_hint}
DATABASE SCHEMA (available as dfs dictionary):
{prep_res['schema']}
{entity_info}
{cross_ref_info}
{context_summary}
{comparison_hint}
USER QUESTION: <user_question>{prep_res['question']}</user_question>

PREVIOUS CODE:
{prep_res.get('previous_code', 'None')}

ERROR: {prep_res['error']}

Write ONLY the corrected Python code. AVOID complex merges - query tables separately instead.
The dataframes are in a dict called 'dfs' where keys are table names.
Store your final answer in a variable called 'final_result' (a dictionary).
Do NOT include markdown code blocks. Just raw Python code."""
        else:
            prompt = f"""You are a Python data analyst. Write comprehensive code to answer the user's question using CSV data.
{self.DYNAMIC_GUIDANCE}
DATABASE SCHEMA (available as dfs dictionary):
{prep_res['schema']}
{entity_info}
{cross_ref_info}
{context_summary}
{comparison_hint}
USER QUESTION: <user_question>{prep_res['question']}</user_question>

PLAN: {prep_res['plan']}

Write Python code to thoroughly analyze and answer the question. 
- The dataframes are in a dict called 'dfs' where keys are table names
- Use the ENTITY LOCATIONS above to find the right tables and columns for filtering
- Use CROSS-REFERENCES if available to find related data by ID
- ONLY query tables that are mentioned in ENTITY LOCATIONS - don't assume tables exist
- AVOID complex multi-table merges - query tables separately and combine in Python
- Store your final answer in a variable called 'final_result' (a dictionary)
- Make final_result a dictionary with all relevant data for deep analysis
- Do NOT include markdown code blocks. Just raw Python code.
- Only use pandas (pd) which is already imported."""

        code = call_llm(prompt)
        code = (code or "").replace("```python", "").replace("```", "").strip()
        if not code:
            raise ValueError("LLM returned empty code - retrying")
        return code

    def exec_fallback(self, prep_res, exc):
        logger.error(f"CodeGenerator failed: {exc}")
        return "print('Code generation failed due to LLM error.')\nfinal_result = {}"

    def post(self, shared, prep_res, exec_res):
        shared["csv_code_snippet"] = exec_res
        return "default"


class NBAApiCodeGenerator(Node):
    """Generate nba_api specific code leveraging the shared nba_client helper."""

    def prep(self, shared):
        return {
            "plan": shared["plan_steps"],
            "api_schema": shared.get("api_schema_str", ""),
            "question": shared.get("question", ""),
            "entity_ids": shared.get("entity_ids", {}),
            "aggregated_context": shared.get("aggregated_context", {}),
            "previous_code": shared.get("api_code_snippet"),
            "error": shared.get("exec_error"),
        }

    def exec(self, prep_res):
        context = json.dumps(prep_res.get("aggregated_context", {}), indent=2)
        entity_ids = json.dumps(prep_res.get("entity_ids", {}), indent=2)
        plan = prep_res.get("plan", "")
        error = prep_res.get("error")

        base_prompt = f"""You are a Python engineer generating code that uses nba_api via the helper `nba_client` from backend.utils.nba_api_client.
Available helper methods: get_player_career_stats, get_player_game_log, get_team_game_log, get_league_leaders, get_common_team_roster, get_scoreboard.
Use pandas as pd. Do NOT import os/sys/subprocess/requests.
Schema (API data already loaded): {prep_res.get('api_schema', '')}
Entity IDs (use these for API calls): {entity_ids}
Context: {context}
PLAN: {plan}
USER QUESTION: <user_question>{prep_res['question']}</user_question>
Requirements:
- Store results in variable `api_result`
- Prefer cached helper methods instead of direct HTTP
- Include source attribution (e.g., api_result['source'] = 'api')
- Keep code defensive: handle missing IDs gracefully
- Do NOT wrap code in markdown fences.
"""

        if error:
            prompt = base_prompt + f"\nPrevious error: {error}\nRewrite the code fixing the issue. Provide only raw Python."
        else:
            prompt = base_prompt + "\nGenerate robust Python code now. Provide only raw Python."

        code = call_llm(prompt)
        code = (code or "").replace("```python", "").replace("```", "").strip()
        if not code:
            raise ValueError("LLM returned empty API code - retrying")
        return code

    def exec_fallback(self, prep_res, exc):
        logger.error(f"NBAApiCodeGenerator failed: {exc}")
        return "api_result = {}"

    def post(self, shared, prep_res, exec_res):
        shared["api_code_snippet"] = exec_res
        return "default"
