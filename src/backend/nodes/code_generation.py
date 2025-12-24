"""Code generation nodes for CSV analysis and NBA API interactions."""

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
        """
        Assembles and normalizes required inputs from the shared execution context for the CSV code generator.

        Parameters:
            shared (dict): Shared state containing execution context and interim results produced by previous nodes. Expected keys used below may be absent; defaults are applied where appropriate.

        Returns:
            dict: Prepared inputs with the following keys:
                plan: The stepwise plan from shared["plan_steps"].
                schema: String representation of the database/table schema from shared["schema_str"].
                question: User question or prompt from shared["question"] or empty string.
                entity_map: Mapping of entity names to their locations/columns from shared["entity_map"] or empty dict.
                entities: List of entity names from shared["entities"] or empty list.
                error: Execution error message from shared["exec_error"] or None.
                previous_code: Previously generated CSV code snippet from shared["csv_code_snippet"] or None.
                context_summary: Aggregated textual context from shared["context_summary"] or empty string.
                cross_references: Cross-reference details between entities/tables from shared["cross_references"] or empty dict.
                data_sources: Metadata about available data sources from shared["data_sources"] or empty dict.
                aggregated_context: Additional contextual information from shared["aggregated_context"] or empty dict.
        """
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
        """
        Generate Python analysis code by composing a prompt from prepared context and calling the LLM.

        Parameters:
            prep_res (dict): Prepared context from prep(), expected keys include:
                - "schema" (str): database schema string.
                - "question" (str): user question.
                - "plan" (str): plan steps for analysis.
                - "entity_map" (dict, optional): mapping of entities to tables and columns.
                - "entities" (list, optional): list of entities (used to detect comparisons).
                - "cross_references" (dict, optional): entity ID cross-reference information.
                - "context_summary" (str, optional): aggregated contextual text.
                - "previous_code" (str, optional): prior generated code to be fixed.
                - "error" (str, optional): previous execution error used to request fixes.

        Returns:
            str: Raw Python code produced by the LLM (cleaned of markdown fences) that assigns the analysis result to a dictionary named `final_result`.

        Raises:
            ValueError: If the LLM returns no code (empty string).
        """
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
The DataFrames are in a dict called 'dfs' where keys are table names.
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
- The DataFrames are in a dict called 'dfs' where keys are table names
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

    def exec_fallback(self, prep_res, exc) -> str:
        """
        Provide a minimal fallback Python snippet when code generation fails.

        Parameters:
            prep_res (dict): Prepared inputs that were passed to the generator (unused by this fallback).
            exc (Exception): The exception raised by the failed code generation attempt.

        Returns:
            str: A Python code snippet that prints a failure message and sets `final_result` to an empty dictionary.
        """
        logger.error(f"CodeGenerator failed: {exc}")
        return "print('Code generation failed due to LLM error.')\nfinal_result = {}"

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Store the generated CSV code snippet in the shared execution context and advance the node flow.

        Parameters:
            exec_res (str): Generated Python code (or snippet) produced by the node.

        Returns:
            str: The next state key, `"default"`.
        """
        shared["csv_code_snippet"] = exec_res
        return "default"


class NBAApiCodeGenerator(Node):
    """Generate nba_api specific code leveraging the shared nba_client helper."""

    def prep(self, shared):
        """
        Assembles and returns the subset of shared runtime state required to generate NBA API code.

        Parameters:
            shared (dict): Shared execution context containing pipeline state and artifacts.

        Returns:
            dict: A mapping with keys:
                plan: pipeline plan steps (from shared["plan_steps"])
                api_schema: API schema string (from shared.get("api_schema_str", ""))
                question: user question or prompt (from shared.get("question", ""))
                entity_ids: identifiers for target entities (from shared.get("entity_ids", {}))
                aggregated_context: additional contextual data (from shared.get("aggregated_context", {}))
                previous_code: previously generated API code snippet (from shared.get("api_code_snippet"))
                error: execution error info if present (from shared.get("exec_error"))
        """
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
        """
        Generate Python code that calls the NBA API helper according to the prepared plan, context, and entity IDs.

        Parameters:
            prep_res (dict): Prepared inputs from prep(), expected keys:
                - aggregated_context (dict): contextual information to include in the prompt.
                - entity_ids (dict): IDs to use for API calls.
                - plan (str): plan or steps guiding the code generation.
                - api_schema (str): string description of available API schema.
                - question (str): the user's question to answer.
                - error (optional): previous execution error to include for regenerating fixed code.

        Returns:
            str: Raw Python code (no markdown fences) that assigns results to `api_result`.

        Raises:
            ValueError: If the language model returns an empty code string.
        """
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

    def exec_fallback(self, prep_res, exc) -> str:
        """
        Provide a minimal fallback Python snippet when NBA API code generation fails.

        Parameters:
            prep_res (dict): Prepared input that was passed to exec; used for context in logging or debugging.
            exc (Exception): The exception raised during code generation.

        Returns:
            str: A Python code string that sets `api_result` to an empty dictionary.
        """
        logger.error(f"NBAApiCodeGenerator failed: {exc}")
        return "api_result = {}"

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Persist the generated API code snippet in the shared workflow state and signal the default next step.

        Parameters:
            shared (dict): Shared workflow/state dictionary used across nodes; the code snippet will be stored here under the "api_code_snippet" key.
            prep_res: Preparation result from the node's prep step (unused by this method).
            exec_res (str): Generated Python code produced by exec to be persisted.

        Returns:
            str: The next state identifier, `"default"`.
        """
        shared["api_code_snippet"] = exec_res
        return "default"
