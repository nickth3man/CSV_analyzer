import os
import ast
import json
import time
import pandas as pd
import matplotlib
from pocketflow import Node
from utils.call_llm import call_llm
from utils.knowledge_store import knowledge_store
from utils.nba_api_client import nba_client
from utils.data_source_manager import data_source_manager

class LoadData(Node):
    def prep(self, shared):
        # Scan the CSV directory
        return shared.get("data_dir", "CSV")

    def exec(self, prep_res):
        # Read all CSV files from the CSV folder
        csv_dir = prep_res
        data = {}
        if os.path.exists(csv_dir):
            for filename in os.listdir(csv_dir):
                # TODO: Support Excel (.xlsx/.xls) and JSON files here by adding loaders and normalizing them into dataframes.
                # TODO: Add database ingestion (connection config + query) and register those tables alongside file-based data.
                # TODO: Expand file handling to detect new extensions and route them through format-specific loaders.
                if filename.endswith(".csv"):
                    filepath = os.path.join(csv_dir, filename)
                    try:
                        table_name = filename.replace(".csv", "")
                        try:
                            data[table_name] = pd.read_csv(filepath, encoding="utf-8")
                        except UnicodeDecodeError:
                            data[table_name] = pd.read_csv(filepath, encoding="latin-1")
                    except (pd.errors.ParserError, UnicodeDecodeError) as e:
                        print(f"Error parsing CSV file {filename}: {e}")
                    except FileNotFoundError as e:
                        print(f"File not found {filename}: {e}")
                    except Exception as e:
                        print(f"Unexpected error loading {filename}: {e}")
        return data

    def post(self, shared, prep_res, exec_res):
        shared["csv_dfs"] = exec_res
        shared["dfs"] = exec_res  # seed with CSV before merging
        shared["data_sources"] = {name: "csv" for name in exec_res}
        print(f"Loaded {len(exec_res)} dataframes from CSV.")
        if not exec_res:
            shared["final_text"] = (
                "No CSV files found in the CSV/ directory. "
                "Please upload data before asking a question."
            )
            return "no_data"
        return "default"

class NBAApiDataLoader(Node):
    """
    Fetches relevant data from the NBA API based on the question context.
    Uses static endpoints for ID resolution and dynamic endpoints for stats.
    """

    DEFAULT_SEASON = os.environ.get("NBA_API_DEFAULT_SEASON", "2023-24")

    def prep(self, shared):
        return {
            "question": shared.get("question", ""),
            "entities": shared.get("entities") or data_source_manager.detect_query_entities(shared.get("question", "")),
        }

    def _resolve_ids(self, entities):
        entity_ids = {}
        for entity in entities:
            player = nba_client.find_player(entity)
            if player:
                entity_ids[entity] = {"player_id": player.get("id")}
                continue
            team = nba_client.find_team(entity)
            if team:
                entity_ids[entity] = {"team_id": team.get("id")}
        return entity_ids

    def exec(self, prep_res):
        question = prep_res["question"]
        entities = prep_res["entities"]
        entity_ids = self._resolve_ids(entities)

        endpoints_to_call = data_source_manager.determine_api_endpoints(entities, question)
        api_dfs = {}
        errors = []
        used = []

        for endpoint in endpoints_to_call:
            name = endpoint["name"]
            params = endpoint.get("params", {})
            used.append({"name": name, "params": params})
            try:
                if name == "player_career":
                    ent = params.get("entity")
                    player_id = entity_ids.get(ent, {}).get("player_id")
                    if not player_id:
                        continue
                    career = nba_client.get_player_career_stats(player_id)
                    for key, df in career.items():
                        api_dfs[f"{ent}_career_{key}"] = df
                elif name == "league_leaders":
                    season = self.DEFAULT_SEASON
                    leaders = nba_client.get_league_leaders(season=season, stat_category="PTS")
                    api_dfs[f"league_leaders_{season}"] = leaders
                elif name == "common_team_roster":
                    ent = params.get("entity")
                    team_id = entity_ids.get(ent, {}).get("team_id")
                    if not team_id:
                        continue
                    roster = nba_client.get_common_team_roster(team_id=team_id, season=self.DEFAULT_SEASON)
                    api_dfs[f"{ent}_roster"] = roster
                elif name == "player_game_log":
                    ent = params.get("entity")
                    player_id = entity_ids.get(ent, {}).get("player_id")
                    if not player_id:
                        continue
                    game_log = nba_client.get_player_game_log(player_id=player_id, season=self.DEFAULT_SEASON)
                    api_dfs[f"{ent}_game_log"] = game_log
                elif name == "scoreboard":
                    api_dfs["live_scoreboard"] = nba_client.get_scoreboard()
                else:
                    errors.append({"endpoint": name, "error": "Unknown endpoint"})
            except Exception as exc:  # noqa: BLE001
                errors.append({"endpoint": name, "error": str(exc)})

        return {"api_dfs": api_dfs, "errors": errors, "used": used, "entity_ids": entity_ids}

    def post(self, shared, prep_res, exec_res):
        shared["api_dfs"] = exec_res["api_dfs"]
        shared["api_errors"] = exec_res["errors"]
        shared["api_endpoints_used"] = exec_res["used"]
        # Merge entity IDs discovered here with future resolution
        shared["entity_ids"] = exec_res.get("entity_ids", {})
        print(f"NBA API loader fetched {len(exec_res['api_dfs'])} tables with {len(exec_res['errors'])} errors.")
        return "default"

class DataMerger(Node):
    """
    Combines CSV and API dataframes with source tracking and discrepancy flags.
    """

    def prep(self, shared):
        return {
            "csv_dfs": shared.get("csv_dfs", {}),
            "api_dfs": shared.get("api_dfs", {}),
        }

    def exec(self, prep_res):
        merged, discrepancies, sources = data_source_manager.merge_data_sources(
            prep_res["csv_dfs"], prep_res["api_dfs"]
        )
        return merged, discrepancies, sources

    def post(self, shared, prep_res, exec_res):
        merged, discrepancies, sources = exec_res
        shared["dfs"] = merged
        shared["discrepancies"] = discrepancies
        shared["data_sources"] = sources
        print(f"Data merged: {len(merged)} tables ({len(discrepancies)} discrepancies flagged).")
        return "default"

class SchemaInference(Node):
    def prep(self, shared):
        return shared["dfs"]

    def exec(self, prep_res):
        dfs = prep_res
        schemas = {}
        csv_schema = {}
        api_schema = {}
        # TODO: Ensure schema inference supports dataframes originating from Excel/JSON/DB sources
        # by preserving source metadata (e.g., sheet names, JSON paths, DB schemas).
        for name, df in dfs.items():
            schemas[name] = list(df.columns)
        for name, df in (dfs.items()):
            source = None
            if name in dfs and "_source" in df.columns:
                source = df["_source"].iloc[0] if not df.empty else None
            if source == "api" and name not in csv_schema:
                api_schema[name] = list(df.columns)
            elif source == "csv" and name not in api_schema:
                csv_schema[name] = list(df.columns)
        return schemas, csv_schema, api_schema

    def post(self, shared, prep_res, exec_res):
        schemas, csv_schema, api_schema = exec_res
        shared["schemas"] = schemas
        shared["csv_schema_str"] = "\n".join(
            [f"Table '{name}' [CSV]: [{', '.join(cols)}]" for name, cols in csv_schema.items()]
        )
        shared["api_schema_str"] = "\n".join(
            [f"Table '{name}' [API]: [{', '.join(cols)}]" for name, cols in api_schema.items()]
        )
        schema_lines = []
        for name, cols in schemas.items():
            source = shared.get("data_sources", {}).get(name, "merged")
            schema_lines.append(f"Table '{name}' [{source.upper()}]: [{', '.join(cols)}]")
        shared["schema_str"] = "\n".join(schema_lines)
        print(f"Schema inferred:\n{shared['schema_str']}")
        # TODO: Generate schema-driven query suggestions (e.g., common aggregations)
        # and store them for the UI to display.
        return "default"

class ClarifyQuery(Node):
    def prep(self, shared):
        return shared["question"], shared["schema_str"], list(shared["dfs"].keys())

    def exec(self, prep_res):
        q, schema, table_names = prep_res
        question_lower = q.lower()

        # Check if user is asking about tables/columns that don't exist
        # Look for patterns like "from X table" or "X column" that don't match schema
        suspicious_patterns = []

        # Check for explicit column/table references that don't exist
        words = question_lower.split()
        for i, word in enumerate(words):
            # Check if word looks like a column reference with underscore
            if '_' in word and len(word) > 3:
                # This looks like a column name - verify it exists in schema
                if word not in schema.lower():
                    suspicious_patterns.append(word)

        # If suspicious patterns found and they don't appear anywhere in schema, flag as ambiguous
        if suspicious_patterns:
            # Double check - maybe it's a partial match
            schema_lower = schema.lower()
            truly_missing = [p for p in suspicious_patterns if p not in schema_lower]
            if truly_missing:
                return "ambiguous", truly_missing

        return "clear", None

    def post(self, shared, prep_res, exec_res):
        status, missing = exec_res
        if status == "ambiguous":
            shared["final_text"] = f"Your query references unknown columns or tables: {missing}. Please clarify or check available schema."
            return "ambiguous"
        return "clear"

class AskUser(Node):
    """
    Terminal node for ambiguous queries.
    In CLI mode, prompts user for clarification and allows re-entering the flow.
    """
    def prep(self, shared):
        return {
            "final_text": shared.get("final_text", ""),
            "question": shared.get("question", ""),
            "schema_str": shared.get("schema_str", ""),
            "is_cli": shared.get("is_cli", False)
        }

    def exec(self, prep_res):
        final_text = prep_res.get("final_text", "")
        is_cli = prep_res.get("is_cli", False)
        
        # In CLI mode, prompt for user clarification
        if is_cli:
            print(f"\n⚠️  {final_text}")
            print("\nPlease provide a clarified question (or type 'quit' to exit):")
            try:
                user_input = input("> ").strip()
                if user_input.lower() in ['quit', 'exit', 'q']:
                    return {"action": "quit", "clarified_question": None}
                if user_input:
                    return {"action": "clarified", "clarified_question": user_input}
            except (EOFError, KeyboardInterrupt):
                return {"action": "quit", "clarified_question": None}
        
        # Non-CLI mode (Chainlit) - just print and exit
        return {"action": "exit", "clarified_question": None}

    def post(self, shared, prep_res, exec_res):
        if exec_res is None:
            exec_res = {"action": "exit", "clarified_question": None}
        
        action = exec_res.get("action", "exit")
        clarified_question = exec_res.get("clarified_question")
        
        if action == "clarified" and clarified_question:
            # Store the clarified question and signal to re-enter flow
            shared["question"] = clarified_question
            # Reset any error state for fresh analysis
            shared["exec_error"] = None
            shared["retry_count"] = 0
            # Clear previous entity resolution to force re-analysis
            shared.pop("entities", None)
            shared.pop("entity_map", None)
            shared.pop("cross_references", None)
            print(f"\n🔄 Re-analyzing with clarified question: {clarified_question}")
            return "clarified"
        elif action == "quit":
            shared["final_text"] = "Session ended by user."
            print("\n👋 Goodbye!")
            return "quit"
        else:
            # Default exit behavior
            print(f"System: {shared.get('final_text', 'Ends')}")
            return "default"

class EntityResolver(Node):
    """
    Discovers which tables contain data about entities mentioned in the query.
    Uses configurable sampling to balance accuracy and performance.
    """
    # Configuration: Sample size for column scanning
    # Adjustable via shared["config"]["entity_sample_size"]
    DEFAULT_SAMPLE_SIZE = 1000
    
    def prep(self, shared):
        return {
            "question": shared["question"],
            "schema": shared["schema_str"],
            "dfs": shared["dfs"],
            "sample_size": shared.get("config", {}).get("entity_sample_size", self.DEFAULT_SAMPLE_SIZE),
            "entity_ids": shared.get("entity_ids", {}),
        }

    def _get_sample(self, df, col, sample_size):
        """
        Get a sample of rows for entity scanning.
        Uses head() for consistency and performance.
        """
        try:
            series = df[col].dropna()
            if len(series) <= sample_size:
                return series
            return series.head(sample_size)
        except (KeyError, AttributeError):
            return pd.Series(dtype='object')

    def exec(self, prep_res):
        question = prep_res["question"]
        schema = prep_res["schema"]
        dfs = prep_res["dfs"]
        sample_size = prep_res["sample_size"]
        entity_ids = dict(prep_res.get("entity_ids", {}))
        
        knowledge_hints = knowledge_store.get_all_hints()
        
        extract_prompt = f"""Extract entities (people, teams, places, specific items) from this question.
Return a JSON array of entity names only.

Question: {question}

Example output: ["LeBron James", "Tracy McGrady", "Chicago Bulls"]
Return ONLY the JSON array, nothing else."""
        
        try:
            entities_response = call_llm(extract_prompt)
            entities_response = (entities_response or "").strip()
            if entities_response.startswith("```"):
                entities_response = entities_response.split("```")[1]
                if entities_response.startswith("json"):
                    entities_response = entities_response[4:]
            entities = json.loads(entities_response)
        except json.JSONDecodeError as e:
            print(f"Failed to parse entity JSON: {e}")
            entities = []
        except Exception as e:
            print(f"Unexpected error extracting entities: {e}")
            entities = []
        
        entity_map = {}
        for entity in entities:
            entity_map[entity] = {}
            entity_lower = entity.lower()
            entity_parts = entity_lower.split()
            # Resolve official IDs via nba_api static data
            player = nba_client.find_player(entity)
            if player:
                entity_ids[entity] = {"player_id": player.get("id")}
            else:
                team = nba_client.find_team(entity)
                if team:
                    entity_ids[entity] = {"team_id": team.get("id")}
            
            for table_name, df in dfs.items():
                matching_cols = []
                name_cols = []
                
                for col in df.columns:
                    col_lower = col.lower()
                    if any(x in col_lower for x in ['first_name', 'last_name', 'player_name', 'full_name', 'display']):
                        name_cols.append(col)
                
                if len(name_cols) >= 2 and len(entity_parts) >= 2:
                    first_name_cols = [c for c in name_cols if 'first' in c.lower()]
                    last_name_cols = [c for c in name_cols if 'last' in c.lower()]
                    
                    if first_name_cols and last_name_cols:
                        try:
                            fc = first_name_cols[0]
                            lc = last_name_cols[0]
                            # Use sampling for name column matching
                            first_sample = self._get_sample(df, fc, sample_size)
                            last_sample = self._get_sample(df, lc, sample_size)
                            first_match = first_sample.astype(str).str.lower().str.contains(entity_parts[0], na=False)
                            last_match = last_sample.astype(str).str.lower().str.contains(entity_parts[-1], na=False)
                            if first_match.any() and last_match.any():
                                matching_cols.extend([fc, lc])
                        except (KeyError, AttributeError, TypeError) as e:
                            # Column doesn't exist or wrong type, skip
                            pass
                
                for col in df.columns:
                    if col in matching_cols:
                        continue
                    try:
                        if df[col].dtype == 'object':
                            # Use sampling for general column scanning
                            sample = self._get_sample(df, col, sample_size)
                            matches = sample.astype(str).str.lower().str.contains(entity_lower, na=False)
                            if matches.any():
                                matching_cols.append(col)
                    except (KeyError, AttributeError, TypeError):
                        # Column access error or type conversion error, skip this column
                        continue
                
                if matching_cols:
                    entity_map[entity][table_name] = list(set(matching_cols))
                    knowledge_store.add_entity_mapping(entity, table_name, matching_cols)
        
        return {
            "entities": entities,
            "entity_map": entity_map,
            "knowledge_hints": knowledge_hints,
            "entity_ids": entity_ids,
        }

    def exec_fallback(self, prep_res, exc):
        print(f"EntityResolver failed: {exc}")
        return {
            "entities": [],
            "entity_map": {},
            "knowledge_hints": {}
        }

    def post(self, shared, prep_res, exec_res):
        shared["entities"] = exec_res["entities"]
        shared["entity_map"] = exec_res["entity_map"]
        shared["knowledge_hints"] = exec_res["knowledge_hints"]
        shared["entity_ids"] = exec_res["entity_ids"]
        print(f"Resolved {len(exec_res['entities'])} entities across tables.")
        if exec_res["entity_map"]:
            for entity, tables in exec_res["entity_map"].items():
                if tables:
                    print(f"  - {entity}: found in {list(tables.keys())}")
                else:
                    print(f"  - {entity}: NOT FOUND in any table")
        return "default"

class Planner(Node):
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

Create a detailed step-by-step plan (4-6 steps) to thoroughly answer the question. Include:
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
        print(f"Planner failed: {exc}")
        return "Plan generation failed. Please proceed with caution."

    def post(self, shared, prep_res, exec_res):
        shared["plan_steps"] = exec_res
        print("Plan generated.")
        return "default"

class CodeGenerator(Node):
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
            print("Fixing code based on error...")
            error_fix_hint = ""
            error = prep_res['error']
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
        print(f"CodeGenerator failed: {exc}")
        return "print('Code generation failed due to LLM error.')\nfinal_result = {}"

    def post(self, shared, prep_res, exec_res):
        shared["csv_code_snippet"] = exec_res
        return "default"

class NBAApiCodeGenerator(Node):
    """
    Generates nba_api specific analysis code leveraging the shared nba_client helper.
    """

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

        base_prompt = f"""You are a Python engineer generating code that uses nba_api via the helper `nba_client` from utils.nba_api_client.
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
        print(f"NBAApiCodeGenerator failed: {exc}")
        return "api_result = {}"

    def post(self, shared, prep_res, exec_res):
        shared["api_code_snippet"] = exec_res
        return "default"

class SafetyCheck(Node):
    """
    Parses code using AST to ensure no malicious imports or dangerous operations.
    """
    FORBIDDEN_MODULES = {
        'os', 'subprocess', 'sys', 'shutil', 'socket', 'requests',
        'urllib', 'http', 'ftplib', 'smtplib', 'ctypes', 'importlib',
        'builtins', 'code', 'codeop', 'compile', 'pickletools'
    }

    FORBIDDEN_FUNCTIONS = {
        '__import__', 'eval', 'exec', 'compile', 'open', 'input',
        'breakpoint', 'getattr', 'setattr', 'delattr', 'globals', 'locals',
        'vars', 'dir', 'type', 'object'
    }

    FORBIDDEN_ATTRIBUTES = {
        '__builtins__', '__globals__', '__code__', '__class__',
        '__bases__', '__subclasses__', '__mro__', '__dict__'
    }

    def prep(self, shared):
        return shared.get("csv_code_snippet", ""), shared.get("api_code_snippet", "")

    def _check_code(self, code):
        if not code:
            return "safe", None
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return "unsafe", f"Syntax Error: {e}"

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    module_root = alias.name.split(".")[0]
                    if module_root in self.FORBIDDEN_MODULES:
                        return "unsafe", f"Forbidden import: {alias.name}"
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    module_root = node.module.split(".")[0]
                    if module_root in self.FORBIDDEN_MODULES:
                        return "unsafe", f"Forbidden from-import: {node.module}"
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in self.FORBIDDEN_FUNCTIONS:
                        return "unsafe", f"Forbidden function call: {node.func.id}"
            elif isinstance(node, ast.Attribute):
                if node.attr in self.FORBIDDEN_ATTRIBUTES:
                    return "unsafe", f"Forbidden attribute access: {node.attr}"
            elif isinstance(node, ast.Subscript):
                if isinstance(node.slice, ast.Constant):
                    if isinstance(node.slice.value, str):
                        if node.slice.value in self.FORBIDDEN_ATTRIBUTES:
                            return "unsafe", f"Forbidden subscript access: {node.slice.value}"

        return "safe", None

    def exec(self, prep_res):
        csv_code, api_code = prep_res
        csv_status, csv_reason = self._check_code(csv_code)
        if csv_status == "unsafe":
            return "unsafe", f"CSV code unsafe: {csv_reason}"

        api_status, api_reason = self._check_code(api_code)
        if api_status == "unsafe":
            return "unsafe", f"API code unsafe: {api_reason}"

        return "safe", None

    def post(self, shared, prep_res, exec_res):
        status, reason = exec_res
        if status == "unsafe":
            print(f"Safety Violation: {reason}")
            shared["exec_error"] = f"Security check failed: {reason}"
            return "unsafe"
        print("Safety Check Passed.")
        return "safe"

class Executor(Node):
    """
    Executes code in a restricted local scope with timeout and resource limits.
    """
    # Configuration: Maximum execution time in seconds
    EXECUTION_TIMEOUT = 30  # seconds for CSV
    API_EXECUTION_TIMEOUT = 60  # seconds for API
    
    def prep(self, shared):
        return {
            "csv_code": shared.get("csv_code_snippet", ""),
            "api_code": shared.get("api_code_snippet", ""),
            "dfs": shared.get("dfs", {}),
        }

    def _execute_code_with_timeout(self, code, dfs, extra_scope=None, timeout=None):
        """
        Execute code in a separate thread with timeout.
        Uses threading for cross-platform compatibility.
        Includes enhanced sandbox security with safe builtins.
        """
        import threading
        import queue
        
        result_queue = queue.Queue()
        
        def target():
            try:
                # The Sandbox: Enhanced security with safe builtins
                final_result_sentinel = object()

                def blocked_global_call(*_args, **_kwargs):
                    raise RuntimeError("globals() is not available in the sandbox")

                safe_builtins = {
                    "abs": abs,
                    "all": all,
                    "any": any,
                    "dict": dict,
                    "enumerate": enumerate,
                    "float": float,
                    "int": int,
                    "len": len,
                    "list": list,
                    "locals": locals,
                    "max": max,
                    "min": min,
                    "range": range,
                    "round": round,
                    "set": set,
                    "sorted": sorted,
                    "str": str,
                    "sum": sum,
                    "tuple": tuple,
                    "zip": zip,
                    "Exception": Exception,
                    "NameError": NameError,
                    "ValueError": ValueError,
                    "KeyError": KeyError,
                    "TypeError": TypeError,
                    "RuntimeError": RuntimeError,
                    "globals": blocked_global_call,
                }

                local_scope = {
                    "dfs": dfs,
                    "pd": pd,
                    "final_result": final_result_sentinel,
                    "__builtins__": safe_builtins,
                }
                if extra_scope:
                    local_scope.update(extra_scope)
                
                # execute the code string
                exec(code, local_scope, local_scope)
                
                # Check if the code defined 'final_result'
                if "final_result" not in local_scope or local_scope["final_result"] is final_result_sentinel:
                    result_queue.put(("error", "Code did not define 'final_result' variable"))
                else:
                    result_queue.put(("success", local_scope["final_result"]))
            except Exception as e:
                result_queue.put(("error", str(e)))
        
        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout=timeout or self.EXECUTION_TIMEOUT)
        
        if thread.is_alive():
            # Thread is still running - timeout occurred
            # Note: We cannot forcibly kill the thread, but daemon=True ensures
            # it won't block program exit. We return a timeout error.
            return "error", f"Execution timed out after {timeout or self.EXECUTION_TIMEOUT} seconds. The code may be stuck in an infinite loop or processing too much data."
        
        try:
            return result_queue.get_nowait()
        except queue.Empty:
            return "error", "Execution failed without producing a result"

    def exec(self, prep_res):
        csv_status, api_status = ("skipped", None), ("skipped", None)
        csv_code = prep_res["csv_code"]
        api_code = prep_res["api_code"]
        dfs = prep_res["dfs"]

        if csv_code:
            csv_status = self._execute_code_with_timeout(csv_code, dfs, timeout=self.EXECUTION_TIMEOUT)
        if api_code:
            api_scope = {"nba_client": nba_client, "time": time}
            api_status = self._execute_code_with_timeout(
                api_code, dfs, extra_scope=api_scope, timeout=self.API_EXECUTION_TIMEOUT
            )
        return {"csv": csv_status, "api": api_status}

    def post(self, shared, prep_res, exec_res):
        csv_status, csv_payload = exec_res["csv"]
        api_status, api_payload = exec_res["api"]

        errors = []
        if csv_status == "error":
            errors.append(f"CSV code error: {csv_payload}")
        else:
            shared["csv_exec_result"] = csv_payload

        if api_status == "error":
            errors.append(f"API code error: {api_payload}")
        else:
            shared["api_exec_result"] = api_payload

        if errors:
            shared["exec_error"] = "; ".join(errors)
            return "error"

        # If only one source executed, propagate that to exec_result for downstream nodes
        shared["exec_result"] = {
            "csv": shared.get("csv_exec_result"),
            "api": shared.get("api_exec_result"),
        }
        print(f"Execution Success. CSV: {csv_status}, API: {api_status}")
        return "success"

class ErrorFixer(Node):
    MAX_RETRIES = 3
    
    def prep(self, shared):
        return shared.get("exec_error"), {
            "csv": shared.get("csv_code_snippet"),
            "api": shared.get("api_code_snippet"),
        }, shared.get("retry_count", 0)

    def exec(self, prep_res):
        error, code, retry_count = prep_res
        if retry_count >= self.MAX_RETRIES:
            return "max_retries_exceeded"
        return "try_again"

    def post(self, shared, prep_res, exec_res):
        if exec_res == "max_retries_exceeded":
            shared["final_text"] = f"Unable to answer the question after multiple attempts. Last error: {shared.get('exec_error', 'Unknown')}"
            print(f"\nMax retries exceeded. Stopping.")
            return "give_up"
        
        shared["retry_count"] = shared.get("retry_count", 0) + 1
        return "fix"

class DeepAnalyzer(Node):
    def prep(self, shared):
        return {
            "exec_result": shared.get("exec_result") or {
                "csv": shared.get("csv_exec_result"),
                "api": shared.get("api_exec_result"),
            },
            "question": shared["question"],
            "entity_map": shared.get("entity_map", {}),
            "entities": shared.get("entities", []),
            "cross_validation": shared.get("cross_validation", {}),
        }

    def _check_data_completeness(self, exec_result, entities):
        missing_entities = []
        data_warnings = []
        
        if isinstance(exec_result, dict):
            for entity in entities:
                entity_lower = entity.lower()
                found = False
                for key, value in exec_result.items():
                    if entity_lower in str(key).lower():
                        if isinstance(value, dict):
                            if not value or all(v is None or v == {} or v == [] for v in value.values()):
                                missing_entities.append(entity)
                                data_warnings.append(f"Data for '{entity}' appears incomplete or empty")
                            else:
                                found = True
                        elif value is not None and value != {} and value != []:
                            found = True
                if not found and entity not in missing_entities:
                    for key, value in exec_result.items():
                        if entity_lower in str(value).lower():
                            found = True
                            break
                    if not found:
                        missing_entities.append(entity)
                        data_warnings.append(f"Could not find data for '{entity}' in results")
        
        return missing_entities, data_warnings

    def exec(self, prep_res):
        exec_result = prep_res["exec_result"]
        question = prep_res["question"]
        entities = prep_res["entities"]
        cross_validation = prep_res["cross_validation"]
        
        if exec_result is None:
            return None
        
        missing_entities, data_warnings = self._check_data_completeness(exec_result, entities)
        
        result_str = str(exec_result)
        if len(result_str) > 5000:
            result_str = result_str[:5000] + "... [truncated]"
        
        entities_str = ", ".join(entities) if entities else "the data"
        
        warning_note = ""
        if data_warnings:
            warning_note = f"\n\nDATA QUALITY WARNING:\n" + "\n".join(f"- {w}" for w in data_warnings)
            warning_note += "\nIMPORTANT: Only analyze data that is actually present. Do NOT make up statistics for missing entities."
        
        prompt = f"""You are a sports data analyst. Analyze the following data results and provide insights.

ORIGINAL QUESTION: <user_question>{question}</user_question>

ENTITIES BEING ANALYZED: {entities_str}
{warning_note}
RAW DATA RESULTS:
<raw_data>
{result_str}
</raw_data>
CROSS VALIDATION SUMMARY:
{json.dumps(cross_validation, indent=2, default=str)}

Provide analysis based ONLY on data that is actually present:
1. KEY STATISTICS: Summarize the most important numbers (only from actual data)
2. COMPARISONS: Compare only entities with complete data
3. INSIGHTS: What insights can we draw from the available data
4. DATA GAPS: Clearly note which entities have incomplete data

CRITICAL: Do NOT fabricate or hallucinate statistics. If data is missing, say so clearly.

Return your analysis as a structured JSON object with these keys:
- "key_stats": dict of important statistics (from actual data only)
- "comparison": summary of comparisons (null if insufficient data)
- "insights": list of insights (based on actual data)
- "data_gaps": list of entities or data points that were not found
- "narrative_points": list of points to include in final response

Return ONLY valid JSON."""

        try:
            analysis_response = call_llm(prompt)
            analysis_response = (analysis_response or "").strip()
            if analysis_response.startswith("```"):
                analysis_response = analysis_response.split("```")[1]
                if analysis_response.startswith("json"):
                    analysis_response = analysis_response[4:]
            deep_analysis = json.loads(analysis_response)
            deep_analysis["_missing_entities"] = missing_entities
            deep_analysis["_data_warnings"] = data_warnings
        except json.JSONDecodeError as e:
            print(f"Failed to parse deep analysis JSON: {e}")
            deep_analysis = {
                "key_stats": {"raw_result": str(exec_result)[:500]},
                "comparison": None,
                "insights": ["Analysis completed with available data"],
                "data_gaps": missing_entities,
                "_missing_entities": missing_entities,
                "_data_warnings": data_warnings
            }
        except Exception as e:
            print(f"Unexpected error in deep analysis: {e}")
            deep_analysis = {
                "key_stats": {"raw_result": str(exec_result)[:500]},
                "comparison": None,
                "insights": ["Analysis completed with available data"],
                "data_gaps": missing_entities,
                "_missing_entities": missing_entities,
                "_data_warnings": data_warnings
            }
        
        return deep_analysis

    def exec_fallback(self, prep_res, exc):
        print(f"DeepAnalyzer failed: {exc}")
        return None

    def post(self, shared, prep_res, exec_res):
        shared["deep_analysis"] = exec_res
        if exec_res:
            if exec_res.get("_data_warnings"):
                print(f"Deep analysis completed with warnings: {exec_res['_data_warnings']}")
            else:
                print("Deep analysis completed.")
        return "default"

class Visualizer(Node):
    def prep(self, shared):
        if "exec_result" in shared:
            return shared["exec_result"]
        return None

    def exec(self, prep_res):
        if prep_res is None:
            return None
        if isinstance(prep_res, pd.DataFrame):
            numeric_cols = [
                col for col in prep_res.columns
                if pd.api.types.is_numeric_dtype(prep_res[col])
            ]
            if not numeric_cols:
                return None

            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            output_dir = "assets"
            os.makedirs(output_dir, exist_ok=True)

            # Clean up old chart files (keep only last 10)
            try:
                chart_files = sorted(
                    [f for f in os.listdir(output_dir) if f.startswith("chart_")],
                    key=lambda x: os.path.getmtime(os.path.join(output_dir, x))
                )
                # Remove oldest files if more than 10
                for old_file in chart_files[:-10]:
                    os.remove(os.path.join(output_dir, old_file))
            except (OSError, IOError):
                pass  # Cleanup is best-effort

            # Use timestamp in filename so cleanup logic works correctly
            timestamp = int(time.time())
            output_path = os.path.join(output_dir, f"chart_{timestamp}.png")

            plot_df = prep_res[numeric_cols].head(10)
            plt.figure(figsize=(8, 4))
            plot_df[numeric_cols[0]].plot(kind="bar")
            plt.title(f"Top 10 rows by {numeric_cols[0]}")
            plt.xlabel("Row")
            plt.ylabel(numeric_cols[0])
            plt.tight_layout()
            plt.savefig(output_path)
            plt.close()
            return output_path
        return None

    def post(self, shared, prep_res, exec_res):
        shared["chart_path"] = exec_res
        return "default"


class ResponseSynthesizer(Node):
    def prep(self, shared):
        return {
            "exec_result": shared.get("exec_result"),
            "deep_analysis": shared.get("deep_analysis"),
            "question": shared["question"],
            "entities": shared.get("entities", []),
            "entity_map": shared.get("entity_map", {}),
            "cross_validation": shared.get("cross_validation", {}),
            "data_sources": shared.get("data_sources", {}),
            "from_error": "exec_result" not in shared
        }

    def exec(self, prep_res):
        if prep_res["from_error"]:
            return None
        
        exec_result = prep_res["exec_result"]
        deep_analysis = prep_res["deep_analysis"]
        question = prep_res["question"]
        entities = prep_res["entities"]
        cross_validation = prep_res.get("cross_validation", {})
        data_sources = prep_res.get("data_sources", {})
        
        result_str = str(exec_result)
        if len(result_str) > 3000:
            result_str = result_str[:3000] + "... [truncated]"
        
        missing_entities = deep_analysis.get("_missing_entities", []) if deep_analysis else []
        data_warnings = deep_analysis.get("_data_warnings", []) if deep_analysis else []
        
        safe_analysis = {k: v for k, v in (deep_analysis or {}).items() if not k.startswith("_")}
        analysis_str = json.dumps(safe_analysis, indent=2, default=str) if safe_analysis else "No deep analysis available"
        if len(analysis_str) > 2000:
            analysis_str = analysis_str[:2000] + "... [truncated]"
        
        entities_str = " and ".join(entities) if entities else "the requested data"
        
        data_quality_note = ""
        if missing_entities or data_warnings:
            data_quality_note = f"""

DATA QUALITY NOTICE:
The following entities had incomplete or missing data: {', '.join(missing_entities) if missing_entities else 'None identified'}
Warnings: {'; '.join(data_warnings) if data_warnings else 'None'}

CRITICAL INSTRUCTION: Do NOT fabricate or hallucinate any statistics or facts for entities with missing data.
If data is incomplete, clearly state what data was found and what was not available.
Be honest about the limitations of the analysis."""
        
        prompt = f"""You are a sports analyst writing a response to a user's question.

QUESTION: <user_question>{question}</user_question>

ENTITIES: {entities_str}
{data_quality_note}
RAW DATA (from actual CSV analysis):
<raw_data>
{result_str}
</raw_data>

ANALYSIS:
<analysis>
{analysis_str}
</analysis>
CROSS VALIDATION:
{json.dumps(cross_validation, indent=2, default=str)}
DATA SOURCES:
{json.dumps(data_sources, indent=2, default=str)}

Write a well-structured response that:
1. Directly addresses the user's question based on ACTUAL DATA ONLY
2. Provides key statistics from the data that was found
3. CLEARLY STATES if data for any entity was not found or incomplete
4. Uses clear sections/headers for readability
5. Does NOT make up statistics - only report what was actually found

If data for some entities is missing, explicitly state:
"Note: Complete data for [entity] was not found in the available datasets."

Write in a professional tone. Use markdown formatting.
Be honest about data limitations - do not fabricate facts."""

        response = call_llm(prompt)
        if not response:
            response = "Unable to generate a response. Please try again."

        if entities:
            for entity in entities:
                if entity not in missing_entities:
                    for table, cols in prep_res["entity_map"].get(entity, {}).items():
                        knowledge_store.add_entity_mapping(entity, table, cols)
            if not missing_entities:
                knowledge_store.add_successful_pattern("comparison" if len(entities) > 1 else "lookup", question[:100])

        return response

    def exec_fallback(self, prep_res, exc):
        print(f"ResponseSynthesizer failed: {exc}")
        return "I apologize, but I am unable to generate a response at this time due to a system error."

    def post(self, shared, prep_res, exec_res):
        if exec_res:
            shared["final_text"] = exec_res
        print(f"\n{'='*60}")
        print("FINAL RESPONSE:")
        print('='*60)
        print(shared.get('final_text', 'No answer'))
        return "default"


class DataProfiler(Node):
    """
    Analyzes each table's data quality and statistics.
    Provides insights to help other nodes understand data better.
    """
    def prep(self, shared):
        return shared["dfs"]

    def exec(self, prep_res):
        dfs = prep_res
        profile = {}
        # TODO: Extend profiling to capture format-specific hints (Excel sheet names,
        # JSON nesting paths, DB schema/table names) for richer downstream analysis.
        for table_name, df in dfs.items():
            table_profile = {
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": {},
                "name_columns": [],
                "name_cols": [],
                "id_columns": [],
                "id_cols": [],
                "numeric_columns": [],
                "numeric_cols": [],
                "date_columns": [],
                "date_cols": []
            }
            
            for col in df.columns:
                col_lower = col.lower()
                col_info = {
                    "dtype": str(df[col].dtype),
                    "null_count": int(df[col].isnull().sum()),
                    "unique_count": int(df[col].nunique())
                }
                
                if 'name' in col_lower or 'first' in col_lower or 'last' in col_lower:
                    table_profile["name_columns"].append(col)
                    table_profile["name_cols"].append(col)
                if 'id' in col_lower:
                    table_profile["id_columns"].append(col)
                    table_profile["id_cols"].append(col)
                if pd.api.types.is_numeric_dtype(df[col]):
                    table_profile["numeric_columns"].append(col)
                    table_profile["numeric_cols"].append(col)
                if 'date' in col_lower or 'year' in col_lower:
                    table_profile["date_columns"].append(col)
                    table_profile["date_cols"].append(col)
                
                table_profile["columns"][col] = col_info
            
            profile[table_name] = table_profile
        
        return profile

    def post(self, shared, prep_res, exec_res):
        shared["data_profile"] = exec_res
        shared["profiles"] = exec_res
        tables_with_names = [t for t, p in exec_res.items() if p["name_columns"]]
        print(f"Data profiled: {len(exec_res)} tables, {len(tables_with_names)} with name columns")
        return "default"


class SearchExpander(Node):
    """
    Expands entity search to find related entities, aliases, and cross-references.
    Uses data profile to search more intelligently with configurable sampling.
    """
    # Configuration: Sample size for column scanning
    # Adjustable via shared["config"]["search_sample_size"]
    DEFAULT_SAMPLE_SIZE = 1000
    
    def prep(self, shared):
        return {
            "entity_map": shared.get("entity_map", {}),
            "entities": shared.get("entities", []),
            "dfs": shared["dfs"],
            "data_profile": shared.get("data_profile", {}),
            "question": shared["question"],
            "sample_size": shared.get("config", {}).get("search_sample_size", self.DEFAULT_SAMPLE_SIZE)
        }

    def _get_sample(self, df, col, sample_size):
        """
        Get a sample of rows for entity scanning.
        Uses head() for consistency and performance.
        """
        try:
            series = df[col].dropna()
            if len(series) <= sample_size:
                return df
            # Return sampled dataframe keeping alignment with original indices
            return df.head(sample_size)
        except (KeyError, AttributeError):
            return df.head(0)

    def exec(self, prep_res):
        entity_map = prep_res["entity_map"]
        entities = prep_res["entities"]
        dfs = prep_res["dfs"]
        profile = prep_res["data_profile"]
        sample_size = prep_res["sample_size"]
        
        expanded_map = dict(entity_map)
        related_entities = {}
        cross_references = {}
        
        for entity in entities:
            entity_lower = entity.lower()
            parts = entity_lower.split()
            
            for table_name, df in dfs.items():
                table_profile = profile.get(table_name, {})
                
                # Use sampling for name column searches
                sampled_df = self._get_sample(df, table_profile.get("name_columns", [""])[0] if table_profile.get("name_columns") else "", sample_size)
                
                for col in table_profile.get("name_columns", []):
                    try:
                        matches = sampled_df[sampled_df[col].astype(str).str.lower().str.contains(entity_lower, na=False)]
                        if not matches.empty and table_name not in expanded_map.get(entity, {}):
                            if entity not in expanded_map:
                                expanded_map[entity] = {}
                            expanded_map[entity][table_name] = [col]
                    except (KeyError, AttributeError, TypeError):
                        # Column access error or type conversion error, skip
                        pass
                
                if table_name in expanded_map.get(entity, {}):
                    id_cols = table_profile.get("id_columns", [])
                    for id_col in id_cols:
                        try:
                            name_cols = table_profile.get("name_columns", [])
                            if name_cols:
                                # Use sampling for cross-reference searches
                                mask = sampled_df[name_cols[0]].astype(str).str.lower().str.contains(parts[0] if parts else entity_lower, na=False)
                                matches = sampled_df[mask]
                                if not matches.empty:
                                    entity_id = str(matches.iloc[0].get(id_col, ''))
                                    if entity_id and entity_id != 'nan':
                                        if entity not in cross_references:
                                            cross_references[entity] = {}
                                        cross_references[entity][f"{table_name}.{id_col}"] = entity_id
                        except (KeyError, AttributeError, IndexError, TypeError):
                            # Column not found, empty matches, or type error, skip
                            pass
        
        return {
            "expanded_map": expanded_map,
            "related_entities": related_entities,
            "cross_references": cross_references
        }

    def post(self, shared, prep_res, exec_res):
        shared["entity_map"] = exec_res["expanded_map"]
        shared["cross_references"] = exec_res["cross_references"]

        total_tables = sum(len(tables) for tables in exec_res["expanded_map"].values())
        print(f"Search expanded: {len(exec_res['expanded_map'])} entities across {total_tables} table matches")
        if exec_res["cross_references"]:
            print(f"Cross-references found: {exec_res['cross_references']}")
        return "default"


class ResultValidator(Node):
    """
    Validates execution results against the original question and entity map.
    Identifies missing data and suggests additional queries.
    """
    def prep(self, shared):
        return {
            "exec_result": shared.get("exec_result"),
            "entities": shared.get("entities", []),
            "entity_map": shared.get("entity_map", {}),
            "question": shared["question"],
            "cross_references": shared.get("cross_references", {})
        }

    def exec(self, prep_res):
        exec_result = prep_res["exec_result"]
        entities = prep_res["entities"]
        entity_map = prep_res["entity_map"]
        
        validation = {
            "entities_found": [],
            "entities_missing": [],
            "data_completeness": {},
            "suggestions": []
        }
        
        result_str = str(exec_result).lower() if exec_result else ""
        
        for entity in entities:
            entity_lower = entity.lower()
            if entity_lower in result_str or any(p in result_str for p in entity_lower.split()):
                validation["entities_found"].append(entity)
                
                tables_with_data = entity_map.get(entity, {})
                completeness = len(tables_with_data)
                validation["data_completeness"][entity] = {
                    "tables_found": list(tables_with_data.keys()),
                    "completeness_score": min(completeness / 3, 1.0)
                }
            else:
                validation["entities_missing"].append(entity)
                validation["suggestions"].append(f"Re-search for {entity} with alternative name patterns")
        
        if isinstance(exec_result, dict):
            for entity in entities:
                if entity in exec_result:
                    entity_data = exec_result[entity]
                    if isinstance(entity_data, dict):
                        found_tables = entity_data.get("found_in_tables", [])
                        if len(found_tables) < 2:
                            validation["suggestions"].append(f"Limited data for {entity} - only in {found_tables}")
        
        return validation

    def post(self, shared, prep_res, exec_res):
        shared["validation_result"] = exec_res
        
        if exec_res["entities_missing"]:
            print(f"Validation: Missing data for {exec_res['entities_missing']}")
        else:
            print(f"Validation: All {len(exec_res['entities_found'])} entities found in results")
        
        return "default"

class CrossValidator(Node):
    """
    Compares CSV and API execution results, flags discrepancies, and reconciles values.
    """

    def prep(self, shared):
        return {
            "csv_result": shared.get("csv_exec_result"),
            "api_result": shared.get("api_exec_result"),
            "entity_ids": shared.get("entity_ids", {}),
        }

    def _compare_scalars(self, csv_value, api_value):
        if csv_value is None or api_value is None:
            return None, None
        try:
            csv_float = float(csv_value)
            api_float = float(api_value)
            if api_float == 0:
                return 0.0, 0.0
            diff_pct = abs(csv_float - api_float) / abs(api_float)
            severity = (
                "minor"
                if diff_pct < 0.02
                else "moderate"
                if diff_pct < 0.05
                else "major"
            )
            return diff_pct, severity
        except (TypeError, ValueError):
            return None, None

    def exec(self, prep_res):
        csv_result = prep_res["csv_result"]
        api_result = prep_res["api_result"]
        discrepancies = []
        reconciled = {}

        if isinstance(csv_result, dict) and isinstance(api_result, dict):
            keys = set(csv_result.keys()) | set(api_result.keys())
            for key in keys:
                csv_val = csv_result.get(key)
                api_val = api_result.get(key)
                diff_pct, severity = self._compare_scalars(csv_val, api_val)
                preferred, source = data_source_manager.reconcile_conflicts(csv_val, api_val, key)
                reconciled[key] = preferred
                if diff_pct is not None and severity:
                    discrepancies.append(
                        {
                            "field": key,
                            "csv": csv_val,
                            "api": api_val,
                            "diff_pct": diff_pct,
                            "severity": severity,
                        }
                    )
        else:
            reconciled = csv_result or api_result

        agreement_score = 1.0
        if discrepancies:
            agreement_score = max(0.0, 1.0 - sum(d["diff_pct"] for d in discrepancies) / len(discrepancies))

        return {
            "agreement_score": agreement_score,
            "discrepancies": discrepancies,
            "reconciled": reconciled,
        }

    def post(self, shared, prep_res, exec_res):
        shared["cross_validation"] = exec_res
        # Store reconciled result for downstream deep analysis
        if exec_res.get("reconciled") is not None:
            shared["exec_result"] = exec_res["reconciled"]
        print(f"Cross validation completed. Agreement score: {exec_res.get('agreement_score')}")
        return "default"


class ContextAggregator(Node):
    """
    Collects insights from previous nodes and creates enriched context for code generation.
    Acts as a communication hub between nodes.
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
            "data_quality_notes": []
        }
        
        for entity, tables in prep_res["entity_map"].items():
            context["entity_locations"][entity] = {
                "tables": list(tables.keys()),
                "primary_table": list(tables.keys())[0] if tables else None
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
        print(f"Context aggregated: {exec_res['context']['query_type']} query with {len(exec_res['context']['recommended_tables'])} tables")
        return "default"
