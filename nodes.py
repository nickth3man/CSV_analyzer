import os
import ast
import json
import pandas as pd
from pocketflow import Node
from utils.call_llm import call_llm
from utils.knowledge_store import knowledge_store

class LoadData(Node):
    def prep(self, shared):
        # Scan the CSV directory
        csv_dir = "CSV"
        return csv_dir

    def exec(self, csv_dir):
        # Read all CSV files from the CSV folder
        data = {}
        if os.path.exists(csv_dir):
            for filename in os.listdir(csv_dir):
                if filename.endswith(".csv"):
                    filepath = os.path.join(csv_dir, filename)
                    try:
                        table_name = filename.replace(".csv", "")
                        data[table_name] = pd.read_csv(filepath)
                    except Exception as e:
                        print(f"Error loading {filename}: {e}")
        return data

    def post(self, shared, prep_res, exec_res):
        shared["dfs"] = exec_res
        print(f"Loaded {len(exec_res)} dataframes.")
        return "default"

class SchemaInference(Node):
    def prep(self, shared):
        return shared["dfs"]

    def exec(self, dfs):
        schema_lines = []
        for name, df in dfs.items():
            cols = ", ".join(df.columns)
            schema_lines.append(f"Table '{name}': [{cols}]")
        return "\n".join(schema_lines)

    def post(self, shared, prep_res, exec_res):
        shared["schema_str"] = exec_res
        print(f"Schema inferred:\n{exec_res}")

class ClarifyQuery(Node):
    def prep(self, shared):
        return shared["question"], shared["schema_str"]

    def exec(self, inputs):
        q, schema = inputs
        # Logic: If query references columns not in schema, return ambiguous
        # Simulating a check...
        if "bad_column" in q:
            return "ambiguous"
        return "clear"

    def post(self, shared, prep_res, exec_res):
        if exec_res == "ambiguous":
            shared["final_text"] = "Your query references unknown columns. Please clarify."
        return exec_res

class AskUser(Node):
    def exec(self, _):
        pass

    def post(self, shared, prep_res, exec_res):
        print(f"System: {shared.get('final_text', 'Ends')}")

class EntityResolver(Node):
    def prep(self, shared):
        return {
            "question": shared["question"],
            "schema": shared["schema_str"],
            "dfs": shared["dfs"]
        }

    def exec(self, inputs):
        question = inputs["question"]
        schema = inputs["schema"]
        dfs = inputs["dfs"]
        
        knowledge_hints = knowledge_store.get_all_hints()
        
        extract_prompt = f"""Extract all named entities (people, teams, places, specific items) from this question.
Return a JSON array of entity names only.

Question: {question}

Example output: ["LeBron James", "Tracy McGrady", "Chicago Bulls"]
Return ONLY the JSON array, nothing else."""
        
        try:
            entities_response = call_llm(extract_prompt)
            entities_response = entities_response.strip()
            if entities_response.startswith("```"):
                entities_response = entities_response.split("```")[1]
                if entities_response.startswith("json"):
                    entities_response = entities_response[4:]
            entities = json.loads(entities_response)
        except (json.JSONDecodeError, Exception):
            entities = []
        
        entity_map = {}
        for entity in entities:
            entity_map[entity] = {}
            entity_lower = entity.lower()
            entity_parts = entity_lower.split()
            
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
                            first_match = df[fc].astype(str).str.lower().str.contains(entity_parts[0], na=False)
                            last_match = df[lc].astype(str).str.lower().str.contains(entity_parts[-1], na=False)
                            if (first_match & last_match).any():
                                matching_cols.extend([fc, lc])
                        except Exception:
                            pass
                
                for col in df.columns:
                    if col in matching_cols:
                        continue
                    try:
                        if df[col].dtype == 'object':
                            sample = df[col].dropna().head(1000)
                            matches = sample.astype(str).str.lower().str.contains(entity_lower, na=False)
                            if matches.any():
                                matching_cols.append(col)
                    except Exception:
                        continue
                
                if matching_cols:
                    entity_map[entity][table_name] = list(set(matching_cols))
                    knowledge_store.add_entity_mapping(entity, table_name, matching_cols)
        
        return {
            "entities": entities,
            "entity_map": entity_map,
            "knowledge_hints": knowledge_hints
        }

    def post(self, shared, prep_res, exec_res):
        shared["entities"] = exec_res["entities"]
        shared["entity_map"] = exec_res["entity_map"]
        shared["knowledge_hints"] = exec_res["knowledge_hints"]
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
            "knowledge_hints": shared.get("knowledge_hints", {})
        }

    def exec(self, inputs):
        question = inputs["question"]
        schema = inputs["schema"]
        entity_map = inputs["entity_map"]
        knowledge_hints = inputs["knowledge_hints"]
        
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
        
        prompt = f"""You are a data analyst. Given the database schema, user question, and entity locations, create a comprehensive analysis plan.

DATABASE SCHEMA:
{schema}
{entity_info}
{hints_info}
USER QUESTION: {question}

Create a detailed step-by-step plan (4-6 steps) to thoroughly answer the question. Include:
1. Which tables to query and how to join them
2. What filters to apply for the specific entities
3. What aggregations or comparisons to perform
4. What insights to extract

Be thorough - this is for deep analysis, not just a simple lookup."""
        
        plan = call_llm(prompt)
        return plan

    def post(self, shared, prep_res, exec_res):
        shared["plan_steps"] = exec_res
        print("Plan generated.")

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
            "question": shared["question"],
            "entity_map": shared.get("entity_map", {}),
            "entities": shared.get("entities", []),
            "error": shared.get("exec_error"),
            "previous_code": shared.get("code_snippet")
        }

    def exec(self, inputs):
        entity_info = ""
        if inputs.get("entity_map"):
            entity_info = "\n\nENTITY LOCATIONS:\n"
            for entity, tables in inputs["entity_map"].items():
                for table, cols in tables.items():
                    entity_info += f"  - '{entity}' is in table '{table}', columns: {cols}\n"
        
        is_comparison = len(inputs.get("entities", [])) > 1
        comparison_hint = ""
        if is_comparison:
            entities = inputs["entities"]
            comparison_hint = f"""
COMPARISON QUERY: You are comparing these entities: {entities}
For each entity, query ONLY the tables listed in ENTITY LOCATIONS above.
Loop through each entity, gather data from their respective tables, then combine into final_result.
"""
        
        if inputs.get("error"):
            print("Fixing code based on error...")
            error_fix_hint = ""
            error = inputs['error']
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
{inputs['schema']}
{entity_info}
{comparison_hint}
USER QUESTION: {inputs['question']}

PREVIOUS CODE:
{inputs.get('previous_code', 'None')}

ERROR: {inputs['error']}

Write ONLY the corrected Python code. AVOID complex merges - query tables separately instead.
The dataframes are in a dict called 'dfs' where keys are table names.
Store your final answer in a variable called 'final_result' (a dictionary).
Do NOT include markdown code blocks. Just raw Python code."""
        else:
            prompt = f"""You are a Python data analyst. Write comprehensive code to answer the user's question.
{self.DYNAMIC_GUIDANCE}
DATABASE SCHEMA (available as dfs dictionary):
{inputs['schema']}
{entity_info}
{comparison_hint}
USER QUESTION: {inputs['question']}

PLAN: {inputs['plan']}

Write Python code to thoroughly analyze and answer the question. 
- The dataframes are in a dict called 'dfs' where keys are table names
- Use the ENTITY LOCATIONS above to find the right tables and columns for filtering
- ONLY query tables that are mentioned in ENTITY LOCATIONS - don't assume tables exist
- AVOID complex multi-table merges - query tables separately and combine in Python
- Store your final answer in a variable called 'final_result' (a dictionary)
- Make final_result a dictionary with all relevant data for deep analysis
- Do NOT include markdown code blocks. Just raw Python code.
- Only use pandas (pd) which is already imported."""

        code = call_llm(prompt)
        code = code.replace("```python", "").replace("```", "").strip()
        return code

    def post(self, shared, prep_res, exec_res):
        shared["code_snippet"] = exec_res

class SafetyCheck(Node):
    """
    Parses code using AST to ensure no malicious imports.
    """
    def prep(self, shared):
        return shared["code_snippet"]

    def exec(self, code):
        try:
            tree = ast.parse(code)
        except SyntaxError:
            return "unsafe", "Syntax Error"

        for node in ast.walk(tree):
            # Block 'import os', 'import subprocess', etc.
            if isinstance(node, ast.Import):
                for name in node.names:
                    if name.name in ['os', 'subprocess', 'sys', 'shutil']:
                        return "unsafe", f"Forbidden import: {name.name}"
            # Block 'from os import ...'
            elif isinstance(node, ast.ImportFrom):
                if node.module in ['os', 'subprocess', 'sys', 'shutil']:
                    return "unsafe", f"Forbidden from-import: {node.module}"

        return "safe", None

    def post(self, shared, prep_res, exec_res):
        status, reason = exec_res
        if status == "unsafe":
            print(f"Safety Violation: {reason}")
            # Inject the reason so CodeGenerator knows what to fix
            shared["exec_error"] = f"Security check failed: {reason}"
            return "unsafe"
        print("Safety Check Passed.")
        return "safe"

class Executor(Node):
    """
    Executes code in a restricted local scope.
    """
    def prep(self, shared):
        return shared["code_snippet"], shared["dfs"]

    def exec(self, inputs):
        code, dfs = inputs

        # The Sandbox: We only pass 'dfs' and 'pd' to the code.
        local_scope = {"dfs": dfs, "pd": pd}

        try:
            # execute the code string - use local_scope for both globals and locals
            # so that dfs/pd are accessible in the executed code
            exec(code, local_scope, local_scope)

            # Check if the code defined 'final_result'
            if "final_result" not in local_scope:
                raise ValueError("Code did not define 'final_result' variable")

            return "success", local_scope["final_result"]

        except Exception as e:
            return "error", str(e)

    def post(self, shared, prep_res, exec_res):
        status, payload = exec_res
        if status == "error":
            print(f"Execution Error: {payload}")
            shared["exec_error"] = payload
            return "error"

        shared["exec_result"] = payload
        print(f"Execution Success. Result: {payload}")
        return "success"

class ErrorFixer(Node):
    MAX_RETRIES = 3
    
    def prep(self, shared):
        return shared["exec_error"], shared["code_snippet"], shared.get("retry_count", 0)

    def exec(self, inputs):
        error, code, retry_count = inputs
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
            "exec_result": shared.get("exec_result"),
            "question": shared["question"],
            "entity_map": shared.get("entity_map", {}),
            "entities": shared.get("entities", [])
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

    def exec(self, inputs):
        exec_result = inputs["exec_result"]
        question = inputs["question"]
        entities = inputs["entities"]
        
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

ORIGINAL QUESTION: {question}

ENTITIES BEING ANALYZED: {entities_str}
{warning_note}
RAW DATA RESULTS:
{result_str}

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
            analysis_response = analysis_response.strip()
            if analysis_response.startswith("```"):
                analysis_response = analysis_response.split("```")[1]
                if analysis_response.startswith("json"):
                    analysis_response = analysis_response[4:]
            deep_analysis = json.loads(analysis_response)
            deep_analysis["_missing_entities"] = missing_entities
            deep_analysis["_data_warnings"] = data_warnings
        except (json.JSONDecodeError, Exception):
            deep_analysis = {
                "key_stats": {"raw_result": str(exec_result)[:500]},
                "comparison": None,
                "insights": ["Analysis completed with available data"],
                "data_gaps": missing_entities,
                "_missing_entities": missing_entities,
                "_data_warnings": data_warnings
            }
        
        return deep_analysis

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

    def exec(self, result):
        if result is None:
            return None
        if isinstance(result, pd.DataFrame):
            return "plot_generated.png"
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
            "from_error": "exec_result" not in shared
        }

    def exec(self, inputs):
        if inputs["from_error"]:
            return None
        
        exec_result = inputs["exec_result"]
        deep_analysis = inputs["deep_analysis"]
        question = inputs["question"]
        entities = inputs["entities"]
        
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

QUESTION: {question}

ENTITIES: {entities_str}
{data_quality_note}
RAW DATA (from actual CSV analysis):
{result_str}

ANALYSIS:
{analysis_str}

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
        
        if entities:
            for entity in entities:
                if entity not in missing_entities:
                    for table, cols in inputs["entity_map"].get(entity, {}).items():
                        knowledge_store.add_entity_mapping(entity, table, cols)
            if not missing_entities:
                knowledge_store.add_successful_pattern("comparison" if len(entities) > 1 else "lookup", question[:100])
        
        return response

    def post(self, shared, prep_res, exec_res):
        if exec_res is not None:
            shared["final_text"] = exec_res
        print(f"\n{'='*60}")
        print("FINAL RESPONSE:")
        print('='*60)
        print(shared.get('final_text', 'No answer'))
