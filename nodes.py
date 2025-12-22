import os
import ast
import pandas as pd
from pocketflow import Node
# Ideally, import your actual utility here
# from utils.call_llm import call_llm 

# Use the real LLM from utils
from utils.call_llm import call_llm

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
        # Terminal node for this flow
        pass

    def post(self, shared, prep_res, exec_res):
        print(f"System: {shared.get('final_text', 'Ends')}")

class Planner(Node):
    def prep(self, shared):
        return shared["question"], shared["schema_str"]

    def exec(self, inputs):
        question, schema = inputs
        prompt = f"""You are a data analyst. Given the following database schema and user question, create a brief analysis plan.

DATABASE SCHEMA:
{schema}

USER QUESTION: {question}

Provide a concise step-by-step plan (2-4 steps) to answer the question using the available tables. Just list the steps, no code."""
        
        plan = call_llm(prompt)
        return plan

    def post(self, shared, prep_res, exec_res):
        shared["plan_steps"] = exec_res
        print("Plan generated.")

class CodeGenerator(Node):
    def prep(self, shared):
        return {
            "plan": shared["plan_steps"],
            "schema": shared["schema_str"],
            "question": shared["question"],
            "error": shared.get("exec_error"),
            "previous_code": shared.get("code_snippet")
        }

    def exec(self, inputs):
        if inputs.get("error"):
            print("Fixing code based on error...")
            prompt = f"""You are a Python data analyst. Your previous code had an error. Fix it.

DATABASE SCHEMA (available as dfs dictionary):
{inputs['schema']}

USER QUESTION: {inputs['question']}

PREVIOUS CODE:
{inputs.get('previous_code', 'None')}

ERROR: {inputs['error']}

Write ONLY the corrected Python code. The dataframes are in a dict called 'dfs' where keys are table names.
You must store your final answer in a variable called 'final_result'.
Do NOT include markdown code blocks. Just raw Python code."""
        else:
            prompt = f"""You are a Python data analyst. Write code to answer the user's question.

DATABASE SCHEMA (available as dfs dictionary):
{inputs['schema']}

USER QUESTION: {inputs['question']}

PLAN: {inputs['plan']}

Write Python code to answer the question. The dataframes are in a dict called 'dfs' where keys are table names (matching the table names in the schema).
You must store your final answer in a variable called 'final_result'.
Do NOT include markdown code blocks. Just raw Python code.
Only use pandas (pd) which is already imported."""

        code = call_llm(prompt)
        # Clean up any markdown code blocks if LLM added them
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

class Visualizer(Node):
    def prep(self, shared):
        return shared["exec_result"]

    def exec(self, result):
        # Check if result is a DataFrame suitable for plotting
        if isinstance(result, pd.DataFrame):
            # Generate plot code...
            return "plot_generated.png"
        return None

    def post(self, shared, prep_res, exec_res):
        shared["chart_path"] = exec_res
        return "default"

class ResponseFormatter(Node):
    def prep(self, shared):
        # Check if we have a result or if we came from give_up path
        if "exec_result" in shared:
            return shared["exec_result"]
        # Coming from give_up - final_text already set by ErrorFixer
        return None

    def exec(self, result):
        if result is None:
            return None  # Skip - final_text already set
        return f"The calculated answer is {result}."

    def post(self, shared, prep_res, exec_res):
        if exec_res is not None:
            shared["final_text"] = exec_res
        print(f"\nFINAL ANSWER: {shared.get('final_text', 'No answer')}")
