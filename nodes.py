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
        # Call LLM to generate plan
        return ["1. Merge players and stats", "2. Group by name"]

    def post(self, shared, prep_res, exec_res):
        shared["plan_steps"] = exec_res
        print("Plan generated.")

class CodeGenerator(Node):
    def prep(self, shared):
        return {
            "plan": shared["plan_steps"],
            "schema": shared["schema_str"],
            "error": shared.get("exec_error") # Context from previous failures
        }

    def exec(self, inputs):
        # In reality, prompt LLM to write code based on plan/error
        # Here we simulate generating valid code
        if inputs.get("error"):
            print("Fixing code based on error...")

        # Valid code simulation
        code = """
import pandas as pd
df_p = dfs['players']
df_s = dfs['stats']
merged = pd.merge(df_p, df_s, on='player_id')
final_result = merged['points'].mean()
"""
        return code.strip()

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

            # execute the code string
            exec(code, {}, local_scope)

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
    def prep(self, shared):
        return shared["exec_error"], shared["code_snippet"]

    def exec(self, inputs):
        # Call LLM to explain the fix
        # In this demo, we just pass through
        return "Add check for column existence"

    def post(self, shared, prep_res, exec_res):
        # We append the hint to the plan or error context so CodeGen sees it
        # The flow loops back to CodeGenerator automatically
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
        return shared["exec_result"]

    def exec(self, result):
        return f"The calculated answer is {result}."

    def post(self, shared, prep_res, exec_res):
        shared["final_text"] = exec_res
        print(f"\nFINAL ANSWER: {exec_res}")
