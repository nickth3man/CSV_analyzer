"""
Code safety and execution nodes.
"""

import ast
import queue
import threading
import time

import pandas as pd
from pocketflow import Node

from backend.config import API_EXECUTION_TIMEOUT, CSV_EXECUTION_TIMEOUT
from backend.utils.nba_api_client import nba_client


class SafetyCheck(Node):
    """Parse generated code to ensure no malicious imports or dangerous operations."""

    FORBIDDEN_MODULES = {
        "os",
        "subprocess",
        "sys",
        "shutil",
        "socket",
        "requests",
        "urllib",
        "http",
        "ftplib",
        "smtplib",
        "ctypes",
        "importlib",
        "builtins",
        "code",
        "codeop",
        "compile",
        "pickletools",
    }

    FORBIDDEN_FUNCTIONS = {
        "__import__",
        "eval",
        "exec",
        "compile",
        "open",
        "input",
        "breakpoint",
        "getattr",
        "setattr",
        "delattr",
        "globals",
        "locals",
        "vars",
        "dir",
        "type",
        "object",
    }

    FORBIDDEN_ATTRIBUTES = {
        "__builtins__",
        "__globals__",
        "__code__",
        "__class__",
        "__bases__",
        "__subclasses__",
        "__mro__",
        "__dict__",
    }

    def prep(self, shared):
        return shared.get("csv_code_snippet", ""), shared.get("api_code_snippet", "")

    def _check_code(self, code):
        if not code:
            return "safe", None
        try:
            tree = ast.parse(code)
        except SyntaxError as exc:
            return "unsafe", f"Syntax Error: {exc}"

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
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in self.FORBIDDEN_FUNCTIONS:
                    return "unsafe", f"Forbidden function call: {node.func.id}"
            elif isinstance(node, ast.Attribute):
                if node.attr in self.FORBIDDEN_ATTRIBUTES:
                    return "unsafe", f"Forbidden attribute access: {node.attr}"
            elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
                if isinstance(node.slice.value, str) and node.slice.value in self.FORBIDDEN_ATTRIBUTES:
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
    """Execute generated code in a restricted local scope with timeout and resource limits."""

    def prep(self, shared):
        return {
            "csv_code": shared.get("csv_code_snippet", ""),
            "api_code": shared.get("api_code_snippet", ""),
            "dfs": shared.get("dfs", {}),
        }

    @staticmethod
    def _execute_code_with_timeout(code, dfs, extra_scope=None, timeout=None):
        """
        Execute code in a separate thread with timeout using an enhanced sandbox.
        """
        result_queue = queue.Queue()

        def target():
            try:
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

                exec(code, local_scope, local_scope)

                if "final_result" not in local_scope or local_scope["final_result"] is final_result_sentinel:
                    result_queue.put(("error", "Code did not define 'final_result' variable"))
                else:
                    result_queue.put(("success", local_scope["final_result"]))
            except Exception as exc:  # noqa: BLE001
                result_queue.put(("error", str(exc)))

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        if thread.is_alive():
            return (
                "error",
                f"Execution timed out after {timeout} seconds. The code may be stuck in an infinite loop or processing too much data.",
            )

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
            csv_status = self._execute_code_with_timeout(csv_code, dfs, timeout=CSV_EXECUTION_TIMEOUT)
        if api_code:
            api_scope = {"nba_client": nba_client, "time": time}
            api_status = self._execute_code_with_timeout(
                api_code, dfs, extra_scope=api_scope, timeout=API_EXECUTION_TIMEOUT
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

        shared["exec_result"] = {
            "csv": shared.get("csv_exec_result"),
            "api": shared.get("api_exec_result"),
        }
        print(f"Execution Success. CSV: {csv_status}, API: {api_status}")
        return "success"


class ErrorFixer(Node):
    """Control loop that limits retries for code generation and execution errors."""

    MAX_RETRIES = 3

    def prep(self, shared):
        return shared.get("exec_error"), {
            "csv": shared.get("csv_code_snippet"),
            "api": shared.get("api_code_snippet"),
        }, shared.get("retry_count", 0)

    def exec(self, prep_res):
        error, _code, retry_count = prep_res
        if retry_count >= self.MAX_RETRIES:
            return "max_retries_exceeded"
        return "try_again"

    def post(self, shared, prep_res, exec_res):
        if exec_res == "max_retries_exceeded":
            shared["final_text"] = (
                "Unable to answer the question after multiple attempts. "
                f"Last error: {shared.get('exec_error', 'Unknown')}"
            )
            print("\nMax retries exceeded. Stopping.")
            return "give_up"

        shared["retry_count"] = shared.get("retry_count", 0) + 1
        return "fix"
