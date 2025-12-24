"""Code safety and execution nodes."""

import ast
import logging
import queue
import threading
import time

import pandas as pd
from pocketflow import Node

logger = logging.getLogger(__name__)

from typing import Never

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
        """
        Extract CSV and API code snippets from the shared state.

        Parameters:
            shared (dict): Shared state mapping that may contain "csv_code_snippet" and "api_code_snippet".

        Returns:
            tuple: A pair (csv_code_snippet, api_code_snippet) where each is the corresponding string from `shared` or an empty string if the key is missing.
        """
        return shared.get("csv_code_snippet", ""), shared.get("api_code_snippet", "")

    def _check_code(self, code):
        """
        Validate a Python source string for disallowed imports, function calls, attribute access, and subscripts.

        Parameters:
            code (str): Python source code to analyze.

        Returns:
            tuple: A pair (status, reason) where `status` is `"safe"` if no violations were found or `"unsafe"` if a violation or syntax error was detected; `reason` is a short explanatory message for `"unsafe"` or `None` when `status` is `"safe"`.
        """
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
            elif isinstance(node, ast.Subscript) and isinstance(
                node.slice, ast.Constant
            ):
                if (
                    isinstance(node.slice.value, str)
                    and node.slice.value in self.FORBIDDEN_ATTRIBUTES
                ):
                    return "unsafe", f"Forbidden subscript access: {node.slice.value}"

        return "safe", None

    def exec(self, prep_res):
        """
        Validate the provided CSV and API code snippets for forbidden imports, calls, or attribute access.

        Parameters:
            prep_res (tuple): A tuple (csv_code, api_code) containing the code snippets to validate as strings.

        Returns:
            tuple: `("safe", None)` if both snippets pass the safety checks, otherwise `("unsafe", reason)` where `reason` explains which snippet failed and why (prefixed with "CSV code unsafe:" or "API code unsafe:").
        """
        csv_code, api_code = prep_res
        csv_status, csv_reason = self._check_code(csv_code)
        if csv_status == "unsafe":
            return "unsafe", f"CSV code unsafe: {csv_reason}"

        api_status, api_reason = self._check_code(api_code)
        if api_status == "unsafe":
            return "unsafe", f"API code unsafe: {api_reason}"

        return "safe", None

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Finalize the safety check by interpreting exec_res and updating shared state.

        Parameters:
            shared (dict): Shared state mapping; will receive an 'exec_error' entry when a violation is detected.
            prep_res: Preparation result (unused by this method).
            exec_res (tuple): (status, reason) returned by the safety check; status is 'safe' or 'unsafe', reason is a human-readable explanation.

        Returns:
            str: 'unsafe' if a safety violation was detected, 'safe' otherwise.

        Side effects:
            - On 'unsafe', sets shared['exec_error'] to "Security check failed: {reason}" and prints a violation message.
            - On 'safe', prints a confirmation message.
        """
        status, reason = exec_res
        if status == "unsafe":
            logger.warning(f"Safety Violation: {reason}")
            shared["exec_error"] = f"Security check failed: {reason}"
            return "unsafe"
        logger.info("Safety Check Passed.")
        return "safe"


class Executor(Node):
    """Execute generated code in a restricted local scope with timeout and resource limits."""

    def prep(self, shared):
        """
        Prepare execution inputs by extracting CSV code, API code, and dataframe state from the shared context.

        Parameters:
            shared (dict): Mutable shared state containing optional keys used by execution nodes.

        Returns:
            dict: Mapping with keys:
                - "csv_code" (str): CSV-processing code snippet from shared["csv_code_snippet"] or empty string.
                - "api_code" (str): API-related code snippet from shared["api_code_snippet"] or empty string.
                - "dfs" (dict): Dataframes/state dictionary from shared["dfs"] or an empty dict.
        """
        return {
            "csv_code": shared.get("csv_code_snippet", ""),
            "api_code": shared.get("api_code_snippet", ""),
            "dfs": shared.get("dfs", {}),
        }

    @staticmethod
    def _execute_code_with_timeout(code, dfs, extra_scope=None, timeout=None):
        """
        Execute a user-provided code snippet in a restricted sandbox and return its result or an error.

        The function runs `code` inside a constrained execution environment where only a limited set of builtins and the provided scopes are available. The executed code must assign the final output to a variable named `final_result`; otherwise an error is returned. If execution exceeds `timeout` seconds, an error describing the timeout is returned.

        Parameters:
            code (str): Python source code to execute. Must set a variable `final_result` to produce a successful result.
            dfs (Any): Dataframes or dataset objects to expose to the executed code under the name `dfs`.
            extra_scope (dict | None): Additional variables to inject into the execution scope (optional).
            timeout (float | None): Maximum number of seconds to allow code to run before aborting with a timeout error (optional).

        Returns:
            tuple: A two-element tuple `(status, payload)` where `status` is `"success"` or `"error"`. For `"success"`, `payload` is the value of `final_result`. For `"error"`, `payload` is an error message describing why execution failed (syntax/runtime error, missing `final_result`, or timeout).
        """
        result_queue = queue.Queue()

        def target() -> None:
            """
            Execute the provided code string in a restricted sandbox and push a result tuple into `result_queue`.

            Executes `code` with a local scope that includes `dfs`, `pd`, a limited set of safe builtins (with `globals()` blocked), and any mappings from `extra_scope`. On successful execution, if the executed code assigns a value to `final_result`, the function puts ("success", final_result) into `result_queue`. If `final_result` is missing or unchanged, it puts ("error", "Code did not define 'final_result' variable"). Any exception raised during execution is caught and put as ("error", str(exception)) into `result_queue`.
            """
            try:
                final_result_sentinel = object()

                def blocked_global_call(*_args, **_kwargs) -> Never:
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

                exec(code, local_scope, local_scope)  # nosec B102

                if (
                    "final_result" not in local_scope
                    or local_scope["final_result"] is final_result_sentinel
                ):
                    result_queue.put(
                        ("error", "Code did not define 'final_result' variable")
                    )
                else:
                    result_queue.put(("success", local_scope["final_result"]))
            except Exception as exc:
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
        """
        Execute prepared CSV and API code snippets with sandboxing and timeouts, returning their execution statuses.

        Parameters:
            prep_res (dict): Prepared inputs with keys:
                - "csv_code" (str|None): Python code snippet to process CSV data; may be None or empty to skip.
                - "api_code" (str|None): Python code snippet that may call external APIs; may be None or empty to skip.
                - "dfs" (dict): Dataframes and related data injected into the execution scope.

        Returns:
            dict: Mapping with keys "csv" and "api", each a tuple (status, payload).
                - status (str): Execution outcome such as "skipped", "success", "error", or "timeout".
                - payload (Any): On "success", the value of `final_result` defined by the snippet; on "error" or "timeout", an error message; `None` if skipped.
        """
        csv_status, api_status = ("skipped", None), ("skipped", None)
        csv_code = prep_res["csv_code"]
        api_code = prep_res["api_code"]
        dfs = prep_res["dfs"]

        if csv_code:
            csv_status = self._execute_code_with_timeout(
                csv_code, dfs, timeout=CSV_EXECUTION_TIMEOUT
            )
        if api_code:
            api_scope = {"nba_client": nba_client, "time": time}
            api_status = self._execute_code_with_timeout(
                api_code, dfs, extra_scope=api_scope, timeout=API_EXECUTION_TIMEOUT
            )
        return {"csv": csv_status, "api": api_status}

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Aggregate CSV and API execution outcomes, record results or errors into `shared`, and return the overall status.

        Parameters:
            shared (dict): Mutable state shared between nodes; on success this will receive keys
                `csv_exec_result`, `api_exec_result`, and `exec_result`. On error this will receive
                `exec_error` containing joined error messages.
            prep_res: Preparation result (unused by this function but provided by node API).
            exec_res (dict): Execution results mapping "csv" and "api" to tuples of (status, payload),
                where `status` is one of `"success"`, `"skipped"`, or `"error"`, and `payload` is the
                corresponding result or error message.

        Returns:
            "error" if either CSV or API execution had status `"error"` (and `shared["exec_error"]` is set),
            "success" if both executions succeeded or were skipped (and `shared["exec_result"]` is set).
        """
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
        logger.info(f"Execution Success. CSV: {csv_status}, API: {api_status}")
        return "success"


class ErrorFixer(Node):
    """Control loop that limits retries for code generation and execution errors."""

    MAX_RETRIES = 3

    def prep(self, shared):
        """
        Prepare retry metadata and the latest generated code snippets from the shared state.

        Parameters:
            shared (dict): Shared state dictionary containing execution context and artifacts.

        Returns:
            tuple: A 3-tuple (last_error, codes, retry_count) where
                - last_error: the value of `shared["exec_error"]` or None if absent.
                - codes: dict with keys `"csv"` and `"api"` containing the corresponding code snippets from shared.
                - retry_count: integer retry counter from `shared["retry_count"]`, defaulting to 0 if not present.
        """
        return (
            shared.get("exec_error"),
            {
                "csv": shared.get("csv_code_snippet"),
                "api": shared.get("api_code_snippet"),
            },
            shared.get("retry_count", 0),
        )

    def exec(self, prep_res) -> str:
        """
        Decide whether to stop retrying or request another attempt based on the current retry count.

        Parameters:
            prep_res (tuple): A tuple of (last_error, code_dict, retry_count) where `retry_count` is the number of attempts already made.

        Returns:
            str: `"max_retries_exceeded"` if `retry_count` is greater than or equal to `MAX_RETRIES`, `"try_again"` otherwise.
        """
        _error, _code, retry_count = prep_res
        if retry_count >= self.MAX_RETRIES:
            return "max_retries_exceeded"
        return "try_again"

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Handle post-execution retry logic and update shared state accordingly.

        When exec_res is "max_retries_exceeded", records a final failure message in shared["final_text"] (including the last exec_error if present), prints a stop message, and returns "give_up". Otherwise increments shared["retry_count"] (initializing it to 1 if missing) and returns "fix".

        Parameters:
            shared (dict): Mutable runtime state shared across nodes; this function may set
                "final_text" and increments or initializes "retry_count".
            prep_res: Ignored in this function (present for node API compatibility).
            exec_res: Execution result indicator; treated specially when equal to "max_retries_exceeded".

        Returns:
            str: "give_up" if the maximum retries were exceeded, "fix" otherwise.
        """
        if exec_res == "max_retries_exceeded":
            shared["final_text"] = (
                "Unable to answer the question after multiple attempts. "
                f"Last error: {shared.get('exec_error', 'Unknown')}"
            )
            logger.warning("\nMax retries exceeded. Stopping.")
            return "give_up"

        shared["retry_count"] = shared.get("retry_count", 0) + 1
        return "fix"
