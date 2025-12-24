"""Security tests for SafetyCheck node - CRITICAL for preventing code injection."""

import pytest
from nodes import SafetyCheck


class TestSafetyCheckForbiddenImports:
    """Test that forbidden module imports are blocked."""

    def test_blocks_os_import(self):
        """Test that 'import os' is blocked."""
        node = SafetyCheck()
        code = "import os\nos.system('rm -rf /')"
        shared = {"csv_code_snippet": code}

        node.prep(shared)
        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "Forbidden import: os" in reason

    def test_blocks_subprocess_import(self):
        """Test that 'import subprocess' is blocked."""
        node = SafetyCheck()
        code = "import subprocess\nsubprocess.call(['ls'])"
        shared = {"csv_code_snippet": code}

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "subprocess" in reason

    def test_blocks_sys_import(self):
        """Test that 'import sys' is blocked."""
        node = SafetyCheck()
        code = "import sys\nsys.exit()"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "sys" in reason

    def test_blocks_socket_import(self):
        """Test that 'import socket' is blocked."""
        node = SafetyCheck()
        code = "import socket\ns = socket.socket()"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "socket" in reason

    def test_blocks_requests_import(self):
        """Test that 'import requests' is blocked."""
        node = SafetyCheck()
        code = "import requests\nrequests.get('http://evil.com')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "requests" in reason

    def test_blocks_urllib_import(self):
        """Test that 'import urllib' is blocked."""
        node = SafetyCheck()
        code = "import urllib"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "urllib" in reason

    def test_blocks_importlib_import(self):
        """Test that 'import importlib' is blocked."""
        node = SafetyCheck()
        code = "import importlib"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "importlib" in reason

    def test_blocks_shutil_import(self):
        """Test that 'import shutil' is blocked."""
        node = SafetyCheck()
        code = "import shutil\nshutil.rmtree('/')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "shutil" in reason

    def test_blocks_from_import(self):
        """Test that 'from os import ...' is blocked."""
        node = SafetyCheck()
        code = "from os import system\nsystem('ls')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "Forbidden from-import: os" in reason

    def test_blocks_nested_module_import(self):
        """Test that 'import os.path' is blocked (root module check)."""
        node = SafetyCheck()
        code = "import os.path"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "os" in reason


class TestSafetyCheckForbiddenFunctions:
    """Test that forbidden function calls are blocked."""

    def test_blocks_eval(self):
        """Test that eval() is blocked."""
        node = SafetyCheck()
        code = "result = eval('1 + 1')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "eval" in reason

    def test_blocks_exec(self):
        """Test that exec() is blocked."""
        node = SafetyCheck()
        code = "exec('print(1)')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "exec" in reason

    def test_blocks_compile(self):
        """Test that compile() is blocked."""
        node = SafetyCheck()
        code = "compile('1+1', '<string>', 'eval')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "compile" in reason

    def test_blocks_open(self):
        """Test that open() is blocked."""
        node = SafetyCheck()
        code = "f = open('/etc/passwd', 'r')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "open" in reason

    def test_blocks_input(self):
        """Test that input() is blocked."""
        node = SafetyCheck()
        code = "user_input = input('Enter something: ')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "input" in reason

    def test_blocks_getattr(self):
        """Test that getattr() is blocked."""
        node = SafetyCheck()
        code = "getattr(obj, '__dict__')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "getattr" in reason

    def test_blocks_setattr(self):
        """Test that setattr() is blocked."""
        node = SafetyCheck()
        code = "setattr(obj, 'attr', 'value')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "setattr" in reason

    def test_blocks_delattr(self):
        """Test that delattr() is blocked."""
        node = SafetyCheck()
        code = "delattr(obj, 'attr')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "delattr" in reason

    def test_blocks_globals(self):
        """Test that globals() is blocked."""
        node = SafetyCheck()
        code = "g = globals()"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "globals" in reason

    def test_blocks_locals(self):
        """Test that locals() is blocked."""
        node = SafetyCheck()
        code = "l = locals()"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "locals" in reason

    def test_blocks_dunder_import(self):
        """Test that __import__() is blocked."""
        node = SafetyCheck()
        code = "os = __import__('os')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "__import__" in reason


class TestSafetyCheckForbiddenAttributes:
    """Test that forbidden attribute access is blocked."""

    def test_blocks_builtins_access(self):
        """Test that __builtins__ access is blocked."""
        node = SafetyCheck()
        code = "b = some_obj.__builtins__"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "__builtins__" in reason

    def test_blocks_globals_attribute(self):
        """Test that __globals__ access is blocked."""
        node = SafetyCheck()
        code = "g = func.__globals__"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "__globals__" in reason

    def test_blocks_code_attribute(self):
        """Test that __code__ access is blocked."""
        node = SafetyCheck()
        code = "c = func.__code__"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "__code__" in reason

    def test_blocks_class_attribute(self):
        """Test that __class__ access is blocked."""
        node = SafetyCheck()
        code = "c = obj.__class__"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "__class__" in reason

    def test_blocks_dict_attribute(self):
        """Test that __dict__ access is blocked."""
        node = SafetyCheck()
        code = "d = obj.__dict__"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "__dict__" in reason

    def test_blocks_subscript_builtins_access(self):
        """Test that obj['__builtins__'] subscript access is blocked."""
        node = SafetyCheck()
        code = "b = some_dict['__builtins__']"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "__builtins__" in reason

    def test_blocks_subscript_globals_access(self):
        """Test that obj['__globals__'] subscript access is blocked."""
        node = SafetyCheck()
        code = "g = some_dict['__globals__']"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "__globals__" in reason


class TestSafetyCheckSyntaxErrors:
    """Test handling of syntax errors."""

    def test_handles_syntax_error(self):
        """Test that syntax errors are caught."""
        node = SafetyCheck()
        code = "if True\n    print('missing colon')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "Syntax Error" in reason

    def test_handles_invalid_python(self):
        """Test that completely invalid Python is caught."""
        node = SafetyCheck()
        code = "this is not valid python at all @@@ ###"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "Syntax Error" in reason


class TestSafetyCheckSafeCode:
    """Test that safe code is allowed."""

    def test_allows_pandas_operations(self):
        """Test that safe pandas operations are allowed."""
        node = SafetyCheck()
        code = """
import pandas as pd
df = pd.DataFrame({'a': [1, 2, 3]})
final_result = df['a'].mean()
"""
        status, reason = node.exec((code, ""))

        assert status == "safe"
        assert reason is None

    def test_allows_numpy_operations(self):
        """Test that numpy operations are allowed."""
        node = SafetyCheck()
        code = """
import numpy as np
arr = np.array([1, 2, 3])
final_result = arr.mean()
"""
        status, reason = node.exec((code, ""))

        assert status == "safe"
        assert reason is None

    def test_allows_basic_arithmetic(self):
        """Test that basic arithmetic is allowed."""
        node = SafetyCheck()
        code = "final_result = 1 + 2 * 3"

        status, reason = node.exec((code, ""))

        assert status == "safe"
        assert reason is None

    def test_allows_dataframe_operations(self):
        """Test that DataFrame operations are allowed."""
        node = SafetyCheck()
        code = """
final_result = dfs['employees']['salary'].mean()
"""
        status, reason = node.exec((code, ""))

        assert status == "safe"
        assert reason is None

    def test_allows_filtering(self):
        """Test that DataFrame filtering is allowed."""
        node = SafetyCheck()
        code = """
filtered = dfs['employees'][dfs['employees']['age'] > 30]
final_result = filtered['salary'].sum()
"""
        status, reason = node.exec((code, ""))

        assert status == "safe"
        assert reason is None


class TestSafetyCheckPostMethod:
    """Test the post() method behavior."""

    def test_post_unsafe_code(self):
        """Test post() method when code is unsafe."""
        node = SafetyCheck()
        shared = {"csv_code_snippet": "import os"}

        node.prep(shared)
        status, reason = node.exec(("import os", ""))
        action = node.post(shared, ("import os", ""), (status, reason))

        assert action == "unsafe"
        assert "exec_error" in shared
        assert "Security check failed" in shared["exec_error"]

    def test_post_safe_code(self):
        """Test post() method when code is safe."""
        node = SafetyCheck()
        shared = {"csv_code_snippet": "final_result = 1 + 1"}

        code = "final_result = 1 + 1"
        status, reason = node.exec((code, ""))
        action = node.post(shared, (code, ""), (status, reason))

        assert action == "safe"
        assert "exec_error" not in shared


class TestSafetyCheckEvasionAttempts:
    """Test various evasion attempts."""

    def test_blocks_string_based_import(self):
        """Test that string-based imports via __import__ are blocked."""
        node = SafetyCheck()
        code = "__import__('os').system('ls')"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "__import__" in reason

    def test_blocks_nested_attribute_access(self):
        """Test that nested forbidden attribute access is blocked."""
        node = SafetyCheck()
        code = "x = obj.method.__globals__"

        status, reason = node.exec((code, ""))

        assert status == "unsafe"
        assert "__globals__" in reason

    def test_allows_safe_subscripts(self):
        """Test that safe dictionary subscript access is allowed."""
        node = SafetyCheck()
        code = "value = dfs['employees']['name'][0]"

        status, reason = node.exec((code, ""))

        assert status == "safe"
        assert reason is None
