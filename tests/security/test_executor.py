"""Security tests for Executor node - sandboxed code execution."""

import pytest
import pandas as pd
from nodes import Executor


class TestExecutorSandboxing:
    """Test that code execution is properly sandboxed."""

    def test_only_dfs_and_pd_available(self, sample_df):
        """Test that only 'dfs' and 'pd' are available in execution scope."""
        node = Executor()
        code = """
# Try to access only allowed variables
available_vars = list(locals().keys())
final_result = sorted([v for v in available_vars if not v.startswith('_')])
"""
        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        # Should only have 'dfs', 'pd', and 'final_result' (created by the code)
        assert set(result) == {'dfs', 'pd', 'final_result'}

    def test_cannot_access_globals(self, sample_df):
        """Test that code cannot access global scope."""
        node = Executor()
        # This code will fail because globals() is not available
        # (it should be blocked by SafetyCheck, but even if not, exec should fail)
        code = """
try:
    g = globals()
    final_result = "BREACH"
except NameError:
    final_result = "SAFE"
"""
        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        # Should fail because globals() is not in scope
        assert status == "error"

    def test_can_access_dfs(self, sample_df):
        """Test that code can access the 'dfs' variable."""
        node = Executor()
        code = "final_result = list(dfs.keys())"

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df, "sales": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        assert set(result) == {"employees", "sales"}

    def test_can_use_pandas(self, sample_df):
        """Test that code can use pandas (pd)."""
        node = Executor()
        code = """
df = dfs['employees']
final_result = df['salary'].mean()
"""
        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        assert result == sample_df['salary'].mean()


class TestExecutorResultExtraction:
    """Test that results are properly extracted."""

    def test_extracts_final_result(self, sample_df):
        """Test that final_result is extracted correctly."""
        node = Executor()
        code = "final_result = 42"

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        assert result == 42

    def test_requires_final_result(self, sample_df):
        """Test that code must define 'final_result'."""
        node = Executor()
        code = "x = 42"  # No final_result defined

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "error"
        assert "final_result" in result

    def test_returns_dataframe(self, sample_df):
        """Test that final_result can be a DataFrame."""
        node = Executor()
        code = """
df = dfs['employees']
final_result = df[df['age'] > 30]
"""
        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # Bob (35) and Charlie (42)

    def test_returns_scalar(self, sample_df):
        """Test that final_result can be a scalar value."""
        node = Executor()
        code = "final_result = dfs['employees']['salary'].max()"

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        assert result == 95000

    def test_returns_list(self, sample_df):
        """Test that final_result can be a list."""
        node = Executor()
        code = "final_result = dfs['employees']['name'].tolist()"

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        assert result == ["Alice", "Bob", "Charlie"]

    def test_returns_dict(self, sample_df):
        """Test that final_result can be a dictionary."""
        node = Executor()
        code = """
df = dfs['employees']
final_result = {
    'count': len(df),
    'avg_salary': df['salary'].mean()
}
"""
        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        assert result['count'] == 3
        assert result['avg_salary'] == 84000


class TestExecutorErrorHandling:
    """Test error handling in code execution."""

    def test_handles_attribute_error(self, sample_df):
        """Test handling of AttributeError."""
        node = Executor()
        code = "final_result = dfs['employees'].nonexistent_column"

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "error"
        assert "nonexistent_column" in result.lower() or "attribute" in result.lower()

    def test_handles_key_error(self, sample_df):
        """Test handling of KeyError."""
        node = Executor()
        code = "final_result = dfs['nonexistent_table']"

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "error"
        assert "nonexistent_table" in result or "KeyError" in result

    def test_handles_type_error(self, sample_df):
        """Test handling of TypeError."""
        node = Executor()
        code = "final_result = 'string' + 123"

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "error"
        assert "type" in result.lower() or "str" in result.lower()

    def test_handles_zero_division(self, sample_df):
        """Test handling of ZeroDivisionError."""
        node = Executor()
        code = "final_result = 1 / 0"

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "error"
        assert "division" in result.lower() or "zero" in result.lower()

    def test_handles_index_error(self, sample_df):
        """Test handling of IndexError."""
        node = Executor()
        code = "final_result = dfs['employees']['name'].iloc[999]"

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "error"
        assert "index" in result.lower() or "out of" in result.lower()


class TestExecutorPostMethod:
    """Test the post() method behavior."""

    def test_post_on_success(self, sample_df):
        """Test post() method when execution succeeds."""
        node = Executor()
        code = "final_result = 42"

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert action == "success"
        assert shared["csv_exec_result"] == 42
        assert "exec_error" not in shared

    def test_post_on_error(self, sample_df):
        """Test post() method when execution fails."""
        node = Executor()
        code = "x = 1 / 0"  # Will cause error

        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert action == "error"
        assert "exec_error" in shared
        assert "csv_exec_result" not in shared or shared.get("csv_exec_result") is None


class TestExecutorDataOperations:
    """Test common data operations."""

    def test_filtering_operation(self, sample_df):
        """Test filtering a DataFrame."""
        node = Executor()
        code = """
df = dfs['employees']
filtered = df[df['department'] == 'Engineering']
final_result = len(filtered)
"""
        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        assert result == 2  # Alice and Charlie

    def test_groupby_operation(self, sample_df):
        """Test groupby operation."""
        node = Executor()
        code = """
df = dfs['employees']
grouped = df.groupby('department')['salary'].mean()
final_result = grouped.to_dict()
"""
        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        assert 'Engineering' in result
        assert result['Engineering'] == (75000 + 95000) / 2

    def test_aggregation_operation(self, sample_df):
        """Test aggregation operations."""
        node = Executor()
        code = """
df = dfs['employees']
final_result = {
    'mean': df['salary'].mean(),
    'median': df['salary'].median(),
    'sum': df['salary'].sum()
}
"""
        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        assert result['mean'] == 84000
        assert result['median'] == 82000
        assert result['sum'] == 252000

    def test_merge_operation(self, sample_df, sample_sales_df):
        """Test merging DataFrames."""
        node = Executor()
        code = """
# Create a simple join scenario
df1 = dfs['employees'][['name', 'department']]
df2 = pd.DataFrame({'department': ['Engineering', 'Marketing'], 'budget': [100000, 80000]})
merged = df1.merge(df2, on='department')
final_result = len(merged)
"""
        shared = {
            "csv_code_snippet": code,
            "dfs": {"employees": sample_df}
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, result = exec_res["csv"]

        assert status == "success"
        assert result == 3  # All 3 employees should match


class TestExecutorIsolation:
    """Test that executions are isolated from each other."""

    def test_execution_isolation(self, sample_df):
        """Test that multiple executions don't interfere with each other."""
        node = Executor()

        # First execution
        code1 = "final_result = 42"
        shared1 = {"csv_code_snippet": code1, "dfs": {"employees": sample_df}}
        prep_res1 = node.prep(shared1)
        exec_res1 = node.exec(prep_res1)
        status1, result1 = exec_res1["csv"]

        # Second execution
        code2 = "final_result = 99"
        shared2 = {"csv_code_snippet": code2, "dfs": {"employees": sample_df}}
        prep_res2 = node.prep(shared2)
        exec_res2 = node.exec(prep_res2)
        status2, result2 = exec_res2["csv"]

        assert status1 == "success"
        assert result1 == 42
        assert status2 == "success"
        assert result2 == 99
