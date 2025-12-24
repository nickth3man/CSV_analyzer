"""Tests for data processing nodes - Schema, Profiler, CodeGenerator, Visualizer."""

import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from backend.nodes import CodeGenerator, DataProfiler, SchemaInference, Visualizer


class TestSchemaInference:
    """Test SchemaInference node."""

    def test_infers_schema_from_dataframe(self, sample_df):
        """Test schema inference from a DataFrame."""
        node = SchemaInference()
        shared = {"dfs": {"employees": sample_df}}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # exec returns (schemas, csv_schema, api_schema)
        schemas = exec_res[0]
        
        assert "employees" in schemas
        # Should have identified columns
        assert "name" in str(schemas["employees"])
        assert "age" in str(schemas["employees"])
        assert "salary" in str(schemas["employees"])

    def test_stores_schema_in_shared(self, sample_df):
        """Test that schema is stored in shared store."""
        node = SchemaInference()
        shared = {"dfs": {"employees": sample_df}}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        node.post(shared, prep_res, exec_res)

        assert "schemas" in shared
        assert "schema_str" in shared

    def test_handles_multiple_dataframes(self, sample_df, sample_sales_df):
        """Test schema inference for multiple DataFrames."""
        node = SchemaInference()
        shared = {
            "dfs": {
                "employees": sample_df,
                "sales": sample_sales_df
            }
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        schemas = exec_res[0]

        assert "employees" in schemas
        assert "sales" in schemas

    def test_handles_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        node = SchemaInference()
        empty_df = pd.DataFrame(columns=["col1", "col2"])
        shared = {"dfs": {"empty": empty_df}}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        schemas = exec_res[0]

        assert "empty" in schemas


class TestDataProfiler:
    """Test DataProfiler node."""

    def test_profiles_dataframe(self, sample_df):
        """Test basic profiling of a DataFrame."""
        node = DataProfiler()
        shared = {"dfs": {"employees": sample_df}}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert "employees" in exec_res
        profile = exec_res["employees"]
        assert "row_count" in profile
        assert profile["row_count"] == 3

    def test_identifies_numeric_columns(self, sample_df):
        """Test identification of numeric columns."""
        node = DataProfiler()
        shared = {"dfs": {"employees": sample_df}}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        profile = exec_res["employees"]
        assert "numeric_cols" in profile
        # age and salary should be numeric
        assert len(profile["numeric_cols"]) >= 2

    def test_identifies_name_columns(self, sample_df):
        """Test identification of name-related columns."""
        node = DataProfiler()
        shared = {"dfs": {"employees": sample_df}}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        profile = exec_res["employees"]
        assert "name_cols" in profile
        # Should find the 'name' column
        assert len(profile["name_cols"]) >= 1

    def test_stores_profiles_in_shared(self, sample_df):
        """Test that profiles are stored in shared store."""
        node = DataProfiler()
        shared = {"dfs": {"employees": sample_df}}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        node.post(shared, prep_res, exec_res)

        assert "profiles" in shared
        assert "employees" in shared["profiles"]


class TestCodeGenerator:
    """Test CodeGenerator node."""

    def test_generates_code(self, mock_call_llm_in_nodes, sample_shared_store):
        """Test basic code generation."""
        # Mock LLM to return Python code
        mock_call_llm_in_nodes.return_value = """```python
final_result = dfs['employees']['salary'].mean()
```"""

        node = CodeGenerator()
        shared = {
            "plan_steps": "Calculate average salary",
            "schema_str": "employees: name, age, salary",
            "entity_map": {},
            "knowledge_hints": {},
            "exec_error": None
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert "final_result" in exec_res
        assert "dfs" in exec_res

    def test_strips_markdown_code_fences(self, mock_call_llm_in_nodes, sample_shared_store):
        """Test that markdown code fences are removed."""
        mock_call_llm_in_nodes.return_value = """```python
final_result = 42
```"""

        node = CodeGenerator()
        shared = {
            "plan_steps": "Test",
            "schema_str": "test",
            "entity_map": {},
            "knowledge_hints": {},
            "exec_error": None
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Should not contain the backticks
        assert "```" not in exec_res

    def test_handles_error_context(self, mock_call_llm_in_nodes):
        """Test code generation with error context for fixing."""
        mock_call_llm_in_nodes.return_value = """```python
# Fixed code
final_result = dfs['employees']['salary'].mean()
```"""

        node = CodeGenerator()
        shared = {
            "plan_steps": "Calculate average",
            "schema_str": "employees: name, age, salary",
            "entity_map": {},
            "knowledge_hints": {},
            "exec_error": "NameError: name 'x' is not defined",
            "code_snippet": "final_result = x"
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Should generate corrected code
        assert exec_res is not None

    def test_stores_code_in_shared(self, mock_call_llm_in_nodes):
        """Test that generated code is stored in shared."""
        mock_call_llm_in_nodes.return_value = "final_result = 42"

        node = CodeGenerator()
        shared = {
            "plan_steps": "Test",
            "schema_str": "test",
            "entity_map": {},
            "knowledge_hints": {},
            "exec_error": None
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        node.post(shared, prep_res, exec_res)

        assert "csv_code_snippet" in shared


class TestVisualizer:
    """Test Visualizer node."""

    def test_generates_chart_for_dataframe(self, sample_df, mock_matplotlib):
        """Test chart generation for DataFrame result."""
        node = Visualizer()
        shared = {"exec_result": sample_df}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Should have called savefig
        mock_matplotlib["savefig"].assert_called_once()
        # Should return a path
        assert exec_res is not None
        assert "chart_" in exec_res

    def test_skips_non_dataframe_results(self, mock_matplotlib):
        """Test that non-DataFrame results are skipped."""
        node = Visualizer()
        shared = {"exec_result": 42}  # Scalar, not DataFrame

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Should return None for non-DataFrame
        assert exec_res is None
        # Should not try to save a chart
        mock_matplotlib["savefig"].assert_not_called()

    def test_skips_dataframe_without_numeric_columns(self, mock_matplotlib):
        """Test that DataFrames without numeric columns are skipped."""
        node = Visualizer()
        df = pd.DataFrame({"name": ["Alice", "Bob"], "city": ["NYC", "LA"]})
        shared = {"exec_result": df}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Should return None when no numeric columns
        assert exec_res is None

    def test_creates_output_directory(self, sample_df, mock_matplotlib, tmp_path):
        """Test that output directory is created if it doesn't exist."""
        node = Visualizer()

        with patch("backend.nodes.analysis.os.makedirs") as mock_makedirs:
            shared = {"exec_result": sample_df}
            node.prep(shared)
            node.exec(sample_df)

            # Should try to create the assets directory
            mock_makedirs.assert_called_once()

    def test_limits_chart_files_to_ten(self, sample_df, mock_matplotlib, tmp_path):
        """Test that old chart files are cleaned up (keep last 10)."""
        # Create the assets directory with mock files
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir()

        # Create 15 old chart files
        for i in range(15):
            chart_file = assets_dir / f"chart_{i}.png"
            chart_file.write_text("fake chart")

        with patch("backend.nodes.analysis.os.makedirs"):
            with patch("backend.nodes.analysis.os.listdir", return_value=[f"chart_{i}.png" for i in range(15)]):
                with patch("backend.nodes.analysis.os.path.getmtime", side_effect=lambda x: int(x.split("_")[1].split(".")[0])):
                    with patch("backend.nodes.analysis.os.remove") as mock_remove:
                        node = Visualizer()
                        shared = {"exec_result": sample_df}
                        node.prep(shared)
                        node.exec(sample_df)

                        # Should have removed 5 oldest files (15 - 10 = 5)
                        assert mock_remove.call_count == 5

    def test_stores_chart_path_in_shared(self, sample_df, mock_matplotlib):
        """Test that chart path is stored in shared."""
        node = Visualizer()
        shared = {"exec_result": sample_df}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        node.post(shared, prep_res, exec_res)

        assert "chart_path" in shared

    def test_handles_none_result(self):
        """Test handling when exec_result is None."""
        node = Visualizer()
        shared = {"exec_result": None}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert exec_res is None

    def test_uses_timestamp_in_filename(self, sample_df, mock_matplotlib):
        """Test that chart filename includes timestamp."""
        node = Visualizer()
        shared = {"exec_result": sample_df}

        with patch("backend.nodes.analysis.time.time", return_value=1234567890):
            prep_res = node.prep(shared)
            exec_res = node.exec(prep_res)

            assert "1234567890" in exec_res

    def test_plots_first_numeric_column(self, mock_matplotlib):
        """Test that the first numeric column is plotted."""
        node = Visualizer()
        df = pd.DataFrame({
            "name": ["A", "B", "C"],
            "score": [10, 20, 30],
            "value": [100, 200, 300]
        })
        shared = {"exec_result": df}

        prep_res = node.prep(shared)
        with patch("matplotlib.pyplot.figure"):
            with patch("matplotlib.pyplot.title") as mock_title:
                exec_res = node.exec(prep_res)

                # Should use the first numeric column (score)
                # The title should mention the column name
                if mock_title.called:
                    title_arg = str(mock_title.call_args)
                    assert "score" in title_arg or "Top 10" in title_arg

    def test_limits_to_top_10_rows(self, mock_matplotlib):
        """Test that only top 10 rows are plotted."""
        node = Visualizer()
        # Create DataFrame with 20 rows
        df = pd.DataFrame({"value": range(20)})
        shared = {"exec_result": df}

        with patch("pandas.DataFrame.head") as mock_head:
            mock_head.return_value = df.head(10)
            prep_res = node.prep(shared)
            exec_res = node.exec(prep_res)

            # Should call head(10)
            mock_head.assert_called_with(10)


class TestDataProcessingIntegration:
    """Integration tests for data processing pipeline."""

    def test_schema_to_profiler_pipeline(self, sample_df):
        """Test SchemaInference -> DataProfiler pipeline."""
        shared = {"dfs": {"employees": sample_df}}

        # Run SchemaInference
        schema_node = SchemaInference()
        prep_res = schema_node.prep(shared)
        exec_res = schema_node.exec(prep_res)
        schema_node.post(shared, prep_res, exec_res)

        # Run DataProfiler (uses the schema)
        profiler_node = DataProfiler()
        prep_res = profiler_node.prep(shared)
        exec_res = profiler_node.exec(prep_res)
        profiler_node.post(shared, prep_res, exec_res)

        # Both should have completed successfully
        assert "schemas" in shared
        assert "profiles" in shared

    def test_full_data_processing_pipeline(self, mock_call_llm_in_nodes, sample_df, mock_matplotlib):
        """Test full pipeline: Schema -> Profiler -> CodeGen -> Visualizer."""
        mock_call_llm_in_nodes.return_value = """```python
final_result = dfs['employees']
```"""

        shared = {"dfs": {"employees": sample_df}}

        # SchemaInference
        schema_node = SchemaInference()
        prep_res = schema_node.prep(shared)
        exec_res = schema_node.exec(prep_res)
        schema_node.post(shared, prep_res, exec_res)

        # DataProfiler
        profiler_node = DataProfiler()
        prep_res = profiler_node.prep(shared)
        exec_res = profiler_node.exec(prep_res)
        profiler_node.post(shared, prep_res, exec_res)

        # CodeGenerator (needs more context)
        shared["plan_steps"] = "Return the employees table"
        shared["entity_map"] = {}
        shared["knowledge_hints"] = {}
        shared["exec_error"] = None

        codegen_node = CodeGenerator()
        prep_res = codegen_node.prep(shared)
        exec_res = codegen_node.exec(prep_res)
        codegen_node.post(shared, prep_res, exec_res)

        # Execute the code (simulated)
        shared["exec_result"] = sample_df

        # Visualizer
        viz_node = Visualizer()
        prep_res = viz_node.prep(shared)
        exec_res = viz_node.exec(prep_res)
        viz_node.post(shared, prep_res, exec_res)

        # All nodes should have completed
        assert "schemas" in shared
        assert "profiles" in shared
        assert "csv_code_snippet" in shared
        assert "chart_path" in shared or shared["chart_path"] is None
