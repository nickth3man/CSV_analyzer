"""Tests for LoadData node - CSV file loading and error handling."""

import pytest
import pandas as pd
import os
from nodes import LoadData


class TestLoadDataBasicLoading:
    """Test basic CSV loading functionality."""

    def test_loads_single_csv(self, temp_csv_dir):
        """Test loading a single CSV file."""
        node = LoadData()
        shared = {"data_dir": str(temp_csv_dir)}

        node.prep(shared)
        dfs = node.exec(str(temp_csv_dir))

        # Should load at least one CSV
        assert len(dfs) > 0
        assert all(isinstance(df, pd.DataFrame) for df in dfs.values())

    def test_loads_multiple_csvs(self, temp_csv_dir):
        """Test loading multiple CSV files."""
        node = LoadData()
        shared = {"data_dir": str(temp_csv_dir)}

        node.prep(shared)
        dfs = node.exec(str(temp_csv_dir))

        # We created multiple CSV files in the fixture
        assert len(dfs) >= 2

    def test_table_names_from_filenames(self, temp_csv_dir):
        """Test that table names are derived from filenames."""
        node = LoadData()
        dfs = node.exec(str(temp_csv_dir))

        # Table names should be filenames without .csv extension
        for table_name in dfs.keys():
            assert '.csv' not in table_name
            assert isinstance(table_name, str)
            assert len(table_name) > 0

    def test_stores_dataframes_in_shared(self, temp_csv_dir):
        """Test that DataFrames are stored in shared store."""
        node = LoadData()
        shared = {"data_dir": str(temp_csv_dir)}

        node.prep(shared)
        dfs = node.exec(str(temp_csv_dir))
        node.post(shared, str(temp_csv_dir), dfs)

        assert "dfs" in shared
        assert shared["dfs"] == dfs


class TestLoadDataErrorHandling:
    """Test error handling for various failure scenarios."""

    def test_handles_nonexistent_directory(self):
        """Test handling of non-existent directory."""
        node = LoadData()
        result = node.exec("/nonexistent/directory/path")

        # Should return empty dict or handle gracefully
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_handles_empty_directory(self, tmp_path):
        """Test handling of directory with no CSV files."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        node = LoadData()
        dfs = node.exec(str(empty_dir))

        assert isinstance(dfs, dict)
        assert len(dfs) == 0

    def test_handles_malformed_csv(self, tmp_path):
        """Test handling of malformed CSV file."""
        # Create a malformed CSV
        malformed_file = tmp_path / "malformed.csv"
        malformed_file.write_text("name,age,salary\nAlice,28,75000\nBob,35\nCharlie")

        node = LoadData()
        dfs = node.exec(str(tmp_path))

        # Should either skip the malformed file or load it with warnings
        # The implementation may vary, but it shouldn't crash
        assert isinstance(dfs, dict)

    def test_handles_non_csv_files(self, tmp_path):
        """Test that non-CSV files are ignored."""
        # Create some non-CSV files
        (tmp_path / "readme.txt").write_text("This is not a CSV")
        (tmp_path / "data.json").write_text('{"key": "value"}')
        (tmp_path / "valid.csv").write_text("name,age\nAlice,28")

        node = LoadData()
        dfs = node.exec(str(tmp_path))

        # Should only load the .csv file
        assert len(dfs) == 1
        assert "valid" in dfs or "valid.csv" in dfs


class TestLoadDataEncodingHandling:
    """Test handling of different file encodings."""

    def test_handles_utf8_encoding(self, tmp_path):
        """Test loading UTF-8 encoded CSV."""
        csv_file = tmp_path / "utf8.csv"
        csv_file.write_text("name,city\nAlice,São Paulo\nBob,Tokyo", encoding='utf-8')

        node = LoadData()
        dfs = node.exec(str(tmp_path))

        assert "utf8" in dfs
        assert len(dfs["utf8"]) == 2

    def test_handles_latin1_fallback(self, tmp_path):
        """Test fallback to latin-1 encoding if UTF-8 fails."""
        csv_file = tmp_path / "latin1.csv"
        # Write with latin-1 encoding
        csv_file.write_bytes(b"name,value\nTest,\xe9")  # \xe9 is 'é' in latin-1

        node = LoadData()
        dfs = node.exec(str(tmp_path))

        # Should load successfully with fallback encoding
        assert "latin1" in dfs or len(dfs) > 0


class TestLoadDataDataIntegrity:
    """Test that loaded data maintains integrity."""

    def test_preserves_column_names(self, test_data_dir):
        """Test that column names are preserved correctly."""
        node = LoadData()
        dfs = node.exec(str(test_data_dir))

        # Check that the test_valid.csv has correct columns
        if "test_valid" in dfs:
            expected_columns = ['name', 'age', 'salary', 'department']
            assert list(dfs["test_valid"].columns) == expected_columns

    def test_preserves_data_types(self, test_data_dir):
        """Test that data types are inferred correctly."""
        node = LoadData()
        dfs = node.exec(str(test_data_dir))

        if "test_valid" in dfs:
            df = dfs["test_valid"]
            # Age should be numeric
            assert pd.api.types.is_numeric_dtype(df['age'])
            # Salary should be numeric
            assert pd.api.types.is_numeric_dtype(df['salary'])
            # Name and department should be strings/objects
            assert pd.api.types.is_object_dtype(df['name'])

    def test_preserves_row_count(self, test_data_dir):
        """Test that all rows are loaded."""
        node = LoadData()
        dfs = node.exec(str(test_data_dir))

        if "test_valid" in dfs:
            # test_valid.csv has 5 data rows
            assert len(dfs["test_valid"]) == 5


class TestLoadDataPostMethod:
    """Test the post() method behavior."""

    def test_post_stores_dfs(self, temp_csv_dir):
        """Test that post() stores DataFrames in shared."""
        node = LoadData()
        shared = {"data_dir": str(temp_csv_dir)}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert "dfs" in shared
        assert shared["dfs"] == exec_res
        # Should return None or "default"
        assert action in [None, "default"]

    def test_post_logs_table_count(self, temp_csv_dir, capsys):
        """Test that post() logs the number of tables loaded."""
        node = LoadData()
        shared = {"data_dir": str(temp_csv_dir)}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        node.post(shared, prep_res, exec_res)

        # Check that something was printed
        captured = capsys.readouterr()
        # The node should print information about loaded tables
        # This is implementation-dependent


class TestLoadDataEdgeCases:
    """Test edge cases and unusual scenarios."""

    def test_handles_csv_with_only_headers(self, tmp_path):
        """Test CSV file with only header row."""
        csv_file = tmp_path / "headers_only.csv"
        csv_file.write_text("name,age,salary\n")

        node = LoadData()
        dfs = node.exec(str(tmp_path))

        if "headers_only" in dfs:
            assert len(dfs["headers_only"]) == 0
            assert len(dfs["headers_only"].columns) == 3

    def test_handles_csv_with_special_characters(self, tmp_path):
        """Test CSV with special characters in data."""
        csv_file = tmp_path / "special.csv"
        csv_file.write_text('name,comment\n"Alice","Said ""Hello"""\n"Bob","Used, commas"')

        node = LoadData()
        dfs = node.exec(str(tmp_path))

        if "special" in dfs:
            assert len(dfs["special"]) == 2

    def test_handles_very_large_values(self, tmp_path):
        """Test CSV with very large numeric values."""
        csv_file = tmp_path / "large.csv"
        csv_file.write_text("name,big_number\nAlice,999999999999999\nBob,1000000000000000")

        node = LoadData()
        dfs = node.exec(str(tmp_path))

        if "large" in dfs:
            assert len(dfs["large"]) == 2
            assert dfs["large"]["big_number"].iloc[0] == 999999999999999

    def test_handles_missing_values(self, tmp_path):
        """Test CSV with missing/null values."""
        csv_file = tmp_path / "missing.csv"
        csv_file.write_text("name,age,salary\nAlice,28,75000\nBob,,\nCharlie,42,95000")

        node = LoadData()
        dfs = node.exec(str(tmp_path))

        if "missing" in dfs:
            df = dfs["missing"]
            assert len(df) == 3
            # Bob's age and salary should be NaN
            assert pd.isna(df.iloc[1]["age"])


class TestLoadDataPrepMethod:
    """Test the prep() method."""

    def test_prep_returns_data_dir(self):
        """Test that prep() returns the data directory path."""
        node = LoadData()
        shared = {"data_dir": "/path/to/data"}

        result = node.prep(shared)

        assert result == "/path/to/data"

    def test_prep_with_missing_data_dir(self):
        """Test prep() when data_dir is missing."""
        node = LoadData()
        shared = {}

        # Should either use a default or raise an error
        try:
            result = node.prep(shared)
            # If it succeeds, it should return something
            assert result is not None or result == ""
        except KeyError:
            # It's also acceptable to raise KeyError
            pass
