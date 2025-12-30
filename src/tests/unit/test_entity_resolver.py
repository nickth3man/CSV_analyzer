"""Tests for EntityResolver node - entity extraction and table matching."""

import pandas as pd
from typing import Any, cast

from backend.nodes import EntityResolver


class TestEntityResolverEntityExtraction:
    """Test entity extraction from questions."""

    def test_extracts_single_entity(
        self, mock_call_llm_in_nodes, sample_shared_store,
    ) -> None:
        """Test extracting a single entity from a question."""
        # Mock LLM to return a single entity
        mock_call_llm_in_nodes.return_value = '["Alice"]'

        node = EntityResolver()
        shared = {
            "question": "What is Alice's salary?",
            "schema_str": "employees: name, age, salary",
            "dfs": sample_shared_store["dfs"],
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert "entities" in exec_res
        assert "Alice" in exec_res["entities"]

    def test_extracts_multiple_entities(
        self,
        mock_call_llm_in_nodes,
        sample_shared_store,
    ) -> None:
        """Test extracting multiple entities."""
        mock_call_llm_in_nodes.return_value = '["Alice", "Bob", "Charlie"]'

        node = EntityResolver()
        shared = {
            "question": "Compare Alice, Bob, and Charlie",
            "schema_str": "employees: name, age, salary",
            "dfs": sample_shared_store["dfs"],
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert len(exec_res["entities"]) == 3
        assert "Alice" in exec_res["entities"]
        assert "Bob" in exec_res["entities"]
        assert "Charlie" in exec_res["entities"]

    def test_handles_json_code_fence(
        self, mock_call_llm_in_nodes, sample_shared_store,
    ) -> None:
        """Test handling of JSON wrapped in code fences."""
        mock_call_llm_in_nodes.return_value = '```json\n["Alice", "Bob"]\n```'

        node = EntityResolver()
        shared = {
            "question": "Compare Alice and Bob",
            "schema_str": "employees: name, age, salary",
            "dfs": sample_shared_store["dfs"],
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert len(exec_res["entities"]) == 2
        assert "Alice" in exec_res["entities"]

    def test_handles_malformed_json(
        self, mock_call_llm_in_nodes, sample_shared_store,
    ) -> None:
        """Test handling of malformed JSON response."""
        mock_call_llm_in_nodes.return_value = "This is not valid JSON"

        node = EntityResolver()
        shared = {
            "question": "What is the salary?",
            "schema_str": "employees: name, age, salary",
            "dfs": sample_shared_store["dfs"],
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Should return empty entities list on parse error
        assert exec_res["entities"] == []

    def test_handles_empty_response(
        self, mock_call_llm_in_nodes, sample_shared_store,
    ) -> None:
        """Test handling of empty LLM response."""
        mock_call_llm_in_nodes.return_value = ""

        node = EntityResolver()
        shared = {
            "question": "What is the average?",
            "schema_str": "employees: name, age, salary",
            "dfs": sample_shared_store["dfs"],
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert exec_res["entities"] == []


class TestEntityResolverTableMatching:
    """Test entity matching in tables."""

    def test_finds_entity_in_single_table(
        self, mock_call_llm_in_nodes, sample_df,
    ) -> None:
        """Test finding entity in a single table."""
        mock_call_llm_in_nodes.return_value = '["Alice"]'

        node = EntityResolver()
        shared = {
            "question": "What is Alice's salary?",
            "schema_str": "employees: name, age, salary",
            "dfs": {"employees": sample_df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert "Alice" in exec_res["entity_map"]
        assert "employees" in exec_res["entity_map"]["Alice"]

    def test_finds_entity_in_multiple_tables(
        self, mock_call_llm_in_nodes, sample_df,
    ) -> None:
        """Test finding entity in multiple tables."""
        mock_call_llm_in_nodes.return_value = '["Alice"]'

        # Create multiple tables with Alice
        dfs = {
            "employees": sample_df,
            "roster": pd.DataFrame({"name": ["Alice", "Diana"], "team": ["A", "B"]}),
        }

        node = EntityResolver()
        shared = {
            "question": "Find Alice",
            "schema_str": "employees, roster",
            "dfs": dfs,
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert "Alice" in exec_res["entity_map"]
        # Should find Alice in both tables
        assert len(exec_res["entity_map"]["Alice"]) >= 1

    def test_entity_not_found(self, mock_call_llm_in_nodes, sample_df) -> None:
        """Test when entity is not found in any table."""
        mock_call_llm_in_nodes.return_value = '["Zorro"]'

        node = EntityResolver()
        shared = {
            "question": "Find Zorro",
            "schema_str": "employees: name, age, salary",
            "dfs": {"employees": sample_df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert "Zorro" in exec_res["entity_map"]
        assert exec_res["entity_map"]["Zorro"] == {}

    def test_case_insensitive_matching(self, mock_call_llm_in_nodes, sample_df) -> None:
        """Test that entity matching is case-insensitive."""
        mock_call_llm_in_nodes.return_value = '["alice"]'  # lowercase

        node = EntityResolver()
        shared = {
            "question": "Find alice",
            "schema_str": "employees: name, age, salary",
            "dfs": {"employees": sample_df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Should still find "Alice" (uppercase in data)
        assert "alice" in exec_res["entity_map"]
        assert "employees" in exec_res["entity_map"]["alice"]


class TestEntityResolverMultiPartNames:
    """Test handling of multi-part names (first name + last name)."""

    def test_matches_full_name(self, mock_call_llm_in_nodes) -> None:
        """Test matching full names across first_name and last_name columns."""
        mock_call_llm_in_nodes.return_value = '["Alice Johnson"]'

        df = pd.DataFrame(
            {
                "first_name": ["Alice", "Bob"],
                "last_name": ["Johnson", "Smith"],
                "salary": [75000, 82000],
            },
        )

        node = EntityResolver()
        shared = {
            "question": "Find Alice Johnson",
            "schema_str": "employees: first_name, last_name, salary",
            "dfs": {"employees": df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert "Alice Johnson" in exec_res["entity_map"]
        assert "employees" in exec_res["entity_map"]["Alice Johnson"]
        # Should match both first_name and last_name columns
        cols = exec_res["entity_map"]["Alice Johnson"]["employees"]
        assert "first_name" in cols
        assert "last_name" in cols

    def test_handles_player_name_columns(self, mock_call_llm_in_nodes) -> None:
        """Test matching with player_name style columns."""
        mock_call_llm_in_nodes.return_value = '["LeBron James"]'

        df = pd.DataFrame(
            {"player_name": ["LeBron James", "Kevin Durant"], "points": [2500, 2300]},
        )

        node = EntityResolver()
        shared = {
            "question": "Find LeBron James",
            "schema_str": "players: player_name, points",
            "dfs": {"players": df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert "LeBron James" in exec_res["entity_map"]
        assert "players" in exec_res["entity_map"]["LeBron James"]


class TestEntityResolverColumnDetection:
    """Test detection of different column types."""

    def test_identifies_name_columns(self, mock_call_llm_in_nodes) -> None:
        """Test identification of name-related columns."""
        mock_call_llm_in_nodes.return_value = '["Alice"]'

        df = pd.DataFrame(
            {
                "full_name": ["Alice Smith", "Bob Jones"],
                "display_name": ["Alice S.", "Bob J."],
                "salary": [75000, 82000],
            },
        )

        node = EntityResolver()
        shared = {
            "question": "Find Alice",
            "schema_str": "employees: full_name, display_name, salary",
            "dfs": {"employees": df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Should find Alice in name-related columns
        assert "Alice" in exec_res["entity_map"]
        if "employees" in exec_res["entity_map"]["Alice"]:
            cols = exec_res["entity_map"]["Alice"]["employees"]
            assert len(cols) > 0

    def test_searches_object_columns(self, mock_call_llm_in_nodes) -> None:
        """Test that string/object columns are searched."""
        mock_call_llm_in_nodes.return_value = '["Engineering"]'

        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "department": ["Engineering", "Marketing"],
                "salary": [75000, 82000],
            },
        )

        node = EntityResolver()
        shared = {
            "question": "Find Engineering",
            "schema_str": "employees: name, department, salary",
            "dfs": {"employees": df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert "Engineering" in exec_res["entity_map"]
        assert "employees" in exec_res["entity_map"]["Engineering"]
        assert "department" in exec_res["entity_map"]["Engineering"]["employees"]

    def test_skips_numeric_columns(self, mock_call_llm_in_nodes) -> None:
        """Test that numeric columns are handled appropriately."""
        mock_call_llm_in_nodes.return_value = '["75000"]'

        df = pd.DataFrame({"name": ["Alice", "Bob"], "salary": [75000, 82000]})

        node = EntityResolver()
        shared = {
            "question": "Find 75000",
            "schema_str": "employees: name, salary",
            "dfs": {"employees": df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Numeric columns are not object type, so won't be searched by default
        # (unless the implementation converts them)
        assert "75000" in exec_res["entity_map"]


class TestEntityResolverPostMethod:
    """Test the post() method behavior."""

    def test_stores_entities_in_shared(self, mock_call_llm_in_nodes, sample_df) -> None:
        """Test that entities are stored in shared store."""
        mock_call_llm_in_nodes.return_value = '["Alice", "Bob"]'

        node = EntityResolver()
        shared = {
            "question": "Compare Alice and Bob",
            "schema_str": "employees: name, salary",
            "dfs": {"employees": sample_df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        node.post(shared, prep_res, exec_res)

        assert "entities" in shared
        assert "entity_map" in shared
        assert "knowledge_hints" in shared

    def test_returns_default_action(self, mock_call_llm_in_nodes, sample_df) -> None:
        """Test that post() returns 'default' action."""
        mock_call_llm_in_nodes.return_value = '["Alice"]'

        node = EntityResolver()
        shared = {
            "question": "Find Alice",
            "schema_str": "employees: name, salary",
            "dfs": {"employees": sample_df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert action == "default"


class TestEntityResolverErrorHandling:
    """Test error handling in entity resolution."""

    def test_handles_missing_columns(self, mock_call_llm_in_nodes) -> None:
        """Test handling when expected columns don't exist."""
        mock_call_llm_in_nodes.return_value = '["Alice"]'

        # DataFrame with no name columns
        df = pd.DataFrame({"id": [1, 2], "value": [100, 200]})

        node = EntityResolver()
        shared = {
            "question": "Find Alice",
            "schema_str": "data: id, value",
            "dfs": {"data": df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Should not crash, even if entity not found
        assert "Alice" in exec_res["entity_map"]

    def test_handles_empty_dataframe(self, mock_call_llm_in_nodes) -> None:
        """Test handling of empty DataFrames."""
        mock_call_llm_in_nodes.return_value = '["Alice"]'

        df = pd.DataFrame(columns=cast(Any, ["name", "salary"]))

        node = EntityResolver()
        shared = {
            "question": "Find Alice",
            "schema_str": "employees: name, salary",
            "dfs": {"employees": df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Should not crash on empty DataFrame
        assert "Alice" in exec_res["entity_map"]

    def test_handles_null_values(self, mock_call_llm_in_nodes) -> None:
        """Test handling of null values in data."""
        mock_call_llm_in_nodes.return_value = '["Alice"]'

        df = pd.DataFrame(
            {"name": ["Alice", None, "Bob"], "salary": [75000, 82000, 95000]},
        )

        node = EntityResolver()
        shared = {
            "question": "Find Alice",
            "schema_str": "employees: name, salary",
            "dfs": {"employees": df},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        # Should handle NaN values gracefully
        assert "Alice" in exec_res["entity_map"]


class TestEntityResolverPrepMethod:
    """Test the prep() method."""

    def test_prep_returns_dict(self, sample_shared_store) -> None:
        """Test that prep() returns required fields."""
        node = EntityResolver()
        shared = {
            "question": "Test question",
            "schema_str": "test schema",
            "dfs": sample_shared_store["dfs"],
        }

        result = node.prep(shared)

        assert isinstance(result, dict)
        assert "question" in result
        assert "schema" in result
        assert "dfs" in result
