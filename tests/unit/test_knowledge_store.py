"""Tests for KnowledgeStore - persistent learning storage."""

import json
import os
import threading

from backend.utils.knowledge_store import KnowledgeStore


class TestKnowledgeStoreInitialization:
    """Test KnowledgeStore initialization."""

    def test_initializes_with_empty_data(self, temp_knowledge_file) -> None:
        """Test that KnowledgeStore initializes with empty data structure."""
        # Override the global KNOWLEDGE_FILE for this test
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()

            assert "entity_mappings" in store.data
            assert "successful_patterns" in store.data
            assert "column_hints" in store.data
            assert "join_patterns" in store.data

            assert isinstance(store.data["entity_mappings"], dict)
            assert isinstance(store.data["successful_patterns"], dict)
            assert isinstance(store.data["column_hints"], dict)
            assert isinstance(store.data["join_patterns"], list)
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_loads_existing_data(self, temp_knowledge_file) -> None:
        """Test loading existing knowledge from file."""
        # Create a file with existing data
        existing_data = {
            "entity_mappings": {"Alice": {"employees": ["name"]}},
            "successful_patterns": {"comparison": ["pattern1"]},
            "column_hints": {"salary": {"table": "employees", "column": "salary"}},
            "join_patterns": [{"tables": ["a", "b"], "keys": ["id"]}],
        }

        with open(temp_knowledge_file, "w") as f:
            json.dump(existing_data, f)

        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()

            assert "Alice" in store.data["entity_mappings"]
            assert "comparison" in store.data["successful_patterns"]
            assert "salary" in store.data["column_hints"]
            assert len(store.data["join_patterns"]) == 1
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_handles_corrupted_json(self, temp_knowledge_file) -> None:
        """Test handling of corrupted JSON file."""
        # Write invalid JSON
        with open(temp_knowledge_file, "w") as f:
            f.write("This is not valid JSON {{{")

        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()

            # Should initialize with empty data instead of crashing
            assert store.data["entity_mappings"] == {}
            assert store.data["successful_patterns"] == {}
        finally:
            ks_module.KNOWLEDGE_FILE = original_file


class TestKnowledgeStoreEntityMappings:
    """Test entity mapping functionality."""

    def test_adds_entity_mapping(self, temp_knowledge_file) -> None:
        """Test adding a new entity mapping."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_entity_mapping("Alice", "employees", ["name", "id"])

            assert "Alice" in store.data["entity_mappings"]
            assert "employees" in store.data["entity_mappings"]["Alice"]
            assert "name" in store.data["entity_mappings"]["Alice"]["employees"]
            assert "id" in store.data["entity_mappings"]["Alice"]["employees"]
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_avoids_duplicate_columns(self, temp_knowledge_file) -> None:
        """Test that duplicate columns are not added."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_entity_mapping("Alice", "employees", ["name"])
            store.add_entity_mapping("Alice", "employees", ["name"])  # Duplicate

            assert (
                store.data["entity_mappings"]["Alice"]["employees"].count("name") == 1
            )
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_adds_multiple_tables_for_entity(self, temp_knowledge_file) -> None:
        """Test adding same entity to multiple tables."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_entity_mapping("Alice", "employees", ["name"])
            store.add_entity_mapping("Alice", "roster", ["player_name"])

            assert "employees" in store.data["entity_mappings"]["Alice"]
            assert "roster" in store.data["entity_mappings"]["Alice"]
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_get_entity_hints(self, temp_knowledge_file) -> None:
        """Test retrieving entity hints."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_entity_mapping("Alice Johnson", "employees", ["name"])
            store.add_entity_mapping("Bob Smith", "employees", ["name"])

            # Should match case-insensitively
            hints = store.get_entity_hints("alice")
            assert "Alice Johnson" in hints

            # Should match partial names
            hints = store.get_entity_hints("Johnson")
            assert "Alice Johnson" in hints
        finally:
            ks_module.KNOWLEDGE_FILE = original_file


class TestKnowledgeStoreSuccessfulPatterns:
    """Test successful pattern storage."""

    def test_adds_successful_pattern(self, temp_knowledge_file) -> None:
        """Test adding a successful query pattern."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_successful_pattern(
                "comparison",
                "Compare X and Y using table.col",
            )

            assert "comparison" in store.data["successful_patterns"]
            assert (
                "Compare X and Y using table.col"
                in store.data["successful_patterns"]["comparison"]
            )
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_avoids_duplicate_patterns(self, temp_knowledge_file) -> None:
        """Test that duplicate patterns are not added."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            pattern = "Compare X and Y"
            store.add_successful_pattern("comparison", pattern)
            store.add_successful_pattern("comparison", pattern)  # Duplicate

            assert store.data["successful_patterns"]["comparison"].count(pattern) == 1
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_limits_pattern_count(self, temp_knowledge_file) -> None:
        """Test that patterns are limited to 10 per type."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()

            # Add 15 patterns
            for i in range(15):
                store.add_successful_pattern("test", f"pattern_{i}")

            # Should keep only last 10
            assert len(store.data["successful_patterns"]["test"]) == 10
            # Should have patterns 5-14 (last 10)
            assert "pattern_14" in store.data["successful_patterns"]["test"]
            assert "pattern_4" not in store.data["successful_patterns"]["test"]
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_get_patterns_by_type(self, temp_knowledge_file) -> None:
        """Test retrieving patterns by query type."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_successful_pattern("comparison", "pattern1")
            store.add_successful_pattern("aggregation", "pattern2")

            comparison_patterns = store.get_successful_patterns("comparison")
            assert "pattern1" in comparison_patterns
            assert "pattern2" not in comparison_patterns
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_get_all_patterns(self, temp_knowledge_file) -> None:
        """Test retrieving all patterns."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_successful_pattern("comparison", "pattern1")
            store.add_successful_pattern("aggregation", "pattern2")

            all_patterns = store.get_successful_patterns()
            assert "comparison" in all_patterns
            assert "aggregation" in all_patterns
        finally:
            ks_module.KNOWLEDGE_FILE = original_file


class TestKnowledgeStoreColumnHints:
    """Test column hint functionality."""

    def test_adds_column_hint(self, temp_knowledge_file) -> None:
        """Test adding a column hint."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_column_hint("total revenue", "sales", "revenue")

            hints = store.get_column_hints()
            assert "total revenue" in hints
            assert hints["total revenue"]["table"] == "sales"
            assert hints["total revenue"]["column"] == "revenue"
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_case_insensitive_hint_storage(self, temp_knowledge_file) -> None:
        """Test that hints are stored case-insensitively."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_column_hint("Total Revenue", "sales", "revenue")

            hints = store.get_column_hints()
            assert "total revenue" in hints  # Stored in lowercase
        finally:
            ks_module.KNOWLEDGE_FILE = original_file


class TestKnowledgeStoreJoinPatterns:
    """Test join pattern functionality."""

    def test_adds_join_pattern(self, temp_knowledge_file) -> None:
        """Test adding a join pattern."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_join_pattern(["employees", "departments"], ["dept_id"])

            patterns = store.get_join_patterns()
            assert len(patterns) == 1
            assert set(patterns[0]["tables"]) == {"departments", "employees"}
            assert patterns[0]["keys"] == ["dept_id"]
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_avoids_duplicate_join_patterns(self, temp_knowledge_file) -> None:
        """Test that duplicate join patterns are not added."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_join_pattern(["employees", "departments"], ["dept_id"])
            store.add_join_pattern(
                ["departments", "employees"],
                ["dept_id"],
            )  # Same but different order

            patterns = store.get_join_patterns()
            # Tables are sorted, so should be treated as duplicate
            assert len(patterns) == 1
        finally:
            ks_module.KNOWLEDGE_FILE = original_file


class TestKnowledgeStorePersistence:
    """Test data persistence functionality."""

    def test_saves_to_file(self, temp_knowledge_file) -> None:
        """Verifies that adding an entity mapping causes the store to persist data to the configured knowledge file.

        This test temporarily points the store's KNOWLEDGE_FILE at a provided temporary file, adds an entity mapping, and asserts the file is created and contains the new entity mapping.

        Parameters:
            temp_knowledge_file (str | pathlib.Path): Temporary filesystem path used as the knowledge file for this test.
        """
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_entity_mapping("Alice", "employees", ["name"])

            # Check that file was created and contains data
            assert os.path.exists(temp_knowledge_file)

            with open(temp_knowledge_file) as f:
                saved_data = json.load(f)

            assert "Alice" in saved_data["entity_mappings"]
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_loads_from_file(self, temp_knowledge_file) -> None:
        """Test that data persists across instances."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            # First instance
            store1 = KnowledgeStore()
            store1.add_entity_mapping("Alice", "employees", ["name"])

            # Second instance should load the same data
            store2 = KnowledgeStore()
            assert "Alice" in store2.data["entity_mappings"]
        finally:
            ks_module.KNOWLEDGE_FILE = original_file


class TestKnowledgeStoreThreadSafety:
    """Test thread-safe operations."""

    def test_concurrent_writes(self, temp_knowledge_file) -> None:
        """Test concurrent writes don't corrupt data."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()

            def add_entities(prefix, count) -> None:
                for i in range(count):
                    store.add_entity_mapping(f"{prefix}_{i}", "table", ["col"])

            # Create multiple threads writing concurrently
            threads = []
            for i in range(5):
                t = threading.Thread(target=add_entities, args=(f"entity{i}", 10))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            # Should have added all entities without corruption
            assert len(store.data["entity_mappings"]) == 50
        finally:
            ks_module.KNOWLEDGE_FILE = original_file

    def test_concurrent_reads_and_writes(self, temp_knowledge_file) -> None:
        """Test concurrent reads and writes."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_entity_mapping("Alice", "table", ["col"])

            results = []

            def read_entities() -> None:
                for _ in range(100):
                    hints = store.get_entity_hints("Alice")
                    results.append(hints)

            def write_entities() -> None:
                for i in range(100):
                    store.add_entity_mapping(f"Entity_{i}", "table", ["col"])

            # Run concurrent reads and writes
            t1 = threading.Thread(target=read_entities)
            t2 = threading.Thread(target=write_entities)

            t1.start()
            t2.start()

            t1.join()
            t2.join()

            # Should complete without errors
            assert len(results) == 100
        finally:
            ks_module.KNOWLEDGE_FILE = original_file


class TestKnowledgeStoreGetAllHints:
    """Test get_all_hints functionality."""

    def test_returns_all_hints(self, temp_knowledge_file) -> None:
        """Test that get_all_hints returns all stored data."""
        from backend.utils import knowledge_store as ks_module

        original_file = ks_module.KNOWLEDGE_FILE
        ks_module.KNOWLEDGE_FILE = str(temp_knowledge_file)

        try:
            store = KnowledgeStore()
            store.add_entity_mapping("Alice", "employees", ["name"])
            store.add_column_hint("revenue", "sales", "total")
            store.add_join_pattern(["a", "b"], ["id"])

            hints = store.get_all_hints()

            assert "entity_mappings" in hints
            assert "column_hints" in hints
            assert "join_patterns" in hints
            assert "note" in hints

            assert "Alice" in hints["entity_mappings"]
            assert "revenue" in hints["column_hints"]
            assert len(hints["join_patterns"]) == 1
        finally:
            ks_module.KNOWLEDGE_FILE = original_file
