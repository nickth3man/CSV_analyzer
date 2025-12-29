"""Persistent knowledge store for learned patterns and entity mappings.

# TODO (Scalability): Replace JSON file with proper database
# Current JSON-based storage won't scale for:
#   - Large knowledge bases (100k+ patterns)
#   - Concurrent write access (race conditions possible)
#   - Query performance (full file scan for every lookup)
# Recommended approach:
#   1. SQLite for single-user deployments:
#      import sqlite3
#      conn = sqlite3.connect("knowledge_store.db")
#      cursor.execute("CREATE TABLE IF NOT EXISTS patterns ...")
#   2. PostgreSQL/Redis for multi-user deployments
#   3. Consider vector database for semantic similarity search

# TODO (Scalability): Add knowledge store sharding by entity type
# As the store grows, partition data for faster lookups:
#   knowledge_store/
#   ├── players.json
#   ├── teams.json
#   ├── patterns.json
#   └── index.json

# TODO (Reliability): Add atomic writes with backup
# Current save() can corrupt data if interrupted. Use atomic write:
#   def save_atomic(self):
#       temp_path = KNOWLEDGE_FILE + ".tmp"
#       with open(temp_path, "w") as f:
#           json.dump(self.data, f)
#       os.replace(temp_path, KNOWLEDGE_FILE)  # Atomic on POSIX
# Also keep a backup: knowledge_store.json.bak

# TODO (Feature): Add knowledge expiration and pruning
# Old patterns may become stale. Add TTL and cleanup:
#   def add_pattern(self, pattern):
#       pattern["created_at"] = time.time()
#       pattern["last_used"] = time.time()
#   def prune_stale(self, max_age_days=90):
#       cutoff = time.time() - (max_age_days * 86400)
#       self.data["patterns"] = [p for p in patterns if p["last_used"] > cutoff]

# TODO (Feature): Add semantic similarity for entity matching
# Current matching is exact/substring. Use embeddings for better matching:
#   from sentence_transformers import SentenceTransformer
#   model = SentenceTransformer('all-MiniLM-L6-v2')
#   def find_similar_entities(self, query):
#       query_emb = model.encode(query)
#       # Compare with stored entity embeddings
#       similarities = cosine_similarity(query_emb, stored_embeddings)

# TODO (Monitoring): Add knowledge store metrics
# Track usage for optimization:
#   - Cache hit/miss ratio
#   - Pattern reuse frequency
#   - Store size over time
#   - Most frequently accessed entities
"""

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

from backend.config import SUCCESSFUL_PATTERN_LIMIT


logger = logging.getLogger(__name__)

KNOWLEDGE_FILE = str(
    Path(__file__).resolve().parents[1] / "data" / "json" / "knowledge_store.json",
)


class KnowledgeStore:
    """Manages persistence and retrieval of knowledge patterns."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {
            "entity_mappings": {},
            "successful_patterns": {},
            "column_hints": {},
            "join_patterns": [],
        }
        self._lock = threading.Lock()
        self.load()

    def load(self) -> None:
        with self._lock:
            if os.path.exists(KNOWLEDGE_FILE):
                try:
                    with open(KNOWLEDGE_FILE) as f:
                        self.data = json.load(f)
                except (OSError, json.JSONDecodeError):
                    pass

    def save(self) -> None:
        with self._lock:
            try:
                Path(KNOWLEDGE_FILE).parent.mkdir(parents=True, exist_ok=True)
                with open(KNOWLEDGE_FILE, "w") as f:
                    json.dump(self.data, f, indent=2)
            except OSError as e:
                logger.warning(f"Warning: Could not save knowledge store: {e}")

    def get_entity_hints(self, entity_name):
        with self._lock:
            entity_lower = entity_name.lower()
            return {
                key: value
                for key, value in self.data.get("entity_mappings", {}).items()
                if entity_lower in key.lower() or key.lower() in entity_lower
            }

    def add_entity_mapping(self, entity, table, columns) -> None:
        with self._lock:
            if entity not in self.data["entity_mappings"]:
                self.data["entity_mappings"][entity] = {}
            if table not in self.data["entity_mappings"][entity]:
                self.data["entity_mappings"][entity][table] = []
            for col in columns:
                if col not in self.data["entity_mappings"][entity][table]:
                    self.data["entity_mappings"][entity][table].append(col)
        self.save()

    def get_successful_patterns(self, query_type=None):
        with self._lock:
            patterns = self.data.get("successful_patterns", {})
            if query_type:
                return patterns.get(query_type, [])
            return patterns

    def add_successful_pattern(self, query_type, pattern) -> None:
        """Store a successful pattern under the given query type and persist the updated list.

        Adds the pattern to the in-memory list for query_type if not already present, trims the list to the most recent SUCCESSFUL_PATTERN_LIMIT entries when it exceeds that limit, and then saves the store to disk.

        Parameters:
            query_type (str): Category or type of query to associate the pattern with.
            pattern: Representation of the successful pattern to record (e.g., string or serializable object).
        """
        with self._lock:
            if query_type not in self.data["successful_patterns"]:
                self.data["successful_patterns"][query_type] = []
            if pattern not in self.data["successful_patterns"][query_type]:
                self.data["successful_patterns"][query_type].append(pattern)
                if (
                    len(self.data["successful_patterns"][query_type])
                    > SUCCESSFUL_PATTERN_LIMIT
                ):
                    self.data["successful_patterns"][query_type] = self.data[
                        "successful_patterns"
                    ][query_type][-SUCCESSFUL_PATTERN_LIMIT:]
        self.save()

    def add_column_hint(self, description, table, column) -> None:
        """Store a column hint in the knowledge store keyed by a lowercase description and persist the change.

        Parameters:
            description (str): Human-readable description used as the lookup key; it is normalized to lowercase.
            table (str): Name of the table associated with the hint.
            column (str): Name of the column associated with the hint.
        """
        with self._lock:
            key = description.lower()
            self.data["column_hints"][key] = {"table": table, "column": column}
        self.save()

    def get_column_hints(self):
        with self._lock:
            return self.data.get("column_hints", {})

    def add_join_pattern(self, tables, join_keys) -> None:
        with self._lock:
            pattern = {"tables": sorted(tables), "keys": join_keys}
            if pattern not in self.data["join_patterns"]:
                self.data["join_patterns"].append(pattern)
        self.save()

    def get_join_patterns(self):
        with self._lock:
            return self.data.get("join_patterns", [])

    def get_all_hints(self):
        with self._lock:
            return {
                "entity_mappings": self.data.get("entity_mappings", {}),
                "column_hints": self.data.get("column_hints", {}),
                "join_patterns": self.data.get("join_patterns", []),
                "note": "These are hints from previous queries. Use as guidance, not absolute facts.",
            }


knowledge_store = KnowledgeStore()
