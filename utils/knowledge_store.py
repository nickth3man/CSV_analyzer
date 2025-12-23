import json
import os
import threading

KNOWLEDGE_FILE = "knowledge_store.json"

class KnowledgeStore:
    def __init__(self):
        self.data = {
            "entity_mappings": {},
            "successful_patterns": {},
            "column_hints": {},
            "join_patterns": []
        }
        self._lock = threading.Lock()
        self.load()
    
    def load(self):
        with self._lock:
            if os.path.exists(KNOWLEDGE_FILE):
                try:
                    with open(KNOWLEDGE_FILE, 'r') as f:
                        self.data = json.load(f)
                except (json.JSONDecodeError, IOError):
                    pass

    def save(self):
        with self._lock:
            try:
                with open(KNOWLEDGE_FILE, 'w') as f:
                    json.dump(self.data, f, indent=2)
            except IOError as e:
                print(f"Warning: Could not save knowledge store: {e}")
    
    def get_entity_hints(self, entity_name):
        with self._lock:
            entity_lower = entity_name.lower()
            hints = {}
            for key, value in self.data.get("entity_mappings", {}).items():
                if entity_lower in key.lower() or key.lower() in entity_lower:
                    hints[key] = value
            return hints

    def add_entity_mapping(self, entity, table, columns):
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

    def add_successful_pattern(self, query_type, pattern):
        with self._lock:
            if query_type not in self.data["successful_patterns"]:
                self.data["successful_patterns"][query_type] = []
            if pattern not in self.data["successful_patterns"][query_type]:
                self.data["successful_patterns"][query_type].append(pattern)
                if len(self.data["successful_patterns"][query_type]) > 10:
                    self.data["successful_patterns"][query_type] = \
                        self.data["successful_patterns"][query_type][-10:]
        self.save()

    def add_column_hint(self, description, table, column):
        with self._lock:
            key = description.lower()
            self.data["column_hints"][key] = {"table": table, "column": column}
        self.save()

    def get_column_hints(self):
        with self._lock:
            return self.data.get("column_hints", {})

    def add_join_pattern(self, tables, join_keys):
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
                "note": "These are hints from previous queries. Use as guidance, not absolute facts."
            }

knowledge_store = KnowledgeStore()
