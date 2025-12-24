"""
Entity resolution and search expansion nodes.
"""

import json
import pandas as pd
from pocketflow import Node

from backend.config import ENTITY_SAMPLE_SIZE, SEARCH_SAMPLE_SIZE
from backend.utils.call_llm import call_llm
from backend.utils.knowledge_store import knowledge_store
from backend.utils.nba_api_client import nba_client


class EntityResolver(Node):
    """
    Discover which tables contain entities mentioned in the query using configurable sampling.
    """

    def prep(self, shared):
        return {
            "question": shared["question"],
            "schema": shared["schema_str"],
            "dfs": shared["dfs"],
            "sample_size": shared.get("config", {}).get("entity_sample_size", ENTITY_SAMPLE_SIZE),
            "entity_ids": shared.get("entity_ids", {}),
        }

    @staticmethod
    def _get_sample(df, col, sample_size):
        """Return a consistent sample for entity scanning."""
        try:
            series = df[col].dropna()
            if len(series) <= sample_size:
                return series
            return series.head(sample_size)
        except (KeyError, AttributeError):
            return pd.Series(dtype="object")

    def exec(self, prep_res):
        question = prep_res["question"]
        dfs = prep_res["dfs"]
        sample_size = prep_res["sample_size"]
        entity_ids = dict(prep_res.get("entity_ids", {}))

        knowledge_hints = knowledge_store.get_all_hints()

        extract_prompt = f"""Extract entities (people, teams, places, specific items) from this question.
Return a JSON array of entity names only.

Question: {question}

Example output: ["LeBron James", "Tracy McGrady", "Chicago Bulls"]
Return ONLY the JSON array, nothing else."""

        try:
            entities_response = call_llm(extract_prompt)
            entities_response = (entities_response or "").strip()
            if entities_response.startswith("```"):
                entities_response = entities_response.split("```")[1]
                if entities_response.startswith("json"):
                    entities_response = entities_response[4:]
            entities = json.loads(entities_response)
        except json.JSONDecodeError as exc:
            print(f"Failed to parse entity JSON: {exc}")
            entities = []
        except Exception as exc:  # noqa: BLE001
            print(f"Unexpected error extracting entities: {exc}")
            entities = []

        entity_map = {}
        for entity in entities:
            entity_map[entity] = {}
            entity_lower = entity.lower()
            entity_parts = entity_lower.split()
            player = nba_client.find_player(entity)
            if player:
                entity_ids[entity] = {"player_id": player.get("id")}
            else:
                team = nba_client.find_team(entity)
                if team:
                    entity_ids[entity] = {"team_id": team.get("id")}

            for table_name, df in dfs.items():
                matching_cols = []
                name_cols = [
                    col
                    for col in df.columns
                    if any(token in col.lower() for token in ["first_name", "last_name", "player_name", "full_name", "display"])
                ]

                if len(name_cols) >= 2 and len(entity_parts) >= 2:
                    first_name_cols = [c for c in name_cols if "first" in c.lower()]
                    last_name_cols = [c for c in name_cols if "last" in c.lower()]
                    if first_name_cols and last_name_cols:
                        try:
                            first_sample = self._get_sample(df, first_name_cols[0], sample_size)
                            last_sample = self._get_sample(df, last_name_cols[0], sample_size)
                            first_match = first_sample.astype(str).str.lower().str.contains(entity_parts[0], na=False)
                            last_match = last_sample.astype(str).str.lower().str.contains(entity_parts[-1], na=False)
                            if first_match.any() and last_match.any():
                                matching_cols.extend([first_name_cols[0], last_name_cols[0]])
                        except (KeyError, AttributeError, TypeError):
                            continue

                for col in df.columns:
                    if col in matching_cols:
                        continue
                    try:
                        if df[col].dtype == "object":
                            sample = self._get_sample(df, col, sample_size)
                            matches = sample.astype(str).str.lower().str.contains(entity_lower, na=False)
                            if matches.any():
                                matching_cols.append(col)
                    except (KeyError, AttributeError, TypeError):
                        continue

                if matching_cols:
                    entity_map[entity][table_name] = list(set(matching_cols))
                    knowledge_store.add_entity_mapping(entity, table_name, matching_cols)

        return {
            "entities": entities,
            "entity_map": entity_map,
            "knowledge_hints": knowledge_hints,
            "entity_ids": entity_ids,
        }

    def exec_fallback(self, prep_res, exc):
        print(f"EntityResolver failed: {exc}")
        return {
            "entities": [],
            "entity_map": {},
            "knowledge_hints": {},
        }

    def post(self, shared, prep_res, exec_res):
        shared["entities"] = exec_res["entities"]
        shared["entity_map"] = exec_res["entity_map"]
        shared["knowledge_hints"] = exec_res["knowledge_hints"]
        shared["entity_ids"] = exec_res["entity_ids"]
        print(f"Resolved {len(exec_res['entities'])} entities across tables.")
        if exec_res["entity_map"]:
            for entity, tables in exec_res["entity_map"].items():
                if tables:
                    print(f"  - {entity}: found in {list(tables.keys())}")
                else:
                    print(f"  - {entity}: NOT FOUND in any table")
        return "default"


class SearchExpander(Node):
    """
    Expand entity search to find aliases and cross-references using data profiles.
    """

    def prep(self, shared):
        return {
            "entity_map": shared.get("entity_map", {}),
            "entities": shared.get("entities", []),
            "dfs": shared["dfs"],
            "data_profile": shared.get("data_profile", {}),
            "question": shared["question"],
            "sample_size": shared.get("config", {}).get("search_sample_size", SEARCH_SAMPLE_SIZE),
        }

    @staticmethod
    def _get_sample(df, col, sample_size):
        """Return a sampled dataframe preserving row context."""
        try:
            series = df[col].dropna()
            if len(series) <= sample_size:
                return df
            return df.head(sample_size)
        except (KeyError, AttributeError):
            return df.head(0)

    def exec(self, prep_res):
        entity_map = prep_res["entity_map"]
        entities = prep_res["entities"]
        dfs = prep_res["dfs"]
        profile = prep_res["data_profile"]
        sample_size = prep_res["sample_size"]

        expanded_map = dict(entity_map)
        related_entities = {}
        cross_references = {}

        for entity in entities:
            entity_lower = entity.lower()
            parts = entity_lower.split()

            for table_name, df in dfs.items():
                table_profile = profile.get(table_name, {})
                sampled_df = self._get_sample(
                    df,
                    table_profile.get("name_columns", [""])[0] if table_profile.get("name_columns") else "",
                    sample_size,
                )

                for col in table_profile.get("name_columns", []):
                    try:
                        matches = sampled_df[sampled_df[col].astype(str).str.lower().str.contains(entity_lower, na=False)]
                        if not matches.empty and table_name not in expanded_map.get(entity, {}):
                            expanded_map.setdefault(entity, {})[table_name] = [col]
                    except (KeyError, AttributeError, TypeError):
                        pass

                if table_name in expanded_map.get(entity, {}):
                    for id_col in table_profile.get("id_columns", []):
                        try:
                            name_cols = table_profile.get("name_columns", [])
                            if name_cols:
                                mask = sampled_df[name_cols[0]].astype(str).str.lower().str.contains(
                                    parts[0] if parts else entity_lower, na=False
                                )
                                matches = sampled_df[mask]
                                if not matches.empty:
                                    entity_id = str(matches.iloc[0].get(id_col, ""))
                                    if entity_id and entity_id != "nan":
                                        cross_references.setdefault(entity, {})[f"{table_name}.{id_col}"] = entity_id
                        except (KeyError, AttributeError, IndexError, TypeError):
                            pass

        return {
            "expanded_map": expanded_map,
            "related_entities": related_entities,
            "cross_references": cross_references,
        }

    def post(self, shared, prep_res, exec_res):
        shared["entity_map"] = exec_res["expanded_map"]
        shared["cross_references"] = exec_res["cross_references"]

        total_tables = sum(len(tables) for tables in exec_res["expanded_map"].values())
        print(f"Search expanded: {len(exec_res['expanded_map'])} entities across {total_tables} table matches")
        if exec_res["cross_references"]:
            print(f"Cross-references found: {exec_res['cross_references']}")
        return "default"
