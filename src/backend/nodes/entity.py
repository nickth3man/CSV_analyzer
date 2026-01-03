"""Entity resolution and search expansion nodes."""

import json
import logging

import pandas as pd
from pocketflow import Node


logger = logging.getLogger(__name__)

from backend.config import ENTITY_SAMPLE_SIZE, SEARCH_SAMPLE_SIZE
from backend.utils.call_llm import call_llm
from backend.utils.knowledge_store import knowledge_store
from backend.utils.nba_api_client import nba_client


class EntityResolver(Node):
    """Discover which tables contain entities mentioned in the query using configurable sampling."""

    def prep(self, shared):
        """Prepare execution inputs for the EntityResolver by extracting required values from the shared context.

        Parameters:
            shared (dict): Shared pipeline state. Expected keys:
                - "question": the user question text.
                - "schema_str": serialized schema used for context.
                - "dfs": mapping of table names to pandas DataFrame objects.
                - "config" (optional): may contain "entity_sample_size" to override the default.
                - "entity_ids" (optional): prepopulated entity identifier mappings.

        Returns:
            dict: Execution-ready dictionary with keys:
                - "question": the extracted question.
                - "schema": the serialized schema string.
                - "dfs": the DataFrames mapping.
                - "sample_size": entity sampling size (configured value or default).
                - "entity_ids": existing entity identifier mappings (empty dict if absent).
        """
        return {
            "question": shared["question"],
            "schema": shared["schema_str"],
            "dfs": shared["dfs"],
            "sample_size": shared.get("config", {}).get(
                "entity_sample_size",
                ENTITY_SAMPLE_SIZE,
            ),
            "entity_ids": shared.get("entity_ids", {}),
        }

    @staticmethod
    def _get_sample(df, col, sample_size):
        """Return a non-null sample from a DataFrame column for entity scanning.

        Attempts to extract non-missing values from `df[col]`. If the column's non-missing count is less than or equal to `sample_size`, returns the full non-missing Series; otherwise returns the first `sample_size` entries. If the column is missing or `df` does not support column access, returns an empty object-typed Series.

        Parameters:
            df: DataFrame-like object containing the column to sample.
            col (str): Column name to sample.
            sample_size (int): Maximum number of rows to return.

        Returns:
            pandas.Series: A Series of non-null values from the column (possibly empty).
        """
        try:
            series = df[col].dropna()
            if len(series) <= sample_size:
                return series
            return series.head(sample_size)
        except (KeyError, AttributeError):
            return pd.Series(dtype="object")

    def exec(self, prep_res):
        """Resolve entities mentioned in the prepared request, locate columns containing those entities across provided DataFrames, enrich with known IDs, and record mappings in the knowledge store.

        Parameters:
            prep_res (dict): Preparation result containing:
                - question (str): The user question to extract entities from.
                - dfs (dict[str, pandas.DataFrame]): Mapping of table name to DataFrame to search for entity occurrences.
                - sample_size (int): Number of rows/values to sample from columns when searching for matches.
                - entity_ids (dict, optional): Existing mapping of entities to ID dicts (e.g., {"player_id": ...} or {"team_id": ...}).

        Returns:
            dict: {
                "entities": list[str],            # Extracted entity names (may be empty on parse failure).
                "entity_map": dict,               # Mapping: entity -> table_name -> list of matching column names.
                "knowledge_hints": any,           # Snapshot of hints retrieved from the knowledge store.
                "entity_ids": dict                # Mapping of entity -> id dicts discovered via nba_client (player_id/team_id).
            }
        """
        question = prep_res["question"]
        dfs = prep_res["dfs"]
        sample_size = prep_res["sample_size"]
        entity_ids = dict(prep_res.get("entity_ids", {}))

        knowledge_hints = knowledge_store.get_all_hints()
        entities = self._extract_entities(question)
        entity_map: dict[str, dict[str, list[str]]] = {}

        for entity in entities:
            entity_map[entity] = {}
            resolved_id = self._resolve_entity_id(entity)
            if resolved_id:
                entity_ids[entity] = resolved_id

            for table_name, df in dfs.items():
                matching_cols = self._find_matching_columns(
                    entity,
                    df,
                    sample_size,
                )
                if matching_cols:
                    entity_map[entity][table_name] = matching_cols
                    knowledge_store.add_entity_mapping(
                        entity,
                        table_name,
                        matching_cols,
                    )

        return {
            "entities": entities,
            "entity_map": entity_map,
            "knowledge_hints": knowledge_hints,
            "entity_ids": entity_ids,
        }

    def _extract_entities(self, question: str) -> list[str]:
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
                entities_response = entities_response.removeprefix("json")
            parsed = json.loads(entities_response)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError as exc:
            logger.exception(f"Failed to parse entity JSON: {exc}")
        except Exception as exc:
            logger.exception(f"Unexpected error extracting entities: {exc}")
        return []

    def _resolve_entity_id(self, entity: str) -> dict[str, int] | None:
        player = nba_client.find_player(entity)
        if player:
            return {"player_id": player.get("id")}
        team = nba_client.find_team(entity)
        if team:
            return {"team_id": team.get("id")}
        return None

    def _find_matching_columns(
        self,
        entity: str,
        df: pd.DataFrame,
        sample_size: int,
    ) -> list[str]:
        entity_lower = entity.lower()
        entity_parts = entity_lower.split()
        matching_cols: list[str] = []

        name_cols = [
            col
            for col in df.columns
            if any(
                token in col.lower()
                for token in [
                    "first_name",
                    "last_name",
                    "player_name",
                    "full_name",
                    "display",
                ]
            )
        ]

        matching_cols.extend(
            self._match_first_last_columns(df, name_cols, entity_parts, sample_size)
        )
        matching_cols.extend(
            self._match_string_columns(df, entity_lower, sample_size, matching_cols)
        )

        return sorted(set(matching_cols))

    def _match_first_last_columns(
        self,
        df: pd.DataFrame,
        name_cols: list[str],
        entity_parts: list[str],
        sample_size: int,
    ) -> list[str]:
        if len(name_cols) < 2 or len(entity_parts) < 2:
            return []

        first_name_cols = [c for c in name_cols if "first" in c.lower()]
        last_name_cols = [c for c in name_cols if "last" in c.lower()]
        if not first_name_cols or not last_name_cols:
            return []

        try:
            first_sample = self._get_sample(df, first_name_cols[0], sample_size)
            last_sample = self._get_sample(df, last_name_cols[0], sample_size)
            first_match = (
                first_sample.astype(str)
                .str.lower()
                .str.contains(entity_parts[0], na=False)
            )
            last_match = (
                last_sample.astype(str)
                .str.lower()
                .str.contains(entity_parts[-1], na=False)
            )
            if first_match.any() and last_match.any():
                return [first_name_cols[0], last_name_cols[0]]
        except (KeyError, AttributeError, TypeError):
            return []

        return []

    def _match_string_columns(
        self,
        df: pd.DataFrame,
        entity_lower: str,
        sample_size: int,
        existing_cols: list[str],
    ) -> list[str]:
        matches: list[str] = []
        for col in df.columns:
            if col in existing_cols:
                continue
            try:
                if df[col].dtype == "object":
                    sample = self._get_sample(df, col, sample_size)
                    contains_entity = (
                        sample.astype(str)
                        .str.lower()
                        .str.contains(entity_lower, na=False)
                    )
                    if contains_entity.any():
                        matches.append(col)
            except (KeyError, AttributeError, TypeError):
                continue
        return matches

    def exec_fallback(self, prep_res, exc):
        """Handle failures during EntityResolver.exec by returning a safe default execution result.

        Parameters:
            prep_res: The preparation result passed to exec; included for signature compatibility but not used.
            exc (Exception): The exception that caused the failure.

        Returns:
            dict: A default execution result containing:
                - "entities": an empty list.
                - "entity_map": an empty mapping of entity names to table columns.
                - "knowledge_hints": an empty mapping of knowledge hints.
        """
        logger.error(f"EntityResolver failed: {exc}")
        return {
            "entities": [],
            "entity_map": {},
            "knowledge_hints": {},
        }

    def post(self, shared, prep_res, exec_res) -> str:
        """Merge execution results into the shared context, log a short summary of resolved entities, and return the next node token.

        Parameters:
            shared (dict): Shared pipeline state to be updated. This function sets the keys "entities", "entity_map", "knowledge_hints", and "entity_ids" from exec_res.
            prep_res (dict): Preparation result (unused by this implementation but passed by the pipeline).
            exec_res (dict): Execution result containing at least the keys "entities", "entity_map", "knowledge_hints", and "entity_ids".

        Returns:
            str: The next pipeline token, `"default"`.
        """
        shared["entities"] = exec_res["entities"]
        shared["entity_map"] = exec_res["entity_map"]
        shared["knowledge_hints"] = exec_res["knowledge_hints"]
        shared["entity_ids"] = exec_res["entity_ids"]
        logger.info(f"Resolved {len(exec_res['entities'])} entities across tables.")
        if exec_res["entity_map"]:
            for entity, tables in exec_res["entity_map"].items():
                if tables:
                    logger.info(f"  - {entity}: found in {list(tables.keys())}")
                else:
                    logger.info(f"  - {entity}: NOT FOUND in any table")
        return "default"


class SearchExpander(Node):
    """Expand entity search to find aliases and cross-references using data profiles."""

    def prep(self, shared):
        """Prepare inputs for the SearchExpander node by extracting required values from the shared pipeline state.

        Parameters:
            shared (dict): Shared pipeline state containing execution context (expected keys: "dfs", "question", optional "entity_map", "entities", "data_profile", and "config").

        Returns:
            dict: A dictionary with the following keys:
                - entity_map: existing entity-to-table mapping (default {}).
                - entities: list of entities to expand (default []).
                - dfs: dictionary of DataFrames (required).
                - data_profile: data profiling information for tables (default {}).
                - question: the original user question (required).
                - sample_size: integer sample size from shared["config"]["search_sample_size"] or module default.
        """
        return {
            "entity_map": shared.get("entity_map", {}),
            "entities": shared.get("entities", []),
            "dfs": shared["dfs"],
            "data_profile": shared.get("data_profile", {}),
            "question": shared["question"],
            "sample_size": shared.get("config", {}).get(
                "search_sample_size",
                SEARCH_SAMPLE_SIZE,
            ),
        }

    @staticmethod
    def _get_sample(df, col, sample_size):
        """Return a DataFrame sample that preserves the input rows' context based on non-null values in a specified column.

        Parameters:
            df (pandas.DataFrame): The DataFrame to sample.
            col (str): Column name whose non-null count determines whether to return the full DataFrame or a subset.
            sample_size (int): Maximum number of rows to keep when sampling.

        Returns:
            pandas.DataFrame: The original DataFrame if the specified column has non-null entries less than or equal to sample_size; otherwise the first `sample_size` rows. Returns an empty DataFrame (zero rows) if `col` is missing or `df` is not a DataFrame.
        """
        try:
            series = df[col].dropna()
            if len(series) <= sample_size:
                return df
            return df.head(sample_size)
        except (KeyError, AttributeError):
            return df.head(0)

    def exec(self, prep_res):
        """Expand the entity-to-table mapping and collect cross-table identifier references for resolved entities.

        Parameters:
            prep_res (dict): Preparation results containing:
                - entity_map: existing mapping of entities to tables/columns
                - entities: list of entity strings to expand
                - dfs: dict of table_name -> DataFrame to search
                - data_profile: dict of table profile metadata (including `name_columns` and `id_columns`)
                - sample_size: integer sample size used when sampling DataFrames

        Returns:
            dict: {
                "expanded_map": dict mapping each entity to tables and matched name columns,
                "related_entities": dict of related entity suggestions (empty by default),
                "cross_references": dict mapping each entity to found id values keyed by "table_name.id_column"
            }

        Notes:
            - Sampling is performed via the class's _get_sample helper using the table's first name column when available.
            - String matching is case-insensitive and tolerant of missing columns or malformed data; index and type errors during scanning are ignored.
            - For each matched table, the method attempts to extract the first matching row's id value for any listed id_columns and records it in cross_references.
        """
        entity_map = prep_res["entity_map"]
        entities = prep_res["entities"]
        dfs = prep_res["dfs"]
        profile = prep_res["data_profile"]
        sample_size = prep_res["sample_size"]

        expanded_map = dict(entity_map)
        related_entities: dict[str, list[str]] = {}
        cross_references: dict[str, dict[str, str]] = {}

        for entity in entities:
            entity_lower = entity.lower()
            parts = entity_lower.split()
            for table_name, df in dfs.items():
                table_profile = profile.get(table_name, {})
                sampled_df = self._sample_table(df, table_profile, sample_size)
                self._update_entity_mapping(
                    expanded_map,
                    entity,
                    entity_lower,
                    table_name,
                    table_profile,
                    sampled_df,
                )
                self._update_cross_references(
                    cross_references,
                    expanded_map,
                    entity,
                    table_name,
                    table_profile,
                    sampled_df,
                    parts,
                    entity_lower,
                )

        return {
            "expanded_map": expanded_map,
            "related_entities": related_entities,
            "cross_references": cross_references,
        }

    def _sample_table(
        self,
        df: pd.DataFrame,
        table_profile: dict[str, Any],
        sample_size: int,
    ) -> pd.DataFrame:
        name_cols = table_profile.get("name_columns", [])
        first_name_col = name_cols[0] if name_cols else ""
        return self._get_sample(df, first_name_col, sample_size)

    def _update_entity_mapping(
        self,
        expanded_map: dict[str, dict[str, list[str]]],
        entity: str,
        entity_lower: str,
        table_name: str,
        table_profile: dict[str, Any],
        sampled_df: pd.DataFrame,
    ) -> None:
        for col in table_profile.get("name_columns", []):
            try:
                matches = sampled_df[
                    sampled_df[col]
                    .astype(str)
                    .str.lower()
                    .str.contains(entity_lower, na=False)
                ]
                if not matches.empty and table_name not in expanded_map.get(entity, {}):
                    expanded_map.setdefault(entity, {})[table_name] = [col]
            except (KeyError, AttributeError, TypeError):
                continue

    def _update_cross_references(
        self,
        cross_references: dict[str, dict[str, str]],
        expanded_map: dict[str, dict[str, list[str]]],
        entity: str,
        table_name: str,
        table_profile: dict[str, Any],
        sampled_df: pd.DataFrame,
        parts: list[str],
        entity_lower: str,
    ) -> None:
        if table_name not in expanded_map.get(entity, {}):
            return

        name_cols = table_profile.get("name_columns", [])
        if not name_cols:
            return

        try:
            mask = (
                sampled_df[name_cols[0]]
                .astype(str)
                .str.lower()
                .str.contains(parts[0] if parts else entity_lower, na=False)
            )
            matches = sampled_df[mask]
        except (KeyError, AttributeError, TypeError):
            return

        if matches.empty:
            return

        for id_col in table_profile.get("id_columns", []):
            try:
                entity_id = str(matches.iloc[0].get(id_col, ""))
                if entity_id and entity_id != "nan":
                    cross_references.setdefault(entity, {})[
                        f"{table_name}.{id_col}"
                    ] = entity_id
            except (IndexError, TypeError):
                continue

    def post(self, shared, prep_res, exec_res) -> str:
        """Merge expanded entity mappings and cross-references into the shared context and log a brief summary.

        Updates shared["entity_map"] with exec_res["expanded_map"] and shared["cross_references"] with exec_res["cross_references"], then prints a count of entities and table matches and any cross-references found.

        Parameters:
            exec_res (dict): Execution result containing:
                expanded_map (dict): Mapping of entities to tables/columns.
                cross_references (dict): Discovered cross-reference values.

        Returns:
            str: The string "default" indicating normal continuation.
        """
        shared["entity_map"] = exec_res["expanded_map"]
        shared["cross_references"] = exec_res["cross_references"]

        total_tables = sum(len(tables) for tables in exec_res["expanded_map"].values())
        logger.info(
            f"Search expanded: {len(exec_res['expanded_map'])} entities across {total_tables} table matches",
        )
        if exec_res["cross_references"]:
            logger.info(f"Cross-references found: {exec_res['cross_references']}")
        return "default"
