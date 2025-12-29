"""Data ingestion nodes for loading local data and fetching NBA API content.

# TODO (Performance): Implement DataFrame caching with invalidation
# Current implementation reloads all CSVs on every question (LoadData.exec).
# This is inefficient for repeated queries. Recommended approach:
#   class DataFrameCache:
#       def __init__(self, ttl=300):
#           self._cache = {}
#           self._timestamps = {}
#       def get(self, filepath):
#           if filepath in self._cache:
#               if time.time() - self._timestamps[filepath] < self.ttl:
#                   return self._cache[filepath]
#           return None
#       def set(self, filepath, df):
#           self._cache[filepath] = df
#           self._timestamps[filepath] = time.time()
#       def invalidate(self, filepath=None):
#           if filepath:
#               self._cache.pop(filepath, None)
#           else:
#               self._cache.clear()
# Invalidate on file upload or modification.

# TODO (Refactoring): Split NBAApiDataLoader.exec into smaller methods
# Current exec() method is 80+ lines with nested conditionals for each
# endpoint type (player_career, league_leaders, etc.). Recommended:
#   def _fetch_player_career(self, entity, entity_ids):
#       ...
#   def _fetch_league_leaders(self, season):
#       ...
#   def _fetch_team_roster(self, entity, entity_ids, season):
#       ...
#   def exec(self, prep_res):
#       handlers = {
#           "player_career": self._fetch_player_career,
#           "league_leaders": self._fetch_league_leaders,
#           ...
#       }
#       for endpoint in endpoints_to_call:
#           handler = handlers.get(endpoint["name"])
#           if handler:
#               result = handler(**params)

# TODO (Performance): Parallel API fetching
# Current API calls are sequential with rate limiting between each.
# For independent endpoints, consider parallel fetching:
#   import asyncio
#   async def fetch_all_endpoints(endpoints):
#       semaphore = asyncio.Semaphore(3)  # Limit concurrent requests
#       async def fetch_with_limit(endpoint):
#           async with semaphore:
#               await asyncio.sleep(0.6)  # Rate limit
#               return await fetch_endpoint_async(endpoint)
#       return await asyncio.gather(*[fetch_with_limit(e) for e in endpoints])

# TODO (Reliability): Add fallback for API failures
# When NBA API is unavailable, the system should:
#   1. Log a warning with details
#   2. Continue with CSV-only mode
#   3. Display a user-facing message about limited data
#   4. Cache the failure to avoid repeated attempts
# Example: shared["api_status"] = "unavailable" for downstream handling.
"""

import logging
import os

import pandas as pd
from pocketflow import Node


logger = logging.getLogger(__name__)

from backend.config import DEFAULT_DATA_DIR, NBA_DEFAULT_SEASON
from backend.utils.data_source_manager import data_source_manager
from backend.utils.nba_api_client import nba_client


class LoadData(Node):
    """Load CSV files from the configured data directory into DataFrames."""

    def prep(self, shared):
        """Resolve the data directory path from shared state or fall back to the configured default.

        Parameters:
            shared (dict): Shared state that may contain a "data_dir" key with a filesystem path.

        Returns:
            data_dir (str): The path to use for loading CSV files; taken from shared["data_dir"] if present, otherwise DEFAULT_DATA_DIR.
        """
        return shared.get("data_dir", DEFAULT_DATA_DIR)

    def exec(self, prep_res):
        """Load all CSV files found in the provided directory into pandas DataFrames.

        Parameters:
            prep_res (str): Path to the directory containing CSV files.

        Returns:
            dict: Mapping from table name (filename without the ".csv" extension) to the corresponding pandas DataFrame for each successfully read file.
        """
        csv_dir = prep_res
        data = {}

        if os.path.exists(csv_dir):
            for filename in os.listdir(csv_dir):
                if not filename.endswith(".csv"):
                    continue

                filepath = os.path.join(csv_dir, filename)
                table_name = filename.replace(".csv", "")
                try:
                    try:
                        data[table_name] = pd.read_csv(filepath, encoding="utf-8")
                    except UnicodeDecodeError:
                        data[table_name] = pd.read_csv(filepath, encoding="latin-1")
                except (pd.errors.ParserError, UnicodeDecodeError) as exc:
                    logger.exception(f"Error parsing CSV file {filename}: {exc}")
                except FileNotFoundError as exc:
                    logger.exception(f"File not found {filename}: {exc}")
                except Exception as exc:
                    logger.exception(f"Unexpected error loading {filename}: {exc}")

        return data

    def post(self, shared, prep_res, exec_res) -> str:
        """Finalize CSV load results into the shared workflow state and choose the next transition.

        Parameters:
            shared (dict): Shared workflow state to update with loaded data and metadata.
            prep_res: Preparation result (unused).
            exec_res (dict): Mapping of table names to pandas DataFrame objects loaded from CSV files.

        Returns:
            str: "no_data" if exec_res is empty (and shared["final_text"] is set with guidance), "default" otherwise.
        """
        shared["csv_dfs"] = exec_res
        shared["dfs"] = exec_res
        shared["data_sources"] = dict.fromkeys(exec_res, "csv")
        logger.info(f"Loaded {len(exec_res)} dataframes from CSV.")
        if not exec_res:
            shared["final_text"] = (
                "No CSV files found in the src/backend/data/raw/csv/ directory. "
                "Please upload data before asking a question."
            )
            return "no_data"
        return "default"


class NBAApiDataLoader(Node):
    """Fetch relevant data from the NBA API based on question context and detected entities."""

    def prep(self, shared):
        """Prepare context for NBA API data loading by extracting the question and resolving entities.

        Parameters:
            shared (dict): Shared runtime state. Expected keys:
                - "question": optional user question string.
                - "entities": optional pre-detected entities.

        Returns:
            dict: A dictionary with:
                - "question" (str): The stored question or an empty string if missing.
                - "entities": Entities from `shared["entities"]` if present; otherwise entities detected from the question.
        """
        return {
            "question": shared.get("question", ""),
            "entities": shared.get("entities")
            or data_source_manager.detect_query_entities(shared.get("question", "")),
        }

    def _resolve_ids(self, entities):
        """Resolve NBA player or team identifiers for the given entity names.

        Parameters:
            entities (Iterable[str]): Names of entities (players or teams) to resolve.

        Returns:
            dict: Mapping from each matched entity name to a dictionary containing either
            `{"player_id": <id>}` or `{"team_id": <id>}` for resolved entities. Entities
            that cannot be resolved are omitted.
        """
        entity_ids = {}
        for entity in entities:
            player = nba_client.find_player(entity)
            if player:
                entity_ids[entity] = {"player_id": player.get("id")}
                continue
            team = nba_client.find_team(entity)
            if team:
                entity_ids[entity] = {"team_id": team.get("id")}
        return entity_ids

    def exec(self, prep_res):
        """Assemble and fetch NBA API datasets determined from the provided question and entities, returning the fetched tables, any errors, the endpoints invoked, and resolved entity identifiers.

        Parameters:
            prep_res (dict): Preparation result containing:
                - "question" (str): The user question or query context.
                - "entities" (iterable): Detected entities referenced by the question.

        Notes:
            Endpoints that require an entity are skipped if that entity's ID could not be resolved.

        Returns:
            dict: A dictionary with the following keys:
                - "api_dfs" (dict): Mapping of table name (str) to pandas.DataFrame for each fetched API table.
                - "errors" (list): List of error records, each a dict with "endpoint" and "error" (string).
                - "used" (list): List of invoked endpoints with their parameters, each a dict with "name" and "params".
                - "entity_ids" (dict): Mapping of entity identifiers to resolved IDs (e.g., player_id, team_id).
        """
        question = prep_res["question"]
        entities = prep_res["entities"]
        entity_ids = self._resolve_ids(entities)

        endpoints_to_call = data_source_manager.determine_api_endpoints(
            entities,
            question,
        )
        api_dfs = {}
        errors = []
        used = []

        for endpoint in endpoints_to_call:
            name = endpoint["name"]
            params = endpoint.get("params", {})
            used.append({"name": name, "params": params})
            try:
                if name == "player_career":
                    ent = params.get("entity")
                    player_id = entity_ids.get(ent, {}).get("player_id")
                    if not player_id:
                        continue
                    career = nba_client.get_player_career_stats(player_id)
                    for key, df in career.items():
                        api_dfs[f"{ent}_career_{key}"] = df
                elif name == "league_leaders":
                    season = NBA_DEFAULT_SEASON
                    leaders = nba_client.get_league_leaders(
                        season=season,
                        stat_category="PTS",
                    )
                    api_dfs[f"league_leaders_{season}"] = leaders
                elif name == "common_team_roster":
                    ent = params.get("entity")
                    team_id = entity_ids.get(ent, {}).get("team_id")
                    if not team_id:
                        continue
                    roster = nba_client.get_common_team_roster(
                        team_id=team_id,
                        season=NBA_DEFAULT_SEASON,
                    )
                    api_dfs[f"{ent}_roster"] = roster
                elif name == "player_game_log":
                    ent = params.get("entity")
                    player_id = entity_ids.get(ent, {}).get("player_id")
                    if not player_id:
                        continue
                    game_log = nba_client.get_player_game_log(
                        player_id=player_id,
                        season=NBA_DEFAULT_SEASON,
                    )
                    api_dfs[f"{ent}_game_log"] = game_log
                elif name == "scoreboard":
                    api_dfs["live_scoreboard"] = nba_client.get_scoreboard()
                else:
                    errors.append({"endpoint": name, "error": "Unknown endpoint"})
            except Exception as exc:
                errors.append({"endpoint": name, "error": str(exc)})

        return {
            "api_dfs": api_dfs,
            "errors": errors,
            "used": used,
            "entity_ids": entity_ids,
        }

    def post(self, shared, prep_res, exec_res) -> str:
        """Store NBA API fetch results into the shared state and log a brief summary.

        Writes `api_dfs`, `api_errors`, `api_endpoints_used`, and `entity_ids` from `exec_res` into the `shared` mapping so downstream nodes can access fetched API tables, any errors, endpoints used, and resolved entity IDs. Also prints a one-line summary reporting how many tables were fetched and how many errors occurred.

        Returns:
            "default" (str): Indicates normal node completion.
        """
        shared["api_dfs"] = exec_res["api_dfs"]
        shared["api_errors"] = exec_res["errors"]
        shared["api_endpoints_used"] = exec_res["used"]
        shared["entity_ids"] = exec_res.get("entity_ids", {})
        logger.info(
            f"NBA API loader fetched {len(exec_res['api_dfs'])} tables with {len(exec_res['errors'])} errors.",
        )
        return "default"


class DataMerger(Node):
    """Combine CSV and API dataframes with source tracking and discrepancy flags."""

    def prep(self, shared):
        """Prepare merged-data inputs from the workflow shared state.

        Parameters:
            shared (dict): Workflow shared state containing previously loaded DataFrames.

        Returns:
            dict: A mapping with keys:
                - "csv_dfs": dict of table name to pandas.DataFrame loaded from CSVs (empty dict if absent).
                - "api_dfs": dict of table name to pandas.DataFrame fetched from the NBA API (empty dict if absent).
        """
        return {
            "csv_dfs": shared.get("csv_dfs", {}),
            "api_dfs": shared.get("api_dfs", {}),
        }

    def exec(self, prep_res):
        """Merge CSV and API DataFrames into a unified set and identify discrepancies and source metadata.

        Parameters:
            prep_res (dict): Preparation result containing:
                - "csv_dfs" (dict): Mapping of table names to DataFrame objects loaded from CSV.
                - "api_dfs" (dict): Mapping of table names to DataFrame objects fetched from APIs.

        Returns:
            tuple: A three-element tuple:
                - merged (dict): Mapping of table names to merged DataFrame objects combining CSV and API sources.
                - discrepancies (dict): Details of detected differences between sources for each table/field.
                - sources (dict): Metadata mapping each table/field to its originating data source(s).
        """
        merged, discrepancies, sources = data_source_manager.merge_data_sources(
            prep_res["csv_dfs"],
            prep_res["api_dfs"],
        )
        return merged, discrepancies, sources

    def post(self, shared, prep_res, exec_res) -> str:
        """Finalize merging by storing merge results into shared state and returning the next node outcome.

        Parameters:
            shared (dict): Shared runtime state; will be updated with merged DataFrames and metadata.
            prep_res: Preparation result (unused).
            exec_res (tuple): Tuple (merged, discrepancies, sources) where:
                merged (dict): Mapping of table name to merged DataFrame.
                discrepancies (list): List of detected discrepancies between sources.
                sources (dict): Mapping of table name to its originating data source(s).

        Returns:
            str: The next node outcome string `"default"`.
        """
        merged, discrepancies, sources = exec_res
        shared["dfs"] = merged
        shared["discrepancies"] = discrepancies
        shared["data_sources"] = sources
        logger.info(
            f"Data merged: {len(merged)} tables ({len(discrepancies)} discrepancies flagged).",
        )
        return "default"
