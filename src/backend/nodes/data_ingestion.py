"""
Data ingestion nodes for loading local data and fetching NBA API content.
"""

import os
import pandas as pd
from pocketflow import Node

from backend.config import DEFAULT_DATA_DIR, NBA_DEFAULT_SEASON
from backend.utils.data_source_manager import data_source_manager
from backend.utils.nba_api_client import nba_client


class LoadData(Node):
    """Load CSV files from the configured data directory into dataframes."""

    def prep(self, shared):
        return shared.get("data_dir", DEFAULT_DATA_DIR)

    def exec(self, prep_res):
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
                    print(f"Error parsing CSV file {filename}: {exc}")
                except FileNotFoundError as exc:
                    print(f"File not found {filename}: {exc}")
                except Exception as exc:  # noqa: BLE001
                    print(f"Unexpected error loading {filename}: {exc}")

        return data

    def post(self, shared, prep_res, exec_res):
        shared["csv_dfs"] = exec_res
        shared["dfs"] = exec_res
        shared["data_sources"] = {name: "csv" for name in exec_res}
        print(f"Loaded {len(exec_res)} dataframes from CSV.")
        if not exec_res:
            shared["final_text"] = (
                "No CSV files found in the CSV/ directory. "
                "Please upload data before asking a question."
            )
            return "no_data"
        return "default"


class NBAApiDataLoader(Node):
    """
    Fetch relevant data from the NBA API based on question context and detected entities.
    """

    def prep(self, shared):
        return {
            "question": shared.get("question", ""),
            "entities": shared.get("entities")
            or data_source_manager.detect_query_entities(shared.get("question", "")),
        }

    def _resolve_ids(self, entities):
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
        question = prep_res["question"]
        entities = prep_res["entities"]
        entity_ids = self._resolve_ids(entities)

        endpoints_to_call = data_source_manager.determine_api_endpoints(entities, question)
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
                    leaders = nba_client.get_league_leaders(season=season, stat_category="PTS")
                    api_dfs[f"league_leaders_{season}"] = leaders
                elif name == "common_team_roster":
                    ent = params.get("entity")
                    team_id = entity_ids.get(ent, {}).get("team_id")
                    if not team_id:
                        continue
                    roster = nba_client.get_common_team_roster(team_id=team_id, season=NBA_DEFAULT_SEASON)
                    api_dfs[f"{ent}_roster"] = roster
                elif name == "player_game_log":
                    ent = params.get("entity")
                    player_id = entity_ids.get(ent, {}).get("player_id")
                    if not player_id:
                        continue
                    game_log = nba_client.get_player_game_log(player_id=player_id, season=NBA_DEFAULT_SEASON)
                    api_dfs[f"{ent}_game_log"] = game_log
                elif name == "scoreboard":
                    api_dfs["live_scoreboard"] = nba_client.get_scoreboard()
                else:
                    errors.append({"endpoint": name, "error": "Unknown endpoint"})
            except Exception as exc:  # noqa: BLE001
                errors.append({"endpoint": name, "error": str(exc)})

        return {"api_dfs": api_dfs, "errors": errors, "used": used, "entity_ids": entity_ids}

    def post(self, shared, prep_res, exec_res):
        shared["api_dfs"] = exec_res["api_dfs"]
        shared["api_errors"] = exec_res["errors"]
        shared["api_endpoints_used"] = exec_res["used"]
        shared["entity_ids"] = exec_res.get("entity_ids", {})
        print(f"NBA API loader fetched {len(exec_res['api_dfs'])} tables with {len(exec_res['errors'])} errors.")
        return "default"


class DataMerger(Node):
    """Combine CSV and API dataframes with source tracking and discrepancy flags."""

    def prep(self, shared):
        return {
            "csv_dfs": shared.get("csv_dfs", {}),
            "api_dfs": shared.get("api_dfs", {}),
        }

    def exec(self, prep_res):
        merged, discrepancies, sources = data_source_manager.merge_data_sources(
            prep_res["csv_dfs"], prep_res["api_dfs"]
        )
        return merged, discrepancies, sources

    def post(self, shared, prep_res, exec_res):
        merged, discrepancies, sources = exec_res
        shared["dfs"] = merged
        shared["discrepancies"] = discrepancies
        shared["data_sources"] = sources
        print(f"Data merged: {len(merged)} tables ({len(discrepancies)} discrepancies flagged).")
        return "default"
