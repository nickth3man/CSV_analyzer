import hashlib
import json
import os
import threading
import time

import pandas as pd
from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats import endpoints
from nba_api.stats.static import players, teams


class NBAApiClient:
    """Lightweight wrapper around nba_api with caching and rate limiting."""

    def __init__(self) -> None:
        self.request_delay = float(os.environ.get("NBA_API_REQUEST_DELAY", 0.6))
        self.cache_ttl = int(os.environ.get("NBA_API_CACHE_TTL", 3600))
        self.cache_dir = os.environ.get("NBA_API_CACHE_DIR", ".nba_cache")
        os.makedirs(self.cache_dir, exist_ok=True)

        self._lock = threading.Lock()
        self._last_request_time = 0.0

    # -------------------------
    # Internal helpers
    # -------------------------
    def _serialize(self, payload):
        if isinstance(payload, pd.DataFrame):
            return {
                "__type": "dataframe",
                "columns": list(payload.columns),
                "data": payload.to_dict(orient="records"),
            }
        if isinstance(payload, dict):
            return {k: self._serialize(v) for k, v in payload.items()}
        if isinstance(payload, list):
            return [self._serialize(v) for v in payload]
        return payload

    def _deserialize(self, payload):
        if isinstance(payload, dict) and payload.get("__type") == "dataframe":
            return pd.DataFrame(
                payload.get("data", []), columns=payload.get("columns", [])
            )
        if isinstance(payload, dict):
            return {k: self._deserialize(v) for k, v in payload.items()}
        if isinstance(payload, list):
            return [self._deserialize(v) for v in payload]
        return payload

    def _cache_path(self, name, params):
        key_str = json.dumps(
            {"name": name, "params": params}, sort_keys=True, default=str
        )
        hashed = hashlib.sha256(key_str.encode("utf-8")).hexdigest()
        return os.path.join(self.cache_dir, f"{name}_{hashed}.json")

    def _read_cache(self, name, params):
        path = self._cache_path(name, params)
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as f:
            cached = json.load(f)
        timestamp = cached.get("timestamp", 0)
        if time.time() - timestamp > self.cache_ttl:
            return None
        return self._deserialize(cached.get("payload"))

    def _write_cache(self, name, params, payload) -> None:
        path = self._cache_path(name, params)
        os.makedirs(self.cache_dir, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"timestamp": time.time(), "payload": self._serialize(payload)}, f
            )

    def _throttle(self) -> None:
        with self._lock:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.request_delay:
                time.sleep(self.request_delay - elapsed)
            self._last_request_time = time.time()

    def _call_with_cache(self, name, params, fetch_fn, cacheable=True):
        if cacheable:
            cached = self._read_cache(name, params)
            if cached is not None:
                return cached

        self._throttle()
        result = fetch_fn()

        if cacheable:
            self._write_cache(name, params, result)
        return result

    # -------------------------
    # Static data helpers (no HTTP)
    # -------------------------
    def get_all_players(self):
        return players.get_players()

    def get_active_players(self):
        return players.get_active_players()

    def find_player(self, name):
        matches = players.find_players_by_full_name(name)
        return matches[0] if matches else None

    def get_player_id(self, name):
        player = self.find_player(name)
        return player.get("id") if player else None

    def get_all_teams(self):
        return teams.get_teams()

    def find_team(self, name):
        normalized = name.lower()
        by_abbr = teams.find_team_by_abbreviation(name.upper())
        if by_abbr:
            return by_abbr

        city_matches = teams.find_teams_by_city(name)
        if city_matches:
            return city_matches[0]

        for team in self.get_all_teams():
            if (
                team["full_name"].lower() == normalized
                or team["nickname"].lower() == normalized
            ):
                return team
        return None

    # -------------------------
    # Stats endpoints (HTTP)
    # -------------------------
    def get_player_career_stats(self, player_id):
        def fetch():
            frames = endpoints.PlayerCareerStats(player_id=player_id).get_data_frames()
            keys = ["regular_season", "post_season", "career_regular", "career_post"]
            return {
                k: frames[i] if i < len(frames) else pd.DataFrame()
                for i, k in enumerate(keys)
            }

        return self._call_with_cache(
            "player_career_stats", {"player_id": player_id}, fetch
        )

    def get_player_game_log(self, player_id, season):
        def fetch():
            return endpoints.PlayerGameLog(
                player_id=player_id, season=season
            ).get_data_frames()[0]

        return self._call_with_cache(
            "player_game_log", {"player_id": player_id, "season": season}, fetch
        )

    def get_team_game_log(self, team_id, season):
        def fetch():
            return endpoints.TeamGameLog(
                team_id=team_id, season=season
            ).get_data_frames()[0]

        return self._call_with_cache(
            "team_game_log", {"team_id": team_id, "season": season}, fetch
        )

    def get_league_leaders(self, season, stat_category="PTS"):
        def fetch():
            return endpoints.LeagueLeaders(
                season=season, stat_category_abbreviation=stat_category
            ).get_data_frames()[0]

        return self._call_with_cache(
            "league_leaders", {"season": season, "stat_category": stat_category}, fetch
        )

    def get_common_team_roster(self, team_id, season):
        def fetch():
            return endpoints.CommonTeamRoster(
                team_id=team_id, season=season
            ).get_data_frames()[0]

        return self._call_with_cache(
            "common_team_roster", {"team_id": team_id, "season": season}, fetch
        )

    def get_scoreboard(self):
        # Live endpoint - do not cache
        self._throttle()
        board = scoreboard.ScoreBoard().get_dict()
        games = board.get("scoreboard", {}).get("games", [])
        return pd.DataFrame(games)


nba_client = NBAApiClient()
