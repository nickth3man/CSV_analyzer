"""NBA API client wrapper with caching, retries, pooling, and async helpers."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import pandas as pd
import requests
from nba_api.library.http import NBAHTTP
from nba_api.live.nba.endpoints import scoreboard
from nba_api.live.nba.library.http import NBALiveHTTP
from nba_api.stats import endpoints
from nba_api.stats.static import players, teams
from requests.adapters import HTTPAdapter
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.backend.utils.resilience import circuit_breaker


logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "nba_expert/1.0"


@dataclass(frozen=True)
class NBARequestConfig:
    timeout: float
    max_retries: int
    pool_size: int
    max_concurrency: int


class NBAApiClient:
    """Wrapper around nba_api with caching, rate limiting, retries, and async helpers."""

    def __init__(self) -> None:
        self.request_delay = float(os.environ.get("NBA_API_REQUEST_DELAY", "0.6"))
        self.cache_ttl = int(os.environ.get("NBA_API_CACHE_TTL", "3600"))
        self.cache_dir = os.environ.get("NBA_API_CACHE_DIR", ".nba_cache")
        os.makedirs(self.cache_dir, exist_ok=True)

        self._lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._last_request_time = 0.0

        self._config = NBARequestConfig(
            timeout=float(os.environ.get("NBA_API_TIMEOUT", "30")),
            max_retries=int(os.environ.get("NBA_API_MAX_RETRIES", "3")),
            pool_size=int(os.environ.get("NBA_API_POOL_SIZE", "10")),
            max_concurrency=int(os.environ.get("NBA_API_MAX_CONCURRENCY", "3")),
        )
        self._session = self._create_session()
        self._configure_nba_api_sessions()

    # -------------------------
    # Session + retry helpers
    # -------------------------
    def _create_session(self) -> requests.Session:
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=self._config.pool_size,
            pool_maxsize=self._config.pool_size,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
        return session

    def _configure_nba_api_sessions(self) -> None:
        NBAHTTP.set_session(self._session)
        NBALiveHTTP.set_session(self._session)

    def _retryer(self) -> Retrying:
        return Retrying(
            stop=stop_after_attempt(max(1, self._config.max_retries)),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(Exception),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

    # -------------------------
    # Internal helpers
    # -------------------------
    def _serialize(self, payload: Any) -> Any:
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

    def _deserialize(self, payload: Any) -> Any:
        if isinstance(payload, dict) and payload.get("__type") == "dataframe":
            return pd.DataFrame(
                payload.get("data", []),
                columns=payload.get("columns", []),
            )
        if isinstance(payload, dict):
            return {k: self._deserialize(v) for k, v in payload.items()}
        if isinstance(payload, list):
            return [self._deserialize(v) for v in payload]
        return payload

    def _cache_path(self, name: str, params: dict[str, Any]) -> str:
        key_str = json.dumps(
            {"name": name, "params": params},
            sort_keys=True,
            default=str,
        )
        hashed = hashlib.sha256(key_str.encode("utf-8")).hexdigest()
        return os.path.join(self.cache_dir, f"{name}_{hashed}.json")

    def _read_cache(self, name: str, params: dict[str, Any]) -> Any:
        path = self._cache_path(name, params)
        if not os.path.exists(path):
            return None
        with self._cache_lock, open(path, encoding="utf-8") as f:
            cached = json.load(f)
        timestamp = cached.get("timestamp", 0)
        if time.time() - timestamp > self.cache_ttl:
            return None
        return self._deserialize(cached.get("payload"))

    def _write_cache(self, name: str, params: dict[str, Any], payload: Any) -> None:
        path = self._cache_path(name, params)
        os.makedirs(self.cache_dir, exist_ok=True)
        with self._cache_lock, open(path, "w", encoding="utf-8") as f:
            json.dump(
                {"timestamp": time.time(), "payload": self._serialize(payload)},
                f,
            )

    def _throttle(self) -> None:
        with self._lock:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.request_delay:
                time.sleep(self.request_delay - elapsed)
            self._last_request_time = time.time()

    @circuit_breaker(threshold=5, recovery=60)
    def _execute(
        self,
        name: str,
        params: dict[str, Any],
        fetch_fn: Callable[[], Any],
        *,
        cacheable: bool = True,
    ) -> Any:
        if cacheable:
            cached = self._read_cache(name, params)
            if cached is not None:
                return cached

        self._throttle()
        start_time = time.time()

        try:
            result = None
            for attempt in self._retryer():
                with attempt:
                    result = fetch_fn()
            if cacheable:
                self._write_cache(name, params, result)
            logger.info(
                "NBA API request %s completed in %.2fs",
                name,
                time.time() - start_time,
            )
            return result
        except Exception as exc:
            logger.warning(
                "NBA API request %s failed after %.2fs: %s",
                name,
                time.time() - start_time,
                exc,
            )
            raise

    async def _execute_async(
        self,
        name: str,
        params: dict[str, Any],
        fetch_fn: Callable[[], Any],
        *,
        cacheable: bool = True,
    ) -> Any:
        return await asyncio.to_thread(
            self._execute,
            name,
            params,
            fetch_fn,
            cacheable=cacheable,
        )

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
            frames = endpoints.PlayerCareerStats(
                player_id=player_id,
                timeout=self._config.timeout,
            ).get_data_frames()
            keys = ["regular_season", "post_season", "career_regular", "career_post"]
            return {
                k: frames[i] if i < len(frames) else pd.DataFrame()
                for i, k in enumerate(keys)
            }

        return self._execute(
            "player_career_stats",
            {"player_id": player_id},
            fetch,
        )

    def get_player_game_log(self, player_id, season):
        def fetch():
            return endpoints.PlayerGameLog(
                player_id=player_id,
                season=season,
                timeout=self._config.timeout,
            ).get_data_frames()[0]

        return self._execute(
            "player_game_log",
            {"player_id": player_id, "season": season},
            fetch,
        )

    def get_team_game_log(self, team_id, season):
        def fetch():
            return endpoints.TeamGameLog(
                team_id=team_id,
                season=season,
                timeout=self._config.timeout,
            ).get_data_frames()[0]

        return self._execute(
            "team_game_log",
            {"team_id": team_id, "season": season},
            fetch,
        )

    def get_league_leaders(self, season, stat_category="PTS"):
        def fetch():
            return endpoints.LeagueLeaders(
                season=season,
                stat_category_abbreviation=stat_category,
                timeout=self._config.timeout,
            ).get_data_frames()[0]

        return self._execute(
            "league_leaders",
            {"season": season, "stat_category": stat_category},
            fetch,
        )

    def get_common_team_roster(self, team_id, season):
        def fetch():
            return endpoints.CommonTeamRoster(
                team_id=team_id,
                season=season,
                timeout=self._config.timeout,
            ).get_data_frames()[0]

        return self._execute(
            "common_team_roster",
            {"team_id": team_id, "season": season},
            fetch,
        )

    def get_scoreboard(self):
        def fetch():
            board = scoreboard.ScoreBoard(timeout=self._config.timeout).get_dict()
            games = board.get("scoreboard", {}).get("games", [])
            return pd.DataFrame(games)

        # Live endpoint - do not cache
        return self._execute("scoreboard", {}, fetch, cacheable=False)

    # -------------------------
    # Async helpers
    # -------------------------
    async def get_player_career_stats_async(self, player_id):
        return await self._execute_async(
            "player_career_stats",
            {"player_id": player_id},
            lambda: self.get_player_career_stats(player_id),
        )

    async def get_player_game_log_async(self, player_id, season):
        return await self._execute_async(
            "player_game_log",
            {"player_id": player_id, "season": season},
            lambda: self.get_player_game_log(player_id, season),
        )

    async def get_team_game_log_async(self, team_id, season):
        return await self._execute_async(
            "team_game_log",
            {"team_id": team_id, "season": season},
            lambda: self.get_team_game_log(team_id, season),
        )

    async def get_common_team_roster_async(self, team_id, season):
        return await self._execute_async(
            "common_team_roster",
            {"team_id": team_id, "season": season},
            lambda: self.get_common_team_roster(team_id, season),
        )

    async def get_league_leaders_async(self, season, stat_category="PTS"):
        return await self._execute_async(
            "league_leaders",
            {"season": season, "stat_category": stat_category},
            lambda: self.get_league_leaders(season, stat_category=stat_category),
        )

    async def get_scoreboard_async(self):
        return await self._execute_async("scoreboard", {}, self.get_scoreboard)

    async def get_player_game_logs_batch_async(
        self,
        player_ids: Iterable[int],
        season: str,
    ) -> dict[int, pd.DataFrame]:
        semaphore = asyncio.Semaphore(self._config.max_concurrency)

        async def fetch(player_id: int):
            async with semaphore:
                data = await self.get_player_game_log_async(player_id, season)
                return player_id, data

        tasks = [fetch(pid) for pid in player_ids]
        results = await asyncio.gather(*tasks)
        return {player_id: data for player_id, data in results}

    def get_player_game_logs_batch(
        self,
        player_ids: Iterable[int],
        season: str,
    ) -> dict[int, pd.DataFrame]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.get_player_game_logs_batch_async(player_ids, season))

        logger.warning(
            "Async loop already running; falling back to sequential NBA API calls."
        )
        return {pid: self.get_player_game_log(pid, season) for pid in player_ids}


nba_client = NBAApiClient()
