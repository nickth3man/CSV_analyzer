"""NBA API Client wrapper with rate limiting and retry logic.

This module provides a robust client for making requests to the NBA Stats API,
built on top of the nba_api package. It includes:

- Rate limiting to avoid API throttling
- Exponential backoff retry logic
- Proper error handling and logging
- Static data access (players, teams)

"""

import logging
import random
import time
import json
from collections.abc import Callable
from functools import wraps
from typing import Any

import pandas as pd
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import HTTPError, Timeout
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.scripts.populate.config import NBAAPIConfig, get_api_config


# Configure logging
logger = logging.getLogger(__name__)

RETRYABLE_EXCEPTIONS = (
    Timeout,
    RequestsConnectionError,
    HTTPError,
    OSError,
    json.decoder.JSONDecodeError,
)


# =============================================================================
# RETRY DECORATOR
# =============================================================================


def create_retry_decorator(
    max_retries: int = 3,
    backoff_multiplier: float = 2.0,
    min_wait: float = 1.0,
    max_wait: float = 60.0,
    retry_exceptions: tuple[type[Exception], ...] = RETRYABLE_EXCEPTIONS,
):
    """Create a tenacity retry decorator with exponential backoff."""
    return retry(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(
            multiplier=backoff_multiplier,
            min=min_wait,
            max=max_wait,
        ),
        retry=retry_if_exception_type(retry_exceptions),
        reraise=True,
    )


def with_retry(
    func: Callable | None = None,
    *,
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    base_delay: float = 0.6,
    retry_exceptions: tuple = (Exception,),
) -> Callable:
    """Decorator for retrying API calls with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Multiplier for exponential backoff
        base_delay: Initial delay between requests
        retry_exceptions: Tuple of exceptions to catch and retry

    Returns:
        Decorated function with retry logic
    """

    def decorator(target: Callable) -> Callable:
        @wraps(target)
        def wrapper(*args, **kwargs):
            last_exception = None
            config = None
            if args and hasattr(args[0], "config"):
                config = args[0].config

            effective_max_retries = (
                config.max_retries if config is not None else max_retries
            )
            effective_backoff = (
                config.retry_backoff_factor if config is not None else backoff_factor
            )
            effective_delay = config.request_delay if config is not None else base_delay

            for attempt in range(effective_max_retries):
                try:
                    # Rate limiting delay
                    if attempt > 0:
                        wait_time = (
                            effective_delay * (effective_backoff**attempt)
                        ) + random.uniform(
                            0,
                            effective_delay,
                        )
                        logger.debug(
                            f"Retry {attempt}/{effective_max_retries}, waiting {wait_time:.1f}s",
                        )
                        time.sleep(wait_time)
                    else:
                        time.sleep(effective_delay)

                    return target(*args, **kwargs)

                except retry_exceptions as e:
                    last_exception = e
                    error_str = str(e).lower()
                    status_code = getattr(
                        getattr(e, "response", None), "status_code", None
                    )

                    # Check for rate limiting
                    if (
                        "rate" in error_str
                        or "429" in error_str
                        or "timeout" in error_str
                        or status_code in {429, 500, 502, 503, 504}
                    ):
                        logger.warning(f"Rate limited on attempt {attempt + 1}")
                        continue

                    # Check for not found (expected for some players/seasons)
                    if "not found" in error_str or "404" in error_str:
                        logger.debug(f"Resource not found: {e}")
                        return None

                    # Log other errors
                    logger.warning(f"Attempt {attempt + 1} failed: {e}")

                    # Don't retry on final attempt
                    if attempt >= effective_max_retries - 1:
                        logger.exception(f"All {effective_max_retries} attempts failed")
                        raise

            # Should not reach here, but just in case
            if last_exception:
                raise last_exception
            return None

        return wrapper

    if func is not None and callable(func):
        return decorator(func)

    return decorator


# =============================================================================
# NBA API CLIENT
# =============================================================================


class NBAClient:
    """Client for NBA Stats API with rate limiting and retry logic.

    This client wraps the nba_api package endpoints and provides:
    - Consistent rate limiting across all requests
    - Automatic retry with exponential backoff
    - Easy access to static player/team data
    - DataFrame conversion for all responses

    Usage:
        client = NBAClient()

        # Get all active players
        players = client.get_active_players()

        # Get player game log
        games = client.get_player_game_log(player_id=2544, season="2023-24")

        # Get league-wide player game logs for a season
        all_games = client.get_player_game_logs(season="2023-24")
    """

    def __init__(self, config: NBAAPIConfig | None = None) -> None:
        """Initialize the NBA API client.

        Args:
            config: Optional API configuration. If not provided,
                   default config with environment overrides is used.
        """
        self.config = config or get_api_config()
        self._validate_nba_api_installed()

    def _validate_nba_api_installed(self) -> None:
        """Check that nba_api is installed and importable."""
        try:
            import importlib.util

            if importlib.util.find_spec("nba_api") is None:
                raise ImportError("nba_api package not found")  # noqa: TRY301
        except ImportError as e:
            raise ImportError(
                "nba_api package is required. Install with: pip install nba_api",
            ) from e

    @staticmethod
    def _non_empty_frame(df: pd.DataFrame) -> pd.DataFrame | None:
        """Return the DataFrame when non-empty, otherwise None."""
        if df.empty:
            return None
        return df

    # -------------------------------------------------------------------------
    # STATIC DATA (NO API CALLS)
    # -------------------------------------------------------------------------

    def get_all_players(self) -> list[dict[str, Any]]:
        """Get all NBA players from static data.

        This uses the embedded static data in nba_api, which means no HTTP
        requests are made. The data is periodically updated in the package.

        Returns:
            List of player dictionaries with keys:
            - id: Player ID
            - full_name: Full player name
            - first_name: First name
            - last_name: Last name
            - is_active: Whether currently active

        Reference:
            nba_api/stats/static/players.py
        """
        from nba_api.stats.static import players

        return players.get_players()

    def get_active_players(self) -> list[dict[str, Any]]:
        """Get all currently active NBA players.

        Returns:
            List of active player dictionaries
        """
        from nba_api.stats.static import players

        return players.get_active_players()

    def get_inactive_players(self) -> list[dict[str, Any]]:
        """Get all inactive (retired/not current) NBA players.

        Returns:
            List of inactive player dictionaries
        """
        from nba_api.stats.static import players

        return players.get_inactive_players()

    def find_player_by_id(self, player_id: int) -> dict[str, Any] | None:
        """Find a player by their NBA ID.

        Args:
            player_id: NBA player ID

        Returns:
            Player dictionary or None if not found
        """
        from nba_api.stats.static import players

        return players.find_player_by_id(player_id)

    def find_players_by_name(self, name: str) -> list[dict[str, Any]]:
        """Find players by name (regex pattern).

        Args:
            name: Name pattern to search (case insensitive)

        Returns:
            List of matching player dictionaries
        """
        from nba_api.stats.static import players

        return players.find_players_by_full_name(name)

    def get_all_teams(self) -> list[dict[str, Any]]:
        """Get all NBA teams from static data.

        Returns:
            List of team dictionaries with keys:
            - id: Team ID
            - full_name: Full team name
            - abbreviation: Team abbreviation (e.g., "LAL")
            - nickname: Team nickname (e.g., "Lakers")
            - city: Team city
            - state: Team state
            - year_founded: Year team was founded

        Reference:
            nba_api/stats/static/teams.py
        """
        from nba_api.stats.static import teams

        return teams.get_teams()

    def find_team_by_id(self, team_id: int) -> dict[str, Any] | None:
        """Find a team by their NBA ID.

        Args:
            team_id: NBA team ID

        Returns:
            Team dictionary or None if not found
        """
        from nba_api.stats.static import teams

        return teams.find_team_name_by_id(team_id)

    def find_team_by_abbreviation(self, abbrev: str) -> dict[str, Any] | None:
        """Find a team by abbreviation.

        Args:
            abbrev: Team abbreviation (e.g., "LAL")

        Returns:
            Team dictionary or None if not found
        """
        from nba_api.stats.static import teams

        return teams.find_team_by_abbreviation(abbrev)

    # -------------------------------------------------------------------------
    # API ENDPOINTS
    # -------------------------------------------------------------------------

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_game_log(
        self,
        player_id: int,
        season: str,
        season_type: str = "Regular Season",
    ) -> pd.DataFrame | None:
        """Get game log for a specific player and season.

        Args:
            player_id: NBA player ID
            season: Season string (e.g., "2023-24")
            season_type: Season type (Regular Season, Playoffs, etc.)

        Returns:
            DataFrame with game log data or None if no data

        Reference:
            nba_api/stats/endpoints/playergamelog.py

        API Fields:
            SEASON_ID, Player_ID, Game_ID, GAME_DATE, MATCHUP, WL,
            MIN, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT,
            OREB, DREB, REB, AST, STL, BLK, TOV, PF, PTS, PLUS_MINUS
        """
        from nba_api.stats.endpoints import playergamelog

        log = playergamelog.PlayerGameLog(
            player_id=player_id,
            season=season,
            season_type_all_star=season_type,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = log.get_data_frames()[0]
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_game_logs(
        self,
        season: str,
        season_type: str = "Regular Season",
        player_id: int | None = None,
        team_id: int | None = None,
    ) -> pd.DataFrame | None:
        """Get game logs for all players in a season (bulk query).

        This is more efficient than calling get_player_game_log() for each
        player individually, as it returns all player game logs in one request.

        Args:
            season: Season string (e.g., "2023-24")
            season_type: Season type (Regular Season, Playoffs, etc.)
            player_id: Optional player ID to filter by
            team_id: Optional team ID to filter by

        Returns:
            DataFrame with all player game logs or None if no data

        Reference:
            nba_api/stats/endpoints/playergamelogs.py

        API Fields:
            SEASON_YEAR, PLAYER_ID, PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION,
            TEAM_NAME, GAME_ID, GAME_DATE, MATCHUP, WL, MIN, FGM, FGA, FG_PCT,
            FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT, OREB, DREB, REB, AST, TOV,
            STL, BLK, BLKA, PF, PFD, PTS, PLUS_MINUS, NBA_FANTASY_PTS, DD2, TD3
        """
        from nba_api.stats.endpoints import playergamelogs

        logs = playergamelogs.PlayerGameLogs(
            season_nullable=season,
            season_type_nullable=season_type,
            player_id_nullable=str(player_id) if player_id else "",
            team_id_nullable=str(team_id) if team_id else "",
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = logs.get_data_frames()[0]
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_league_game_log(
        self,
        season: str,
        season_type: str = "Regular Season",
        player_or_team: str = "P",  # "P" for Player, "T" for Team
    ) -> pd.DataFrame | None:
        """Get league-wide game logs for a season.

        Args:
            season: Season string (e.g., "2023-24")
            season_type: Season type (Regular Season, Playoffs, etc.)
            player_or_team: "P" for player stats, "T" for team stats

        Returns:
            DataFrame with league game logs or None if no data

        Reference:
            nba_api/stats/endpoints/leaguegamelog.py
        """
        from nba_api.stats.endpoints import leaguegamelog

        log = leaguegamelog.LeagueGameLog(
            season=season,
            season_type_all_star=season_type,
            player_or_team_abbreviation=player_or_team,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = log.get_data_frames()[0]
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_career_stats(
        self,
        player_id: int,
    ) -> dict[str, pd.DataFrame] | None:
        """Get career statistics for a player.

        Args:
            player_id: NBA player ID

        Returns:
            Dictionary of DataFrames with career stats by type:
            - season_totals_regular_season
            - career_totals_regular_season
            - season_totals_post_season
            - career_totals_post_season
            - etc.

        Reference:
            nba_api/stats/endpoints/playercareerstats.py
        """
        from nba_api.stats.endpoints import playercareerstats

        career = playercareerstats.PlayerCareerStats(
            player_id=player_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        return {
            "season_totals_regular_season": career.season_totals_regular_season.get_data_frame(),
            "career_totals_regular_season": career.career_totals_regular_season.get_data_frame(),
            "season_totals_post_season": career.season_totals_post_season.get_data_frame(),
            "career_totals_post_season": career.career_totals_post_season.get_data_frame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_common_player_info(
        self,
        player_id: int,
    ) -> pd.DataFrame | None:
        """Get detailed player information.

        Args:
            player_id: NBA player ID

        Returns:
            DataFrame with player info or None if not found

        Reference:
            nba_api/stats/endpoints/commonplayerinfo.py
        """
        from nba_api.stats.endpoints import commonplayerinfo

        info = commonplayerinfo.CommonPlayerInfo(
            player_id=player_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = info.common_player_info.get_data_frame()
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_team_info_common(
        self,
        team_id: int,
        season: str | None = None,
        season_type: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Get team info and season ranks for a team/season."""
        from nba_api.stats.endpoints import teaminfocommon

        info = teaminfocommon.TeamInfoCommon(
            team_id=team_id,
            league_id="00",
            season_nullable=season or "",
            season_type_nullable=season_type or "",
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        return {
            "team_info_common": info.team_info_common.get_data_frame(),
            "team_season_ranks": info.team_season_ranks.get_data_frame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_team_details(self, team_id: int) -> dict[str, pd.DataFrame]:
        """Get team details, history, and social links."""
        from nba_api.stats.endpoints import teamdetails

        details = teamdetails.TeamDetails(
            team_id=team_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        frames = details.get_data_frames()
        return {
            "team_details": frames[0] if len(frames) > 0 else pd.DataFrame(),
            "team_history": frames[1] if len(frames) > 1 else pd.DataFrame(),
            "team_social": frames[2] if len(frames) > 2 else pd.DataFrame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_team_game_log(
        self,
        team_id: int,
        season: str,
        season_type: str = "Regular Season",
    ) -> pd.DataFrame | None:
        """Get game log for a specific team and season.

        Args:
            team_id: NBA team ID
            season: Season string (e.g., "2023-24")
            season_type: Season type (Regular Season, Playoffs, etc.)

        Returns:
            DataFrame with team game log data or None if no data

        Reference:
            nba_api/stats/endpoints/teamgamelog.py
        """
        from nba_api.stats.endpoints import teamgamelog

        log = teamgamelog.TeamGameLog(
            team_id=team_id,
            season=season,
            season_type_all_star=season_type,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = log.team_game_log.get_data_frame()
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_draft_history(
        self,
        season: str | None = None,
        team_id: int | None = None,
        round_num: int | None = None,
        round_pick: int | None = None,
        overall_pick: int | None = None,
        college: str | None = None,
        topx: int | None = None,
    ) -> pd.DataFrame | None:
        """Get draft history records."""
        from nba_api.stats.endpoints import drafthistory

        history = drafthistory.DraftHistory(
            league_id="00",
            season_year_nullable=season or "",
            team_id_nullable=str(team_id) if team_id else "",
            round_num_nullable=str(round_num) if round_num else "",
            round_pick_nullable=str(round_pick) if round_pick else "",
            overall_pick_nullable=str(overall_pick) if overall_pick else "",
            college_nullable=college or "",
            topx_nullable=str(topx) if topx else "",
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = history.draft_history.get_data_frame()
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_draft_combine_stats(self, season: str) -> pd.DataFrame | None:
        """Get draft combine stats for a season (season_all_time format)."""
        from nba_api.stats.endpoints import draftcombinestats

        stats = draftcombinestats.DraftCombineStats(
            league_id="00",
            season_all_time=season,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = stats.draft_combine_stats.get_data_frame()
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_league_game_finder(
        self,
        player_or_team: str = "T",
        season: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        team_id: int | None = None,
        player_id: int | None = None,
        **kwargs,
    ) -> pd.DataFrame | None:
        """Find games based on various filters using LeagueGameFinder.

        This is one of the most powerful endpoints for finding specific games
        based on numerous criteria including date ranges, teams, players, stats, etc.

        Args:
            player_or_team: "P" for player stats, "T" for team stats
            season: Season string (e.g., "2023-24")
            date_from: Start date in YYYY-MM-DD format
            date_to: End date in YYYY-MM-DD format
            team_id: Team ID to filter by
            player_id: Player ID to filter by
            **kwargs: Additional filter parameters (see NBA API docs)

        Returns:
            DataFrame with matching games or None if no data

        Reference:
            nba_api/stats/endpoints/leaguegamefinder.py

        Examples:
            # Get all Lakers games in 2023-24
            games = client.get_league_game_finder(
                player_or_team="T",
                team_id=1610612747,  # Lakers
                season="2023-24"
            )

            # Get all games in January 2024
            games = client.get_league_game_finder(
                player_or_team="T",
                date_from="2024-01-01",
                date_to="2024-01-31"
            )
        """
        from nba_api.stats.endpoints import leaguegamefinder

        # Build parameters
        params = {
            "player_or_team_abbreviation": player_or_team,
            "proxy": self.config.proxy,
            "headers": self.config.headers,
            "timeout": self.config.timeout,
        }

        # Add optional parameters
        if season:
            params["season_nullable"] = season
        if date_from:
            params["date_from_nullable"] = date_from
        if date_to:
            params["date_to_nullable"] = date_to
        if team_id:
            params["team_id_nullable"] = str(team_id)
        if player_id:
            params["player_id_nullable"] = str(player_id)

        # Add any additional parameters
        params.update(kwargs)

        finder = leaguegamefinder.LeagueGameFinder(**params)
        df = finder.league_game_finder_results.get_data_frame()

        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_boxscore_traditional(
        self,
        game_id: str,
    ) -> dict[str, pd.DataFrame] | None:
        """Get traditional box score statistics for a game.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            Dictionary of DataFrames containing:
            - player_stats: Player-level statistics
            - team_stats: Team-level statistics
            - team_starter_bench_stats: Starter/bench breakdown

        Reference:
            nba_api/stats/endpoints/boxscoretraditionalv3.py

        API Fields Include:
            - Basic stats: MIN, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT
            - Rebounds: OREB, DREB, REB
            - Other: AST, STL, BLK, TOV, PF, PTS, PLUS_MINUS
        """
        from nba_api.stats.endpoints import boxscoretraditionalv3

        boxscore = boxscoretraditionalv3.BoxScoreTraditionalV3(
            game_id=game_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        return {
            "player_stats": boxscore.player_stats.get_data_frame(),
            "team_stats": boxscore.team_stats.get_data_frame(),
            "team_starter_bench_stats": boxscore.team_starter_bench_stats.get_data_frame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_boxscore_advanced(
        self,
        game_id: str,
    ) -> dict[str, pd.DataFrame] | None:
        """Get advanced box score statistics for a game.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            Dictionary of DataFrames containing advanced statistics

        Reference:
            nba_api/stats/endpoints/boxscoreadvancedv3.py

        API Fields Include:
            - Advanced metrics: OFF_RATING, DEF_RATING, NET_RATING, AST_PCT, AST_TO
            - Usage: USG_PCT, PIE
            - Pace: PACE, PACE_PER40
            - Efficiency: EFG_PCT, TS_PCT
        """
        from nba_api.stats.endpoints import boxscoreadvancedv3

        boxscore = boxscoreadvancedv3.BoxScoreAdvancedV3(
            game_id=game_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        return {
            "player_stats": boxscore.player_stats.get_data_frame(),
            "team_stats": boxscore.team_stats.get_data_frame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_boxscore_four_factors(self, game_id: str) -> dict[str, pd.DataFrame] | None:
        """Get four factors box score for a game.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            Dictionary of DataFrames containing four factors statistics

        Reference:
            nba_api/stats/endpoints/boxscorefourfactorsv3.py

        API Fields Include:
            - EFG_PCT, FTA_RATE, TM_TOV_PCT, OREB_PCT
            - OPP_EFG_PCT, OPP_FTA_RATE, OPP_TOV_PCT, OPP_OREB_PCT
        """
        from nba_api.stats.endpoints import boxscorefourfactorsv3

        boxscore = boxscorefourfactorsv3.BoxScoreFourFactorsV3(
            game_id=game_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        return {
            "player_stats": boxscore.player_stats.get_data_frame(),
            "team_stats": boxscore.team_stats.get_data_frame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_boxscore_hustle(self, game_id: str) -> dict[str, pd.DataFrame] | None:
        """Get hustle box score statistics for a game.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            Dictionary of DataFrames containing hustle statistics

        Reference:
            nba_api/stats/endpoints/boxscorehustlev2.py

        API Fields Include:
            - DEFLECTIONS, LOOSE_BALLS_RECOVERED
            - CONTESTED_SHOTS, CHARGES_DRAWN
            - SCREEN_ASSISTS, BOX_OUTS
        """
        from nba_api.stats.endpoints import boxscorehustlev2

        boxscore = boxscorehustlev2.BoxScoreHustleV2(
            game_id=game_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        return {
            "player_stats": boxscore.player_stats.get_data_frame(),
            "team_stats": boxscore.team_stats.get_data_frame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_boxscore_misc(self, game_id: str) -> dict[str, pd.DataFrame] | None:
        """Get miscellaneous box score statistics for a game.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            Dictionary of DataFrames containing misc statistics

        Reference:
            nba_api/stats/endpoints/boxscoremiscv3.py

        API Fields Include:
            - PTS_OFF_TOV, PTS_2ND_CHANCE, PTS_FB, PTS_PAINT
            - OPP_PTS_OFF_TOV, OPP_PTS_2ND_CHANCE, etc.
        """
        from nba_api.stats.endpoints import boxscoremiscv3

        boxscore = boxscoremiscv3.BoxScoreMiscV3(
            game_id=game_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        return {
            "player_stats": boxscore.player_stats.get_data_frame(),
            "team_stats": boxscore.team_stats.get_data_frame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_boxscore_player_track(self, game_id: str) -> dict[str, pd.DataFrame] | None:
        """Get player tracking box score for a game.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            Dictionary of DataFrames containing player tracking statistics

        Reference:
            nba_api/stats/endpoints/boxscoreplayertrackv3.py

        API Fields Include:
            - SPD, DIST, ORBC, DRBC, RBC, TCHS, SAST, FTAST
            - PASS, AST, CFGM, CFGA, CFG_PCT, UFGM, UFGA, UFG_PCT
            - FG_PCT_DIFF, DFGM, DFGA, DFG_PCT
        """
        from nba_api.stats.endpoints import boxscoreplayertrackv3

        boxscore = boxscoreplayertrackv3.BoxScorePlayerTrackV3(
            game_id=game_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        return {
            "player_stats": boxscore.player_stats.get_data_frame(),
            "team_stats": boxscore.team_stats.get_data_frame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_boxscore_scoring(self, game_id: str) -> dict[str, pd.DataFrame] | None:
        """Get scoring breakdown box score for a game.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            Dictionary of DataFrames containing scoring statistics

        Reference:
            nba_api/stats/endpoints/boxscorescoringv3.py

        API Fields Include:
            - PCT_FGA_2PT, PCT_FGA_3PT, PCT_PTS_2PT, PCT_PTS_3PT
            - PCT_PTS_MR, PCT_PTS_FB, PCT_PTS_FT, PCT_PTS_OFF_TOV
            - PCT_PTS_PAINT, PCT_AST_2PM, PCT_UAST_2PM
        """
        from nba_api.stats.endpoints import boxscorescoringv3

        boxscore = boxscorescoringv3.BoxScoreScoringV3(
            game_id=game_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        return {
            "player_stats": boxscore.player_stats.get_data_frame(),
            "team_stats": boxscore.team_stats.get_data_frame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_boxscore_usage(self, game_id: str) -> dict[str, pd.DataFrame] | None:
        """Get usage rate box score for a game.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            Dictionary of DataFrames containing usage statistics

        Reference:
            nba_api/stats/endpoints/boxscoreusagev3.py

        API Fields Include:
            - USG_PCT, PCT_FGM, PCT_FGA, PCT_FG3M, PCT_FG3A
            - PCT_FTM, PCT_FTA, PCT_OREB, PCT_DREB, PCT_REB
            - PCT_AST, PCT_TOV, PCT_STL, PCT_BLK, PCT_PF, PCT_PTS
        """
        from nba_api.stats.endpoints import boxscoreusagev3

        boxscore = boxscoreusagev3.BoxScoreUsageV3(
            game_id=game_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        return {
            "player_stats": boxscore.player_stats.get_data_frame(),
            "team_stats": boxscore.team_stats.get_data_frame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_play_by_play(
        self,
        game_id: str,
    ) -> pd.DataFrame | None:
        """Get play-by-play data for a game.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            DataFrame with play-by-play data or None if not available

        Reference:
            nba_api/stats/endpoints/playbyplayv3.py

        API Fields Include:
            - Game context: PERIOD, PCTIMESTRING, SCOREMARGIN
            - Play details: HOMEDESCRIPTION, NEUTRALDESCRIPTION, VISITORDESCRIPTION
            - Player info: PLAYER1_ID, PLAYER2_ID, PLAYER3_ID
            - Event type: EVENTMSGTYPE, EVENTMSGACTIONTYPE
        """
        from nba_api.stats.endpoints import playbyplayv3

        pbp = playbyplayv3.PlayByPlayV3(
            game_id=game_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = pbp.play_by_play.get_data_frame()
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_shot_chart_detail(
        self,
        game_id: str | None = None,
        team_id: int = 0,
        player_id: int = 0,
        season: str | None = None,
        season_type: str = "Regular Season",
    ) -> pd.DataFrame | None:
        """Get detailed shot chart data for a game, player, or team.

        Args:
            game_id: NBA game ID (10-digit string), optional filter
            team_id: Team ID (required, use 0 for all teams)
            player_id: Player ID (required, use 0 for all players)
            season: Season string (e.g., "2024-25"), optional
            season_type: Season type (e.g., "Regular Season", "Playoffs")

        Returns:
            DataFrame with shot chart data or None if not available

        Reference:
            nba_api/stats/endpoints/shotchartdetail.py

        API Fields Include:
            - Location: LOC_X, LOC_Y
            - Shot details: SHOT_DISTANCE, SHOT_TYPE, SHOT_ZONE_BASIC, SHOT_ZONE_AREA
            - Context: PERIOD, MINUTES_REMAINING, SECONDS_REMAINING
            - Results: SHOT_MADE_FLAG, PTS_TYPE
        """
        from nba_api.stats.endpoints import shotchartdetail

        params: dict[str, Any] = {
            "team_id": team_id,
            "player_id": player_id,
            "season_type_all_star": season_type,
            "proxy": self.config.proxy,
            "headers": self.config.headers,
            "timeout": self.config.timeout,
        }

        # Add optional game_id filter
        if game_id:
            params["game_id_nullable"] = game_id

        # Add optional season filter
        if season:
            params["season_nullable"] = season

        shot_chart = shotchartdetail.ShotChartDetail(**params)

        df = shot_chart.shot_chart_detail.get_data_frame()
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_league_standings(
        self,
        season: str,
        season_type: str = "Regular Season",
    ) -> pd.DataFrame | None:
        """Get league standings for a season.

        Args:
            season: Season string (e.g., "2023-24")
            season_type: Season type (Regular Season, Playoffs, etc.)

        Returns:
            DataFrame with league standings or None if not available

        Reference:
            nba_api/stats/endpoints/leaguestandingsv3.py
        """
        from nba_api.stats.endpoints import leaguestandingsv3

        standings = leaguestandingsv3.LeagueStandingsV3(
            season=season,
            season_type=season_type,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = standings.standings.get_data_frame()
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_tracking_stats(
        self,
        season: str,
        season_type: str = "Regular Season",
        pt_measure_type: str = "SpeedDistance",
    ) -> pd.DataFrame | None:
        """Fetch player tracking statistics.

        Args:
            season: Season (e.g., "2024-25")
            season_type: Season type
            pt_measure_type: Tracking measure type:
                - SpeedDistance: Speed and distance traveled
                - Rebounding: Rebounding tracking
                - Possessions: Touches and time of possession
                - CatchShoot: Catch and shoot stats
                - PullUpShot: Pull-up shooting
                - Defense: Defensive tracking
                - Drives: Driving stats
                - Passing: Passing stats
                - ElbowTouch: Elbow touch stats
                - PostTouch: Post touch stats
                - PaintTouch: Paint touch stats

        Returns:
            DataFrame with player tracking stats

        Reference:
            nba_api/stats/endpoints/leaguedashptstats.py
        """
        from nba_api.stats.endpoints import leaguedashptstats

        endpoint = leaguedashptstats.LeagueDashPtStats(
            season=season,
            season_type_all_star=season_type,
            pt_measure_type=pt_measure_type,
            per_mode_simple="Totals",
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )
        df = endpoint.get_data_frames()[0]
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_hustle_stats(
        self,
        season: str,
        season_type: str = "Regular Season",
        player_or_team: str = "P",
    ) -> pd.DataFrame | None:
        """Get hustle statistics (screens, deflections, loose balls, etc.).

        Args:
            season: Season string (e.g., "2023-24")
            season_type: Season type (Regular Season, Playoffs, etc.)
            player_or_team: "P" for player stats, "T" for team stats

        Returns:
            DataFrame with hustle statistics or None if not available

        Reference:
            nba_api/stats/endpoints/leaguehustlestatsplayer.py

        API Fields Include:
            - Defense: DEFLECTIONS, CHARGES_DRAWN
            - Loose Balls: LOOSE_BALLS_RECOVERED, LOOSE_BALLS_RECOVERED_PCT
            - Screens: SCREEN_ASSISTS, SCREEN_AST_PTS
        """
        from nba_api.stats.endpoints import leaguehustlestatsplayer

        hustle = leaguehustlestatsplayer.LeagueHustleStatsPlayer(
            season=season,
            season_type_all_star=season_type,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        # Use get_data_frames() for better robustness and type checking
        dfs = hustle.get_data_frames()
        if not dfs:
            return None

        return dfs[0]

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_awards(
        self,
        player_id: int,
    ) -> pd.DataFrame | None:
        """Fetch player awards and achievements.

        Args:
            player_id: NBA player ID

        Returns:
            DataFrame with player awards (All-NBA, All-Star, MVP, etc.) or None if no data

        Reference:
            nba_api/stats/endpoints/playerawards.py

        API Fields Include:
            - PERSON_ID, FIRST_NAME, LAST_NAME, TEAM
            - DESCRIPTION (award description)
            - ALL_NBA_TEAM_NUMBER (1st/2nd/3rd team designation)
            - SEASON, MONTH, WEEK
            - CONFERENCE, TYPE, SUBTYPE
        """
        from nba_api.stats.endpoints import playerawards

        endpoint = playerawards.PlayerAwards(
            player_id=player_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = endpoint.get_data_frames()[0]
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_scoreboard(
        self,
        game_date: str | None = None,
        league_id: str = "00",
    ) -> pd.DataFrame | None:
        """Get scoreboard data for games on a specific date.

        Args:
            game_date: Date in YYYY-MM-DD format (defaults to today if None)
            league_id: League ID ("00" for NBA)

        Returns:
            DataFrame with scoreboard data or None if no games

        Reference:
            nba_api/stats/endpoints/scoreboardv3.py

        API Fields Include:
            - Game info: GAME_ID, GAME_STATUS_TEXT, GAME_STATUS
            - Teams: HOME_TEAM_ID, AWAY_TEAM_ID, HOME_TEAM_SCORE, AWAY_TEAM_SCORE
            - Time: GAME_TIME, GAME_ET
        """
        from nba_api.stats.endpoints import scoreboardv3

        params = {
            "league_id": league_id,
            "proxy": self.config.proxy,
            "headers": self.config.headers,
            "timeout": self.config.timeout,
        }

        if game_date:
            params["game_date"] = game_date

        # ScoreboardV3 naming in nba_api is often scoreboardv3.ScoreboardV3
        # Use getattr to be safe if naming varies across versions
        try:
            sb_class = getattr(scoreboardv3, "ScoreboardV3", None) or getattr(
                scoreboardv3, "ScoreBoardV3", None
            )
            if not sb_class:
                logger.error(
                    "Neither ScoreboardV3 nor ScoreBoardV3 found in scoreboardv3 module"
                )
                return None

            scoreboard = sb_class(**params)
            dfs = scoreboard.get_data_frames()
            if not dfs:
                return None
            return dfs[0]
        except Exception as e:
            logger.error(f"Error initializing ScoreboardV3: {e}")
            return None

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_matchup_stats(
        self,
        season: str,
        season_type: str = "Regular Season",
        def_player_id_nullable: str = "",
        off_player_id_nullable: str = "",
    ) -> pd.DataFrame | None:
        """Fetch player defensive matchup statistics.

        Args:
            season: Season (e.g., "2024-25")
            season_type: Season type
            def_player_id_nullable: Defensive player ID filter (optional)
            off_player_id_nullable: Offensive player ID filter (optional)

        Returns:
            DataFrame with matchup stats showing how players perform against specific defenders

        Reference:
            nba_api/stats/endpoints/leagueseasonmatchups.py

        API Fields Include:
            - Identifiers: OFF_PLAYER_ID, DEF_PLAYER_ID, OFF_PLAYER_NAME, DEF_PLAYER_NAME
            - Matchup stats: MATCHUP_MIN, PARTIAL_POSS, PLAYER_PTS, TEAM_PTS
            - Shot stats: MATCHUP_FGM, MATCHUP_FGA, MATCHUP_FG_PCT
            - Other: MATCHUP_AST, MATCHUP_TOV
        """
        from nba_api.stats.endpoints import leagueseasonmatchups

        endpoint = leagueseasonmatchups.LeagueSeasonMatchups(
            season=season,
            season_type_all_star=season_type,
            def_player_id_nullable=def_player_id_nullable,
            off_player_id_nullable=off_player_id_nullable,
            per_mode_simple="Totals",
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = endpoint.get_data_frames()[0]
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_lineup_stats(
        self,
        season: str,
        season_type: str = "Regular Season",
        group_quantity: int = 5,
        measure_type: str = "Base",
    ) -> pd.DataFrame | None:
        """Fetch lineup combination statistics.

        Args:
            season: Season (e.g., "2024-25")
            season_type: Season type
            group_quantity: Number of players in lineup (2, 3, 4, or 5)
            measure_type: Stat type (Base, Advanced, Misc, etc.)

        Returns:
            DataFrame with lineup stats including NET rating, +/-, minutes

        Reference:
            nba_api/stats/endpoints/leaguedashlineups.py

        API Fields Include:
            - Lineup info: GROUP_ID, GROUP_NAME, TEAM_ID, TEAM_ABBREVIATION
            - Minutes: MIN
            - Offensive: OFF_RATING, FG_PCT, FG3_PCT, FT_PCT
            - Defensive: DEF_RATING
            - Overall: NET_RATING, PLUS_MINUS, PACE
            - Counting stats: W, L, W_PCT, GP, FGM, FGA, etc.
        """
        from nba_api.stats.endpoints import leaguedashlineups

        endpoint = leaguedashlineups.LeagueDashLineups(
            season=season,
            season_type_all_star=season_type,
            group_quantity=group_quantity,
            measure_type_detailed_defense=measure_type,
            per_mode_detailed="Totals",
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )
        df = endpoint.get_data_frames()[0]
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_game_rotation(
        self,
        game_id: str,
    ) -> dict[str, pd.DataFrame] | None:
        """Get game rotation data showing player substitution patterns.

        This endpoint provides detailed rotation data for a specific game,
        including player in/out times, stint durations, and box score stats
        for each stint.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            Dictionary with 'home_team' and 'away_team' DataFrames containing
            rotation data for each team, or None if no data available.

        Reference:
            nba_api/stats/endpoints/gamerotation.py

        API Fields Include:
            - Identifiers: GAME_ID, TEAM_ID, TEAM_CITY, TEAM_NAME, PERSON_ID, PLAYER_NAME
            - Timing: IN_TIME_REAL, OUT_TIME_REAL, PLAYER_PTS
            - Box score: PTS, AST, REB, STL, BLK, TOV, FGM, FGA, FG3M, FG3A, FTM, FTA, PF, PLUS_MINUS
        """
        from nba_api.stats.endpoints import gamerotation

        rotation = gamerotation.GameRotation(
            game_id=game_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        frames = rotation.get_data_frames()
        return {
            "home_team": frames[0] if len(frames) > 0 else pd.DataFrame(),
            "away_team": frames[1] if len(frames) > 1 else pd.DataFrame(),
        }

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_clutch_splits(
        self,
        player_id: int,
        season: str,
        season_type: str = "Regular Season",
    ) -> dict[str, pd.DataFrame] | None:
        """Get clutch-time splits for a player.

        This endpoint provides performance breakdowns for different clutch
        situations (last 5 minutes, last 3 minutes, last 1 minute, etc.)
        with the score within various point margins.

        Args:
            player_id: NBA player ID
            season: Season string (e.g., "2024-25")
            season_type: Season type (Regular Season, Playoffs, etc.)

        Returns:
            Dictionary of DataFrames containing clutch splits by scenario,
            or None if no data available.

        Reference:
            nba_api/stats/endpoints/playerdashboardbyclutch.py

        API Fields Include:
            - Identifiers: GROUP_SET, GROUP_VALUE
            - Games: GP, W, L, W_PCT
            - Minutes: MIN
            - Shooting: FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT
            - Rebounds: OREB, DREB, REB
            - Other: AST, TOV, STL, BLK, BLKA, PF, PFD, PTS, PLUS_MINUS
        """
        from nba_api.stats.endpoints import playerdashboardbyclutch

        dashboard = playerdashboardbyclutch.PlayerDashboardByClutch(
            player_id=player_id,
            season=season,
            season_type_playoffs=season_type,
            per_mode_detailed="Totals",
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        dfs = dashboard.get_data_frames()
        if not dfs:
            return None

        # Return all available clutch scenario DataFrames
        result = {}
        # Index 0 is OverallPlayerDashboard, subsequent are clutch scenarios
        dataset_names = [
            "overall",
            "last_5min_lte_5pts",
            "last_3min_lte_5pts",
            "last_1min_lte_5pts",
            "last_30sec_lte_3pts",
            "last_10sec_lte_3pts",
        ]
        for idx, name in enumerate(dataset_names):
            if idx < len(dfs) and not dfs[idx].empty:
                result[name] = dfs[idx]

        return result if result else None

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_game_splits(
        self,
        player_id: int,
        season: str,
        season_type: str = "Regular Season",
    ) -> dict[str, pd.DataFrame] | None:
        """Get game splits (win/loss, home/away, etc.) for a player.

        This endpoint provides performance breakdowns by various game
        situations: by result, location, days rest, month, etc.

        Args:
            player_id: NBA player ID
            season: Season string (e.g., "2024-25")
            season_type: Season type (Regular Season, Playoffs, etc.)

        Returns:
            Dictionary of DataFrames containing game splits by category,
            or None if no data available.

        Reference:
            nba_api/stats/endpoints/playerdashboardbygamesplits.py

        API Fields Include:
            - Identifiers: GROUP_SET, GROUP_VALUE
            - Games: GP, W, L, W_PCT
            - Minutes: MIN
            - Shooting: FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT
            - Rebounds: OREB, DREB, REB
            - Other: AST, TOV, STL, BLK, BLKA, PF, PFD, PTS, PLUS_MINUS
        """
        from nba_api.stats.endpoints import playerdashboardbygamesplits

        dashboard = playerdashboardbygamesplits.PlayerDashboardByGameSplits(
            player_id=player_id,
            season=season,
            season_type_playoffs=season_type,
            per_mode_detailed="Totals",
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        dfs = dashboard.get_data_frames()
        if not dfs:
            return None

        # Return all available game split DataFrames
        result = {}
        dataset_names = [
            "overall",
            "by_half",
            "by_period",
            "by_score_margin",
            "by_actual_margin",
            "by_days_rest",
        ]
        for idx, name in enumerate(dataset_names):
            if idx < len(dfs) and not dfs[idx].empty:
                result[name] = dfs[idx]

        return result if result else None

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_last_n_games_splits(
        self,
        player_id: int,
        season: str,
        season_type: str = "Regular Season",
    ) -> dict[str, pd.DataFrame] | None:
        """Get last N games splits for a player (recent performance trends).

        This endpoint provides performance breakdowns over different
        rolling game windows: last 5, 10, 15, 20 games and game number ranges.

        Args:
            player_id: NBA player ID
            season: Season string (e.g., "2024-25")
            season_type: Season type (Regular Season, Playoffs, etc.)

        Returns:
            Dictionary of DataFrames containing last N games splits,
            or None if no data available.

        Reference:
            nba_api/stats/endpoints/playerdashboardbylastngames.py

        API Fields Include:
            - Identifiers: GROUP_SET, GROUP_VALUE
            - Games: GP, W, L, W_PCT
            - Minutes: MIN
            - Shooting: FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT
            - Rebounds: OREB, DREB, REB
            - Other: AST, TOV, STL, BLK, BLKA, PF, PFD, PTS, PLUS_MINUS
        """
        from nba_api.stats.endpoints import playerdashboardbylastngames

        dashboard = playerdashboardbylastngames.PlayerDashboardByLastNGames(
            player_id=player_id,
            season=season,
            season_type_playoffs=season_type,
            per_mode_detailed="Totals",
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        dfs = dashboard.get_data_frames()
        if not dfs:
            return None

        # Return all available last N games DataFrames
        result = {}
        dataset_names = [
            "overall",
            "last_5_games",
            "last_10_games",
            "last_15_games",
            "last_20_games",
            "game_number_range",
        ]
        for idx, name in enumerate(dataset_names):
            if idx < len(dfs) and not dfs[idx].empty:
                result[name] = dfs[idx]

        return result if result else None

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_shooting_splits(
        self,
        player_id: int,
        season: str,
        season_type: str = "Regular Season",
    ) -> dict[str, pd.DataFrame] | None:
        """Get shooting splits for a player by shot type and zone.

        This endpoint provides detailed shooting breakdowns by distance,
        area, assisted/unassisted, shot clock, dribbles, touch time, etc.

        Args:
            player_id: NBA player ID
            season: Season string (e.g., "2024-25")
            season_type: Season type (Regular Season, Playoffs, etc.)

        Returns:
            Dictionary of DataFrames containing shooting splits by category,
            or None if no data available.

        Reference:
            nba_api/stats/endpoints/playerdashboardbyshootingsplits.py

        API Fields Include:
            - Identifiers: GROUP_SET, GROUP_VALUE
            - Games: GP, G
            - Shooting: FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, EFG_PCT
            - Context: BLKA, PCT_AST_2PM, PCT_AST_3PM, PCT_UAST_2PM, PCT_UAST_3PM
        """
        from nba_api.stats.endpoints import playerdashboardbyshootingsplits

        dashboard = playerdashboardbyshootingsplits.PlayerDashboardByShootingSplits(
            player_id=player_id,
            season=season,
            season_type_playoffs=season_type,
            per_mode_detailed="Totals",
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        dfs = dashboard.get_data_frames()
        if not dfs:
            return None

        # Return all available shooting split DataFrames
        result = {}
        dataset_names = [
            "overall",
            "shot_5ft",
            "shot_8ft",
            "shot_area",
            "assisted_by",
            "shot_type",
            "assisted_shot",
            "shot_clock",
            "dribbles",
            "touch_time",
        ]
        for idx, name in enumerate(dataset_names):
            if idx < len(dfs) and not dfs[idx].empty:
                result[name] = dfs[idx]

        return result if result else None

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_estimated_metrics(
        self,
        season: str,
        season_type: str = "Regular Season",
    ) -> pd.DataFrame | None:
        """Get estimated metrics for all players.

        This endpoint provides player-level estimated metrics including offensive
        rating, defensive rating, net rating, and various other advanced metrics
        that are estimated using play-by-play data.

        Args:
            season: Season string (e.g., "2024-25")
            season_type: Season type ("Regular Season", "Playoffs", etc.)

        Returns:
            DataFrame with player estimated metrics or None if no data

        Reference:
            nba_api/stats/endpoints/playerestimatedmetrics.py

        API Fields Include:
            - Identifiers: PLAYER_ID, PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION
            - Games: GP, W, L, W_PCT, MIN
            - Estimated Metrics: E_OFF_RATING, E_DEF_RATING, E_NET_RATING
            - Advanced: E_AST_RATIO, E_OREB_PCT, E_DREB_PCT, E_REB_PCT
            - Efficiency: E_TOV_PCT, E_EFG_PCT, E_TS_PCT, E_USG_PCT
            - Other: E_PACE, E_PIE
        """
        from nba_api.stats.endpoints import playerestimatedmetrics

        metrics = playerestimatedmetrics.PlayerEstimatedMetrics(
            season=season,
            season_type=season_type,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )
        df = metrics.get_data_frames()[0]
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_team_estimated_metrics(
        self,
        season: str,
        season_type: str = "Regular Season",
    ) -> pd.DataFrame | None:
        """Get estimated metrics for all teams.

        This endpoint provides team-level estimated metrics including offensive
        rating, defensive rating, net rating, and various other advanced metrics
        that are estimated using play-by-play data.

        Args:
            season: Season string (e.g., "2024-25")
            season_type: Season type ("Regular Season", "Playoffs", etc.)

        Returns:
            DataFrame with team estimated metrics or None if no data

        Reference:
            nba_api/stats/endpoints/teamestimatedmetrics.py

        API Fields Include:
            - Identifiers: TEAM_ID, TEAM_NAME, TEAM_ABBREVIATION
            - Games: GP, W, L, W_PCT, MIN
            - Estimated Metrics: E_OFF_RATING, E_DEF_RATING, E_NET_RATING
            - Advanced: E_AST_RATIO, E_OREB_PCT, E_DREB_PCT, E_REB_PCT
            - Efficiency: E_TOV_PCT, E_EFG_PCT, E_TS_PCT
            - Other: E_PACE, E_PIE
        """
        from nba_api.stats.endpoints import teamestimatedmetrics

        metrics = teamestimatedmetrics.TeamEstimatedMetrics(
            season=season,
            season_type=season_type,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )
        df = metrics.get_data_frames()[0]
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_synergy_playtypes(
        self,
        season: str,
        season_type: str = "Regular Season",
        play_type: str = "Isolation",
        type_grouping: str = "offensive",
        player_or_team: str = "P",
    ) -> pd.DataFrame | None:
        """Fetch Synergy Play Type efficiency data.

        This endpoint provides detailed play type efficiency statistics including
        points per possession, percentile rankings, and shooting breakdowns for
        various play types like Isolation, Transition, Pick & Roll, etc.

        Args:
            season: Season (e.g., "2024-25")
            season_type: Season type ("Regular Season", "Playoffs", etc.)
            play_type: Play type category. Valid values:
                - "Isolation": One-on-one plays
                - "Transition": Fast break plays
                - "PRBallHandler": Pick and roll ball handler
                - "PRRollman": Pick and roll roll man
                - "Postup": Post up plays
                - "Spotup": Spot up shooting
                - "Handoff": Handoff plays
                - "Cut": Cutting plays
                - "OffScreen": Off screen plays
                - "Putbacks": Putback plays
                - "Misc": Miscellaneous plays
            type_grouping: "offensive" or "defensive" stats
            player_or_team: "P" for player stats, "T" for team stats

        Returns:
            DataFrame with Synergy play type stats or None if no data

        Reference:
            nba_api/stats/endpoints/synergyplaytypes.py

        API Fields Include:
            - Identifiers: PLAYER_ID/TEAM_ID, PLAYER_NAME/TEAM_NAME, TEAM_ABBREVIATION
            - Games: GP (games played)
            - Possessions: POSS, POSS_PCT (possession percentage)
            - Scoring: PTS, FGM, FGA, FG_PCT, EFG_PCT
            - Possession Outcomes: FT_POSS_PCT, TOV_POSS_PCT, SF_POSS_PCT,
              PLUSONE_POSS_PCT, SCORE_POSS_PCT
            - Efficiency: PPP (points per possession), PERCENTILE
        """
        from nba_api.stats.endpoints import synergyplaytypes

        endpoint = synergyplaytypes.SynergyPlayTypes(
            season=season,
            season_type_all_star=season_type,
            play_type_nullable=play_type,
            type_grouping_nullable=type_grouping,
            player_or_team_abbreviation=player_or_team,
            per_mode_simple="Totals",
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = endpoint.get_data_frames()[0]
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_franchise_history(self, league_id: str = "00") -> pd.DataFrame | None:
        """Get franchise history for all teams.

        Returns historical franchise information including name changes,
        city relocations, and years active.

        Args:
            league_id: League ID ("00" for NBA)

        Returns:
            DataFrame with franchise history or None if no data

        Reference:
            nba_api/stats/endpoints/franchisehistory.py

        API Fields Include:
            - Identifiers: LEAGUE_ID, TEAM_ID, TEAM_CITY, TEAM_NAME
            - Years: START_YEAR, END_YEAR, YEARS
            - Performance: GAMES, WINS, LOSSES, WIN_PCT, PO_APPEARANCES, DIV_TITLES, CONF_TITLES, LEAGUE_TITLES
        """
        from nba_api.stats.endpoints import franchisehistory

        history = franchisehistory.FranchiseHistory(
            league_id=league_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = history.franchise_history.get_data_frame()
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_franchise_leaders(self, team_id: int) -> pd.DataFrame | None:
        """Get all-time statistical leaders for a franchise.

        Returns career leaders for the specified team in various categories.

        Args:
            team_id: NBA team ID

        Returns:
            DataFrame with franchise leaders or None if no data

        Reference:
            nba_api/stats/endpoints/franchiseleaders.py

        API Fields Include:
            - Identifiers: TEAM_ID, PLAYER_ID, PLAYER
            - Points: PTS, PTS_RANK
            - Assists: AST, AST_RANK
            - Rebounds: REB, REB_RANK
            - Blocks: BLK, BLK_RANK
            - Steals: STL, STL_RANK
            - Games: FGM, FGA, FG3M, FG3A, FTM, FTA, GP
        """
        from nba_api.stats.endpoints import franchiseleaders

        leaders = franchiseleaders.FranchiseLeaders(
            team_id=team_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = leaders.franchise_leaders.get_data_frame()
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_league_leaders(
        self,
        season: str,
        season_type: str = "Regular Season",
        stat_category: str = "PTS",
        per_mode: str = "Totals",
        scope: str = "S",
    ) -> pd.DataFrame | None:
        """Get league leaders for a specific stat category.

        This endpoint returns the top players in a given statistical category
        for a specific season. Useful for identifying league-leading performers.

        Args:
            season: Season string (e.g., "2024-25")
            season_type: Season type ("Regular Season", "Playoffs", etc.)
            stat_category: Stat to rank by. Valid values:
                - "PTS": Points
                - "AST": Assists
                - "REB": Rebounds
                - "STL": Steals
                - "BLK": Blocks
                - "FGM": Field Goals Made
                - "FG3M": Three Pointers Made
                - "FTM": Free Throws Made
                - "EFF": Efficiency
                - "MIN": Minutes
            per_mode: Stat aggregation mode:
                - "Totals": Total stats for the season
                - "PerGame": Per-game averages
                - "Per48": Per-48-minute stats
            scope: Player scope:
                - "S": All players (Season)
                - "Rookies": Rookies only

        Returns:
            DataFrame with league leaders for the stat category or None if no data

        Reference:
            nba_api/stats/endpoints/leagueleaders.py

        API Fields Include:
            - Identifiers: PLAYER_ID, PLAYER_NAME, TEAM_ID, TEAM_ABBREVIATION, RANK
            - Games: GP, MIN
            - Shooting: FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT
            - Rebounds: OREB, DREB, REB
            - Other: AST, STL, BLK, TOV, PF, PTS, EFF, AST_TOV, STL_TOV
        """
        from nba_api.stats.endpoints import leagueleaders

        leaders = leagueleaders.LeagueLeaders(
            season=season,
            season_type_all_star=season_type,
            stat_category_abbreviation=stat_category,
            per_mode48=per_mode,
            scope=scope,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        df = leaders.league_leaders.get_data_frame()
        return self._non_empty_frame(df)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_all_time_leaders(
        self,
        per_mode: str = "Totals",
        top_x: int = 10,
    ) -> pd.DataFrame | None:
        """Get all-time statistical leaders across NBA history.

        This endpoint returns the top N players in each major statistical
        category across all-time NBA history. Useful for historical comparisons.

        Args:
            per_mode: Stat aggregation mode:
                - "Totals": Career totals
                - "PerGame": Career per-game averages
            top_x: Number of top players to return per category (default: 10)

        Returns:
            DataFrame with all-time leaders combined from all stat categories,
            or None if no data available

        Reference:
            nba_api/stats/endpoints/alltimeleadersgrids.py

        API Fields Include:
            - Identifiers: PLAYER_ID, PLAYER_NAME, TEAM_ID
            - Stats: The specific stat value for the category
            - Seasons: ACTIVE_WITH (team currently playing for), SEASON_COUNT
            - STAT_CATEGORY: Added field indicating which category (PTS, AST, etc.)
        """
        from nba_api.stats.endpoints import alltimeleadersgrids

        leaders = alltimeleadersgrids.AllTimeLeadersGrids(
            per_mode_simple=per_mode,
            topx=top_x,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        # This endpoint returns multiple result sets (one per stat category)
        dfs = leaders.get_data_frames()
        if not dfs:
            return None

        # Combine all result sets with stat category labels
        combined = []
        stat_categories = [
            "GP",
            "PTS",
            "AST",
            "REB",
            "STL",
            "BLK",
            "FGM",
            "FGA",
            "FG3M",
            "FG3A",
            "FTM",
            "FTA",
            "OREB",
            "DREB",
            "TOV",
            "PF",
        ]

        for i, df in enumerate(dfs):
            if df is not None and not df.empty:
                if i < len(stat_categories):
                    df = df.copy()
                    df["STAT_CATEGORY"] = stat_categories[i]
                combined.append(df)

        if not combined:
            return None

        return pd.concat(combined, ignore_index=True)

    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_win_probability(self, game_id: str) -> pd.DataFrame | None:
        """Get win probability at each play for a game.

        This endpoint provides real-time win probability for each play in a game,
        useful for clutch analysis, momentum tracking, and game flow visualization.

        Args:
            game_id: NBA game ID (10-digit string)

        Returns:
            DataFrame with win probability at each play, or None if not available

        Reference:
            nba_api/stats/endpoints/winprobabilitypbp.py

        API Fields Include:
            - GAME_ID, EVENT_NUM, HOME_PCT, VISITOR_PCT
            - HOME_PTS, VISITOR_PTS, HOME_SCORE_MARGIN
            - SECONDS_REMAINING, PERIOD, DESCRIPTION
            - LOCATION, EVENTMSGTYPE, PLAYER_ID, TEAM_ID
        """
        from nba_api.stats.endpoints import winprobabilitypbp

        wp = winprobabilitypbp.WinProbabilityPBP(
            game_id=game_id,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )

        # WinProbabilityPBP returns data in first DataFrame
        dfs = wp.get_data_frames()
        if not dfs:
            return None
        return self._non_empty_frame(dfs[0])


# =============================================================================
# MODULE-LEVEL CLIENT INSTANCE
# =============================================================================

# Singleton client instance for convenience
_client: NBAClient | None = None


def get_client() -> NBAClient:
    """Get or create the singleton NBA API client.

    Returns:
        NBAClient instance
    """
    global _client
    if _client is None:
        _client = NBAClient()
    return _client
