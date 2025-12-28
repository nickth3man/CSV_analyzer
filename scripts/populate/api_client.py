"""NBA API Client wrapper with rate limiting and retry logic.

This module provides a robust client for making requests to the NBA Stats API,
built on top of the nba_api package. It includes:

- Rate limiting to avoid API throttling
- Exponential backoff retry logic
- Proper error handling and logging
- Static data access (players, teams)

Based on nba_api patterns from:
- reference/nba_api/src/nba_api/stats/endpoints/
- reference/nba_api/src/nba_api/stats/static/
"""

import logging
import time
from typing import Optional, List, Dict, Any, Callable
from functools import wraps

import pandas as pd

from .config import NBAAPIConfig, get_api_config

# Configure logging
logger = logging.getLogger(__name__)


# =============================================================================
# RETRY DECORATOR
# =============================================================================

def with_retry(
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    base_delay: float = 0.6,
    retry_exceptions: tuple = (Exception,),
):
    """Decorator for retrying API calls with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Multiplier for exponential backoff
        base_delay: Initial delay between requests
        retry_exceptions: Tuple of exceptions to catch and retry
        
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    # Rate limiting delay
                    if attempt > 0:
                        wait_time = base_delay * (backoff_factor ** attempt)
                        logger.debug(f"Retry {attempt}/{max_retries}, waiting {wait_time:.1f}s")
                        time.sleep(wait_time)
                    else:
                        time.sleep(base_delay)
                    
                    return func(*args, **kwargs)
                    
                except retry_exceptions as e:
                    last_exception = e
                    error_str = str(e).lower()
                    
                    # Check for rate limiting
                    if "rate" in error_str or "429" in error_str or "timeout" in error_str:
                        logger.warning(f"Rate limited on attempt {attempt + 1}")
                        continue
                    
                    # Check for not found (expected for some players/seasons)
                    if "not found" in error_str or "404" in error_str:
                        logger.debug(f"Resource not found: {e}")
                        return None
                    
                    # Log other errors
                    logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    
                    # Don't retry on final attempt
                    if attempt >= max_retries - 1:
                        logger.error(f"All {max_retries} attempts failed")
                        raise
            
            # Should not reach here, but just in case
            if last_exception:
                raise last_exception
            return None
            
        return wrapper
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
    
    def __init__(self, config: Optional[NBAAPIConfig] = None):
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
            from nba_api.stats.endpoints import playergamelog
            from nba_api.stats.static import players, teams
        except ImportError as e:
            raise ImportError(
                "nba_api package is required. Install with: pip install nba_api"
            ) from e
    
    # -------------------------------------------------------------------------
    # STATIC DATA (NO API CALLS)
    # -------------------------------------------------------------------------
    
    def get_all_players(self) -> List[Dict[str, Any]]:
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
    
    def get_active_players(self) -> List[Dict[str, Any]]:
        """Get all currently active NBA players.
        
        Returns:
            List of active player dictionaries
        """
        from nba_api.stats.static import players
        return players.get_active_players()
    
    def get_inactive_players(self) -> List[Dict[str, Any]]:
        """Get all inactive (retired/not current) NBA players.
        
        Returns:
            List of inactive player dictionaries
        """
        from nba_api.stats.static import players
        return players.get_inactive_players()
    
    def find_player_by_id(self, player_id: int) -> Optional[Dict[str, Any]]:
        """Find a player by their NBA ID.
        
        Args:
            player_id: NBA player ID
            
        Returns:
            Player dictionary or None if not found
        """
        from nba_api.stats.static import players
        return players.find_player_by_id(player_id)
    
    def find_players_by_name(self, name: str) -> List[Dict[str, Any]]:
        """Find players by name (regex pattern).
        
        Args:
            name: Name pattern to search (case insensitive)
            
        Returns:
            List of matching player dictionaries
        """
        from nba_api.stats.static import players
        return players.find_players_by_full_name(name)
    
    def get_all_teams(self) -> List[Dict[str, Any]]:
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
    
    def find_team_by_id(self, team_id: int) -> Optional[Dict[str, Any]]:
        """Find a team by their NBA ID.
        
        Args:
            team_id: NBA team ID
            
        Returns:
            Team dictionary or None if not found
        """
        from nba_api.stats.static import teams
        return teams.find_team_name_by_id(team_id)
    
    def find_team_by_abbreviation(self, abbrev: str) -> Optional[Dict[str, Any]]:
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
    ) -> Optional[pd.DataFrame]:
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
        
        if df.empty:
            return None
            
        return df
    
    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_game_logs(
        self,
        season: str,
        season_type: str = "Regular Season",
        player_id: Optional[int] = None,
        team_id: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
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
        
        if df.empty:
            return None
            
        return df
    
    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_league_game_log(
        self,
        season: str,
        season_type: str = "Regular Season",
        player_or_team: str = "P",  # "P" for Player, "T" for Team
    ) -> Optional[pd.DataFrame]:
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
        
        if df.empty:
            return None
            
        return df
    
    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_career_stats(
        self,
        player_id: int,
    ) -> Optional[Dict[str, pd.DataFrame]]:
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
    ) -> Optional[pd.DataFrame]:
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
        
        if df.empty:
            return None
            
        return df
    
    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_team_game_log(
        self,
        team_id: int,
        season: str,
        season_type: str = "Regular Season",
    ) -> Optional[pd.DataFrame]:
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
        
        if df.empty:
            return None
            
        return df
    
    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_league_game_finder(
        self,
        player_or_team: str = "T",
        season: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        team_id: Optional[int] = None,
        player_id: Optional[int] = None,
        **kwargs
    ) -> Optional[pd.DataFrame]:
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
        
        if df.empty:
            return None
            
        return df
    
    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_boxscore_traditional(
        self,
        game_id: str,
    ) -> Optional[Dict[str, pd.DataFrame]]:
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
    ) -> Optional[Dict[str, pd.DataFrame]]:
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
    def get_play_by_play(
        self,
        game_id: str,
    ) -> Optional[pd.DataFrame]:
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
        
        if df.empty:
            return None
            
        return df
    
    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_shot_chart_detail(
        self,
        game_id: str,
        team_id: Optional[int] = None,
        player_id: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """Get detailed shot chart data for a game.
        
        Args:
            game_id: NBA game ID (10-digit string)
            team_id: Optional team ID to filter by
            player_id: Optional player ID to filter by
            
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
        
        params = {
            "game_id": game_id,
            "proxy": self.config.proxy,
            "headers": self.config.headers,
            "timeout": self.config.timeout,
        }
        
        if team_id:
            params["team_id"] = team_id
        if player_id:
            params["player_id"] = player_id
            
        shot_chart = shotchartdetail.ShotChartDetail(**params)
        
        df = shot_chart.shot_chart_detail.get_data_frame()
        
        if df.empty:
            return None
            
        return df
    
    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_league_standings(
        self,
        season: str,
        season_type: str = "Regular Season",
    ) -> Optional[pd.DataFrame]:
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
        
        if df.empty:
            return None
            
        return df
    
    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_player_tracking_stats(
        self,
        season: str,
        season_type: str = "Regular Season",
        player_or_team: str = "P",
    ) -> Optional[pd.DataFrame]:
        """Get player tracking statistics.
        
        Args:
            season: Season string (e.g., "2023-24")
            season_type: Season type (Regular Season, Playoffs, etc.)
            player_or_team: "P" for player stats, "T" for team stats
            
        Returns:
            DataFrame with tracking statistics or None if not available
            
        Reference:
            nba_api/stats/endpoints/leaguedashptstats.py
            
        API Fields Include:
            - Speed/Distance: SPEED, DIST, ORBCONTRIB, DRCONTRIB
            - Rebounding: REBCONTRIB, REBCHANCES, REBCHANCE_PCT
            - Defense: DEF_RIM_PCT, DFGM, DFGA
        """
        from nba_api.stats.endpoints import leaguedashptstats
        
        tracking = leaguedashptstats.LeagueDashPtStats(
            season=season,
            season_type_all_star=season_type,
            player_or_team=player_or_team,
            proxy=self.config.proxy,
            headers=self.config.headers,
            timeout=self.config.timeout,
        )
        
        df = tracking.league_dash_pt_stats.get_data_frame()
        
        if df.empty:
            return None
            
        return df
    
    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_hustle_stats(
        self,
        season: str,
        season_type: str = "Regular Season",
        player_or_team: str = "P",
    ) -> Optional[pd.DataFrame]:
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
        
        df = hustle.league_hustle_stats_player.get_data_frame()
        
        if df.empty:
            return None
            
        return df
    
    @with_retry(max_retries=3, backoff_factor=2.0, base_delay=0.6)
    def get_scoreboard(
        self,
        game_date: Optional[str] = None,
        league_id: str = "00",
    ) -> Optional[pd.DataFrame]:
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
            
        scoreboard = scoreboardv3.ScoreBoardV3(**params)
        
        df = scoreboard.score_board.get_data_frame()
        
        if df.empty:
            return None
            
        return df


# =============================================================================
# MODULE-LEVEL CLIENT INSTANCE
# =============================================================================

# Singleton client instance for convenience
_client: Optional[NBAClient] = None


def get_client() -> NBAClient:
    """Get or create the singleton NBA API client.
    
    Returns:
        NBAClient instance
    """
    global _client
    if _client is None:
        _client = NBAClient()
    return _client
