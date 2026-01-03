# NBA Expert Population Scripts Audit Report

> **Date:** January 2, 2026  
> **Scope:** Comprehensive audit of `src/scripts/populate/` folder  
> **Goal:** Identify improvements using nba_api and basketball_reference_web_scraper libraries

---

## Executive Summary

This audit examines the current population scripts and identifies **40+ improvement opportunities** across the following categories:

1. **Missing Data Sources** - New endpoints from nba_api not yet utilized
2. **Basketball Reference Integration** - Untapped BR data to complement NBA stats
3. **Code Architecture Improvements** - Better patterns for robustness and maintainability
4. **Performance Optimizations** - Bulk fetching, caching, and parallel processing
5. **Data Quality Enhancements** - Validation, reconciliation, and integrity checks

---

## 1. Current State Analysis

### 1.1 Existing Population Scripts

| Script | Data Source | Status | Notes |
|--------|-------------|--------|-------|
| `populate_nba_data.py` | nba_api | ✅ Active | Main orchestrator |
| `api_client.py` | nba_api | ✅ Active | ~20 endpoints wrapped |
| `populate_player_game_stats.py` | nba_api | ✅ Active | Per-player game logs |
| `populate_player_game_stats_v2.py` | nba_api | ✅ Active | Bulk game logs |
| `populate_play_by_play.py` | nba_api | ⚠️ Partial | Has TODO for API issues |
| `populate_shot_chart.py` | nba_api | ❌ Placeholder | Not implemented |
| `populate_br_player_box_scores.py` | BR | ✅ Active | Historical backfill |
| `populate_br_season_stats.py` | BR | ✅ Active | Season totals |
| `populate_draft_history.py` | nba_api | ✅ Active | Draft data |
| `populate_draft_combine_stats.py` | nba_api | ✅ Active | Combine measurements |
| `populate_team_details.py` | nba_api | ✅ Active | Team info |
| `populate_team_info_common.py` | nba_api | ✅ Active | Team common info |
| `populate_league_game_logs.py` | nba_api | ✅ Active | League-wide logs |
| `populate_common_player_info.py` | nba_api | ✅ Active | Player biographical |
| `populate_player_season_stats.py` | nba_api | ✅ Active | Season aggregates |
| `populate_win_probability.py` | nba_api | ❓ Unknown | Status unclear |
| `populate_salaries.py` | External | ❓ Unknown | Status unclear |
| `populate_injury_data.py` | External | ❓ Unknown | Status unclear |
| `populate_transactions.py` | External | ❓ Unknown | Status unclear |
| `populate_arenas.py` | External | ❓ Unknown | Status unclear |
| `populate_officials.py` | External | ❓ Unknown | Status unclear |
| `populate_franchises.py` | External | ❓ Unknown | Status unclear |

### 1.2 Currently Used nba_api Endpoints

The `api_client.py` wraps approximately 20 endpoints:

```python
# Static data
- players.get_players()
- players.get_active_players()
- teams.get_teams()

# Player endpoints
- playergamelog.PlayerGameLog
- playergamelogs.PlayerGameLogs
- playercareerstats.PlayerCareerStats
- commonplayerinfo.CommonPlayerInfo

# Team endpoints
- teamgamelog.TeamGameLog
- teaminfocommon.TeamInfoCommon
- teamdetails.TeamDetails

# League endpoints
- leaguegamelog.LeagueGameLog
- leaguegamefinder.LeagueGameFinder
- leaguestandingsv3.LeagueStandingsV3

# Box scores
- boxscoretraditionalv3.BoxScoreTraditionalV3
- boxscoreadvancedv3.BoxScoreAdvancedV3

# Play-by-play & shots
- playbyplayv3.PlayByPlayV3
- shotchartdetail.ShotChartDetail

# Draft
- drafthistory.DraftHistory
- draftcombinestats.DraftCombineStats

# Other
- leaguedashptstats.LeagueDashPtStats (tracking)
- leaguehustlestatsplayer.LeagueHustleStatsPlayer
- scoreboardv3.ScoreboardV3
```

---

## 2. Missing nba_api Endpoints (HIGH PRIORITY)

### 2.1 Player Tracking Data ⭐ NEW

**Endpoint:** `LeagueDashPtStats`  
**Data:** Speed, distance, touches, drives, rebounds, passes  
**Use Case:** Advanced analytics, player efficiency beyond traditional stats

```python
# Example implementation needed
from nba_api.stats.endpoints import leaguedashptstats

# Different measure types available:
# - SpeedDistance (speed, distance)
# - Rebounding (reb chances, contested/uncontested)
# - Possessions (touches, time of possession)
# - CatchShoot (catch and shoot stats)
# - PullUpShot (pull up shooting)
# - Defense (rim protection)
# - Drives (drives, drive FG%)
# - Passing (potential assists, passes)
# - ElbowTouch (elbow touches)
# - PostTouch (post touches)
# - PaintTouch (paint touches)
```

**Recommendation:** Create `populate_player_tracking.py`

---

### 2.2 Synergy Play Types ⭐ NEW

**Endpoint:** `SynergyPlayTypes`  
**Data:** Play type efficiency (Isolation, P&R, Post-up, Spot-up, etc.)  
**Use Case:** Offensive/defensive scheme analysis, matchup scouting

```python
from nba_api.stats.endpoints import synergyplaytypes

# Available play types:
# - Isolation, Transition, PRBallHandler, PRRollman
# - Postup, Spotup, Handoff, Cut, OffScreen, Putbacks, Misc
```

**Recommendation:** Create `populate_synergy_playtypes.py`

---

### 2.3 Matchup Data ⭐ NEW

**Endpoint:** `LeagueSeasonMatchups`, `MatchupsRollup`, `BoxScoreMatchupsV3`  
**Data:** Individual player defensive matchups  
**Use Case:** Defensive analysis, lineup optimization

```python
from nba_api.stats.endpoints import leagueseasonmatchups, matchupsrollup
```

**Recommendation:** Create `populate_matchups.py`

---

### 2.4 Lineup Data ⭐ NEW

**Endpoint:** `LeagueDashLineups`, `LeagueLineupViz`  
**Data:** 2-5 man lineup stats (NET rating, +/-, minutes)  
**Use Case:** Lineup optimization, rotation analysis

```python
from nba_api.stats.endpoints import leaguedashlineups

lineups = leaguedashlineups.LeagueDashLineups(
    season='2024-25',
    group_quantity=5  # 2, 3, 4, or 5 man lineups
)
```

**Recommendation:** Create `populate_lineups.py`

---

### 2.5 Game Rotation Data ⭐ NEW

**Endpoint:** `GameRotation`  
**Data:** Player substitution patterns, stint durations  
**Use Case:** Rotation analysis, fatigue modeling

```python
from nba_api.stats.endpoints import gamerotation
```

**Recommendation:** Add to box score population or create separate script

---

### 2.6 Player Awards ⭐ NEW

**Endpoint:** `PlayerAwards`  
**Data:** All-NBA, MVP, All-Star, weekly/monthly awards  
**Use Case:** Historical context, player achievements

```python
from nba_api.stats.endpoints import playerawards
```

**Recommendation:** Create `populate_player_awards.py`

---

### 2.7 Estimated Metrics ⭐ NEW

**Endpoint:** `PlayerEstimatedMetrics`, `TeamEstimatedMetrics`  
**Data:** Estimated OFF/DEF/NET rating without play-by-play  
**Use Case:** Fill gaps when PBP unavailable, quick estimates

```python
from nba_api.stats.endpoints import playerestimatedmetrics, teamestimatedmetrics
```

**Recommendation:** Add to player/team stats population

---

### 2.8 Win Probability (Enhance Existing)

**Endpoint:** `WinProbabilityPBP`  
**Data:** Win probability at each play  
**Use Case:** Clutch analysis, game flow visualization

**Recommendation:** Ensure `populate_win_probability.py` is fully implemented

---

### 2.9 Additional Box Score Types

**Endpoints:**
- `BoxScoreHustleV2` - Hustle stats per game
- `BoxScoreMiscV2/V3` - Points off turnovers, 2nd chance, fast break, paint
- `BoxScoreScoringV2/V3` - Scoring breakdown
- `BoxScoreUsageV2/V3` - Usage rates per game
- `BoxScoreFourFactorsV2/V3` - Four factors analysis
- `BoxScoreDefensiveV2` - Defensive matchups in box score
- `BoxScorePlayerTrackV3` - Player tracking in box score

**Recommendation:** Expand `get_boxscore_*` methods in api_client.py

---

### 2.10 Player Dashboard Splits ⭐ NEW

**Endpoints:**
- `PlayerDashboardByClutch` - Clutch-time splits
- `PlayerDashboardByGameSplits` - By win/loss, blowout, etc.
- `PlayerDashboardByLastNGames` - Recent performance
- `PlayerDashboardByShootingSplits` - By shot type, zone
- `PlayerDashboardByYearOverYear` - Season comparison

**Recommendation:** Create `populate_player_splits.py`

---

### 2.11 Draft Combine Enhanced Data

**Endpoints:**
- `DraftCombineDrillResults` - Agility drill times
- `DraftCombineNonStationaryShooting` - Moving shot results
- `DraftCombineSpotShooting` - Spot shooting %
- `DraftCombinePlayerAnthro` - Detailed measurements
- `DraftBoard` - Draft board order

**Recommendation:** Enhance `populate_draft_combine_stats.py`

---

### 2.12 Live Data Integration ⭐ NEW

**Endpoints (nba_api.live):**
- `ScoreBoard` - Today's live scoreboard
- `BoxScore` - Live box scores
- `PlayByPlay` - Live play-by-play
- `Odds` - Betting odds

**Recommendation:** Create `populate_live_data.py` for real-time tracking

---

### 2.13 Video Details

**Endpoints:**
- `VideoDetails` - Video clip metadata
- `VideoEventsAsset` - Video URLs for plays

**Recommendation:** Create `populate_video_metadata.py` (if video analysis needed)

---

### 2.14 Player Comparison Tools

**Endpoints:**
- `PlayerCompare` - Head-to-head comparison
- `PlayerVsPlayer` - Matchup history

**Recommendation:** Useful for expert system queries

---

### 2.15 Additional League-Wide Data

**Endpoints:**
- `LeagueLeaders` - Statistical leaders
- `LeaguePlayerOnDetails` - Player on-court impact
- `AllTimeLeadersGrids` - Historical leaders
- `FranchiseHistory` - Team history
- `FranchiseLeaders` - Franchise records
- `PlayoffPicture` - Playoff scenarios

---

## 3. Basketball Reference Integration Improvements

### 3.1 Currently Used BR Data

```python
# populate_br_player_box_scores.py
- client.player_box_scores(day, month, year)  # Daily box scores

# populate_br_season_stats.py  
- client.players_season_totals(season_end_year)  # Basic season stats
- client.players_advanced_season_totals(season_end_year)  # Advanced stats
```

### 3.2 Missing BR Data Sources ⭐ NEW

| Method | Data | Current Status | Recommendation |
|--------|------|----------------|----------------|
| `team_box_scores(day, month, year)` | Daily team box scores | ❌ Not used | Add to BR box scores |
| `season_schedule(season_end_year)` | Full season schedule | ❌ Not used | Create `populate_br_schedule.py` |
| `regular_season_player_box_scores(player_id, year)` | Player game logs | ❌ Not used | Alternative to NBA API |
| `playoff_player_box_scores(player_id, year)` | Playoff game logs | ❌ Not used | Backfill playoff data |
| `play_by_play(home_team, year, month, day)` | Game PBP | ❌ Not used | Fallback for NBA API issues |
| `standings(season_end_year)` | Standings | ❌ Not used | Historical standings |
| `search(term)` | Player search | ❌ Not used | Player ID resolution |

### 3.3 BR Advantages Over NBA API

1. **Historical Coverage**: BR has data back to 1947, NBA API limited for older seasons
2. **Advanced Metrics**: VORP, BPM, WS/48 calculated differently (often preferred)
3. **Stability**: Less rate limiting than NBA API
4. **Player Identifiers**: Different ID system for cross-referencing

**Recommendation:** Create player/team ID mapping table between NBA and BR

---

## 4. Code Architecture Improvements

### 4.1 Base Populator Pattern Enhancement

Current `base.py` provides good foundation. Suggested enhancements:

```python
class BasePopulator:
    # ADD: Built-in data validation with schemas
    def validate_data(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
        schema = self.get_schema()
        return schema.validate_dataframe(df)
    
    # ADD: Cross-source reconciliation
    def reconcile_with_source(self, source: str) -> dict:
        """Compare local data with API/BR data for consistency"""
        pass
    
    # ADD: Incremental update detection
    def get_incremental_scope(self) -> dict:
        """Detect what needs updating based on timestamps"""
        pass
```

### 4.2 API Client Enhancements

```python
class NBAClient:
    # ADD: Request pooling for parallel fetches
    async def get_multiple_player_logs(self, player_ids: list) -> list[pd.DataFrame]:
        """Fetch multiple player logs concurrently with rate limiting"""
        pass
    
    # ADD: Automatic retry with circuit breaker
    @circuit_breaker(failure_threshold=5, recovery_timeout=60)
    def get_with_circuit_breaker(self, endpoint, **kwargs):
        pass
    
    # ADD: Response caching with TTL
    @cached(ttl=3600)  # 1 hour cache
    def get_static_player_info(self, player_id: int):
        pass
    
    # ADD: Bulk endpoint support
    def get_bulk_boxscores(self, game_ids: list[str]) -> dict[str, pd.DataFrame]:
        """Fetch multiple box scores with optimized batching"""
        pass
```

### 4.3 Configuration Improvements

```python
# config.py enhancements
class PopulationConfig:
    # ADD: Per-endpoint rate limits
    ENDPOINT_RATE_LIMITS = {
        'playbyplayv3': 1.0,  # 1 second
        'shotchartdetail': 2.0,  # 2 seconds (heavier)
        'default': 0.6
    }
    
    # ADD: Data freshness thresholds
    FRESHNESS_THRESHOLDS = {
        'player_game_stats': timedelta(hours=6),
        'standings': timedelta(hours=1),
        'player_info': timedelta(days=7)
    }
    
    # ADD: Priority ordering for population
    POPULATION_ORDER = [
        'teams',  # First - no dependencies
        'players',  # Second - no dependencies  
        'games',  # Third - needs teams
        'player_game_stats',  # Fourth - needs players, games
        'boxscores',  # Fifth - needs games
        'play_by_play',  # Sixth - needs games
        'shot_charts',  # Last - needs games, players
    ]
```

### 4.4 Error Recovery Improvements

```python
# resilience.py enhancements
class ResilientPopulator:
    def __init__(self):
        self.dead_letter_queue = []  # Failed items for retry
        self.partial_commits = True  # Commit successful batches
        
    def process_with_recovery(self, items):
        for batch in self.batch_items(items):
            try:
                self.process_batch(batch)
                self.commit_batch(batch)
            except Exception as e:
                self.dead_letter_queue.extend(batch)
                self.log_failure(batch, e)
        
        # Retry dead letter queue with backoff
        self.retry_failed_items()
```

---

## 5. New Population Scripts to Create

### Priority 1 - High Value, Immediate Need

| Script | Endpoints | Estimated Effort |
|--------|-----------|------------------|
| `populate_synergy_playtypes.py` | SynergyPlayTypes | 2-3 hours |
| `populate_player_tracking.py` | LeagueDashPtStats | 3-4 hours |
| `populate_lineups.py` | LeagueDashLineups | 2-3 hours |
| `populate_matchups.py` | LeagueSeasonMatchups | 2-3 hours |
| `populate_player_awards.py` | PlayerAwards | 1-2 hours |

### Priority 2 - Enhanced Coverage

| Script | Endpoints | Estimated Effort |
|--------|-----------|------------------|
| `populate_shot_chart.py` | ShotChartDetail (implement) | 3-4 hours |
| `populate_boxscore_advanced.py` | All BoxScore* variants | 4-5 hours |
| `populate_player_splits.py` | PlayerDashboard* | 3-4 hours |
| `populate_game_rotation.py` | GameRotation | 2-3 hours |
| `populate_estimated_metrics.py` | PlayerEstimatedMetrics | 1-2 hours |

### Priority 3 - Historical & Supplemental

| Script | Endpoints | Estimated Effort |
|--------|-----------|------------------|
| `populate_br_schedule.py` | BR schedule | 1-2 hours |
| `populate_br_standings.py` | BR standings | 1-2 hours |
| `populate_franchise_data.py` | FranchiseHistory, Leaders | 2-3 hours |
| `populate_league_leaders.py` | LeagueLeaders, AllTime | 2-3 hours |
| `populate_live_scoreboard.py` | Live endpoints | 3-4 hours |

---

## 6. Data Quality Improvements

### 6.1 Schema Validation Enhancements

Enhance `schemas.py` with:

```python
# New schemas needed
class SynergyPlayTypeStats(NBABaseModel):
    """Synergy play type efficiency data."""
    season_id: str
    team_id: int
    player_id: int | None
    play_type: str  # Isolation, PRBallHandler, etc.
    ppp: float = Field(ge=0, le=3)  # Points per possession
    percentile: float = Field(ge=0, le=100)
    possessions: int = Field(ge=0)
    # ... more fields

class PlayerTrackingStats(NBABaseModel):
    """Player tracking data."""
    player_id: int
    speed: float = Field(ge=0, le=30)  # mph
    distance_miles: float = Field(ge=0)
    touches: int = Field(ge=0)
    # ... more fields

class LineupStats(NBABaseModel):
    """Lineup combination stats."""
    lineup_id: str  # Hash of player IDs
    player_ids: list[int]
    minutes: float = Field(ge=0)
    net_rating: float
    # ... more fields
```

### 6.2 Cross-Source Validation

```python
class DataReconciler:
    """Reconcile data between NBA API and Basketball Reference."""
    
    def reconcile_player_stats(self, player_id: int, season: str):
        """Compare player stats between sources."""
        nba_stats = self.get_nba_api_stats(player_id, season)
        br_stats = self.get_br_stats(player_id, season)
        
        discrepancies = []
        for stat in ['pts', 'reb', 'ast', 'games']:
            if abs(nba_stats[stat] - br_stats[stat]) > 0.5:
                discrepancies.append({
                    'stat': stat,
                    'nba_value': nba_stats[stat],
                    'br_value': br_stats[stat],
                    'diff': abs(nba_stats[stat] - br_stats[stat])
                })
        
        return discrepancies
```

### 6.3 Data Freshness Monitoring

```python
class FreshnessMonitor:
    """Monitor data freshness and trigger updates."""
    
    def get_stale_tables(self) -> list[str]:
        """Identify tables needing refresh."""
        stale = []
        for table, threshold in FRESHNESS_THRESHOLDS.items():
            last_update = self.get_last_update(table)
            if datetime.now() - last_update > threshold:
                stale.append(table)
        return stale
    
    def schedule_refresh(self):
        """Schedule automatic refreshes for stale data."""
        pass
```

---

## 7. Performance Optimizations

### 7.1 Bulk Fetching Strategy

**Current:** Many scripts fetch data one record at a time  
**Improvement:** Use bulk endpoints where available

```python
# BEFORE: Per-player fetching (SLOW)
for player in players:
    log = client.get_player_game_log(player['id'], season)

# AFTER: Bulk league fetching (FAST)
all_logs = client.get_player_game_logs(season=season)  # Single API call!
```

### 7.2 Parallel Processing

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

class ParallelPopulator:
    def __init__(self, max_workers: int = 5):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.semaphore = asyncio.Semaphore(max_workers)
    
    async def fetch_parallel(self, tasks: list):
        """Fetch multiple resources in parallel with rate limiting."""
        async with self.semaphore:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
```

### 7.3 Caching Strategy

```python
from functools import lru_cache
import diskcache

class CachedClient:
    def __init__(self):
        self.cache = diskcache.Cache('./cache/nba_api')
    
    def get_with_cache(self, key: str, fetch_func, ttl: int = 3600):
        """Get from cache or fetch and cache."""
        if key in self.cache:
            return self.cache[key]
        
        result = fetch_func()
        self.cache.set(key, result, expire=ttl)
        return result
```

### 7.4 Incremental Updates

```python
class IncrementalPopulator:
    """Only fetch/process changed data."""
    
    def get_changed_games(self, since: datetime) -> list[str]:
        """Get game IDs modified since timestamp."""
        scoreboard = self.client.get_scoreboard()
        # Filter to recent/changed games
        pass
    
    def get_update_scope(self, table: str) -> dict:
        """Determine minimal update scope."""
        last_run = self.get_last_run_timestamp(table)
        return {
            'start_date': last_run,
            'end_date': datetime.now(),
            'affected_ids': self.get_changed_ids(table, last_run)
        }
```

---

## 8. Implementation Roadmap

### Phase 1: Quick Wins (1-2 weeks)

1. ✅ Add missing endpoints to `api_client.py`:
   - `SynergyPlayTypes`
   - `LeagueDashLineups`
   - `PlayerAwards`
   - `GameRotation`
   - Enhanced `BoxScore*` methods

2. ✅ Create high-priority scripts:
   - `populate_synergy_playtypes.py`
   - `populate_player_tracking.py`
   - `populate_lineups.py`

3. ✅ Enhance BR integration:
   - Add `team_box_scores` support
   - Add `standings` support

### Phase 2: Core Improvements (2-4 weeks)

1. Complete missing scripts:
   - `populate_shot_chart.py` (implement placeholder)
   - `populate_matchups.py`
   - `populate_player_splits.py`
   - `populate_game_rotation.py`

2. Architecture improvements:
   - Implement bulk fetching patterns
   - Add caching layer
   - Enhance error recovery

3. Data quality:
   - Add cross-source reconciliation
   - Implement freshness monitoring

### Phase 3: Advanced Features (1-2 months)

1. Live data integration
2. Video metadata (if needed)
3. Full historical backfill orchestration
4. Performance optimization (parallel processing)
5. Comprehensive monitoring dashboard

---

## 9. Summary of Recommendations

### Must Have (Critical)

| Item | Impact | Effort |
|------|--------|--------|
| Add Synergy Play Types | High - Advanced analytics | Medium |
| Add Player Tracking Data | High - Speed/distance metrics | Medium |
| Add Lineup Data | High - Rotation analysis | Medium |
| Implement Shot Chart | High - Spatial analysis | Medium |
| Add Matchup Data | High - Defensive analysis | Medium |

### Should Have (Important)

| Item | Impact | Effort |
|------|--------|--------|
| Enhanced Box Scores (Hustle, Misc, etc.) | Medium-High | Low-Medium |
| Player Awards | Medium | Low |
| Game Rotation | Medium | Low |
| Player Splits (Clutch, etc.) | Medium | Medium |
| Estimated Metrics | Medium | Low |

### Nice to Have (Future)

| Item | Impact | Effort |
|------|--------|--------|
| Live Data Integration | Medium | High |
| Video Metadata | Low | Medium |
| Full BR Integration | Medium | Medium |
| Parallel Processing | Medium | High |

---

## 10. Appendix: Complete nba_api Endpoint Reference

### Stats Endpoints (130+)

<details>
<summary>Click to expand full list</summary>

**All Time Statistics**
- AllTimeLeadersGrids
- AssistLeaders
- AssistTracker

**Box Scores**
- BoxScoreAdvancedV2, BoxScoreAdvancedV3
- BoxScoreDefensiveV2
- BoxScoreFourFactorsV2, BoxScoreFourFactorsV3
- BoxScoreHustleV2
- BoxScoreMatchupsV3
- BoxScoreMiscV2, BoxScoreMiscV3
- BoxScorePlayerTrackV3
- BoxScoreScoringV2, BoxScoreScoringV3
- BoxScoreSummaryV2, BoxScoreSummaryV3
- BoxScoreTraditionalV2, BoxScoreTraditionalV3
- BoxScoreUsageV2, BoxScoreUsageV3

**Common Info**
- CommonAllPlayers
- CommonPlayerInfo
- CommonPlayoffSeries
- CommonTeamRoster
- CommonTeamYears

**Cumulative Stats**
- CumeStatsPlayer, CumeStatsPlayerGames
- CumeStatsTeam, CumeStatsTeamGames

**Defense**
- DefenseHub

**Draft**
- DraftBoard
- DraftCombineDrillResults
- DraftCombineNonStationaryShooting
- DraftCombinePlayerAnthro
- DraftCombineSpotShooting
- DraftCombineStats
- DraftHistory

**Fantasy/Misc**
- DunkScoreLeaders
- FantasyWidget
- InfographicFanDuelPlayer

**Franchise**
- FranchiseHistory
- FranchiseLeaders
- FranchisePlayers

**Game**
- GameRotation
- GLAlumBoxScoreSimilarityScore

**Home Page**
- HomePageLeaders
- HomePageV2

**Hustle**
- HustleStatsBoxScore

**IST**
- ISTStandings

**Leaders**
- LeadersTiles

**League Dash**
- LeagueDashLineups
- LeagueDashOppPtShot
- LeagueDashPlayerBioStats
- LeagueDashPlayerClutch
- LeagueDashPlayerPtShot
- LeagueDashPlayerShotLocations
- LeagueDashPlayerStats
- LeagueDashPtDefend
- LeagueDashPtStats
- LeagueDashPtTeamDefend
- LeagueDashTeamClutch
- LeagueDashTeamPtShot
- LeagueDashTeamShotLocations
- LeagueDashTeamStats

**League Game**
- LeagueGameFinder
- LeagueGameLog

**League Hustle**
- LeagueHustleStatsPlayer
- LeagueHustleStatsTeam

**League Leaders**
- LeagueLeaders
- LeagueLineupViz

**League Other**
- LeaguePlayerOnDetails
- LeagueSeasonMatchups
- LeagueStandings, LeagueStandingsV3

**Matchups**
- MatchupsRollup

**Play-by-Play**
- PlayByPlay, PlayByPlayV2, PlayByPlayV3

**Player**
- PlayerAwards
- PlayerCareerByCollege, PlayerCareerByCollegeRollup
- PlayerCareerStats
- PlayerCompare
- PlayerDashboardByClutch
- PlayerDashboardByGameSplits
- PlayerDashboardByGeneralSplits
- PlayerDashboardByLastNGames
- PlayerDashboardByShootingSplits
- PlayerDashboardByTeamPerformance
- PlayerDashboardByYearOverYear
- PlayerDashPtPass
- PlayerDashPtReb
- PlayerDashPtShotDefend
- PlayerDashPtShots
- PlayerEstimatedMetrics
- PlayerFantasyProfileBarGraph
- PlayerGameLog, PlayerGameLogs
- PlayerGameStreakFinder
- PlayerIndex
- PlayerNextNGames
- PlayerProfileV2
- PlayerVsPlayer

**Playoff**
- PlayoffPicture

**Schedule**
- ScheduleLeagueV2, ScheduleLeagueV2Int

**Scoreboard**
- ScoreboardV2, ScoreboardV3

**Shot Chart**
- ShotChartDetail
- ShotChartLeagueWide
- ShotChartLineupDetail

**Synergy**
- SynergyPlayTypes

**Team**
- TeamAndPlayersVsPlayers
- TeamDashboardByGeneralSplits
- TeamDashboardByShootingSplits
- TeamDashLineups
- TeamDashPtPass
- TeamDashPtReb
- TeamDashPtShots
- TeamDetails
- TeamEstimatedMetrics
- TeamGameLog, TeamGameLogs
- TeamGameStreakFinder
- TeamHistoricalLeaders
- TeamInfoCommon
- TeamPlayerDashboard
- TeamPlayerOnOffDetails
- TeamPlayerOnOffSummary
- TeamVsPlayer
- TeamYearByYearStats

**Video**
- VideoDetails
- VideoDetailsAsset
- VideoEvents
- VideoEventsAsset
- VideoStatus

**Win Probability**
- WinProbabilityPBP

</details>

### Live Endpoints

- BoxScore
- Odds
- PlayByPlay
- ScoreBoard

### Static Data

- players.get_players()
- players.get_active_players()
- players.find_player_by_id()
- teams.get_teams()
- teams.find_team_by_abbreviation()

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-02 | Audit | Initial comprehensive audit |

