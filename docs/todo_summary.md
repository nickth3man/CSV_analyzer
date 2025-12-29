# TODO Comments Summary

This document summarizes all TODO comments added to the codebase based on docs/roadmap.md.

**Date Created**: 2024-12-28
**Source**: docs/roadmap.md database development roadmap
**Purpose**: Track implementation tasks across all development phases

---

## Phase 1: Schema Hygiene (High Impact, Medium Effort)

### 1.2 - Document Canonical Tables ✅ Partial → TODO
**File**: `scripts/maintenance/normalize_db.py`
**Status**: Documentation needed for raw vs canonical table relationships
**Tasks**:
- Document which tables are canonical (silver/gold) vs raw text
- Explain relationships between raw and canonical tables
- Document transformation logic (raw → silver → gold)

### 1.4 - Add FK Constraints/Tests 🔴 TODO
**File**: `scripts/maintenance/check_integrity.py`
**Status**: Not implemented
**Tasks**:
- Implement systematic FK constraint validation
- Add referential integrity tests for game_gold → team_silver
- Add constraints for player_game_stats → player_silver/game_gold
- Missing FK checks:
  - player_game_stats.player_id → player_silver.id
  - player_game_stats.team_id → team_silver.id
  - player_game_stats.game_id → game_gold.game_id
  - team_game_stats.team_id → team_silver.id
  - team_game_stats.game_id → game_gold.game_id
- Consider using dbt or similar for automated constraint testing

### 1.5 - Quarantine Raw Text Tables 🔴 TODO
**File**: `scripts/maintenance/normalize_db.py`
**Status**: Not implemented
**Tasks**:
- Rename raw tables with '_raw' suffix (game → game_raw, etc.)
- OR move to separate schema/database ('raw' schema)
- Update documentation to clarify canonical vs raw
- Prevent accidental use of raw tables in queries
**Priority**: MEDIUM

---

## Phase 2: Player & Advanced Metrics (High Impact, High Effort)

### 2.3 - Create Advanced Metrics Views 🟡 Partial → TODO
**File**: `scripts/analysis/create_advanced_metrics.py`
**Status**: Basic metrics implemented, advanced metrics incomplete
**Current**: TS%, eFG%, TOV%, GmSc implemented
**Missing**:
- USG% (usage rate) - requires possessions data
- ORB%, DRB%, TRB% (rebounding percentages)
- AST% (assist percentage)
- STL%, BLK% (defensive activity rates)
- PER (player efficiency rating) - complex formula
- BPM, VORP (box plus/minus) - require regression models
- ORtg, DRtg (offensive/defensive rating) - require possessions
- WS, WS/48 (win shares)

### 2.4 - Add Possessions/Pace Data 🔴 TODO
**File**: `scripts/analysis/create_advanced_metrics.py`
**Status**: Not implemented
**Tasks**:
- Calculate team possessions per game for advanced metrics
- Formula: `Poss = 0.5 * ((Tm_FGA + 0.4*Tm_FTA - 1.07*(Tm_ORB/(Tm_ORB+Opp_DRB))*(Tm_FGA-Tm_FGM) + Tm_TOV) + (Opp_FGA + 0.4*Opp_FTA - 1.07*(Opp_ORB/(Opp_ORB+Tm_DRB))*(Opp_FGA-Opp_FGM) + Opp_TOV))`
- Required for: USG%, ORtg, DRtg, Pace calculation
- Blocked by: Need opponent team stats joined per game

### 2.5 - Verify bridge_player_team_season 🟡 Partial → TODO
**File**: `scripts/populate/populate_player_season_stats.py`
**Status**: Partial verification needed
**Tasks**:
1. Verify bridge_player_team_season table exists and is populated
2. Check data quality and completeness
3. Consider using this table for player-team-season relationships
4. May simplify season aggregation logic if properly maintained

---

## Phase 3: Events & Economics (Medium Impact, High Effort)

### 3.1 - Populate play_by_play 🟡 HIGH Priority → Blocked
**File**: `scripts/populate/populate_play_by_play.py`
**Status**: Script implemented but blocked by NBA API access issues
**Tasks**:
- Resolve NBA API authentication/access issues
- API endpoints may require authentication or have changed
- Consider alternatives:
  1. Use nba_api library's PlayByPlayV2/V3 endpoints
  2. Investigate if API keys/tokens are needed
  3. Check for rate limiting or IP blocking
  4. Consider caching/historical data sources if API unavailable
**Impact**: Blocks clutch analysis, lineup analysis, detailed event tracking

### 3.2 - Add shot_chart_detail 🔴 TODO
**File**: `scripts/populate/populate_shot_chart.py` ✨ NEW PLACEHOLDER
**Status**: Not yet implemented
**Tasks**:
1. Fetch shot chart data from NBA API (shotchartdetail endpoint)
2. Store x/y coordinates, shot distance, shot type, make/miss
3. Link to player_id, team_id, game_id
4. Support filtering by season, player, team
**Use Cases**: Shot distribution, hot zones, shooting efficiency by location
**Priority**: MEDIUM

### 3.3 - Populate salaries 🔴 TODO
**File**: `scripts/populate/populate_salaries.py` ✨ NEW PLACEHOLDER
**Status**: Not yet implemented
**Tasks**:
1. Identify reliable salary data source (NBA API doesn't provide this)
2. Options: Basketball Reference, HoopsHype, Spotrac, ESPN
3. Store player_id, season, team_id, salary amount, contract details
4. Handle multi-year contracts, options, guarantees
**Use Cases**: Value analysis, cap space tracking, contract comparison
**Priority**: MEDIUM
**Blocks**: Value-based player analysis

### 3.4 - Populate transactions 🔴 TODO
**File**: `scripts/populate/populate_transactions.py` ✨ NEW PLACEHOLDER
**Status**: Not yet implemented
**Tasks**:
1. Fetch transaction data from NBA API or alternative source
2. Track: trades, signings, waivers, releases, G-League assignments
3. Store: player_id, transaction_type, from_team, to_team, date, details
4. Handle multi-player trades and package deals
**Use Cases**: Roster movement, trade analysis, player career paths
**Priority**: LOW

### 3.5 - Add injury data 🔴 TODO
**File**: `scripts/populate/populate_injury_data.py` ✨ NEW PLACEHOLDER
**Status**: Not yet implemented
**Tasks**:
1. Fetch injury reports and player availability status
2. Track: injury type, severity, dates (injury/return), games missed
3. Sources: NBA official injury reports, team reports, sports data providers
4. Store: player_id, injury_date, return_date, injury_type, status, games_missed
**Use Cases**: Injury risk analysis, load management tracking, availability predictions
**Priority**: LOW

---

## Phase 4: Enrichment & Documentation (Medium Impact, Medium Effort)

### 4.1 - Populate arenas 🔴 TODO
**File**: `scripts/populate/populate_arenas.py` ✨ NEW PLACEHOLDER
**Status**: Not yet implemented (0 rows in arenas table)
**Tasks**:
1. Fetch arena/venue information for all NBA teams
2. Track: arena_id, name, city, state, capacity, opened_year, closed_year
3. Support historical arenas (teams that moved/changed venues)
4. Link to team_id with date ranges
**Use Cases**: Home court advantage analysis, venue-specific performance
**Priority**: LOW

### 4.2 - Populate franchises 🔴 TODO
**File**: `scripts/populate/populate_franchises.py` ✨ NEW PLACEHOLDER
**Status**: Not yet implemented
**Tasks**:
1. Track complete franchise history including relocations
2. Store: franchise_id, name, city, founded_year, folded_year
3. Link to team_id (franchises can have multiple team identities)
4. Track: relocations, name changes, ownership changes
**Use Cases**: Franchise history analysis, team legacy, relocation patterns
**Priority**: LOW

### 4.3 - Populate officials_directory 🔴 TODO
**File**: `scripts/populate/populate_officials.py` ✨ NEW PLACEHOLDER
**Status**: Not yet implemented (0 rows)
**Tasks**:
1. Fetch referee/official information from NBA
2. Track: official_id, name, jersey_number, years_experience
3. Link officials to games they worked
4. Support historical officials data
**Use Cases**: Referee patterns, officiating bias analysis, foul tendencies
**Priority**: LOW

### 4.4 - Create data dictionary 🟡 Partial → TODO
**File**: `docs/data_dictionary.md` ✨ NEW SKELETON
**Status**: Skeleton created, needs comprehensive content
**Tasks**:
1. Document all 58 tables in the database
2. For each table: purpose, column descriptions, keys, sample queries, data sources
3. Document table relationships and foreign keys
4. Add data lineage (raw → silver → gold)
5. Include advanced metrics formulas and calculations
**Format**: Markdown with examples
**Priority**: MEDIUM
**Impact**: Critical for end users and developers

### 4.5 - Add automated quality tests 🔴 TODO
**File**: `scripts/maintenance/check_integrity.py`
**Status**: Not implemented
**Tasks**:
1. Null value checks for critical columns
2. Data range validation (e.g., fg_pct between 0 and 1)
3. Cross-table consistency checks
4. Duplicate detection beyond primary keys
5. Historical data completeness checks
**Consider**: Great Expectations, dbt tests, or custom test suite
**Priority**: MEDIUM

---

## Summary Statistics

### Files Modified
- ✏️ `scripts/check_integrity.py` - Added Phase 1.4 and 4.5 TODOs
- ✏️ `scripts/create_advanced_metrics.py` - Added Phase 2.3 and 2.4 TODOs
- ✏️ `scripts/normalize_db.py` - Added Phase 1.2 and 1.5 TODOs
- ✏️ `scripts/populate/populate_play_by_play.py` - Added Phase 3.1 TODO
- ✏️ `scripts/populate/populate_player_season_stats.py` - Added Phase 2.5 TODO

### Files Created (Placeholders)
- ✨ `scripts/populate/populate_shot_chart.py` - Phase 3.2
- ✨ `scripts/populate/populate_salaries.py` - Phase 3.3
- ✨ `scripts/populate/populate_transactions.py` - Phase 3.4
- ✨ `scripts/populate/populate_injury_data.py` - Phase 3.5
- ✨ `scripts/populate/populate_arenas.py` - Phase 4.1
- ✨ `scripts/populate/populate_franchises.py` - Phase 4.2
- ✨ `scripts/populate/populate_officials.py` - Phase 4.3
- ✨ `docs/data_dictionary.md` - Phase 4.4 skeleton

### By Priority
- 🔴 **P0 (CRITICAL)**: 0 items (Phase 2.1 already completed)
- 🟡 **P1 (HIGH)**: 1 item (Phase 3.1 - blocked by API)
- 🟠 **P2 (MEDIUM)**: 8 items (Phases 1.2, 1.5, 2.3, 2.4, 2.5, 3.2, 3.3, 4.4, 4.5)
- ⚪ **P3 (LOW)**: 4 items (Phases 3.4, 3.5, 4.1, 4.2, 4.3)

### By Status
- ✅ **Complete**: 0 items with TODOs (marking what's needed)
- 🟡 **Partial**: 5 items (1.2, 2.3, 2.5, 3.1, 4.4)
- 🔴 **Not Started**: 8 items (1.4, 1.5, 2.4, 3.2, 3.3, 3.4, 3.5, 4.1, 4.2, 4.3, 4.5)

---

## How to Use This Document

1. **Find TODOs in code**: Search for `TODO: ROADMAP Phase` in the codebase
2. **Track progress**: Update this document as TODOs are completed
3. **Prioritize work**: Focus on P0/P1 items first, especially blocked items
4. **Reference docs/roadmap.md**: See complete context and formulas in docs/roadmap.md

## Next Steps

### Immediate (P0/P1)
1. ✅ DONE: Phase 2.1 - player_game_stats populated (10,000+ rows)
2. 🔴 **BLOCKED**: Phase 3.1 - Resolve NBA API issues for play-by-play

### Short-term (P2)
1. Phase 2.3 - Complete advanced metrics (USG%, PER, etc.)
2. Phase 2.4 - Add possessions/pace calculations
3. Phase 4.4 - Complete data dictionary
4. Phase 4.5 - Add automated quality tests

### Long-term (P3)
1. Phases 3.2-3.5 - Event and economic data
2. Phases 4.1-4.3 - Enrichment data

---

**Last Updated**: 2024-12-28
**TODO Count**: 13 major items across 4 phases
