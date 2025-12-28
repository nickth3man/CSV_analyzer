# NBA Data Analyst Agent - Database Roadmap

> **Last Updated**: 2024-12-28  
> **Current Database**: `data/nba.duckdb` (52 tables, ~46MB)

---

## Executive Summary

This roadmap outlines the path to transform our NBA DuckDB database from a team-game focused dataset into a comprehensive, analytics-ready platform supporting player comparisons, advanced metrics, play-by-play analysis, and economic insights.

### Quick Status Dashboard

| Phase | Status | Progress | Blocking Issues |
|-------|--------|----------|-----------------|
| Phase 1 (Schema Hygiene) | 🟡 In Progress | 70% | `plus_minus` type fix needed |
| Phase 2 (Player & Metrics) | 🔴 Critical | 10% | `player_game_stats` empty (0 rows) |
| Phase 3 (Events & Economics) | ⚪ Not Started | 0% | Depends on Phase 2 |
| Phase 4 (Enrichment & Docs) | ⚪ Not Started | 0% | Depends on Phase 3 |

---

## Current DuckDB Snapshot (As of 2024-12-28)

### Database Statistics

| Metric | Value | Notes |
|--------|-------|-------|
| Total Tables | 52 | Mix of raw, silver, gold, dimension, fact |
| Empty Tables | 12 (23%) | Critical gaps in player and event data |
| Total Rows (est.) | ~400K | Primarily team-game data |
| File Size | 46 MB | Room for growth |

### Table Categories

#### ✅ Well-Populated Tables (Ready for Analytics)
| Table | Rows | Quality | Use Case |
|-------|------|---------|----------|
| `team_game_stats` | 131,284 | Good | Team performance per game |
| `game_gold` | 65,642 | Good | Game-level facts (deduped) |
| `player_silver` | 4,831 | Good | Player dimension |
| `team_silver` | 30 | Good | Team dimension |
| `season_dim` | 225 | Good | Season decoding |
| `common_player_info` | 4,831 | Good | Player biographical data |
| `draft_history` | 5,294 | Good | Draft picks |

#### 🔴 Empty Tables (Critical Gaps)
| Table | Status | Priority | Impact |
|-------|--------|----------|--------|
| `player_game_stats` | 0 rows | 🔴 CRITICAL | Blocks ALL player-level analytics |
| `play_by_play` | 0 rows | 🟡 HIGH | Blocks clutch/lineup analysis |
| `salaries` | 0 rows | 🟡 MEDIUM | Blocks value analysis |
| `transactions` | 0 rows | 🟠 LOW | Blocks roster movement tracking |
| `arenas` | 0 rows | 🟠 LOW | Blocks venue analytics |
| `awards` | 0 rows | 🟠 LOW | Blocks accolade analysis |

### Data Type Issues

| Column | Current Type | Should Be | Tables Affected | Status |
|--------|--------------|-----------|-----------------|--------|
| `fg_pct` | DOUBLE | DOUBLE | fact_*, player_game_stats | ✅ Fixed |
| `fg3_pct` | DOUBLE | DOUBLE | fact_*, player_game_stats | ✅ Fixed |
| `ft_pct` | DOUBLE | DOUBLE | fact_*, player_game_stats | ✅ Fixed |
| `plus_minus` | BIGINT | DOUBLE | team_game_stats, player_game_stats | 🔴 Needs Fix |

---

## What Richer NBA Schemas Provide

| Source | Notable Structures / Content |
|--------|------------------------------|
| **mpope9/nba-sql** | Full relational model with `player_game_log`, `player_season`, `team_game_log`, `play_by_play`, `play_by_playv3`, `shot_chart_detail`, and `player_general_traditional_total` tables, plus an ER diagram showing consistent primary/foreign keys across games, teams, players, events, and advanced totals.[1] |
| **GanyLJR/nba_stats_database** | Seven-entity design explicitly covering `season`, `team`, `player`, `coach`, `player stats`, `team stats`, and `contract`, aimed at end-user comparisons (rosters, radar charts) – highlights the importance of contracts and coaching metadata that our DuckDB lacks.[2] |
| **Paradime dbt NBA challenge** | Snowflake source layer ships `player_game_logs`, `team_stats_by_season`, `team_spend_by_season`, `player_salaries_by_season`, etc., underscoring the value of salary, spend, and season aggregates for downstream modeling.[3] |
| **SportsDataIO NBA dictionary** | Commercial feeds append injuries, lineup confirmation, daily fantasy salaries, and advanced box-score rates (TS%, ORB%, Usage, PER, BPM derivatives, etc.) at the player-game grain.[4] |
| **Basketball-Reference glossary** | Defines widely used derived metrics (AST%, ORtg/DRtg, WS / WS48, BPM, VORP, Pace, SOS, Four Factors) that analysts expect from a canonical NBA dataset.[5] |

---

## Detailed Phase Breakdown

### Phase 1: Schema Hygiene (High Impact, Medium Effort)

**Goal**: Establish clean, typed, canonical tables with proper relationships.

| Task ID | Task | Status | Owner | Notes |
|---------|------|--------|-------|-------|
| 1.1 | Fix `plus_minus` → DOUBLE | 🔴 TODO | - | In `team_game_stats`, `player_game_stats` |
| 1.2 | Document canonical tables | 🟡 Partial | - | silver/gold vs raw text |
| 1.3 | Create `season_dim` | ✅ DONE | - | 225 rows, proper columns |
| 1.4 | Add FK constraints/tests | 🔴 TODO | - | dbt or DuckDB constraints |
| 1.5 | Quarantine raw text tables | 🔴 TODO | - | `game`, `player`, `team` |

**Canonical Table Selection**:
- ✅ `player_silver` (not `player`) - typed player dimension
- ✅ `team_silver` (not `team`) - typed team dimension  
- ✅ `game_gold` (not `game`) - typed game facts
- ✅ `team_game_stats` - team performance per game
- ✅ `season_dim` - season decoding

---

### Phase 2: Player & Advanced Metrics (High Impact, High Effort)

**Goal**: Enable player-level analytics with industry-standard metrics.

| Task ID | Task | Status | Priority | Rows Expected |
|---------|------|--------|----------|---------------|
| 2.1 | Populate `player_game_stats` | 🔴 CRITICAL | P0 | 500K-1M+ |
| 2.2 | Create `player_season` aggregation | 🔴 TODO | P1 | ~50K |
| 2.3 | Create advanced metrics views | 🔴 TODO | P1 | Computed |
| 2.4 | Add possessions/pace data | 🔴 TODO | P2 | Per game |
| 2.5 | Verify `bridge_player_team_season` | 🟡 Partial | P2 | Exists |

**Advanced Metrics to Implement**:

| Metric | Formula | Category |
|--------|---------|----------|
| **TS%** (True Shooting) | `PTS / (2 * (FGA + 0.475 * FTA))` | Efficiency |
| **eFG%** (Effective FG) | `(FGM + 0.5 * FG3M) / FGA` | Efficiency |
| **AST%** | `AST / (((MP / (Tm_MP / 5)) * Tm_FGM) - FGM)` | Playmaking |
| **TOV%** | `TOV / (FGA + 0.44 * FTA + TOV)` | Ball Security |
| **USG%** | `((FGA + 0.44 * FTA + TOV) * (Tm_MP / 5)) / (MP * Tm_Poss)` | Volume |
| **ORtg** | Points produced per 100 possessions | Offense |
| **DRtg** | Points allowed per 100 possessions | Defense |
| **PER** | Player Efficiency Rating (complex) | Overall |
| **BPM** | Box Plus/Minus | Impact |
| **VORP** | Value Over Replacement | Impact |

---

### Phase 3: Events & Economics (Medium Impact, High Effort)

**Goal**: Enable play-by-play analysis and economic insights.

| Task ID | Task | Status | Priority | Data Source |
|---------|------|--------|----------|-------------|
| 3.1 | Populate `play_by_play` | ⚪ TODO | P1 | NBA API |
| 3.2 | Add `shot_chart_detail` | ⚪ TODO | P2 | NBA API |
| 3.3 | Populate `salaries` | ⚪ TODO | P2 | External |
| 3.4 | Populate `transactions` | ⚪ TODO | P3 | NBA API |
| 3.5 | Add injury data | ⚪ TODO | P3 | External |

---

### Phase 4: Enrichment & Documentation (Medium Impact, Medium Effort)

**Goal**: Complete the dataset and document for users.

| Task ID | Task | Status | Priority |
|---------|------|--------|----------|
| 4.1 | Populate `arenas` | ⚪ TODO | P3 |
| 4.2 | Populate `franchises` | ⚪ TODO | P3 |
| 4.3 | Populate `officials_directory` | ⚪ TODO | P3 |
| 4.4 | Create data dictionary | ⚪ TODO | P2 |
| 4.5 | Add automated quality tests | ⚪ TODO | P2 |

---

## Implementation Scripts

### Available Scripts (in `/scripts/`)

| Script | Purpose | Status |
|--------|---------|--------|
| `convert_csvs.py` | CSV to DuckDB ingestion | ✅ Working |
| `normalize_db.py` | Data type normalization | ✅ Working |
| `check_integrity.py` | Database integrity checks | ✅ Working |
| `expand_schema.py` | Schema expansion utilities | ✅ Working |
| `create_advanced_shells.py` | Advanced metric shells | ✅ Working |
| `populate_player_game_stats.py` | Player game data population | 🔴 TODO |
| `create_advanced_views.py` | Advanced metrics views | 🔴 TODO |

---

## Success Metrics

| Milestone | Metric | Target | Current |
|-----------|--------|--------|---------|
| Phase 1 Complete | Data type issues | 0 | 1 |
| Phase 2 Complete | `player_game_stats` rows | > 500,000 | 0 |
| Phase 2 Complete | Advanced metrics available | 10+ | 0 |
| Phase 3 Complete | `play_by_play` events | > 1,000,000 | 0 |
| Phase 4 Complete | Documentation coverage | 100% | ~30% |

---

## Risk Register

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| NBA API rate limiting | High | High | Implement caching, 0.6s delays |
| API endpoint changes | Medium | Low | Version lock nba_api package |
| Large data volumes | Medium | Medium | Batch processing, chunked inserts |
| Historical data gaps | Low | Medium | Document known gaps |

---

## Changelog

### 2024-12-28
- Updated table counts (43 → 52 tables discovered)
- Confirmed `season_dim` exists and is populated (225 rows)
- Identified `player_game_stats` as critical blocker (0 rows)
- Verified percentage columns fixed to DOUBLE (except `plus_minus`)
- Added detailed task tracking and success metrics

### Previous
- Initial roadmap created
- Identified schema hygiene issues
- Documented external reference schemas

---

## References

[1] mpope9/nba-sql – supported tables & ER diagram (Postgres/SQLite NBA schema).  
[2] GanyLJR/nba_stats_database – README outlining season, coach, contract entities.  
[3] Paradime dbt NBA challenge – README describing source tables.  
[4] SportsDataIO NBA data dictionary – player-game feed with injuries, fantasy salaries, advanced rates.  
[5] Basketball-Reference glossary – definitions for AST%, ORtg, TS%, BPM, VORP, WS, etc.
