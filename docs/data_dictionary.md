# NBA Database Data Dictionary

TODO: ROADMAP Phase 4.4 - Create comprehensive data dictionary
- Current Status: Partial documentation exists, needs completion
- Requirements:
  1. Document all 58 tables in the database
  2. For each table:
     - Purpose and use cases
     - Column descriptions with data types
     - Primary and foreign keys
     - Sample queries
     - Data sources and update frequency
  3. Document relationships between tables
  4. Add data lineage (raw -> silver -> gold)
  5. Include advanced metrics formulas and calculations
- Format: Markdown with examples
- Priority: MEDIUM (Phase 4.4)
- Impact: Critical for end users and developers
Reference: ROADMAP.md Phase 4.4

## Current Database Status

- **Total Tables**: 58 (52 base tables + 6 views)
- **Well-Populated**: ~15 tables with substantial data
- **Empty Tables**: 13 tables (22%)
- **Database Size**: ~49MB
- **Last Updated**: 2024-12-28

## Table Categories

### Core Dimensions (Silver Layer)
TODO: Document these canonical dimension tables
- `player_silver` - Typed player dimension (4,831 rows)
- `team_silver` - Typed team dimension (30 rows)
- `season_dim` - Season decoding (225 rows)

### Game Facts (Gold Layer)
TODO: Document game-level fact tables
- `game_gold` - Typed game facts (65,642 rows)
- `team_game_stats` - Team performance per game (131,284 rows)
- `player_game_stats` - Player performance per game (10,000+ rows, recently expanded)

### Player Information
TODO: Document player detail tables
- `common_player_info` - Player biographical info (4,831 rows)
- `player_season_stats` - Season-aggregated stats (228 rows, recently added)
- `draft_history` - Draft information (7,990 rows)

### Advanced Metrics Views
TODO: Document calculated metrics views
- `player_game_advanced` - Game-level advanced metrics (TS%, eFG%, GmSc, etc.)
- `team_game_advanced` - Team-level efficiency metrics
- `team_four_factors` - Dean Oliver's Four Factors
- `league_season_averages` - League-wide benchmarks
- `player_career_summary` - Career totals and biographical data

### Empty Tables (Awaiting Population)
TODO: Document planned schemas for unpopulated tables
- `play_by_play` - Event-level data (blocked by API issues)
- `salaries` - Player salary information
- `transactions` - Trades, signings, releases
- `arenas` - Venue information
- `awards` - Player and team awards
- `shot_chart_detail` - Shot location data
- And 7 more...

## Column Naming Conventions

TODO: Document naming standards
- `id` vs `_id` suffix
- Percentage columns: `_pct` suffix
- Total vs per-game: `total_pts` vs `ppg`
- Advanced metrics: `ts_pct`, `efg_pct`, etc.

## Data Types

TODO: Document type conventions
- Integer stats: BIGINT
- Percentages: DOUBLE (0.0 to 1.0 or 0 to 100)
- Identifiers: BIGINT or VARCHAR
- Dates: DATE or VARCHAR (depending on source)

## Key Relationships

TODO: Document foreign key relationships and join patterns

### Primary Relationships
- `player_game_stats.player_id` -> `player_silver.id`
- `player_game_stats.team_id` -> `team_silver.id`
- `player_game_stats.game_id` -> `game_gold.game_id`
- `team_game_stats.team_id` -> `team_silver.id`
- `team_game_stats.game_id` -> `game_gold.game_id`

## Common Query Patterns

TODO: Add example queries for common use cases

### Example: Player Season Stats
```sql
-- TODO: Add comprehensive query examples
SELECT
    player_name,
    season,
    games_played,
    pts_per_game,
    ts_pct,
    efg_pct
FROM player_season_stats
WHERE games_played >= 20
ORDER BY pts_per_game DESC
LIMIT 10;
```

## Advanced Metrics Formulas

TODO: Document all advanced metric calculations

### Currently Implemented
- **TS% (True Shooting %)**: `PTS / (2 * (FGA + 0.44 * FTA))`
- **eFG% (Effective FG %)**: `(FGM + 0.5 * FG3M) / FGA`
- **TOV% (Turnover %)**: `100 * TOV / (FGA + 0.44 * FTA + TOV)`
- **GmSc (Game Score)**: Complex formula (see create_advanced_metrics.py)

### Planned (Phase 2.3, 2.4)
- USG% (Usage Rate) - requires possessions data
- ORB%, DRB%, TRB% - requires opponent data
- AST% - requires team context
- PER (Player Efficiency Rating) - complex formula
- ORtg, DRtg - requires possessions
- BPM, VORP - requires regression models

## Data Sources

TODO: Document data provenance for each table
- NBA API endpoints used
- Calculation methods for derived tables
- External data sources (if any)
- Update frequency and methods

## Update History

TODO: Track major schema and data changes
- 2024-12-28: `player_game_stats` expanded from 200 to 10,000+ rows
- 2024-12-28: `player_season_stats` created with 228 records
- 2024-12-28: Advanced metrics views created

## Notes for Users

TODO: Add user guidance
- Data quality notes
- Known limitations
- Recommended best practices
- Common gotchas

---

**This data dictionary is a work in progress.**
See ROADMAP.md Phase 4.4 for the complete implementation plan.
