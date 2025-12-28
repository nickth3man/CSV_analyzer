### Current DuckDB Snapshot

- **Breadth vs. depth** – The `data/nba.duckdb` catalog exposes 43 tables, but many are empty (`salaries`, `arenas`, `transactions`, `play_by_play`, `seasons`, etc.) and several core tables are duplicated as raw text (`game`, `player`, `team`) versus typed “silver/gold” versions.  
- **Data-type drift** – Key numeric fields were coerced to `BIGINT`, so shooting percentages end up as 0/1 flags (`fg_pct`, `fg3_pct`, `ft_pct`, `plus_minus`, etc.), wiping out real precision.  
- **Season semantics** – `season_id` encodes both season type and year (e.g., `42022` for 2021-22 playoffs) but lacks a shared dimension table, making analytics brittle.  
- **Coverage focus** – The database is richest at the team-game grain (`team_game_stats`, `game_gold`) and limited at player-season, player-game, and transactional layers.

### What richer NBA schemas provide

| Source | Notable structures / content |
| --- | --- |
| **mpope9/nba-sql** | Full relational model with `player_game_log`, `player_season`, `team_game_log`, `play_by_play`, `play_by_playv3`, `shot_chart_detail`, and `player_general_traditional_total` tables, plus an ER diagram showing consistent primary/foreign keys across games, teams, players, events, and advanced totals.[1] |
| **GanyLJR/nba_stats_database** | Seven-entity design explicitly covering `season`, `team`, `player`, `coach`, `player stats`, `team stats`, and `contract`, aimed at end-user comparisons (rosters, radar charts) – highlights the importance of contracts and coaching metadata that our DuckDB lacks.[2] |
| **Paradime dbt NBA challenge** | Snowflake source layer ships `player_game_logs`, `team_stats_by_season`, `team_spend_by_season`, `player_salaries_by_season`, etc., underscoring the value of salary, spend, and season aggregates for downstream modeling.[3] |
| **SportsDataIO NBA dictionary** | Commercial feeds append injuries, lineup confirmation, daily fantasy salaries, and advanced box-score rates (TS%, ORB%, Usage, PER, BPM derivatives, etc.) at the player-game grain.[4] |
| **Basketball-Reference glossary** | Defines widely used derived metrics (AST%, ORtg/DRtg, WS / WS48, BPM, VORP, Pace, SOS, Four Factors) that analysts expect from a canonical NBA dataset.[5] |

### Gaps & improvement opportunities

1. **Canonical typing & table deduplication**
   - Declare `game_gold`, `player_silver`, `team_silver`, `team_game_stats` as the authoritative fact tables; drop or quarantine the string-based copies to avoid divergent ETL logic.
   - Recast percentage columns as `DOUBLE` (or store made/attempted pairs only) to restore analytic fidelity.
   - Create `season_dim (season_id, season_type, season_year_start, season_year_end)` so season decoding is explicit.

2. **Player-centric grains that match industry practice**
   - Materialize `player_game_log` and `player_season` tables (either sourced or derived from existing data) to align with nba-sql and Paradime patterns.[1][3]
   - Enforce surrogate keys (`player_id`, `game_id`, `team_id`) and add bridging tables (e.g., `player_team_season`) for roster movement and tenure tracking.

3. **Advanced metrics & possessions**
   - Populate derived metrics such as TS%, eFG%, Usage, ORtg/DRtg, PER, BPM/VORP, Win Shares, Four Factors—either computed from existing box-score data or sourced from APIs. (These are standard per Basketball-Reference and SportsDataIO.)[4][5]
   - Store possessions and pace to support per-100/per-possession reporting.

4. **Events, spatial data, and officiating**
   - Ingest play-by-play feeds into the empty `play_by_play` table and align with event message types à la nba-sql, enabling clutch analysis, lineup impacts, and whistle tracking.[1]
   - Add shot-location detail (`shot_chart_detail`) for modern spatial analytics (shot quality, spacing, defensive impact).[1]

5. **Contracts, salaries, and economics**
   - Revive the empty `salaries` table or add `player_contracts`, `team_payroll` structures similar to the NBA stats database and Paradime sources to answer “value for spend” questions.[2][3]
   - Tie salary data to seasons and teams, accounting for trades and guarantees.

6. **Injuries, availability, and transactions**
   - Mirror SportsDataIO’s injury schema (status, start date, body part, notes, lineup status) and transactions feed to model availability, load management, and roster churn.[4]
   - Populate existing `transactions` table with waiver, signing, trade events, linking to players and teams.

7. **Arena, franchise, and schedule enrichment**
   - Populate `arenas`, `franchises`, `officials_directory`, `team_history` to capture venue, franchise lineage, officiating crews, and to support travel/rest analysis.
   - Build a `schedule` dimension (date, tip-off time, home/away flags, national TV, attendance) using `game_info` and `game_summary`.

8. **Data quality guardrails**
   - Add not-null and foreign-key constraints in DuckDB (or enforce via dbt/tests) to prevent orphan records.
   - Document ETL provenance, refresh cadence, and table semantics in a central data dictionary.

### Suggested roadmap

| Horizon | Impact | Effort | Actions |
| --- | --- | --- | --- |
| **Phase 1 (Schema hygiene)** | High | Medium | Type corrections, select canonical tables, add season dimension, enforce keys. |
| **Phase 2 (Player & advanced metrics)** | High | High | Build `player_game_log` + `player_season`, compute/per ingest advanced statistics, create views for per-possession conditioning. |
| **Phase 3 (Events & economics)** | Medium | High | Load play-by-play & shot charts, populate salaries/contracts, add injury & transaction feeds. |
| **Phase 4 (Enrichment & docs)** | Medium | Medium | Fill arenas/franchise/officials, publish data dictionary, add automated data-quality tests. |

Implementing these steps will bring the DuckDB closer to feature parity with the strongest open-source and commercial NBA datasets while preserving our existing Chainlit/agent workflows.

---

**References**

[1] mpop9/nba-sql – supported tables & ER diagram (Postgres/SQLite NBA schema).  
[2] GanyLJR/nba_stats_database – README outlining season, coach, contract entities.  
[3] Paradime dbt NBA challenge – README describing source tables (`player_game_logs`, `team_stats_by_season`, `player_salaries_by_season`, etc.).  
[4] SportsDataIO NBA data dictionary – player-game feed with injuries, fantasy salaries, advanced rates.  
[5] Basketball-Reference glossary – definitions for AST%, ORtg, TS%, BPM, VORP, WS, etc.
