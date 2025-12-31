"""Check database integrity constraints and quality.

TODO: ROADMAP Phase 1.4 - Add comprehensive FK constraints/tests
- Implement systematic FK constraint validation for all major table relationships
- Add referential integrity tests for game_gold -> team_silver
- Add constraints for player_game_stats -> player_silver/game_gold
- Consider using dbt or similar for automated constraint testing
Reference: docs/roadmap.md Phase 1.4

TODO: ROADMAP Phase 4.5 - Add automated quality tests
- Implement automated data quality checks beyond FK constraints
- Tests needed:
  1. Null value checks for critical columns
  2. Data range validation (e.g., fg_pct between 0 and 1)
  3. Cross-table consistency checks
  4. Duplicate detection beyond primary keys
  5. Historical data completeness checks
- Consider: Great Expectations, dbt tests, or custom test suite
- Priority: MEDIUM (Phase 4.5)
Reference: docs/roadmap.md Phase 4.5
"""

import contextlib

import duckdb


DATABASE = "src/backend/data/nba.duckdb"


def check_integrity() -> None:
    """Validate and enforce primary key and foreign key integrity for tables in the DuckDB database.

    Checks a set of primary-key candidates and, when every row has a unique, non-null key,
    attempts to set the column NOT NULL, create a unique index, and add a PRIMARY KEY
    constraint. Checks a set of foreign-key relationships by counting orphaned child keys;
    when no orphans are found, attempts to add a FOREIGN KEY constraint, otherwise it
    fetches up to three sample orphan keys. Operations that modify schema are attempted
    but errors are suppressed. The function opens a DuckDB connection to DATABASE and
    closes it before returning.
    """
    con = duckdb.connect(DATABASE)

    # 1. Check Primary Keys
    pk_candidates = [
        ("team_silver", "id"),
        ("player_silver", "id"),
        ("game_silver", "game_id"),
    ]

    for table, pk in pk_candidates:
        try:
            row = con.sql(f"SELECT count(*) FROM {table}").fetchone()
            total = row[0] if row else 0
            row = con.sql(f"SELECT count(DISTINCT {pk}) FROM {table}").fetchone()
            unique = row[0] if row else 0
            row = con.sql(
                f"SELECT count(*) FROM {table} WHERE {pk} IS NULL",
            ).fetchone()
            nulls = row[0] if row else 0

            if total == unique and nulls == 0:
                # We can explicitly add the constraint in DuckDB
                try:
                    con.sql(f"ALTER TABLE {table} ALTER {pk} SET NOT NULL")
                    con.sql(
                        f"CREATE UNIQUE INDEX idx_{table}_{pk} ON {table} ({pk})",
                    )  # DuckDB PK syntax via index usually or just PK constraint
                    # DuckDB support for adding PK to existing table is limited, index works for performance.
                    # Standard SQL: ALTER TABLE t ADD PRIMARY KEY (id)
                    con.sql(f"ALTER TABLE {table} ADD PRIMARY KEY ({pk})")
                except Exception:
                    pass
            else:
                pass
        except Exception:
            pass

    # 2. Check Foreign Keys
    # TODO: ROADMAP Phase 1.4 - Expand FK checks to cover additional relationships
    # Missing FK checks:
    # - player_game_stats.player_id -> player_silver.id
    # - player_game_stats.team_id -> team_silver.id
    # - player_game_stats.game_id -> game_gold.game_id
    # - team_game_stats.team_id -> team_silver.id
    # - team_game_stats.game_id -> game_gold.game_id
    # Reference: docs/roadmap.md Phase 1.4
    fk_checks = [
        ("game_silver", "team_id_home", "team_silver", "id"),
        ("game_silver", "team_id_away", "team_silver", "id"),
        ("common_player_info_silver", "person_id", "player_silver", "id"),
        ("player_game_stats", "player_id", "player_silver", "id"),
        ("player_game_stats", "team_id", "team_silver", "id"),
        ("player_game_stats", "game_id", "game_gold", "game_id"),
    ]

    for child_table, child_col, parent_table, parent_col in fk_checks:
        try:
            # count orphans
            query = f"""
                SELECT count(DISTINCT c.{child_col})
                FROM {child_table} c
                LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL
            """
            row = con.sql(query).fetchone()
            orphan_count = row[0] if row else 0

            if orphan_count == 0:
                # Adding FK constraint
                with contextlib.suppress(Exception):
                    con.sql(
                        f"ALTER TABLE {child_table} ADD FOREIGN KEY ({child_col}) REFERENCES {parent_table}({parent_col})",
                    )
            else:
                # Show sample orphans
                con.sql(f"""
                    SELECT DISTINCT c.{child_col}
                    FROM {child_table} c
                    LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                    WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL
                    LIMIT 3
                """).fetchall()

        except Exception:
            pass

    con.close()


if __name__ == "__main__":
    check_integrity()
