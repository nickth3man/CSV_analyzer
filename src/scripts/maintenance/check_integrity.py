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
from typing import Any

import duckdb


DATABASE = "src/backend/data/nba.duckdb"


def check_integrity(db_path: str | None = None) -> dict[str, Any]:
    """Validate and enforce primary key and foreign key integrity for tables in the DuckDB database.

    Checks a set of primary-key candidates and, when every row has a unique, non-null key,
    attempts to set the column NOT NULL, create a unique index, and add a PRIMARY KEY
    constraint. Checks a set of foreign-key relationships by counting orphaned child keys;
    when no orphans are found, attempts to add a FOREIGN KEY constraint, otherwise it
    fetches up to three sample orphan keys. Operations that modify schema are attempted
    but errors are suppressed. The function opens a DuckDB connection to DATABASE and
    closes it before returning.
    """
    db_path = db_path or DATABASE
    con = duckdb.connect(db_path)

    results = {"pk_checks": [], "fk_checks": [], "error_count": 0}

    # 1. Check Primary Keys
    pk_candidates = [
        ("team_gold", "id"),
        ("player_gold", "id"),
        ("games", "game_id"),
    ]

    for table, pk in pk_candidates:
        try:
            # Check if table exists first
            table_exists = (
                con.sql(
                    f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table}'"
                ).fetchone()[0]
                > 0
            )
            if not table_exists:
                continue

            row = con.sql(f"SELECT count(*) FROM {table}").fetchone()
            total = row[0] if row else 0
            row = con.sql(f"SELECT count(DISTINCT {pk}) FROM {table}").fetchone()
            unique = row[0] if row else 0
            row = con.sql(
                f"SELECT count(*) FROM {table} WHERE {pk} IS NULL",
            ).fetchone()
            nulls = row[0] if row else 0

            status = "Passed"
            if total != unique or nulls > 0:
                status = "Failed"
                results["error_count"] += 1

            results["pk_checks"].append(
                {
                    "table": table,
                    "column": pk,
                    "total": total,
                    "unique": unique,
                    "nulls": nulls,
                    "status": status,
                }
            )

            if status == "Passed":
                # We can explicitly add the constraint in DuckDB
                try:
                    con.sql(f"ALTER TABLE {table} ALTER {pk} SET NOT NULL")
                    # DuckDB support for adding PK to existing table is limited, index works for performance.
                    con.sql(
                        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_{pk} ON {table} ({pk})"
                    )
                    # Standard SQL: ALTER TABLE t ADD PRIMARY KEY (id)
                    # Note: DuckDB might fail if PK already exists or table was created without it
                    con.sql(f"ALTER TABLE {table} ADD PRIMARY KEY ({pk})")
                except Exception:
                    pass
        except Exception as e:
            results["error_count"] += 1
            results["pk_checks"].append(
                {"table": table, "column": pk, "status": "Error", "error": str(e)}
            )

    # 2. Check Foreign Keys
    fk_checks = [
        ("games", "home_team_id", "team_gold", "id"),
        ("games", "visitor_team_id", "team_gold", "id"),
        ("common_player_info_silver", "person_id", "player_gold", "id"),
        ("player_game_stats", "player_id", "player_gold", "id"),
        ("player_game_stats", "team_id", "team_gold", "id"),
        ("player_game_stats", "game_id", "games", "game_id"),
    ]

    for child_table, child_col, parent_table, parent_col in fk_checks:
        try:
            # Check if tables exist
            child_exists = (
                con.sql(
                    f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{child_table}'"
                ).fetchone()[0]
                > 0
            )
            parent_exists = (
                con.sql(
                    f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{parent_table}'"
                ).fetchone()[0]
                > 0
            )

            if not child_exists or not parent_exists:
                continue

            # count orphans
            query = f"""
                SELECT count(DISTINCT c.{child_col})
                FROM {child_table} c
                LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL
            """
            row = con.sql(query).fetchone()
            orphan_count = row[0] if row else 0

            status = "Passed"
            orphans = []
            if orphan_count > 0:
                status = "Failed"
                results["error_count"] += 1
                # Show sample orphans
                orphans = [
                    r[0]
                    for r in con.sql(f"""
                    SELECT DISTINCT c.{child_col}
                    FROM {child_table} c
                    LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                    WHERE p.{parent_col} IS NULL AND c.{child_col} IS NOT NULL
                    LIMIT 3
                """).fetchall()
                ]

            results["fk_checks"].append(
                {
                    "child_table": child_table,
                    "child_col": child_col,
                    "parent_table": parent_table,
                    "parent_col": parent_col,
                    "orphan_count": orphan_count,
                    "status": status,
                    "sample_orphans": orphans,
                }
            )

            if status == "Passed":
                # Adding FK constraint
                with contextlib.suppress(Exception):
                    con.sql(
                        f"ALTER TABLE {child_table} ADD FOREIGN KEY ({child_col}) REFERENCES {parent_table}({parent_col})",
                    )

        except Exception as e:
            results["error_count"] += 1
            results["fk_checks"].append(
                {
                    "child_table": child_table,
                    "child_col": child_col,
                    "status": "Error",
                    "error": str(e),
                }
            )

    con.close()
    return results


if __name__ == "__main__":
    import json

    results = check_integrity()
    print(json.dumps(results, indent=2))
