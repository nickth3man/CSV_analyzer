import duckdb


DATABASE = "src/backend/data/nba.duckdb"


def fix_duplicates() -> None:
    con = duckdb.connect(DATABASE)

    # Check duplicate counts
    row = con.sql("SELECT count(*) FROM game_silver").fetchone()
    total = row[0] if row else 0
    row = con.sql("SELECT count(DISTINCT game_id) FROM game_silver").fetchone()
    unique = row[0] if row else 0

    if total > unique:
        # Simple deduplication: keep one row per game_id.
        # If rows are identical, DISTINCT works. If they differ, we need arbitrary choice.
        # Let's check if they are identical using DISTINCT *
        row = con.sql(
            "SELECT count(*) FROM (SELECT DISTINCT * FROM game_silver)",
        ).fetchone()
        distinct_rows = row[0] if row else 0

        if distinct_rows == unique:
            con.sql(
                "CREATE OR REPLACE TABLE game_gold AS SELECT DISTINCT * FROM game_silver",
            )
        else:
            # Pick the one with most data? or just arbitrary. Let's use arbitrary row_number.
            con.sql("""
                CREATE OR REPLACE TABLE game_gold AS
                SELECT * FROM game_silver
                QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY game_date DESC) = 1
             """)

        # Verify
        row = con.sql("SELECT count(*) FROM game_gold").fetchone()
        new_total = row[0] if row else 0

        # Verify PK
        row = con.sql(
            "SELECT count(DISTINCT game_id) FROM game_gold",
        ).fetchone()
        new_unique = row[0] if row else 0
        if new_total == new_unique:
            pass
            # Create View for cleaner access? Or just leave as table.
        else:
            pass

    con.close()


if __name__ == "__main__":
    fix_duplicates()
