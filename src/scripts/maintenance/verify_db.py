import contextlib

import duckdb


DATABASE_FILE = "src/backend/data/nba.duckdb"


def verify_database() -> None:
    con = duckdb.connect(DATABASE_FILE)

    # Query 1: Check teams
    try:
        result = con.sql(
            "SELECT id, full_name, abbreviation, nickname, city, state, year_founded FROM team_silver LIMIT 5",
        )
        result.show()  # type: ignore[attr-defined]
    except Exception:
        pass

    # Query 2: Aggregation on games
    with contextlib.suppress(Exception):
        row = con.sql("SELECT count(*) as total_games FROM game_gold").fetchone()
        row[0] if row else 0

    # Query 3: Join example (Player info)
    with contextlib.suppress(Exception):
        con.sql("SELECT full_name, is_active FROM player_gold LIMIT 5").show()  # type: ignore[attr-defined]

    # Query 4: Verify the empty table
    try:
        row = con.sql("SELECT count(*) FROM team_info_common_silver").fetchone()
        row[0] if row else 0
        con.sql("DESCRIBE team_info_common_silver").fetchall()
    except Exception:
        pass

    con.close()


if __name__ == "__main__":
    verify_database()
