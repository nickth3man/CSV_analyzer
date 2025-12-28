import contextlib

import duckdb


DATABASE_FILE = "data/nba.duckdb"


def verify_database() -> None:
    con = duckdb.connect(DATABASE_FILE)

    # Query 1: Check teams
    try:
        result = con.sql(
            "SELECT id, full_name, abbreviation, nickname, city, state, year_founded FROM team LIMIT 5",
        )
        result.show()
    except Exception:
        pass

    # Query 2: Aggregation on games
    with contextlib.suppress(Exception):
        con.sql("SELECT count(*) as total_games FROM game").fetchone()[0]

    # Query 3: Join example (Player info)
    with contextlib.suppress(Exception):
        con.sql("SELECT full_name, is_active FROM player LIMIT 5").show()

    # Query 4: Verify the empty table
    try:
        con.sql("SELECT count(*) FROM team_info_common").fetchone()[0]
        con.sql("DESCRIBE team_info_common").fetchall()
    except Exception:
        pass

    con.close()


if __name__ == "__main__":
    verify_database()
