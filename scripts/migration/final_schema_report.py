import duckdb


DATABASE = "src/backend/data/nba.duckdb"


def final_report() -> None:
    con = duckdb.connect(DATABASE)

    tables = ["team_silver", "player_silver", "game_gold"]

    for t in tables:
        row = con.sql(f"SELECT count(*) FROM {t}").fetchone()
        row_count = row[0] if row else 0
        desc = con.sql(f"DESCRIBE {t}").fetchall()
        for _col in desc:
            pass

    con.close()


if __name__ == "__main__":
    final_report()
