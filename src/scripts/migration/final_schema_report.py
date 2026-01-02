import duckdb


DATABASE = "src/backend/data/nba.duckdb"


def final_report() -> None:
    con = duckdb.connect(DATABASE)

    tables = ["team_silver", "player_silver", "game_gold"]

    for t in tables:
        desc = con.sql(f"DESCRIBE {t}").fetchall()
        for _col in desc:
            pass

    con.close()


if __name__ == "__main__":
    final_report()
