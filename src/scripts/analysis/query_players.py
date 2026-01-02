import duckdb


DATABASE_FILE = "src/backend/data/nba.duckdb"


def query_players() -> None:
    con = duckdb.connect(DATABASE_FILE)

    players = ["Pete Maravich", "Tracy McGrady"]

    for player_name in players:
        # Search in common_player_info_silver which contains bio info
        # We use ILIKE for case-insensitive search

        try:
            # First check if columns exist by selecting * first in a safe way or just try-catch
            # But based on report we know some columns. Let's try select * first to be safe if I guessed column names wrong,
            # but I saw the report.
            # actually, let's just select * for the matching row to see everything available.

            # Use parameterized query via sql() which returns a relation
            search_pattern = f"%{player_name}%"
            rel = con.sql(
                "SELECT * FROM common_player_info_silver WHERE display_first_last ILIKE $1",
                params=[search_pattern],
            )

            if rel.fetchone() is not None:
                # showing the result
                # Use a separate relation object and ignore type checking for .show()
                res_rel = con.sql(
                    "SELECT person_id, display_first_last, birthdate, school, country, height, weight, season_exp, jersey, position, team_name, from_year, to_year FROM common_player_info_silver WHERE display_first_last ILIKE $1",
                    params=[search_pattern],
                )
                res_rel.show()  # type: ignore[attr-defined]
            else:
                # Fallback: Search in canonical player table
                player_rel = con.sql(
                    "SELECT * FROM player_gold WHERE full_name ILIKE $1",
                    params=[search_pattern],
                )
                if player_rel.fetchone() is not None:
                    # Re-create relation for showing
                    show_rel = con.sql(
                        "SELECT * FROM player_gold WHERE full_name ILIKE $1",
                        params=[search_pattern],
                    )
                    show_rel.show()  # type: ignore[attr-defined]
                else:
                    print(f"No results found for: {player_name}")

        except Exception as e:
            print(f"Error querying player '{player_name}': {e}")

    con.close()


if __name__ == "__main__":
    query_players()
