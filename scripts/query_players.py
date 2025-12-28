import duckdb


DATABASE_FILE = "data/nba.duckdb"


def query_players() -> None:
    con = duckdb.connect(DATABASE_FILE)

    players = ["Pete Maravich", "Tracy McGrady"]

    for player_name in players:
        # Search in common_player_info which seems to have bio info
        # We use ILIKE for case-insensitive search

        try:
            # First check if columns exist by selecting * first in a safe way or just try-catch
            # But based on report we know some columns. Let's try select * first to be safe if I guessed column names wrong,
            # but I saw the report.
            # actually, let's just select * for the matching row to see everything available.

            # Use parameterized query to prevent SQL injection
            search_pattern = f"%{player_name}%"
            result = con.execute(
                "SELECT * FROM common_player_info WHERE display_first_last ILIKE ?",
                [search_pattern],
            )

            if len(result.fetchall()) > 0:
                # showing the result
                con.execute(
                    "SELECT person_id, display_first_last, birthdate, school, country, height, weight, season_exp, jersey, position, team_name, from_year, to_year FROM common_player_info WHERE display_first_last ILIKE ?",
                    [search_pattern],
                ).show()
            else:
                # Fallback: Search in 'player' table
                player_res = con.execute(
                    "SELECT * FROM player WHERE full_name ILIKE ?",
                    [search_pattern],
                )
                if len(player_res.fetchall()) > 0:
                    player_res.show()
                else:
                    print(f"No results found for: {player_name}")

        except Exception as e:
            print(f"Error querying player '{player_name}': {e}")

    con.close()


if __name__ == "__main__":
    query_players()
