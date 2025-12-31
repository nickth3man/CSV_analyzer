#!/usr/bin/env python3
"""Quick script to check database table row counts."""

import duckdb


def check_db_state():
    db_path = "src/backend/data/nba.duckdb"
    conn = duckdb.connect(db_path)

    tables = [
        "common_player_info",
        "draft_combine_stats",
        "draft_history",
        "game",
        "game_gold",
        "game_info",
        "game_summary",
        "inactive_players",
        "line_score",
        "officials",
        "other_stats",
        "play_by_play",
        "player",
        "player_game_stats",
        "player_season_stats",
        "player_silver",
        "team",
        "team_details",
        "team_history",
        "team_info_common",
        "team_silver",
    ]

    print("=" * 50)
    print("Database Table Row Counts")
    print("=" * 50)
    print(f"{'Table Name':<25} {'Rows':>15}")
    print("-" * 50)

    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table:<25} {count:>15,}")
        except Exception as e:
            print(f"{table:<25} Error: {str(e)[:30]}")

    print("=" * 50)

    # Check game_gold for season distribution
    print("\nGame Gold Season Distribution:")
    print("-" * 50)
    try:
        result = conn.execute("""
            SELECT season_id, COUNT(*) as game_count
            FROM game_gold
            GROUP BY season_id
            ORDER BY season_id DESC
        """).fetchall()
        for season_id, count in result:
            print(f"  {season_id}: {count:,} games")
    except Exception as e:
        print(f"  Error: {e}")

    conn.close()


if __name__ == "__main__":
    check_db_state()
