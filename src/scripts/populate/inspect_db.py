from pathlib import Path

import duckdb


def inspect_db():
    db_path = Path("src/backend/data/nba.duckdb")
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return

    print(f"Connecting to database at {db_path}")
    conn = duckdb.connect(str(db_path))

    # Get list of tables
    tables = conn.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]

    print(f"Found {len(table_names)} tables: {', '.join(table_names)}")
    print("-" * 50)

    for table in table_names:
        print(f"Inspecting table: {table}")

        # Row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  Row count: {row_count}")

        if row_count == 0:
            print("  Table is empty.")
            print("-" * 50)
            continue

        # Check for nulls in all columns
        columns = conn.execute(f"DESCRIBE {table}").fetchall()
        # columns: (column_name, column_type, null, key, default, extra)
        col_names = [c[0] for c in columns]

        print("  Null counts per column:")
        null_counts = []
        for col in col_names:
            null_count = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
            ).fetchone()[0]
            if null_count > 0:
                null_counts.append((col, null_count, null_count / row_count * 100))

        if not null_counts:
            print("    No null values found.")
        else:
            for col, count, pct in null_counts:
                print(f"    {col}: {count} ({pct:.1f}%)")

        # Show sample if small enough, or just distinct seasons if applicable
        if "season_year" in col_names:
            seasons = conn.execute(
                f"SELECT DISTINCT season_year FROM {table} ORDER BY season_year"
            ).fetchall()
            seasons_list = [s[0] for s in seasons]
            print(f"  Seasons present: {seasons_list}")

        print("-" * 50)

    conn.close()


if __name__ == "__main__":
    # Adjust working directory if running from helper
    # Assuming we run from project root, path relative to root is src/backend/data/nba.duckdb
    inspect_db()
