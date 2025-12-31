"""Deep Database Structural Analysis for nba_expert project.

This script performs a comprehensive analysis of the DuckDB database including:
- Schema analysis (tables, columns, data types)
- Row count analysis
- Data distribution analysis (null percentages)
- Timestamp analysis (data freshness)
- CSV vs Database comparison
"""

import io
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


# Set stdout to UTF-8 encoding for Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))


# Current date for comparison
CURRENT_DATE = datetime(2025, 12, 31)
CURRENT_DATE_ISO = "2025-12-31T09:19:13.767Z"

# Database and CSV paths
DB_PATH = project_root / "src" / "backend" / "data" / "nba.duckdb"
CSV_DIR = project_root / "src" / "backend" / "data" / "raw" / "csv"

# Null percentage threshold
NULL_THRESHOLD = 20.0


def get_csv_row_count(csv_file: Path) -> int:
    """Get row count from a CSV file."""
    try:
        # Read CSV and count rows (excluding header)
        df = pd.read_csv(csv_file)
        return len(df)
    except Exception as e:
        print(f"  Error reading CSV {csv_file.name}: {e}")
        return 0


def analyze_database() -> dict[str, Any]:
    """Perform comprehensive database analysis."""
    results = {
        "analysis_date": CURRENT_DATE_ISO,
        "database_path": str(DB_PATH),
        "tables": {},
        "summary": {
            "total_tables": 0,
            "total_rows": 0,
            "empty_tables": [],
            "low_row_tables": [],
            "high_null_columns": [],
            "timestamp_columns": [],
            "csv_comparison": {},
        },
    }

    # Connect to database directly using duckdb
    conn = duckdb.connect(str(DB_PATH), read_only=True)

    # Get all tables
    tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """
    tables_df = conn.execute(tables_query).fetchdf()
    table_names = tables_df["table_name"].tolist()

    results["summary"]["total_tables"] = len(table_names)

    print("=" * 80)
    print("DEEP DATABASE STRUCTURAL ANALYSIS")
    print("=" * 80)
    print(f"Analysis Date: {CURRENT_DATE_ISO}")
    print(f"Database Path: {DB_PATH}")
    print("=" * 80)
    print()

    # 1. Table List
    print("1. TABLE LIST")
    print("-" * 80)
    print(f"Total tables found: {len(table_names)}")
    print()
    for i, table in enumerate(table_names, 1):
        print(f"  {i}. {table}")
    print()

    # 2. Detailed Table Analysis
    print("2. DETAILED TABLE ANALYSIS")
    print("=" * 80)
    print()

    for table_name in table_names:
        print(f"--- Table: {table_name} ---")

        table_info = {
            "columns": {},
            "row_count": 0,
            "null_analysis": {},
            "timestamp_columns": [],
        }

        # Get row count
        count_query = f'SELECT COUNT(*) FROM "{table_name}"'
        row_count = conn.execute(count_query).fetchone()[0]
        table_info["row_count"] = row_count
        results["summary"]["total_rows"] += row_count

        print(f"Row Count: {row_count:,}")

        # Check for empty tables
        if row_count == 0:
            results["summary"]["empty_tables"].append(table_name)
            print("  [WARNING] Table is EMPTY!")
        elif row_count < 10:
            results["summary"]["low_row_tables"].append(table_name)
            print(f"  [WARNING] Table has suspiciously low row count: {row_count}")

        # Get column schema
        schema_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        columns_df = conn.execute(schema_query).fetchdf()

        print(f"Columns ({len(columns_df)}):")
        timestamp_columns = []

        for _, row in columns_df.iterrows():
            col_name = row["column_name"]
            data_type = row["data_type"]
            nullable = row["is_nullable"]

            table_info["columns"][col_name] = {
                "data_type": data_type,
                "nullable": nullable,
            }

            # Identify timestamp/date columns
            is_timestamp = col_name.lower() in [
                "game_date",
                "created",
                "updated",
                "date",
                "timestamp",
            ] or data_type in [
                "DATE",
                "TIMESTAMP",
                "TIMESTAMP WITH TIME ZONE",
            ]
            if is_timestamp:
                timestamp_columns.append(col_name)

            print(
                f"  - {col_name}: {data_type} ({'NULL' if nullable == 'YES' else 'NOT NULL'})"
            )

        # Null value analysis (sample first 1000 rows)
        if row_count > 0:
            sample_query = f'SELECT * FROM "{table_name}" LIMIT 1000'
            sample_df = conn.execute(sample_query).fetchdf()

            print(f"\nNull Value Analysis (sample of {len(sample_df)} rows):")
            for col_name in sample_df.columns:
                null_count = sample_df[col_name].isna().sum()
                null_pct = (null_count / len(sample_df)) * 100
                table_info["null_analysis"][col_name] = {
                    "null_count": int(null_count),
                    "null_percentage": round(null_pct, 2),
                }

                if null_pct > NULL_THRESHOLD:
                    results["summary"]["high_null_columns"].append(
                        {
                            "table": table_name,
                            "column": col_name,
                            "null_percentage": round(null_pct, 2),
                        }
                    )
                    print(
                        f"  [WARN] {col_name}: {null_pct:.1f}% null ({int(null_count)}/{len(sample_df)})"
                    )
                elif null_pct > 0:
                    print(f"  [OK] {col_name}: {null_pct:.1f}% null")

        # Timestamp analysis
        if timestamp_columns:
            print("\nTimestamp Analysis:")
            for ts_col in timestamp_columns:
                try:
                    ts_query = f"""
                        SELECT MAX("{ts_col}") as max_date, MIN("{ts_col}") as min_date
                        FROM "{table_name}"
                        WHERE "{ts_col}" IS NOT NULL
                    """
                    ts_result = conn.execute(ts_query).fetchone()

                    if ts_result and ts_result[0]:
                        max_date = ts_result[0]
                        min_date = ts_result[1]
                        print(f"  - {ts_col}:")
                        print(f"    Earliest: {min_date}")
                        print(f"    Latest: {max_date}")

                        # Try to parse the date to calculate freshness
                        try:
                            if isinstance(max_date, str):
                                # Try common date formats
                                for fmt in [
                                    "%Y-%m-%d",
                                    "%Y-%m-%d %H:%M:%S",
                                    "%Y-%m-%dT%H:%M:%S",
                                ]:
                                    try:
                                        parsed_date = datetime.strptime(
                                            max_date.split()[0], "%Y-%m-%d"
                                        )
                                        days_old = (CURRENT_DATE - parsed_date).days
                                        print(f"    Data Age: {days_old} days old")

                                        results["summary"]["timestamp_columns"].append(
                                            {
                                                "table": table_name,
                                                "column": ts_col,
                                                "latest_date": max_date,
                                                "days_old": days_old,
                                            }
                                        )
                                        break
                                    except (ValueError, IndexError):
                                        continue
                        except Exception:
                            pass
                except Exception as e:
                    print(f"  Error analyzing {ts_col}: {e}")

        table_info["timestamp_columns"] = timestamp_columns
        results["tables"][table_name] = table_info
        print()

    # 3. CSV vs Database Comparison
    print("3. CSV VS DATABASE COMPARISON")
    print("=" * 80)
    print()

    if CSV_DIR.exists():
        csv_files = list(CSV_DIR.glob("*.csv"))
        print(f"Found {len(csv_files)} CSV files in {CSV_DIR}")
        print()

        for csv_file in csv_files:
            csv_row_count = get_csv_row_count(csv_file)
            table_name = csv_file.stem  # Remove .csv extension

            if table_name in results["tables"]:
                db_row_count = results["tables"][table_name]["row_count"]
                diff = csv_row_count - db_row_count

                comparison = {
                    "csv_rows": csv_row_count,
                    "db_rows": db_row_count,
                    "difference": diff,
                }
                results["summary"]["csv_comparison"][table_name] = comparison

                print(f"{table_name}:")
                print(f"  CSV rows: {csv_row_count:,}")
                print(f"  DB rows:  {db_row_count:,}")
                print(f"  Difference: {diff:+,}")

                if diff != 0:
                    print("  [WARNING] Row count mismatch!")
            else:
                print(f"{table_name}:")
                print(f"  CSV rows: {csv_row_count:,}")
                print("  [WARNING] Table not found in database!")
            print()
    else:
        print(f"CSV directory not found: {CSV_DIR}")
        print()

    conn.close()
    return results


def print_summary_report(results: dict[str, Any]) -> None:
    """Print a summary report of the analysis."""
    print("=" * 80)
    print("SUMMARY REPORT")
    print("=" * 80)
    print()

    summary = results["summary"]

    # Total tables
    print(f"Total Tables: {summary['total_tables']}")
    print()

    # Row counts
    print("Row Counts:")
    for table_name, table_info in results["tables"].items():
        print(f"  {table_name}: {table_info['row_count']:,} rows")
    print()

    # Empty tables
    if summary["empty_tables"]:
        print("[WARNING] Empty Tables (0 rows):")
        for table in summary["empty_tables"]:
            print(f"  - {table}")
        print()
    else:
        print("No empty tables found.")
        print()

    # Low row tables
    if summary["low_row_tables"]:
        print("[WARNING] Tables with suspiciously low row counts (<10):")
        for table in summary["low_row_tables"]:
            print(f"  - {table}: {results['tables'][table]['row_count']} rows")
        print()
    else:
        print("No tables with suspiciously low row counts.")
        print()

    # High null columns
    if summary["high_null_columns"]:
        print(f"[WARNING] Columns with >{NULL_THRESHOLD}% null values:")
        for item in summary["high_null_columns"]:
            print(
                f"  - {item['table']}.{item['column']}: {item['null_percentage']}% null"
            )
        print()
    else:
        print(f"No columns with >{NULL_THRESHOLD}% null values.")
        print()

    # Timestamp columns
    if summary["timestamp_columns"]:
        print("Timestamp/Data Freshness Analysis:")
        for item in summary["timestamp_columns"]:
            print(f"  - {item['table']}.{item['column']}:")
            print(f"    Latest: {item['latest_date']} ({item['days_old']} days old)")
        print()
    else:
        print("No timestamp/date columns found.")
        print()

    # CSV comparison
    if summary["csv_comparison"]:
        print("CSV vs Database Row Count Comparison:")
        for table_name, comp in summary["csv_comparison"].items():
            status = "[OK]" if comp["difference"] == 0 else "[WARNING]"
            print(f"  {status} {table_name}:")
            print(
                f"    CSV: {comp['csv_rows']:,} | DB: {comp['db_rows']:,} | Diff: {comp['difference']:+,}"
            )
        print()
    else:
        print("No CSV comparison data available.")
        print()


def main():
    """Main entry point."""
    results = analyze_database()
    print_summary_report(results)

    print("=" * 80)
    print("Analysis complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
