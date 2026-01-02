#!/usr/bin/env python3
"""Comprehensive structural analysis of DuckDB database schema.

This script performs schema integrity checks, table relationship analysis,
and overall database structure documentation.
"""

import json
import os
from datetime import datetime
from typing import Any

import duckdb


# Reference timestamp for temporal comparisons
REFERENCE_TIMESTAMP = "2025-12-31T09:10:30.386Z"


def connect_to_duckdb(database_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB database."""
    try:
        if database_path and os.path.exists(database_path):
            conn = duckdb.connect(database=database_path, read_only=True)
        else:
            # Connect to in-memory database if no path provided
            conn = duckdb.connect()
        print("[OK] Successfully connected to DuckDB database")
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to DuckDB: {e}")
        raise


def get_all_tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Retrieve all table names from the database."""
    try:
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        result = conn.execute(query).fetchall()
        tables = [row[0] for row in result]
        print(f"[OK] Found {len(tables)} tables in the database")
        return tables
    except Exception as e:
        print(f"[ERROR] Failed to retrieve tables: {e}")
        return []


def get_table_schema(
    conn: duckdb.DuckDBPyConnection, table_name: str
) -> dict[str, Any]:
    """Retrieve detailed schema information for a specific table."""
    try:
        # Get column information
        columns_query = f"""
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                datetime_precision
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        columns = conn.execute(columns_query).fetchall()

        # Get constraints
        constraints_query = f"""
            SELECT
                constraint_name,
                constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = '{table_name}'
        """
        constraints = conn.execute(constraints_query).fetchall()

        # Get primary key
        pk_query = f"""
            SELECT
                kcu.column_name
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name
            WHERE tc.table_name = '{table_name}'
            AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """
        primary_keys = conn.execute(pk_query).fetchall()

        # Get foreign keys
        fk_query = f"""
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.constraint_column_usage ccu
                ON kcu.constraint_name = ccu.constraint_name
            JOIN information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name
            WHERE tc.table_name = '{table_name}'
            AND tc.constraint_type = 'FOREIGN KEY'
        """
        foreign_keys = conn.execute(fk_query).fetchall()

        # Get table statistics
        stats_query = f"""
            SELECT COUNT(*) as row_count
            FROM {table_name}
        """
        try:
            row_count = conn.execute(stats_query).fetchone()[0]
        except Exception:
            row_count = None

        return {
            "table_name": table_name,
            "columns": [
                {
                    "name": col[0],
                    "data_type": col[1],
                    "is_nullable": col[2] == "YES",
                    "default": col[3],
                    "max_length": col[4],
                    "precision": col[5],
                    "scale": col[6],
                    "datetime_precision": col[7],
                }
                for col in columns
            ],
            "constraints": [
                {"name": constr[0], "type": constr[1]} for constr in constraints
            ],
            "primary_keys": [pk[0] for pk in primary_keys],
            "foreign_keys": [
                {"column": fk[0], "foreign_table": fk[1], "foreign_column": fk[2]}
                for fk in foreign_keys
            ],
            "row_count": row_count,
            "last_analyzed": datetime.now().isoformat(),
        }
    except Exception as e:
        print(f"[ERROR] Failed to retrieve schema for table {table_name}: {e}")
        return {}


def analyze_schema_integrity(schema: dict[str, Any]) -> dict[str, Any]:
    """Analyze schema integrity and identify potential issues."""
    issues = []
    warnings = []

    table_name = schema["table_name"]

    # Check for missing primary keys
    if not schema["primary_keys"]:
        warnings.append(f"Table {table_name} has no primary key defined")

    # Check for nullable columns that might be problematic
    for column in schema["columns"]:
        if column["is_nullable"] and column["name"] in ["id", "uuid", "key"]:
            issues.append(
                f"Potential issue: Column {column['name']} is nullable but appears to be an identifier"
            )

        # Check for potential data type issues
        if column["data_type"] == "VARCHAR" and column["max_length"] is None:
            warnings.append(
                f"Column {column['name']} is VARCHAR with no maximum length specified"
            )

    # Check foreign key relationships
    issues.extend(
        [
            f"Foreign key relationship issue in column {fk['column']}"
            for fk in schema["foreign_keys"]
            if not fk["foreign_table"] or not fk["foreign_column"]
        ]
    )

    return {
        "issues": issues,
        "warnings": warnings,
        "integrity_score": calculate_integrity_score(len(issues), len(warnings)),
    }


def calculate_integrity_score(issues_count: int, warnings_count: int) -> float:
    """Calculate integrity score based on issues and warnings."""
    # Base score of 100, deduct points for issues and warnings
    score = 100.0
    score -= issues_count * 10  # Each issue deducts 10 points
    score -= warnings_count * 2  # Each warning deducts 2 points
    return max(0.0, min(100.0, score))


def analyze_table_relationships(tables: list[dict[str, Any]]) -> dict[str, Any]:
    """Analyze relationships between tables."""
    relationships = []
    relationship_graph = {}

    # Build relationship graph
    for table in tables:
        table_name = table["table_name"]
        relationship_graph[table_name] = {"depends_on": [], "depended_by": []}

    # Analyze foreign key relationships
    for table in tables:
        source_table = table["table_name"]
        for fk in table["foreign_keys"]:
            target_table = fk["foreign_table"]
            if target_table in relationship_graph:
                relationship_graph[source_table]["depends_on"].append(target_table)
                relationship_graph[target_table]["depended_by"].append(source_table)

                relationships.append(
                    {
                        "source_table": source_table,
                        "source_column": fk["column"],
                        "target_table": target_table,
                        "target_column": fk["foreign_column"],
                    }
                )

    # Identify orphan tables (no relationships)
    orphan_tables = [
        table
        for table in relationship_graph
        if not relationship_graph[table]["depends_on"]
        and not relationship_graph[table]["depended_by"]
    ]

    # Identify circular dependencies
    circular_deps = find_circular_dependencies(relationship_graph)

    return {
        "relationships": relationships,
        "relationship_graph": relationship_graph,
        "orphan_tables": orphan_tables,
        "circular_dependencies": circular_deps,
        "total_relationships": len(relationships),
    }


def find_circular_dependencies(graph: dict[str, Any]) -> list[list[str]]:
    """Find circular dependencies in the relationship graph using DFS."""
    circular_deps = []
    visited = set()

    def dfs(node, path):
        if node in path:
            # Found a cycle
            cycle_start = path.index(node)
            cycle = [*path[cycle_start:], node]
            if cycle not in circular_deps and len(cycle) > 1:
                circular_deps.append(cycle)
            return

        if node in visited:
            return

        visited.add(node)
        path.append(node)

        for neighbor in graph[node]["depends_on"]:
            dfs(neighbor, path)

        path.pop()

    for node in graph:
        dfs(node, [])

    return circular_deps


def generate_schema_documentation(schema_data: dict[str, Any]) -> str:
    """Generate comprehensive schema documentation."""
    doc = []
    doc.append("# Database Schema Analysis Report")
    doc.append(f"Generated: {datetime.now().isoformat()}")
    doc.append(f"Reference Timestamp: {REFERENCE_TIMESTAMP}")
    doc.append("")

    # Summary section
    doc.append("## Summary")
    doc.append(f"Total Tables: {len(schema_data['tables'])}")
    doc.append(
        f"Total Relationships: {schema_data['relationships']['total_relationships']}"
    )
    doc.append(f"Orphan Tables: {len(schema_data['relationships']['orphan_tables'])}")
    doc.append(
        f"Circular Dependencies: {len(schema_data['relationships']['circular_dependencies'])}"
    )
    doc.append("")

    # Overall integrity
    avg_integrity = sum(
        t["integrity"]["integrity_score"] for t in schema_data["tables"]
    ) / len(schema_data["tables"])
    doc.append(f"Average Integrity Score: {avg_integrity:.1f}/100")
    doc.append("")

    # Tables section
    doc.append("## Tables")
    for table in sorted(schema_data["tables"], key=lambda x: x["table_name"]):
        doc.append(f"### {table['table_name']}")
        doc.append(
            f"Row Count: {table['row_count'] if table['row_count'] is not None else 'Unknown'}"
        )
        doc.append(f"Integrity Score: {table['integrity']['integrity_score']:.1f}/100")

        if table["primary_keys"]:
            doc.append(f"Primary Key: {', '.join(table['primary_keys'])}")
        else:
            doc.append("Primary Key: None")

        if table["foreign_keys"]:
            doc.append("Foreign Keys:")
            doc.extend(
                [
                    f"  - {fk['column']} -> {fk['foreign_table']}.{fk['foreign_column']}"
                    for fk in table["foreign_keys"]
                ]
            )

        if table["integrity"]["issues"]:
            doc.append("Issues:")
            doc.extend(
                [f"  - [ERROR] {issue}" for issue in table["integrity"]["issues"]]
            )

        if table["integrity"]["warnings"]:
            doc.append("Warnings:")
            doc.extend(
                [f"  - [WARN] {warning}" for warning in table["integrity"]["warnings"]]
            )

        doc.append("Columns:")
        for col in table["columns"]:
            nullable = "YES" if col["is_nullable"] else "NO"
            default = f" = {col['default']}" if col["default"] else ""
            doc.append(
                f"  - {col['name']}: {col['data_type']} (Nullable: {nullable}{default})"
            )

        doc.append("")

    # Relationships section
    doc.append("## Relationships")
    if schema_data["relationships"]["relationships"]:
        doc.append("### Foreign Key Relationships")
        doc.extend(
            [
                (
                    f"- {rel['source_table']}.{rel['source_column']} -> "
                    f"{rel['target_table']}.{rel['target_column']}"
                )
                for rel in schema_data["relationships"]["relationships"]
            ]
        )
    else:
        doc.append("No foreign key relationships found")

    if schema_data["relationships"]["orphan_tables"]:
        doc.append("### Orphan Tables (No Relationships)")
        doc.extend(
            [f"- {table}" for table in schema_data["relationships"]["orphan_tables"]]
        )

    if schema_data["relationships"]["circular_dependencies"]:
        doc.append("### Circular Dependencies")
        doc.extend(
            [
                f"- {' -> '.join(cycle)}"
                for cycle in schema_data["relationships"]["circular_dependencies"]
            ]
        )

    return "\n".join(doc)


def main():
    """Main analysis function."""
    print("Starting DuckDB Schema Analysis...")
    print(f"Reference Timestamp: {REFERENCE_TIMESTAMP}")

    conn = None
    try:
        # Connect to database
        conn = connect_to_duckdb()

        # Get all tables
        tables = get_all_tables(conn)
        if not tables:
            print("No tables found in database")
            return None

        # Retrieve schemas for all tables
        schemas = []
        for table in tables:
            print(f"Analyzing table: {table}")
            schema = get_table_schema(conn, table)
            if schema:
                # Analyze integrity
                integrity = analyze_schema_integrity(schema)
                schema["integrity"] = integrity
                schemas.append(schema)

        # Analyze relationships
        relationships = analyze_table_relationships(schemas)

        # Generate documentation
        schema_data = {
            "tables": schemas,
            "relationships": relationships,
            "generated_at": datetime.now().isoformat(),
            "reference_timestamp": REFERENCE_TIMESTAMP,
        }

        documentation = generate_schema_documentation(schema_data)

        # Save results
        output_dir = "src/scripts/analysis/output"
        os.makedirs(output_dir, exist_ok=True)

        # Save JSON report
        json_path = f"{output_dir}/schema_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(json_path, "w") as f:
            json.dump(schema_data, f, indent=2)

        # Save markdown report
        md_path = f"{output_dir}/schema_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(md_path, "w") as f:
            f.write(documentation)

        print("[OK] Analysis complete!")
        print(f"JSON report saved to: {json_path}")
        print(f"Markdown report saved to: {md_path}")

        # Print summary
        print("\n" + "=" * 50)
        print("ANALYSIS SUMMARY")
        print("=" * 50)
        print(f"Tables Analyzed: {len(schemas)}")
        print(f"Total Relationships: {relationships['total_relationships']}")
        print(f"Orphan Tables: {len(relationships['orphan_tables'])}")
        print(f"Circular Dependencies: {len(relationships['circular_dependencies'])}")

        avg_score = sum(t["integrity"]["integrity_score"] for t in schemas) / len(
            schemas
        )
        print(f"Average Integrity Score: {avg_score:.1f}/100")

        # Count issues and warnings
        total_issues = sum(len(t["integrity"]["issues"]) for t in schemas)
        total_warnings = sum(len(t["integrity"]["warnings"]) for t in schemas)
        print(f"Total Issues Found: {total_issues}")
        print(f"Total Warnings Found: {total_warnings}")

        return schema_data, documentation

    except Exception as e:
        print(f"[ERROR] Analysis failed: {e}")
        raise
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
