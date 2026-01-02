"""Utilities for DuckDB data access and schema presentation."""

from __future__ import annotations

import logging

import pandas as pd

from src.backend.utils.duckdb_client import get_duckdb_client


logger = logging.getLogger(__name__)


def get_table_names() -> list[str]:
    """Get list of table names from DuckDB."""
    client = get_duckdb_client()
    tables = client.get_all_tables()
    return [table.name for table in tables]


def get_schema_info() -> str:
    """Get schema information for all tables."""
    client = get_duckdb_client()
    tables = client.get_all_tables()

    if not tables:
        return "No tables found in the DuckDB database."

    schema_lines = []
    for table in tables:
        cols = ", ".join(table.columns[:10])
        if len(table.columns) > 10:
            cols += f"... (+{len(table.columns) - 10} more)"
        rows = f"{table.row_count:,}" if table.row_count is not None else "unknown"
        schema_lines.append(
            f"**{table.name}** ({rows} rows, {len(table.columns)} columns)\n"
            f"  Columns: {cols}"
        )

    return "\n\n".join(schema_lines)


def get_table_schema(table_name: str) -> str:
    """Get DDL schema for a specific table."""
    client = get_duckdb_client()
    ddl = client.get_table_schema([table_name])
    if not ddl:
        return f"Table '{table_name}' not found."
    return f"```sql\n{ddl}\n```"


def get_data_profile() -> str:
    """Get a lightweight data profile for all tables."""
    client = get_duckdb_client()
    tables = client.get_all_tables()

    if not tables:
        return "No tables found in the DuckDB database."

    lines = []
    for table in tables:
        rows = f"{table.row_count:,}" if table.row_count is not None else "unknown"
        lines.append(f"- **{table.name}**: {rows} rows, {len(table.columns)} columns")

    return "\n".join(lines)


def preview_table(table_name: str) -> str:
    """Get a preview of a table as markdown."""
    client = get_duckdb_client()
    try:
        df = client.get_sample_data(table_name, limit=20)
    except Exception as exc:
        logger.warning("Failed to preview table %s: %s", table_name, exc)
        return f"Unable to preview table '{table_name}'."

    if df.empty:
        return f"Table '{table_name}' has no rows."
    return str(df.to_markdown(index=False))


def get_table_preview_data(table_name: str, max_rows: int = 10) -> dict | None:
    """Get table preview data for display."""
    client = get_duckdb_client()
    tables = {table.name: table for table in client.get_all_tables()}

    if table_name not in tables:
        return None

    try:
        preview_df = client.get_sample_data(table_name, limit=max_rows)
    except Exception as exc:
        logger.warning("Failed to load preview for %s: %s", table_name, exc)
        return None

    table_meta = tables[table_name]
    num_cols = len(
        [c for c in preview_df.columns if pd.api.types.is_numeric_dtype(preview_df[c])]
    )
    str_cols = len(
        [c for c in preview_df.columns if pd.api.types.is_string_dtype(preview_df[c])]
    )

    return {
        "table_name": table_name,
        "preview_df": preview_df,
        "total_rows": table_meta.row_count or len(preview_df),
        "total_cols": len(table_meta.columns),
        "num_cols": num_cols,
        "str_cols": str_cols,
        "rows_shown": len(preview_df),
    }


def get_schema_summary_data() -> dict | None:
    """Get schema summary data for all tables."""
    client = get_duckdb_client()
    tables = client.get_all_tables()

    if not tables:
        return None

    summary = []
    for table in tables:
        col_info = _get_column_info(table.name)
        summary.append(
            {
                "name": table.name,
                "rows": table.row_count or 0,
                "cols": len(table.columns),
                "num_cols": len(
                    [col for col in table.columns if col.lower().endswith("_id")]
                ),
                "col_info": col_info,
            }
        )

    return {"table_count": len(summary), "tables": summary}


def _get_column_info(table_name: str) -> pd.DataFrame:
    """Fetch column metadata for a table."""
    client = get_duckdb_client()
    df = client.execute_query(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = ?
        ORDER BY ordinal_position
        """,
        params=[table_name],
    )
    return df.rename(
        columns={
            "column_name": "Column",
            "data_type": "Type",
            "is_nullable": "Nullable",
        }
    )
