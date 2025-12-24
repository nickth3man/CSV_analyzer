"""Utilities for data loading, schema, and profiling.

This module provides UI-agnostic data utilities. All functions return
data structures (dicts, DataFrames, strings) rather than sending UI messages.
For Chainlit-specific display functions, see the display module.
"""

import logging
import os

import pandas as pd

from src.frontend.cache import get_dataframe_cache

logger = logging.getLogger(__name__)


def get_csv_files():
    """Get list of CSV files in the CSV directory."""
    csv_dir = "CSV"
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    return [f.replace(".csv", "") for f in os.listdir(csv_dir) if f.endswith(".csv")]


def load_dataframes():
    """Load DataFrames using cache to avoid redundant disk reads."""
    return get_dataframe_cache().get_dataframes()


def invalidate_dataframe_cache() -> None:
    """Invalidate the DataFrame cache after file changes."""
    get_dataframe_cache().invalidate()


def get_schema_info():
    """Get schema information for all loaded DataFrames."""
    dfs = load_dataframes()
    if not dfs:
        return "No CSV files loaded. Upload some data to get started!"

    schema_lines = []
    # TODO: Expand schema summaries to include Excel/JSON/DB metadata once additional formats are supported.
    for name, df in dfs.items():
        cols = ", ".join(df.columns[:10])
        if len(df.columns) > 10:
            cols += f"... (+{len(df.columns) - 10} more)"
        schema_lines.append(
            f"**{name}** ({len(df)} rows, {len(df.columns)} columns)\n  Columns: {cols}"
        )
    return "\n\n".join(schema_lines)


def get_table_schema(table_name) -> str:
    """Get schema information for a specific table."""
    dfs = load_dataframes()
    if not dfs:
        return "No CSV files loaded. Upload some data to get started!"
    if table_name not in dfs:
        return f"Table '{table_name}' not found."

    df = dfs[table_name]
    cols = ", ".join(df.columns)
    return f"**{table_name}** ({len(df)} rows, {len(df.columns)} columns)\n  Columns: {cols}"


def get_data_profile():
    """Get data profile summary for all loaded DataFrames."""
    dfs = load_dataframes()
    if not dfs:
        return "No data loaded."

    profile_text = []
    for name, df in dfs.items():
        name_cols = [
            c
            for c in df.columns
            if any(x in c.lower() for x in ["name", "first", "last", "player", "team"])
        ]
        id_cols = [c for c in df.columns if "id" in c.lower()]
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

        profile_text.append(
            f"""### {name}
- Rows: {len(df):,}
- Columns: {len(df.columns)}
- Key columns: {', '.join(name_cols[:5]) if name_cols else 'None identified'}
- ID columns: {', '.join(id_cols[:5]) if id_cols else 'None identified'}
- Numeric columns: {len(numeric_cols)}
"""
        )
    return "\n".join(profile_text)


def preview_table(table_name):
    """Get a preview of a table as markdown."""
    dfs = load_dataframes()
    if table_name in dfs:
        df = dfs[table_name]
        return df.head(20).to_markdown(index=False)
    return "Table not found"


def get_table_preview_data(table_name: str, max_rows: int = 10) -> dict | None:
    """
    Get table preview data for display.

    This is a UI-agnostic function that returns data structures
    suitable for rendering by any UI layer.

    Args:
        table_name: Name of the table to preview
        max_rows: Maximum number of rows to display

    Returns:
        Dictionary with preview data, or None if table not found:
        {
            'table_name': str,
            'preview_df': DataFrame,
            'total_rows': int,
            'total_cols': int,
            'num_cols': int,
            'str_cols': int,
            'rows_shown': int
        }
    """
    logger.info(
        f"Getting preview data for table '{table_name}' with max_rows={max_rows}"
    )
    dfs = load_dataframes()

    if table_name not in dfs:
        logger.warning(f"Table '{table_name}' not found for preview")
        return None

    df = dfs[table_name]
    preview_df = df.head(max_rows)

    # Build summary stats - use is_string_dtype for robust string detection
    num_cols = len([c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])])
    str_cols = len([c for c in df.columns if pd.api.types.is_string_dtype(df[c])])

    logger.debug(
        f"Table '{table_name}': {len(df)} rows, {len(df.columns)} cols ({num_cols} numeric, {str_cols} text)"
    )

    return {
        "table_name": table_name,
        "preview_df": preview_df,
        "total_rows": len(df),
        "total_cols": len(df.columns),
        "num_cols": num_cols,
        "str_cols": str_cols,
        "rows_shown": len(preview_df),
    }


def get_schema_summary_data() -> dict | None:
    """
    Get schema summary data for all loaded tables.

    This is a UI-agnostic function that returns data structures
    suitable for rendering by any UI layer.

    Returns:
        Dictionary with schema data, or None if no tables loaded:
        {
            'table_count': int,
            'tables': [
                {
                    'name': str,
                    'rows': int,
                    'cols': int,
                    'num_cols': int,
                    'col_info': DataFrame  # Column, Type, Non-Null, Sample
                },
                ...
            ]
        }
    """
    logger.info("Getting schema summary data")
    dfs = load_dataframes()

    if not dfs:
        logger.warning("No tables loaded for schema summary")
        return None

    tables = []
    for name, df in dfs.items():
        num_cols = len([c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])])

        # Create a summary DataFrame of column info
        col_info = pd.DataFrame(
            {
                "Column": df.columns,
                "Type": [str(df[c].dtype) for c in df.columns],
                "Non-Null": [df[c].notna().sum() for c in df.columns],
                "Sample": [
                    str(df[c].iloc[0])[:30] if len(df) > 0 else "" for c in df.columns
                ],
            }
        )

        tables.append(
            {
                "name": name,
                "rows": len(df),
                "cols": len(df.columns),
                "num_cols": num_cols,
                "col_info": col_info,
            }
        )

    logger.info(f"Retrieved schema summary data for {len(tables)} tables")

    return {"table_count": len(tables), "tables": tables}
