"""Utilities for data loading, schema, and profiling."""

import os
import pandas as pd
from .cache import get_dataframe_cache


def get_csv_files():
    """Get list of CSV files in the CSV directory."""
    csv_dir = "CSV"
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    return [f.replace(".csv", "") for f in os.listdir(csv_dir) if f.endswith('.csv')]


def load_dataframes():
    """Load dataframes using cache to avoid redundant disk reads."""
    return get_dataframe_cache().get_dataframes()


def invalidate_dataframe_cache():
    """Invalidate the dataframe cache after file changes."""
    get_dataframe_cache().invalidate()


def get_schema_info():
    """Get schema information for all loaded dataframes."""
    dfs = load_dataframes()
    if not dfs:
        return "No CSV files loaded. Upload some data to get started!"

    schema_lines = []
    # TODO: Expand schema summaries to include Excel/JSON/DB metadata once additional formats are supported.
    for name, df in dfs.items():
        cols = ", ".join(df.columns[:10])
        if len(df.columns) > 10:
            cols += f"... (+{len(df.columns) - 10} more)"
        schema_lines.append(f"**{name}** ({len(df)} rows, {len(df.columns)} columns)\n  Columns: {cols}")
    return "\n\n".join(schema_lines)


def get_table_schema(table_name):
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
    """Get data profile summary for all loaded dataframes."""
    dfs = load_dataframes()
    if not dfs:
        return "No data loaded."

    profile_text = []
    for name, df in dfs.items():
        name_cols = [c for c in df.columns if any(x in c.lower() for x in ['name', 'first', 'last', 'player', 'team'])]
        id_cols = [c for c in df.columns if 'id' in c.lower()]
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

        profile_text.append(f"""### {name}
- Rows: {len(df):,}
- Columns: {len(df.columns)}
- Key columns: {', '.join(name_cols[:5]) if name_cols else 'None identified'}
- ID columns: {', '.join(id_cols[:5]) if id_cols else 'None identified'}
- Numeric columns: {len(numeric_cols)}
""")
    return "\n".join(profile_text)


def preview_table(table_name):
    """Get a preview of a table."""
    dfs = load_dataframes()
    if table_name in dfs:
        df = dfs[table_name]
        return df.head(20).to_markdown(index=False)
    return "Table not found"
