"""Utilities for data loading, schema, and profiling."""

import os
import pandas as pd
import chainlit as cl
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
    """
    Create a markdown-formatted preview of the specified table.
    
    Parameters:
        table_name (str): Name of the table (key in the loaded dataframe cache) to preview.
    
    Returns:
        A Markdown string representing the first 20 rows of the table as a table, or the literal "Table not found" if the table does not exist.
    """
    dfs = load_dataframes()
    if table_name in dfs:
        df = dfs[table_name]
        return df.head(20).to_markdown(index=False)
    return "Table not found"


async def display_table_preview(table_name: str, max_rows: int = 10):
    """
    Show an inline preview of a table using Chainlit elements.
    
    Returns:
        `true` if the table was found and displayed, `false` otherwise.
    """
    dfs = load_dataframes()

    if table_name not in dfs:
        await cl.Message(content=f"❌ Table `{table_name}` not found.").send()
        return False

    df = dfs[table_name]
    preview_df = df.head(max_rows)

    # Create DataFrame element for native display
    elements = [
        cl.Dataframe(data=preview_df, name=f"{table_name}_preview", display="inline")
    ]

    # Build summary stats
    num_cols = len([c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])])
    str_cols = len([c for c in df.columns if df[c].dtype == 'object'])

    await cl.Message(
        content=f"""## 📋 Preview: {table_name}

**Stats:** {len(df):,} rows × {len(df.columns)} columns ({num_cols} numeric, {str_cols} text)

*Showing first {min(max_rows, len(df))} rows:*
""",
        elements=elements
    ).send()
    return True


async def display_schema_summary():
    """
    Display an interactive schema overview for all loaded DataFrames using Chainlit messages and Dataframe elements.
    
    When no tables are loaded, sends an informational message and returns. Otherwise sends a summary message listing each table with row and column counts and the number of numeric columns, then sends one message per table containing a DataFrame with per-column details (name, dtype, non-null count, and a short sample).
    """
    dfs = load_dataframes()

    if not dfs:
        await cl.Message(content="📂 No tables loaded. Upload CSV files to get started!").send()
        return

    # Build overview message
    overview = f"## 📊 Data Schema Overview\n\n**{len(dfs)} tables loaded:**\n\n"

    for name, df in dfs.items():
        num_cols = len([c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])])
        overview += f"- **{name}**: {len(df):,} rows × {len(df.columns)} cols ({num_cols} numeric)\n"

    overview += "\n---\n\n**Column Details:**\n"

    await cl.Message(content=overview).send()

    # Show column info for each table
    for name, df in dfs.items():
        # Create a summary DataFrame of column info
        col_info = pd.DataFrame({
            'Column': df.columns,
            'Type': [str(df[c].dtype) for c in df.columns],
            'Non-Null': [df[c].notna().sum() for c in df.columns],
            'Sample': [str(df[c].iloc[0])[:30] if len(df) > 0 else '' for c in df.columns]
        })

        elements = [
            cl.Dataframe(data=col_info, name=f"{name}_schema", display="inline")
        ]

        await cl.Message(
            content=f"### {name}",
            elements=elements
        ).send()