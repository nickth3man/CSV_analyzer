"""Chainlit-specific display utilities.

This module contains UI-specific display functions that depend on Chainlit.
It separates presentation logic from the data utilities in data_utils.py,
following the principle of separating UI concerns from business logic.

For UI-agnostic data functions, see the data_utils module.
"""

import logging

import chainlit as cl

from src.frontend.data_utils import get_schema_summary_data, get_table_preview_data

logger = logging.getLogger(__name__)


async def display_table_preview(table_name: str, max_rows: int = 10) -> bool:
    """
    Display a table preview using Chainlit's native DataFrame elements.

    This function uses the UI-agnostic get_table_preview_data() to fetch
    the data, then renders it using Chainlit's message and element APIs.

    Args:
        table_name: Name of the table to preview
        max_rows: Maximum number of rows to display

    Returns:
        True if table was found and displayed, False otherwise
    """
    logger.info(f"Displaying preview for table '{table_name}'")

    # Get data from UI-agnostic function
    data = get_table_preview_data(table_name, max_rows)

    if data is None:
        await cl.Message(content=f"❌ Table `{table_name}` not found.").send()
        return False

    # Create Chainlit DataFrame element for native display
    elements = [
        cl.Dataframe(
            data=data["preview_df"], name=f"{table_name}_preview", display="inline"
        )
    ]

    # Build and send the message
    await cl.Message(
        content=f"""## 📋 Preview: {data['table_name']}

**Stats:** {data['total_rows']:,} rows x {data['total_cols']} columns ({data['num_cols']} numeric, {data['str_cols']} text)

*Showing first {data['rows_shown']} rows:*
""",
        elements=elements,
    ).send()

    logger.info(
        f"Successfully displayed preview for table '{table_name}' with {data['rows_shown']} rows"
    )
    return True


async def display_schema_summary() -> bool:
    """
    Display an enhanced schema summary with DataFrames for each table.

    This function uses the UI-agnostic get_schema_summary_data() to fetch
    the data, then renders it using Chainlit's message and element APIs.

    Returns:
        True if schema was displayed, False if no tables loaded
    """
    logger.info("Displaying schema summary")

    # Get data from UI-agnostic function
    data = get_schema_summary_data()

    if data is None:
        await cl.Message(
            content="📂 No tables loaded. Upload CSV files to get started!"
        ).send()
        return False

    # Build overview message
    overview = (
        f"## 📊 Data Schema Overview\n\n**{data['table_count']} tables loaded:**\n\n"
    )

    for table in data["tables"]:
        overview += f"- **{table['name']}**: {table['rows']:,} rows x {table['cols']} cols ({table['num_cols']} numeric)\n"

    overview += "\n---\n\n**Column Details:**\n"

    await cl.Message(content=overview).send()

    # Show column info for each table using Chainlit DataFrame elements
    for table in data["tables"]:
        elements = [
            cl.Dataframe(
                data=table["col_info"], name=f"{table['name']}_schema", display="inline"
            )
        ]

        await cl.Message(content=f"### {table['name']}", elements=elements).send()

    logger.info(
        f"Successfully displayed schema summary for {data['table_count']} tables"
    )
    return True
