"""Chainlit display utilities for DuckDB schema and previews."""

from __future__ import annotations

import logging

import chainlit as cl

from src.frontend.data_utils import get_schema_summary_data, get_table_preview_data


logger = logging.getLogger(__name__)


async def display_table_preview(table_name: str, max_rows: int = 10) -> bool:
    """Display a table preview using Chainlit DataFrame elements."""
    data = get_table_preview_data(table_name, max_rows)
    if data is None:
        await cl.Message(content=f"Table `{table_name}` not found.").send()
        return False

    elements = [
        cl.Dataframe(
            data=data["preview_df"],
            name=f"{table_name}_preview",
            display="inline",
        )
    ]

    await cl.Message(
        content=(
            f"## Preview: {data['table_name']}\n\n"
            f"**Stats:** {data['total_rows']:,} rows x {data['total_cols']} columns "
            f"({data['num_cols']} numeric, {data['str_cols']} text)\n\n"
            f"*Showing first {data['rows_shown']} rows:*"
        ),
        elements=elements,
    ).send()

    return True


async def display_schema_summary() -> bool:
    """Display schema summary with column info tables."""
    data = get_schema_summary_data()
    if data is None:
        await cl.Message(
            content="No tables found in the DuckDB database.",
        ).send()
        return False

    overview = (
        f"## Data Schema Overview\n\n**{data['table_count']} tables available:**\n\n"
    )
    for table in data["tables"]:
        overview += (
            f"- **{table['name']}**: {table['rows']:,} rows x {table['cols']} cols\n"
        )

    overview += "\n---\n\n**Column Details:**\n"
    await cl.Message(content=overview).send()

    for table in data["tables"]:
        elements = [
            cl.Dataframe(
                data=table["col_info"],
                name=f"{table['name']}_schema",
                display="inline",
            )
        ]
        await cl.Message(content=f"### {table['name']}", elements=elements).send()

    return True
