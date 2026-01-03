"""Command handling for the Chainlit app."""

import chainlit as cl

from src.frontend.config import HELP_TEXT
from src.frontend.data_utils import (
    get_schema_info,
    get_table_names,
    get_table_schema,
    preview_table,
)
from src.frontend.display import display_table_preview


async def handle_command(message_content: str) -> bool:
    """Handle slash commands from the user."""
    content = message_content.strip()
    lower = content.lower()
    handled = True

    if lower == "/tables":
        tables = get_table_names()
        if tables:
            msg = "## Available Tables\n\n" + "\n".join(f"- **{t}**" for t in tables)
        else:
            msg = "No tables found in the DuckDB database."
        await cl.Message(content=msg).send()
    elif lower.startswith("/preview "):
        table_name = content[9:].strip()
        await display_table_preview(table_name)
    elif lower.startswith("/schema "):
        table_name = content[8:].strip()
        schema = get_table_schema(table_name)
        await cl.Message(content=f"## Table Schema\n\n{schema}").send()
    elif lower == "/schema":
        schema = get_schema_info()
        await cl.Message(content=f"## Data Schema\n\n{schema}").send()
    elif lower == "/help":
        await cl.Message(content=HELP_TEXT).send()
    elif lower.startswith("/preview-md "):
        table_name = content[12:].strip()
        preview = preview_table(table_name)
        await cl.Message(content=f"## Preview: {table_name}\n\n{preview}").send()
    else:
        handled = False

    return handled
