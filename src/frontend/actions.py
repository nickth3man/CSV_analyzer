"""Action callbacks for Chainlit buttons."""

import chainlit as cl

from src.frontend.config import HELP_TEXT
from src.frontend.data_utils import get_schema_info, get_table_names


@cl.action_callback("list_tables")
async def on_list_tables(action: cl.Action) -> str:
    """Handle the list tables action button."""
    tables = get_table_names()
    if tables:
        content = "## Available Tables\n\n" + "\n".join(f"- **{t}**" for t in tables)
    else:
        content = "No tables found in the DuckDB database."

    await cl.Message(content=content).send()
    return "Listed tables"


@cl.action_callback("view_schema")
async def on_view_schema(action: cl.Action) -> str:
    """Handle the view schema action button."""
    schema = get_schema_info()
    await cl.Message(content=f"## Data Schema\n\n{schema}").send()
    return "Showed schema"


@cl.action_callback("show_help")
async def on_show_help(action: cl.Action) -> str:
    """Handle the show help action button."""
    await cl.Message(content=HELP_TEXT).send()
    return "Showed help"
