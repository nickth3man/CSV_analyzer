"""Command handling for the Chainlit app."""

import os
import shutil

import chainlit as cl

from backend.utils.file_sanitizer import sanitize_csv_filename
from .config import HELP_TEXT
from .data_utils import (
    get_csv_files,
    get_data_profile,
    get_schema_info,
    get_table_schema,
    invalidate_dataframe_cache,
    preview_table,
)
from .knowledge_utils import clear_knowledge_store, get_knowledge_store_data


async def handle_command(message_content: str) -> bool:
    """Handle slash commands from the user.

    Returns True if command was handled, False otherwise.
    """
    content = message_content.strip().lower()

    if content == "/upload":
        files = await cl.AskFileMessage(
            content="Please upload your CSV file(s)",
            accept=["text/csv", "application/vnd.ms-excel", ".csv"],
            max_size_mb=50,
            max_files=10,
        ).send()

        if files:
            csv_dir = "src/backend/data/raw/csv"
            os.makedirs(csv_dir, exist_ok=True)

            uploaded = []
            for file in files:
                # SECURITY: sanitize filenames to prevent path traversal
                filename = sanitize_csv_filename(file.name)
                if not filename:
                    continue  # Skip invalid filenames
                dest = os.path.join(csv_dir, filename)
                shutil.copy(file.path, dest)
                uploaded.append(filename.replace(".csv", ""))

            # Invalidate cache after upload
            invalidate_dataframe_cache()
            await cl.Message(content=f"✅ Uploaded: {', '.join(uploaded)}").send()
        return True

    if content == "/tables":
        tables = get_csv_files()
        if tables:
            msg = "## Loaded Tables\n\n" + "\n".join([f"- **{t}**" for t in tables])
        else:
            msg = "No tables loaded. Use `/upload` to add CSV files."
        await cl.Message(content=msg).send()
        return True

    if content.startswith("/preview "):
        table_name = message_content[9:].strip()
        preview = preview_table(table_name)
        await cl.Message(content=f"## Preview: {table_name}\n\n{preview}").send()
        return True

    if content.startswith("/delete "):
        table_name = message_content[8:].strip()
        filepath = os.path.join("src/backend/data/raw/csv", f"{table_name}.csv")
        if os.path.exists(filepath):
            os.remove(filepath)
            # Invalidate cache after delete
            invalidate_dataframe_cache()
            await cl.Message(content=f"✅ Deleted table: {table_name}").send()
        else:
            await cl.Message(content=f"❌ Table not found: {table_name}").send()
        return True

    if content.startswith("/schema "):
        table_name = message_content[8:].strip()
        schema = get_table_schema(table_name)
        await cl.Message(content=f"## Table Schema\n\n{schema}").send()
        return True

    if content == "/schema":
        schema = get_schema_info()
        await cl.Message(content=f"## Data Schema\n\n{schema}").send()
        return True

    if content == "/profile":
        profile = get_data_profile()
        await cl.Message(content=f"## Data Profile\n\n{profile}").send()
        return True

    if content == "/knowledge":
        knowledge = get_knowledge_store_data()
        await cl.Message(content=f"## Knowledge Store\n\n{knowledge}").send()
        return True

    if content == "/clear_knowledge":
        result = clear_knowledge_store()
        await cl.Message(content=f"✅ {result}").send()
        return True

    if content == "/help":
        await cl.Message(content=HELP_TEXT).send()
        return True

    return False
