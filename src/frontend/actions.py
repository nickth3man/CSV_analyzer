"""Action callbacks for Chainlit buttons."""

import os
import shutil

import chainlit as cl

from backend.utils.file_sanitizer import sanitize_csv_filename
from src.frontend.config import HELP_TEXT
from src.frontend.data_utils import (
    get_csv_files,
    get_data_profile,
    get_schema_info,
    invalidate_dataframe_cache,
)


@cl.action_callback("upload_csv")
async def on_upload_action(action: cl.Action) -> str:
    """Handle the upload CSV action button."""
    files = await cl.AskFileMessage(
        content="Please upload your CSV file(s)",
        accept=["text/csv", "application/vnd.ms-excel", ".csv"],
        max_size_mb=50,
        max_files=10
    ).send()

    if files:
        csv_dir = "CSV"
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
        await cl.Message(content=f"✅ Uploaded: {', '.join(uploaded)}\n\nYou can now ask questions about your data!").send()

    return "Upload complete"


@cl.action_callback("list_tables")
async def on_list_tables(action: cl.Action) -> str:
    """Handle the list tables action button."""
    tables = get_csv_files()
    if tables:
        content = "## Loaded Tables\n\n"
        for t in tables:
            content += f"- **{t}**\n"
        content += "\nUse `/preview <table_name>` to preview a table."
    else:
        content = "No tables loaded. Use the 📁 button to upload CSV files."

    await cl.Message(content=content).send()
    return "Listed tables"


@cl.action_callback("view_schema")
async def on_view_schema(action: cl.Action) -> str:
    """Handle the view schema action button."""
    schema = get_schema_info()
    profile = get_data_profile()

    content = f"## Data Schema\n\n{schema}\n\n## Data Profile\n\n{profile}"
    await cl.Message(content=content).send()
    return "Showed schema"


@cl.action_callback("view_profile")
async def on_view_profile(action: cl.Action) -> str:
    """Handle the view profile action button."""
    profile = get_data_profile()
    await cl.Message(content=f"## Data Profile\n\n{profile}").send()
    return "Showed data profile"


@cl.action_callback("show_help")
async def on_show_help(action: cl.Action) -> str:
    """Handle the show help action button."""
    await cl.Message(content=HELP_TEXT).send()
    return "Showed help"
