"""Main event handlers for Chainlit app."""

import os
import shutil
import chainlit as cl
from chainlit.input_widget import Select, TextInput
from utils.file_sanitizer import sanitize_csv_filename
from .config import (
    DEFAULT_API_KEY,
    EXAMPLE_QUESTIONS,
    fetch_openrouter_models
)
from .data_utils import (
    get_csv_files,
    load_dataframes,
    invalidate_dataframe_cache
)
from .commands import handle_command
from .steps import step_load_data, step_schema, step_run_analysis


@cl.on_chat_start
async def on_chat_start():
    """Initialize the chat session."""
    # Use default API key if none is set in environment
    current_api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not current_api_key:
        current_api_key = DEFAULT_API_KEY
        os.environ["OPENROUTER_API_KEY"] = DEFAULT_API_KEY

    # Determine if we should filter models (when using default key)
    using_default_key = (current_api_key == DEFAULT_API_KEY)
    models = fetch_openrouter_models(current_api_key, filter_models=using_default_key)

    settings = await cl.ChatSettings(
        [
            TextInput(
                id="api_key",
                label="OpenRouter API Key",
                initial=current_api_key,
                placeholder="sk-or-v1-..."
            ),
            Select(
                id="model",
                label="LLM Model",
                values=models,
                initial_index=0
            ),
        ]
    ).send()

    cl.user_session.set("settings", settings)
    cl.user_session.set("chat_history", [])

    tables = get_csv_files()
    table_info = f"**{len(tables)} tables loaded**: {', '.join(tables)}" if tables else "No data loaded yet"

    actions = [
        cl.Action(name="upload_csv", payload={"action": "upload"}, label="📁 Upload CSV", description="Upload CSV files"),
        cl.Action(name="list_tables", payload={"action": "tables"}, label="📋 Tables", description="List loaded tables"),
        cl.Action(name="view_schema", payload={"action": "schema"}, label="📊 Schema", description="View data schema"),
        cl.Action(name="view_profile", payload={"action": "profile"}, label="🧾 Profile", description="View data profile"),
        cl.Action(name="show_help", payload={"action": "help"}, label="❓ Help", description="Show help"),
    ]

    welcome_msg = f"""# Data Analyst Agent

Ask questions about your data in plain English. The agent will analyze your CSV files and provide insights.

{table_info}

**Quick Actions:** Use the buttons below or type commands like `/upload`, `/tables`, `/help`

**Example questions:**
"""
    for q in EXAMPLE_QUESTIONS[:3]:
        welcome_msg += f"\n- {q}"

    await cl.Message(
        content=welcome_msg,
        actions=actions
    ).send()


@cl.on_settings_update
async def on_settings_update(settings):
    """Handle settings updates."""
    cl.user_session.set("settings", settings)

    api_key = settings.get("api_key", "")

    # Use default API key if none provided
    if not api_key or api_key.strip() == "":
        api_key = DEFAULT_API_KEY

    os.environ["OPENROUTER_API_KEY"] = api_key

    # Determine if we should filter models (when using default key)
    using_default_key = (api_key == DEFAULT_API_KEY)

    # Fetch latest models with the (potentially new) API key
    # Filter models if using the default API key
    models = fetch_openrouter_models(api_key, filter_models=using_default_key)

    model = settings.get("model", "")
    initial_index = 0

    if model and model in models:
        os.environ["OPENROUTER_MODEL"] = model
        initial_index = models.index(model)
    elif models:
        # Fallback to first model if selected one is invalid or not in list
        os.environ["OPENROUTER_MODEL"] = models[0]
        model = models[0]

    # Re-render settings to update dropdown options
    await cl.ChatSettings(
        [
            TextInput(
                id="api_key",
                label="OpenRouter API Key",
                initial=api_key,
                placeholder="sk-or-v1-..."
            ),
            Select(
                id="model",
                label="LLM Model",
                values=models,
                initial_index=initial_index
            ),
        ]
    ).send()

    # Notify user about the model filtering
    if using_default_key:
        await cl.Message(content="⚙️ Settings updated! Using default API key - showing free models and MistralAI models only.").send()
    else:
        await cl.Message(content="⚙️ Settings updated! Using your API key - all models available.").send()


@cl.on_message
async def on_message(message: cl.Message):
    """Handle incoming messages."""
    # Handle file uploads via message elements
    if message.elements:
        csv_dir = "CSV"
        os.makedirs(csv_dir, exist_ok=True)

        uploaded = []
        for element in message.elements:
            if hasattr(element, 'path') and element.path:
                # SECURITY: sanitize filenames to prevent path traversal
                raw_name = element.name if hasattr(element, 'name') else element.path
                filename = sanitize_csv_filename(raw_name)
                if not filename:
                    continue  # Skip invalid filenames
                dest = os.path.join(csv_dir, filename)
                shutil.copy(element.path, dest)
                uploaded.append(filename.replace('.csv', ''))

        if uploaded:
            # Invalidate cache after upload
            invalidate_dataframe_cache()
            await cl.Message(content=f"✅ Uploaded: {', '.join(uploaded)}\n\nYou can now ask questions about your data!").send()
            return

    # Handle commands
    if await handle_command(message.content):
        return

    # Process question
    question = message.content.strip()
    if not question:
        return

    settings = cl.user_session.get("settings", {})
    api_key = settings.get("api_key", os.environ.get("OPENROUTER_API_KEY", ""))

    if not api_key or len(api_key.strip()) == 0:
        await cl.Message(content="⚠️ Please set your OpenRouter API key in Settings (gear icon) first.").send()
        return

    # Basic API key format validation
    if not api_key.startswith("sk-or-"):
        await cl.Message(content="⚠️ Invalid API key format. OpenRouter keys should start with 'sk-or-'.").send()
        return

    dfs = load_dataframes()
    if not dfs:
        await cl.Message(content="⚠️ No data loaded. Please upload CSV files first using the 📁 button or `/upload` command.").send()
        return

    progress_msg = await cl.Message(content="⏳ Starting analysis...").send()
    await progress_msg.update(content="📥 Loading data...")
    await step_load_data()
    await progress_msg.update(content="🧭 Analyzing schema...")
    await step_schema()
    # TODO: Display schema-driven query suggestions to the user (e.g., via a message or action buttons)
    # before running the analysis, once suggestions are generated.
    await progress_msg.update(content="🧠 Running analysis pipeline...")

    shared, final_text, chart_path = await step_run_analysis(question, settings)
    if shared:
        await progress_msg.update(content="✅ Analysis complete.")
    else:
        await progress_msg.update(content="⚠️ Analysis failed. See details below.")

    elements = []
    if chart_path:
        try:
            # Try to create the image element, avoiding TOCTOU race condition
            elements.append(cl.Image(path=chart_path, name="chart", display="inline"))
        except (FileNotFoundError, OSError) as e:
            print(f"Warning: Could not load chart image: {e}")

    await cl.Message(content=final_text, elements=elements).send()
