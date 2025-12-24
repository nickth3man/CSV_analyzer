"""Main event handlers for Chainlit app."""

import logging
import os
import shutil

import chainlit as cl


logger = logging.getLogger(__name__)
from chainlit.input_widget import Select, TextInput

from backend.utils.file_sanitizer import sanitize_csv_filename
from src.frontend.commands import handle_command
from src.frontend.config import DEFAULT_API_KEY, fetch_openrouter_models
from src.frontend.data_utils import (
    get_csv_files,
    invalidate_dataframe_cache,
    load_dataframes,
)
from src.frontend.steps import (
    display_result_with_streaming,
    step_load_data,
    step_run_analysis,
    step_schema,
)


@cl.set_starters
async def set_starters():
    """Define starter suggestions for users."""
    return [
        cl.Starter(
            label="Compare Player Careers",
            message="Compare the careers of LeBron James and Tracy McGrady",
            icon="/public/icons/compare.svg",
        ),
        cl.Starter(
            label="Team Statistics",
            message="Which team has the most draft picks?",
            icon="/public/icons/stats.svg",
        ),
        cl.Starter(
            label="Top Players Ranking",
            message="Show me the top 10 players by games played",
            icon="/public/icons/ranking.svg",
        ),
        cl.Starter(
            label="Search Draft Class",
            message="Find all players drafted in 2003",
            icon="/public/icons/search.svg",
        ),
    ]


@cl.set_chat_profiles
async def chat_profile():
    """Define chat profiles for different analysis modes."""
    return [
        cl.ChatProfile(
            name="Quick Analysis",
            markdown_description="**Fast answers** with basic statistics and summaries. Best for simple queries.",
            icon="/public/icons/quick.svg",
            starters=[
                cl.Starter(
                    label="Top Scorers",
                    message="Show me the top 10 players by points scored",
                    icon="/public/icons/ranking.svg",
                ),
                cl.Starter(
                    label="Team Overview",
                    message="Give me a quick overview of the Chicago Bulls",
                    icon="/public/icons/stats.svg",
                ),
            ],
        ),
        cl.ChatProfile(
            name="Deep Analysis",
            markdown_description="**Detailed comparisons** with charts and comprehensive insights. Best for complex queries.",
            icon="/public/icons/deep.svg",
            starters=[
                cl.Starter(
                    label="Career Comparison",
                    message="Compare the full careers of LeBron James and Michael Jordan including stats, achievements, and playing style",
                    icon="/public/icons/compare.svg",
                ),
                cl.Starter(
                    label="Draft Analysis",
                    message="Analyze the 2003 NBA draft class performance over their careers with charts",
                    icon="/public/icons/search.svg",
                ),
            ],
        ),
    ]


@cl.on_chat_start
async def on_chat_start() -> None:
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

    # Build data status section
    if tables:
        table_list = ", ".join(f"`{t}`" for t in tables)
        table_info = f"**{len(tables)} tables loaded:** {table_list}"
        data_status = f"📊 {table_info}"
    else:
        data_status = "📂 **No data loaded yet** — Upload CSV files to get started!"

    # Organized action buttons
    actions = [
        # Data Management
        cl.Action(name="upload_csv", payload={"action": "upload"},
                  label="📁 Upload", description="Upload CSV files"),
        cl.Action(name="list_tables", payload={"action": "tables"},
                  label="📋 Tables", description="View loaded tables"),
        # Analysis Tools
        cl.Action(name="view_schema", payload={"action": "schema"},
                  label="📊 Schema", description="View data structure"),
        cl.Action(name="view_profile", payload={"action": "profile"},
                  label="📈 Profile", description="View data statistics"),
        # Help
        cl.Action(name="show_help", payload={"action": "help"},
                  label="❓ Help", description="Show usage guide"),
    ]

    welcome_msg = f"""# 🏀 NBA Data Analyst

Ask me anything about NBA data! I can analyze player stats, compare careers, find draft picks, and visualize trends.

---

{data_status}

---

**Quick Actions:** Click the buttons below or use commands like `/upload`, `/tables`, `/schema`

**Tip:** Select a starter suggestion above to get started quickly!
"""

    await cl.Message(
        content=welcome_msg,
        actions=actions
    ).send()


@cl.on_settings_update
async def on_settings_update(settings) -> None:
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
async def on_message(message: cl.Message) -> None:
    """Handle incoming messages."""
    # Handle file uploads via message elements
    if message.elements:
        csv_dir = "CSV"
        os.makedirs(csv_dir, exist_ok=True)

        uploaded = []
        for element in message.elements:
            if hasattr(element, "path") and element.path:
                # SECURITY: sanitize filenames to prevent path traversal
                raw_name = element.name if hasattr(element, "name") else element.path
                filename = sanitize_csv_filename(raw_name)
                if not filename:
                    continue  # Skip invalid filenames
                dest = os.path.join(csv_dir, filename)
                shutil.copy(element.path, dest)
                uploaded.append(filename.replace(".csv", ""))

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

    # Enhanced API key validation with helpful actions
    if not api_key or len(api_key.strip()) == 0:
        await cl.Message(
            content="""## ⚠️ API Key Required

Please set your OpenRouter API key to continue.

**How to set up:**
1. Click the **gear icon** (⚙️) in the top right
2. Enter your API key in the settings panel
3. Get a free API key at [openrouter.ai](https://openrouter.ai)

> 💡 **Tip:** A default API key is provided for testing, but it has limited quota.
"""
        ).send()
        return

    # Basic API key format validation
    if not api_key.startswith("sk-or-"):
        await cl.Message(
            content="""## ⚠️ Invalid API Key Format

OpenRouter API keys should start with `sk-or-`.

**Please check:**
- You're using an OpenRouter key, not an OpenAI key
- The key was copied correctly without extra spaces
- Get a valid key at [openrouter.ai](https://openrouter.ai)
"""
        ).send()
        return

    dfs = load_dataframes()
    if not dfs:
        # Enhanced "no data" error with upload action
        upload_action = cl.Action(
            name="upload_csv",
            payload={"action": "upload"},
            label="📁 Upload CSV Now",
            description="Upload your data files"
        )
        await cl.Message(
            content="""## 📂 No Data Loaded

I need some data to analyze! Please upload your CSV files first.

**Options:**
1. Click the **Upload CSV Now** button below
2. Type `/upload` to open the file picker
3. Drag and drop CSV files directly into the chat

**Supported formats:** `.csv` files up to 50MB each
""",
            actions=[upload_action]
        ).send()
        return

    progress_msg = await cl.Message(content="⏳ Starting analysis...").send()

    try:
        await progress_msg.update(content="📥 Loading data...")
        await step_load_data()

        await progress_msg.update(content="🧭 Analyzing schema...")
        await step_schema()

        await progress_msg.update(content="🧠 Running analysis pipeline...")
        shared, final_text, chart_path = await step_run_analysis(question, settings)

        if shared:
            await progress_msg.update(content="✅ Analysis complete!")
        else:
            await progress_msg.update(content="⚠️ Analysis encountered an issue.")

        # Use streaming display for the result
        await display_result_with_streaming(final_text, chart_path)

    except Exception as e:
        logger.exception("Analysis failed with unexpected error")
        await progress_msg.update(content="❌ Analysis failed.")
        await cl.Message(
            content=f"""## ❌ Analysis Error

An unexpected error occurred during analysis:

```
{e!s}
```

**Try:**
- Simplifying your question
- Checking if the required data columns exist
- Uploading additional data if needed
"""
        ).send()
