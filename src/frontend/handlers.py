"""Main event handlers for Chainlit app."""

from __future__ import annotations

import logging
import os

import chainlit as cl
from chainlit.input_widget import Select, TextInput

from .commands import handle_command
from .config import EXAMPLE_QUESTIONS, fetch_openrouter_models
from .data_utils import get_table_names
from .steps import display_result_with_streaming, step_load_data, step_run_analysis, step_schema


logger = logging.getLogger(__name__)


@cl.set_starters  # type: ignore[arg-type]
async def set_starters():
    """Define starter suggestions for users."""
    return [
        cl.Starter(
            label="Points Leader 2023",
            message=EXAMPLE_QUESTIONS[0],
            icon="/icons/ranking.svg",
        ),
        cl.Starter(
            label="Team Comparison",
            message=EXAMPLE_QUESTIONS[1],
            icon="/icons/compare.svg",
        ),
        cl.Starter(
            label="Top Players",
            message=EXAMPLE_QUESTIONS[2],
            icon="/icons/stats.svg",
        ),
        cl.Starter(
            label="Draft Class",
            message=EXAMPLE_QUESTIONS[4],
            icon="/icons/search.svg",
        ),
    ]


@cl.set_chat_profiles  # type: ignore[arg-type]
async def chat_profile():
    """Define chat profiles for different analysis modes."""
    return [
        cl.ChatProfile(
            name="NBA Analyst",
            markdown_description="**Text-to-SQL NBA analysis** powered by DuckDB.",
            icon="/icons/stats.svg",
            starters=[
                cl.Starter(
                    label="Points Leader 2023",
                    message=EXAMPLE_QUESTIONS[0],
                    icon="/icons/ranking.svg",
                )
            ],
        )
    ]


@cl.on_chat_start
async def on_chat_start() -> None:
    """Initialize the chat session."""
    current_api_key = os.environ.get("OPENROUTER_API_KEY", "")
    models = fetch_openrouter_models(
        current_api_key,
        filter_models=not bool(current_api_key),
    )

    settings = await cl.ChatSettings(
        [
            TextInput(
                id="api_key",
                label="OpenRouter API Key",
                initial=current_api_key,
                placeholder="sk-or-v1-...",
            ),
            Select(id="model", label="LLM Model", values=models, initial_index=0),
        ],
    ).send()

    cl.user_session.set("settings", settings)
    cl.user_session.set("chat_history", [])

    tables = get_table_names()
    if tables:
        table_list = ", ".join(f"`{t}`" for t in tables[:8])
        table_info = f"**{len(tables)} tables available:** {table_list}"
    else:
        table_info = "**No tables found** in `src/backend/data/nba.duckdb`."

    actions = [
        cl.Action(name="list_tables", payload={"action": "tables"}, label="List Tables"),
        cl.Action(name="view_schema", payload={"action": "schema"}, label="View Schema"),
        cl.Action(name="show_help", payload={"action": "help"}, label="Help"),
    ]

    welcome_msg = f"""# NBA Data Analyst

Ask questions about NBA data stored in DuckDB. I will generate SQL, run it, and explain the results.

---

{table_info}

---

**Quick Actions:** Use the buttons below or type `/tables`, `/schema`, or `/help`.
"""

    await cl.Message(content=welcome_msg, actions=actions).send()


@cl.on_settings_update
async def on_settings_update(settings) -> None:
    """Handle settings updates."""
    cl.user_session.set("settings", settings)

    api_key = settings.get("api_key", "") or ""
    if api_key:
        os.environ["OPENROUTER_API_KEY"] = api_key

    models = fetch_openrouter_models(api_key, filter_models=not bool(api_key))

    model = settings.get("model", "")
    initial_index = 0

    if model and model in models:
        os.environ["OPENROUTER_MODEL"] = model
        initial_index = models.index(model)
    elif models:
        os.environ["OPENROUTER_MODEL"] = models[0]
        model = models[0]

    await cl.ChatSettings(
        [
            TextInput(
                id="api_key",
                label="OpenRouter API Key",
                initial=api_key,
                placeholder="sk-or-v1-...",
            ),
            Select(
                id="model",
                label="LLM Model",
                values=models,
                initial_index=initial_index,
            ),
        ],
    ).send()

    notice = "Settings updated!" if api_key else "Settings updated! Add an API key to enable full model access."
    await cl.Message(content=notice).send()


@cl.on_message
async def on_message(message: cl.Message) -> None:
    """Handle incoming messages."""
    if await handle_command(message.content):
        return

    question = message.content.strip()
    if not question:
        return

    settings = cl.user_session.get("settings", {})
    api_key = settings.get("api_key", os.environ.get("OPENROUTER_API_KEY", ""))

    if not api_key or not api_key.startswith("sk-or-"):
        await cl.Message(
            content="Please set a valid OpenRouter API key in settings (top right).",
        ).send()
        return

    tables = get_table_names()
    if not tables:
        await cl.Message(
            content="No tables found in the DuckDB database. Populate `src/backend/data/nba.duckdb` first.",
        ).send()
        return

    progress_msg = await cl.Message(content="Starting analysis...").send()

    try:
        await progress_msg.update(content="Loading tables...")
        await step_load_data()

        await progress_msg.update(content="Analyzing schema...")
        await step_schema()

        await progress_msg.update(content="Running analysis...")
        shared = await step_run_analysis(question, settings)

        if not shared:
            await progress_msg.update(content="Analysis failed.")
            await cl.Message(content="No response was generated.").send()
            return

        await progress_msg.update(content="Analysis complete.")

        answer = shared.get("final_answer") or shared.get("final_text") or ""
        transparency_note = shared.get("transparency_note")
        sql_query = shared.get("sql_query")
        sub_query_sqls = shared.get("sub_query_sqls") or {}

        sql_block = ""
        if sub_query_sqls:
            sql_block = "\n".join(
                [f"-- {sub_id}\n{sql.strip()}" for sub_id, sql in sub_query_sqls.items()]
            )
        elif sql_query:
            sql_block = sql_query.strip()

        response_parts = [answer]
        if transparency_note:
            response_parts.append(f"How I found this:\n{transparency_note}")
        if sql_block:
            response_parts.append(f"SQL used:\n```sql\n{sql_block}\n```")

        final_text = "\n\n".join(part for part in response_parts if part)
        await display_result_with_streaming(final_text)

    except Exception as exc:
        logger.exception("Analysis failed with unexpected error")
        await progress_msg.update(content="Analysis failed.")
        await cl.Message(content=f"Unexpected error: {exc!s}").send()
