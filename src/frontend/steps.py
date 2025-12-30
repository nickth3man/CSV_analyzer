"""Step functions for analysis pipeline."""

from __future__ import annotations

import asyncio
import logging
import os

import chainlit as cl

from src.backend.config import get_config
from src.backend.flow import create_analyst_flow
from src.backend.utils.logger import get_logger
from src.backend.utils.memory import get_memory
from .data_utils import get_schema_info, get_table_names


logger = logging.getLogger(__name__)


@cl.step(type="tool", name="Loading Tables")
async def step_load_data() -> str:
    """Load table metadata from DuckDB."""
    tables = get_table_names()
    if not tables:
        return "No tables found in the DuckDB database."
    table_names = ", ".join(f"`{name}`" for name in tables)
    return f"Loaded {len(tables)} tables: {table_names}"


@cl.step(type="tool", name="Analyzing Schema")
async def step_schema() -> str:
    """Analyze schema step."""
    return get_schema_info()


@cl.step(type="tool", name="Running Analysis")
async def step_run_analysis(question: str, settings: dict) -> dict | None:
    """Run the analysis pipeline."""
    api_key = settings.get("api_key", "")
    model = settings.get("model", "")

    if api_key:
        os.environ["OPENROUTER_API_KEY"] = api_key
    if model:
        os.environ["OPENROUTER_MODEL"] = model

    config = get_config()
    memory = get_memory()

    shared = {
        "question": question,
        "conversation_history": memory.get_context(n_turns=5).turns,
        "total_retries": 0,
        "grader_retries": 0,
        "max_retries": config.resilience.max_retries,
    }

    trace_logger = get_logger()
    trace_id = trace_logger.start_trace(question=question)

    try:
        analyst_flow = create_analyst_flow()
        analyst_flow.run(shared)
    finally:
        shared["execution_trace"] = trace_logger.end_trace(trace_id)

    return shared


async def stream_response(content: str, elements: list | None = None):
    """Stream a response to the user for a more interactive feel."""
    msg = cl.Message(content="", elements=elements or [])
    await msg.send()

    chunk_size = 50
    position = 0

    while position < len(content):
        end_pos = min(position + chunk_size, len(content))
        chunk = content[position:end_pos]
        await msg.stream_token(chunk)
        position = end_pos
        await asyncio.sleep(0.02)

    await msg.update()
    return msg


async def display_result_with_streaming(
    final_text: str,
    elements: list | None = None,
) -> None:
    """Display the analysis result with optional streaming."""
    if len(final_text) < 2000:
        await stream_response(final_text, elements)
    else:
        await cl.Message(content=final_text, elements=elements or []).send()
