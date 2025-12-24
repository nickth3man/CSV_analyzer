"""Step functions for analysis pipeline."""

import os
import asyncio
import logging

import chainlit as cl

from backend.flow import create_analyst_flow
from .data_utils import get_schema_info, load_dataframes

logger = logging.getLogger(__name__)


@cl.step(type="tool", name="Loading Data")
async def step_load_data():
    """Load data step."""
    dfs = load_dataframes()
    table_names = ", ".join(f"`{name}`" for name in dfs.keys())
    return f"Loaded {len(dfs)} tables: {table_names}"


@cl.step(type="tool", name="Analyzing Schema")
async def step_schema():
    """Analyze schema step."""
    return get_schema_info()


@cl.step(type="tool", name="Running Analysis")
async def step_run_analysis(question: str, settings: dict):
    """
    Run the analysis pipeline.

    Args:
        question: The user's question
        settings: User settings (API key, model)

    Returns:
        Tuple of (shared dict, final text, chart path)
    """
    api_key = settings.get("api_key", "")
    model = settings.get("model", "")

    if api_key:
        os.environ["OPENROUTER_API_KEY"] = api_key
    if model:
        os.environ["OPENROUTER_MODEL"] = model

    dfs = load_dataframes()
    if not dfs:
        return None, "No CSV files loaded. Please upload some data first.", None

    shared = {
        "question": question,
        "retry_count": 0,
        "exec_error": None,
    }

    try:
        analyst_flow = create_analyst_flow()
        analyst_flow.run(shared)

        final_text = shared.get("final_text", "Analysis complete but no response was generated.")
        chart_path = shared.get("chart_path")

        return shared, final_text, chart_path

    except Exception as e:
        return None, f"An error occurred: {str(e)}", None


async def stream_response(content: str, elements: list | None = None):
    """
    Stream a response to the user for a more interactive feel.

    Args:
        content: The text content to stream
        elements: Optional list of elements (images, etc.) to attach

    Returns:
        The sent message object
    """
    msg = cl.Message(content="", elements=elements or [])
    await msg.send()

    # Stream the content in chunks to preserve formatting (whitespace, line breaks)
    # Use fixed-size chunks instead of splitting by words to preserve markdown
    chunk_size = 50  # characters per chunk
    position = 0

    while position < len(content):
        # Stream in chunks, preserving original formatting
        end_pos = min(position + chunk_size, len(content))
        chunk = content[position:end_pos]
        await msg.stream_token(chunk)
        position = end_pos
        # Small delay for natural streaming effect
        await asyncio.sleep(0.02)

    await msg.update()
    return msg


async def display_result_with_streaming(final_text: str, chart_path: str | None = None):
    """
    Display the analysis result with optional streaming and chart.

    Args:
        final_text: The analysis result text
        chart_path: Optional path to a chart image
    """
    elements = []
    if chart_path:
        try:
            elements.append(cl.Image(path=chart_path, name="chart", display="inline"))
        except (FileNotFoundError, OSError) as e:
            logger.warning(f"Could not load chart image: {e}")

    # For shorter responses, stream for effect
    # For longer responses, just send directly to avoid delay
    if len(final_text) < 2000:
        await stream_response(final_text, elements)
    else:
        await cl.Message(content=final_text, elements=elements).send()
