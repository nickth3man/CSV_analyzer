"""Step functions for analysis pipeline."""

import os
import asyncio
import chainlit as cl
from flow import create_analyst_flow
from .data_utils import load_dataframes, get_schema_info


@cl.step(type="tool", name="Loading Data")
async def step_load_data():
    """
    Load all available dataframes and report which tables were loaded.
    
    Returns:
        str: A summary string in the form "Loaded N tables: `name1`, `name2`, ...", where N is the number of loaded tables and each table name is wrapped in backticks.
    """
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
    Run the analysis pipeline for a user's question using provided settings.
    
    Parameters:
        question (str): The user's natural-language question to analyze.
        settings (dict): Configuration dict; recognized keys:
            - "api_key": API key to set for the analysis (optional).
            - "model": Model identifier to set for the analysis (optional).
    
    Returns:
        tuple: (shared, final_text, chart_path)
            - shared (dict | None): Execution state dict containing keys like "question", "retry_count", "exec_error", and any outputs produced by the flow; `None` if an internal error occurred before or during processing.
            - final_text (str): Final analysis text produced by the pipeline, or an error message if processing failed.
            - chart_path (str | None): Filesystem path to a generated chart image when available, otherwise `None`.
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


async def stream_response(content: str, elements: list = None):
    """
    Stream text content incrementally to the user to create a natural typing effect, optionally attaching UI elements.
    
    Parameters:
        content (str): The text to stream to the user.
        elements (list, optional): Optional list of UI elements (for example images) to attach to the message.
    
    Returns:
        cl.Message: The sent Chainlit message object.
    """
    msg = cl.Message(content="", elements=elements or [])
    await msg.send()

    # Stream the content word by word for natural feel
    words = content.split()
    buffer = ""

    for i, word in enumerate(words):
        buffer += word + " "

        # Stream in chunks of ~5 words for efficiency
        if i % 5 == 4 or i == len(words) - 1:
            await msg.stream_token(buffer)
            buffer = ""
            # Small delay for natural streaming effect
            await asyncio.sleep(0.02)

    await msg.update()
    return msg


async def display_result_with_streaming(final_text: str, chart_path: str = None):
    """
    Show analysis text to the user, streaming short responses for effect and attaching an optional chart image.
    
    Parameters:
        final_text (str): The analysis result text to display.
        chart_path (str | None): Optional filesystem path to an image to include inline with the message.
    """
    elements = []
    if chart_path:
        try:
            elements.append(cl.Image(path=chart_path, name="chart", display="inline"))
        except (FileNotFoundError, OSError) as e:
            print(f"Warning: Could not load chart image: {e}")

    # For shorter responses, stream for effect
    # For longer responses, just send directly to avoid delay
    if len(final_text) < 2000:
        await stream_response(final_text, elements)
    else:
        await cl.Message(content=final_text, elements=elements).send()