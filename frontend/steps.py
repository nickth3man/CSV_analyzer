"""Step functions for analysis pipeline."""

import os
import chainlit as cl
from flow import create_analyst_flow
from .data_utils import load_dataframes, get_schema_info


@cl.step(type="tool", name="Loading Data")
async def step_load_data():
    """Load data step."""
    dfs = load_dataframes()
    return f"Loaded {len(dfs)} tables"


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
    # TODO: Generate query suggestions from the loaded schema and attach them to the shared store
    # so the UI can surface suggested questions before/after answering.

    try:
        analyst_flow = create_analyst_flow()
        analyst_flow.run(shared)

        final_text = shared.get("final_text", "Analysis complete but no response was generated.")
        chart_path = shared.get("chart_path")

        return shared, final_text, chart_path

    except Exception as e:
        return None, f"An error occurred: {str(e)}", None
