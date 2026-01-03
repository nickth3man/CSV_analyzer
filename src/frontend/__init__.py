"""Chainlit frontend for the NBA Data Analyst Agent.

This module provides the web interface for interacting with the data analyst agent.
It is organized into the following components:

- config: Configuration, constants, and model fetching
- data_utils: DuckDB schema and preview utilities (UI-agnostic)
- display: Chainlit-specific display utilities (UI layer)
- commands: Slash command handling
- actions: Action callbacks for buttons
- steps: Analysis pipeline steps with streaming support
- handlers: Main event handlers, starters, and chat profiles
"""

# Register authentication callbacks (JWT/password) if configured.
from src.frontend import auth  # noqa: F401

# Import handlers, starters, profiles and actions to register them with Chainlit
from src.frontend.actions import on_list_tables, on_show_help, on_view_schema

# Also export UI-agnostic data functions for external use
from src.frontend.data_utils import get_schema_summary_data, get_table_preview_data

# Display utilities are now in display.py (separated from data_utils.py)
from src.frontend.display import display_schema_summary, display_table_preview
from src.frontend.handlers import (
    chat_profile,
    on_chat_start,
    on_message,
    on_settings_update,
    set_starters,
)
from src.frontend.steps import (
    display_result_with_streaming,
    step_load_data,
    step_run_analysis,
    step_schema,
    stream_response,
)


__all__ = [
    "chat_profile",
    "display_result_with_streaming",
    "display_schema_summary",
    # Chainlit display utilities (UI layer)
    "display_table_preview",
    "get_schema_summary_data",
    # UI-agnostic data functions
    "get_table_preview_data",
    # Event handlers
    "on_chat_start",
    "on_list_tables",
    "on_message",
    "on_settings_update",
    "on_show_help",
    # Action callbacks
    "on_view_schema",
    # Starters and profiles
    "set_starters",
    # Step functions
    "step_load_data",
    "step_run_analysis",
    "step_schema",
    "stream_response",
]
