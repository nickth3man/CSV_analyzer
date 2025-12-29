"""Chainlit frontend for the NBA Data Analyst Agent.

This module provides the web interface for interacting with the data analyst agent.
It is organized into the following components:

- config: Configuration, constants, and model fetching
- cache: Dataframe caching with smart invalidation
- data_utils: Data loading, schema, and profiling utilities (UI-agnostic)
- display: Chainlit-specific display utilities (UI layer)
- knowledge_utils: Knowledge store utilities
- commands: Slash command handling
- actions: Action callbacks for buttons
- steps: Analysis pipeline steps with streaming support
- handlers: Main event handlers, starters, and chat profiles
"""

# Import handlers, starters, profiles and actions to register them with Chainlit
from frontend.actions import (
    on_list_tables,
    on_show_help,
    on_upload_action,
    on_view_profile,
    on_view_schema,
)

# Also export UI-agnostic data functions for external use
from frontend.data_utils import get_schema_summary_data, get_table_preview_data

# Display utilities are now in display.py (separated from data_utils.py)
from frontend.display import display_schema_summary, display_table_preview
from frontend.handlers import (
    chat_profile,
    on_chat_start,
    on_message,
    on_settings_update,
    set_starters,
)
from frontend.steps import (
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
    "on_upload_action",
    "on_view_profile",
    "on_view_schema",
    # Starters and profiles
    "set_starters",
    # Step functions
    "step_load_data",
    "step_run_analysis",
    "step_schema",
    "stream_response",
]
