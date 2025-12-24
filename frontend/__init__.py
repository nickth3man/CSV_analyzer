"""
Chainlit frontend for the NBA Data Analyst Agent.

This module provides the web interface for interacting with the data analyst agent.
It is organized into the following components:

- config: Configuration, constants, and model fetching
- cache: Dataframe caching with smart invalidation
- data_utils: Data loading, schema, and profiling utilities
- knowledge_utils: Knowledge store utilities
- commands: Slash command handling
- actions: Action callbacks for buttons
- steps: Analysis pipeline steps with streaming support
- handlers: Main event handlers, starters, and chat profiles
"""

# Import handlers, starters, profiles and actions to register them with Chainlit
from .handlers import (
    on_chat_start,
    on_settings_update,
    on_message,
    set_starters,
    chat_profile
)
from .actions import (
    on_upload_action,
    on_list_tables,
    on_view_schema,
    on_view_profile,
    on_show_help
)
from .steps import (
    step_load_data,
    step_schema,
    step_run_analysis,
    stream_response,
    display_result_with_streaming
)
from .data_utils import (
    display_table_preview,
    display_schema_summary
)

__all__ = [
    # Event handlers
    'on_chat_start',
    'on_settings_update',
    'on_message',
    # Starters and profiles
    'set_starters',
    'chat_profile',
    # Action callbacks
    'on_upload_action',
    'on_list_tables',
    'on_view_schema',
    'on_view_profile',
    'on_show_help',
    # Step functions
    'step_load_data',
    'step_schema',
    'step_run_analysis',
    'stream_response',
    'display_result_with_streaming',
    # Data display utilities
    'display_table_preview',
    'display_schema_summary',
]
