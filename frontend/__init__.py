"""
Chainlit frontend for the Data Analyst Agent.

This module provides the web interface for interacting with the data analyst agent.
It is organized into the following components:

- config: Configuration and constants
- cache: Dataframe caching
- data_utils: Data loading and schema utilities
- knowledge_utils: Knowledge store utilities
- commands: Command handling
- actions: Action callbacks for buttons
- steps: Analysis pipeline steps
- handlers: Main event handlers
"""

# Import handlers and actions to register them with Chainlit
from .handlers import on_chat_start, on_settings_update, on_message
from .actions import (
    on_upload_action,
    on_list_tables,
    on_view_schema,
    on_view_profile,
    on_show_help
)

__all__ = [
    'on_chat_start',
    'on_settings_update',
    'on_message',
    'on_upload_action',
    'on_list_tables',
    'on_view_schema',
    'on_view_profile',
    'on_show_help',
]
