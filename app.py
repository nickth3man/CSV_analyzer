"""
Main entry point for the Chainlit Data Analyst Agent.

This file imports and registers all handlers from the frontend module.
Run with: chainlit run app.py
"""

# Import all handlers and actions to register them with Chainlit
from frontend import (
    on_chat_start,
    on_settings_update,
    on_message,
    on_upload_action,
    on_list_tables,
    on_view_schema,
    on_view_profile,
    on_show_help,
)

if __name__ == "__main__":
    from chainlit.cli import run_chainlit
    run_chainlit(__file__)
