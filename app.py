"""
Main entry point for the Chainlit Data Analyst Agent.

This file imports and registers all handlers from the frontend module.
Run with: chainlit run app.py
"""

import sys
from pathlib import Path

# NOTE: Assuming the application is run from the project root where 'src' is a package.
# If running via `python -m src...` or `chainlit run app.py` from root, imports should resolve.

from src.frontend import (
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
