"""Main entry point for the Chainlit Data Analyst Agent.

This file imports and registers all handlers from the frontend module.
Run with: chainlit run src/frontend/app.py
"""

import sys
from pathlib import Path

# Add src directory to Python path so imports like "from backend.xxx" work
src_path = Path(__file__).resolve().parents[1]
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Import and register all Chainlit handlers
# These decorators register themselves with Chainlit when imported
from frontend import (  # noqa: E402, F401
    chat_profile,
    on_chat_start,
    on_list_tables,
    on_message,
    on_settings_update,
    on_show_help,
    on_upload_action,
    on_view_profile,
    on_view_schema,
    set_starters,
)


if __name__ == "__main__":
    from chainlit.cli import run_chainlit

    run_chainlit(__file__)
