"""
Main entry point for the Chainlit Data Analyst Agent.

This file imports and registers all handlers from the frontend module.
Run with: chainlit run app.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

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
