"""Main entry point for the Chainlit Data Analyst Agent.

This file imports and registers all handlers from the frontend module.
Run with: chainlit run src/frontend/app.py
"""

import sys
from pathlib import Path


# Add project root + src so "src.*" and "backend.*" imports resolve in Chainlit.
project_root = Path(__file__).resolve().parents[2]
src_path = project_root / "src"
for path in (project_root, src_path):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

# Import and register all Chainlit handlers
# These decorators register themselves with Chainlit when imported
from frontend import (  # noqa: F401
    chat_profile,
    on_chat_start,
    on_list_tables,
    on_message,
    on_settings_update,
    on_show_help,
    on_view_schema,
    set_starters,
)


if __name__ == "__main__":
    from chainlit.cli import run_chainlit

    run_chainlit(__file__)
