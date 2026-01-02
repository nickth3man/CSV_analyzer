"""UI utilities for CLI output using Rich."""

import contextlib
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.theme import Theme


# Custom theme for NBA Expert
nba_theme = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "bold red",
        "success": "bold green",
        "highlight": "magenta",
        "header": "bold blue",
    }
)

console = Console(theme=nba_theme)


def print_header(title: str):
    """Print a stylized header."""
    console.print(f"\n[header]{'=' * 70}[/header]")
    console.print(f"[header]{title.center(70)}[/header]")
    console.print(f"[header]{'=' * 70}[/header]\n")


def print_step(step_name: str):
    """Print a step indicator."""
    console.print(f"\n[highlight]>>> {step_name}[/highlight]")


def print_success(message: str):
    """Print a success message."""
    console.print(f"[success]+ {message}[/success]")


def print_error(message: str):
    """Print an error message."""
    console.print(f"[error]- {message}[/error]")


def print_warning(message: str):
    """Print a warning message."""
    console.print(f"[warning]! {message}[/warning]")


def print_summary_table(title: str, data: dict[str, Any]):
    """Print a summary table of results."""
    table = Table(title=title, show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")

    for key, value in data.items():
        # Format key for display
        display_key = key.replace("_", " ").title()

        # Format value based on type/content
        display_value = str(value)
        if key == "status" and value == "success":
            display_value = f"[success]{value}[/success]"
        elif key == "status" and value == "error":
            display_value = f"[error]{value}[/error]"
        elif "count" in key or "records" in key:
            with contextlib.suppress(ValueError, TypeError):
                display_value = f"{int(value):,}"
        elif key == "duration":
            with contextlib.suppress(ValueError, TypeError):
                display_value = f"{float(value):.2f}s"

        table.add_row(display_key, display_value)

    console.print(table)


def create_progress_bar() -> Progress:
    """Create a standard progress bar for long-running tasks."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TextColumn("/"),
        TimeRemainingColumn(),
        console=console,
    )


def print_panel(message: str, title: str | None = None, style: str = "info"):
    """Print a message in a panel."""
    console.print(Panel(message, title=title, border_style=style))
