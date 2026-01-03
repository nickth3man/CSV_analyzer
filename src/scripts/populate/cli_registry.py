"""Command registry for NBA population CLI.

This module provides a decorator-based command registration system that
reduces boilerplate in the CLI. Commands are registered with their
argument definitions and automatically wired up.

Usage:
    from src.scripts.populate.cli_registry import command, Arg, run_cli

    @command(
        name="player-games",
        help_text="Fetch player game stats",
        args=[
            Arg("--seasons", nargs="+", help_text="Seasons to fetch"),
            Arg("--delay", type=float, default=0.6, help_text="API delay"),
            Arg.flag("--dry-run", help_text="Don't write to database"),
        ],
    )
    def cmd_player_games(args):
        # Implementation
        pass

    if __name__ == "__main__":
        run_cli()
"""

from __future__ import annotations

import argparse
import functools
import logging
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

from src.scripts.utils.ui import (
    print_error,
    print_summary_table,
    print_warning,
)


logger = logging.getLogger(__name__)

# Type for command handler functions
CommandHandler = Callable[[argparse.Namespace], Any]
F = TypeVar("F", bound=CommandHandler)


# =============================================================================
# ARGUMENT DEFINITIONS
# =============================================================================


@dataclass
class Arg:
    """Defines a CLI argument for a command.

    This is a wrapper around argparse.add_argument() that allows
    declarative argument definition for commands.

    Attributes:
        name_or_flags: Argument name(s) (e.g., "--seasons", "-s", "--seasons").
        kwargs: Keyword arguments passed to add_argument().
    """

    name_or_flags: tuple[str, ...]
    kwargs: dict[str, Any] = field(default_factory=dict)

    def __init__(
        self,
        *name_or_flags: str,
        **kwargs: Any,
    ) -> None:
        """Initialize an argument definition.

        Args:
            *name_or_flags: Argument name(s) like "--seasons" or "-s".
            **kwargs: Arguments passed to argparse.add_argument().
        """
        self.name_or_flags = name_or_flags
        self.kwargs = kwargs

    def add_to_parser(self, parser: argparse.ArgumentParser) -> None:
        """Add this argument to an argument parser."""
        parser.add_argument(*self.name_or_flags, **self.kwargs)

    @classmethod
    def flag(cls, *name_or_flags: str, help_text: str = "") -> Arg:
        """Create a boolean flag argument (action="store_true").

        Args:
            *name_or_flags: Flag name(s) like "--dry-run".
            help_text: Help text for the flag.

        Returns:
            Arg configured as a boolean flag.
        """
        return cls(*name_or_flags, action="store_true", help=help_text)

    @classmethod
    def seasons(cls, help_text: str = "Seasons to fetch (YYYY-YY format)") -> Arg:
        """Create a standard --seasons argument."""
        return cls("--seasons", nargs="+", help=help_text)

    @classmethod
    def delay(
        cls, default: float = 0.6, help_text: str = "API delay in seconds"
    ) -> Arg:
        """Create a standard --delay argument."""
        return cls("--delay", type=float, default=default, help=help_text)

    @classmethod
    def limit(cls, help_text: str = "Limit number of items to process") -> Arg:
        """Create a standard --limit argument."""
        return cls("--limit", type=int, help=help_text)

    @classmethod
    def reset(cls, help_text: str = "Reset progress tracking") -> Arg:
        """Create a standard --reset flag."""
        return cls.flag("--reset", help_text=help_text)

    @classmethod
    def dry_run(cls, help_text: str = "Don't write to database") -> Arg:
        """Create a standard --dry-run flag."""
        return cls.flag("--dry-run", help_text=help_text)

    @classmethod
    def regular_only(cls, help_text: str = "Regular season only") -> Arg:
        """Create a standard --regular-only flag."""
        return cls.flag("--regular-only", help_text=help_text)

    @classmethod
    def playoffs_only(cls, help_text: str = "Playoffs only") -> Arg:
        """Create a standard --playoffs-only flag."""
        return cls.flag("--playoffs-only", help_text=help_text)


# =============================================================================
# COMMON ARGUMENT SETS
# =============================================================================


# Arguments shared by most API-fetching commands
STANDARD_API_ARGS: list[Arg] = [
    Arg.seasons(),
    Arg.delay(),
    Arg.reset(),
    Arg.dry_run(),
]

# Arguments for commands that support season type filtering
SEASON_TYPE_ARGS: list[Arg] = [
    Arg.regular_only(),
    Arg.playoffs_only(),
]

# Arguments for player-based commands
PLAYER_ARGS: list[Arg] = [
    Arg.flag("--active-only", help_text="Only process active players"),
    Arg.limit(help_text="Limit number of players"),
]


# =============================================================================
# COMMAND REGISTRY
# =============================================================================


@dataclass
class CommandDefinition:
    """Stores metadata about a registered command.

    Attributes:
        name: Command name (used in CLI).
        handler: Function that handles the command.
        help_text: Help text for the command.
        args: List of argument definitions.
        aliases: Alternative names for the command.
        category: Category for grouping in help output.
    """

    name: str
    handler: CommandHandler
    help_text: str
    args: list[Arg] = field(default_factory=list)
    aliases: list[str] = field(default_factory=list)
    category: str = "general"


class CommandRegistry:
    """Central registry for CLI commands.

    This class maintains a registry of all commands and provides
    methods to register, look up, and build argument parsers.
    """

    def __init__(self) -> None:
        """Initialize an empty command registry."""
        self._commands: dict[str, CommandDefinition] = {}
        self._aliases: dict[str, str] = {}

    def register(
        self,
        name: str,
        handler: CommandHandler,
        *,
        help_text: str = "",
        args: Sequence[Arg] | None = None,
        aliases: Sequence[str] | None = None,
        category: str = "general",
    ) -> None:
        """Register a command with the registry.

        Args:
            name: Primary command name.
            handler: Function that handles the command.
            help_text: Help text displayed in --help.
            args: List of Arg definitions for the command.
            aliases: Alternative names for the command.
            category: Category for grouping in help output.
        """
        cmd_def = CommandDefinition(
            name=name,
            handler=handler,
            help_text=help_text,
            args=list(args) if args else [],
            aliases=list(aliases) if aliases else [],
            category=category,
        )
        self._commands[name] = cmd_def

        # Register aliases
        for alias in cmd_def.aliases:
            self._aliases[alias] = name

    def get(self, name: str) -> CommandDefinition | None:
        """Get a command definition by name or alias.

        Args:
            name: Command name or alias.

        Returns:
            CommandDefinition if found, None otherwise.
        """
        # Check if it's an alias
        if name in self._aliases:
            name = self._aliases[name]

        return self._commands.get(name)

    def get_handler(self, name: str) -> CommandHandler | None:
        """Get the handler function for a command.

        Args:
            name: Command name or alias.

        Returns:
            Handler function if found, None otherwise.
        """
        cmd_def = self.get(name)
        return cmd_def.handler if cmd_def else None

    def all_commands(self) -> list[CommandDefinition]:
        """Return all registered commands."""
        return list(self._commands.values())

    def commands_by_category(self) -> dict[str, list[CommandDefinition]]:
        """Return commands grouped by category."""
        by_category: dict[str, list[CommandDefinition]] = {}
        for cmd_def in self._commands.values():
            if cmd_def.category not in by_category:
                by_category[cmd_def.category] = []
            by_category[cmd_def.category].append(cmd_def)
        return by_category

    def build_subparsers(
        self,
        parser: argparse.ArgumentParser,
    ) -> argparse._SubParsersAction:  # type: ignore[type-arg]
        """Build subparsers for all registered commands.

        Args:
            parser: Parent argument parser.

        Returns:
            SubParsersAction for the created subparsers.
        """
        subparsers = parser.add_subparsers(dest="command")

        for cmd_def in self._commands.values():
            # Create subparser for this command
            cmd_parser = subparsers.add_parser(
                cmd_def.name,
                help=cmd_def.help_text,
                aliases=cmd_def.aliases,
            )

            # Add arguments
            for arg in cmd_def.args:
                arg.add_to_parser(cmd_parser)

        return subparsers


# Global registry instance
registry = CommandRegistry()


# =============================================================================
# DECORATOR API
# =============================================================================


def command(
    name: str,
    *,
    help_text: str = "",
    args: Sequence[Arg] | None = None,
    aliases: Sequence[str] | None = None,
    category: str = "general",
) -> Callable[[F], F]:
    """Decorator to register a function as a CLI command.

    Args:
        name: Command name (used in CLI).
        help_text: Help text for the command.
        args: List of Arg definitions.
        aliases: Alternative names for the command.
        category: Category for grouping.

    Returns:
        Decorator function that registers the command.

    Example:
        @command(
            name="player-games",
            help_text="Fetch player game stats",
            args=[Arg.seasons(), Arg.delay()],
        )
        def cmd_player_games(args):
            pass
    """

    def decorator(func: F) -> F:
        registry.register(
            name=name,
            handler=func,
            help_text=help_text,
            args=list(args) if args else [],
            aliases=list(aliases) if aliases else [],
            category=category,
        )

        @functools.wraps(func)
        def wrapper(parsed_args: argparse.Namespace) -> Any:
            return func(parsed_args)

        return wrapper  # type: ignore[return-value]

    return decorator


# =============================================================================
# CLI RUNNER
# =============================================================================


def create_parser(
    prog: str = "nba-populate",
    description: str = "NBA Database Population CLI",
) -> argparse.ArgumentParser:
    """Create the main argument parser with all registered commands.

    Args:
        prog: Program name.
        description: CLI description.

    Returns:
        Configured ArgumentParser with all subcommands.
    """
    parser = argparse.ArgumentParser(
        prog=prog,
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Global arguments
    parser.add_argument(
        "--db",
        help="Database path (overrides config)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    # Build subparsers from registry
    registry.build_subparsers(parser)

    return parser


def run_cli(
    args: Sequence[str] | None = None,
    *,
    exit_on_error: bool = True,
) -> int:
    """Run the CLI with the given arguments.

    Args:
        args: Command-line arguments (defaults to sys.argv).
        exit_on_error: Whether to call sys.exit on error.

    Returns:
        Exit code (0 for success, non-zero for error).
    """
    parser = create_parser()
    parsed = parser.parse_args(args)

    if parsed.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not parsed.command:
        parser.print_help()
        return 0

    handler = registry.get_handler(parsed.command)
    if not handler:
        print_error(f"Unknown command: {parsed.command}")
        return 1

    try:
        result = handler(parsed)

        # Check for errors in result
        if isinstance(result, dict):
            error_count = result.get("error_count", 0)
            if error_count > 0:
                return 1

            # Print summary if available
            if "records_fetched" in result or "duration_seconds" in result:
                print_summary_table(f"{parsed.command} Summary", result)

        return 0

    except KeyboardInterrupt:
        print_warning("Interrupted by user")
        if exit_on_error:
            sys.exit(1)
        return 1

    except Exception as e:
        print_error(f"Command failed: {e}")
        if parsed.verbose:
            import traceback

            traceback.print_exc()
        if exit_on_error:
            sys.exit(1)
        return 1
