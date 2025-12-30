#!/usr/bin/env python3
"""Unified CLI for NBA database population.

This script provides a single entry point for all database population operations:
- Initialize database schema
- Load data from CSV files
- Fetch data from NBA API
- Create aggregated views

Usage:
    # Show all available commands
    python -m scripts.populate.cli --help

    # Initialize database
    python -m scripts.populate.cli init

    # Load CSV data
    python -m scripts.populate.cli load-csv

    # Fetch player game stats (bulk)
    python -m scripts.populate.cli player-games --seasons 2025-26 2024-25

    # Run full population pipeline
    python -m scripts.populate.cli all
"""

import argparse
import logging
import sys
from typing import Any

from src.scripts.populate.config import (
    DEFAULT_SEASON_TYPES,
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def cmd_init(args):
    """Initialize the database schema at the configured database path.

    Returns:
        dict: Result of the initialization operation; may include an `error_count` key indicating the number of errors encountered.
    """
    from src.scripts.populate.init_db import init_database

    return init_database(
        db_path=args.db,
        force=args.force,
        tables=args.tables,
    )


def cmd_info(args) -> None:
    """Prints information about the database at the path specified in `args.db`.

    Parameters:
        args: An object (typically argparse.Namespace) with attribute `db` giving the path to the database. The function prints the database path, whether it exists, the number of tables (if present), and per-table row counts or error messages.
    """
    from src.scripts.populate.init_db import get_database_info

    info = get_database_info(args.db)

    if info["exists"]:
        tables_dict: dict[str, Any] = info.get("tables", {})
        for _table, data in sorted(tables_dict.items()):
            if "rows" in data:
                pass
            else:
                pass


def cmd_load_csv(args) -> None:
    """Load data from CSV files into the configured database.

    Runs the scripts/migration/convert_csvs.py helper and prints its stdout. If the helper exits with a non-zero status, prints the helper's stderr and exits the process with status 1.
    """
    import subprocess

    logger.info("Loading CSV files into database...")
    result = subprocess.run(
        [sys.executable, "src/scripts/migration/convert_csvs.py"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.exit(1)


def cmd_normalize(args) -> None:
    """Run the database normalization step to create the "silver" tables.

    Invokes the external scripts/maintenance/normalize_db.py script, prints its standard output, and on non-zero exit prints the script's standard error and terminates the process with status 1.
    """
    import subprocess

    logger.info("Normalizing database tables...")
    result = subprocess.run(
        [sys.executable, "src/scripts/maintenance/normalize_db.py"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.exit(1)


def cmd_player_games(args):
    """Run the bulk player game stats population step for the specified seasons and season types.

    @returns The value returned by populate_player_game_stats_v2 — typically a dict containing status information (may include an `error_count` key).
    """
    from src.scripts.populate.populate_player_game_stats_v2 import (
        populate_player_game_stats_v2,
    )

    season_types = DEFAULT_SEASON_TYPES
    if args.regular_only:
        season_types = ["Regular Season"]
    elif args.playoffs_only:
        season_types = ["Playoffs"]

    return populate_player_game_stats_v2(
        db_path=args.db,
        seasons=args.seasons,
        season_types=season_types,
        delay=args.delay,
        reset_progress=args.reset,
        dry_run=args.dry_run,
    )


def cmd_player_games_legacy(args):
    """Populate player game statistics using the legacy per-player endpoint according to the provided CLI arguments.

    Returns:
        The value returned by `populate_player_game_stats` — typically a dictionary containing status information (for example progress or `error_count`).
    """
    from src.scripts.populate.populate_player_game_stats import populate_player_game_stats

    season_types = DEFAULT_SEASON_TYPES
    if args.regular_only:
        season_types = ["Regular Season"]
    elif args.playoffs_only:
        season_types = ["Playoffs"]

    return populate_player_game_stats(
        db_path=args.db,
        seasons=args.seasons,
        active_only=args.active_only,
        limit=args.limit,
        resume_from=args.resume_from,
        delay=args.delay,
        season_types=season_types,
    )


def cmd_play_by_play(args):
    """Fetch play-by-play data for the specified games or seasons.

    Parameters:
        args (argparse.Namespace): Parsed CLI arguments containing:
            - db: path to the DuckDB database
            - games: iterable or list of game IDs to fetch
            - seasons: iterable or list of seasons to fetch
            - limit: maximum number of games to fetch (optional)
            - delay: delay in seconds between API requests
            - resume_from: game ID or cursor to resume fetching from (optional)

    Returns:
        result (dict): Summary of the operation. May include keys such as `error_count` (int) and other status details.
    """
    from src.scripts.populate.populate_play_by_play import populate_play_by_play

    return populate_play_by_play(
        db_path=args.db,
        games=args.games,
        seasons=args.seasons,
        limit=args.limit,
        delay=args.delay,
        resume_from=args.resume_from,
    )


def cmd_season_stats(args):
    """Create player season stats (aggregated)."""
    from src.scripts.populate.populate_player_season_stats import (
        populate_player_season_stats,
    )

    return populate_player_season_stats(
        db_path=args.db,
        seasons=args.seasons,
    )


def cmd_all(args) -> None:
    """Run the complete database population pipeline using the provided CLI arguments.

    Parameters:
        args (argparse.Namespace): Parsed CLI arguments. Recognized attributes:
            - skip_api (bool): If true, skip API fetching steps.
            - continue_on_error (bool): If true, continue executing remaining steps when a step raises an exception.
            - Any other flags (seasons, delay, force, tables, reset, dry_run, etc.) are forwarded to the individual step handlers.
    """
    logger.info("=" * 70)
    logger.info("FULL NBA DATABASE POPULATION PIPELINE")
    logger.info("=" * 70)

    steps = [
        ("Initialize database", lambda: cmd_init(args)),
        ("Load CSV files", lambda: cmd_load_csv(args)),
        ("Normalize tables", lambda: cmd_normalize(args)),
    ]

    if not args.skip_api:
        steps.extend(
            [
                ("Fetch player game stats", lambda: cmd_player_games(args)),
            ],
        )

    steps.append(("Create season stats", lambda: cmd_season_stats(args)))

    for step_name, step_func in steps:
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Step: {step_name}")
        logger.info("=" * 60)
        try:
            step_func()
        except Exception as e:
            logger.exception(f"Step failed: {e}")
            if not args.continue_on_error:
                sys.exit(1)

    logger.info("\n" + "=" * 70)
    logger.info("POPULATION PIPELINE COMPLETE")
    logger.info("=" * 70)


def main() -> None:
    """Parse CLI arguments for NBA population tasks and invoke the chosen command handler.

    Supports global options (database path, verbose), multiple subcommands (init, info, load-csv, normalize,
    player-games, player-games-legacy, play-by-play, season-stats, all), and dispatches to the corresponding
    cmd_* handler. Sets logging to DEBUG when verbose is enabled, prints help and exits when no command is given,
    and exits with status 1 when a handler reports errors or an exception occurs (prints a traceback when verbose).
    """
    parser = argparse.ArgumentParser(
        description="NBA Database Population CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Global arguments
    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database (default: src/backend/data/nba.duckdb)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # init command
    init_parser = subparsers.add_parser("init", help="Initialize database schema")
    init_parser.add_argument(
        "--force", action="store_true", help="Force recreate tables",
    )
    init_parser.add_argument("--tables", nargs="+", help="Specific tables to create")

    # info command
    subparsers.add_parser("info", help="Show database information")

    # load-csv command
    subparsers.add_parser("load-csv", help="Load CSV files into database")

    # normalize command
    subparsers.add_parser("normalize", help="Normalize database tables")

    # player-games command
    pg_parser = subparsers.add_parser(
        "player-games",
        help="Fetch player game stats (bulk endpoint)",
    )
    pg_parser.add_argument("--seasons", nargs="+", help="Seasons to fetch")
    pg_parser.add_argument(
        "--delay", type=float, default=0.6, help="API delay in seconds",
    )
    pg_parser.add_argument(
        "--regular-only", action="store_true", help="Regular season only",
    )
    pg_parser.add_argument("--playoffs-only", action="store_true", help="Playoffs only")
    pg_parser.add_argument("--reset", action="store_true", help="Reset progress")
    pg_parser.add_argument(
        "--dry-run", action="store_true", help="Don't write to database",
    )

    # player-games-legacy command
    pgl_parser = subparsers.add_parser(
        "player-games-legacy",
        help="Fetch player game stats (per-player endpoint)",
    )
    pgl_parser.add_argument("--seasons", nargs="+", help="Seasons to fetch")
    pgl_parser.add_argument(
        "--active-only", action="store_true", help="Active players only",
    )
    pgl_parser.add_argument("--limit", type=int, help="Limit number of players")
    pgl_parser.add_argument("--resume-from", type=int, help="Resume from player ID")
    pgl_parser.add_argument("--delay", type=float, default=0.6, help="API delay")
    pgl_parser.add_argument("--regular-only", action="store_true")
    pgl_parser.add_argument("--playoffs-only", action="store_true")

    # play-by-play command
    pbp_parser = subparsers.add_parser("play-by-play", help="Fetch play-by-play data")
    pbp_parser.add_argument("--games", nargs="+", help="Specific game IDs")
    pbp_parser.add_argument("--seasons", nargs="+", help="Seasons to fetch")
    pbp_parser.add_argument("--limit", type=int, help="Limit number of games")
    pbp_parser.add_argument("--delay", type=float, default=0.6, help="API delay")
    pbp_parser.add_argument("--resume-from", help="Resume from game ID")

    # season-stats command
    ss_parser = subparsers.add_parser("season-stats", help="Create player season stats")
    ss_parser.add_argument("--seasons", nargs="+", help="Specific seasons")

    # all command
    all_parser = subparsers.add_parser("all", help="Run full population pipeline")
    all_parser.add_argument("--skip-api", action="store_true", help="Skip API fetching")
    all_parser.add_argument(
        "--continue-on-error", action="store_true", help="Continue on errors",
    )
    all_parser.add_argument("--seasons", nargs="+", help="Seasons for API fetch")
    all_parser.add_argument("--delay", type=float, default=0.6, help="API delay")
    all_parser.add_argument("--regular-only", action="store_true")
    all_parser.add_argument("--playoffs-only", action="store_true")
    all_parser.add_argument(
        "--force", action="store_true", help="Force recreate tables",
    )
    all_parser.add_argument("--tables", nargs="+", help="Specific tables")
    all_parser.add_argument("--reset", action="store_true")
    all_parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Dispatch to command handler
    handlers = {
        "init": cmd_init,
        "info": cmd_info,
        "load-csv": cmd_load_csv,
        "normalize": cmd_normalize,
        "player-games": cmd_player_games,
        "player-games-legacy": cmd_player_games_legacy,
        "play-by-play": cmd_play_by_play,
        "season-stats": cmd_season_stats,
        "all": cmd_all,
    }

    handler = handlers.get(args.command)
    if handler:
        try:
            result = handler(args)
            if isinstance(result, dict) and result.get("error_count", 0) > 0:
                sys.exit(1)
        except KeyboardInterrupt:
            logger.info("\nInterrupted by user")
            sys.exit(1)
        except Exception as e:
            logger.exception(f"Command failed: {e}")
            if args.verbose:
                import traceback

                traceback.print_exc()
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
