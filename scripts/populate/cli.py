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
    python -m scripts.populate.cli player-games --seasons 2023-24

    # Run full population pipeline
    python -m scripts.populate.cli all
"""

import argparse
import logging
import sys
from typing import List, Optional

from scripts.populate.config import ALL_SEASONS, DEFAULT_SEASON_TYPES, get_db_path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def cmd_init(args):
    """Initialize database schema."""
    from scripts.populate.init_db import init_database

    return init_database(
        db_path=args.db,
        force=args.force,
        tables=args.tables,
    )


def cmd_info(args):
    """Show database information."""
    from scripts.populate.init_db import get_database_info

    info = get_database_info(args.db)

    print(f"\nDatabase: {info['path']}")
    print(f"Exists: {info['exists']}")

    if info['exists']:
        print(f"Tables: {info['table_count']}")
        print("\nTable details:")
        for table, data in sorted(info.get('tables', {}).items()):
            if 'rows' in data:
                print(f"  {table}: {data['rows']:,} rows")
            else:
                print(f"  {table}: {data.get('error', 'unknown')}")


def cmd_load_csv(args):
    """Load data from CSV files."""
    import subprocess

    logger.info("Loading CSV files into database...")
    result = subprocess.run(
        [sys.executable, "scripts/convert_csvs.py"],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        sys.exit(1)


def cmd_normalize(args):
    """Normalize database (create silver tables)."""
    import subprocess

    logger.info("Normalizing database tables...")
    result = subprocess.run(
        [sys.executable, "scripts/normalize_db.py"],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        sys.exit(1)


def cmd_player_games(args):
    """Fetch player game stats using bulk endpoint."""
    from scripts.populate.populate_player_game_stats_v2 import populate_player_game_stats_v2

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
    """Fetch player game stats using per-player endpoint (legacy)."""
    from scripts.populate.populate_player_game_stats import populate_player_game_stats

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
    """Fetch play-by-play data."""
    from scripts.populate.populate_play_by_play import populate_play_by_play

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
    from scripts.populate.populate_player_season_stats import populate_player_season_stats

    return populate_player_season_stats(
        db_path=args.db,
        seasons=args.seasons,
    )


def cmd_all(args):
    """Run full population pipeline."""
    logger.info("=" * 70)
    logger.info("FULL NBA DATABASE POPULATION PIPELINE")
    logger.info("=" * 70)

    steps = [
        ("Initialize database", lambda: cmd_init(args)),
        ("Load CSV files", lambda: cmd_load_csv(args)),
        ("Normalize tables", lambda: cmd_normalize(args)),
    ]

    if not args.skip_api:
        steps.extend([
            ("Fetch player game stats", lambda: cmd_player_games(args)),
        ])

    steps.append(("Create season stats", lambda: cmd_season_stats(args)))

    for step_name, step_func in steps:
        logger.info(f"\n{'='*60}")
        logger.info(f"Step: {step_name}")
        logger.info("=" * 60)
        try:
            step_func()
        except Exception as e:
            logger.error(f"Step failed: {e}")
            if not args.continue_on_error:
                sys.exit(1)

    logger.info("\n" + "=" * 70)
    logger.info("POPULATION PIPELINE COMPLETE")
    logger.info("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="NBA Database Population CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Global arguments
    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database (default: data/nba.duckdb)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # init command
    init_parser = subparsers.add_parser("init", help="Initialize database schema")
    init_parser.add_argument("--force", action="store_true", help="Force recreate tables")
    init_parser.add_argument("--tables", nargs="+", help="Specific tables to create")

    # info command
    info_parser = subparsers.add_parser("info", help="Show database information")

    # load-csv command
    csv_parser = subparsers.add_parser("load-csv", help="Load CSV files into database")

    # normalize command
    norm_parser = subparsers.add_parser("normalize", help="Normalize database tables")

    # player-games command
    pg_parser = subparsers.add_parser(
        "player-games",
        help="Fetch player game stats (bulk endpoint)"
    )
    pg_parser.add_argument("--seasons", nargs="+", help="Seasons to fetch")
    pg_parser.add_argument("--delay", type=float, default=0.6, help="API delay in seconds")
    pg_parser.add_argument("--regular-only", action="store_true", help="Regular season only")
    pg_parser.add_argument("--playoffs-only", action="store_true", help="Playoffs only")
    pg_parser.add_argument("--reset", action="store_true", help="Reset progress")
    pg_parser.add_argument("--dry-run", action="store_true", help="Don't write to database")

    # player-games-legacy command
    pgl_parser = subparsers.add_parser(
        "player-games-legacy",
        help="Fetch player game stats (per-player endpoint)"
    )
    pgl_parser.add_argument("--seasons", nargs="+", help="Seasons to fetch")
    pgl_parser.add_argument("--active-only", action="store_true", help="Active players only")
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
    all_parser.add_argument("--continue-on-error", action="store_true", help="Continue on errors")
    all_parser.add_argument("--seasons", nargs="+", help="Seasons for API fetch")
    all_parser.add_argument("--delay", type=float, default=0.6, help="API delay")
    all_parser.add_argument("--regular-only", action="store_true")
    all_parser.add_argument("--playoffs-only", action="store_true")
    all_parser.add_argument("--force", action="store_true", help="Force recreate tables")
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
            logger.error(f"Command failed: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
