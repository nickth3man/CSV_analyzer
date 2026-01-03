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
import time
from typing import Any

from src.scripts.populate.config import (
    DEFAULT_SEASON_TYPES,
)
from src.scripts.populate.helpers import configure_logging, resolve_season_types
from src.scripts.utils.ui import (
    console,
    print_error,
    print_header,
    print_step,
    print_success,
    print_summary_table,
    print_warning,
)


# Configure logging
configure_logging()

logger = logging.getLogger(__name__)


def cmd_init(args):
    """Initialize the database schema at the configured database path."""
    from src.scripts.populate.init_db import init_database

    print_step("Initializing database schema")
    result = init_database(
        db_path=args.db,
        force=args.force,
        tables=args.tables,
    )
    error_count = sum(
        1
        for status in result.values()
        if isinstance(status, str)
        and (status.startswith("error") or status == "unknown")
    )
    result["error_count"] = error_count
    if error_count == 0:
        print_success("Database initialized successfully")
    else:
        print_error(f"Database initialization failed with {error_count} errors")
    return result


def cmd_info(args) -> None:
    """Prints information about the database."""
    from src.scripts.populate.init_db import get_database_info

    print_step(f"Database Information: {args.db or 'default'}")
    info = get_database_info(args.db)

    if info["exists"]:
        from rich.table import Table

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Table Name", style="cyan")
        table.add_column("Rows", justify="right", style="green")

        tables_dict: dict[str, Any] = info.get("tables", {})
        for table_name, data in sorted(tables_dict.items()):
            rows = data.get("rows", "Error")
            if isinstance(rows, int):
                rows = f"{rows:,}"
            table.add_row(table_name, str(rows))

        console.print(table)
    else:
        print_error("Database does not exist")


def cmd_load_csv(args) -> None:
    """Load data from CSV files into the configured database."""
    print_step("Loading CSV files into database")
    from src.scripts.migration.convert_csvs import run_ingestion_pipeline

    try:
        run_ingestion_pipeline(db_path=args.db)
    except Exception as exc:
        print_error(f"Failed to load CSV files: {exc}")
        raise
    print_success("CSV files loaded successfully")


def cmd_normalize(args) -> None:
    """Run the database normalization step to create the "silver" tables."""
    print_step("Normalizing database tables")
    from src.scripts.maintenance.normalize_db import transform_to_silver

    transform_to_silver(db_path=args.db)
    print_success("Database normalization complete")


def cmd_game_gold(args) -> None:
    """Create game_gold table by deduplicating game_silver."""
    print_step("Creating game_gold table")
    from src.scripts.maintenance.fix_game_duplicates import fix_duplicates

    fix_duplicates(db_path=args.db)
    print_success("game_gold table created successfully")


def cmd_gold_entities(args) -> None:
    """Create gold entity tables (player_gold, team_gold)."""
    from src.scripts.maintenance.create_gold_entities import create_gold_entities
    from src.scripts.populate.config import get_db_path

    print_step("Creating gold entity tables")
    create_gold_entities(db_path=args.db or str(get_db_path()))
    print_success("Gold entities created successfully")


def cmd_gold_tables(args) -> None:
    """Create canonical gold tables (games, team_game_stats, player_game_stats)."""
    from src.scripts.maintenance.create_gold_tables import create_gold_tables
    from src.scripts.populate.config import get_db_path

    print_step("Creating canonical gold tables")
    create_gold_tables(db_path=args.db or str(get_db_path()))
    print_success("Gold tables created successfully")


def cmd_common_player_info(args):
    """Populate common_player_info using NBA API."""
    from src.scripts.populate.populate_common_player_info import (
        populate_common_player_info,
    )

    print_step("Fetching Common Player Info")
    result = populate_common_player_info(
        db_path=args.db,
        active_only=getattr(args, "active_only", False),
        limit=getattr(args, "limit", None),
        reset_progress=args.reset,
        dry_run=args.dry_run,
    )
    print_summary_table("Common Player Info Summary", result)
    return result


def cmd_draft_history(args):
    """Populate draft_history using NBA API."""
    from src.scripts.populate.populate_draft_history import populate_draft_history

    print_step("Fetching Draft History")
    result = populate_draft_history(
        db_path=args.db,
        season=getattr(args, "season", None),
        reset_progress=args.reset,
        dry_run=args.dry_run,
    )
    print_summary_table("Draft History Summary", result)
    return result


def cmd_draft_combine(args):
    """Populate draft_combine_stats using NBA API."""
    from src.scripts.populate.populate_draft_combine_stats import (
        populate_draft_combine_stats,
    )

    print_step("Fetching Draft Combine Stats")
    result = populate_draft_combine_stats(
        db_path=args.db,
        seasons=args.seasons,
        reset_progress=args.reset,
        dry_run=args.dry_run,
    )
    print_summary_table("Draft Combine Summary", result)
    return result


def cmd_team_info_common(args):
    """Populate team_info_common using NBA API."""
    from src.scripts.populate.populate_team_info_common import populate_team_info_common

    print_step("Fetching Team Info Common")
    result = populate_team_info_common(
        db_path=args.db,
        seasons=args.seasons,
        season_type=getattr(args, "season_type", None),
        reset_progress=args.reset,
        dry_run=args.dry_run,
    )
    print_summary_table("Team Info Summary", result)
    return result


def cmd_team_details(args):
    """Populate team_details using NBA API."""
    from src.scripts.populate.populate_team_details import populate_team_details

    print_step("Fetching Team Details")
    result = populate_team_details(
        db_path=args.db,
        reset_progress=args.reset,
        dry_run=args.dry_run,
    )
    print_summary_table("Team Details Summary", result)
    return result


def cmd_player_games(args):
    """Run the bulk player game stats population step."""
    from src.scripts.populate.populate_player_game_stats_v2 import (
        populate_player_game_stats_v2,
    )

    print_step("Fetching Player Game Stats (Bulk)")
    season_types = resolve_season_types(
        DEFAULT_SEASON_TYPES,
        regular_only=args.regular_only,
        playoffs_only=args.playoffs_only,
    )

    result = populate_player_game_stats_v2(
        db_path=args.db,
        seasons=args.seasons,
        season_types=season_types,
        delay=args.delay,
        reset_progress=args.reset,
        dry_run=args.dry_run,
    )
    print_summary_table("Player Games Summary", result)
    return result


def cmd_league_games(args):
    """Populate game table using LeagueGameLog (team-level)."""
    from src.scripts.populate.populate_league_game_logs import (
        populate_league_game_logs,
    )

    print_step("Fetching League Game Logs")
    season_types = resolve_season_types(
        DEFAULT_SEASON_TYPES,
        regular_only=args.regular_only,
        playoffs_only=args.playoffs_only,
    )

    result = populate_league_game_logs(
        db_path=args.db,
        seasons=args.seasons,
        season_types=season_types,
        delay=args.delay,
        reset_progress=args.reset,
        dry_run=args.dry_run,
    )
    print_summary_table("League Games Summary", result)
    return result


def cmd_player_games_legacy(args):
    """Populate player game statistics using the legacy per-player endpoint."""
    from src.scripts.populate.populate_player_game_stats import (
        populate_player_game_stats,
    )

    print_step("Fetching Player Game Stats (Legacy)")
    season_types = resolve_season_types(
        DEFAULT_SEASON_TYPES,
        regular_only=args.regular_only,
        playoffs_only=args.playoffs_only,
    )

    result = populate_player_game_stats(
        db_path=args.db,
        seasons=args.seasons,
        active_only=args.active_only,
        limit=args.limit,
        resume_from=args.resume_from,
        delay=args.delay,
        season_types=season_types,
    )
    print_summary_table("Player Games (Legacy) Summary", result)
    return result


def cmd_play_by_play(args):
    """Fetch play-by-play data for the specified games or seasons."""
    from src.scripts.populate.populate_play_by_play import populate_play_by_play

    print_step("Fetching Play-by-Play Data")
    result = populate_play_by_play(
        db_path=args.db,
        games=args.games,
        seasons=args.seasons,
        limit=args.limit,
        delay=args.delay,
        resume_from=args.resume_from,
    )
    print_summary_table("Play-by-Play Summary", result)
    return result


def cmd_validate(args) -> None:
    """Run integrity checks against the database."""
    print_step("Running database integrity checks")
    from rich.table import Table

    from src.scripts.maintenance.check_integrity import check_integrity

    results = check_integrity(db_path=args.db)
    summary = {
        "error_count": results.get("error_count", 0),
        "pk_checks": len(results.get("pk_checks", [])),
        "fk_checks": len(results.get("fk_checks", [])),
    }
    print_summary_table("Integrity Check Summary", summary)

    failures = []
    for entry in results.get("pk_checks", []):
        if entry.get("status") != "Passed":
            failures.append(
                (
                    entry.get("table"),
                    entry.get("column"),
                    entry.get("status"),
                    entry.get("error", ""),
                ),
            )
    for entry in results.get("fk_checks", []):
        if entry.get("status") != "Passed":
            failures.append(
                (
                    entry.get("child_table"),
                    entry.get("child_col"),
                    entry.get("status"),
                    f"orphans={entry.get('orphan_count', '')}",
                ),
            )

    if failures:
        table = Table(
            title="Integrity Failures",
            show_header=True,
            header_style="bold magenta",
        )
        table.add_column("Table", style="cyan")
        table.add_column("Column", style="cyan")
        table.add_column("Status", style="bold red")
        table.add_column("Details", style="yellow")
        for row in failures:
            table.add_row(*[str(item or "") for item in row])
        console.print(table)
        print_error("Integrity checks failed")
        sys.exit(1)

    print_success("Integrity checks passed")


def cmd_season_stats(args):
    """Create player season stats (aggregated)."""
    from src.scripts.populate.populate_player_season_stats import (
        populate_player_season_stats,
    )

    print_step("Creating Player Season Stats (Aggregated)")
    result = populate_player_season_stats(
        db_path=args.db,
        seasons=args.seasons,
    )
    print_summary_table("Season Stats Summary", result)
    return result


def cmd_metrics(args):
    """Create advanced analytics metrics (rolling averages, standings, etc.)."""
    print_step("Creating Advanced Analytics Metrics")
    from src.scripts.analysis.create_advanced_metrics import create_advanced_metrics
    from src.scripts.populate.config import get_db_path

    create_advanced_metrics(db_path=args.db or str(get_db_path()))
    print_success("Advanced metrics created successfully")


def cmd_br_box_scores(args):
    """Fetch player box scores from Basketball Reference."""
    from src.scripts.populate.populate_br_player_box_scores import (
        populate_br_player_box_scores,
    )

    print_step("Fetching Basketball Reference Box Scores")
    result = populate_br_player_box_scores(
        db_path=args.db,
        start_date=getattr(args, "start_date", None),
        end_date=getattr(args, "end_date", None),
        start_year=getattr(args, "start_year", 1980),
        end_year=getattr(args, "end_year", 1996),
        limit=getattr(args, "limit", None),
        delay=getattr(args, "delay", None),
        reset_progress=getattr(args, "reset", False),
        dry_run=getattr(args, "dry_run", False),
    )
    print_summary_table("BR Box Scores Summary", result)
    return result


def cmd_br_season_stats(args):
    """Fetch season stats from Basketball Reference."""
    from src.scripts.populate.populate_br_season_stats import (
        populate_br_season_stats,
    )

    print_step("Fetching Basketball Reference Season Stats")
    seasons = None
    if hasattr(args, "seasons") and args.seasons:
        seasons = [int(s) for s in args.seasons]

    result = populate_br_season_stats(
        db_path=args.db,
        seasons=seasons,
        start_year=getattr(args, "start_year", 1980),
        end_year=getattr(args, "end_year", 1996),
        include_advanced=not getattr(args, "basic_only", False),
        reset_progress=getattr(args, "reset", False),
        dry_run=getattr(args, "dry_run", False),
    )
    print_summary_table("BR Season Stats Summary", result)
    return result


def cmd_all(args) -> None:
    """Run the complete database population pipeline."""
    from rich.table import Table

    from src.scripts.populate.config import ALL_SEASONS

    if not args.seasons:
        args.seasons = ALL_SEASONS

    print_header("FULL NBA DATABASE POPULATION PIPELINE")

    steps = [
        ("Initialize database", lambda: cmd_init(args)),
        ("Load CSV files", lambda: cmd_load_csv(args)),
    ]

    if not args.skip_api:
        steps.extend(
            [
                ("Populate draft history", lambda: cmd_draft_history(args)),
                ("Populate draft combine stats", lambda: cmd_draft_combine(args)),
                ("Populate team details", lambda: cmd_team_details(args)),
                ("Populate team info common", lambda: cmd_team_info_common(args)),
                ("Populate common player info", lambda: cmd_common_player_info(args)),
                ("Populate league game logs", lambda: cmd_league_games(args)),
                ("Fetch player game stats", lambda: cmd_player_games(args)),
                ("Fetch play-by-play", lambda: cmd_play_by_play(args)),
            ],
        )

    steps.extend(
        [
            ("Normalize tables", lambda: cmd_normalize(args)),
            ("Create game_gold", lambda: cmd_game_gold(args)),
            ("Create gold entities", lambda: cmd_gold_entities(args)),
            ("Create gold tables", lambda: cmd_gold_tables(args)),
        ],
    )

    steps.append(("Create season stats", lambda: cmd_season_stats(args)))
    steps.append(("Create advanced metrics", lambda: cmd_metrics(args)))
    steps.append(("Validate database", lambda: cmd_validate(args)))

    results = []
    start_time = time.time()

    for step_name, step_func in steps:
        step_start = time.time()
        status = "Success"
        error_msg = None
        try:
            print_step(f"Starting: {step_name}")
            step_func()
        except Exception as e:
            status = "Failed"
            error_msg = str(e)
            print_error(f"Step '{step_name}' failed: {e}")
            if not args.continue_on_error:
                sys.exit(1)
        finally:
            duration = time.time() - step_start
            results.append(
                {
                    "Step": step_name,
                    "Status": status,
                    "Duration": f"{duration:.2f}s",
                    "Error": error_msg or "",
                },
            )

    total_duration = time.time() - start_time

    print_header("POPULATION PIPELINE COMPLETE")

    # Print summary table
    table = Table(
        title="Pipeline Execution Summary",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Step", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Duration", style="green")

    for res in results:
        status_style = "green" if res["Status"] == "Success" else "red"
        table.add_row(
            res["Step"],
            f"[{status_style}]{res['Status']}[/{status_style}]",
            res["Duration"],
        )

    console.print(table)
    console.print(f"\n[bold]Total Duration:[/bold] {total_duration:.2f}s")


def main() -> None:
    """Parse CLI arguments for NBA population tasks."""
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
        "--force",
        action="store_true",
        help="Force recreate tables",
    )
    init_parser.add_argument("--tables", nargs="+", help="Specific tables to create")

    # info command
    subparsers.add_parser("info", help="Show database information")

    # load-csv command
    subparsers.add_parser("load-csv", help="Load CSV files into database")

    # normalize command
    subparsers.add_parser("normalize", help="Normalize database tables")

    # game-gold command
    subparsers.add_parser("game-gold", help="Create game_gold from game_silver")

    # gold-entities command
    subparsers.add_parser(
        "gold-entities",
        help="Create gold entity tables (player_gold, team_gold)",
    )

    # gold-tables command
    subparsers.add_parser(
        "gold-tables",
        help="Create canonical gold tables (games, team_game_stats, player_game_stats)",
    )

    # common-player-info command
    cpi_parser = subparsers.add_parser(
        "common-player-info",
        help="Fetch CommonPlayerInfo for all players",
    )
    cpi_parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only active players",
    )
    cpi_parser.add_argument("--limit", type=int, help="Limit number of players")
    cpi_parser.add_argument("--reset", action="store_true", help="Reset progress")
    cpi_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to database",
    )

    # draft-history command
    dh_parser = subparsers.add_parser("draft-history", help="Fetch draft history")
    dh_parser.add_argument("--season", help="Draft season year (YYYY)")
    dh_parser.add_argument("--reset", action="store_true", help="Reset progress")
    dh_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to database",
    )

    # draft-combine command
    dc_parser = subparsers.add_parser(
        "draft-combine",
        help="Fetch draft combine stats",
    )
    dc_parser.add_argument("--seasons", nargs="+", help="Season list (YYYY-YY)")
    dc_parser.add_argument("--reset", action="store_true", help="Reset progress")
    dc_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to database",
    )

    # team-info-common command
    tic_parser = subparsers.add_parser(
        "team-info-common",
        help="Fetch team info common",
    )
    tic_parser.add_argument("--seasons", nargs="+", help="Season list (YYYY-YY)")
    tic_parser.add_argument("--season-type", help="Season type (Regular Season)")
    tic_parser.add_argument("--reset", action="store_true", help="Reset progress")
    tic_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to database",
    )

    # team-details command
    td_parser = subparsers.add_parser("team-details", help="Fetch team details")
    td_parser.add_argument("--reset", action="store_true", help="Reset progress")
    td_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to database",
    )

    # player-games command
    pg_parser = subparsers.add_parser(
        "player-games",
        help="Fetch player game stats (bulk endpoint)",
    )
    pg_parser.add_argument("--seasons", nargs="+", help="Seasons to fetch")
    pg_parser.add_argument(
        "--delay",
        type=float,
        default=0.6,
        help="API delay in seconds",
    )
    pg_parser.add_argument(
        "--regular-only",
        action="store_true",
        help="Regular season only",
    )
    pg_parser.add_argument("--playoffs-only", action="store_true", help="Playoffs only")
    pg_parser.add_argument("--reset", action="store_true", help="Reset progress")
    pg_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to database",
    )

    # league-games command
    lg_parser = subparsers.add_parser(
        "league-games",
        help="Fetch league game logs (team-level) into game table",
    )
    lg_parser.add_argument("--seasons", nargs="+", help="Seasons to fetch")
    lg_parser.add_argument(
        "--delay",
        type=float,
        default=0.6,
        help="API delay in seconds",
    )
    lg_parser.add_argument(
        "--regular-only",
        action="store_true",
        help="Regular season only",
    )
    lg_parser.add_argument("--playoffs-only", action="store_true", help="Playoffs only")
    lg_parser.add_argument("--reset", action="store_true", help="Reset progress")
    lg_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to database",
    )

    # player-games-legacy command
    pgl_parser = subparsers.add_parser(
        "player-games-legacy",
        help="Fetch player game stats (per-player endpoint)",
    )
    pgl_parser.add_argument("--seasons", nargs="+", help="Seasons to fetch")
    pgl_parser.add_argument(
        "--active-only",
        action="store_true",
        help="Active players only",
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

    # metrics command
    subparsers.add_parser("metrics", help="Create advanced analytics metrics")

    # validate command
    subparsers.add_parser("validate", help="Run integrity checks")

    # br-box-scores command (Basketball Reference)
    br_box_parser = subparsers.add_parser(
        "br-box-scores",
        help="Fetch player box scores from Basketball Reference (pre-1996)",
    )
    br_box_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    br_box_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    br_box_parser.add_argument(
        "--start-year",
        type=int,
        default=1980,
        help="Start year for auto detection",
    )
    br_box_parser.add_argument(
        "--end-year",
        type=int,
        default=1996,
        help="End year for auto detection",
    )
    br_box_parser.add_argument("--limit", type=int, help="Limit number of dates")
    br_box_parser.add_argument(
        "--delay",
        type=float,
        help="Override BR request delay (seconds)",
    )
    br_box_parser.add_argument("--reset", action="store_true", help="Reset progress")
    br_box_parser.add_argument(
        "--dry-run", action="store_true", help="Don't write to DB"
    )

    # br-season-stats command (Basketball Reference)
    br_season_parser = subparsers.add_parser(
        "br-season-stats",
        help="Fetch season stats from Basketball Reference (pre-1996)",
    )
    br_season_parser.add_argument(
        "--seasons",
        nargs="+",
        help="Specific seasons (end years, e.g., 1995 1994)",
    )
    br_season_parser.add_argument(
        "--start-year",
        type=int,
        default=1980,
        help="Start year",
    )
    br_season_parser.add_argument(
        "--end-year",
        type=int,
        default=1996,
        help="End year",
    )
    br_season_parser.add_argument(
        "--basic-only",
        action="store_true",
        help="Skip advanced stats",
    )
    br_season_parser.add_argument("--reset", action="store_true", help="Reset progress")
    br_season_parser.add_argument(
        "--dry-run", action="store_true", help="Don't write to DB"
    )

    # all command
    all_parser = subparsers.add_parser("all", help="Run full population pipeline")
    all_parser.add_argument("--skip-api", action="store_true", help="Skip API fetching")
    all_parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue on errors",
    )
    all_parser.add_argument("--seasons", nargs="+", help="Seasons for API fetch")
    all_parser.add_argument("--delay", type=float, default=0.6, help="API delay")
    all_parser.add_argument("--regular-only", action="store_true")
    all_parser.add_argument("--playoffs-only", action="store_true")
    all_parser.add_argument(
        "--force",
        action="store_true",
        help="Force recreate tables",
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
        "game-gold": cmd_game_gold,
        "gold-entities": cmd_gold_entities,
        "gold-tables": cmd_gold_tables,
        "common-player-info": cmd_common_player_info,
        "draft-history": cmd_draft_history,
        "draft-combine": cmd_draft_combine,
        "team-info-common": cmd_team_info_common,
        "team-details": cmd_team_details,
        "player-games": cmd_player_games,
        "league-games": cmd_league_games,
        "player-games-legacy": cmd_player_games_legacy,
        "play-by-play": cmd_play_by_play,
        "season-stats": cmd_season_stats,
        "metrics": cmd_metrics,
        "validate": cmd_validate,
        "br-box-scores": cmd_br_box_scores,
        "br-season-stats": cmd_br_season_stats,
        "all": cmd_all,
    }

    handler = handlers.get(args.command)
    if handler:
        try:
            result = handler(args)
            if isinstance(result, dict) and result.get("error_count", 0) > 0:
                sys.exit(1)
        except KeyboardInterrupt:
            print_warning("Interrupted by user")
            sys.exit(1)
        except Exception as e:
            print_error(f"Command failed: {e}")
            if args.verbose:
                import traceback

                traceback.print_exc()
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
