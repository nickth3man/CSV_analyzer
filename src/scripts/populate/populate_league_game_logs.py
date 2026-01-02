#!/usr/bin/env python3
"""Populate the game table using the LeagueGameLog endpoint.

This script fetches team-level game logs and pivots them into a single
row per game with home/away columns matching the `game_raw` table schema.
"""

import argparse
import logging
import sys
import time
from typing import Any

import pandas as pd

from src.scripts.populate.base import BasePopulator
from src.scripts.populate.config import (
    ALL_SEASONS,
    DEFAULT_SEASON_TYPES,
    LEAGUE_GAME_LOG_FIELDS,
    get_db_path,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


GAME_TABLE_COLUMNS = [
    "season_id",
    "team_id_home",
    "team_abbreviation_home",
    "team_name_home",
    "game_id",
    "game_date",
    "matchup_home",
    "wl_home",
    "min",
    "fgm_home",
    "fga_home",
    "fg_pct_home",
    "fg3m_home",
    "fg3a_home",
    "fg3_pct_home",
    "ftm_home",
    "fta_home",
    "ft_pct_home",
    "oreb_home",
    "dreb_home",
    "reb_home",
    "ast_home",
    "stl_home",
    "blk_home",
    "tov_home",
    "pf_home",
    "pts_home",
    "plus_minus_home",
    "video_available_home",
    "team_id_away",
    "team_abbreviation_away",
    "team_name_away",
    "matchup_away",
    "wl_away",
    "fgm_away",
    "fga_away",
    "fg_pct_away",
    "fg3m_away",
    "fg3a_away",
    "fg3_pct_away",
    "ftm_away",
    "fta_away",
    "ft_pct_away",
    "oreb_away",
    "dreb_away",
    "reb_away",
    "ast_away",
    "stl_away",
    "blk_away",
    "tov_away",
    "pf_away",
    "pts_away",
    "plus_minus_away",
    "video_available_away",
    "season_type",
    "filename",
]

COMMON_COLUMN_MAP = {
    "GAME_ID": "game_id",
    "SEASON_ID": "season_id",
    "GAME_DATE": "game_date",
    "MIN": "min",
    "_season_type": "season_type",
}

HOME_COLUMN_MAP = {
    "GAME_ID": "game_id",
    "TEAM_ID": "team_id_home",
    "TEAM_ABBREVIATION": "team_abbreviation_home",
    "TEAM_NAME": "team_name_home",
    "MATCHUP": "matchup_home",
    "WL": "wl_home",
    "FGM": "fgm_home",
    "FGA": "fga_home",
    "FG_PCT": "fg_pct_home",
    "FG3M": "fg3m_home",
    "FG3A": "fg3a_home",
    "FG3_PCT": "fg3_pct_home",
    "FTM": "ftm_home",
    "FTA": "fta_home",
    "FT_PCT": "ft_pct_home",
    "OREB": "oreb_home",
    "DREB": "dreb_home",
    "REB": "reb_home",
    "AST": "ast_home",
    "STL": "stl_home",
    "BLK": "blk_home",
    "TOV": "tov_home",
    "PF": "pf_home",
    "PTS": "pts_home",
    "PLUS_MINUS": "plus_minus_home",
    "VIDEO_AVAILABLE": "video_available_home",
}

AWAY_COLUMN_MAP = {
    "GAME_ID": "game_id",
    "TEAM_ID": "team_id_away",
    "TEAM_ABBREVIATION": "team_abbreviation_away",
    "TEAM_NAME": "team_name_away",
    "MATCHUP": "matchup_away",
    "WL": "wl_away",
    "FGM": "fgm_away",
    "FGA": "fga_away",
    "FG_PCT": "fg_pct_away",
    "FG3M": "fg3m_away",
    "FG3A": "fg3a_away",
    "FG3_PCT": "fg3_pct_away",
    "FTM": "ftm_away",
    "FTA": "fta_away",
    "FT_PCT": "ft_pct_away",
    "OREB": "oreb_away",
    "DREB": "dreb_away",
    "REB": "reb_away",
    "AST": "ast_away",
    "STL": "stl_away",
    "BLK": "blk_away",
    "TOV": "tov_away",
    "PF": "pf_away",
    "PTS": "pts_away",
    "PLUS_MINUS": "plus_minus_away",
    "VIDEO_AVAILABLE": "video_available_away",
}


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = None
    return df


class LeagueGameLogPopulator(BasePopulator):
    """Populate the raw game table using LeagueGameLog (team-level)."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._fetched_season_keys: list[str] = []

    def get_table_name(self) -> str:
        return "game"

    def get_key_columns(self) -> list[str]:
        return ["game_id"]

    def get_expected_columns(self) -> list[str] | None:
        return GAME_TABLE_COLUMNS

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        seasons = kwargs.get("seasons") or ALL_SEASONS
        season_types = kwargs.get("season_types") or DEFAULT_SEASON_TYPES
        delay = kwargs.get("delay")
        if delay is None:
            delay = self.client.config.request_delay

        all_data = []
        for season in seasons:
            for season_type in season_types:
                progress_key = f"{season}_{season_type}"
                if self.progress.is_completed(progress_key):
                    logger.info("Skipping %s %s (already completed)", season, season_type)
                    continue

                logger.info("Fetching %s %s...", season, season_type)
                df = self.client.get_league_game_log(
                    season=season,
                    season_type=season_type,
                    player_or_team="T",
                )
                self.metrics.api_calls += 1

                if df is not None and not df.empty:
                    df = df.copy()
                    df["_season_type"] = season_type
                    all_data.append(df)
                    self._fetched_season_keys.append(progress_key)
                    logger.info("  Found %s records", len(df))

                time.sleep(delay)

        if not all_data:
            return None

        return pd.concat(all_data, ignore_index=True)

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        df = _ensure_columns(df, LEAGUE_GAME_LOG_FIELDS + ["_season_type"])
        df["IS_HOME"] = df["MATCHUP"].astype(str).str.contains(
            r"\bvs\.?\b",
            case=False,
            na=False,
        )

        common = (
            df[list(COMMON_COLUMN_MAP.keys())]
            .drop_duplicates("GAME_ID")
            .rename(columns=COMMON_COLUMN_MAP)
        )
        common["game_id"] = common["game_id"].astype(str)
        common["season_id"] = common["season_id"].astype(str)
        common["game_date"] = common["game_date"].astype(str)

        home = (
            df[df["IS_HOME"]]
            .drop_duplicates("GAME_ID")
            .rename(columns=HOME_COLUMN_MAP)
        )
        away = (
            df[~df["IS_HOME"]]
            .drop_duplicates("GAME_ID")
            .rename(columns=AWAY_COLUMN_MAP)
        )

        home = home[["game_id"] + [c for c in HOME_COLUMN_MAP.values() if c != "game_id"]]
        away = away[["game_id"] + [c for c in AWAY_COLUMN_MAP.values() if c != "game_id"]]

        combined = common.merge(home, on="game_id", how="left").merge(
            away,
            on="game_id",
            how="left",
        )

        combined["filename"] = "leaguegamelog_api"
        combined = combined.reindex(columns=GAME_TABLE_COLUMNS)
        return combined

    def post_run_hook(self, **kwargs) -> None:
        if kwargs.get("dry_run", False):
            logger.info("DRY RUN - not marking progress")
            return
        for key in self._fetched_season_keys:
            self.progress.mark_completed(key)


def populate_league_game_logs(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    season_types: list[str] | None = None,
    delay: float | None = None,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Populate the raw game table using LeagueGameLog data."""
    db_path = db_path or str(get_db_path())
    populator = LeagueGameLogPopulator(db_path=db_path)
    return populator.run(
        reset_progress=reset_progress,
        dry_run=dry_run,
        seasons=seasons,
        season_types=season_types,
        delay=delay,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Populate game table using LeagueGameLog endpoint",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database (default: src/backend/data/nba.duckdb)",
    )
    parser.add_argument("--seasons", nargs="+", help="Seasons to fetch")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.6,
        help="API delay in seconds",
    )
    parser.add_argument(
        "--regular-only",
        action="store_true",
        help="Regular season only",
    )
    parser.add_argument("--playoffs-only", action="store_true", help="Playoffs only")
    parser.add_argument("--reset", action="store_true", help="Reset progress")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")

    args = parser.parse_args()

    season_types = DEFAULT_SEASON_TYPES
    if args.regular_only:
        season_types = ["Regular Season"]
    elif args.playoffs_only:
        season_types = ["Playoffs"]

    result = populate_league_game_logs(
        db_path=args.db,
        seasons=args.seasons,
        season_types=season_types,
        delay=args.delay,
        reset_progress=args.reset,
        dry_run=args.dry_run,
    )

    if isinstance(result, dict) and result.get("error_count", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
