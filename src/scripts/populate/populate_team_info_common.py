#!/usr/bin/env python3
"""Populate team_info_common table from NBA API."""

from __future__ import annotations

import argparse
import logging
import time
from typing import Any

import pandas as pd

from src.scripts.populate.api_client import get_client
from src.scripts.populate.base import BasePopulator
from src.scripts.populate.config import (
    CURRENT_SEASON,
    DEFAULT_SEASON_TYPES,
    get_db_path,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


EXPECTED_COLUMNS = [
    "team_id",
    "season_year",
    "team_city",
    "team_name",
    "team_abbreviation",
    "team_conference",
    "team_division",
    "team_code",
    "team_slug",
    "w",
    "l",
    "pct",
    "conf_rank",
    "div_rank",
    "min_year",
    "max_year",
    "league_id",
    "season_id",
    "pts_rank",
    "pts_pg",
    "reb_rank",
    "reb_pg",
    "ast_rank",
    "ast_pg",
    "opp_pts_rank",
    "opp_pts_pg",
    "filename",
]


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    minutes, secs = divmod(total_seconds, 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s" if minutes else f"{secs}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{secs:02d}s"


class TeamInfoCommonPopulator(BasePopulator):
    """Populate team info common data from the NBA API."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._fetched_keys: list[str] = []

    def get_table_name(self) -> str:
        return "team_info_common"

    def get_key_columns(self) -> list[str]:
        return ["team_id", "season_year"]

    def get_expected_columns(self) -> list[str]:
        return EXPECTED_COLUMNS

    def _load_team_ids(self) -> list[int]:
        conn = self.connect()
        for table_name in ("team_gold", "team_silver", "team_raw", "team"):
            try:
                rows = conn.execute(f"SELECT DISTINCT id FROM {table_name}").fetchall()
                team_ids = [int(row[0]) for row in rows if row and row[0] is not None]
                if team_ids:
                    return team_ids
            except Exception:
                continue

        teams = self.client.get_all_teams()
        return [team["id"] for team in teams if "id" in team]

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        seasons: list[str] = kwargs.get("seasons") or [CURRENT_SEASON]
        season_type = kwargs.get("season_type") or DEFAULT_SEASON_TYPES[0]
        resume = kwargs.get("resume", True)
        log_every = max(1, int(kwargs.get("log_every", 10)))

        if any(season != CURRENT_SEASON for season in seasons):
            logger.warning(
                "TeamInfoCommon returns current season only; using %s instead of %s",
                CURRENT_SEASON,
                seasons,
            )
            seasons = [CURRENT_SEASON]

        team_ids = self._load_team_ids()
        data_frames: list[pd.DataFrame] = []

        total_requests = len(team_ids) * len(seasons)
        if total_requests == 0:
            return None

        logger.info(
            "Fetching team_info_common for %s teams x %s seasons (%s requests)",
            len(team_ids),
            len(seasons),
            total_requests,
        )
        logger.info(
            "Season type: %s | timeout=%s | delay=%s | max_retries=%s",
            season_type,
            self.client.config.timeout,
            self.client.config.request_delay,
            self.client.config.max_retries,
        )
        logger.info(
            "Estimated minimum runtime (delay only): %s",
            _format_duration(total_requests * self.client.config.request_delay),
        )

        start_time = time.monotonic()
        processed = 0
        fetched = 0
        skipped = 0
        errors = 0

        def log_progress(force: bool = False) -> None:
            if not force and processed % log_every != 0:
                return
            elapsed = time.monotonic() - start_time
            avg = elapsed / processed if processed else 0
            remaining = (total_requests - processed) * avg
            pct = (processed / total_requests) * 100 if total_requests else 0.0
            logger.info(
                "Progress: %s/%s (%.1f%%) | fetched=%s skipped=%s errors=%s | elapsed=%s eta=%s",
                processed,
                total_requests,
                pct,
                fetched,
                skipped,
                errors,
                _format_duration(elapsed),
                _format_duration(remaining),
            )

        for team_id in team_ids:
            for season in seasons:
                progress_key = f"{team_id}_{season}"
                processed += 1
                if resume and self.progress.is_completed(progress_key):
                    skipped += 1
                    log_progress()
                    continue

                logger.info(
                    "[%s/%s] Fetching team_id=%s season=%s",
                    processed,
                    total_requests,
                    team_id,
                    season,
                )
                call_start = time.monotonic()
                self.metrics.api_calls += 1
                try:
                    data = self.client.get_team_info_common(
                        team_id=team_id,
                        season=season,
                        season_type=season_type,
                    )
                except Exception as exc:
                    errors += 1
                    self.progress.add_error(progress_key, str(exc))
                    logger.warning(
                        "[%s/%s] Failed team_id=%s season=%s after %.1fs: %s",
                        processed,
                        total_requests,
                        team_id,
                        season,
                        time.monotonic() - call_start,
                        exc,
                    )
                    log_progress()
                    continue

                info_df = data.get("team_info_common", pd.DataFrame())
                if info_df.empty:
                    logger.info(
                        "[%s/%s] No data for team_id=%s season=%s (%.1fs)",
                        processed,
                        total_requests,
                        team_id,
                        season,
                        time.monotonic() - call_start,
                    )
                    log_progress()
                    continue

                ranks_df = data.get("team_season_ranks", pd.DataFrame())
                row = info_df.iloc[0].to_dict()
                if not ranks_df.empty:
                    row.update(ranks_df.iloc[0].to_dict())

                data_frames.append(pd.DataFrame([row]))
                self._fetched_keys.append(progress_key)
                fetched += 1
                logger.info(
                    "[%s/%s] Added row for team_id=%s season=%s (%.1fs)",
                    processed,
                    total_requests,
                    team_id,
                    season,
                    time.monotonic() - call_start,
                )
                log_progress()

        log_progress(force=True)

        if not data_frames:
            return None

        return pd.concat(data_frames, ignore_index=True)

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if df.empty:
            return df

        output = df.copy()
        output.columns = [col.lower() for col in output.columns]
        output["filename"] = "nba_api.teaminfocommon"

        for col in EXPECTED_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return output[EXPECTED_COLUMNS]

    def post_run_hook(self, **kwargs) -> None:
        if kwargs.get("dry_run", False):
            logger.info("DRY RUN - not marking progress")
            return

        for key in self._fetched_keys:
            self.progress.mark_completed(key)
        self.progress.save()


def populate_team_info_common(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    season_type: str | None = None,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    client = get_client()
    populator = TeamInfoCommonPopulator(
        db_path=db_path or str(get_db_path()), client=client
    )
    return populator.run(
        seasons=seasons,
        season_type=season_type,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Populate team_info_common from NBA API"
    )
    parser.add_argument("--db", default=None, help="Database path")
    parser.add_argument(
        "--seasons", nargs="+", help="Season list (e.g., 2024-25 2023-24)"
    )
    parser.add_argument(
        "--season-type", default=None, help="Season type (default: Regular Season)"
    )
    parser.add_argument("--reset-progress", action="store_true", help="Reset progress")
    parser.add_argument("--dry-run", action="store_true", help="Skip database writes")
    args = parser.parse_args()

    populate_team_info_common(
        db_path=args.db,
        seasons=args.seasons,
        season_type=args.season_type,
        reset_progress=args.reset_progress,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
