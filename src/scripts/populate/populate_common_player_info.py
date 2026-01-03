#!/usr/bin/env python3
"""Populate common_player_info table from NBA API."""

from __future__ import annotations

import argparse
import logging
import time
from typing import Any

import pandas as pd

from src.scripts.populate.api_client import get_client
from src.scripts.populate.base import BasePopulator
from src.scripts.populate.config import get_db_path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


EXPECTED_COLUMNS = [
    "person_id",
    "first_name",
    "last_name",
    "display_first_last",
    "display_last_comma_first",
    "display_fi_last",
    "player_slug",
    "birthdate",
    "school",
    "country",
    "last_affiliation",
    "height",
    "weight",
    "season_exp",
    "jersey",
    "position",
    "rosterstatus",
    "games_played_current_season_flag",
    "team_id",
    "team_name",
    "team_abbreviation",
    "team_code",
    "team_city",
    "playercode",
    "from_year",
    "to_year",
    "dleague_flag",
    "nba_flag",
    "games_played_flag",
    "draft_year",
    "draft_round",
    "draft_number",
    "greatest_75_flag",
    "filename",
]


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    minutes, secs = divmod(total_seconds, 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s" if minutes else f"{secs}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{secs:02d}s"


class CommonPlayerInfoPopulator(BasePopulator):
    """Populate common_player_info with per-player API calls."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._fetched_keys: list[str] = []

    def get_table_name(self) -> str:
        return "common_player_info"

    def get_key_columns(self) -> list[str]:
        return ["person_id"]

    def get_expected_columns(self) -> list[str]:
        return EXPECTED_COLUMNS

    def _load_player_ids(
        self,
        active_only: bool = False,
        limit: int | None = None,
    ) -> list[int]:
        conn = self.connect()
        player_ids: list[int] = []

        rows = []
        for table_name in ("player_gold", "player_silver", "player_raw", "player"):
            try:
                rows = conn.execute(
                    f"SELECT id, is_active FROM {table_name}",
                ).fetchall()
                if rows:
                    break
            except Exception:
                rows = []

        if rows:
            active_values = {"true", "t", "1", "yes", "y"}
            for player_id, is_active in rows:
                if player_id is None:
                    continue
                if active_only and str(is_active).strip().lower() not in active_values:
                    continue
                try:
                    player_ids.append(int(player_id))
                except (TypeError, ValueError):
                    continue
        else:
            players = (
                self.client.get_active_players()
                if active_only
                else self.client.get_all_players()
            )
            player_ids = [p["id"] for p in players if "id" in p]

        if limit:
            return player_ids[:limit]
        return player_ids

    def _load_existing_ids(self) -> set[int]:
        conn = self.connect()
        for table_name in ("common_player_info_raw", "common_player_info"):
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT person_id FROM {table_name}",
                ).fetchall()
                return {int(row[0]) for row in rows if row and row[0] is not None}
            except Exception:
                continue
        return set()

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        active_only = kwargs.get("active_only", False)
        limit = kwargs.get("limit")
        resume = kwargs.get("resume", True)
        log_every = max(1, int(kwargs.get("log_every", 100)))

        player_ids = self._load_player_ids(active_only=active_only, limit=limit)
        if not player_ids:
            logger.warning("No player IDs found to fetch.")
            return None

        existing_ids = self._load_existing_ids()
        if existing_ids:
            player_ids = [pid for pid in player_ids if pid not in existing_ids]
        data_frames: list[pd.DataFrame] = []

        total_requests = len(player_ids)
        logger.info(
            "Fetching common_player_info for %s players (active_only=%s, limit=%s)",
            total_requests,
            active_only,
            limit,
        )
        logger.info(
            "timeout=%s | delay=%s | max_retries=%s",
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

        for idx, player_id in enumerate(player_ids, start=1):
            processed += 1
            if player_id in existing_ids:
                skipped += 1
                log_progress()
                continue
            if resume and self.progress.is_completed(str(player_id)):
                skipped += 1
                log_progress()
                continue

            logger.info(
                "[%s/%s] Fetching player_id=%s", processed, total_requests, player_id
            )
            call_start = time.monotonic()
            self.metrics.api_calls += 1
            try:
                df = self.client.get_common_player_info(player_id)
            except Exception as exc:
                errors += 1
                self.progress.add_error(str(player_id), str(exc))
                logger.warning(
                    "[%s/%s] Failed player_id=%s after %.1fs: %s",
                    processed,
                    total_requests,
                    player_id,
                    time.monotonic() - call_start,
                    exc,
                )
                log_progress()
                continue
            if df is None or df.empty:
                logger.info(
                    "[%s/%s] No data for player_id=%s (%.1fs)",
                    processed,
                    total_requests,
                    player_id,
                    time.monotonic() - call_start,
                )
                log_progress()
                continue

            data_frames.append(df)
            self._fetched_keys.append(str(player_id))
            fetched += 1
            logger.info(
                "[%s/%s] Added player_id=%s (%.1fs)",
                processed,
                total_requests,
                player_id,
                time.monotonic() - call_start,
            )
            log_progress()

            if idx % 100 == 0:
                logger.info("Fetched %s/%s players", idx, len(player_ids))

        if not data_frames:
            return None

        log_progress(force=True)

        return pd.concat(data_frames, ignore_index=True)

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if df.empty:
            return df

        output = df.copy()
        output.columns = [col.lower() for col in output.columns]
        output["filename"] = "nba_api.commonplayerinfo"

        for col in EXPECTED_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return output[EXPECTED_COLUMNS]

    def post_run_hook(self, **kwargs) -> None:
        if kwargs.get("dry_run", False):
            logger.info("DRY RUN - not marking progress")
            return

        if self._fetched_keys:
            for key in self._fetched_keys:
                self.progress.mark_completed(key)
            self.progress.save()


def populate_common_player_info(
    db_path: str | None = None,
    active_only: bool = False,
    limit: int | None = None,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    client = get_client()
    populator = CommonPlayerInfoPopulator(
        db_path=db_path or str(get_db_path()), client=client
    )
    return populator.run(
        active_only=active_only,
        limit=limit,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Populate common_player_info from NBA API",
    )
    parser.add_argument("--db", default=None, help="Database path")
    parser.add_argument(
        "--active-only", action="store_true", help="Only active players"
    )
    parser.add_argument("--limit", type=int, help="Limit number of players")
    parser.add_argument("--reset-progress", action="store_true", help="Reset progress")
    parser.add_argument("--dry-run", action="store_true", help="Skip database writes")

    args = parser.parse_args()

    populate_common_player_info(
        db_path=args.db,
        active_only=args.active_only,
        limit=args.limit,
        reset_progress=args.reset_progress,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
