#!/usr/bin/env python3
"""Populate player_awards table from NBA API."""

from __future__ import annotations

import argparse
import logging
import time
from typing import Any

import pandas as pd

from src.scripts.populate.api_client import get_client
from src.scripts.populate.base import BasePopulator, ProgressMixin
from src.scripts.populate.config import get_db_path
from src.scripts.populate.helpers import configure_logging, format_duration


configure_logging()
logger = logging.getLogger(__name__)


EXPECTED_COLUMNS = [
    "player_id",
    "person_id",
    "first_name",
    "last_name",
    "team",
    "description",
    "all_nba_team_number",
    "season",
    "month",
    "week",
    "conference",
    "type",
    "subtype",
]


class PlayerAwardsPopulator(BasePopulator, ProgressMixin):
    """Populate player_awards with per-player API calls."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._fetched_keys: list[str] = []

    def get_table_name(self) -> str:
        return "player_awards"

    def get_key_columns(self) -> list[str]:
        return ["player_id", "season", "award_type"]

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
        for table_name in ("player_awards_raw", "player_awards"):
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT player_id FROM {table_name}",
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
            "Fetching player_awards for %s players (active_only=%s, limit=%s)",
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
            format_duration(total_requests * self.client.config.request_delay),
        )

        start_time = time.monotonic()
        processed = 0
        fetched = 0
        skipped = 0
        errors = 0
        no_awards = 0

        def log_progress(force: bool = False) -> None:
            if not force and processed % log_every != 0:
                return
            elapsed = time.monotonic() - start_time
            avg = elapsed / processed if processed else 0
            remaining = (total_requests - processed) * avg
            pct = (processed / total_requests) * 100 if total_requests else 0.0
            logger.info(
                "Progress: %s/%s (%.1f%%) | fetched=%s no_awards=%s skipped=%s errors=%s | elapsed=%s eta=%s",
                processed,
                total_requests,
                pct,
                fetched,
                no_awards,
                skipped,
                errors,
                format_duration(elapsed),
                format_duration(remaining),
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

            logger.debug(
                "[%s/%s] Fetching player_id=%s", processed, total_requests, player_id
            )
            call_start = time.monotonic()
            self.metrics.api_calls += 1
            try:
                df = self.client.get_player_awards(player_id)
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
                no_awards += 1
                logger.debug(
                    "[%s/%s] No awards for player_id=%s (%.1fs)",
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
            logger.debug(
                "[%s/%s] Added player_id=%s (%.1fs)",
                processed,
                total_requests,
                player_id,
                time.monotonic() - call_start,
            )
            log_progress()

            if idx % 100 == 0:
                logger.info("Fetched %s/%s players (%s with awards)", idx, len(player_ids), fetched)

        if not data_frames:
            logger.info("No player awards data found.")
            return None

        log_progress(force=True)

        return pd.concat(data_frames, ignore_index=True)

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if df.empty:
            return df

        output = df.copy()
        output.columns = [col.lower() for col in output.columns]

        # Add player_id column if not present (should be person_id)
        if "player_id" not in output.columns and "person_id" in output.columns:
            output["player_id"] = output["person_id"]

        # Create award_type from description for uniqueness
        if "description" in output.columns:
            output["award_type"] = output["description"]
        else:
            output["award_type"] = None

        for col in EXPECTED_COLUMNS:
            if col not in output.columns:
                output[col] = None

        # Ensure key columns are included
        result_columns = list(dict.fromkeys(["player_id", "season", "award_type"] + EXPECTED_COLUMNS))
        return output[result_columns]

    def post_run_hook(self, **kwargs) -> None:
        if kwargs.get("dry_run", False):
            logger.info("DRY RUN - not marking progress")
            return

        if self._fetched_keys:
            for key in self._fetched_keys:
                self.progress.mark_completed(key)
            self.progress.save()


def populate_player_awards(
    db_path: str | None = None,
    active_only: bool = False,
    limit: int | None = None,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    client = get_client()
    populator = PlayerAwardsPopulator(
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
        description="Populate player_awards from NBA API",
    )
    parser.add_argument("--db", default=None, help="Database path")
    parser.add_argument(
        "--active-only", action="store_true", help="Only active players"
    )
    parser.add_argument("--limit", type=int, help="Limit number of players")
    parser.add_argument("--reset-progress", action="store_true", help="Reset progress")
    parser.add_argument("--dry-run", action="store_true", help="Skip database writes")

    args = parser.parse_args()

    populate_player_awards(
        db_path=args.db,
        active_only=args.active_only,
        limit=args.limit,
        reset_progress=args.reset_progress,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
