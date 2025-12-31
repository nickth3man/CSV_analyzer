#!/usr/bin/env python3
"""Populate common_player_info table from NBA API."""

from __future__ import annotations

import argparse
import logging
from typing import Any

import duckdb
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

        try:
            rows = conn.execute(
                "SELECT id, is_active FROM player",
            ).fetchall()
        except duckdb.CatalogException:
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
            players = self.client.get_active_players() if active_only else self.client.get_all_players()
            player_ids = [p["id"] for p in players if "id" in p]

        if limit:
            return player_ids[:limit]
        return player_ids

    def _load_existing_ids(self) -> set[int]:
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT DISTINCT person_id FROM common_player_info",
            ).fetchall()
            return {int(row[0]) for row in rows if row and row[0] is not None}
        except duckdb.CatalogException:
            return set()

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        active_only = kwargs.get("active_only", False)
        limit = kwargs.get("limit")
        resume = kwargs.get("resume", True)

        player_ids = self._load_player_ids(active_only=active_only, limit=limit)
        if not player_ids:
            logger.warning("No player IDs found to fetch.")
            return None

        existing_ids = self._load_existing_ids()
        data_frames: list[pd.DataFrame] = []

        for idx, player_id in enumerate(player_ids, start=1):
            if player_id in existing_ids:
                continue
            if resume and self.progress.is_completed(str(player_id)):
                continue

            df = self.client.get_common_player_info(player_id)
            if df is None or df.empty:
                continue

            data_frames.append(df)
            self._fetched_keys.append(str(player_id))

            if idx % 100 == 0:
                logger.info("Fetched %s/%s players", idx, len(player_ids))

        if not data_frames:
            return None

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
    populator = CommonPlayerInfoPopulator(db_path=db_path or str(get_db_path()), client=client)
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
    parser.add_argument("--active-only", action="store_true", help="Only active players")
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
