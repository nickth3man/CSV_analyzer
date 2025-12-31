#!/usr/bin/env python3
"""Populate draft_history table from NBA API."""

from __future__ import annotations

import argparse
import logging
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
    "player_name",
    "season",
    "round_number",
    "round_pick",
    "overall_pick",
    "draft_type",
    "team_id",
    "team_city",
    "team_name",
    "team_abbreviation",
    "organization",
    "organization_type",
    "player_profile_flag",
    "filename",
]


class DraftHistoryPopulator(BasePopulator):
    def get_table_name(self) -> str:
        return "draft_history"

    def get_key_columns(self) -> list[str]:
        return ["person_id", "season"]

    def get_expected_columns(self) -> list[str]:
        return EXPECTED_COLUMNS

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        season = kwargs.get("season")
        df = self.client.get_draft_history(season=season)
        return df

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if df.empty:
            return df

        output = df.copy()
        output.columns = [col.lower() for col in output.columns]
        output["filename"] = "nba_api.drafthistory"

        for col in EXPECTED_COLUMNS:
            if col not in output.columns:
                output[col] = None

        return output[EXPECTED_COLUMNS]


def populate_draft_history(
    db_path: str | None = None,
    season: str | None = None,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    client = get_client()
    populator = DraftHistoryPopulator(db_path=db_path or str(get_db_path()), client=client)
    return populator.run(
        season=season,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate draft_history from NBA API")
    parser.add_argument("--db", default=None, help="Database path")
    parser.add_argument("--season", help="Filter by draft season year (YYYY)")
    parser.add_argument("--reset-progress", action="store_true", help="Reset progress")
    parser.add_argument("--dry-run", action="store_true", help="Skip database writes")
    args = parser.parse_args()

    populate_draft_history(
        db_path=args.db,
        season=args.season,
        reset_progress=args.reset_progress,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
