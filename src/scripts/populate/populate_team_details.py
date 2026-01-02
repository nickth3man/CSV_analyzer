#!/usr/bin/env python3
"""Populate team_details table from NBA API."""

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
    "team_id",
    "abbreviation",
    "nickname",
    "yearfounded",
    "city",
    "arena",
    "arenacapacity",
    "owner",
    "generalmanager",
    "headcoach",
    "dleagueaffiliation",
    "facebook",
    "instagram",
    "twitter",
    "filename",
]


class TeamDetailsPopulator(BasePopulator):
    """Populate team details data from the NBA API."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._fetched_keys: list[str] = []

    def get_table_name(self) -> str:
        return "team_details"

    def get_key_columns(self) -> list[str]:
        return ["team_id"]

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
        resume = kwargs.get("resume", True)
        team_ids = self._load_team_ids()
        data_frames: list[pd.DataFrame] = []

        for team_id in team_ids:
            if resume and self.progress.is_completed(str(team_id)):
                continue

            data = self.client.get_team_details(team_id)
            details_df = data.get("team_details", pd.DataFrame())
            if details_df.empty:
                continue

            row = details_df.iloc[0].to_dict()
            social_df = data.get("team_social", pd.DataFrame())
            if not social_df.empty:
                social_links = {
                    str(r.get("ACCOUNTTYPE", "")).strip().lower(): r.get("WEBSITE_LINK")
                    for r in social_df.to_dict(orient="records")
                }
                row["FACEBOOK"] = social_links.get("facebook")
                row["INSTAGRAM"] = social_links.get("instagram")
                row["TWITTER"] = social_links.get("twitter")

            data_frames.append(pd.DataFrame([row]))
            self._fetched_keys.append(str(team_id))

        if not data_frames:
            return None

        return pd.concat(data_frames, ignore_index=True)

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if df.empty:
            return df

        output = df.copy()
        output.columns = [col.lower() for col in output.columns]
        output["filename"] = "nba_api.teamdetails"

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


def populate_team_details(
    db_path: str | None = None,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    client = get_client()
    populator = TeamDetailsPopulator(
        db_path=db_path or str(get_db_path()), client=client
    )
    return populator.run(
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate team_details from NBA API")
    parser.add_argument("--db", default=None, help="Database path")
    parser.add_argument("--reset-progress", action="store_true", help="Reset progress")
    parser.add_argument("--dry-run", action="store_true", help="Skip database writes")
    args = parser.parse_args()

    populate_team_details(
        db_path=args.db,
        reset_progress=args.reset_progress,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
