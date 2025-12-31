#!/usr/bin/env python3
"""Populate draft_combine_stats table from NBA API."""

from __future__ import annotations

import argparse
import logging
from typing import Any

import pandas as pd

from src.scripts.populate.api_client import get_client
from src.scripts.populate.base import BasePopulator
from src.scripts.populate.config import ALL_SEASONS, get_db_path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


EXPECTED_COLUMNS = [
    "season",
    "player_id",
    "first_name",
    "last_name",
    "player_name",
    "position",
    "height_wo_shoes",
    "height_wo_shoes_ft_in",
    "height_w_shoes",
    "height_w_shoes_ft_in",
    "weight",
    "wingspan",
    "wingspan_ft_in",
    "standing_reach",
    "standing_reach_ft_in",
    "body_fat_pct",
    "hand_length",
    "hand_width",
    "standing_vertical_leap",
    "max_vertical_leap",
    "lane_agility_time",
    "modified_lane_agility_time",
    "three_quarter_sprint",
    "bench_press",
    "spot_fifteen_corner_left",
    "spot_fifteen_break_left",
    "spot_fifteen_top_key",
    "spot_fifteen_break_right",
    "spot_fifteen_corner_right",
    "spot_college_corner_left",
    "spot_college_break_left",
    "spot_college_top_key",
    "spot_college_break_right",
    "spot_college_corner_right",
    "spot_nba_corner_left",
    "spot_nba_break_left",
    "spot_nba_top_key",
    "spot_nba_break_right",
    "spot_nba_corner_right",
    "off_drib_fifteen_break_left",
    "off_drib_fifteen_top_key",
    "off_drib_fifteen_break_right",
    "off_drib_college_break_left",
    "off_drib_college_top_key",
    "off_drib_college_break_right",
    "on_move_fifteen",
    "on_move_college",
    "filename",
]


class DraftCombineStatsPopulator(BasePopulator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._fetched_keys: list[str] = []

    def get_table_name(self) -> str:
        return "draft_combine_stats"

    def get_key_columns(self) -> list[str]:
        return ["season", "player_id"]

    def get_expected_columns(self) -> list[str]:
        return EXPECTED_COLUMNS

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        seasons: list[str] = kwargs.get("seasons") or [
            s for s in ALL_SEASONS if int(s.split("-")[0]) >= 2000
        ]
        resume = kwargs.get("resume", True)

        data_frames: list[pd.DataFrame] = []

        for season in seasons:
            if resume and self.progress.is_completed(season):
                continue

            df = self.client.get_draft_combine_stats(season=season)
            if df is None or df.empty:
                continue

            data_frames.append(df)
            self._fetched_keys.append(season)

        if not data_frames:
            return None

        return pd.concat(data_frames, ignore_index=True)

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if df.empty:
            return df

        output = df.copy()
        output.columns = [col.lower() for col in output.columns]
        output["filename"] = "nba_api.draftcombinestats"

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


def populate_draft_combine_stats(
    db_path: str | None = None,
    seasons: list[str] | None = None,
    reset_progress: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    client = get_client()
    populator = DraftCombineStatsPopulator(db_path=db_path or str(get_db_path()), client=client)
    return populator.run(
        seasons=seasons,
        reset_progress=reset_progress,
        dry_run=dry_run,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate draft_combine_stats from NBA API")
    parser.add_argument("--db", default=None, help="Database path")
    parser.add_argument("--seasons", nargs="+", help="Season list (e.g., 2024-25 2023-24)")
    parser.add_argument("--reset-progress", action="store_true", help="Reset progress")
    parser.add_argument("--dry-run", action="store_true", help="Skip database writes")
    args = parser.parse_args()

    populate_draft_combine_stats(
        db_path=args.db,
        seasons=args.seasons,
        reset_progress=args.reset_progress,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
