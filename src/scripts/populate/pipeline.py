"""Pipeline manager for NBA data ingestion using PocketFlow.

This module provides a unified pipeline to:
1. Fetch raw data (Raw Layer)
2. Normalize data types (Silver Layer)
3. Aggregate and deduplicate (Gold Layer)
4. Validate integrity and consistency

Usage:
    from src.scripts.populate.pipeline import create_nba_pipeline

    shared = {
        "db_path": "src/backend/data/nba.duckdb",
        "populators": [
            (PopulateTeamDetails, {}),
            (PopulateCommonPlayerInfo, {"active_only": True})
        ]
    }
    pipeline = create_nba_pipeline()
    pipeline.run(shared)
"""

import logging
from typing import Any

from pocketflow import BatchNode, Flow, Node

from src.scripts.analysis.create_advanced_metrics import create_advanced_metrics
from src.scripts.maintenance.check_integrity import check_integrity
from src.scripts.maintenance.create_gold_entities import create_gold_entities
from src.scripts.maintenance.create_gold_tables import create_gold_tables
from src.scripts.maintenance.deduplicate_silver import deduplicate_silver_tables
from src.scripts.maintenance.fix_game_duplicates import fix_duplicates
from src.scripts.maintenance.normalize_db import transform_to_silver
from src.scripts.populate.base import BasePopulator
from src.scripts.utils.ui import print_header, print_step, print_success


logger = logging.getLogger(__name__)


class FetchNode(BatchNode):
    """Node to fetch raw data using multiple populators."""

    def prep(
        self, shared: dict[str, Any]
    ) -> list[tuple[type[BasePopulator], dict[str, Any]]]:
        """Get the list of populators to run."""
        return shared.get("populators", [])

    def exec(self, prep_res: Any) -> Any:
        """Run a single populator."""
        populator_class, kwargs = prep_res
        db_path = self.params.get("db_path")
        db_path_str = str(db_path) if db_path is not None else None

        print_step(f"Running populator: {populator_class.__name__}")
        populator = populator_class(db_path=db_path_str)
        return populator.run(**kwargs)

    def post(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
        """Store results and move to normalization."""
        shared["fetch_results"] = exec_res
        print_success(f"Successfully ran {len(exec_res)} populators")
        return "default"


class NormalizeNode(Node):
    """Node to normalize raw tables into silver tables."""

    def exec(self, prep_res: Any) -> Any:
        """Run normalization logic."""
        db_path = self.params.get("db_path")
        db_path_str = str(db_path) if db_path is not None else None
        print_step("Normalizing database tables (Silver Layer)")
        transform_to_silver(db_path=db_path_str)

    def post(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
        """Move to gold layer."""
        print_success("Normalization complete")
        return "default"


class GoldNode(Node):
    """Node to create gold layer tables (deduplicated/aggregated)."""

    def exec(self, prep_res: Any) -> Any:
        """Run gold layer logic."""
        db_path = self.params.get("db_path")
        db_path_str = str(db_path) if db_path is not None else None

        print_step("Deduplicating Silver tables")
        deduplicate_silver_tables(db_path=db_path_str or "src/backend/data/nba.duckdb")

        print_step("Fixing game duplicates")
        fix_duplicates(db_path=db_path_str or "src/backend/data/nba.duckdb")

        print_step("Creating Gold entities (Teams, Players)")
        create_gold_entities(db_path=db_path_str or "src/backend/data/nba.duckdb")

        print_step("Creating canonical Gold tables")
        create_gold_tables(db_path=db_path_str or "src/backend/data/nba.duckdb")

        print_step("Creating advanced metrics")
        create_advanced_metrics(db_path=db_path_str or "src/backend/data/nba.duckdb")

    def post(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
        """Move to integrity check."""
        print_success("Gold layer creation complete")
        return "default"


class IntegrityNode(Node):
    """Node to run final integrity checks."""

    def exec(self, prep_res: Any) -> dict[str, Any]:
        """Run integrity checks."""
        db_path = self.params.get("db_path")
        db_path_str = str(db_path) if db_path is not None else None
        print_step("Running final integrity checks")
        return check_integrity(db_path=db_path_str)

    def post(self, shared: dict[str, Any], prep_res: Any, exec_res: Any) -> str:
        """Finish the pipeline."""
        shared["integrity_results"] = exec_res
        if exec_res.get("error_count", 0) == 0:
            print_success("All integrity checks passed")
        else:
            logger.warning(f"Integrity checks found {exec_res['error_count']} issues")
        return "default"


def create_nba_pipeline() -> Flow:
    """Create and return the NBA data pipeline flow."""
    fetch = FetchNode()
    normalize = NormalizeNode()
    gold = GoldNode()
    integrity = IntegrityNode()

    # Define flow: Fetch -> Normalize -> Gold -> Integrity
    fetch >> normalize >> gold >> integrity

    return Flow(start=fetch)


if __name__ == "__main__":
    # Full pipeline run configuration
    from src.scripts.populate.populate_common_player_info import (
        CommonPlayerInfoPopulator,
    )
    from src.scripts.populate.populate_draft_combine_stats import (
        DraftCombineStatsPopulator,
    )
    from src.scripts.populate.populate_draft_history import DraftHistoryPopulator
    from src.scripts.populate.populate_league_game_logs import LeagueGameLogPopulator
    from src.scripts.populate.populate_player_game_stats_v2 import (
        PlayerGameStatsPopulator,
    )
    from src.scripts.populate.populate_team_details import TeamDetailsPopulator
    from src.scripts.populate.populate_team_info_common import TeamInfoCommonPopulator

    print_header("NBA DATA PIPELINE - FULL RUN")

    # Define all populators to run
    populators = [
        (TeamDetailsPopulator, {}),
        (TeamInfoCommonPopulator, {}),
        (PlayerGameStatsPopulator, {"seasons": ["2024-25"]}),  # Limit for test
        (LeagueGameLogPopulator, {"seasons": ["2024-25"]}),
        (DraftHistoryPopulator, {}),
        (DraftCombineStatsPopulator, {"seasons": ["2024"]}),
        (CommonPlayerInfoPopulator, {"active_only": True}),
    ]

    shared = {"populators": populators}

    pipeline = create_nba_pipeline()
    pipeline.set_params({"db_path": "src/backend/data/nba.duckdb"})
    pipeline.run(shared)
