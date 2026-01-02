"""Integration tests for the SQL-based analysis flow."""

import pandas as pd

from backend.flow import create_analyst_flow
from backend.models import TableMeta, ValidationResult


class StubDuckDBClient:
    """Stub DuckDB client for tests."""

    def get_table_schema(self, tables):
        return """CREATE TABLE player_game_stats (
    player_name TEXT,
    points INTEGER,
    season TEXT
);"""

    def validate_sql_syntax(self, sql):
        return ValidationResult(is_valid=True, errors=[], warnings=[])

    def execute_query(self, sql):
        return pd.DataFrame([{"player_name": "Player X", "total_points": 2000}])


def test_simple_flow_runs(mocker, mock_call_llm_in_nodes, mock_llm_response) -> None:
    """Run the flow end-to-end with mocked LLM and DB."""
    mocker.patch(
        "backend.nodes.table_selector.get_duckdb_client",
        return_value=StubDuckDBClient(),
    )
    mocker.patch(
        "backend.nodes.sql_generator.get_duckdb_client", return_value=StubDuckDBClient()
    )
    mocker.patch(
        "backend.nodes.sql_executor.get_duckdb_client", return_value=StubDuckDBClient()
    )
    mock_call_llm_in_nodes.side_effect = mock_llm_response

    flow = create_analyst_flow()

    shared = {
        "question": "Who led the league in points in 2023?",
        "available_tables": [
            TableMeta(
                name="player_game_stats",
                description="Player stats by game",
                columns=["player_name", "points", "season"],
                row_count=10,
            )
        ],
        "table_embeddings": {"player_game_stats": [0.0]},
        "total_retries": 0,
        "grader_retries": 0,
    }

    flow.run(shared)

    assert shared.get("final_answer")
