"""Golden dataset tests for query planning and SQL generation."""

from pathlib import Path

import yaml

from backend.models import QueryComplexity, ValidationResult
from backend.nodes.planning import QueryPlanner
from backend.nodes.sql_generator import SQLGenerator


class StubDuckDBClient:
    def validate_sql_syntax(self, sql):
        return ValidationResult(is_valid=True, errors=[], warnings=[])


def _load_golden_dataset():
    path = Path(__file__).resolve().parents[1] / "golden_dataset.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_golden_dataset_planner(mocker) -> None:
    """Planner assigns expected complexity for golden dataset."""
    dataset = _load_golden_dataset()

    def planner_llm(prompt: str) -> str:
        if "Compare Lakers and Celtics" in prompt:
            return """```yaml
complexity: complex
combination_strategy: synthesize_comparison
sub_queries:
  - id: lakers
    description: "Get Lakers win percentage"
    depends_on: []
  - id: celtics
    description: "Get Celtics win percentage"
    depends_on: []
```"""
        return """```yaml
complexity: simple
combination_strategy: synthesize
sub_queries: []
```"""

    mocker.patch("backend.nodes.planning.call_llm", side_effect=planner_llm)

    planner = QueryPlanner()
    for entry in dataset:
        shared = {"rewritten_query": entry["question"]}
        prep = planner.prep(shared)
        exec_res = planner.exec(prep)
        planner.post(shared, prep, exec_res)

        if entry.get("complexity") == "complex":
            assert shared["query_plan"].complexity == QueryComplexity.COMPLEX
            assert len(shared["query_plan"].sub_queries) == entry.get(
                "expected_sub_queries", 0
            )
        else:
            assert shared["query_plan"].complexity == QueryComplexity.SIMPLE


def test_golden_dataset_sql_contains(mocker) -> None:
    """SQL generator output includes required tokens."""
    dataset = _load_golden_dataset()

    mocker.patch(
        "backend.nodes.sql_generator.get_duckdb_client",
        return_value=StubDuckDBClient(),
    )

    def generator_llm(prompt: str) -> str:
        return """```yaml
sql: |
  SELECT player_name, SUM(points) AS total_points
  FROM player_game_stats
  WHERE season = '2023'
  GROUP BY player_name
  ORDER BY total_points DESC
  LIMIT 1;
```"""

    mocker.patch("backend.nodes.sql_generator.call_llm", side_effect=generator_llm)

    generator = SQLGenerator()
    for entry in dataset:
        expected_tokens = entry.get("expected_sql_contains")
        if not expected_tokens:
            continue

        shared = {
            "rewritten_query": entry["question"],
            "table_schemas": "CREATE TABLE player_game_stats (player_name TEXT, points INTEGER, season TEXT);",
        }
        prep = generator.prep(shared)
        exec_res = generator.exec(prep)

        sql = exec_res["sql"].lower()
        for token in expected_tokens:
            assert token.lower() in sql
