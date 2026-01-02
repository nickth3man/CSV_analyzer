"""Shared fixtures and mocks for all tests."""

import os
import shutil
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


# ============================================================================
# Path Fixtures
# ============================================================================


@pytest.fixture
def test_data_dir():
    """Returns the path to the test data directory."""
    return Path(__file__).parent / "fixtures" / "sample_data"


@pytest.fixture
def temp_csv_dir(tmp_path):
    """Creates a temporary directory with sample CSV files."""
    csv_dir = tmp_path / "csv_data"
    csv_dir.mkdir()

    # Copy test CSV files to temp directory
    test_data = Path(__file__).parent / "fixtures" / "sample_data"
    for csv_file in test_data.glob("*.csv"):
        shutil.copy(csv_file, csv_dir / csv_file.name)

    return csv_dir


@pytest.fixture
def temp_knowledge_file(tmp_path):
    """Creates a temporary knowledge store file."""
    return tmp_path / "test_knowledge.json"


# ============================================================================
# Sample Data Fixtures
# ============================================================================


@pytest.fixture
def sample_df():
    """Returns a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie"],
            "age": [28, 35, 42],
            "salary": [75000, 82000, 95000],
            "department": ["Engineering", "Marketing", "Engineering"],
        },
    )


@pytest.fixture
def sample_sales_df():
    """Returns a sample sales DataFrame."""
    return pd.DataFrame(
        {
            "product_id": [1, 2, 3, 4, 5],
            "product_name": ["Laptop", "Mouse", "Desk Chair", "Notebook", "Monitor"],
            "category": [
                "Electronics",
                "Electronics",
                "Furniture",
                "Stationery",
                "Electronics",
            ],
            "price": [1200.00, 25.50, 350.00, 5.99, 450.00],
            "quantity_sold": [5, 20, 3, 50, 8],
            "revenue": [6000.00, 510.00, 1050.00, 299.50, 3600.00],
        },
    )


@pytest.fixture
def sample_shared_store(sample_df):
    """Returns a sample shared store with loaded data."""
    return {
        "data_dir": "/fake/path",
        "dfs": {"employees": sample_df},
        "schemas": {},
        "profiles": {},
        "question": "What is the average salary?",
        "context": {},
        "plan": "",
        "code": "",
        "result": None,
        "error": None,
        "deep_analysis": {},
        "chart_path": None,
        "response": "",
    }


# ============================================================================
# LLM Mock Fixtures
# ============================================================================


@pytest.fixture
def mock_llm_response():
    """Returns a mock LLM response function."""

    def _mock_response(prompt) -> str:
        """Generate deterministic mock responses based on prompt content."""
        prompt_lower = prompt.lower()

        if "analyzing if a user's nba question" in prompt_lower:
            return """```yaml
intent: clear
reasoning: "Query is specific enough"
clarification_questions: []
```"""

        if "rewriting a user's nba question" in prompt_lower:
            return """```yaml
rewritten_query: "Who led the league in points in 2023?"
resolved_entities: {}
reasoning: "No changes needed"
```"""

        if "determine if the question is simple" in prompt_lower:
            return """```yaml
complexity: simple
combination_strategy: synthesize
sub_queries: []
```"""

        if "select the most relevant tables" in prompt_lower:
            return """```yaml
selected_tables:
  - table_name: player
    reason: "Contains player information"
  - table_name: game
    reason: "Contains game stats"
```"""

        if "duckdb sql expert" in prompt_lower:
            return """```yaml
thinking: |
  Use a simple aggregation on the game table.
sql: |
  SELECT player_name, SUM(points) AS total_points
  FROM player_game_stats
  WHERE season = '2022-23'
  GROUP BY player_name
  ORDER BY total_points DESC
  LIMIT 1;
```"""

        if "quality assurance reviewer" in prompt_lower:
            return """```yaml
status: pass
confidence: 0.9
issues: []
suggestions: []
```"""

        if "nba data analyst explaining query results" in prompt_lower:
            return """```yaml
answer: |
  The top scorer in 2023 was Player X with 2,000 points.
transparency_note: |
  I summed points by player for the 2022-23 season and sorted the totals.
```"""

        return "Mock LLM response for testing purposes."

    return _mock_response


@pytest.fixture
def mock_call_llm(mock_llm_response):
    """Patch `backend.utils.call_llm.call_llm` to use the provided mock LLM response and yield the mock object.

    Parameters:
        mock_llm_response (callable): A function used as the `side_effect` for the patched `call_llm` to simulate LLM responses.

    Returns:
        mock: The mocked object that replaced `call_llm`.
    """
    with patch(
        "backend.utils.call_llm.call_llm",
        side_effect=mock_llm_response,
    ) as mock:
        yield mock


@pytest.fixture
def mock_call_llm_in_nodes(mock_llm_response):
    """Provide patched `call_llm` in all node modules that use it.

    This patches call_llm in entity, code_generation, planning, and analysis modules
    to prevent real LLM calls during tests.

    Parameters:
        mock_llm_response: Fixture dependency that ensures the LLM response mock is available in the test context.

    Returns:
        mock: A mock object that replaces call_llm in node modules.
    """
    with (
        patch("backend.nodes.query.call_llm") as query_mock,
        patch("backend.nodes.query_rewriter.call_llm") as rewriter_mock,
        patch("backend.nodes.planning.call_llm") as planning_mock,
        patch("backend.nodes.table_selector.call_llm") as table_selector_mock,
        patch("backend.nodes.sql_generator.call_llm") as sql_generator_mock,
        patch("backend.nodes.response_grader.call_llm") as grader_mock,
        patch("backend.nodes.analysis.call_llm") as analysis_mock,
    ):
        # Set default return values
        default_response = "Mock LLM response for testing purposes."
        query_mock.return_value = default_response
        rewriter_mock.return_value = default_response
        planning_mock.return_value = default_response
        table_selector_mock.return_value = default_response
        sql_generator_mock.return_value = default_response
        grader_mock.return_value = default_response
        analysis_mock.return_value = default_response

        # Return the entity mock for backward compatibility (tests can set return_value on it)
        # But configure all mocks to use the same side_effect/return_value when set
        class MultiMock:
            def __init__(self, mocks) -> None:
                self._mocks = mocks
                self._return_value = default_response

            @property
            def return_value(self):
                return self._return_value

            @return_value.setter
            def return_value(self, value) -> None:
                self._return_value = value
                for m in self._mocks:
                    m.return_value = value

            @property
            def side_effect(self):
                return self._mocks[0].side_effect

            @side_effect.setter
            def side_effect(self, value) -> None:
                for m in self._mocks:
                    m.side_effect = value

            def assert_called(self):
                return any(m.called for m in self._mocks)

            def assert_called_once(self) -> None:
                call_count = sum(m.call_count for m in self._mocks)
                assert call_count == 1

        yield MultiMock(
            [
                query_mock,
                rewriter_mock,
                planning_mock,
                table_selector_mock,
                sql_generator_mock,
                grader_mock,
                analysis_mock,
            ]
        )


# ============================================================================
# Environment Fixtures
# ============================================================================


@pytest.fixture
def mock_env_vars():
    """Sets up mock environment variables."""
    with patch.dict(
        os.environ,
        {"OPENROUTER_API_KEY": "test_api_key_12345", "OPENROUTER_MODEL": "test-model"},
    ):
        yield


@pytest.fixture(autouse=True)
def reset_knowledge_store() -> None:
    """Ensures knowledge store is clean between tests."""
    # This will run before each test
    return
    # Cleanup after test if needed


# ============================================================================
# Mock OpenAI Client
# ============================================================================


@pytest.fixture
def mock_openai_client():
    """Provide a MagicMock OpenAI client and patch the OpenAI constructor used by backend.utils.call_llm.

    The fixture yields a mock client whose chat completions create method returns a mock response
    with a single choice whose message content is "Mock LLM response". The backend.utils.call_llm.OpenAI
    constructor is patched to return this mock client for the duration of the fixture.

    Returns:
        MagicMock: The mocked OpenAI client with .chat.completions.create configured.
    """
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "Mock LLM response"

    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_response

    with patch("backend.utils.call_llm.OpenAI", return_value=mock_client):
        yield mock_client


# ============================================================================
# Code Execution Fixtures
# ============================================================================


@pytest.fixture
def safe_exec_scope(sample_df):
    """Returns a safe execution scope for testing code execution."""
    return {"dfs": {"employees": sample_df}, "pd": pd}


# ============================================================================
# File System Fixtures
# ============================================================================


@pytest.fixture
def mock_csv_files(tmp_path):
    """Creates temporary CSV files for testing."""
    csv_dir = tmp_path / "data"
    csv_dir.mkdir()

    # Create test CSV files
    employees = pd.DataFrame(
        {"name": ["Alice", "Bob"], "age": [28, 35], "salary": [75000, 82000]},
    )
    employees.to_csv(csv_dir / "employees.csv", index=False)

    sales = pd.DataFrame({"product": ["Laptop", "Mouse"], "price": [1200, 25]})
    sales.to_csv(csv_dir / "sales.csv", index=False)

    return csv_dir


# ============================================================================
# Visualization Fixtures
# ============================================================================


@pytest.fixture
def mock_matplotlib():
    """Mocks matplotlib to prevent actual plot generation during tests."""
    with (
        patch("matplotlib.pyplot.savefig") as mock_savefig,
        patch("matplotlib.pyplot.close") as mock_close,
    ):
        yield {"savefig": mock_savefig, "close": mock_close}


# ============================================================================
# Populate Module Fixtures
# ============================================================================


@pytest.fixture
def temp_duckdb(tmp_path):
    """Create a temporary DuckDB database for testing populators."""
    import duckdb

    db_path = tmp_path / "test_nba.duckdb"
    conn = duckdb.connect(str(db_path))

    # Create base schema tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_game_stats_raw (
            game_id VARCHAR,
            player_id INTEGER,
            team_id INTEGER,
            player_name VARCHAR,
            min VARCHAR,
            pts INTEGER,
            fgm INTEGER,
            fga INTEGER,
            fg_pct DOUBLE,
            fg3m INTEGER,
            fg3a INTEGER,
            fg3_pct DOUBLE,
            ftm INTEGER,
            fta INTEGER,
            ft_pct DOUBLE,
            oreb INTEGER,
            dreb INTEGER,
            reb INTEGER,
            ast INTEGER,
            stl INTEGER,
            blk INTEGER,
            tov INTEGER,
            pf INTEGER,
            plus_minus INTEGER,
            populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (game_id, player_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS games_raw (
            game_id VARCHAR PRIMARY KEY,
            season_year VARCHAR,
            game_date DATE,
            home_team_id INTEGER,
            away_team_id INTEGER,
            home_team_score INTEGER,
            away_team_score INTEGER,
            populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.close()
    return db_path


@pytest.fixture
def mock_nba_client():
    """Create a mock NBA API client for testing populators."""
    mock_client = MagicMock()

    # Mock player game logs response
    mock_client.get_player_game_logs.return_value = pd.DataFrame(
        {
            "GAME_ID": ["0022300001", "0022300002"],
            "PLAYER_ID": [2544, 2544],
            "PLAYER_NAME": ["LeBron James", "LeBron James"],
            "TEAM_ID": [1610612747, 1610612747],
            "TEAM_ABBREVIATION": ["LAL", "LAL"],
            "GAME_DATE": ["2023-10-24", "2023-10-26"],
            "MATCHUP": ["LAL vs. DEN", "LAL @ PHX"],
            "WL": ["W", "L"],
            "MIN": ["35:20", "38:15"],
            "PTS": [28, 32],
            "FGM": [10, 12],
            "FGA": [18, 22],
            "FG_PCT": [0.556, 0.545],
            "FG3M": [2, 3],
            "FG3A": [5, 7],
            "FG3_PCT": [0.400, 0.429],
            "FTM": [6, 5],
            "FTA": [8, 6],
            "FT_PCT": [0.750, 0.833],
            "OREB": [1, 2],
            "DREB": [7, 6],
            "REB": [8, 8],
            "AST": [11, 9],
            "STL": [2, 1],
            "BLK": [1, 0],
            "TOV": [3, 4],
            "PF": [2, 3],
            "PLUS_MINUS": [12, -5],
        }
    )

    # Mock static players
    mock_client.get_all_players.return_value = [
        {"id": 2544, "full_name": "LeBron James", "is_active": True},
        {"id": 201566, "full_name": "Stephen Curry", "is_active": True},
    ]

    # Mock static teams
    mock_client.get_all_teams.return_value = [
        {"id": 1610612747, "full_name": "Los Angeles Lakers", "abbreviation": "LAL"},
        {"id": 1610612744, "full_name": "Golden State Warriors", "abbreviation": "GSW"},
    ]

    # Mock league game finder
    mock_client.get_league_game_finder.return_value = pd.DataFrame(
        {
            "GAME_ID": ["0022300001"],
            "SEASON_ID": ["22023"],
            "TEAM_ID": [1610612747],
            "TEAM_ABBREVIATION": ["LAL"],
            "GAME_DATE": ["2023-10-24"],
            "PTS": [117],
            "WL": ["W"],
        }
    )

    return mock_client


@pytest.fixture
def sample_player_game_stats_df():
    """Sample DataFrame matching player_game_stats schema."""
    return pd.DataFrame(
        {
            "game_id": ["0022300001", "0022300002"],
            "player_id": [2544, 201566],
            "player_name": ["LeBron James", "Stephen Curry"],
            "team_id": [1610612747, 1610612744],
            "game_date": ["2023-10-24", "2023-10-24"],
            "matchup": ["LAL vs. DEN", "GSW vs. PHX"],
            "wl": ["W", "L"],
            "min": [35.5, 38.0],
            "pts": [28, 35],
            "fgm": [10, 12],
            "fga": [18, 24],
            "fg_pct": [0.556, 0.500],
            "fg3m": [2, 8],
            "fg3a": [5, 15],
            "fg3_pct": [0.400, 0.533],
            "ftm": [6, 3],
            "fta": [8, 3],
            "ft_pct": [0.750, 1.000],
            "oreb": [1, 0],
            "dreb": [7, 4],
            "reb": [8, 4],
            "ast": [11, 6],
            "stl": [2, 3],
            "blk": [1, 0],
            "tov": [3, 2],
            "pf": [2, 4],
            "plus_minus": [12, -8],
        }
    )


@pytest.fixture
def mock_circuit_breaker():
    """Create a mock circuit breaker for testing."""
    from src.scripts.populate.resilience import CircuitBreaker

    breaker = CircuitBreaker(
        name="test_breaker",
        failure_threshold=3,
        success_threshold=2,
        timeout=5.0,
    )
    return breaker


@pytest.fixture
def population_test_context(temp_duckdb, mock_nba_client):
    """Complete test context for population tests."""
    return {
        "db_path": temp_duckdb,
        "client": mock_nba_client,
        "season": "2023-24",
        "season_type": "Regular Season",
    }
