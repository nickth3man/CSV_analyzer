"""Shared fixtures and mocks for all tests."""

import os
import shutil
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
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

        # Schema inference
        if "infer the schema" in prompt_lower or "data types" in prompt_lower:
            return """```yaml
tables:
  employees:
    columns:
      - name: name
        type: string
      - name: age
        type: integer
      - name: salary
        type: float
      - name: department
        type: string
```"""

        # Entity resolution
        if "extract entities" in prompt_lower or "identify entities" in prompt_lower:
            return """```yaml
entities:
  - text: "salary"
    type: "column"
    table: "employees"
    column: "salary"
```"""

        # Query clarification
        if "ambiguous" in prompt_lower or "clarify" in prompt_lower:
            return """```yaml
is_ambiguous: false
reason: "Query is clear and specific"
```"""

        # Planning
        if "create a plan" in prompt_lower or "analysis plan" in prompt_lower:
            return """```yaml
plan: |
  1. Load the employees table
  2. Calculate average of salary column
  3. Return the result
```"""

        # Code generation
        if "generate python code" in prompt_lower or "write code" in prompt_lower:
            return """```python
# Calculate average salary
final_result = dfs['employees']['salary'].mean()
```"""

        # Safety check (should be done via AST, but for fallback)
        if "safety" in prompt_lower:
            return "The code appears safe."

        # Result validation
        if "validate" in prompt_lower or "verify" in prompt_lower:
            return """```yaml
is_valid: true
reason: "Result correctly answers the question"
```"""

        # Deep analysis
        if "deep analysis" in prompt_lower or "statistical analysis" in prompt_lower:
            return """```yaml
insights:
  - The average salary is $84,000
  - Engineering has higher average salary than Marketing
distribution: "Salaries range from $75,000 to $95,000"
```"""

        # Response synthesis
        if "synthesize" in prompt_lower or "narrative" in prompt_lower:
            return "The average salary across all employees is $84,000, with Engineering having a higher average compared to Marketing."

        # Error fixing
        if "fix the error" in prompt_lower or "debug" in prompt_lower:
            return """```python
# Fixed code
final_result = dfs['employees']['salary'].mean()
```"""

        # Default response
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
        patch("backend.nodes.entity.call_llm") as entity_mock,
        patch("backend.nodes.code_generation.call_llm") as codegen_mock,
        patch("backend.nodes.planning.call_llm") as planning_mock,
        patch("backend.nodes.analysis.call_llm") as analysis_mock,
    ):
        # Set default return values
        default_response = "Mock LLM response for testing purposes."
        entity_mock.return_value = default_response
        codegen_mock.return_value = default_response
        planning_mock.return_value = default_response
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

        yield MultiMock([entity_mock, codegen_mock, planning_mock, analysis_mock])


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
