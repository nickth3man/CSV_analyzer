"""Shared fixtures and mocks for all tests."""

import os
import sys
import tempfile
import shutil
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

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
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [28, 35, 42],
        "salary": [75000, 82000, 95000],
        "department": ["Engineering", "Marketing", "Engineering"]
    })


@pytest.fixture
def sample_sales_df():
    """Returns a sample sales DataFrame."""
    return pd.DataFrame({
        "product_id": [1, 2, 3, 4, 5],
        "product_name": ["Laptop", "Mouse", "Desk Chair", "Notebook", "Monitor"],
        "category": ["Electronics", "Electronics", "Furniture", "Stationery", "Electronics"],
        "price": [1200.00, 25.50, 350.00, 5.99, 450.00],
        "quantity_sold": [5, 20, 3, 50, 8],
        "revenue": [6000.00, 510.00, 1050.00, 299.50, 3600.00]
    })


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
        "response": ""
    }


# ============================================================================
# LLM Mock Fixtures
# ============================================================================

@pytest.fixture
def mock_llm_response():
    """Returns a mock LLM response function."""
    def _mock_response(prompt):
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
        elif "extract entities" in prompt_lower or "identify entities" in prompt_lower:
            return """```yaml
entities:
  - text: "salary"
    type: "column"
    table: "employees"
    column: "salary"
```"""

        # Query clarification
        elif "ambiguous" in prompt_lower or "clarify" in prompt_lower:
            return """```yaml
is_ambiguous: false
reason: "Query is clear and specific"
```"""

        # Planning
        elif "create a plan" in prompt_lower or "analysis plan" in prompt_lower:
            return """```yaml
plan: |
  1. Load the employees table
  2. Calculate average of salary column
  3. Return the result
```"""

        # Code generation
        elif "generate python code" in prompt_lower or "write code" in prompt_lower:
            return """```python
# Calculate average salary
final_result = dfs['employees']['salary'].mean()
```"""

        # Safety check (should be done via AST, but for fallback)
        elif "safety" in prompt_lower:
            return "The code appears safe."

        # Result validation
        elif "validate" in prompt_lower or "verify" in prompt_lower:
            return """```yaml
is_valid: true
reason: "Result correctly answers the question"
```"""

        # Deep analysis
        elif "deep analysis" in prompt_lower or "statistical analysis" in prompt_lower:
            return """```yaml
insights:
  - The average salary is $84,000
  - Engineering has higher average salary than Marketing
distribution: "Salaries range from $75,000 to $95,000"
```"""

        # Response synthesis
        elif "synthesize" in prompt_lower or "narrative" in prompt_lower:
            return "The average salary across all employees is $84,000, with Engineering having a higher average compared to Marketing."

        # Error fixing
        elif "fix the error" in prompt_lower or "debug" in prompt_lower:
            return """```python
# Fixed code
final_result = dfs['employees']['salary'].mean()
```"""

        # Default response
        else:
            return "Mock LLM response for testing purposes."

    return _mock_response


@pytest.fixture
def mock_call_llm(mock_llm_response):
    """Mocks the call_llm function."""
    with patch("backend.utils.call_llm.call_llm", side_effect=mock_llm_response) as mock:
        yield mock


@pytest.fixture
def mock_call_llm_in_nodes(mock_llm_response):
    """Mocks the call_llm function as imported in nodes.py."""
    with patch("backend.nodes.entity.call_llm") as mock:
        mock.return_value = "Mock LLM response for testing purposes."
        yield mock


# ============================================================================
# Environment Fixtures
# ============================================================================

@pytest.fixture
def mock_env_vars():
    """Sets up mock environment variables."""
    with patch.dict(os.environ, {
        "OPENROUTER_API_KEY": "test_api_key_12345",
        "OPENROUTER_MODEL": "test-model"
    }):
        yield


@pytest.fixture(autouse=True)
def reset_knowledge_store():
    """Ensures knowledge store is clean between tests."""
    # This will run before each test
    yield
    # Cleanup after test if needed


# ============================================================================
# Mock OpenAI Client
# ============================================================================

@pytest.fixture
def mock_openai_client():
    """Mocks the OpenAI client for API calls."""
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
    return {
        "dfs": {"employees": sample_df},
        "pd": pd
    }


# ============================================================================
# File System Fixtures
# ============================================================================

@pytest.fixture
def mock_csv_files(tmp_path):
    """Creates temporary CSV files for testing."""
    csv_dir = tmp_path / "data"
    csv_dir.mkdir()

    # Create test CSV files
    employees = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "age": [28, 35],
        "salary": [75000, 82000]
    })
    employees.to_csv(csv_dir / "employees.csv", index=False)

    sales = pd.DataFrame({
        "product": ["Laptop", "Mouse"],
        "price": [1200, 25]
    })
    sales.to_csv(csv_dir / "sales.csv", index=False)

    return csv_dir


# ============================================================================
# Visualization Fixtures
# ============================================================================

@pytest.fixture
def mock_matplotlib():
    """Mocks matplotlib to prevent actual plot generation during tests."""
    with patch("matplotlib.pyplot.savefig") as mock_savefig:
        with patch("matplotlib.pyplot.close") as mock_close:
            yield {"savefig": mock_savefig, "close": mock_close}
