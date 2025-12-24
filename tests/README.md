# CSV Analyzer Test Suite

This directory contains comprehensive tests for the CSV Analyzer project.

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures and mocks
├── unit/                    # Unit tests for individual components
│   ├── test_error_fixer.py
│   ├── test_load_data.py
│   ├── test_entity_resolver.py
│   ├── test_knowledge_store.py
│   ├── test_call_llm.py
│   └── test_data_processing_nodes.py
├── security/                # Security-critical tests
│   ├── test_safety_check.py
│   ├── test_executor.py
│   └── test_file_upload.py
├── integration/             # Integration tests
│   └── test_full_flow.py
└── fixtures/                # Test data
    └── sample_data/
        ├── test_valid.csv
        ├── test_sales.csv
        ├── test_malformed.csv
        └── test_empty.csv
```

## Running Tests

### Install Test Dependencies

```bash
pip install -r requirements-test.txt
```

### Run All Tests

```bash
pytest
```

### Run Specific Test Categories

```bash
# Security tests only
pytest tests/security/

# Unit tests only
pytest tests/unit/

# Integration tests only
pytest tests/integration/

# Specific test file
pytest tests/security/test_safety_check.py

# Specific test class
pytest tests/security/test_safety_check.py::TestSafetyCheckForbiddenImports

# Specific test function
pytest tests/security/test_safety_check.py::TestSafetyCheckForbiddenImports::test_blocks_os_import
```

### Run Tests with Coverage

```bash
# Generate coverage report
pytest --cov=. --cov-report=html

# View coverage report
open htmlcov/index.html  # On macOS
xdg-open htmlcov/index.html  # On Linux
```

### Run Tests with Verbose Output

```bash
pytest -v
```

### Run Tests and Stop on First Failure

```bash
pytest -x
```

## Test Categories

### Priority 1: Security Tests (CRITICAL)

**`tests/security/test_safety_check.py`**
- Tests AST-based code validation
- Verifies forbidden imports are blocked (os, subprocess, sys, etc.)
- Verifies forbidden functions are blocked (eval, exec, compile, etc.)
- Verifies forbidden attributes are blocked (__builtins__, __globals__, etc.)
- Tests evasion attempts and edge cases

**`tests/security/test_executor.py`**
- Tests sandboxed code execution
- Verifies only 'dfs' and 'pd' are available in scope
- Tests result extraction and error handling
- Verifies execution isolation

**`tests/security/test_file_upload.py`**
- Tests path traversal prevention
- Tests filename validation
- Tests CSV extension enforcement
- Tests various security edge cases

### Priority 2: Core Functionality Tests

**`tests/unit/test_error_fixer.py`**
- Tests retry counter and max retries enforcement
- Tests action logic ("fix" vs "give_up")
- Tests error context preservation

**`tests/unit/test_load_data.py`**
- Tests CSV loading with various encodings
- Tests error handling for malformed files
- Tests data integrity preservation

**`tests/unit/test_entity_resolver.py`**
- Tests entity extraction from questions
- Tests table/column matching logic
- Tests multi-part name handling (first/last names)
- Tests case-insensitive matching

### Priority 3: Utility Tests

**`tests/unit/test_knowledge_store.py`**
- Tests thread-safe operations
- Tests JSON persistence (load/save)
- Tests entity mapping storage/retrieval
- Tests concurrent access scenarios

**`tests/unit/test_call_llm.py`**
- Tests retry logic with exponential backoff
- Tests error handling after max retries
- Tests API key validation
- Tests timeout handling

### Priority 4: Data Processing Tests

**`tests/unit/test_data_processing_nodes.py`**
- Tests SchemaInference node
- Tests DataProfiler node
- Tests CodeGenerator node
- Tests Visualizer node
- Tests data processing pipeline integration

### Priority 5: Integration Tests

**`tests/integration/test_full_flow.py`**
- Tests end-to-end flow execution
- Tests error recovery across nodes
- Tests data propagation through the flow

## Key Testing Patterns

### Mocking LLM Calls

All tests use mocked LLM calls to ensure deterministic behavior:

```python
def test_example(mock_call_llm_in_nodes, sample_df):
    """Test with mocked LLM."""
    mock_call_llm_in_nodes.return_value = '["Alice", "Bob"]'
    # Your test code here
```

### Using Fixtures

Common fixtures are available in `conftest.py`:

```python
def test_example(sample_df, temp_csv_dir, mock_env_vars):
    """Test using shared fixtures."""
    # sample_df: A sample DataFrame with employee data
    # temp_csv_dir: A temporary directory with CSV files
    # mock_env_vars: Mocked environment variables
```

### Testing Thread Safety

For concurrent operations:

```python
def test_concurrent_access(temp_knowledge_file):
    """Test thread-safe operations."""
    import threading
    # Create threads and test concurrent access
```

## Coverage Goals

- **Phase 1 (Security & Critical)**: >50% coverage
- **Phase 2 (Core Business Logic)**: >80% coverage
- **Phase 3 (Integration & Edge Cases)**: >90% coverage

## Current Coverage

Run `pytest --cov=.` to see current coverage statistics.

## Contributing

When adding new tests:

1. Follow the existing test structure
2. Use descriptive test names that explain what is being tested
3. Include docstrings for test classes and methods
4. Use fixtures from `conftest.py` when possible
5. Mock external dependencies (LLM calls, file I/O, etc.)
6. Test both success and failure paths
7. Add security tests for any code that handles user input or executes code

## Common Issues

### Import Errors

If you see import errors, make sure you're running pytest from the project root:

```bash
cd /path/to/CSV_analyzer
pytest
```

### Mock Not Working

Make sure you're patching the correct import path:

```python
# Patch where the function is USED, not where it's DEFINED
with patch("backend.nodes.call_llm"):  # Correct
    # test code

with patch("backend.utils.call_llm.call_llm"):  # Wrong (usually)
    # test code
```

### Fixtures Not Found

Make sure `conftest.py` is in the tests directory and pytest can find it.

## CI/CD Integration

These tests are designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: |
    pip install -r requirements-test.txt
    pytest --cov=. --cov-report=xml
```
