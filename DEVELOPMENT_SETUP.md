# Development Setup Guide

This document describes the development tools configuration for this project using `uv`, `ruff`, and `mypy`.

## Overview

This project uses industry-standard Python development tools with best practices for code quality, type safety, and file management.

### Tools Configured

- **uv**: Modern, fast Python package manager for dependency management
- **ruff**: Extremely fast Python linter and code formatter written in Rust
- **mypy**: Static type checker for Python
- **pre-commit**: Framework for managing pre-commit hooks
- **pytest**: Testing framework with coverage reporting
- **black**: Code formatter (integrated with ruff)
- **isort**: Import sorter (integrated with ruff)

## Installation

### Prerequisites

- Python 3.12 or later
- `uv` package manager (should already be installed)

### Setup Steps

1. **Create Virtual Environment**
   ```bash
   uv venv
   ```

2. **Activate Virtual Environment**
   ```bash
   source .venv/bin/activate
   ```

3. **Install Development Dependencies**
   ```bash
   uv pip install --group dev
   ```

4. **Install Pre-commit Hooks**
   ```bash
   pre-commit install
   ```

## Configuration Details

### Ruff Configuration

**Location:** `pyproject.toml` under `[tool.ruff]` and `[tool.ruff.lint]`

#### Key Settings:
- **Line Length:** 88 characters (Black-compatible)
- **Target Version:** Python 3.12
- **Complexity Limit:** Maximum McCabe complexity of 10

#### Enabled Rule Categories:
- **F** - Pyflakes (basic error detection)
- **E, W** - pycodestyle (PEP 8 style checks)
- **C90** - McCabe complexity
- **I** - isort (import sorting)
- **N** - pep8-naming (naming conventions)
- **D** - pydocstyle (docstring conventions, Google style)
- **UP** - pyupgrade (modern Python syntax)
- **ANN** - flake8-annotations (type annotations)
- **ASYNC** - flake8-async (async/await patterns)
- **S** - flake8-bandit (security checks)
- **B** - flake8-bugbear (additional bug detection)
- **A** - flake8-builtins (builtin shadowing)
- **C4** - flake8-comprehensions (comprehension improvements)
- **PIE** - flake8-pie (miscellaneous lints)
- **PT** - flake8-pytest-style (pytest best practices)
- **SIM** - flake8-simplify (code simplification)
- **TRY** - tryceratops (exception handling best practices)
- **PERF** - Perflint (performance anti-patterns)
- **FURB** - refurb (modernization suggestions)
- **RUF** - Ruff-specific rules

#### Ignored Rules:
The configuration selectively ignores rules that:
- Conflict with the formatter (E501 for line length)
- Are overly strict for pragmatic code (various docstring rules)
- Don't fit the project style (boolean trap rules for function signatures)

#### Per-File Ignores:
- **tests/\***: Relaxed docstring and annotation requirements, allows assert statements
- **\_\_init\_\_.py**: Allows missing module docstrings

### Mypy Configuration

**Location:** `pyproject.toml` under `[tool.mypy]`

#### Strictness Level: High

The configuration enables comprehensive type checking with the following settings:

##### Type Checking Strictness:
- `disallow_untyped_defs` - All functions must have type annotations
- `disallow_incomplete_defs` - Partial annotations not allowed
- `disallow_untyped_calls` - Cannot call untyped functions from typed code
- `disallow_untyped_decorators` - Decorators must be typed
- `disallow_any_generics` - Generic types must specify type parameters
- `disallow_subclassing_any` - Cannot subclass Any
- `check_untyped_defs` - Type-check bodies of untyped functions

##### Warning Configurations:
- `warn_return_any` - Warn when returning Any from non-Any function
- `warn_redundant_casts` - Warn about unnecessary type casts
- `warn_unused_ignores` - Warn about unnecessary # type: ignore comments
- `warn_no_return` - Warn about missing return statements
- `warn_unreachable` - Warn about unreachable code
- `warn_unused_configs` - Warn about unused configuration

##### Type System Settings:
- `strict_equality` - Disallow equality between incompatible types
- `strict_optional` - Strict handling of Optional types
- `no_implicit_optional` - Don't treat None default as making parameter Optional
- `extra_checks` - Enable additional checks (replaces deprecated strict_concatenate)

##### Import Handling:
- `follow_imports = "normal"` - Follow and type-check imports
- `namespace_packages = true` - Support PEP 420 namespace packages
- `explicit_package_bases = true` - Require explicit package structure

##### Test File Overrides:
Test files have relaxed type checking:
- `disallow_untyped_defs = false`
- `disallow_untyped_calls = false`

### Pre-commit Hooks

**Location:** `.pre-commit-config.yaml`

The following hooks run automatically before each commit:

1. **Ruff Linter** - Checks and fixes code quality issues
2. **Ruff Formatter** - Formats code consistently
3. **Mypy** - Performs static type checking
4. **Standard Hooks**:
   - Trailing whitespace removal
   - End-of-file fixer
   - YAML/TOML/JSON validation
   - Large file check (max 1000KB)
   - Merge conflict detection
   - Debug statement detection
   - Private key detection
   - **Prevents commits to main branch** (use feature branches and PRs)
5. **Black** - Additional formatting
6. **isort** - Import sorting
7. **Bandit** - Security vulnerability scanning (with TOML support)

**Optional Hooks (run manually):**
- **Pytest** - Run with `pytest` command
- **Safety** - Not compatible with uv; run with `pip install safety && safety check`

### Pytest Configuration

**Location:** `pyproject.toml` under `[tool.pytest.ini_options]`

#### Test Discovery:
- Test paths: `tests/`
- File patterns: `test_*.py`, `*_test.py`
- Class patterns: `Test*`
- Function patterns: `test_*`

#### Coverage Settings:
- Source: `src/`
- Reports: Terminal (with missing lines), HTML, XML
- Minimum coverage tracking enabled

#### Custom Markers:
- `slow` - Marks slow tests
- `integration` - Marks integration tests
- `unit` - Marks unit tests

## Usage

### Running Linter

```bash
# Check all files
ruff check .

# Check specific file
ruff check path/to/file.py

# Auto-fix issues
ruff check --fix .

# Format code
ruff format .
```

### Running Type Checker

```bash
# Check all files
mypy .

# Check specific file
mypy path/to/file.py

# Check with verbose output
mypy --verbose .
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov

# Run specific test file
pytest tests/test_something.py

# Run tests by marker
pytest -m unit
pytest -m "not slow"
```

### Running Pre-commit Hooks Manually

```bash
# Run all hooks on all files
pre-commit run --all-files

# Run specific hook
pre-commit run ruff --all-files
pre-commit run mypy --all-files

# Update hook versions
pre-commit autoupdate
```

## Best Practices

### Code Quality Standards

1. **Line Length**: Keep lines under 88 characters
2. **Function Length**: Aim for functions under 50 lines
3. **File Length**: Keep modules under 500 lines (mypy suggestion)
4. **Complexity**: Keep McCabe complexity under 10

### Type Annotations

- All public functions must have complete type annotations
- Use modern Python type syntax (`list[str]` instead of `List[str]`)
- Avoid using `Any` unless absolutely necessary
- Use `None | T` instead of `Optional[T]` (PEP 604)

### Documentation

- All public modules, classes, and functions should have docstrings
- Use Google-style docstrings
- Include type information in docstrings even when type-annotated

### Testing

- Maintain high test coverage (aim for >80%)
- Use descriptive test names
- Mark slow or integration tests appropriately
- Tests can have relaxed type checking requirements

## Troubleshooting

### Common Issues

**Issue: Ruff shows deprecation warnings**
- Solution: Configuration has been updated to use `[tool.ruff.lint]` format

**Issue: Mypy shows "Unrecognized option" errors**
- Solution: Deprecated options have been removed (e.g., `warn_unused_variables`, `strict_concatenate`)

**Issue: Pre-commit hooks fail**
- Solution: Run `pre-commit run --all-files` to see detailed errors
- Fix issues reported by individual tools

**Issue: Type errors in test files**
- Solution: Test files have relaxed type checking; this is intentional

### Getting Help

- Ruff documentation: https://docs.astral.sh/ruff/
- Mypy documentation: https://mypy.readthedocs.io/
- Pre-commit documentation: https://pre-commit.com/
- uv documentation: https://docs.astral.sh/uv/

## Continuous Integration

This configuration is designed to work seamlessly with CI/CD pipelines:

```yaml
# Example GitHub Actions workflow snippet
- name: Install dependencies
  run: |
    pip install uv
    uv pip install --group dev

- name: Run linter
  run: ruff check .

- name: Run type checker
  run: mypy .

- name: Run tests
  run: pytest --cov --cov-report=xml
```

## Maintenance

### Updating Tools

```bash
# Update all dependencies
uv pip install --group dev --upgrade

# Update pre-commit hooks
pre-commit autoupdate

# Update specific tool
uv pip install --upgrade ruff mypy
```

### Configuration Changes

When modifying `pyproject.toml`:
1. Test changes locally with `ruff check .` and `mypy .`
2. Run `pre-commit run --all-files` to verify hooks work
3. Update this documentation if adding new rules or changing behavior
4. Commit changes following the project's git workflow

## Summary

This development setup provides:
- **Fast feedback** with ruff's performance
- **Type safety** with strict mypy configuration
- **Code quality** with comprehensive linting rules
- **Consistency** with automated formatting and pre-commit hooks
- **Security** with bandit and safety checks
- **Reliability** with comprehensive testing requirements

The configuration balances strictness with pragmatism, enforcing best practices while allowing reasonable flexibility where needed.
