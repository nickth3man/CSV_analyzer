# Development Tools Guide

This document provides a comprehensive overview of all development tools configured for this project. These tools ensure code quality, security, consistency, and maintainability.

## Table of Contents

- [Quick Start](#quick-start)
- [Code Quality Tools](#code-quality-tools)
- [Testing Tools](#testing-tools)
- [Security Tools](#security-tools)
- [Documentation Tools](#documentation-tools)
- [Development Workflow Tools](#development-workflow-tools)
- [CI/CD Integration](#cicd-integration)
- [Tool Configuration](#tool-configuration)

## Quick Start

### Install All Development Tools

```bash
# Using Make (recommended)
make install-dev

# Or manually
pip install --editable '.[dev]'
pre-commit install
```

### Run All Quality Checks

```bash
make ci
```

This runs: linting, type checking, tests, and security scans.

## Code Quality Tools

### 1. Ruff - Fast Python Linter

**Purpose**: Extremely fast Python linter that replaces multiple tools (Flake8, isort, etc.)

**Usage**:
```bash
# Check for issues
make lint
# or
ruff check .

# Auto-fix issues
make lint-fix
# or
ruff check --fix .
```

**Configuration**: `pyproject.toml` under `[tool.ruff]`

**Features**:
- 500+ lint rules enabled
- Auto-fixes common issues
- Replaces: Flake8, isort, pyupgrade, and more
- Written in Rust for speed

**Documentation**: [Ruff Documentation](https://docs.astral.sh/ruff/)

---

### 2. Black - Code Formatter

**Purpose**: Uncompromising Python code formatter

**Usage**:
```bash
# Check formatting
black --check .

# Format code
make format
# or
black .
```

**Configuration**: `pyproject.toml` under `[tool.black]`

**Settings**:
- Line length: 88 characters
- Target: Python 3.12

**Documentation**: [Black Documentation](https://black.readthedocs.io/)

---

### 3. isort - Import Sorter

**Purpose**: Sorts and organizes import statements

**Usage**:
```bash
# Check imports
isort --check-only .

# Sort imports
isort .
```

**Configuration**: `pyproject.toml` under `[tool.isort]`

**Features**:
- Black-compatible profile
- Known first-party packages configured
- Consistent import organization

**Documentation**: [isort Documentation](https://pycqa.github.io/isort/)

---

### 4. mypy - Static Type Checker

**Purpose**: Static type checking for Python

**Usage**:
```bash
make type-check
# or
mypy src
```

**Configuration**: `pyproject.toml` under `[tool.mypy]`

**Settings**:
- Lenient mode for gradual typing
- Ignores errors in tests and legacy code
- Focus on new code quality

**Documentation**: [mypy Documentation](https://mypy.readthedocs.io/)

## Testing Tools

### 1. pytest - Testing Framework

**Purpose**: Powerful and flexible testing framework

**Usage**:
```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run unit tests only
make test-unit

# Run integration tests only
make test-integration

# Verbose output
make test-verbose
```

**Configuration**: `pyproject.toml` under `[tool.pytest.ini_options]`

**Features**:
- Fixtures for test setup
- Parametrized testing
- Coverage reporting
- Multiple test markers (unit, integration, slow)

**Documentation**: [pytest Documentation](https://docs.pytest.org/)

---

### 2. pytest-cov - Coverage Plugin

**Purpose**: Measure code coverage

**Usage**:
```bash
make test-cov
```

**Reports Generated**:
- Terminal output (term-missing)
- HTML report (`htmlcov/index.html`)
- XML report (for CI tools)

**Configuration**: `pyproject.toml` under `[tool.coverage]`

**Documentation**: [pytest-cov Documentation](https://pytest-cov.readthedocs.io/)

---

### 3. pytest-mock - Mocking Plugin

**Purpose**: Simplified mocking in tests

**Usage**:
```python
def test_example(mocker):
    mock_obj = mocker.patch('module.function')
    mock_obj.return_value = 'mocked'
```

**Documentation**: [pytest-mock Documentation](https://pytest-mock.readthedocs.io/)

---

### 4. pytest-asyncio - Async Testing

**Purpose**: Test async/await code

**Usage**:
```python
@pytest.mark.asyncio
async def test_async_function():
    result = await async_function()
    assert result == expected
```

**Documentation**: [pytest-asyncio Documentation](https://pytest-asyncio.readthedocs.io/)

## Security Tools

### 1. Bandit - Security Vulnerability Scanner

**Purpose**: Finds common security issues in Python code

**Usage**:
```bash
make security
# or
bandit -r src -c pyproject.toml
```

**Configuration**: `pyproject.toml` under `[tool.bandit]`

**Checks For**:
- SQL injection vulnerabilities
- Hardcoded passwords
- Use of exec/eval
- Insecure cryptography
- And 100+ more security patterns

**Documentation**: [Bandit Documentation](https://bandit.readthedocs.io/)

---

### 2. pip-audit - Dependency Vulnerability Scanner

**Purpose**: Scans Python dependencies for known security vulnerabilities

**Usage**:
```bash
make audit
# or
pip-audit
```

**Features**:
- Uses PyPI Advisory Database
- Checks for known CVEs
- Suggests fixes
- Can auto-fix with `--fix` flag

**Manual Stages**:
- Run manually with: `make audit`
- Configured in pre-commit as manual stage
- Runs in CI on every push

**Documentation**: [pip-audit Documentation](https://pypi.org/project/pip-audit/)

## Documentation Tools

### 1. interrogate - Docstring Coverage

**Purpose**: Measures docstring coverage in your codebase

**Usage**:
```bash
make interrogate
# or
interrogate -vv --fail-under=80 src
```

**Configuration**: `pyproject.toml` under `[tool.interrogate]`

**Settings**:
- Minimum coverage: 80%
- Generates badge: `interrogate_badge.svg`
- Ignores: tests, setup files
- Special regex for Node methods (prep, exec, post)

**Features**:
- Detailed coverage report
- Badge generation for README
- Configurable thresholds
- Excludes test files

**Documentation**: [interrogate Documentation](https://interrogate.readthedocs.io/)

---

### 2. MkDocs - Documentation Generator

**Purpose**: Generate beautiful project documentation

**Usage**:
```bash
# Build documentation
make docs

# Serve locally (http://localhost:8000)
make docs-serve

# Deploy to GitHub Pages
make docs-deploy
```

**Configuration**: `mkdocs.yml`

**Theme**: Material for MkDocs

**Features**:
- Automatic API documentation from docstrings
- Mermaid diagram support
- Code highlighting
- Search functionality
- Dark/light mode toggle
- Mobile responsive

**Documentation**: [MkDocs Documentation](https://www.mkdocs.org/)

---

### 3. mkdocstrings - API Documentation

**Purpose**: Auto-generate API docs from Python docstrings

**Features**:
- Extracts Google-style docstrings
- Type annotations support
- Cross-referencing
- Inheritance tracking

**Documentation**: [mkdocstrings Documentation](https://mkdocstrings.github.io/)

## Development Workflow Tools

### 1. pre-commit - Git Hook Manager

**Purpose**: Run checks before commits

**Usage**:
```bash
# Run all hooks manually
make pre-commit

# Update hooks to latest versions
make pre-commit-update
```

**Configuration**: `.pre-commit-config.yaml`

**Hooks Configured**:
1. **Basic Checks**:
   - Trailing whitespace removal
   - End-of-file fixer
   - YAML/TOML/JSON validation
   - Large file detection (max 1MB)
   - Merge conflict detection
   - Debug statement detection
   - Private key detection

2. **Code Quality**:
   - Ruff (linting + auto-fix)
   - Ruff-format (formatting)
   - Black (formatting)
   - isort (import sorting)
   - mypy (type checking)

3. **Security**:
   - Bandit (security scanning)

4. **Documentation**:
   - interrogate (docstring coverage on changed files)

5. **Manual Stages** (run with `--hook-stage manual`):
   - pip-audit (dependency vulnerabilities)
   - vulture (dead code detection)

**Documentation**: [pre-commit Documentation](https://pre-commit.com/)

---

### 2. commitizen - Conventional Commits

**Purpose**: Standardize commit messages and automate versioning

**Usage**:
```bash
# Make a commit
make commit
# or
cz commit

# Bump version
make bump
# or
cz bump

# Generate changelog
make changelog
# or
cz changelog
```

**Configuration**: `pyproject.toml` under `[tool.commitizen]`

**Commit Format**:
```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting
- `refactor`: Code restructuring
- `test`: Tests
- `chore`: Maintenance

**Features**:
- Interactive commit message builder
- Automatic version bumping (semver)
- Changelog generation
- Pre-commit hook integration

**Documentation**: [Commitizen Documentation](https://commitizen-tools.github.io/commitizen/)

---

### 3. vulture - Dead Code Detector

**Purpose**: Find unused code (dead code)

**Usage**:
```bash
make vulture
# or
vulture src app.py --min-confidence=80
```

**Configuration**: `pyproject.toml` under `[tool.vulture]`

**Settings**:
- Minimum confidence: 80%
- Excludes: tests, .venv
- Sorts by size
- Ignores test patterns

**Features**:
- Detects unused:
  - Functions
  - Classes
  - Variables
  - Imports
  - Properties
  - Arguments

**Documentation**: [Vulture Documentation](https://github.com/jendrikseipp/vulture)

---

### 4. Make - Task Automation

**Purpose**: Simplify common development tasks

**Usage**:
```bash
# See all available commands
make help

# Common commands
make install-dev  # Install dev dependencies
make test         # Run tests
make lint         # Check code quality
make ci           # Run all CI checks
make clean        # Remove build artifacts
```

**Configuration**: `Makefile`

**Available Targets**: 30+ commands for all development tasks

**Benefits**:
- Single command interface
- Self-documenting (make help)
- Platform independent
- Composable tasks

---

### 5. .editorconfig - Editor Configuration

**Purpose**: Maintain consistent coding styles across editors

**Configuration**: `.editorconfig`

**Settings**:
- Charset: UTF-8
- Line endings: LF (Unix-style)
- Trim trailing whitespace
- Insert final newline
- Python: 4 spaces indent
- YAML/JSON: 2 spaces indent
- Max line length: 88

**Supported Editors**:
- VS Code
- PyCharm
- Sublime Text
- Vim
- Emacs
- And many more

**Documentation**: [EditorConfig](https://editorconfig.org/)

## CI/CD Integration

### GitHub Actions

**Configuration**: `.github/workflows/ci.yml`

**Workflow Jobs**:

1. **Lint** (runs in parallel):
   - Ruff check
   - Black format check
   - isort check

2. **Type Check** (runs in parallel):
   - mypy static analysis

3. **Test** (runs in parallel):
   - pytest with coverage
   - Upload coverage to Codecov
   - Matrix: Python 3.12

4. **Security** (runs in parallel):
   - Bandit scan
   - pip-audit

5. **Documentation Coverage** (runs in parallel):
   - interrogate
   - Badge generation

6. **Dead Code** (runs in parallel):
   - vulture

7. **All Checks** (runs after all):
   - Validates all jobs passed
   - Gates for merge

**Triggers**:
- Push to: main, develop, claude/*
- Pull requests to: main, develop
- Manual workflow dispatch

**Features**:
- Parallel execution for speed
- Caching for faster builds
- Artifact uploads
- Continue-on-error for optional checks

---

### Dependabot

**Configuration**: `.github/dependabot.yml`

**Features**:
- **Python Dependencies**:
  - Weekly updates (Mondays at 9 AM)
  - Groups by category (testing, linting, security)
  - Ignores major version bumps for stable deps
  - Auto-assigns to maintainers

- **GitHub Actions**:
  - Weekly updates
  - Keeps workflows up-to-date

**Dependency Groups**:
1. Testing: pytest, pytest-cov, pytest-mock, pytest-asyncio
2. Linting: ruff, black, isort, mypy
3. Security: bandit, pip-audit

**Documentation**: [Dependabot Documentation](https://docs.github.com/en/code-security/dependabot)

## Tool Configuration

All tools are configured in `pyproject.toml` for centralized management:

```toml
[tool.ruff]          # Linting rules
[tool.black]         # Formatting
[tool.isort]         # Import sorting
[tool.mypy]          # Type checking
[tool.pytest.ini_options]  # Testing
[tool.coverage]      # Coverage settings
[tool.bandit]        # Security scanning
[tool.interrogate]   # Docstring coverage
[tool.vulture]       # Dead code detection
[tool.commitizen]    # Commit conventions
```

## Best Practices

### Daily Development

1. **Before Starting Work**:
   ```bash
   make pre-commit-update  # Update hooks weekly
   ```

2. **During Development**:
   ```bash
   make lint-fix  # Fix linting issues
   make test      # Run tests
   ```

3. **Before Committing**:
   ```bash
   make ci  # Run all checks
   ```

   Or let pre-commit handle it automatically!

4. **Making Commits**:
   ```bash
   make commit  # Use commitizen for conventional commits
   ```

### Weekly Maintenance

1. **Update Dependencies**:
   - Review Dependabot PRs
   - Test updates locally
   - Merge if all checks pass

2. **Security Audits**:
   ```bash
   make audit  # Check for vulnerabilities
   ```

3. **Dead Code Cleanup**:
   ```bash
   make vulture  # Find unused code
   ```

4. **Documentation**:
   ```bash
   make interrogate  # Check docstring coverage
   make docs-serve  # Review documentation
   ```

### Before Releases

1. **Full CI Check**:
   ```bash
   make ci
   ```

2. **Version Bump**:
   ```bash
   make bump
   ```

3. **Generate Changelog**:
   ```bash
   make changelog
   ```

4. **Build Documentation**:
   ```bash
   make docs-deploy
   ```

## Troubleshooting

### Common Issues

**Pre-commit hooks failing**:
```bash
pre-commit clean
pre-commit install --install-hooks
pre-commit run --all-files
```

**Type checking errors**:
```bash
# Check specific file
mypy path/to/file.py --show-error-codes

# Add to pyproject.toml ignore list if needed
```

**Test failures**:
```bash
# Run with verbose output and logs
pytest tests/ -vv -s --log-cli-level=DEBUG
```

**Coverage too low**:
```bash
# See what's not covered
pytest --cov=src --cov-report=html
# Open htmlcov/index.html in browser
```

## Resources

- [Python Code Quality Guide](https://realpython.com/python-code-quality/)
- [KDnuggets: 30 Must-Know Tools for Python Development](https://www.kdnuggets.com/2025/02/nettresults/30-must-know-tools-for-python-development)
- [Top Python Libraries 2025](https://tryolabs.com/blog/top-python-libraries-2025)
- [Python Security Tools](https://www.aikido.dev/blog/top-python-security-tools)

## Summary

This project is equipped with a comprehensive set of modern development tools:

✅ **Code Quality**: Ruff, Black, isort, mypy
✅ **Testing**: pytest with coverage, mocking, and async support
✅ **Security**: Bandit, pip-audit
✅ **Documentation**: interrogate, MkDocs, mkdocstrings
✅ **Workflow**: pre-commit, commitizen, vulture, Make
✅ **CI/CD**: GitHub Actions, Dependabot
✅ **Configuration**: Centralized in pyproject.toml
✅ **Automation**: Makefile with 30+ commands

All tools are configured, tested, and ready to use. Start with `make help` to explore available commands!
