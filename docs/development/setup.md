# Development Setup

This guide will help you set up your development environment for contributing to NBA Expert.

## Prerequisites

- Python 3.12 or higher
- Git
- Make (optional, but recommended)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/nickth3man/nba_expert.git
cd nba_expert
```

### 2. Install Development Dependencies

Using Make:
```bash
make install-dev
```

Or manually:
```bash
pip install --editable '.[dev]'
pre-commit install
```

## Development Tools

The project uses a comprehensive set of development tools:

### Code Quality

- **Ruff**: Fast Python linter and formatter
- **Black**: Code formatter
- **isort**: Import statement organizer
- **mypy**: Static type checker

### Testing

- **pytest**: Testing framework
- **pytest-cov**: Code coverage
- **pytest-mock**: Mocking support
- **pytest-asyncio**: Async test support

### Security

- **Bandit**: Security vulnerability scanner
- **pip-audit**: Dependency vulnerability scanner

### Documentation

- **interrogate**: Docstring coverage checker
- **MkDocs**: Documentation generator
- **mkdocs-material**: Material theme for MkDocs

### Other Tools

- **vulture**: Dead code detector
- **commitizen**: Conventional commit helper
- **pre-commit**: Git hook manager

## Common Tasks

All common development tasks are available through the Makefile:

### Running Tests

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run unit tests only
make test-unit

# Run integration tests only
make test-integration
```

### Code Quality Checks

```bash
# Run all linters
make lint

# Auto-fix linting issues
make lint-fix

# Run type checking
make type-check

# Run security checks
make security

# Check for dead code
make vulture

# Check docstring coverage
make interrogate
```

### Running the Application

```bash
# Run Chainlit web app
make run

# Run CLI version
make run-cli
```

### Documentation

```bash
# Build documentation
make docs

# Serve documentation locally
make docs-serve

# Deploy documentation to GitHub Pages
make docs-deploy
```

### All CI Checks

Run all checks that CI runs:
```bash
make ci
```

### Cleaning

Remove build artifacts and cache files:
```bash
make clean
```

### Committing Changes

Use conventional commits:
```bash
make commit
```

## Pre-commit Hooks

Pre-commit hooks are configured to run automatically before each commit. They include:

- Trailing whitespace removal
- End-of-file fixer
- YAML/TOML/JSON validation
- Large file check
- Merge conflict detection
- Debug statement detection
- Private key detection
- Ruff linting and formatting
- Black formatting
- isort import sorting
- mypy type checking
- Bandit security scanning
- interrogate docstring coverage (on modified Python files)

To run pre-commit hooks manually:
```bash
make pre-commit
```

To update pre-commit hooks:
```bash
make pre-commit-update
```

## Code Style

- Line length: 88 characters (Black default)
- Python version: 3.12+
- Docstring style: Google
- Import style: Black-compatible isort

See [Code Style](code-style.md) for detailed guidelines.

## Testing Guidelines

- Write tests for all new features
- Maintain or improve code coverage
- Use descriptive test names
- Follow the AAA pattern (Arrange, Act, Assert)

See [Testing](testing.md) for detailed guidelines.

## Continuous Integration

GitHub Actions runs the following checks on every push and pull request:

1. **Lint**: Ruff, Black, isort
2. **Type Check**: mypy
3. **Test**: pytest with coverage
4. **Security**: Bandit, pip-audit
5. **Documentation Coverage**: interrogate
6. **Dead Code**: vulture

All checks must pass before merging.

## Troubleshooting

### Pre-commit hooks failing

Update hooks to latest versions:
```bash
pre-commit autoupdate
pre-commit run --all-files
```

### Type checking errors

Mypy is configured to be lenient. If you encounter errors:
```bash
# Check specific file
mypy path/to/file.py

# Ignore errors for legacy code (add to pyproject.toml)
```

### Test failures

Run tests with verbose output:
```bash
pytest tests/ -vv -s
```

## Getting Help

- Check the [Contributing Guide](contributing.md)
- Open an issue on GitHub
- Review existing issues and pull requests
