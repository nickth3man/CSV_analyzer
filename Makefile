.PHONY: help install install-dev test test-verbose test-cov lint format type-check security audit docs clean all ci pre-commit

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies using uv
	uv sync --no-dev

install-dev: ## Install development dependencies using uv
	uv sync
	uv run pre-commit install

test: ## Run tests
	uv run pytest src/tests/ -v --no-cov

test-verbose: ## Run tests with verbose output
	uv run pytest src/tests/ -vv -s --no-cov

test-cov: ## Run tests with coverage report
	uv run pytest src/tests/ -v --cov=src --cov-report=term-missing --cov-report=html --cov-report=xml

test-unit: ## Run unit tests only
	uv run pytest src/tests/unit/ -v --no-cov

test-integration: ## Run integration tests only
	uv run pytest src/tests/integration/ -v --no-cov

test-security: ## Run security tests only
	uv run pytest src/tests/security/ -v --no-cov

lint: ## Run all linters
	uv run ruff check .
	uv run ruff format --check .

lint-fix: ## Run linters with auto-fix
	uv run ruff check --fix .
	uv run ruff format .

format: lint-fix ## Format code (alias for lint-fix)

type-check: ## Run type checking with mypy
	uv run mypy src/

security: ## Run security checks
	uv run bandit -r src -c pyproject.toml || true
	uv run pip-audit || true

audit: security ## Alias for security

vulture: ## Check for dead code
	uv run vulture src src/frontend/app.py --min-confidence=80

interrogate: ## Check docstring coverage
	uv run interrogate -vv --fail-under=80 src || true

docs-coverage: interrogate ## Alias for interrogate

docs: ## Build documentation
	uv run mkdocs build

docs-serve: ## Serve documentation locally
	uv run mkdocs serve

docs-deploy: ## Deploy documentation to GitHub Pages
	uv run mkdocs gh-deploy

clean: ## Clean build artifacts and cache files
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -f coverage.xml
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

pre-commit: ## Run pre-commit hooks on all files
	uv run pre-commit run --all-files

pre-commit-update: ## Update pre-commit hooks
	uv run pre-commit autoupdate

ci: lint type-check test ## Run all CI checks (lint, type-check, test)

all: clean install-dev lint type-check test docs ## Run full development setup

commit: ## Make a conventional commit using commitizen
	uv run cz commit

bump: ## Bump version using commitizen
	uv run cz bump

changelog: ## Generate changelog
	uv run cz changelog

run: ## Run the Chainlit application
	uv run chainlit run src/frontend/app.py

run-cli: ## Run the CLI version
	uv run python src/backend/main.py
