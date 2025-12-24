.PHONY: help install install-dev test test-verbose test-cov lint format type-check security audit docs clean all ci pre-commit

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install --editable '.[dev]'
	pre-commit install

test: ## Run tests
	pytest tests/ -v

test-verbose: ## Run tests with verbose output
	pytest tests/ -vv -s

test-cov: ## Run tests with coverage report
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html --cov-report=xml

test-unit: ## Run unit tests only
	pytest tests/unit/ -v

test-integration: ## Run integration tests only
	pytest tests/integration/ -v

test-security: ## Run security tests only
	pytest tests/security/ -v

lint: ## Run all linters
	ruff check .
	black --check .
	isort --check-only .

lint-fix: ## Run linters with auto-fix
	ruff check --fix .
	black .
	isort .

format: lint-fix ## Format code (alias for lint-fix)

type-check: ## Run type checking with mypy
	mypy src

security: ## Run security checks
	bandit -r src -c pyproject.toml
	pip-audit

audit: security ## Alias for security

vulture: ## Check for dead code
	vulture src app.py --min-confidence=80

interrogate: ## Check docstring coverage
	interrogate -vv --fail-under=80 src

docs-coverage: interrogate ## Alias for interrogate

docs: ## Build documentation
	cd docs && mkdocs build

docs-serve: ## Serve documentation locally
	cd docs && mkdocs serve

docs-deploy: ## Deploy documentation to GitHub Pages
	cd docs && mkdocs gh-deploy

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
	pre-commit run --all-files

pre-commit-update: ## Update pre-commit hooks
	pre-commit autoupdate

ci: lint type-check test security ## Run all CI checks (lint, type-check, test, security)

all: clean install-dev lint type-check test security docs ## Run full development setup

commit: ## Make a conventional commit using commitizen
	cz commit

bump: ## Bump version using commitizen
	cz bump

changelog: ## Generate changelog
	cz changelog

run: ## Run the Chainlit application
	chainlit run app.py

run-cli: ## Run the CLI version
	python src/backend/main.py
